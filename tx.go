package mydb

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"
	"unsafe"
)

// 表示一个内部事务的标识
type txid uint64

// Tx 表示一个在数据库里的只读或者读写事务
// 只读事务只用来通过键获取值和创建游标
// 读写事务用来创建和删除桶，还有创建和删除键
//
// 重要：如果你用完了事务，你必须提交或回滚事务
// 否则页不能被回收，那就不能被读写事务去申请
// 一个长时间的读事务可能造成数据库大小很快的变大
type Tx struct {
	writable       bool //标志是否为一个读写事务
	managed        bool
	db             *DB  // 关联的数据库
	meta           *meta // 关联的元数据
	root           Bucket // 关联的根桶
	pages          map[pgid]*page // 页id的一个映射，用来在读写事务中保存脏页（即需要写回到磁盘的页）
	stats          TxStats // 事务相关统计数据
	commitHandlers []func() // 提交事务的回调函数

	// 这个标志是专门给写相关方法用的，例如WriteTo()
	// 设置这个标志，事务就用指定的标志位（syscall.O_DIRECT）来打开数据库文件来拷贝数据
	//
	// 默认这个标志是不设值的，因为没有这个标志已经满足大部分情况了
	// 但是如果数据库文件很大，大过可用内存的时候，要读出整个数据库的话，
	// 设置这个标志，那就可以利用syscall.O_DIRECT来避免损坏页高速缓存
	WriteFlag int
}

// init 初始化事务
func (tx *Tx) init(db *DB) {
	tx.db = db
	tx.pages = nil

	// 从元数据页拷贝一个元数据来初始化事务，
	// 这样读写事务可以改变这个拷贝出来的元数据而不会污染原来的（写时复制）
	tx.meta = &meta{}
	db.meta().copy(tx.meta)

	// 从元数据拷贝根桶进来
	tx.root = newBucket(tx)
	tx.root.bucket = &bucket{}
	*tx.root.bucket = tx.meta.root

	// 如果是读写事务，则要累加事务和初始化脏页映射
	if tx.writable {
		tx.pages = make(map[pgid]*page)
		tx.meta.txid += txid(1)
	}
}

// ID 返回事务标识
func (tx *Tx) ID() int {
	return int(tx.meta.txid)
}

// DB 返回一个创建当前事务的数据库引用
func (tx *Tx) DB() *DB {
	return tx.db
}

// Size 返回当前事务下的数据库大小（字节）
func (tx *Tx) Size() int64 {
	return int64(tx.meta.pgid) * int64(tx.db.pageSize)
}

// Writable 返回事务是否可以执行写操作
func (tx *Tx) Writable() bool {
	return tx.writable
}


// Cursor 创建一个跟根桶关联的游标
// 游标返回的只有键，值是nil，因为根桶的键都是指向桶
// 游标只在事务打开的时候有效
// 事务关闭后不要使用游标了
func (tx *Tx) Cursor() *Cursor {
	return tx.root.Cursor()
}

// Stats 返回当前事务统计数据的一个拷贝
func (tx *Tx) Stats() TxStats {
	return tx.stats
}

// Bucket 通过名字返回桶
// 如果桶不存在返回nil
// 桶实例只在当前事务有效
func (tx *Tx) Bucket(name []byte) *Bucket {
	return tx.root.Bucket(name)
}

// CreateBucket 创建一个新桶
// 如果一个桶存在，桶名为空或者太长，就返回错误
// 桶实例只在当前事务有效
func (tx *Tx) CreateBucket(name []byte) (*Bucket, error) {
	return tx.root.CreateBucket(name)
}

// CreateBucketIfNotExists 如果桶不存在就创建一个新桶
// 如果桶名为空或者太长，就返回错误
// 桶实例只在当前事务有效
func (tx *Tx) CreateBucketIfNotExists(name []byte) (*Bucket, error) {
	return tx.root.CreateBucketIfNotExists(name)
}

// DeleteBucket 删除一个桶
// 如果桶不存在或者根据桶名搜索这个桶时返回的不是一个桶该返回的值时报错
func (tx *Tx) DeleteBucket(name []byte) error {
	return tx.root.DeleteBucket(name)
}

// ForEach 循环在根上的所有桶执行一个函数
// 如果函数返回错误，则循环停止，并返回该错误给调用者
func (tx *Tx) ForEach(fn func(name []byte, b *Bucket) error) error {
	return tx.root.ForEach(func(k, v []byte) error {
		if err := fn(k, tx.root.Bucket(k)); err != nil {
			return err
		}
		return nil
	})
}

// OnCommit 加一个回调函数，这个回调函数会在事务成功提交时执行
func (tx *Tx) OnCommit(fn func()) {
	tx.commitHandlers = append(tx.commitHandlers, fn)
}

// Commit 在写事务到时候用来写所有改动到磁盘并更新meta页，如果写入磁盘时发生错误，就返回错误
// Commit 也用来在只读事务中调用
func (tx *Tx) Commit() error {
	_assert(!tx.managed, "managed tx commit not allowed")
	if tx.db == nil {
		return ErrTxClosed
	} else if !tx.writable {
		return ErrTxNotWritable
	}


	// 重新平衡，因为有些节点有可能被删除了
	var startTime = time.Now()
	tx.root.rebalance()
	if tx.stats.Rebalance > 0 {
		tx.stats.RebalanceTime += time.Since(startTime)
	}

	// 溢出数据到脏页
	startTime = time.Now()
	if err := tx.root.spill(); err != nil {
		tx.rollback()
		return err
	}
	tx.stats.SpillTime += time.Since(startTime)

	// 释放旧的根桶
	tx.meta.root.root = tx.root.root

	opgid := tx.meta.pgid

	// 释放空闲列表并分配一个新页给他
	tx.db.freelist.free(tx.meta.txid, tx.db.page(tx.meta.freelist))
	p, err := tx.allocate((tx.db.freelist.size() / tx.db.pageSize) + 1)
	if err != nil {
		tx.rollback()
		return err
	}
	if err := tx.db.freelist.write(p); err != nil {
		tx.rollback()
		return err
	}
	tx.meta.freelist = p.id

	// 如果高水位已经上升，就扩大数据库
	if tx.meta.pgid > opgid {
		if err := tx.db.grow(int(tx.meta.pgid+1) * tx.db.pageSize); err != nil {
			tx.rollback()
			return err
		}
	}

	// 写脏页到磁盘
	startTime = time.Now()
	if err := tx.write(); err != nil {
		tx.rollback()
		return err
	}

	// 如果严格模式启用，则会触发一次一致性检查
	// 发现错误并报Panic
	if tx.db.StrictMode {
		ch := tx.Check()
		var errs []string
		for {
			err, ok := <-ch
			if !ok {
				break
			}
			errs = append(errs, err.Error())
		}
		if len(errs) > 0 {
			panic("check fail: " + strings.Join(errs, "\n"))
		}
	}

	// 写meta到磁盘
	if err := tx.writeMeta(); err != nil {
		tx.rollback()
		return err
	}
	tx.stats.WriteTime += time.Since(startTime)

	// 关闭事务
	tx.close()

	// 执行提交的回调函数
	for _, fn := range tx.commitHandlers {
		fn()
	}

	return nil
}

// Rollback 关闭事务并忽略所有之前的改动，
// 只读事务必须回滚和没有提交
func (tx *Tx) Rollback() error {
	_assert(!tx.managed, "managed tx rollback not allowed")
	if tx.db == nil {
		return ErrTxClosed
	}
	tx.rollback()
	return nil
}

func (tx *Tx) rollback() {
	if tx.db == nil {
		return
	}
	if tx.writable {
		tx.db.freelist.rollback(tx.meta.txid) // 回滚之前的改动
		tx.db.freelist.reload(tx.db.page(tx.db.meta().freelist)) // 重新加载
	}
	tx.close()
}

// close 关闭事务
func (tx *Tx) close() {
	if tx.db == nil {
		return
	}
	if tx.writable {
		// 获取空闲列表统计数据
		var freelistFreeN = tx.db.freelist.free_count()
		var freelistPendingN = tx.db.freelist.pending_count()
		var freelistAlloc = tx.db.freelist.size()

		// 移除事务引用和写锁
		tx.db.rwtx = nil
		tx.db.rwlock.Unlock()

		// 合并统计数据
		tx.db.statlock.Lock()
		tx.db.stats.FreePageN = freelistFreeN
		tx.db.stats.PendingPageN = freelistPendingN
		tx.db.stats.FreeAlloc = (freelistFreeN + freelistPendingN) * tx.db.pageSize
		tx.db.stats.FreelistInuse = freelistAlloc
		tx.db.stats.TxStats.add(&tx.stats)
		tx.db.statlock.Unlock()
	} else {
		tx.db.removeTx(tx)
	}

	// 清空所有引用
	tx.db = nil
	tx.meta = nil
	tx.root = Bucket{tx: tx}
	tx.pages = nil
}

// Copy 写完整的数据库到一个Writer
// 这个函数存在是为了向后兼容
//
//  废弃；请用WriteTo()代替
func (tx *Tx) Copy(w io.Writer) error {
	_, err := tx.WriteTo(w)
	return err
}

// WriteTo 写完整的数据库到一个Writer
// 如果 err == nil 则写入 tx.Size() 字节到 writer.
func (tx *Tx) WriteTo(w io.Writer) (n int64, err error) {
	// 尝试用写标志打开文件（reader）
	f, err := os.OpenFile(tx.db.path, os.O_RDONLY|tx.WriteFlag, 0)
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()

	// 生成一个meta页，用这个页的数据来写两个meta页到writer
	buf := make([]byte, tx.db.pageSize)
	page := (*page)(unsafe.Pointer(&buf[0]))
	page.flags = metaPageFlag
	*page.meta() = *tx.meta

	// 写 meta 0.
	page.id = 0
	page.meta().checksum = page.meta().sum64()
	nn, err := w.Write(buf)
	n += int64(nn)
	if err != nil {
		return n, fmt.Errorf("meta 0 copy: %s", err)
	}

	// 用更低的事务来写 meta 1 
	page.id = 1
	page.meta().txid -= 1
	page.meta().checksum = page.meta().sum64()
	nn, err = w.Write(buf)
	n += int64(nn)
	if err != nil {
		return n, fmt.Errorf("meta 1 copy: %s", err)
	}

	// 在文件上跳过两个meta页的位置
	if _, err := f.Seek(int64(tx.db.pageSize*2), os.SEEK_SET); err != nil {
		return n, fmt.Errorf("seek: %s", err)
	}

	// 拷贝数据页
	wn, err := io.CopyN(w, f, tx.Size()-int64(tx.db.pageSize*2))
	n += wn
	if err != nil {
		return n, err
	}

	return n, f.Close()
}

// CopyFile 拷贝完整的数据库到指定路径下到文件
// 一个可读的事务用来拷贝可以使得在拷贝过程中安全的被其他事务使用数据库而不受影响
func (tx *Tx) CopyFile(path string, mode os.FileMode) error {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return err
	}

	err = tx.Copy(f)
	if err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// Check 为这个事务在数据库上执行一堆一致性检查
// 如果找到不一致的，就返回一个错误
//
// It can be safely run concurrently on a writable transaction. However, this
// incurs a high cost for large databases and databases with a lot of subbuckets
// because of caching. This overhead can be removed if running on a read-only
// transaction, however, it is not safe to execute other writer transactions at
// the same time.
func (tx *Tx) Check() <-chan error {
	ch := make(chan error)
	go tx.check(ch)
	return ch
}

func (tx *Tx) check(ch chan error) {
	// 检查是否页被重复释放
	freed := make(map[pgid]bool)
	all := make([]pgid, tx.db.freelist.count())
	tx.db.freelist.copyall(all)
	for _, id := range all {
		if freed[id] {
			ch <- fmt.Errorf("page %d: already freed", id)
		}
		freed[id] = true
	}

	// 跟踪每个可达的页
	reachable := make(map[pgid]*page)
	reachable[0] = tx.page(0) // meta0
	reachable[1] = tx.page(1) // meta1
	for i := uint32(0); i <= tx.page(tx.meta.freelist).overflow; i++ {
		reachable[tx.meta.freelist+pgid(i)] = tx.page(tx.meta.freelist)
	}

	// 递归检查桶
	tx.checkBucket(&tx.root, reachable, freed, ch)

	// 保证低于高水位的页都是可达或者释放的了
	for i := pgid(0); i < tx.meta.pgid; i++ {
		_, isReachable := reachable[i]
		if !isReachable && !freed[i] {
			ch <- fmt.Errorf("page %d: unreachable unfreed", int(i))
		}
	}

	// 关闭管道用来通知完成了
	close(ch)
}

func (tx *Tx) checkBucket(b *Bucket, reachable map[pgid]*page, freed map[pgid]bool, ch chan error) {

	// 忽略内联桶
	if b.root == 0 {
		return
	}


	// 检查这个桶里用过的页
	b.tx.forEachPage(b.root, 0, func(p *page, _ int) {
		if p.id > tx.meta.pgid {
			ch <- fmt.Errorf("page %d: out of bounds: %d", int(p.id), int(b.tx.meta.pgid))
		}

		// 保证每个页只被引用一次
		for i := pgid(0); i <= pgid(p.overflow); i++ {
			var id = p.id + i
			if _, ok := reachable[id]; ok {
				ch <- fmt.Errorf("page %d: multiple references", int(id))
			}
			reachable[id] = p
		}

		// 只会遇到没有被释放的叶子和分支页
		if freed[p.id] {
			ch <- fmt.Errorf("page %d: reachable freed", int(p.id))
		} else if (p.flags&branchPageFlag) == 0 && (p.flags&leafPageFlag) == 0 {
			ch <- fmt.Errorf("page %d: invalid type: %s", int(p.id), p.typ())
		}
	})

	// 检查这个桶下的每个桶
	_ = b.ForEach(func(k, v []byte) error {
		if child := b.Bucket(k); child != nil {
			tx.checkBucket(child, reachable, freed, ch)
		}
		return nil
	})
}

// 分配一个连续块的内存，并返回以该内存开始的页
func (tx *Tx) allocate(count int) (*page, error) {
	p, err := tx.db.allocate(count)
	if err != nil {
		return nil, err
	}

	// 保存到脏页映射表里
	tx.pages[p.id] = p

	// 更新统计数据
	tx.stats.PageCount++
	tx.stats.PageAlloc += count * tx.db.pageSize

	return p, nil
}

// 写脏页到硬盘里
func (tx *Tx) write() error {
	// 根据id排序页
	pages := make(pages, 0, len(tx.pages))
	for _, p := range tx.pages {
		pages = append(pages, p)
	}
	// 提前清空页缓存
	tx.pages = make(map[pgid]*page)
	sort.Sort(pages)

	// 按顺序把页写入磁盘
	for _, p := range pages {
		size := (int(p.overflow) + 1) * tx.db.pageSize
		offset := int64(p.id) * int64(tx.db.pageSize)

		// 按照“最大分配”的大小的块写入磁盘
		ptr := (*[maxAllocSize]byte)(unsafe.Pointer(p))
		for {
			// 限制一次写入只能是“最大分配”的大小的块
			sz := size
			if sz > maxAllocSize-1 {
				sz = maxAllocSize - 1
			}

			// 写块到磁盘
			buf := ptr[:sz]
			if _, err := tx.db.ops.writeAt(buf, offset); err != nil {
				return err
			}

			// 更新统计数据
			tx.stats.Write++

			// 如果写完所有块则退出里循环
			size -= sz
			if size == 0 {
				break
			}

			// 否则移动位移和指针到下一个块
			offset += int64(sz)
			ptr = (*[maxAllocSize]byte)(unsafe.Pointer(&ptr[sz]))
		}
	}

	// 如果标记有设置则忽略文件同步
	if !tx.db.NoSync || IgnoreNoSync {
		if err := fdatasync(tx.db); err != nil {
			return err
		}
	}

	// 把小页放回到页池子
	for _, p := range pages {

		/// 忽略那些大于一个页的页
		// 因为这些是由make()分配而不是由页池分配
		if int(p.overflow) != 0 {
			continue
		}

		buf := (*[maxAllocSize]byte)(unsafe.Pointer(p))[:tx.db.pageSize]

		// 看 https://go.googlesource.com/go/+/f03c9202c43e0abb130669852082117ca50aa9b1
		for i := range buf {
			buf[i] = 0
		}
		tx.db.pagePool.Put(buf)
	}

	return nil
}

// writeMeta 写meta到磁盘
func (tx *Tx) writeMeta() error {
	// 创建一个临时的buffer给meta页
	buf := make([]byte, tx.db.pageSize)
	p := tx.db.pageInBuffer(buf, 0)
	tx.meta.write(p)

	// 写meta页到文件
	if _, err := tx.db.ops.writeAt(buf, int64(p.id)*int64(tx.db.pageSize)); err != nil {
		return err
	}
	if !tx.db.NoSync || IgnoreNoSync {
		if err := fdatasync(tx.db); err != nil {
			return err
		}
	}

	// 更新统计数据
	tx.stats.Write++

	return nil
}

// page 根据id返回一个页到引用
// 如果页已经脏，则在脏页缓存里面返回
func (tx *Tx) page(id pgid) *page {
	// 检查是否为脏页
	if tx.pages != nil {
		if p, ok := tx.pages[id]; ok {
			return p
		}
	}

	// 否则从mmap（内存映射）直接返回
	return tx.db.page(id)
}

// forEachPage 遍历每个页并对这个页执行一个函数
func (tx *Tx) forEachPage(pgid pgid, depth int, fn func(*page, int)) {
	p := tx.page(pgid)

	// 执行函数
	fn(p, depth)

	// 递归遍历孩子
	if (p.flags & branchPageFlag) != 0 {
		for i := 0; i < int(p.count); i++ {
			elem := p.branchPageElement(uint16(i))
			tx.forEachPage(elem.pgid, depth+1, fn)
		}
	}
}

// Page 通过页ID，返回一个页信息
// 在并发环境下，这是唯一安全的使用写事务的方法
func (tx *Tx) Page(id int) (*PageInfo, error) {
	if tx.db == nil {
		return nil, ErrTxClosed
	} else if pgid(id) >= tx.meta.pgid {
		return nil, nil
	}

	// 创建页信息结构
	p := tx.db.page(pgid(id))
	info := &PageInfo{
		ID:            id,
		Count:         int(p.count),
		OverflowCount: int(p.overflow),
	}

	// 决定类型（是否释放）
	if tx.db.freelist.freed(pgid(id)) {
		info.Type = "free"
	} else {
		info.Type = p.typ()
	}

	return info, nil
}

// TxStats 代表这个事务执行的动作的统计数据
type TxStats struct {
	// Page statistics.
	PageCount int // number of page allocations
	PageAlloc int // total bytes allocated

	// Cursor statistics.
	CursorCount int // number of cursors created

	// Node statistics
	NodeCount int // number of node allocations
	NodeDeref int // number of node dereferences

	// Rebalance statistics.
	Rebalance     int           // number of node rebalances
	RebalanceTime time.Duration // total time spent rebalancing

	// Split/Spill statistics.
	Split     int           // number of nodes split
	Spill     int           // number of nodes spilled
	SpillTime time.Duration // total time spent spilling

	// Write statistics.
	Write     int           // number of writes performed
	WriteTime time.Duration // total time spent writing to disk
}

func (s *TxStats) add(other *TxStats) {
	s.PageCount += other.PageCount
	s.PageAlloc += other.PageAlloc
	s.CursorCount += other.CursorCount
	s.NodeCount += other.NodeCount
	s.NodeDeref += other.NodeDeref
	s.Rebalance += other.Rebalance
	s.RebalanceTime += other.RebalanceTime
	s.Split += other.Split
	s.Spill += other.Spill
	s.SpillTime += other.SpillTime
	s.Write += other.Write
	s.WriteTime += other.WriteTime
}

// Sub calculates and returns the difference between two sets of transaction stats.
// This is useful when obtaining stats at two different points and time and
// you need the performance counters that occurred within that time span.
func (s *TxStats) Sub(other *TxStats) TxStats {
	var diff TxStats
	diff.PageCount = s.PageCount - other.PageCount
	diff.PageAlloc = s.PageAlloc - other.PageAlloc
	diff.CursorCount = s.CursorCount - other.CursorCount
	diff.NodeCount = s.NodeCount - other.NodeCount
	diff.NodeDeref = s.NodeDeref - other.NodeDeref
	diff.Rebalance = s.Rebalance - other.Rebalance
	diff.RebalanceTime = s.RebalanceTime - other.RebalanceTime
	diff.Split = s.Split - other.Split
	diff.Spill = s.Spill - other.Spill
	diff.SpillTime = s.SpillTime - other.SpillTime
	diff.Write = s.Write - other.Write
	diff.WriteTime = s.WriteTime - other.WriteTime
	return diff
}