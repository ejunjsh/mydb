package mydb

import (
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"time"
	"unsafe"
)

// 重新映射时候加的最大大小
const maxMmapStep = 1 << 30 // 1GB

// 数据文件格式版本
const version = 2

// 魔数，代表这个是一个db
const magic uint32 = 0xED0CDAED

// IgnoreNoSync 表示当要同步改动到文件时，DB里面的NoSync字段是否被忽略
// 因为有些操作系统例如OpenBSD，它没有联合缓冲缓存(UBC)，每次写都必须用msync(2)系统调用同步
const IgnoreNoSync = runtime.GOOS == "openbsd"

// 如果没有设置到DB实例，那就用下面的默认值
const (
	DefaultMaxBatchSize  int = 1000
	DefaultMaxBatchDelay     = 10 * time.Millisecond
	DefaultAllocSize         = 16 * 1024 * 1024
)

// 默认页大小等于系统的页大小
var defaultPageSize = os.Getpagesize()

// DB 表示一个桶集合，并把桶持久化到文件里面
// 所有数据访问都是通过事务，而这个事务又可以从DB里面获得
// 如果访问之前没有打开Open()，则DB里面的所有函数都会返回ErrDatabaseNotOpen
type DB struct {
	// 如果启用，数据库会在每次提交的时候调用Check()
	// 如果数据库处于不一致状态就会报panic
	// 因为这个标志有很大性能影响，所以应该只用在调试的时候
	StrictMode bool

	// 设置NoSync标志将导致数据库在每次提交时跳过fsync()
	// 这对于批量加载数据进数据库非常有用，这样你可以重启这个批量加载，即使发生了系统错误或数据库损害
	// 尽可能正常使用时不要设置这个标志位
	//
	// 如果包全局常量IgnoreNoSync是true，则这个值被忽略
	// 详情可以看看这个常量到注释
	//
	// 这个不安全，请谨慎使用
	NoSync bool

	// 当这个为true时，当扩张数据库时，跳过调用truncate
	// 当这个为true时，只在non-ext3/ext4 系统安全
	// 跳过truncate可以避免预分配硬盘空间和绕过在重新映射时truncate()和fsync()这两个系统调用
	//
	// https://github.com/boltdb/bolt/issues/284
	NoGrowSync bool

	// MmapFlags设置内存映射时的标志位
	// 如果你想读取数据库快点，并且你的linux版本是2.6.23+，
	// 请设置syscall.MAP_POPULATE，开启文件预读（sequential read-ahead）
	MmapFlags int

	// MaxBatchSize 是一个batch的最大容量
	// 默认值是DefaultMaxBatchSize
	// 如果小于等于0，则batch功能不可用
	// 不要在调用Batch的时候改这个值
	MaxBatchSize int

	// MaxBatchDelay 是开始一个batch之前的等待时间
	// 默认值是DefaultMaxBatchDelay
	// 如果小于等于0，则batch功能不可用
	// 不要在调用Batch的时候改这个值
	MaxBatchDelay time.Duration

	// 当数据库需要创建新当页时，AllocSize 是每次数据库扩张的时候加的量
	// 当扩张数据文件时，这样做是用来分摊调用truncate()和fsync()成本
	AllocSize int

	path     string
	file     *os.File
	lockfile *os.File // 只在window下有用
	dataref  []byte   // mmap'ed readonly, write throws SEGV
	data     *[maxMapSize]byte
	datasz   int
	filesz   int // 当前数据文件大小
	meta0    *meta
	meta1    *meta
	pageSize int
	opened   bool
	rwtx     *Tx
	txs      []*Tx
	freelist *freelist
	stats    Stats

	pagePool sync.Pool

	batchMu sync.Mutex
	batch   *batch

	rwlock   sync.Mutex   // 一次只允许一个写
	metalock sync.Mutex   // 保护meta页的访问
	mmaplock sync.RWMutex // 在重新映射时保护内存映射访问
	statlock sync.RWMutex // 保护统计数据的访问

	ops struct {
		writeAt func(b []byte, off int64) (n int, err error)
	}

	// 只读模式
	// 当这个为true时，Update() 和 Begin(true)会立即返回ErrDatabaseReadOnly错误
	readOnly bool
}

// Path 返回当前打开的数据库文件的路径
func (db *DB) Path() string {
	return db.path
}

// GoString 返回数据库的Go字符串的表示
func (db *DB) GoString() string {
	return fmt.Sprintf("bolt.DB{path:%q}", db.path)
}

// String 返回数据库的字符串的表示
func (db *DB) String() string {
	return fmt.Sprintf("DB<%q>", db.path)
}

// Open 给定一个路径，创建和打开一个数据库
// 如果数据库文件不存在，自动创建一个
// options参数传入nil的话，数据库会用默认配置创建
func Open(path string, mode os.FileMode, options *Options) (*DB, error) {
	var db = &DB{opened: true}

	// 如果option不提供，使用默认配置
	if options == nil {
		options = DefaultOptions
	}
	db.NoGrowSync = options.NoGrowSync
	db.MmapFlags = options.MmapFlags

	// 为后面DB操作赋默认值
	db.MaxBatchSize = DefaultMaxBatchSize
	db.MaxBatchDelay = DefaultMaxBatchDelay
	db.AllocSize = DefaultAllocSize

	flag := os.O_RDWR
	if options.ReadOnly {
		flag = os.O_RDONLY
		db.readOnly = true
	}

	// 打开数据文件
	db.path = path
	var err error
	if db.file, err = os.OpenFile(db.path, flag|os.O_CREATE, mode); err != nil {
		_ = db.close()
		return nil, err
	}

	// 锁文件用来防止其他进程同一时间对数据库文件进行读写操作
	// 如果不锁，容易造成数据有问题，因为两个进程有可能分别写meta页和空闲页
	// 如果!options.ReadOnly 为true，则是独占锁来锁（只允许一个进程访问）
	// 否则用共享锁（这时候允许多个进程来访问）
	if err := flock(db, mode, !db.readOnly, options.Timeout); err != nil {
		_ = db.close()
		return nil, err
	}

	// 设置默认值，方便测试
	db.ops.writeAt = db.file.WriteAt

	// 如果数据库不存在则创建它
	if info, err := db.file.Stat(); err != nil {
		return nil, err
	} else if info.Size() == 0 {
		// 用meta页初始化文件
		if err := db.init(); err != nil {
			return nil, err
		}
	} else {
		// 读取第一个meta页来确定页大小
		var buf [0x1000]byte
		if _, err := db.file.ReadAt(buf[:], 0); err == nil {
			m := db.pageInBuffer(buf[:], 0).meta()
			if err := m.validate(); err != nil {
				// If we can't read the page size, we can assume it's the same
				// as the OS -- since that's how the page size was chosen in the
				// first place.
				// 如果我们不能读取页大小，我们假设这个页大小是跟系统一样的
				// 因为第一次取页大小也是从系统那里取的
				//
				// 如果第一个页无效，并且这个操作系统使用的页大小与创建数据库时使用的页大小不同，
				// 那么我们就走运了，无法访问数据库。
				db.pageSize = os.Getpagesize()
			} else {
				db.pageSize = int(m.pageSize)
			}
		}
	}

	// 初始化页池
	db.pagePool = sync.Pool{
		New: func() interface{} {
			return make([]byte, db.pageSize)
		},
	}

	// 内存映射到数据文件
	if err := db.mmap(options.InitialMmapSize); err != nil {
		_ = db.close()
		return nil, err
	}

	// 加载空闲列表
	db.freelist = newFreelist()
	db.freelist.read(db.page(db.meta().freelist))

	// 标志这个数据库为打开然后返回
	return db, nil
}

// mmap 打开一个系统底层的内存映射到一个文件，同时初始化meta的引用
// minsz是最小的映射大小
func (db *DB) mmap(minsz int) error {
	db.mmaplock.Lock()
	defer db.mmaplock.Unlock()

	info, err := db.file.Stat()
	if err != nil {
		return fmt.Errorf("mmap stat error: %s", err)
	} else if int(info.Size()) < db.pageSize*2 {
		return fmt.Errorf("file size too small")
	}

	// 保证大小至少是minsz
	var size = int(info.Size())
	if size < minsz {
		size = minsz
	}
	size, err = db.mmapSize(size)
	if err != nil {
		return err
	}

	// 在解除映射之前去除所有对内存映射的引用
	if db.rwtx != nil {
		db.rwtx.root.dereference()
	}

	// 往下走之前解除映射
	if err := db.munmap(); err != nil {
		return err
	}

	// 内存映射
	if err := mmap(db, size); err != nil {
		return err
	}

	// 保存对meta页对引用
	db.meta0 = db.page(0).meta()
	db.meta1 = db.page(1).meta()

	// 只有两个validate失败才算失败，因为有一个失败只是代表它没保存适当，我们可以从另外一个中恢复
	err0 := db.meta0.validate()
	err1 := db.meta1.validate()
	if err0 != nil && err1 != nil {
		return err0
	}

	return nil
}

// munmap 取消到文件的内存映射
func (db *DB) munmap() error {
	if err := munmap(db); err != nil {
		return fmt.Errorf("unmap error: " + err.Error())
	}
	return nil
}

// mmapSize 通过给定的数据库当前大小来决定合适映射大小
// 最小的大小为32KB，然后乘2直到1GB
// 如果新的映射大小大于最大允许大小，则返回错误
func (db *DB) mmapSize(size int) (int, error) {
	// 32KB乘2，直到1GB
	for i := uint(15); i <= 30; i++ {
		if size <= 1<<i {
			return 1 << i, nil
		}
	}

	// 验证是否申请的大小大于允许的大小
	if size > maxMapSize {
		return 0, fmt.Errorf("mmap too large")
	}

	// 如果大于1GB，则每次加1GB
	sz := int64(size)
	if remainder := sz % int64(maxMmapStep); remainder > 0 {
		sz += int64(maxMmapStep) - remainder
	}

	// 保证映射的大小一定是页大小的整数倍
	pageSize := int64(db.pageSize)
	if (sz % pageSize) != 0 {
		sz = ((sz / pageSize) + 1) * pageSize
	}

	// 如果超过允许大小，就用允许大小返回
	if sz > maxMapSize {
		sz = maxMapSize
	}

	return int(sz), nil
}

// init 创建一个新的数据库文件和初始化meta页
func (db *DB) init() error {
	// 设置页大小为系统页大小
	db.pageSize = os.Getpagesize()

	// 在缓存里面创建两个页
	buf := make([]byte, db.pageSize*4)
	for i := 0; i < 2; i++ {
		p := db.pageInBuffer(buf[:], pgid(i))
		p.id = pgid(i)
		p.flags = metaPageFlag

		// 初始化meta页
		m := p.meta()
		m.magic = magic
		m.version = version
		m.pageSize = uint32(db.pageSize)
		m.freelist = 2
		m.root = bucket{root: 3}
		m.pgid = 4
		m.txid = txid(i)
		m.checksum = m.sum64()
	}

	// 写一个空的空闲链表到页2
	p := db.pageInBuffer(buf[:], pgid(2))
	p.id = pgid(2)
	p.flags = freelistPageFlag
	p.count = 0

	// 写一个空叶子页到页3
	p = db.pageInBuffer(buf[:], pgid(3))
	p.id = pgid(3)
	p.flags = leafPageFlag
	p.count = 0

	// 写缓冲到数据文件
	if _, err := db.ops.writeAt(buf, 0); err != nil {
		return err
	}
	if err := fdatasync(db); err != nil {
		return err
	}

	return nil
}

// Close 释放所有数据库资源
// 所有事务必须在数据库关闭之前关闭
func (db *DB) Close() error {
	db.rwlock.Lock()
	defer db.rwlock.Unlock()

	db.metalock.Lock()
	defer db.metalock.Unlock()

	db.mmaplock.RLock()
	defer db.mmaplock.RUnlock()

	return db.close()
}

func (db *DB) close() error {
	if !db.opened {
		return nil
	}

	db.opened = false

	db.freelist = nil

	// 清空io操作
	db.ops.writeAt = nil

	// 关闭内存映射
	if err := db.munmap(); err != nil {
		return err
	}

	// 关闭文件
	if db.file != nil {
		// 没必要去解锁只读文件
		if !db.readOnly {
			// 解锁文件
			if err := funlock(db); err != nil {
				log.Printf("bolt.Close(): funlock error: %s", err)
			}
		}

		// 关闭文件描述符
		if err := db.file.Close(); err != nil {
			return fmt.Errorf("db file close: %s", err)
		}
		db.file = nil
	}

	db.path = ""
	return nil
}

// Begin 开始一个新的事务
// 多个只读事务可以并发使用，但是一次只能有一个写事务
// 多个写事务会造成阻塞直到当前写事务完成为止
// 
// 事务不应该依赖另外一个。如果一个协程里面有一个读事务和写事务，有可能会造成写事务死锁，
// 因为数据库会周期性变大然后重新映射，如果这时候有个读事务打开的时候，写事务就有可能死锁了
//
// 如果一个长时间运行的读事务，你可能需要设置DB.InitialMmapSize为一个足够大的值
// 这样就可以避免潜在的来自写事务的阻塞
//
// 重要：你在使用完之后必须关闭只读事务，否则数据库没办法重复利用那些旧的页
func (db *DB) Begin(writable bool) (*Tx, error) {
	if writable {
		return db.beginRWTx()
	}
	return db.beginTx()
}

// beginTx 开始只读事务
func (db *DB) beginTx() (*Tx, error) {
	// 初始化事务的时候，锁meta页
	// 锁meta页在锁内存映射之前，是因为写事务也是这样的顺序
	db.metalock.Lock()

	// 获取一个内存映射只读锁。
	// 当内存重新映射的时候，需要获取写锁，所以所有获取读锁的事务完成之后才能获取写锁来重新映射内存
	db.mmaplock.RLock()

	// 如果数据库没有打开，则退出
	if !db.opened {
		db.mmaplock.RUnlock()
		db.metalock.Unlock()
		return nil, ErrDatabaseNotOpen
	}

	// 创建一个跟这个数据库关联的事务
	t := &Tx{}
	t.init(db)

	// 加到数据库的事务列表里面方便跟踪，直到事务关闭
	db.txs = append(db.txs, t)
	n := len(db.txs)

	// 解锁meta页
	db.metalock.Unlock()

	// 更新事务相关统计数据
	db.statlock.Lock()
	db.stats.TxN++
	db.stats.OpenTxN = n
	db.statlock.Unlock()

	return t, nil
}

// beginRWTx 开始写事务
func (db *DB) beginRWTx() (*Tx, error) {
	// 如果数据库是个只读的，则返回错误
	if db.readOnly {
		return nil, ErrDatabaseReadOnly
	}

	// 获取一个写事务锁，这个之后在当前事务关闭才会释放，所以整个数据库只能一次有一个写事务
	db.rwlock.Lock()

	// 一旦获取写事务锁，则接着锁meta页，这样就可以开始配置这个事务了
	db.metalock.Lock()
	defer db.metalock.Unlock()

	// 如果数据库没有打开则退出
	if !db.opened {
		db.rwlock.Unlock()
		return nil, ErrDatabaseNotOpen
	}

	// 创建一个跟这个数据库关联的事务
	t := &Tx{writable: true}
	t.init(db)
	db.rwtx = t

	// 释放只读事务关联的页
	var minid txid = 0xFFFFFFFFFFFFFFFF
	for _, t := range db.txs {
		if t.meta.txid < minid {
			minid = t.meta.txid
		}
	}
	if minid > 0 {
		db.freelist.release(minid - 1)
	}

	return t, nil
}

// removeTx 从数据库里面删除事务
func (db *DB) removeTx(tx *Tx) {
	// 释放一个内存映射的读锁
	db.mmaplock.RUnlock()

	// 获取meta锁来限制下对数据库的访问
	db.metalock.Lock()

	// 移除事务
	for i, t := range db.txs {
		if t == tx {
			last := len(db.txs) - 1
			db.txs[i] = db.txs[last]
			db.txs[last] = nil
			db.txs = db.txs[:last]
			break
		}
	}
	n := len(db.txs)

	// 解锁meta页
	db.metalock.Unlock()

	// 合并统计数据
	db.statlock.Lock()
	db.stats.OpenTxN = n
	db.stats.TxStats.add(&tx.stats)
	db.statlock.Unlock()
}

// Update 在读写事务上下文下执行一个函数
// 如果函数没有错误返回，则事务会被提交
// 如果函数有错误返回，则事务回滚
// 任何错误从函数或者提交时候返回都会作为Update的返回值
// 尝试在函数里面手动提交或回滚事务都会报panic
func (db *DB) Update(fn func(*Tx) error) error {
	t, err := db.Begin(true)
	if err != nil {
		return err
	}

	// 确保panic的时候事务能够回滚
	defer func() {
		if t.db != nil {
			t.rollback()
		}
	}()

	// 标记为管理事务，这样里面的函数就不能手动提交
	t.managed = true

	// 如果返回错误，则回滚和往外返回错误
	err = fn(t)
	t.managed = false
	if err != nil {
		_ = t.Rollback()
		return err
	}

	return t.Commit()
}

// View 在只读事务上下文下执行一个函数
// 任何错误从函数返回都会作为View的返回值
// 尝试在函数里面手动回滚事务都会报panic
func (db *DB) View(fn func(*Tx) error) error {
	t, err := db.Begin(false)
	if err != nil {
		return err
	}

	// 确保panic的时候事务能够回滚
	defer func() {
		if t.db != nil {
			t.rollback()
		}
	}()

	// 标记为管理事务，这样里面的函数就不能手动回滚
	t.managed = true

	// 如果返回错误，则回滚和往外返回错误
	err = fn(t)
	t.managed = false
	if err != nil {
		_ = t.Rollback()
		return err
	}

	if err := t.Rollback(); err != nil {
		return err
	}

	return nil
}

// Batch 调用一个函数fn，它的行为类似Update
// 除了：
//
// 1. 并发调用Batch，会合并到一个事务执行
//
// 2. 传给Batch的函数有可能被执行多次，无论这个函数报错没报错
//
// 这意味着Batch执行的函数必须是幂等的，而且在执行成功后数据才能持久化和被调用者看到
//
// 最大批大小和延迟时间可以通过这两个配置修改，DB.MaxBatchSize，DB.MaxBatchDelay
//
// Batch 只有在多协程调用它时，有用
func (db *DB) Batch(fn func(*Tx) error) error {
	errCh := make(chan error, 1)

	db.batchMu.Lock()
	if (db.batch == nil) || (db.batch != nil && len(db.batch.calls) >= db.MaxBatchSize) {
		// batch不存在，或者batch里面的函数满了，开一个新的
		db.batch = &batch{
			db: db,
		}
		db.batch.timer = time.AfterFunc(db.MaxBatchDelay, db.batch.trigger)
	}
	db.batch.calls = append(db.batch.calls, call{fn: fn, err: errCh})
	if len(db.batch.calls) >= db.MaxBatchSize {
		// 如果已经可以跑了，唤醒batch
		go db.batch.trigger()
	}
	db.batchMu.Unlock()

	err := <-errCh
	if err == trySolo {
		err = db.Update(fn)
	}
	return err
}

type call struct {
	fn  func(*Tx) error
	err chan<- error
}

type batch struct {
	db    *DB
	timer *time.Timer
	start sync.Once
	calls []call
}

// 如果没有运行，则触发运行这个批
func (b *batch) trigger() {
	b.start.Do(b.run)
}

// run 执行事务并返回结果给DB.Batch
func (b *batch) run() {
	b.db.batchMu.Lock()
	b.timer.Stop()
	// 确保没有新的被加到这个batch上，而且不会打断其他batch
	if b.db.batch == b {
		b.db.batch = nil
	}
	b.db.batchMu.Unlock()

retry:
	for len(b.calls) > 0 {
		var failIdx = -1
		err := b.db.Update(func(tx *Tx) error {
			for i, c := range b.calls {
				if err := safelyCall(c.fn, tx); err != nil {
					failIdx = i
					return err
				}
			}
			return nil
		})

		if failIdx >= 0 {
			// 把失败的移出去
			// 因为db.batch已经不指向当前的batch了，所以用下面的方式来移除失败的
			c := b.calls[failIdx]
			b.calls[failIdx], b.calls = b.calls[len(b.calls)-1], b.calls[:len(b.calls)-1]
			// 告诉调用者单独运行一下失败的，
			c.err <- trySolo
			continue retry // batch里其他的再运行一次
		}

		// 传递成功或者内部错误给所有调用者
		for _, c := range b.calls {
			c.err <- err
		}
		break retry
	}
}

// trySolo 是一个特殊的哨兵错误，专门用来通知那些需要重新运行的函数
// 这个不应该暴露给调用者看到
var trySolo = errors.New("batch function returned an error and should be re-run solo")

type panicked struct {
	reason interface{}
}

func (p panicked) Error() string {
	if err, ok := p.reason.(error); ok {
		return err.Error()
	}
	return fmt.Sprintf("panic: %v", p.reason)
}

// 安全调用，对Panic进行捕获并返回错误
func safelyCall(fn func(*Tx) error, tx *Tx) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = panicked{p}
		}
	}()
	return fn(tx)
}

// Sync 执行fdatasync()去同步数据到磁盘
// 在正常操作下是不需要调用这个方法到。但是如果设置了NoSync，则你就要手动调用这个方法来同步了
func (db *DB) Sync() error { return fdatasync(db) }

// Stats 返回数据库的性能统计数据
// 这个只会在事务关闭后更新
func (db *DB) Stats() Stats {
	db.statlock.RLock()
	defer db.statlock.RUnlock()
	return db.stats
}

// Info 这个是给内部去访问原始数据的，因为是通过c指针来访问，所以要小心点，或者就尽量别用吧
func (db *DB) Info() *Info {
	return &Info{uintptr(unsafe.Pointer(&db.data[0])), db.pageSize}
}

// page 返回一个页引用，该引用来自当前页大小的内存映射（mmap）
func (db *DB) page(id pgid) *page {
	pos := id * pgid(db.pageSize)
	return (*page)(unsafe.Pointer(&db.data[pos]))
}

// pageInBuffer 通过给定的字节数组和页大小，返回页引用
func (db *DB) pageInBuffer(b []byte, id pgid) *page {
	return (*page)(unsafe.Pointer(&b[id*pgid(db.pageSize)]))
}

// meta 返回一个当前最新的meta页引用
func (db *DB) meta() *meta {
	// 必须要返回一个最高事务id的meta并且它的校验是成功的
	// 否则返回错误，表示数据库不一致了
	// metaA是最高事务id的那个
	metaA := db.meta0
	metaB := db.meta1
	if db.meta1.txid > db.meta0.txid {
		metaA = db.meta1
		metaB = db.meta0
	}

	// 如果metaA有效，则用它，否则用metaB，当然也要有效才行
	if err := metaA.validate(); err == nil {
		return metaA
	} else if err := metaB.validate(); err == nil {
		return metaB
	}

	// 这个不可能到达的，因为meta1和meta0在mmap()是有效的，并且每次写的时候都fsync() 
	panic("bolt.DB.meta(): invalid meta pages")
}

// allocate 返回一个连续的内存块，它开始于给定的页
func (db *DB) allocate(count int) (*page, error) {
	// 为这个页分配一个临时的缓冲区
	var buf []byte
	if count == 1 {
		buf = db.pagePool.Get().([]byte)
	} else {
		buf = make([]byte, count*db.pageSize)
	}
	p := (*page)(unsafe.Pointer(&buf[0]))
	p.overflow = uint32(count - 1)

	// 如果空闲列表里面有，则直接返回
	if p.id = db.freelist.allocate(count); p.id != 0 {
		return p, nil
	}

	// 如果到底了， 重新映射 mmap() 
	p.id = db.rwtx.meta.pgid
	var minsz = int((p.id+pgid(count))+1) * db.pageSize
	if minsz >= db.datasz {
		if err := db.mmap(minsz); err != nil {
			return nil, fmt.Errorf("mmap allocate error: %s", err)
		}
	}

	// 移动水位
	db.rwtx.meta.pgid += pgid(count)

	return p, nil
}

// grow 扩大数据库的大小到sz
func (db *DB) grow(sz int) error {
	// 如果sz小于数据库的文件大小，则返回
	if sz <= db.filesz {
		return nil
	}

	// 如果数据大小小于每次分配的大小，则直接用数据大小
	// 否则就在加上一个分配大小
	if db.datasz < db.AllocSize {
		sz = db.datasz
	} else {
		sz += db.AllocSize
	}

	// Truncate和fsync保证文件大小能刷进文件里面
	// https://github.com/boltdb/bolt/issues/284
	if !db.NoGrowSync && !db.readOnly {
		if runtime.GOOS != "windows" {
			if err := db.file.Truncate(int64(sz)); err != nil {
				return fmt.Errorf("file resize error: %s", err)
			}
		}
		if err := db.file.Sync(); err != nil {
			return fmt.Errorf("file sync error: %s", err)
		}
	}

	db.filesz = sz
	return nil
}

// IsReadOnly 返回数据库是否为只读
func (db *DB) IsReadOnly() bool {
	return db.readOnly
}

// Options 代表打开数据库是可设置的选项
type Options struct {
	// Timeout 是等文件锁的时间，如果设置为0，则会无限等下去。
	// 这个选项只有在Dawin和Linux下有效
	Timeout time.Duration

	// 在内存映射文件之前设置DB.NoGrowSync
	NoGrowSync bool

	// 以只读打开数据库。用flock(..., LOCK_SH |LOCK_NB)来获取一个共享锁
	ReadOnly bool

	// 在内存映射之前设置DB.MmapFlags
	MmapFlags int

	// InitialMmapSize 数据库的初始映射大小
	// 如果这个大小足够大，则读事务不会阻塞写事务（详情可以看DB.Begin）
	//
	// 如果小于等于0，则初始映射大小为0
	// 如果小于数据库文件大小，则它没有任何作用了
	InitialMmapSize int
}

// DefaultOptions 默认选项，当nil传入open()的时候用这个默认选项
// 这里timeout为0，将会无限等待一个文件锁的释放
var DefaultOptions = &Options{
	Timeout:    0,
	NoGrowSync: false,
}

// Stats 数据库的统计数据
type Stats struct {
	// Freelist stats
	FreePageN     int // total number of free pages on the freelist
	PendingPageN  int // total number of pending pages on the freelist
	FreeAlloc     int // total bytes allocated in free pages
	FreelistInuse int // total bytes used by the freelist

	// Transaction stats
	TxN     int // total number of started read transactions
	OpenTxN int // number of currently open read transactions

	TxStats TxStats // global, ongoing stats.
}

// Sub calculates and returns the difference between two sets of database stats.
// This is useful when obtaining stats at two different points and time and
// you need the performance counters that occurred within that time span.
// (统计相关就不翻译了)
func (s *Stats) Sub(other *Stats) Stats {
	if other == nil {
		return *s
	}
	var diff Stats
	diff.FreePageN = s.FreePageN
	diff.PendingPageN = s.PendingPageN
	diff.FreeAlloc = s.FreeAlloc
	diff.FreelistInuse = s.FreelistInuse
	diff.TxN = s.TxN - other.TxN
	diff.TxStats = s.TxStats.Sub(&other.TxStats)
	return diff
}

func (s *Stats) add(other *Stats) {
	s.TxStats.add(&other.TxStats)
}

// Info 内部使用的一个数据结构
type Info struct {
	Data     uintptr
	PageSize int
}

type meta struct {
	magic    uint32
	version  uint32
	pageSize uint32
	flags    uint32
	root     bucket
	freelist pgid
	pgid     pgid
	txid     txid
	checksum uint64
}

// validate 检查meta页的魔数，版本，还有哈希摘要是否正确来判断meta是否有效
func (m *meta) validate() error {
	if m.magic != magic {
		return ErrInvalid
	} else if m.version != version {
		return ErrVersionMismatch
	} else if m.checksum != 0 && m.checksum != m.sum64() {
		return ErrChecksum
	}
	return nil
}

// copy 拷贝一个meta到另外一个
func (m *meta) copy(dest *meta) {
	*dest = *m
}

// write 写一个meta到页里面去
func (m *meta) write(p *page) {
	if m.root.root >= m.pgid {
		panic(fmt.Sprintf("root bucket pgid (%d) above high water mark (%d)", m.root.root, m.pgid))
	} else if m.freelist >= m.pgid {
		panic(fmt.Sprintf("freelist pgid (%d) above high water mark (%d)", m.freelist, m.pgid))
	}

	// 页id是0和1，由当前事务id确定
	p.id = pgid(m.txid % 2)
	p.flags |= metaPageFlag

	// 计算哈希摘要
	m.checksum = m.sum64()

	m.copy(p.meta())
}

// 为meta生成哈希摘要
func (m *meta) sum64() uint64 {
	var h = fnv.New64a()
	_, _ = h.Write((*[unsafe.Offsetof(meta{}.checksum)]byte)(unsafe.Pointer(m))[:])
	return h.Sum64()
}

// _assert 如果条件为false，则会产生给定格式信息的panic
func _assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assertion failed: "+msg, v...))
	}
}

func warn(v ...interface{})              { fmt.Fprintln(os.Stderr, v...) }
func warnf(msg string, v ...interface{}) { fmt.Fprintf(os.Stderr, msg+"\n", v...) }

func printstack() {
	stack := strings.Join(strings.Split(string(debug.Stack()), "\n")[2:], "\n")
	fmt.Fprintln(os.Stderr, stack)
}