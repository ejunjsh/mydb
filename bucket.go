package mydb

import (
	"bytes"
	"fmt"
	"unsafe"
)

const (
	// MaxKeySize 一个键的最大字节数
	MaxKeySize = 32768

	// MaxValueSize 一个值的最大字节数
	MaxValueSize = (1 << 31) - 2
)

const (
	maxUint = ^uint(0)
	minUint = 0
	maxInt  = int(^uint(0) >> 1)
	minInt  = -maxInt - 1
)

const bucketHeaderSize = int(unsafe.Sizeof(bucket{}))

const (
	minFillPercent = 0.1
	maxFillPercent = 1.0
)

// DefaultFillPercent 默认拆分一个页的百分比阀值，
// 这个可以通过Bucket.FillPercent来修改
const DefaultFillPercent = 0.5

// Bucket 代表一个在数据库对键值对集合。
type Bucket struct {
	*bucket
	tx       *Tx                // 关联的事务
	buckets  map[string]*Bucket // 子桶缓存
	page     *page              // 内联页引用
	rootNode *node              // 根页的具体化（materialized）节点
	nodes    map[pgid]*node     // 节点缓存

	// 这个值不会保存进磁盘，所以必须每个事务都要设置
	// 默认情况下页里面装到50%的数据时就会拆分，但是如果你大部分的写都是后面插入（append-only）的话，
	// 增加这个值大小非常有用
	FillPercent float64
}

// 代表一个存在在文件的桶（其实就是序列化到文件的格式）
// 存的是一个桶的”值“。如果桶足够小，那么根页则会内联存在这个“值”里，放在桶的头后面
// 内联桶的root是0
type bucket struct {
	root     pgid   // 桶的根页id
	sequence uint64 // 单调递增，只用在方法NextSequence()
}

// newBucket 返回一个关联事务的新桶
func newBucket(tx *Tx) Bucket {
	var b = Bucket{tx: tx, FillPercent: DefaultFillPercent}
	if tx.writable {
		b.buckets = make(map[string]*Bucket)
		b.nodes = make(map[pgid]*node)
	}
	return b
}

// Tx 返回桶的事务
func (b *Bucket) Tx() *Tx {
	return b.tx
}

// Root 返回桶的根页
func (b *Bucket) Root() pgid {
	return b.root
}

// Writable 返回是否这个桶可写
func (b *Bucket) Writable() bool {
	return b.tx.writable
}

// Cursor 创建一个游标关联这个桶
// 游标只在事务打开的时候有效
// 事务关闭就别用游标了
func (b *Bucket) Cursor() *Cursor {
	// 更新事务统计数据
	b.tx.stats.CursorCount++

	// 分配和返回一个游标
	return &Cursor{
		bucket: b,
		stack:  make([]elemRef, 0),
	}
}

// Bucket 通过名字搜索桶里面的子桶
// 如果桶不存在，返回nil
// 桶只在事务生命周期内有效
func (b *Bucket) Bucket(name []byte) *Bucket {
	if b.buckets != nil {
		if child := b.buckets[string(name)]; child != nil {
			return child
		}
	}

	// 移动游标去查找键
	c := b.Cursor()
	k, v, flags := c.seek(name)

	// 如果键不存在或者不是个桶就返回nil
	if !bytes.Equal(name, k) || (flags&bucketLeafFlag) == 0 {
		return nil
	}

	// 否则创建一个桶并缓存它
	var child = b.openBucket(v)
	if b.buckets != nil {
		b.buckets[string(name)] = child
	}

	return child
}

// 通过这个value 重新解释一个子桶并返回
func (b *Bucket) openBucket(value []byte) *Bucket {
	var child = newBucket(b.tx)

	// 如果没有对齐，则需要把value拷贝到一个对齐过的字节数组
	unaligned := brokenUnaligned && uintptr(unsafe.Pointer(&value[0]))&3 != 0

	if unaligned {
		value = cloneBytes(value)
	}


	// 如果是个写事务，则需要拷贝一个桶出来
	// 如果是只读事务，则直接引用mmap里面的桶
	if b.tx.writable && !unaligned {
		child.bucket = &bucket{}
		*child.bucket = *(*bucket)(unsafe.Pointer(&value[0]))
	} else {
		child.bucket = (*bucket)(unsafe.Pointer(&value[0]))
	}

	// 如果桶是一个内联的，就保存一个内联页的引用
	if child.root == 0 {
		child.page = (*page)(unsafe.Pointer(&value[bucketHeaderSize]))
	}

	return &child
}

// CreateBucket 给定一个键创建一个桶，并返回这个新桶
// 如果键已经存在，名字是空或者太长，则返回错误
// 桶实例只在事务的生命周期里面有效
func (b *Bucket) CreateBucket(key []byte) (*Bucket, error) {
	if b.tx.db == nil {
		return nil, ErrTxClosed
	} else if !b.tx.writable {
		return nil, ErrTxNotWritable
	} else if len(key) == 0 {
		return nil, ErrBucketNameRequired
	}

	// 移动游标到正确到位置
	c := b.Cursor()
	k, _, flags := c.seek(key)

	// 如果已经存在这个键，则返回错误
	if bytes.Equal(key, k) {
		if (flags & bucketLeafFlag) != 0 {
			return nil, ErrBucketExists
		}
		return nil, ErrIncompatibleValue
	}

	// 创建一个空的内联桶
	var bucket = Bucket{
		bucket:      &bucket{},
		rootNode:    &node{isLeaf: true},
		FillPercent: DefaultFillPercent,
	}
	var value = bucket.write()

	// 插入到节点
	key = cloneBytes(key)
	c.node().put(key, key, value, 0, bucketLeafFlag)

	// 因为子桶不允许放在内联桶里面，所以设置为空是为了让他变回一个正常的非内联桶
	b.page = nil

	return b.Bucket(key), nil
}

// CreateBucketIfNotExists 给定一个键创建一个桶，并返回这个新桶,如果桶已经存在，则返回该桶
// 如果名字是空或者太长，则返回错误
// 桶实例只在事务的生命周期里面有效
func (b *Bucket) CreateBucketIfNotExists(key []byte) (*Bucket, error) {
	child, err := b.CreateBucket(key)
	if err == ErrBucketExists {
		return b.Bucket(key), nil
	} else if err != nil {
		return nil, err
	}
	return child, nil
}

// DeleteBucket 删除给定的键对应的桶
// 如果桶不存在或者给定的键对应的不是个桶，就返回错误
func (b *Bucket) DeleteBucket(key []byte) error {
	if b.tx.db == nil {
		return ErrTxClosed
	} else if !b.Writable() {
		return ErrTxNotWritable
	}

	// 移动游标到正确到位置
	c := b.Cursor()
	k, _, flags := c.seek(key)

	// 如果桶不存在或者不是一个桶，则返回错误
	if !bytes.Equal(key, k) {
		return ErrBucketNotFound
	} else if (flags & bucketLeafFlag) == 0 {
		return ErrIncompatibleValue
	}

	// 递归删除所有子桶
	child := b.Bucket(key)
	err := child.ForEach(func(k, v []byte) error {
		if v == nil {
			if err := child.DeleteBucket(k); err != nil {
				return fmt.Errorf("delete bucket: %s", err)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	// 清除缓存
	delete(b.buckets, string(key))

	// 释放所有桶关联到页到空闲列表
	child.nodes = nil
	child.rootNode = nil
	child.free()

	// 删除对应键的节点
	c.node().del(key)

	return nil
}

// Get 获取桶里面键对应的值
// 如果键不存在，或者键是个桶，则返回nil
// 返回值只在事务生命周期下有效
func (b *Bucket) Get(key []byte) []byte {
	k, v, flags := b.Cursor().seek(key)

	// 如果是个桶，返回nil
	if (flags & bucketLeafFlag) != 0 {
		return nil
	}

	// 如果找到的节点的键和传进来的不等，则返回nil
	if !bytes.Equal(key, k) {
		return nil
	}
	return v
}

// Put 设置值到桶的键里面
// 如果键已经存在，则之前的值会被覆盖
// 传进来的value必须要在整个事务中有效
// 如果桶所在的事务是只读事务，键是空，键太大，或者键的值太大，则返回错误
func (b *Bucket) Put(key []byte, value []byte) error {
	if b.tx.db == nil {
		return ErrTxClosed
	} else if !b.Writable() {
		return ErrTxNotWritable
	} else if len(key) == 0 {
		return ErrKeyRequired
	} else if len(key) > MaxKeySize {
		return ErrKeyTooLarge
	} else if int64(len(value)) > MaxValueSize {
		return ErrValueTooLarge
	}

	// 移动游标到正确位置
	c := b.Cursor()
	k, _, flags := c.seek(key)

	// 如果键对应的是一个桶，则返回错误
	if bytes.Equal(key, k) && (flags&bucketLeafFlag) != 0 {
		return ErrIncompatibleValue
	}

	// 插入节点
	key = cloneBytes(key)
	c.node().put(key, key, value, 0, 0)

	return nil
}

// Delete 在桶里面删除一个键
// 如果键不存在，什么都没有发生并返回错误
// 如果事务是只读事务，也会报错
func (b *Bucket) Delete(key []byte) error {
	if b.tx.db == nil {
		return ErrTxClosed
	} else if !b.Writable() {
		return ErrTxNotWritable
	}

	// 移动游标到正确位置
	c := b.Cursor()
	_, _, flags := c.seek(key)

	// 如果键对应到是桶，则报错
	if (flags & bucketLeafFlag) != 0 {
		return ErrIncompatibleValue
	}

	// 删除对应的节点
	c.node().del(key)

	return nil
}

// Sequence 返回桶当前的自增值（还没有增加它）
func (b *Bucket) Sequence() uint64 { return b.bucket.sequence }

// SetSequence 更新桶的自增值
func (b *Bucket) SetSequence(v uint64) error {
	if b.tx.db == nil {
		return ErrTxClosed
	} else if !b.Writable() {
		return ErrTxNotWritable
	}

	// 实例化桶的根节点，这样可以在提交事务的时候保存这个桶
	if b.rootNode == nil {
		_ = b.node(b.root, nil)
	}

	// 设置
	b.bucket.sequence = v
	return nil
}

// NextSequence 为这个桶返回下个自增的值
func (b *Bucket) NextSequence() (uint64, error) {
	if b.tx.db == nil {
		return 0, ErrTxClosed
	} else if !b.Writable() {
		return 0, ErrTxNotWritable
	}

	// 实例化桶的根节点，这样可以在提交事务的时候保存这个桶
	if b.rootNode == nil {
		_ = b.node(b.root, nil)
	}

	// 自增并返回
	b.bucket.sequence++
	return b.bucket.sequence, nil
}

// ForEach 给桶里面的所有键值执行一个函数fn
// 如果函数fn返回错误，则循环结束，并返回这个错误给调用者
// 在函数fn里面不要修改这个桶，否则会导致未定义的行为
func (b *Bucket) ForEach(fn func(k, v []byte) error) error {
	if b.tx.db == nil {
		return ErrTxClosed
	}
	c := b.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if err := fn(k, v); err != nil {
			return err
		}
	}
	return nil
}

// Stat 返回桶的统计数据
// （统计相关就不翻译了）
func (b *Bucket) Stats() BucketStats {
	var s, subStats BucketStats
	pageSize := b.tx.db.pageSize
	s.BucketN += 1
	if b.root == 0 {
		s.InlineBucketN += 1
	}
	b.forEachPage(func(p *page, depth int) {
		if (p.flags & leafPageFlag) != 0 {
			s.KeyN += int(p.count)

			// used totals the used bytes for the page
			used := pageHeaderSize

			if p.count != 0 {
				// If page has any elements, add all element headers.
				used += leafPageElementSize * int(p.count-1)

				// Add all element key, value sizes.
				// The computation takes advantage of the fact that the position
				// of the last element's key/value equals to the total of the sizes
				// of all previous elements' keys and values.
				// It also includes the last element's header.
				lastElement := p.leafPageElement(p.count - 1)
				used += int(lastElement.pos + lastElement.ksize + lastElement.vsize)
			}

			if b.root == 0 {
				// For inlined bucket just update the inline stats
				s.InlineBucketInuse += used
			} else {
				// For non-inlined bucket update all the leaf stats
				s.LeafPageN++
				s.LeafInuse += used
				s.LeafOverflowN += int(p.overflow)

				// Collect stats from sub-buckets.
				// Do that by iterating over all element headers
				// looking for the ones with the bucketLeafFlag.
				for i := uint16(0); i < p.count; i++ {
					e := p.leafPageElement(i)
					if (e.flags & bucketLeafFlag) != 0 {
						// For any bucket element, open the element value
						// and recursively call Stats on the contained bucket.
						subStats.Add(b.openBucket(e.value()).Stats())
					}
				}
			}
		} else if (p.flags & branchPageFlag) != 0 {
			s.BranchPageN++
			lastElement := p.branchPageElement(p.count - 1)

			// used totals the used bytes for the page
			// Add header and all element headers.
			used := pageHeaderSize + (branchPageElementSize * int(p.count-1))

			// Add size of all keys and values.
			// Again, use the fact that last element's position equals to
			// the total of key, value sizes of all previous elements.
			used += int(lastElement.pos + lastElement.ksize)
			s.BranchInuse += used
			s.BranchOverflowN += int(p.overflow)
		}

		// Keep track of maximum page depth.
		if depth+1 > s.Depth {
			s.Depth = (depth + 1)
		}
	})

	// Alloc stats can be computed from page counts and pageSize.
	s.BranchAlloc = (s.BranchPageN + s.BranchOverflowN) * pageSize
	s.LeafAlloc = (s.LeafPageN + s.LeafOverflowN) * pageSize

	// Add the max depth of sub-buckets to get total nested depth.
	s.Depth += subStats.Depth
	// Add the stats for all sub-buckets
	s.Add(subStats)
	return s
}

// forEachPage 遍历桶里面的每个页，包括内联页
func (b *Bucket) forEachPage(fn func(*page, int)) {
	// 如果有内联页，则直接使用它
	if b.page != nil {
		fn(b.page, 0)
		return
	}

	// 否则遍历所有页
	b.tx.forEachPage(b.root, 0, fn)
}

// forEachPageNode 遍历桶里面的每个页或者节点
// 这个也包含内联页
func (b *Bucket) forEachPageNode(fn func(*page, *node, int)) {
	// 如果有内联页，则直接使用它
	if b.page != nil {
		fn(b.page, nil, 0)
		return
	}
	b._forEachPageNode(b.root, 0, fn)
}

func (b *Bucket) _forEachPageNode(pgid pgid, depth int, fn func(*page, *node, int)) {
	var p, n = b.pageNode(pgid)

	// 执行函数
	fn(p, n, depth)

	// 递归遍历孩子
	if p != nil {
		if (p.flags & branchPageFlag) != 0 {
			for i := 0; i < int(p.count); i++ {
				elem := p.branchPageElement(uint16(i))
				b._forEachPageNode(elem.pgid, depth+1, fn)
			}
		}
	} else {
		if !n.isLeaf {
			for _, inode := range n.inodes {
				b._forEachPageNode(inode.pgid, depth+1, fn)
			}
		}
	}
}

// spill 写桶里面所有节点到脏页
func (b *Bucket) spill() error {
	// 拆分所有子桶先
	for name, child := range b.buckets {
		// 如果子桶足够小，并且没有子子桶，那么就把它写入父桶页里面。
		// 否则像其他一般桶那样拆分，并把这个子桶到桶头写入父桶页里面
		var value []byte
		if child.inlineable() {
			child.free()
			value = child.write()
		} else {
			if err := child.spill(); err != nil {
				return err
			}

			// 更新子桶头
			value = make([]byte, unsafe.Sizeof(bucket{}))
			var bucket = (*bucket)(unsafe.Pointer(&value[0]))
			*bucket = *child.bucket
		}

		// 跳过写桶，因为没有实例化到根节点
		if child.rootNode == nil {
			continue
		}

		// 更新父节点
		var c = b.Cursor()
		k, _, flags := c.seek([]byte(name))
		if !bytes.Equal([]byte(name), k) {
			panic(fmt.Sprintf("misplaced bucket header: %x -> %x", []byte(name), k))
		}
		if flags&bucketLeafFlag == 0 {
			panic(fmt.Sprintf("unexpected bucket header flag: %x", flags))
		}
		c.node().put([]byte(name), []byte(name), value, 0, bucketLeafFlag)
	}

	// 如果没有根节点，就忽略它
	if b.rootNode == nil {
		return nil
	}

	// 拆分节点
	if err := b.rootNode.spill(); err != nil {
		return err
	}
	b.rootNode = b.rootNode.root()

	// 更新这个桶的根节点
	if b.rootNode.pgid >= b.tx.meta.pgid {
		panic(fmt.Sprintf("pgid (%d) above high water mark (%d)", b.rootNode.pgid, b.tx.meta.pgid))
	}
	b.root = b.rootNode.pgid

	return nil
}

// inlineable 如果桶足够小的能够写入内联页，同时不包含子桶，则返回true，否则返回false
func (b *Bucket) inlineable() bool {
	var n = b.rootNode

	// 桶必须是一个叶子节点
	if n == nil || !n.isLeaf {
		return false
	}

	// 桶包含子桶或者超过内联桶大小的阀值，则不是内联的
	var size = pageHeaderSize
	for _, inode := range n.inodes {
		size += leafPageElementSize + len(inode.key) + len(inode.value)

		if inode.flags&bucketLeafFlag != 0 {
			return false
		} else if size > b.maxInlineBucketSize() {
			return false
		}
	}

	return true
}

// 返回内联桶大小阀值
func (b *Bucket) maxInlineBucketSize() int {
	return b.tx.db.pageSize / 4
}

// write 分配和写一个桶到一个字节切片
func (b *Bucket) write() []byte {
	// 分配合适大小
	var n = b.rootNode
	var value = make([]byte, bucketHeaderSize+n.size())

	// 写桶头
	var bucket = (*bucket)(unsafe.Pointer(&value[0]))
	*bucket = *b.bucket

	// 转换字节切片到一个假的页，然后把根节点写入这个页
	var p = (*page)(unsafe.Pointer(&value[bucketHeaderSize]))
	n.write(p)

	return value
}

// rebalance 尝试平衡所有节点
func (b *Bucket) rebalance() {
	for _, n := range b.nodes {
		n.rebalance()
	}
	for _, child := range b.buckets {
		child.rebalance()
	}
}

// node根据一个页id和它对应的父亲来创建一个节点
func (b *Bucket) node(pgid pgid, parent *node) *node {
	_assert(b.nodes != nil, "nodes map expected")

	// 如果缓存里面有，则直接返回
	if n := b.nodes[pgid]; n != nil {
		return n
	}

	// 否则创建一个节点并缓存它
	n := &node{bucket: b, parent: parent}
	if parent == nil {
		b.rootNode = n
	} else {
		parent.children = append(parent.children, n)
	}

	// 如果是内联桶，则用这个内联页
	var p = b.page
	if p == nil {
		p = b.tx.page(pgid)
	}

	// 读取页到一个节点，并缓存它
	n.read(p)
	b.nodes[pgid] = n

	// 更新统计数据
	b.tx.stats.NodeCount++

	return n
}

// free 递归释放桶里面的所有页
func (b *Bucket) free() {
	if b.root == 0 {
		return
	}

	var tx = b.tx
	b.forEachPageNode(func(p *page, n *node, _ int) {
		if p != nil {
			tx.db.freelist.free(tx.meta.txid, p)
		} else {
			n.free()
		}
	})
	b.root = 0
}

// dereference 移除所有到mmap的引用（转换到go堆内存到引用）
func (b *Bucket) dereference() {
	if b.rootNode != nil {
		b.rootNode.root().dereference()
	}

	for _, child := range b.buckets {
		child.dereference()
	}
}

// pageNode 返回内存里的节点，如果存在的化话
// 否则返回页
func (b *Bucket) pageNode(id pgid) (*page, *node) {
	// 内联桶有个假的页嵌入在桶的值里面，所以处理有点不同
	// 如果根节点存在则返回，假的页存在则返回
	if b.root == 0 {
		if id != 0 {
			panic(fmt.Sprintf("inline bucket non-zero page access(2): %d != 0", id))
		}
		if b.rootNode != nil {
			return nil, b.rootNode
		}
		return b.page, nil
	}

	// 对于非内联桶，检查节点缓存
	if b.nodes != nil {
		if n := b.nodes[id]; n != nil {
			return nil, n
		}
	}

	// 如果节点没有实例化，则从事务里面找页
	return b.tx.page(id), nil
}

// BucketStats records statistics about resources used by a bucket.
// (统计相关不翻译了)
type BucketStats struct {
	// Page count statistics.
	BranchPageN     int // number of logical branch pages
	BranchOverflowN int // number of physical branch overflow pages
	LeafPageN       int // number of logical leaf pages
	LeafOverflowN   int // number of physical leaf overflow pages

	// Tree statistics.
	KeyN  int // number of keys/value pairs
	Depth int // number of levels in B+tree

	// Page size utilization.
	BranchAlloc int // bytes allocated for physical branch pages
	BranchInuse int // bytes actually used for branch data
	LeafAlloc   int // bytes allocated for physical leaf pages
	LeafInuse   int // bytes actually used for leaf data

	// Bucket statistics
	BucketN           int // total number of buckets including the top bucket
	InlineBucketN     int // total number on inlined buckets
	InlineBucketInuse int // bytes used for inlined buckets (also accounted for in LeafInuse)
}

func (s *BucketStats) Add(other BucketStats) {
	s.BranchPageN += other.BranchPageN
	s.BranchOverflowN += other.BranchOverflowN
	s.LeafPageN += other.LeafPageN
	s.LeafOverflowN += other.LeafOverflowN
	s.KeyN += other.KeyN
	if s.Depth < other.Depth {
		s.Depth = other.Depth
	}
	s.BranchAlloc += other.BranchAlloc
	s.BranchInuse += other.BranchInuse
	s.LeafAlloc += other.LeafAlloc
	s.LeafInuse += other.LeafInuse

	s.BucketN += other.BucketN
	s.InlineBucketN += other.InlineBucketN
	s.InlineBucketInuse += other.InlineBucketInuse
}

// cloneBytes 返回给定切片的拷贝
func cloneBytes(v []byte) []byte {
	var clone = make([]byte, len(v))
	copy(clone, v)
	return clone
}
