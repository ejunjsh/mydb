package bolt

import (
	"bytes"
	"fmt"
	"sort"
	"unsafe"
)

// node(节点)表示一个存在内存中的对页（page）反序列化后的数据结构
type node struct {
	bucket     *Bucket // 关联的桶
	isLeaf     bool // 是否叶子
	unbalanced bool // 是否平衡
	spilled    bool // 是否拆分过
	key        []byte // 节点关联的键
	pgid       pgid // 页id
	parent     *node // 父节点
	children   nodes // 用来跟踪拆分时需要拆分的儿子
	inodes     inodes
}

// 返回当前节点的顶层的根
func (n *node) root() *node {
	if n.parent == nil {
		return n
	}
	return n.parent.root()
}

// 返回当前节点应该有inode的最少数量
func (n *node) minKeys() int {
	if n.isLeaf {
		return 1
	}
	return 2
}

// 返回当前节点序列化后的大小
func (n *node) size() int {
	sz, elsz := pageHeaderSize, n.pageElementSize()
	for i := 0; i < len(n.inodes); i++ {
		item := &n.inodes[i]
		sz += elsz + len(item.key) + len(item.value)
	}
	return sz
}

// 如果当前节点的大小小于给定的大小将返回true，否则就是false。
// 这个函数主要用来避免计算一个大的节点的大小，如果我们只是想知道这个节点是否能够放进某个页中
func (n *node) sizeLessThan(v int) bool {
	sz, elsz := pageHeaderSize, n.pageElementSize()
	for i := 0; i < len(n.inodes); i++ {
		item := &n.inodes[i]
		sz += elsz + len(item.key) + len(item.value)
		if sz >= v {
			return false
		}
	}
	return true
}

// 根据节点类型返回相应的页元素大小
func (n *node) pageElementSize() int {
	if n.isLeaf {
		return leafPageElementSize
	}
	return branchPageElementSize
}

// 根据index返回子节点
func (n *node) childAt(index int) *node {
	if n.isLeaf {
		panic(fmt.Sprintf("invalid childAt(%d) on a leaf node", index))
	}
	return n.bucket.node(n.inodes[index].pgid, n)
}

// 根据子node返回索引
func (n *node) childIndex(child *node) int {
	index := sort.Search(len(n.inodes), func(i int) bool { return bytes.Compare(n.inodes[i].key, child.key) != -1 })
	return index
}

// 返回子节点的数量
func (n *node) numChildren() int {
	return len(n.inodes)
}

// 返回同父的下一个兄弟节点
func (n *node) nextSibling() *node {
	if n.parent == nil {
		return nil
	}
	index := n.parent.childIndex(n)
	if index >= n.parent.numChildren()-1 {
		return nil
	}
	return n.parent.childAt(index + 1)
}

// 返回同父的前一个兄弟节点
func (n *node) prevSibling() *node {
	if n.parent == nil {
		return nil
	}
	index := n.parent.childIndex(n)
	if index == 0 {
		return nil
	}
	return n.parent.childAt(index - 1)
}

// 插入一个key/value
func (n *node) put(oldKey, newKey, value []byte, pgid pgid, flags uint32) {
	if pgid >= n.bucket.tx.meta.pgid {
		panic(fmt.Sprintf("pgid (%d) above high water mark (%d)", pgid, n.bucket.tx.meta.pgid))
	} else if len(oldKey) <= 0 {
		panic("put: zero-length old key")
	} else if len(newKey) <= 0 {
		panic("put: zero-length new key")
	}

	// 查找要插入的位置
	index := sort.Search(len(n.inodes), func(i int) bool { return bytes.Compare(n.inodes[i].key, oldKey) != -1 })

	// 如果oldKey找不到，就先扩大下数组，把要插入的位置后面的元素后移
	exact := (len(n.inodes) > 0 && index < len(n.inodes) && bytes.Equal(n.inodes[index].key, oldKey))
	if !exact {
		n.inodes = append(n.inodes, inode{})
		copy(n.inodes[index+1:], n.inodes[index:])
	}

	inode := &n.inodes[index]
	inode.flags = flags
	inode.key = newKey
	inode.value = value
	inode.pgid = pgid
	_assert(len(inode.key) > 0, "put: zero-length inode key")
}

// 从节点中删除一个key
func (n *node) del(key []byte) {
	// 找这个key的索引
	index := sort.Search(len(n.inodes), func(i int) bool { return bytes.Compare(n.inodes[i].key, key) != -1 })

	// 找不到就返回
	if index >= len(n.inodes) || !bytes.Equal(n.inodes[index].key, key) {
		return
	}

	// 从节点中删除这个inode
	n.inodes = append(n.inodes[:index], n.inodes[index+1:]...)

	// 标记这个节点需要重新平衡
	n.unbalanced = true
}

// 从一个页初始化一个节点
func (n *node) read(p *page) {
	n.pgid = p.id
	n.isLeaf = ((p.flags & leafPageFlag) != 0)
	n.inodes = make(inodes, int(p.count))

	for i := 0; i < int(p.count); i++ {
		inode := &n.inodes[i]
		if n.isLeaf {
			elem := p.leafPageElement(uint16(i))
			inode.flags = elem.flags
			inode.key = elem.key()
			inode.value = elem.value()
		} else {
			elem := p.branchPageElement(uint16(i))
			inode.pgid = elem.pgid
			inode.key = elem.key()
		}
		_assert(len(inode.key) > 0, "read: zero-length inode key")
	}

	// 保存第一个key，这样当spill的时候就方便查找到这个节点
	if len(n.inodes) > 0 {
		n.key = n.inodes[0].key
		_assert(len(n.key) > 0, "read: zero-length node key")
	} else {
		n.key = nil
	}
}

// 从节点写到一个或多个页
func (n *node) write(p *page) {
	// 初始化页
	if n.isLeaf {
		p.flags |= leafPageFlag
	} else {
		p.flags |= branchPageFlag
	}

	if len(n.inodes) >= 0xFFFF {
		panic(fmt.Sprintf("inode overflow: %d (pgid=%d)", len(n.inodes), p.id))
	}
	p.count = uint16(len(n.inodes))

	// 如果没有什么可写，就返回
	if p.count == 0 {
		return
	}

	// 循环每一个项，写到页中
	b := (*[maxAllocSize]byte)(unsafe.Pointer(&p.ptr))[n.pageElementSize()*len(n.inodes):]
	for i, item := range n.inodes {
		_assert(len(item.key) > 0, "write: zero-length inode key")

		// 写入页元素
		if n.isLeaf {
			elem := p.leafPageElement(uint16(i))
			elem.pos = uint32(uintptr(unsafe.Pointer(&b[0])) - uintptr(unsafe.Pointer(elem)))
			elem.flags = item.flags
			elem.ksize = uint32(len(item.key))
			elem.vsize = uint32(len(item.value))
		} else {
			elem := p.branchPageElement(uint16(i))
			elem.pos = uint32(uintptr(unsafe.Pointer(&b[0])) - uintptr(unsafe.Pointer(elem)))
			elem.ksize = uint32(len(item.key))
			elem.pgid = item.pgid
			_assert(elem.pgid != p.id, "write: circular dependency occurred")
		}

		// 如果key+value的长度大于最大分配大小，我们需要重新分配这个字节数组指针
		//
		// 详情请看: https://github.com/boltdb/bolt/pull/335
		klen, vlen := len(item.key), len(item.value)
		if len(b) < klen+vlen {
			b = (*[maxAllocSize]byte)(unsafe.Pointer(&b[0]))[:]
		}

		// 为这个元素写数据到页的结尾
		copy(b[0:], item.key)
		b = b[klen:]
		copy(b[0:], item.value)
		b = b[vlen:]
	}

}

// 拆分一个节点为多个更小的节点，这个方法只会被spill()函数调用
func (n *node) split(pageSize int) []*node {
	var nodes []*node

	node := n
	for {
		// 拆成两个
		a, b := node.splitTwo(pageSize)
		nodes = append(nodes, a)

		// 如果不能拆分就退出循环
		if b == nil {
			break
		}

		// 下个迭代走起
		node = b
	}

	return nodes
}

// 拆分一个节点为两个更小的节点，这个方法只会被split()函数调用
func (n *node) splitTwo(pageSize int) (*node, *node) {
	// 如果节点里面的inode数量不满足至少两个页应该有的数量
	// 或者这个节点大小小于页大小
	// 就返回，不拆分了
	if len(n.inodes) <= (minKeysPerPage*2) || n.sizeLessThan(pageSize) {
		return n, nil
	}

	// 拆分前先确定拆分的阀值
	var fillPercent = n.bucket.FillPercent
	if fillPercent < minFillPercent {
		fillPercent = minFillPercent
	} else if fillPercent > maxFillPercent {
		fillPercent = maxFillPercent
	}
	threshold := int(float64(pageSize) * fillPercent)

	// 确定拆分的位置
	splitIndex, _ := n.splitIndex(threshold)

	// 拆分成两个节点
	// 如果没有父节点，就创建一个
	if n.parent == nil {
		n.parent = &node{bucket: n.bucket, children: []*node{n}}
	}

	// 创建一个新的节点并把它加到父节点
	next := &node{bucket: n.bucket, isLeaf: n.isLeaf, parent: n.parent}
	n.parent.children = append(n.parent.children, next)

	// 拆开inodes分到两个节点上
	next.inodes = n.inodes[splitIndex:]
	n.inodes = n.inodes[:splitIndex]

	// 更新统计信息
	n.bucket.tx.stats.Split++

	return n, next
}

// 根据阀值去找拆分的位置，并返回位置的索引和跟阀值相关的大小
// 这个方法只会被splitTwo()函数调用
func (n *node) splitIndex(threshold int) (index, sz int) {
	sz = pageHeaderSize // 初始化总大小

	// 循环直到留下最少数量的key能够满足给第二个页时
	for i := 0; i < len(n.inodes)-minKeysPerPage; i++ {
		index = i
		inode := n.inodes[i]
		elsize := n.pageElementSize() + len(inode.key) + len(inode.value)

		// 如果有足够的key，同时节点加起来的总大小大于阀值的话就返回当前索引
		if i >= minKeysPerPage && sz+elsize > threshold {
			break
		}

		// 累加总大小
		sz += elsize
	}

	return
}

// 写节点到脏页同时拆分节点
// 如果脏页没法分配就返回错误
func (n *node) spill() error {
	var tx = n.bucket.tx
	if n.spilled {
		return nil
	}

	// 拆分子节点先。在拆分过程中n.children的长度会增加（具体可以看看splitTwo函数）
	// 所以这里不能用range来做循环，需要每次循环时检查一下n.children的长度
	sort.Sort(n.children)
	for i := 0; i < len(n.children); i++ {
		if err := n.children[i].spill(); err != nil {
			return err
		}
	}

	// 我们不再需要这个属性因为它只是用来跟踪拆分过程的
	n.children = nil

	// 拆分节点到合适大小，第一个节点一直是n
	var nodes = n.split(tx.db.pageSize)
	for _, node := range nodes {
		
		// 如果节点已经脏了，释放节点的页到空闲列表
		if node.pgid > 0 {
			tx.db.freelist.free(tx.meta.txid, tx.page(node.pgid))
			node.pgid = 0
		}

		// 为节点分配连续的空间
		p, err := tx.allocate((node.size() / tx.db.pageSize) + 1)
		if err != nil {
			return err
		}

		// 写节点到脏页中
		if p.id >= tx.meta.pgid {
			panic(fmt.Sprintf("pgid (%d) above high water mark (%d)", p.id, tx.meta.pgid))
		}
		node.pgid = p.id
		node.write(p)
		node.spilled = true

		// 插入到父节点的inode上
		if node.parent != nil {
			var key = node.key
			if key == nil {
				key = node.inodes[0].key
			}

			node.parent.put(key, node.inodes[0].key, nil, node.pgid, 0)
			node.key = node.inodes[0].key
			_assert(len(node.key) > 0, "spill: zero-length node key")
		}

		// 更新统计数据
		tx.stats.Spill++
	}


	// 如果根节点拆分成一个新的根节点，我们还要继续拆分它，
	// 因为拆分这个函数同时也做了写入脏页的操作，所以这里虽然是拆分一个新的根节点，其实就是把这个根节点写入到脏页中
	// 清掉children是保证不会重复拆分
	if n.parent != nil && n.parent.pgid == 0 {
		n.children = nil
		return n.parent.spill()
	}

	return nil
}

// 如果节点大小低于阈值或者没有足够的键，重新平衡用来合并当前节点和它的兄弟节点
func (n *node) rebalance() {
	if !n.unbalanced {
		return
	}
	n.unbalanced = false

	// 更新统计数据
	n.bucket.tx.stats.Rebalance++

	// 如果在阈值(25%)之上或者有足够的键就不重新平衡了
	var threshold = n.bucket.tx.db.pageSize / 4
	if n.size() > threshold && len(n.inodes) > n.minKeys() {
		return
	}

	// 根节点需要特殊处理
	if n.parent == nil {
		// 如果根节点是分支节点而它只有一个儿子，就把他们合并吧
		if !n.isLeaf && len(n.inodes) == 1 {
			// 合并儿子
			child := n.bucket.node(n.inodes[0].pgid, n)
			n.isLeaf = child.isLeaf
			n.inodes = child.inodes[:]
			n.children = child.children

			// 帮儿子换个父亲
			for _, inode := range n.inodes {
				if child, ok := n.bucket.nodes[inode.pgid]; ok {
					child.parent = n
				}
			}

			// 移除儿子
			child.parent = nil
			delete(n.bucket.nodes, child.pgid)
			child.free()
		}

		return
	}

	// 如果节点没有键就删除它
	if n.numChildren() == 0 {
		n.parent.del(n.key)
		n.parent.removeChild(n)
		delete(n.bucket.nodes, n.pgid)
		n.free()
		n.parent.rebalance()
		return
	}

	_assert(n.parent.numChildren() > 1, "parent must have at least 2 children")

	// 如果当前节点的索引是0，那目标节点就是当前节点的右边那个，否则就是左边那个
	var target *node // 目标节点
	var useNextSibling = (n.parent.childIndex(n) == 0)
	if useNextSibling {
		target = n.nextSibling()
	} else {
		target = n.prevSibling()
	}

	// 如果当前节点和目标节点都太小就合并他们吧
	if useNextSibling {
		// 给孩子换父亲
		for _, inode := range target.inodes {
			if child, ok := n.bucket.nodes[inode.pgid]; ok {
				child.parent.removeChild(child)
				child.parent = n
				child.parent.children = append(child.parent.children, child)
			}
		}

		// 从目标节点拷贝inode，然后删掉目标节点
		n.inodes = append(n.inodes, target.inodes...)
		n.parent.del(target.key)
		n.parent.removeChild(target)
		delete(n.bucket.nodes, target.pgid)
		target.free()
	} else {
		// 给孩子换父亲
		for _, inode := range n.inodes {
			if child, ok := n.bucket.nodes[inode.pgid]; ok {
				child.parent.removeChild(child)
				child.parent = target
				child.parent.children = append(child.parent.children, child)
			}
		}

		// 从当前节点拷贝inode，然后删掉当前节点
		target.inodes = append(target.inodes, n.inodes...)
		n.parent.del(n.key)
		n.parent.removeChild(n)
		delete(n.bucket.nodes, n.pgid)
		n.free()
	}

	// 无论你是删除当前节点还是目标节点，父节点还是要重新平衡下
	n.parent.rebalance()
}

// 在当前节点的孩子列表中移除节点
// 这个不会影响inodes
func (n *node) removeChild(target *node) {
	for i, child := range n.children {
		if child == target {
			n.children = append(n.children[:i], n.children[i+1:]...)
			return
		}
	}
}

// 解引用用来把节点里面的inode关联的键值对引用换到堆内存上
// 这个发生在mmap的时候，可以使inode不会指向到一个失效的数据上
func (n *node) dereference() {
	if n.key != nil {
		key := make([]byte, len(n.key))
		copy(key, n.key)
		n.key = key
		_assert(n.pgid == 0 || len(n.key) > 0, "dereference: zero-length node key on existing node")
	}

	for i := range n.inodes {
		inode := &n.inodes[i]

		key := make([]byte, len(inode.key))
		copy(key, inode.key)
		inode.key = key
		_assert(len(inode.key) > 0, "dereference: zero-length inode key")

		value := make([]byte, len(inode.value))
		copy(value, inode.value)
		inode.value = value
	}

	// 递归对孩子解引用
	for _, child := range n.children {
		child.dereference()
	}

	// 更新统计数据
	n.bucket.tx.stats.NodeDeref++
}

// 释放节点底下的页到空闲列表
func (n *node) free() {
	if n.pgid != 0 {
		n.bucket.tx.db.freelist.free(n.bucket.tx.meta.txid, n.bucket.tx.page(n.pgid))
		n.pgid = 0
	}
}


type nodes []*node

func (s nodes) Len() int           { return len(s) }
func (s nodes) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s nodes) Less(i, j int) bool { return bytes.Compare(s[i].inodes[0].key, s[j].inodes[0].key) == -1 }

// inode代表节点里面的一个内部节点
// 它用来指向页的元素
// 也可以指向一个还没有加到页里面的元素
type inode struct {
	flags uint32
	pgid  pgid
	key   []byte
	value []byte
}

type inodes []inode
