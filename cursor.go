package bolt

import (
	"bytes"
	"fmt"
	"sort"
)

// 游标用来有序的遍历一个桶里面所有的key/value，
// 游标看到桶类型的页时返回的value是nil
// 游标从一个事务中获得，而且只在这个事务中有效
// 同理游标返回的key/value也是只在当前事务中有效
// 改变游标返回的key/value可能会使游标无效
// 并返回不符合期望的key/value。在改变数据之后，你必须重置你的游标
type Cursor struct {
	bucket *Bucket // 创建游标的桶
	stack  []elemRef // 游标经历过的元素都要放到这个栈中
}

// 返回创建游标的桶
func (c *Cursor) Bucket() *Bucket {
	return c.bucket
}

// 移动游标到桶里的第一个元素并返回key和value
// 如果桶是空的，返回nil
// 这返回的key和value只在当前事务有效
func (c *Cursor) First() (key []byte, value []byte) {
	_assert(c.bucket.tx.db != nil, "tx closed")
	c.stack = c.stack[:0]
	p, n := c.bucket.pageNode(c.bucket.root)
	c.stack = append(c.stack, elemRef{page: p, node: n, index: 0})
	c.first()

	// 如果碰到空的页就移到下一个值，详细见下面issue
	// https://github.com/boltdb/bolt/issues/450
	if c.stack[len(c.stack)-1].count() == 0 {
		c.next()
	}

	k, v, flags := c.keyValue()
	if (flags & uint32(bucketLeafFlag)) != 0 { //是桶页，就不返回value
		return k, nil
	}
	return k, v

}

// 移动游标到桶里的最后一个元素并返回key和value
// 如果桶是空的，返回nil
// 这返回的key和value只在当前事务有效
func (c *Cursor) Last() (key []byte, value []byte) {
	_assert(c.bucket.tx.db != nil, "tx closed")
	c.stack = c.stack[:0]
	p, n := c.bucket.pageNode(c.bucket.root)
	ref := elemRef{page: p, node: n}
	ref.index = ref.count() - 1
	c.stack = append(c.stack, ref)
	c.last()
	k, v, flags := c.keyValue()
	if (flags & uint32(bucketLeafFlag)) != 0 {
		return k, nil
	}
	return k, v
}

// 移动游标到下一个元素并返回key和value
// 如果游标已经在桶的底部了，则返回nil
// 这返回的key和value只在当前事务有效
func (c *Cursor) Next() (key []byte, value []byte) {
	_assert(c.bucket.tx.db != nil, "tx closed")
	k, v, flags := c.next()
	if (flags & uint32(bucketLeafFlag)) != 0 {
		return k, nil
	}
	return k, v
}

// 移动游标到前一个元素并返回key和value
// 如果游标已经在桶的开始，则返回nil
// 这返回的key和value只在当前事务有效
func (c *Cursor) Prev() (key []byte, value []byte) {
	_assert(c.bucket.tx.db != nil, "tx closed")

	// 沿着栈往前遍历，直到遇到元素不是在开头的，这时把索引减一，退出循环
	// 或者直到栈都遍历完
	for i := len(c.stack) - 1; i >= 0; i-- {
		elem := &c.stack[i]
		if elem.index > 0 { // 不是在开头
			elem.index--
			break
		}
		c.stack = c.stack[:i] // 弹出最后一个，继续循环
	}

	// 如果栈遍历完之后就是空的了，所以返回
	// 这种情况意味着游标在桶的开始
	if len(c.stack) == 0 {
		return nil, nil
	}

	// 向下移动栈去找当前分支下的最后一个叶子的最后一个元素，看不懂直接看last()方法吧
	c.last()
	k, v, flags := c.keyValue()
	if (flags & uint32(bucketLeafFlag)) != 0 {
		return k, nil
	}
	return k, v
}

// 移动游标到给定的键所在的位置并返回它
// 如果这个键不存在，那返回它下一个键，如果也没有就返回nil
// 这返回的键值只在当前事务有效
func (c *Cursor) Seek(seek []byte) (key []byte, value []byte) {
	k, v, flags := c.seek(seek)

	// 如果游标在一个页的最后一个元素那里停住了，就移动到相邻的下一个页
	if ref := &c.stack[len(c.stack)-1]; ref.index >= ref.count() {
		k, v, flags = c.next()
	}

	if k == nil {
		return nil, nil
	} else if (flags & uint32(bucketLeafFlag)) != 0 {
		return k, nil
	}
	return k, v
}

// 从桶中移除当前游标所在的键值
// 如果删除的是个桶或者事务只读的时候会删除失败
func (c *Cursor) Delete() error {
	if c.bucket.tx.db == nil {
		return ErrTxClosed
	} else if !c.bucket.Writable() { // 只读事务，返回错误
		return ErrTxNotWritable
	}

	key, _, flags := c.keyValue()
	// 如果当前值是个桶，就返回错误
	if (flags & bucketLeafFlag) != 0 {
		return ErrIncompatibleValue
	}
	c.node().del(key)

	return nil
}

// 移动游标到给定的键所在的位置并返回它
// 如果这个键不存在，那返回它下一个键
func (c *Cursor) seek(seek []byte) (key []byte, value []byte, flags uint32) {
	_assert(c.bucket.tx.db != nil, "tx closed")

	// 从根页/节点开始遍历，直到找到正确的位置
	c.stack = c.stack[:0]
	c.search(seek, c.bucket.root)
	ref := &c.stack[len(c.stack)-1]

	// 如果游标指向页/节点的结尾，则返回nil
	if ref.index >= ref.count() {
		return nil, nil, 0
	}

	// 如果桶页，则会返回nil值
	return c.keyValue()
}

// 移动游标到栈顶上的页的第一个叶子元素
func (c *Cursor) first() {
	for {
		// 如果是个叶子页，退出循环
		var ref = &c.stack[len(c.stack)-1]
		if ref.isLeaf() {
			break
		}

		// 不断将指向第一个元素的页压入栈中
		var pgid pgid
		if ref.node != nil {
			pgid = ref.node.inodes[ref.index].pgid
		} else {
			pgid = ref.page.branchPageElement(uint16(ref.index)).pgid
		}
		p, n := c.bucket.pageNode(pgid)
		c.stack = append(c.stack, elemRef{page: p, node: n, index: 0})
	}
}

// 移动游标到栈顶上的页的最后一个叶子元素
func (c *Cursor) last() {
	for {
		// 如果是个叶子页，退出循环
		ref := &c.stack[len(c.stack)-1]
		if ref.isLeaf() {
			break
		}

		// 不断将指向最后一个元素的页压入栈中
		var pgid pgid
		if ref.node != nil {
			pgid = ref.node.inodes[ref.index].pgid
		} else {
			pgid = ref.page.branchPageElement(uint16(ref.index)).pgid
		}
		p, n := c.bucket.pageNode(pgid)

		var nextRef = elemRef{page: p, node: n}
		nextRef.index = nextRef.count() - 1
		c.stack = append(c.stack, nextRef)
	}
}

// next moves to the next leaf element and returns the key and value.
// If the cursor is at the last leaf element then it stays there and returns nil.
func (c *Cursor) next() (key []byte, value []byte, flags uint32) {
	for {
		// Attempt to move over one element until we're successful.
		// Move up the stack as we hit the end of each page in our stack.
		var i int
		for i = len(c.stack) - 1; i >= 0; i-- {
			elem := &c.stack[i]
			if elem.index < elem.count()-1 {
				elem.index++
				break
			}
		}

		// If we've hit the root page then stop and return. This will leave the
		// cursor on the last element of the last page.
		if i == -1 {
			return nil, nil, 0
		}

		// Otherwise start from where we left off in the stack and find the
		// first element of the first leaf page.
		c.stack = c.stack[:i+1]
		c.first()

		// If this is an empty page then restart and move back up the stack.
		// https://github.com/boltdb/bolt/issues/450
		if c.stack[len(c.stack)-1].count() == 0 {
			continue
		}

		return c.keyValue()
	}
}

// 递归的在给定的页或节点里面进行二分查找某个键
func (c *Cursor) search(key []byte, pgid pgid) {
	p, n := c.bucket.pageNode(pgid)
	if p != nil && (p.flags&(branchPageFlag|leafPageFlag)) == 0 {
		panic(fmt.Sprintf("invalid page type: %d: %x", p.id, p.flags))
	}
	e := elemRef{page: p, node: n}
	c.stack = append(c.stack, e)

	// 如果我们正在叶子页/节点，直接在叶子上面查找
	if e.isLeaf() {
		c.nsearch(key)
		return
	}

	//如果节点已经初始化，节点上查找
	if n != nil {
		c.searchNode(key, n)
		return
	}
	// 页上查
	c.searchPage(key, p)
}

// 节点搜索
func (c *Cursor) searchNode(key []byte, n *node) {
	var exact bool
	index := sort.Search(len(n.inodes), func(i int) bool {
		ret := bytes.Compare(n.inodes[i].key, key)
		if ret == 0 {
			exact = true
		}
		return ret != -1
	})
	if !exact && index > 0 {
		index--
	}
	c.stack[len(c.stack)-1].index = index

	// 递归查找下一个页
	c.search(key, n.inodes[index].pgid)
}

// 页中搜索
func (c *Cursor) searchPage(key []byte, p *page) {
	// Binary search for the correct range.
	inodes := p.branchPageElements()

	var exact bool
	index := sort.Search(int(p.count), func(i int) bool {
		ret := bytes.Compare(inodes[i].key(), key)
		if ret == 0 {
			exact = true
		}
		return ret != -1
	})
	if !exact && index > 0 {
		index--
	}
	c.stack[len(c.stack)-1].index = index

	// 递归查找下一个页
	c.search(key, inodes[index].pgid)
}

// 在栈顶的叶子节点中查找key
func (c *Cursor) nsearch(key []byte) {
	e := &c.stack[len(c.stack)-1]
	p, n := e.page, e.node

	// 如果节点有就查找inode
	if n != nil {
		index := sort.Search(len(n.inodes), func(i int) bool {
			return bytes.Compare(n.inodes[i].key, key) != -1
		})
		e.index = index
		return
	}

	// 如果页有（肯定有），则查找叶子元素
	inodes := p.leafPageElements()
	index := sort.Search(int(p.count), func(i int) bool {
		return bytes.Compare(inodes[i].key(), key) != -1
	})
	e.index = index
}

// 返回当前叶子的键和值
func (c *Cursor) keyValue() ([]byte, []byte, uint32) {
	ref := &c.stack[len(c.stack)-1]
	if ref.count() == 0 || ref.index >= ref.count() {
		return nil, nil, 0
	}

	// 从节点拿值
	if ref.node != nil {
		inode := &ref.node.inodes[ref.index]
		return inode.key, inode.value, inode.flags
	}

	// 或者从页拿值
	elem := ref.page.leafPageElement(uint16(ref.index))
	return elem.key(), elem.value(), elem.flags
}

// 返回当前游标所在的节点
func (c *Cursor) node() *node {
	_assert(len(c.stack) > 0, "accessing a node with a zero-length cursor stack")

	// 如果栈顶是叶子节点，就返回它
	if ref := &c.stack[len(c.stack)-1]; ref.node != nil && ref.isLeaf() {
		return ref.node
	}

	// 从根开始遍历下去，一直到底
	var n = c.stack[0].node
	if n == nil {
		n = c.bucket.node(c.stack[0].page.id, nil)
	}
	for _, ref := range c.stack[:len(c.stack)-1] {
		_assert(!n.isLeaf, "expected branch node")
		n = n.childAt(int(ref.index))
	}
	_assert(n.isLeaf, "expected leaf node")
	return n
}

// 代表在页或节点的里面的一个元素的引用
type elemRef struct {
	page  *page
	node  *node
	index int
}

// 返回页或节点是否为叶子
func (r *elemRef) isLeaf() bool {
	if r.node != nil {
		return r.node.isLeaf
	}
	return (r.page.flags & leafPageFlag) != 0
}

// 返回inode的个数或者页的元素个数
func (r *elemRef) count() int {
	if r.node != nil {
		return len(r.node.inodes)
	}
	return int(r.page.count)
}
