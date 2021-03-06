package mydb

import (
	"fmt"
	"os"
	"sort"
	"unsafe"
)

// 页头大小
const pageHeaderSize = int(unsafe.Offsetof(((*page)(nil)).ptr))

// 每个页最小key数
const minKeysPerPage = 2

// 分支页元素数量
const branchPageElementSize = int(unsafe.Sizeof(branchPageElement{}))
// 叶子页元素数量
const leafPageElementSize = int(unsafe.Sizeof(leafPageElement{}))

// 页类型
const (
	branchPageFlag   = 0x01 // 分支页
	leafPageFlag     = 0x02 // 叶子页
	metaPageFlag     = 0x04 // 元数据页
	freelistPageFlag = 0x10 // 空闲链表页
)

const (
	bucketLeafFlag = 0x01 // 桶页
)

type pgid uint64

// 直接映射到磁盘的页
type page struct {
	id       pgid // 页id
	flags    uint16 // 页类型
	count    uint16 // 页里面包含多少元素
	overflow uint32 // 页是否跨页
	ptr      uintptr // 指向第一个元素的指针
}

// 返回页类型
func (p *page) typ() string {
	if (p.flags & branchPageFlag) != 0 {
		return "branch"
	} else if (p.flags & leafPageFlag) != 0 {
		return "leaf"
	} else if (p.flags & metaPageFlag) != 0 {
		return "meta"
	} else if (p.flags & freelistPageFlag) != 0 {
		return "freelist"
	}
	return fmt.Sprintf("unknown<%02x>", p.flags)
}

// 返回指向在页中的元数据的指针
func (p *page) meta() *meta {
	return (*meta)(unsafe.Pointer(&p.ptr))
}

// 通过索引返回叶子节点
func (p *page) leafPageElement(index uint16) *leafPageElement {
	n := &((*[0x7FFFFFF]leafPageElement)(unsafe.Pointer(&p.ptr)))[index]
	return n
}

// 返回一个列表的叶子节点
func (p *page) leafPageElements() []leafPageElement {
	if p.count == 0 {
		return nil
	}
	return ((*[0x7FFFFFF]leafPageElement)(unsafe.Pointer(&p.ptr)))[:]
}

// 通过索引返回分支节点
func (p *page) branchPageElement(index uint16) *branchPageElement {
	return &((*[0x7FFFFFF]branchPageElement)(unsafe.Pointer(&p.ptr)))[index]
}

// 返回一个列表的分支节点
func (p *page) branchPageElements() []branchPageElement {
	if p.count == 0 {
		return nil
	}
	return ((*[0x7FFFFFF]branchPageElement)(unsafe.Pointer(&p.ptr)))[:]
}

// 输出页的n个字节到STDERR上
func (p *page) hexdump(n int) {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(p))[:n]
	fmt.Fprintf(os.Stderr, "%x\n", buf)
}

type pages []*page

func (s pages) Len() int           { return len(s) }
func (s pages) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s pages) Less(i, j int) bool { return s[i].id < s[j].id }

// 代表一个在分支页的节点
type branchPageElement struct {
	pos   uint32
	ksize uint32
	pgid  pgid
}

// 返回节点key的字节切片
func (n *branchPageElement) key() []byte {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(n))
	return (*[maxAllocSize]byte)(unsafe.Pointer(&buf[n.pos]))[:n.ksize]
}

// 代表一个在叶子页的节点
type leafPageElement struct {
	flags uint32
	pos   uint32
	ksize uint32
	vsize uint32
}

// 返回节点key的字节切片
func (n *leafPageElement) key() []byte {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(n))
	return (*[maxAllocSize]byte)(unsafe.Pointer(&buf[n.pos]))[:n.ksize:n.ksize]
}

// 返回节点value的字节切片
func (n *leafPageElement) value() []byte {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(n))
	return (*[maxAllocSize]byte)(unsafe.Pointer(&buf[n.pos+n.ksize]))[:n.vsize:n.vsize]
}

// 一个用来更清晰描述页的页信息结构
type PageInfo struct {
	ID            int
	Type          string
	Count         int
	OverflowCount int
}

// 页id
type pgids []pgid

func (s pgids) Len() int           { return len(s) }
func (s pgids) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s pgids) Less(i, j int) bool { return s[i] < s[j] }

// 将a和b用排序联合（sorted union）进行合并
func (a pgids) merge(b pgids) pgids {
	// 如果其中是nil，就返回另外一个
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}
	merged := make(pgids, len(a)+len(b))
	mergepgids(merged, a, b)
	return merged
}

// 拷贝a和b的排序联合到dst中
// 如果dst大小太小，将会报错
func mergepgids(dst, a, b pgids) {
	if len(dst) < len(a)+len(b) {
		panic(fmt.Errorf("mergepgids bad len %d < %d + %d", len(dst), len(a), len(b)))
	}
	// 如果其中是nil，就拷贝另外一个
	if len(a) == 0 {
		copy(dst, b)
		return
	}
	if len(b) == 0 {
		copy(dst, a)
		return
	}

	// merged包含两个列表的所有元素
	merged := dst[:0]

	// 用更小的开始值赋值给lead，而follow持有更大的开始值
	lead, follow := a, b
	if b[0] < a[0] {
		lead, follow = b, a
	}

	// 只要lead有值就一直循环下去
	for len(lead) > 0 {
		// 把lead里面小于follow[0]的值放到merged里面
		n := sort.Search(len(lead), func(i int) bool { return lead[i] > follow[0] })
		merged = append(merged, lead[:n]...)
		if n >= len(lead) {
			break
		}

		// 交换lead和follow
		lead, follow = follow, lead[n:]
	}

	// 添加follow里面剩下的值到merged
	_ = append(merged, follow...)
}
