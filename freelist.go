package mydb

import (
	"fmt"
	"sort"
	"unsafe"
)

// freelist(空闲列表)，表示一批可用来分配的空闲页
// 它也用来跟踪那些虽然已经释放但是还在被其他打开事务使用的页
type freelist struct {
	ids     []pgid          // 包含所有空闲可用的页id
	pending map[txid][]pgid // 包含空闲但是还被事务使用的挂起页id
	cache   map[pgid]bool   // 用来快速查找id是否在空闲列表里的缓存（如果页id可用或挂起都返回true）
}

// 返回一个空的并且初始化的空闲列表
func newFreelist() *freelist {
	return &freelist{
		pending: make(map[txid][]pgid),
		cache:   make(map[pgid]bool),
	}
}

// 返回空闲列表序列化后占用的页大小
func (f *freelist) size() int {
	n := f.count()
	if n >= 0xFFFF {
		// 第一个元素用来存储数量，详情看freelist.write
		n++
	}
	return pageHeaderSize + (int(unsafe.Sizeof(pgid(0))) * n)
}

// 返回页的总数（包括空闲和挂起的）
func (f *freelist) count() int {
	return f.free_count() + f.pending_count()
}

// 返回空闲的页数量
func (f *freelist) free_count() int {
	return len(f.ids)
}

// 返回挂起的页数量
func (f *freelist) pending_count() int {
	var count int
	for _, list := range f.pending {
		count += len(list)
	}
	return count
}

// 把空闲和挂起的页id合并到dst
// dst的最小长度为f.count
func (f *freelist) copyall(dst []pgid) {
	m := make(pgids, 0, f.pending_count())
	for _, list := range f.pending {
		m = append(m, list...)
	}
	sort.Sort(m)
	mergepgids(dst, f.ids, m)
}

// 返回n个连续页组成的块的开始页id
// 如果分配不到就返回0
func (f *freelist) allocate(n int) pgid {
	if len(f.ids) == 0 {
		return 0
	}

	var initial, previd pgid
	// 遍历
	for i, id := range f.ids {
		if id <= 1 {
			panic(fmt.Sprintf("invalid page allocation: %d", id))
		}

		// 如果不连续就重置下开始的页id
		if previd == 0 || id-previd != 1 {
			initial = id
		}

		// 如果找到一个连续页的块，然后移除它再返回它的开始id
		if (id-initial)+1 == pgid(n) {
			// 如果前面几个页就是连续的，那就直接slice操作就可以移除它
			// 如果不是，则就要把后面的填到前面去把那部分连续页占的空间填好
			if (i + 1) == n {
				f.ids = f.ids[i+1:]
			} else {
				copy(f.ids[i-n+1:], f.ids[i+1:])
				f.ids = f.ids[:len(f.ids)-n]
			}

			// 从缓存中移除
			for i := pgid(0); i < pgid(n); i++ {
				delete(f.cache, initial+i)
			}

			return initial
		}

		previd = id
	}
	return 0
}

// 释放页和它对应溢出的那部分页，这个不会直接释放到空闲，而是放到挂起那里，所以需要一个事务txid关联
// 如果页已经是空闲，会抛panic
func (f *freelist) free(txid txid, p *page) {
	if p.id <= 1 {
		panic(fmt.Sprintf("cannot free page 0 or 1: %d", p.id))
	}

	// 释放页和它对应溢出的那部分页
	var ids = f.pending[txid]
	for id := p.id; id <= p.id+pgid(p.overflow); id++ {
		// 验证这个页是否已经释放了
		if f.cache[id] {
			panic(fmt.Sprintf("page %d already freed", id))
		}

		// 加到挂起列表和缓存里面
		ids = append(ids, id)
		f.cache[id] = true
	}
	f.pending[txid] = ids
}

// 释放某个事务和它之前的挂起的空闲页
func (f *freelist) release(txid txid) {
	m := make(pgids, 0)
	for tid, ids := range f.pending {
		if tid <= txid {
			// 把挂起页回收到可用列表里面
			// 缓存不用删掉，因为本来就是空闲的
			m = append(m, ids...)
			delete(f.pending, tid)
		}
	}
	sort.Sort(m)
	f.ids = pgids(f.ids).merge(m)
}

// 从某个事务中移除挂起的页
func (f *freelist) rollback(txid txid) {
	// 移除缓存
	for _, id := range f.pending[txid] {
		delete(f.cache, id)
	}

	// 从挂起列表移除页
	delete(f.pending, txid)
}

// 返回页是否是在空闲列表里（在挂起列表也算是）
func (f *freelist) freed(pgid pgid) bool {
	return f.cache[pgid]
}

// 从一个空闲列表页里面初始化一个空闲列表
func (f *freelist) read(p *page) {
	// 如果page.count是大于64k，那空闲列表的数量是放在页的第一个元素里面
	idx, count := 0, int(p.count)
	if count == 0xFFFF {
		idx = 1
		count = int(((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[0]) // 第一个元素
	}

	// 从页里面拷贝到空闲列表里面
	if count == 0 {
		f.ids = nil
	} else {
		ids := ((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[idx:count]
		f.ids = make([]pgid, len(ids))
		copy(f.ids, ids)

		// 保证是排序的
		sort.Sort(pgids(f.ids))
	}

	// 重建缓存
	f.reindex()
}

// 把空闲列表写到空闲列表类型的页中，包括空闲和挂起的都写到磁盘里面，一般发生在程序错误退出
// 这样的话，表示所有挂起的页变成可用的了
func (f *freelist) write(p *page) error {

	// 设置类型为空闲列表
	p.flags |= freelistPageFlag

	// page.count只能小于或等于64k，如果大于，则放在页里面的第一个元素里面
	lenids := f.count()
	if lenids == 0 {
		p.count = uint16(lenids)
	} else if lenids < 0xFFFF {
		p.count = uint16(lenids)
		f.copyall(((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[:])
	} else {
		p.count = 0xFFFF
		((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[0] = pgid(lenids) // 设置到第一个元素
		f.copyall(((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[1:])
	}

	return nil
}

// 在页里面重新构造空闲列表并排除那些挂起的页
func (f *freelist) reload(p *page) {
	// 先从页里面读入
	f.read(p)

	// 为挂起的页创建缓存
	pcache := make(map[pgid]bool)
	for _, pendingIDs := range f.pending {
		for _, pendingID := range pendingIDs {
			pcache[pendingID] = true
		}
	}

	// 检查每个在空闲列表（可用）的页，筛选出不在挂起的页，放到一个新的可用列表
	var a []pgid
	for _, id := range f.ids {
		if !pcache[id] {
			a = append(a, id)
		}
	}
	f.ids = a // 替换掉原来那个

	// 一旦可用列表更新，重建缓存，这样缓存就包含最新的可用和挂起的页
	f.reindex()
}

// 把空闲和挂起的页都重建到缓存中
func (f *freelist) reindex() {
	f.cache = make(map[pgid]bool, len(f.ids))
	for _, id := range f.ids {
		f.cache[id] = true
	}
	for _, pendingIDs := range f.pending {
		for _, pendingID := range pendingIDs {
			f.cache[pendingID] = true
		}
	}
}
