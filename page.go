package bbolt

import (
	"fmt"
	"os"
	"reflect"
	"sort"
	"unsafe"
)

// page 结构体的大小
const pageHeaderSize = unsafe.Sizeof(page{})

const minKeysPerPage = 2

const branchPageElementSize = unsafe.Sizeof(branchPageElement{})
const leafPageElementSize = unsafe.Sizeof(leafPageElement{})

const (
	// 分支节点
	branchPageFlag = 0x01
	// 叶子节点
	leafPageFlag = 0x02
	// meta 页
	metaPageFlag = 0x04
	// freelist 页，存放无数据的空 page
	freelistPageFlag = 0x10
)

const (
	bucketLeafFlag = 0x01
)

type pgid uint64

//
type page struct {
	// page id
	id pgid
	// 此页中保存的具体数据类型，即上面四个 Flag
	flags uint16
	// 具体数据类型中的计数
	count uint16
	// 是否有后序页，如果有，overflow 表示后续页的数量
	overflow uint32
}

// typ returns a human readable page type string used for debugging.
// 返回 page 的类型
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

// meta returns a pointer to the metadata section of the page.
func (p *page) meta() *meta {
	return (*meta)(unsafe.Pointer(uintptr(unsafe.Pointer(p)) + unsafe.Sizeof(*p)))
}

// leafPageElement retrieves the leaf node by index
// 根据 index 检索 叶子节点
func (p *page) leafPageElement(index uint16) *leafPageElement {
	off := uintptr(index) * unsafe.Sizeof(leafPageElement{})
	return (*leafPageElement)(unsafe.Pointer(uintptr(unsafe.Pointer(p)) + unsafe.Sizeof(*p) + off))
}

// leafPageElements retrieves a list of leaf nodes.
func (p *page) leafPageElements() []leafPageElement {
	if p.count == 0 {
		return nil
	}
	return *(*[]leafPageElement)(unsafe.Pointer(&reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(p)) + unsafe.Sizeof(*p),
		Len:  int(p.count),
		Cap:  int(p.count),
	}))
}

// branchPageElement retrieves the branch node by index
func (p *page) branchPageElement(index uint16) *branchPageElement {
	off := uintptr(index) * unsafe.Sizeof(branchPageElement{})
	return (*branchPageElement)(unsafe.Pointer(uintptr(unsafe.Pointer(p)) + unsafe.Sizeof(*p) + off))
}

// branchPageElements retrieves a list of branch nodes.
func (p *page) branchPageElements() []branchPageElement {
	if p.count == 0 {
		return nil
	}
	return *(*[]branchPageElement)(unsafe.Pointer(&reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(p)) + unsafe.Sizeof(*p),
		Len:  int(p.count),
		Cap:  int(p.count),
	}))
}

// dump writes n bytes of the page to STDERR as hex output.
func (p *page) hexdump(n int) {
	buf := *(*[]byte)(unsafe.Pointer(&reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(p)),
		Len:  n,
		Cap:  n,
	}))
	fmt.Fprintf(os.Stderr, "%x\n", buf)
}

type pages []*page

func (s pages) Len() int           { return len(s) }
func (s pages) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s pages) Less(i, j int) bool { return s[i].id < s[j].id }

// branchPageElement represents a node on a branch page.
// branch node 的 value 是子节点的 page id
type branchPageElement struct {
	// Element 对应的键值对存储位置相对于当前 Element 的偏移量
	pos uint32
	// Element 对应 key 的大小，以 byte 为单位
	ksize uint32
	// Element 指向的子节点所在 page id
	pgid pgid
}

// key returns a byte slice of the node key.
func (n *branchPageElement) key() []byte {
	return *(*[]byte)(unsafe.Pointer(&reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(n)) + uintptr(n.pos),
		Len:  int(n.ksize),
		Cap:  int(n.ksize),
	}))
}

// leafPageElement represents a node on a leaf page.
// 磁盘上记录具体 key-value 的索引
// &leafPageElement + pos == &key
// &leafPageElement + pos + ksize == &value
type leafPageElement struct {
	// 标明当前 Element 是否代表一个 Bucket，如果是则其值为 1，如果不是则其值为 0;
	flags uint32
	// Element 对应的键值对存储位置相对于当前 Element 的偏移量
	pos uint32
	// Element 对应 key 的大小，以 byte 为单位
	ksize uint32
	// Element 对应 value 的大小，以 byte 为单位
	vsize uint32
}

// key returns a byte slice of the node key.
func (n *leafPageElement) key() []byte {
	return *(*[]byte)(unsafe.Pointer(&reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(n)) + uintptr(n.pos),
		Len:  int(n.ksize),
		Cap:  int(n.ksize),
	}))
}

// value returns a byte slice of the node value.
func (n *leafPageElement) value() []byte {
	return *(*[]byte)(unsafe.Pointer(&reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(n)) + uintptr(n.pos) + uintptr(n.ksize),
		Len:  int(n.vsize),
		Cap:  int(n.vsize),
	}))
}

// PageInfo represents human readable information about a page.
type PageInfo struct {
	ID            int
	Type          string
	Count         int
	OverflowCount int
}

type pgids []pgid

func (s pgids) Len() int           { return len(s) }
func (s pgids) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s pgids) Less(i, j int) bool { return s[i] < s[j] }

// merge returns the sorted union of a and b.
func (a pgids) merge(b pgids) pgids {
	// Return the opposite slice if one is nil.
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

// mergepgids copies the sorted union of a and b into dst.
// If dst is too small, it panics.
// 合并两个 page
func mergepgids(dst, a, b pgids) {
	if len(dst) < len(a)+len(b) {
		panic(fmt.Errorf("mergepgids bad len %d < %d + %d", len(dst), len(a), len(b)))
	}
	// Copy in the opposite slice if one is nil.
	if len(a) == 0 {
		copy(dst, b)
		return
	}
	if len(b) == 0 {
		copy(dst, a)
		return
	}

	// Merged will hold all elements from both lists.
	merged := dst[:0]

	// Assign lead to the slice with a lower starting value, follow to the higher value.
	lead, follow := a, b
	if b[0] < a[0] {
		lead, follow = b, a
	}

	// Continue while there are elements in the lead.
	for len(lead) > 0 {
		// Merge largest prefix of lead that is ahead of follow[0].
		n := sort.Search(len(lead), func(i int) bool { return lead[i] > follow[0] })
		merged = append(merged, lead[:n]...)
		if n >= len(lead) {
			break
		}

		// Swap lead and follow.
		lead, follow = follow, lead[n:]
	}

	// Append what's left in follow.
	_ = append(merged, follow...)
}
