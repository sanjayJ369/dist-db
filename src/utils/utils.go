package utils

import (
	"container/heap"
	"strings"
)

type StringStoreHeap []string

func (s StringStoreHeap) Len() int {
	return len(s)
}

func (s StringStoreHeap) Less(i, j int) bool {
	res := strings.Compare(s[i], s[j])
	return res == -1
}

func (s *StringStoreHeap) Swap(i, j int) {
	(*s)[i], (*s)[j] = (*s)[j], (*s)[i]
}

func (s *StringStoreHeap) Push(x any) {
	*s = append(*s, x.(string))
}

func (s *StringStoreHeap) Pop() any {
	res := (*s)[len(*s)-1]
	*s = (*s)[0 : len(*s)-1]
	return res
}

type StringHeap struct {
	StringStoreHeap
}

func NewStringHeap() *StringHeap {
	h := &StringHeap{}
	heap.Init(h)
	return h
}

func (s *StringHeap) PushString(x string) {
	heap.Push(&s.StringStoreHeap, x)
}

func (s *StringHeap) PopString() string {
	return heap.Pop(&s.StringStoreHeap).(string)
}
