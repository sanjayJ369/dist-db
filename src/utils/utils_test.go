package utils

import (
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStringHeap(t *testing.T) {
	t.Run("Basic Functionality", func(t *testing.T) {
		want := []string{"abcd", "aaa", "efgh", "ccc"}
		sheap := NewStringHeap()
		for _, s := range want {
			sheap.PushString(s)
		}
		sort.Slice(want, func(i, j int) bool {
			cmp := strings.Compare(want[i], want[j])
			return cmp == -1
		})
		got := []string{}
		for i := 0; i < len(want); i++ {
			got = append(got, sheap.PopString())
		}
		assert.Equal(t, want, got)
	})

	t.Run("Empty Heap", func(t *testing.T) {
		sheap := NewStringHeap()
		assert.Panics(t, func() {
			sheap.PopString()
		}, "Popping from an empty heap should panic")
	})

	t.Run("Single Element", func(t *testing.T) {
		sheap := NewStringHeap()
		sheap.PushString("single")
		assert.Equal(t, "single", sheap.PopString())
	})

	t.Run("Duplicate Elements", func(t *testing.T) {
		want := []string{"dup", "dup", "dup"}
		sheap := NewStringHeap()
		for _, s := range want {
			sheap.PushString(s)
		}
		got := []string{}
		for i := 0; i < len(want); i++ {
			got = append(got, sheap.PopString())
		}
		assert.Equal(t, want, got)
	})

	t.Run("Reverse Order Input", func(t *testing.T) {
		input := []string{"zzz", "yyy", "xxx", "www"}
		want := []string{"www", "xxx", "yyy", "zzz"}
		sheap := NewStringHeap()
		for _, s := range input {
			sheap.PushString(s)
		}
		got := []string{}
		for i := 0; i < len(input); i++ {
			got = append(got, sheap.PopString())
		}
		assert.Equal(t, want, got)
	})

	t.Run("Mixed Case Strings", func(t *testing.T) {
		input := []string{"Apple", "apple", "Banana", "banana"}
		want := []string{"Apple", "Banana", "apple", "banana"}
		sheap := NewStringHeap()
		for _, s := range input {
			sheap.PushString(s)
		}
		got := []string{}
		for i := 0; i < len(input); i++ {
			got = append(got, sheap.PopString())
		}
		assert.Equal(t, want, got)
	})
}
