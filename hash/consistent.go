// Package hash implements consistent hashing (Karger et al., 1997) with
// virtual nodes, so adding or removing an upstream remaps only ~1/N of keys.
package hash

import (
	"crypto/md5"
	"encoding/binary"
	"sort"
	"sync"
)

const vnodes = 160

type Ring struct {
	mu      sync.RWMutex
	points  []uint32
	owners  map[uint32]string
	members map[string]struct{}
}

func New() *Ring {
	return &Ring{owners: map[uint32]string{}, members: map[string]struct{}{}}
}

func hashKey(s string) uint32 {
	sum := md5.Sum([]byte(s))
	return binary.BigEndian.Uint32(sum[:4])
}

func (r *Ring) Add(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.members[node]; ok {
		return
	}
	r.members[node] = struct{}{}
	for i := 0; i < vnodes; i++ {
		h := hashKey(node + "#" + itoa(i))
		r.points = append(r.points, h)
		r.owners[h] = node
	}
	sort.Slice(r.points, func(i, j int) bool { return r.points[i] < r.points[j] })
}

func (r *Ring) Remove(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.members[node]; !ok {
		return
	}
	delete(r.members, node)
	kept := r.points[:0]
	for _, p := range r.points {
		if r.owners[p] == node {
			delete(r.owners, p)
		} else {
			kept = append(kept, p)
		}
	}
	r.points = kept
}

// Get returns the upstream owning key, or "" when the ring is empty.
func (r *Ring) Get(key string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.points) == 0 {
		return ""
	}
	h := hashKey(key)
	i := sort.Search(len(r.points), func(i int) bool { return r.points[i] >= h })
	if i == len(r.points) {
		i = 0
	}
	return r.owners[r.points[i]]
}

func (r *Ring) Members() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.members))
	for m := range r.members {
		out = append(out, m)
	}
	sort.Strings(out)
	return out
}

func itoa(i int) string {
	var buf [8]byte
	n := 0
	v := i
	if v == 0 {
		return "0"
	}
	for v > 0 {
		buf[n] = byte('0' + v%10)
		v /= 10
		n++
	}
	b := make([]byte, n)
	for j := 0; j < n; j++ {
		b[j] = buf[n-1-j]
	}
	return string(b)
}
