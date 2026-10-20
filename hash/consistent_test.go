package hash

import (
	"fmt"
	"testing"
)

func TestRingBasicGet(t *testing.T) {
	r := New()
	if got := r.Get("anything"); got != "" {
		t.Fatal("empty ring must return empty")
	}
	r.Add("a")
	if got := r.Get("k1"); got != "a" {
		t.Fatalf("single-node ring must own every key, got %q", got)
	}
	// stable: same key always maps to same node
	first := r.Get("k1")
	for i := 0; i < 50; i++ {
		if r.Get("k1") != first {
			t.Fatal("mapping must be stable")
		}
	}
}

func TestRingDistribution(t *testing.T) {
	r := New()
	for _, n := range []string{"a", "b", "c"} {
		r.Add(n)
	}
	counts := map[string]int{}
	const keys = 30000
	for i := 0; i < keys; i++ {
		counts[r.Get(fmt.Sprintf("key-%d", i))]++
	}
	for node, c := range counts {
		share := float64(c) / keys
		if share < 0.25 || share > 0.42 {
			t.Fatalf("node %s holds %.1f%% of keys, want ~33%%", node, share*100)
		}
	}
}

func TestRingMinimalRemapping(t *testing.T) {
	r := New()
	nodes := []string{"a", "b", "c", "d"}
	for _, n := range nodes {
		r.Add(n)
	}
	before := map[string]string{}
	const keys = 20000
	for i := 0; i < keys; i++ {
		k := fmt.Sprintf("key-%d", i)
		before[k] = r.Get(k)
	}
	r.Add("e") // adding a 5th node should move ~1/5 of keys, no more
	moved := 0
	for k, v := range before {
		if r.Get(k) != v {
			moved++
		}
	}
	frac := float64(moved) / keys
	if frac > 0.35 {
		t.Fatalf("adding one node moved %.1f%% of keys; consistent hashing promises ~20%%", frac*100)
	}
	// removing a node must not strand keys
	r.Remove("e")
	for i := 0; i < 100; i++ {
		if r.Get(fmt.Sprintf("key-%d", i)) == "" {
			t.Fatal("key stranded after remove")
		}
	}
}
