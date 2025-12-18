package main

import (
	"fmt"
	"hash/fnv"
	"slices"
	"sort"
)

type HashRing struct {
	ring map[uint32]string
	keys []uint32
}

func NewHashRing() *HashRing {
	return &HashRing{
		ring: make(map[uint32]string),
		keys: make([]uint32, 0),
	}
}

func HashKey(name string) uint32 {
	h := fnv.New32()
	h.Write([]byte(name))
	return h.Sum32()
}

func (h *HashRing) AddServer(name string, replica int) {

	for r := range replica {
		node := fmt.Sprintf("%s#%d", name, r)
		hash := HashKey(node)
		h.ring[hash] = node
		h.keys = append(h.keys, hash)
	}

	// sort.Slice(h.keys, func(i, j int) bool {
	// 	return h.keys[i] < h.keys[j]
	// })

	slices.Sort(h.keys)
}

func (h *HashRing) GetNode(key string) string {

	hash := HashKey(key)
	idx := sort.Search(len(h.keys), func(i int) bool {
		return h.keys[i] >= hash
	})

	if idx == len(h.keys) {
		idx = 0
	}

	return h.ring[h.keys[idx]]
}

func main() {
	ring := NewHashRing()
	ring.AddServer("server-1", 3)
	ring.AddServer("server-2", 3)
	ring.AddServer("server-3", 3)

	s1 := ring.GetNode("User-1")
	s2 := ring.GetNode("User-2")
	s3 := ring.GetNode("User-3")

	fmt.Println("User-1", "->", s1)
	fmt.Println("User-2", "->", s2)
	fmt.Println("User-3", "->", s3)
}
