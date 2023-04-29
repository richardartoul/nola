package virtual

import (
	"fmt"
	"sync"

	"github.com/google/btree"
	"github.com/richardartoul/nola/virtual/types"
)

type actorResourceTracker struct {
	sync.RWMutex

	_actors               map[types.NamespacedActorID]*actorResources
	_topActorsByMem       *btree.BTreeG[actorByMem]
	_currMemoryUsageBytes int
}

func newActorResourceTracker() *actorResourceTracker {
	return &actorResourceTracker{
		_actors: make(map[types.NamespacedActorID]*actorResources),
		// _actors: btree.NewG(16, func(a, b *actorResources) bool {
		// 	return a.id.Less(b.id) < 0
		// }),
		_topActorsByMem: btree.NewG(16, func(a, b actorByMem) bool {
			if a.memoryBytes != b.memoryBytes {
				return a.memoryBytes > b.memoryBytes
			}
			return a.id.Less(b.id) > 0
		}),
		_currMemoryUsageBytes: 0,
	}
}

func (m *actorResourceTracker) track(
	id types.NamespacedActorID,
	memUsageBytes int,
) {
	m.Lock()
	defer m.Unlock()

	// First, find the actor (if it exists) and updates its current memory usage.
	curr, ok := m._actors[id]
	if !ok {
		curr = &actorResources{}
		m._actors[id] = curr
	}
	prevMemUsage := curr.memoryBytes
	curr.memoryBytes = memUsageBytes
	delta := memUsageBytes - prevMemUsage
	m._currMemoryUsageBytes += delta

	// Next, update the index of actors by memory usage.
	m._topActorsByMem.Delete(actorByMem{id: id, memoryBytes: prevMemUsage})
	if memUsageBytes == 0 {
		delete(m._actors, id)
	} else {
		m._topActorsByMem.ReplaceOrInsert(actorByMem{id: id, memoryBytes: memUsageBytes})
	}
}

func (m *actorResourceTracker) memUsageBytes() int {
	m.Lock()
	defer m.Unlock()
	return m._currMemoryUsageBytes
}

func (m *actorResourceTracker) topNByMemory(n int) []actorByMem {
	// Pre-alloc before acquiring lock to avoid tail latencies.
	topN := make([]actorByMem, 0, n)

	m.Lock()
	defer m.Unlock()
	m._topActorsByMem.Ascend(func(x actorByMem) bool {
		if len(topN) >= n {
			return false
		}

		topN = append(topN, x)
		return true
	})

	fmt.Println(len(topN), n)
	return topN[:n:n]
}

type actorResources struct {
	memoryBytes int
}

type actorByMem struct {
	id          types.NamespacedActorID
	memoryBytes int
}
