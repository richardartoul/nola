package virtual

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/richardartoul/nola/virtual/types"
	"github.com/stretchr/testify/require"
)

func TestActorResourceTrackerConcurrentProperty(t *testing.T) {
	for i := 0; i < 100; i++ {
		testActorResourceTrackerConcurrentProperty(t)
	}
}

func testActorResourceTrackerConcurrentProperty(t *testing.T) {
	var (
		numActors         = 100
		numWorkers        = 10
		numItersPerWorker = 100

		actorIDs = []types.NamespacedActorID{}
	)

	for i := 0; i < numActors; i++ {
		id := types.NewNamespacedActorID(
			"ns", fmt.Sprintf("actor-%d", i), "module", types.IDTypeActor)
		actorIDs = append(actorIDs, id)
	}

	var (
		tracker   = newActorResourceTracker()
		reference = newTestActorResourceReferenceImpl()
		wg        sync.WaitGroup
	)
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().Unix()))
			for j := 0; j < numItersPerWorker; j++ {
				var (
					idIdx = rng.Intn(len(actorIDs))
					id    = actorIDs[idIdx]
					// Relatively low value so the special case of 0 (where the actors
					// usage is deleted/forgotten) gets exercised.
					memUsage = rng.Intn(100)
				)
				tracker.track(id, memUsage)
				reference.track(id, memUsage)
			}
		}()
	}

	wg.Wait()

	require.Equal(t, reference.memUsageBytes(), tracker.memUsageBytes())

	n := rand.Intn(len(actorIDs))
	require.Equal(t, reference.topNByMemory(n), tracker.topNByMemory(n))
}

// actorResourceReferenceImpl is a simplified reference implementation of
// actorResourceTracker that is used in property tests to verify the behavior
// of actorResourceTracker.
type testActorResourceReferenceImpl struct {
	sync.Mutex
	m map[types.NamespacedActorID]*actorResources
}

func newTestActorResourceReferenceImpl() *testActorResourceReferenceImpl {
	return &testActorResourceReferenceImpl{
		m: make(map[types.NamespacedActorID]*actorResources),
	}
}

func (a *testActorResourceReferenceImpl) track(
	id types.NamespacedActorID,
	memUsageBytes int,
) {
	a.Lock()
	defer a.Unlock()

	curr, ok := a.m[id]
	if !ok {
		curr = &actorResources{}
		a.m[id] = curr
	}
	curr.memoryBytes = memUsageBytes

	if memUsageBytes == 0 {
		delete(a.m, id)
	}
}

func (a *testActorResourceReferenceImpl) memUsageBytes() int {
	a.Lock()
	defer a.Unlock()
	usage := 0
	for _, v := range a.m {
		usage += v.memoryBytes
	}
	return usage
}

func (a *testActorResourceReferenceImpl) topNByMemory(n int) []actorByMem {
	a.Lock()
	defer a.Unlock()

	topN := make([]actorByMem, 0, n)
	for k, v := range a.m {
		topN = append(topN, actorByMem{id: k, memoryBytes: v.memoryBytes})
	}

	sort.Slice(topN, func(a, b int) bool {
		if topN[a].memoryBytes != topN[b].memoryBytes {
			return topN[a].memoryBytes > topN[b].memoryBytes
		}
		return topN[a].id.Less(topN[b].id) > 0
	})

	return topN[:n:n]
}
