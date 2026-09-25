package queue

import (
	"container/heap"
	"testing"
	"time"

	"github.com/charanp11/carbon-scheduler/internal/job"
)

func TestPriorityQueueOrdering(t *testing.T) {
	now := time.Now()
	pq := New()

	// Pushed out of order on purpose: a late flexible job first, then
	// an urgent job, then an earlier flexible job. The heap should
	// still pop urgent first, then flexible ordered by deadline.
	heap.Push(pq, &Item{Spec: job.Spec{ID: "flex-late", Priority: job.Flexible, Deadline: now.Add(3 * time.Hour)}})
	heap.Push(pq, &Item{Spec: job.Spec{ID: "urgent", Priority: job.Urgent, Deadline: now.Add(time.Hour)}})
	heap.Push(pq, &Item{Spec: job.Spec{ID: "flex-early", Priority: job.Flexible, Deadline: now.Add(time.Hour)}})

	want := []string{"urgent", "flex-early", "flex-late"}
	for _, id := range want {
		got := heap.Pop(pq).(*Item).Spec.ID
		if got != id {
			t.Errorf("Pop() = %q, want %q", got, id)
		}
	}
	if pq.Len() != 0 {
		t.Errorf("queue should be empty, has %d items left", pq.Len())
	}
}
