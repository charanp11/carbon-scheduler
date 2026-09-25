// Package queue provides a priority queue of pending jobs, ordered so
// that urgent jobs and the earliest deadlines surface first.
package queue

import (
	"container/heap"

	"github.com/charanp11/carbon-scheduler/internal/job"
)

// Item wraps a job.Spec for placement in the priority queue. The index
// field is maintained by container/heap and shouldn't be set directly.
type Item struct {
	Spec  job.Spec
	index int
}

// PriorityQueue implements container/heap.Interface over pending jobs.
// Urgent jobs always sort ahead of flexible ones; within the same
// priority, the earlier deadline sorts first.
type PriorityQueue []*Item

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	a, b := pq[i].Spec, pq[j].Spec
	if a.Priority != b.Priority {
		return a.Priority == job.Urgent
	}
	return a.Deadline.Before(b.Deadline)
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x any) {
	item := x.(*Item)
	item.index = len(*pq)
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid a memory leak on the popped slot
	item.index = -1
	*pq = old[:n-1]
	return item
}

// New returns an empty, ready-to-use priority queue.
func New() *PriorityQueue {
	pq := make(PriorityQueue, 0)
	heap.Init(&pq)
	return &pq
}
