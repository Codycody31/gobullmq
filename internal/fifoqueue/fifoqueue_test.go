package fifoqueue

import (
	"errors"
	"testing"
	"time"
)

func TestWaitAllTimeoutDoesNotClosePublisherChannels(t *testing.T) {
	q := NewFifoQueue[int](1, false)
	started := make(chan struct{})
	release := make(chan struct{})
	if err := q.Add(func() (int, error) {
		close(started)
		<-release
		return 1, nil
	}); err != nil {
		t.Fatal(err)
	}
	<-started

	if err := q.WaitAll(10 * time.Millisecond); !errors.Is(err, ErrWaitTimeout) {
		t.Fatalf("WaitAll error = %v, want ErrWaitTimeout", err)
	}
	close(release)

	deadline := time.Now().Add(time.Second)
	for {
		if err := q.WaitAll(10 * time.Millisecond); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("worker did not exit after the blocked task was released")
		}
	}
	if err := q.Add(func() (int, error) { return 2, nil }); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("Add after WaitAll = %v, want ErrQueueClosed", err)
	}
}
