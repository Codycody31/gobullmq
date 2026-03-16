package eventemitter

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestEventEmitter_On(t *testing.T) {
	e := NewEventEmitter()
	var wg sync.WaitGroup
	wg.Add(1)

	e.On("testEvent", func(args ...interface{}) {
		if args[0].(string) != "test" {
			t.Errorf("Expected 'test', got %v", args[0])
		}
		wg.Done()
	})

	e.Emit("testEvent", "test")
	wg.Wait()
}

func TestEventEmitter_MultipleListeners(t *testing.T) {
	e := NewEventEmitter()
	var wg sync.WaitGroup
	wg.Add(2)

	var count1, count2 atomic.Int32

	e.On("multi", func(args ...interface{}) {
		count1.Add(1)
		wg.Done()
	})
	e.On("multi", func(args ...interface{}) {
		count2.Add(1)
		wg.Done()
	})

	e.Emit("multi", "data")
	wg.Wait()

	if count1.Load() != 1 || count2.Load() != 1 {
		t.Errorf("Expected both listeners called once, got %d and %d", count1.Load(), count2.Load())
	}
}

func TestEventEmitter_Once(t *testing.T) {
	e := NewEventEmitter()
	var count atomic.Int32

	var wg sync.WaitGroup
	wg.Add(1)
	e.Once("onceEvent", func(args ...interface{}) {
		count.Add(1)
		wg.Done()
	})

	e.Emit("onceEvent", "first")
	wg.Wait()

	// Second emit should not call the listener.
	e.Emit("onceEvent", "second")

	if count.Load() != 1 {
		t.Errorf("Once listener should be called exactly once, got %d", count.Load())
	}
}

func TestEventEmitter_RemoveListener(t *testing.T) {
	e := NewEventEmitter()
	var count atomic.Int32

	id := e.On("testRemoveEvent", func(args ...interface{}) {
		count.Add(1)
	})

	e.RemoveListener("testRemoveEvent", id)
	e.Emit("testRemoveEvent", "test")

	if count.Load() != 0 {
		t.Errorf("Listener should have been removed but it was triggered")
	}
}

func TestEventEmitter_RemoveSpecificListener(t *testing.T) {
	e := NewEventEmitter()
	var count1, count2 atomic.Int32

	id1 := e.On("event", func(args ...interface{}) {
		count1.Add(1)
	})
	e.On("event", func(args ...interface{}) {
		count2.Add(1)
	})

	// Remove only the first listener
	e.RemoveListener("event", id1)
	e.Emit("event", "test")

	if count1.Load() != 0 {
		t.Errorf("Removed listener should not be called, got %d", count1.Load())
	}
	if count2.Load() != 1 {
		t.Errorf("Remaining listener should be called once, got %d", count2.Load())
	}
}

func TestEventEmitter_RemoveAllListeners(t *testing.T) {
	e := NewEventEmitter()
	var count1, count2 atomic.Int32

	e.On("testRemoveAllEvent", func(args ...interface{}) {
		count1.Add(1)
	})
	e.On("testRemoveAllEvent", func(args ...interface{}) {
		count2.Add(1)
	})

	e.RemoveAllListeners("testRemoveAllEvent")
	e.Emit("testRemoveAllEvent", "test")

	if count1.Load() != 0 || count2.Load() != 0 {
		t.Errorf("Listeners should have been removed but at least one was triggered")
	}
}

func TestEventEmitter_RemoveAll(t *testing.T) {
	e := NewEventEmitter()
	var count atomic.Int32

	e.On("event1", func(args ...interface{}) { count.Add(1) })
	e.On("event2", func(args ...interface{}) { count.Add(1) })

	e.RemoveAll()
	e.Emit("event1", "test")
	e.Emit("event2", "test")

	if count.Load() != 0 {
		t.Errorf("All listeners should have been removed, got %d calls", count.Load())
	}
}

func TestEventEmitter_Emit(t *testing.T) {
	e := NewEventEmitter()
	var wg sync.WaitGroup
	wg.Add(1)

	e.On("testEmitEvent", func(args ...interface{}) {
		if args[0].(string) != "emit" {
			t.Errorf("Expected 'emit', got %v", args[0])
		}
		wg.Done()
	})

	e.Emit("testEmitEvent", "emit")
	wg.Wait()
}

func TestEventEmitter_EmitNoListeners(t *testing.T) {
	e := NewEventEmitter()
	// Should not panic.
	e.Emit("nonexistent", "data")
}

func TestEventEmitter_ConcurrentEmitAndRemove(t *testing.T) {
	e := NewEventEmitter()
	var wg sync.WaitGroup

	e.On("concurrent", func(args ...interface{}) {})

	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			e.Emit("concurrent", i)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			e.On("concurrent", func(args ...interface{}) {})
			e.RemoveAllListeners("concurrent")
		}
	}()
	wg.Wait()
}

func TestEventEmitter_ListenerIDUniqueness(t *testing.T) {
	e := NewEventEmitter()
	id1 := e.On("event", func(args ...interface{}) {})
	id2 := e.On("event", func(args ...interface{}) {})
	id3 := e.Once("event", func(args ...interface{}) {})

	if id1 == id2 || id2 == id3 || id1 == id3 {
		t.Errorf("Listener IDs should be unique, got %d, %d, %d", id1, id2, id3)
	}
}

func TestEventEmitter_RemoveNonexistentListener(t *testing.T) {
	e := NewEventEmitter()
	// Should not panic when removing a listener that doesn't exist
	e.RemoveListener("nonexistent", 999)

	e.On("event", func(args ...interface{}) {})
	e.RemoveListener("event", 999) // wrong ID
}
