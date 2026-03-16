package eventemitter

import "sync"

// ListenerID is a unique identifier for a registered listener.
type ListenerID uint64

type listener struct {
	id   ListenerID
	fn   func(...interface{})
	once bool
}

type EventEmitter struct {
	mu        sync.Mutex
	listeners map[string][]listener
	nextID    ListenerID
}

func NewEventEmitter() *EventEmitter {
	return &EventEmitter{
		listeners: make(map[string][]listener),
	}
}

func (e *EventEmitter) On(event string, fn func(...interface{})) ListenerID {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.nextID++
	id := e.nextID
	e.listeners[event] = append(e.listeners[event], listener{id: id, fn: fn})
	return id
}

func (e *EventEmitter) Once(event string, fn func(...interface{})) ListenerID {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.nextID++
	id := e.nextID
	e.listeners[event] = append(e.listeners[event], listener{id: id, fn: fn, once: true})
	return id
}

// RemoveListener removes a specific listener by its ID from the given event.
func (e *EventEmitter) RemoveListener(event string, id ListenerID) {
	e.mu.Lock()
	defer e.mu.Unlock()
	ls, ok := e.listeners[event]
	if !ok {
		return
	}
	for i, l := range ls {
		if l.id == id {
			e.listeners[event] = append(ls[:i], ls[i+1:]...)
			if len(e.listeners[event]) == 0 {
				delete(e.listeners, event)
			}
			return
		}
	}
}

func (e *EventEmitter) RemoveAllListeners(event string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.listeners, event)
}

// RemoveAll removes all listeners for all events.
func (e *EventEmitter) RemoveAll() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.listeners = make(map[string][]listener)
}

func (e *EventEmitter) Emit(event string, args ...interface{}) {
	e.mu.Lock()
	ls, ok := e.listeners[event]
	if !ok || len(ls) == 0 {
		e.mu.Unlock()
		return
	}

	// Copy listener functions and filter out once listeners in a single pass.
	// remaining uses ls[:0] which shares the backing array, but this is safe
	// because remaining always trails behind or equals the loop index i,
	// so we never overwrite an element that hasn't been read yet.
	toCall := make([]func(...interface{}), len(ls))
	remaining := ls[:0]
	for i, l := range ls {
		toCall[i] = l.fn
		if !l.once {
			remaining = append(remaining, l)
		}
	}
	if len(remaining) == 0 {
		delete(e.listeners, event)
	} else {
		e.listeners[event] = remaining
	}
	e.mu.Unlock()

	for _, fn := range toCall {
		fn(args...)
	}
}
