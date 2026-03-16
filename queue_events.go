package gobullmq

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	eventemitter "go.codycody31.dev/gobullmq/internal/eventEmitter"
)

type QueueEvents struct {
	name        string                     // Name of the queue
	token       uuid.UUID                  // Token used to identify the queue events
	ee          *eventemitter.EventEmitter // Event emitter used to handle events occuring in worker threads/go routines/etc
	running     atomic.Bool                // Flag to indicate if the queue events is running
	closing     atomic.Bool                // Flag to indicate if the queue events is closing
	redisClient redis.Cmdable              // Redis client used to interact with the redis server
	cancel      context.CancelFunc         // Cancel function used to stop the queue events
	prefix      string
	keyPrefix   string
	mutex       sync.Mutex     // Mutex used to lock/unlock the queue events
	wg          sync.WaitGroup // WaitGroup used to wait for the queue events to finish
	opts        struct {
		LastEventId string // Last event id
	}
}

type QueueEventsOptions struct {
	Autorun bool   // If true, run the queue events immediately after creation
	Prefix  string // Prefix for the queue events key
}

// NewQueueEvents creates a new QueueEvents instance.
// The provided ctx is used when Autorun is true; otherwise it is ignored.
func NewQueueEvents(ctx context.Context, name string, client redis.Cmdable, opts *QueueEventsOptions) (*QueueEvents, error) {
	if client == nil {
		return nil, fmt.Errorf("redis client must not be nil")
	}
	if opts == nil {
		opts = &QueueEventsOptions{}
	}

	qe := &QueueEvents{
		name:        name,
		token:       uuid.New(),
		ee:          eventemitter.NewEventEmitter(),
		redisClient: client,
	}

	if opts.Prefix == "" {
		qe.keyPrefix = "bull"
	} else {
		qe.keyPrefix = strings.Trim(opts.Prefix, ":")
		if qe.keyPrefix == "" {
			return nil, fmt.Errorf("prefix cannot be empty or just colons")
		}
	}
	qe.prefix = qe.keyPrefix
	qe.keyPrefix = qe.keyPrefix + ":" + name + ":"

	if opts.Autorun {
		err := qe.Run(ctx)
		if err != nil {
			return nil, fmt.Errorf("error running queue events: %w", err)
		}
	}

	return qe, nil
}

// Name returns the name of the queue
func (qe *QueueEvents) Name() string {
	return qe.name
}

// Emit emits the event with the given name and arguments
func (qe *QueueEvents) Emit(event string, args ...interface{}) {
	qe.ee.Emit(event, args...)
}

// Off removes a specific listener by its ListenerID.
func (qe *QueueEvents) Off(event string, id eventemitter.ListenerID) {
	qe.ee.RemoveListener(event, id)
}

// On listens for the event and returns a ListenerID that can be used with Off.
func (qe *QueueEvents) On(event string, listener func(...interface{})) eventemitter.ListenerID {
	return qe.ee.On(event, listener)
}

// Once listens for the event only once and returns a ListenerID.
func (qe *QueueEvents) Once(event string, listener func(...interface{})) eventemitter.ListenerID {
	return qe.ee.Once(event, listener)
}

// Run starts the queue events and listens for events from the redis stream
func (qe *QueueEvents) Run(ctx context.Context) error {
	qe.mutex.Lock()
	defer qe.mutex.Unlock()

	if qe.running.Load() {
		return errors.New("queue events is already running")
	}

	ctx, cancel := context.WithCancel(ctx)
	qe.cancel = cancel
	qe.running.Store(true)
	client := qe.redisClient
	// Set client name on provided client(s), handling both single and cluster
	switch c := client.(type) {
	case *redis.Client:
		_ = c.Do(ctx, "CLIENT", "SETNAME", fmt.Sprintf("%s:%s%s", qe.prefix, base64.StdEncoding.EncodeToString([]byte(qe.name)), ":qe")).Err()
	case *redis.ClusterClient:
		_ = c.ForEachShard(ctx, func(shardCtx context.Context, shardClient *redis.Client) error {
			return shardClient.Do(shardCtx, "CLIENT", "SETNAME", fmt.Sprintf("%s:%s%s", qe.prefix, base64.StdEncoding.EncodeToString([]byte(qe.name)), ":qe")).Err()
		})
	}

	qe.wg.Add(1)

	go func() {
		defer func() {
			qe.running.Store(false)
			qe.wg.Done()
		}()
		if err := qe.consumeEvents(ctx, client); err != nil {
			qe.Emit("error", fmt.Sprintf("Critical error in consumeEvents: %v", err))
			qe.cancel()
		}
	}()

	return nil
}

// consumeEvents consumes events from the redis stream
func (qe *QueueEvents) consumeEvents(ctx context.Context, client redis.Cmdable) error {
	eventKey := qe.keyPrefix + "events"
	id := "$"
	if qe.opts.LastEventId != "" {
		id = qe.opts.LastEventId
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		streams, err := client.XRead(ctx, &redis.XReadArgs{
			Streams: []string{eventKey, id},
			Block:   2 * time.Second,
		}).Result()

		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			// XRead returns redis.Nil when the block timeout expires with no data.
			if errors.Is(err, redis.Nil) {
				continue
			}
			qe.Emit("error", fmt.Sprintf("Error reading from stream: %v", err))
			select {
			case <-time.After(500 * time.Millisecond):
			case <-ctx.Done():
				return nil
			}
			continue
		}

		for _, stream := range streams {
			for _, message := range stream.Messages {
				id = message.ID
				args := message.Values

				if err := qe.processEvent(args, id); err != nil {
					qe.Emit("error", fmt.Sprintf("Error processing event: %v", err))
					continue
				}
			}
		}
	}
}

// processEvent processes the event with the given arguments
func (qe *QueueEvents) processEvent(args map[string]interface{}, id string) error {
	// Extract the event name
	eventName, ok := args["event"].(string)
	if !ok {
		return fmt.Errorf("missing or invalid 'event' field in message ID %s", id)
	}

	// Initialize event data
	var data interface{}
	var err error

	// Handle specific events that require data unmarshaling
	switch eventName {
	case "progress", "completed":
		dataKey := "data"
		if eventName == "completed" {
			dataKey = "returnvalue"
		}
		dataStr, ok := args[dataKey].(string)
		if !ok {
			return fmt.Errorf("missing or invalid '%s' field in message ID %s", dataKey, id)
		}
		// Unmarshal the JSON data
		if err = json.Unmarshal([]byte(dataStr), &data); err != nil {
			return fmt.Errorf("error unmarshaling '%s': %w", dataKey, err)
		}
		args[dataKey] = data
	}

	// Emit the event
	qe.emitEvent(eventName, args, id)
	return nil
}

// emitEvent emits the event with the given name and arguments
func (qe *QueueEvents) emitEvent(eventName string, args map[string]interface{}, id string) {
	jobId, _ := args["jobId"].(string)

	if eventName == "drained" {
		qe.Emit(eventName, id)
	} else {
		qe.Emit(eventName, args, id)
		if jobId != "" {
			qe.Emit(fmt.Sprintf("%s:%s", eventName, jobId), args, id)
		}
	}
}

// --- Typed Event Subscriptions ---

// CompletedEvent represents a typed completed event from the queue stream.
type CompletedEvent struct {
	JobID       string
	StreamID    string
	ReturnValue interface{}
}

// FailedEvent represents a typed failed event from the queue stream.
type FailedEvent struct {
	JobID        string
	StreamID     string
	FailedReason string
}

// ProgressEvent represents a typed progress event from the queue stream.
type ProgressEvent struct {
	JobID    string
	StreamID string
	Data     interface{}
}

// QueueEventData represents a generic typed event from the queue stream.
type QueueEventData struct {
	JobID    string
	StreamID string
	Data     map[string]interface{}
}

// OnCompleted registers a typed callback for completed events and returns a ListenerID.
func (qe *QueueEvents) OnCompleted(fn func(CompletedEvent)) eventemitter.ListenerID {
	return qe.ee.On("completed", func(args ...interface{}) {
		evt := CompletedEvent{}
		if len(args) >= 2 {
			if data, ok := args[0].(map[string]interface{}); ok {
				evt.JobID, _ = data["jobId"].(string)
				evt.ReturnValue = data["returnvalue"]
			}
			evt.StreamID, _ = args[1].(string)
		}
		fn(evt)
	})
}

// OnFailed registers a typed callback for failed events and returns a ListenerID.
func (qe *QueueEvents) OnFailed(fn func(FailedEvent)) eventemitter.ListenerID {
	return qe.ee.On("failed", func(args ...interface{}) {
		evt := FailedEvent{}
		if len(args) >= 2 {
			if data, ok := args[0].(map[string]interface{}); ok {
				evt.JobID, _ = data["jobId"].(string)
				evt.FailedReason, _ = data["failedReason"].(string)
			}
			evt.StreamID, _ = args[1].(string)
		}
		fn(evt)
	})
}

// OnProgress registers a typed callback for progress events and returns a ListenerID.
func (qe *QueueEvents) OnProgress(fn func(ProgressEvent)) eventemitter.ListenerID {
	return qe.ee.On("progress", func(args ...interface{}) {
		evt := ProgressEvent{}
		if len(args) >= 2 {
			if data, ok := args[0].(map[string]interface{}); ok {
				evt.JobID, _ = data["jobId"].(string)
				evt.Data = data["data"]
			}
			evt.StreamID, _ = args[1].(string)
		}
		fn(evt)
	})
}

// OnActive registers a typed callback for active events and returns a ListenerID.
func (qe *QueueEvents) OnActive(fn func(QueueEventData)) eventemitter.ListenerID {
	return qe.ee.On("active", func(args ...interface{}) {
		evt := QueueEventData{}
		if len(args) >= 2 {
			if data, ok := args[0].(map[string]interface{}); ok {
				evt.JobID, _ = data["jobId"].(string)
				evt.Data = data
			}
			evt.StreamID, _ = args[1].(string)
		}
		fn(evt)
	})
}

// OnStalled registers a typed callback for stalled events and returns a ListenerID.
func (qe *QueueEvents) OnStalled(fn func(QueueEventData)) eventemitter.ListenerID {
	return qe.ee.On("stalled", func(args ...interface{}) {
		evt := QueueEventData{}
		if len(args) >= 2 {
			if data, ok := args[0].(map[string]interface{}); ok {
				evt.JobID, _ = data["jobId"].(string)
				evt.Data = data
			}
			evt.StreamID, _ = args[1].(string)
		}
		fn(evt)
	})
}

// OnDrained registers a typed callback for drained events and returns a ListenerID.
func (qe *QueueEvents) OnDrained(fn func(streamID string)) eventemitter.ListenerID {
	return qe.ee.On("drained", func(args ...interface{}) {
		var streamID string
		if len(args) >= 1 {
			streamID, _ = args[0].(string)
		}
		fn(streamID)
	})
}

// OnError registers a typed callback for error events and returns a ListenerID.
func (qe *QueueEvents) OnError(fn func(err error)) eventemitter.ListenerID {
	return qe.ee.On("error", func(args ...interface{}) {
		if len(args) >= 1 {
			switch e := args[0].(type) {
			case error:
				fn(e)
			case string:
				fn(errors.New(e))
			default:
				fn(fmt.Errorf("%v", e))
			}
		}
	})
}

// Ping checks the connection to the Redis server.
func (qe *QueueEvents) Ping(ctx context.Context) error {
	_, err := qe.redisClient.Ping(ctx).Result()
	return err
}

// Close stops the queue events with a default 5-second shutdown timeout.
func (qe *QueueEvents) Close() error {
	return qe.CloseWithTimeout(5 * time.Second)
}

// CloseWithTimeout stops the queue events, waiting up to timeout for graceful shutdown.
func (qe *QueueEvents) CloseWithTimeout(timeout time.Duration) error {
	qe.mutex.Lock()
	defer qe.mutex.Unlock()

	if !qe.running.Load() {
		return nil
	}

	qe.closing.Store(true)
	qe.cancel()

	done := make(chan struct{})
	go func() {
		qe.wg.Wait()
		close(done)
	}()

	var err error
	select {
	case <-done:
	case <-time.After(timeout):
		err = fmt.Errorf("queue events shutdown timed out after %v", timeout)
	}

	qe.running.Store(false)
	qe.closing.Store(false)
	return err
}
