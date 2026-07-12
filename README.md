# BullMQ for Golang

BullMQ for Golang is a Redis-backed job queue compatible with [BullMQ](https://github.com/taskforcesh/bullmq). Go and Node.js processes can produce and consume jobs on the same queues.

## BullMQ compatibility

The embedded Lua scripts are pinned to BullMQ v4.12.2 and checked by CI. Keys, hashes, packed options, and event streams follow that version's Redis data model.

### Differences from BullMQ

- Cron patterns use [robfig/cron](https://github.com/robfig/cron) instead of `cron-parser`. It accepts 5 or 6 fields, but does not support day-of-week `7` (use `0` for Sunday), `L`, `nthDayOfWeek`, or occurrences more than roughly 5 years out. Day-of-week and day-of-month union semantics may differ at the edges. Repeat dates are normalized to epoch milliseconds when stored.
- `WorkerOptions.MaxStalledCount` treats 0 as "use the default (1)"; pass a negative value for "fail on first stall" (JS `maxStalledCount: 0`).
- `WorkerOptions.Backoff` is a default backoff for jobs that set none (the JS equivalent is `defaultJobOptions.backoff` on the queue); `WorkerOptions.BackoffStrategy` handles non-builtin backoff types like JS `settings.backoffStrategy`.
- Failure stacktraces record the Go error text (Go errors carry no stack); the `stacktrace`/`failedReason` hash fields are written on every failure like upstream.
- Sandboxed (child-process) processors are not implemented.
- Lifecycle is context-based: `Worker.Shutdown(ctx)` and `Worker.PauseAndWait(ctx)` bound the graceful drain by the context's deadline, instead of upstream's unbounded blocking `close()`/`pause()`; `Worker.Close()` is the immediate (force) variant.

## Installation

```bash
go get go.codycody31.dev/gobullmq
```

## Quick start

Queues, workers, and jobs are generic over the payload type. You provide your own Redis client (`redis.Cmdable`); it is never closed by the library. Use a separate client for the queue, each worker, and each `QueueEvents` instance: blocking consumers monopolize a connection and set their own `CLIENT SETNAME`.

```go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"go.codycody31.dev/gobullmq"
)

// EmailJob is the typed job payload (anything JSON-marshalable works).
type EmailJob struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	queueClient := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	workerClient := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	defer queueClient.Close()
	defer workerClient.Close()

	// Queue[EmailJob]: constructor does no I/O and takes no context.
	queue, err := gobullmq.NewQueue[EmailJob]("mail", queueClient, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer queue.Close()

	job, err := queue.Add(ctx, "welcome", EmailJob{To: "user@example.com", Subject: "Welcome!"},
		gobullmq.AddWithAttempts(3),
		gobullmq.AddWithBackoff(gobullmq.BackoffOptions{Type: "exponential", Delay: 500}),
	)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("added job", job.ID())

	// Worker[EmailJob, string]: processes EmailJob payloads, returns string results.
	process := func(ctx context.Context, job *gobullmq.Job[EmailJob]) (string, error) {
		fmt.Printf("sending %q to %s\n", job.Data().Subject, job.Data().To)
		return "sent", nil
	}
	worker, err := gobullmq.NewWorker[EmailJob, string]("mail", workerClient, process, &gobullmq.WorkerOptions{
		Concurrency: 4,
	})
	if err != nil {
		log.Fatal(err)
	}

	if err := worker.Run(ctx); err != nil { // starts processing; returns immediately
		log.Fatal(err)
	}

	<-ctx.Done() // wait for SIGINT/SIGTERM

	// Graceful shutdown: stop fetching, drain in-flight jobs, bounded by ctx.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()
	if err := worker.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown: %v", err)
	}
}
```

Query jobs with typed states and orderings:

```go
counts, err := queue.JobCounts(ctx, gobullmq.JobStateWaiting, gobullmq.JobStateActive, gobullmq.JobStateFailed)
if err != nil {
	log.Fatal(err)
}
fmt.Println("waiting:", counts[gobullmq.JobStateWaiting])

failed, err := queue.Jobs(ctx, []gobullmq.JobState{gobullmq.JobStateFailed}, 0, 9, gobullmq.SortDesc)
if err != nil {
	log.Fatal(err)
}
for _, j := range failed {
	// Retry back onto the wait list, at the front (LIFO) or back (FIFO).
	if err := j.Retry(ctx, gobullmq.JobStateFailed, gobullmq.FIFO); err != nil {
		log.Printf("retry %s: %v", j.ID(), err)
	}
}

state, err := queue.JobState(ctx, job.ID())
if err != nil {
	log.Fatal(err)
}
fmt.Println(state == gobullmq.JobStateCompleted)
```

## Events

Workers emit in-process events with typed callbacks:

```go
worker.OnCompleted(func(job *gobullmq.Job[EmailJob], result string) {
	fmt.Println("completed:", job.ID(), result)
})
worker.OnFailed(func(job *gobullmq.Job[EmailJob], err error) {
	fmt.Println("failed:", job.ID(), err)
})
worker.OnActive(func(job *gobullmq.Job[EmailJob]) {
	fmt.Println("active:", job.ID())
})
worker.OnError(func(err error) {
	fmt.Println("worker error:", err)
})
```

`QueueEvents` reads the queue's Redis event stream, including events from Node.js producers and workers. Give it a dedicated Redis client. When `Autorun` is set, the constructor context controls the consumer; otherwise call `Run(ctx)` yourself.

```go
eventsClient := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
defer eventsClient.Close()
events, err := gobullmq.NewQueueEvents(ctx, "mail", eventsClient, &gobullmq.QueueEventsOptions{
	Autorun: true,
})
if err != nil {
	log.Fatal(err)
}
defer events.Close()

events.OnCompleted(func(evt gobullmq.CompletedEvent) {
	fmt.Println("completed:", evt.JobID, evt.ReturnValue)
})
events.OnFailed(func(evt gobullmq.FailedEvent) {
	fmt.Println("failed:", evt.JobID, evt.FailedReason)
})
events.OnProgress(func(evt gobullmq.ProgressEvent) {
	fmt.Println("progress:", evt.JobID, evt.Data)
})
```

A producer can block until a job finishes (bound the wait with the context):

```go
waitCtx, cancel := context.WithTimeout(ctx, time.Minute)
defer cancel()
result, err := job.WaitUntilFinished(waitCtx, events)
```

## Flows

A `FlowProducer` adds a parent job together with its children atomically; the parent stays in the `waiting-children` state until every child has completed. The producer holds no resources of its own, so there is nothing to close.

```go
flows, err := gobullmq.NewFlowProducer(client, nil)
if err != nil {
	log.Fatal(err)
}

tree, err := flows.Add(ctx, gobullmq.FlowJob{
	Name:      "assemble",
	QueueName: "assembly",
	Children: []gobullmq.FlowJob{
		{Name: "weld", QueueName: "parts", Data: map[string]any{"part": "frame"}},
		{Name: "paint", QueueName: "parts", Data: map[string]any{"part": "chassis"}},
	},
})
if err != nil {
	log.Fatal(err)
}
fmt.Println("parent:", tree.Job.ID(), "children:", len(tree.Children))

// Fetch the tree back later:
fetched, err := flows.Flow(ctx, gobullmq.FlowGetOptions{
	ID:        tree.Job.ID(),
	QueueName: "assembly",
})
if err != nil {
	log.Fatal(err)
}
fmt.Println("fetched parent:", fetched.Job.ID())
```

Inside a parent's processor, `job.ChildrenValues(ctx)` returns the children's results.

## Repeatable jobs

Pass repeat options when adding; each due iteration is materialized as a delayed job. `Every` is in milliseconds, or use a cron `Pattern`:

```go
_, err = queue.Add(ctx, "report", EmailJob{To: "ops@example.com", Subject: "Hourly report"},
	gobullmq.AddWithRepeat(gobullmq.JobRepeatOptions{
		Pattern: "0 * * * *", // hourly; or Every: 10000 for every 10s
	}),
)
```

List and remove repeatables:

```go
repeatables, err := queue.RepeatableJobs(ctx, 0, -1, gobullmq.SortAsc)
if err != nil {
	log.Fatal(err)
}
for _, r := range repeatables {
	if err := queue.RemoveRepeatableByKey(ctx, r.Key); err != nil {
		log.Printf("remove %s: %v", r.Key, err)
	}
}
```

## Manual processing

A worker can be used without `Run()`: fetch jobs explicitly and settle them yourself. The token identifies the lock owner; the fetched job carries it (`job.Token()`).

```go
worker, err := gobullmq.NewWorker[EmailJob, string]("mail", workerClient, nil, nil)
if err != nil {
	log.Fatal(err)
}

job, err := worker.NextJob(ctx, "manual-consumer-1")
if err != nil {
	log.Fatal(err)
}
if job != nil {
	if err := send(job.Data()); err != nil {
		if ferr := job.MoveToFailed(ctx, err, false); ferr != nil {
			log.Printf("moveToFailed %s: %v", job.ID(), ferr)
		}
	} else {
		// Pass true to get the next waiting job back, already locked with
		// the same token (nil when none).
		next, err := job.MoveToCompleted(ctx, "sent", true)
		if err != nil {
			log.Printf("moveToCompleted %s: %v", job.ID(), err)
		}
		_ = next
	}
}
```

## Lifecycle

```go
// Worker, graceful: stop fetching, drain in-flight jobs, bounded by ctx.
// When ctx expires first, remaining jobs are abandoned and the error wraps
// gobullmq.ErrShutdownTimeout.
shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
err = worker.Shutdown(shutdownCtx)

// Worker, immediate: in-flight jobs are abandoned (their process contexts
// are cancelled); the stalled checker of another worker will recover them.
err = worker.Close()

// Worker pause/resume without closing.
worker.Pause()                      // immediate; in-flight jobs keep running
err = worker.PauseAndWait(pauseCtx) // pause, then wait for in-flight jobs
worker.Resume()

// Queue pausing affects all workers on the queue (state lives in Redis).
err = queue.Pause(ctx)
err = queue.Resume(ctx)

// Queue and QueueEvents close locally, without a context; the Redis clients
// are yours to close.
err = queue.Close()
err = events.Close()
```

## Contributing

Open an issue or submit a pull request on GitHub.

## License

MIT. See [LICENSE](LICENSE).
