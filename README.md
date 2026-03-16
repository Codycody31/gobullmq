
# BullMQ for Golang

BullMQ for Golang is a powerful and flexible job queue library that allows you to manage and process jobs using Redis. It provides a robust set of features for creating, processing, and managing jobs in a distributed environment.

## Supported Versions

- BullMQ v4.12.2 - The current version of gobullmq is based on/compatible with BullMQ v4.12.2.

## Features

- **Queue Management**: Create and manage job queues with ease.
- **Worker Processing**: Define workers to process jobs concurrently.
- **Event Handling**: Listen to and emit events for job lifecycle management.
- **Repeatable Jobs**: Schedule jobs to run at regular intervals.
- **Job Options**: Configure job behavior with flexible options.

## Installation

To install BullMQ for Golang, use the following command:

```bash
go get go.codycody31.dev/gobullmq
```

## Usage

### Queue

Create a new queue and add jobs to it. Note: you must provide your own Redis client.

```go
import (
  "context"
  "log"

  "github.com/redis/go-redis/v9"
  "go.codycody31.dev/gobullmq"
)

func main() {
  ctx := context.Background()
  queueClient := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379", DB: 0})

  queue, err := gobullmq.NewQueue("myQueue", queueClient, &gobullmq.QueueOptions{
    Prefix: "myCustomPrefix",
  })
  if err != nil {
    log.Fatalf("Failed to create queue: %v", err)
  }

  // Define job data (can be any struct that can be JSON marshaled)
  jobData := struct {
    Message string
    Count   int
  }{
    Message: "Hello BullMQ!",
    Count:   1,
  }

  // Add a job using functional options
  job, err := queue.Add(ctx, "myJob", jobData,
    gobullmq.AddWithPriority(5),
    gobullmq.AddWithDelay(2*time.Second), // Delay by 2 seconds
  )
  if err != nil {
    log.Fatalf("Failed to add job: %v", err)
  }
  log.Printf("Added job %s with ID: %s\n", job.Name, job.ID)
}
```

### Worker

Define a worker to process jobs from the queue. Note: use a separate Redis client from the queue and events to avoid CLIENT SETNAME collisions.

```go
import (
  "context"
  "fmt"
  "log"
  "time"

  "github.com/redis/go-redis/v9"
  "go.codycody31.dev/gobullmq"
)

func main() {
  ctx := context.Background()
  workerClient := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379", DB: 0})

  workerProcess := func(ctx context.Context, job *gobullmq.Job) (interface{}, error) {
    fmt.Printf("Processing job: %s\n", job.Name)
    _ = job.UpdateProgress(ctx, 25)
    return "ok", nil
  }

  worker, err := gobullmq.NewWorker("myQueue", workerClient, workerProcess, &gobullmq.WorkerOptions{
    Concurrency:     1,
    StalledInterval: 30 * time.Second,
    Backoff:         &gobullmq.BackoffOptions{Type: "exponential", Delay: 500},
  })
  if err != nil {
    log.Fatal(err)
  }

  // Run blocks until ctx is cancelled
  if err := worker.Run(ctx); err != nil {
    log.Printf("Worker error: %v", err)
  }
}
```

### Worker in Cluster Mode

```go
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

func main() {
  ctx, cancel := context.WithCancel(context.Background())
  defer cancel()
  queueName := "jobQueue"

  // Create Redis Cluster client options
  rdb := redis.NewClusterClient(&redis.ClusterOptions{
    Addrs: []string{
      "127.0.0.1:7000",
      "127.0.0.1:7001",
      "127.0.0.1:7002",
    },
  })

  _, err := rdb.Ping(ctx).Result()
  if err != nil {
    log.Fatalf("Failed to connect to Redis Cluster: %v", err)
  }
  fmt.Println("Connected to Redis Cluster")

  // Define the worker process function
  workerProcess := func(ctx context.Context, job *gobullmq.Job) (interface{}, error) {
    fmt.Printf("job.Data type: %T, value: %v\n", job.Data, job.Data)
    return "ok", nil
  }

  // Initialize the worker with Redis cluster connection
  worker, err := gobullmq.NewWorker(queueName, rdb, workerProcess, &gobullmq.WorkerOptions{
    Concurrency:     10,
    StalledInterval: 30 * time.Second,
    Prefix:          "{jobQueue}",
  })
  if err != nil {
    log.Fatalf("Failed to create worker: %v", err)
  }

  // Set up typed error callback
  worker.OnError(func(err error) {
    fmt.Printf("Worker error: %v\n", err)
  })

  fmt.Println("Starting gobullmq worker with concurrency 10...")
  fmt.Println("Waiting for 'job' tasks in queue 'jobQueue'...")

  // Handle graceful shutdown
  c := make(chan os.Signal, 1)
  signal.Notify(c, os.Interrupt, syscall.SIGTERM)

  // Run the worker in a goroutine; cancel ctx to stop it
  go func() {
    if err := worker.Run(ctx); err != nil {
      log.Printf("Worker error: %v", err)
    }
  }()

  // Wait for interrupt signal
  <-c

  fmt.Println("\nShutting down worker...")
  cancel()
  rdb.Close()
  fmt.Println("Worker shut down gracefully")
}
```

### QueueEvents

Listen to events emitted by the queue. Use a separate Redis client:

```go
eventsClient := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379", DB: 0})
events, err := gobullmq.NewQueueEvents("myQueue", eventsClient, &gobullmq.QueueEventsOptions{
    Autorun: true,
})
if err != nil {
    log.Fatal(err)
}

events.On("added", func(args ...interface{}) {
    fmt.Println("Job added:", args)
})

events.On("error", func(args ...interface{}) {
    fmt.Println("Error event:", args)
})
```

## Configuration

Configuration is done using option structs passed to `NewQueue`, `NewWorker`, and `NewQueueEvents`. You must construct and pass your own `redis.Cmdable` (e.g., `*redis.Client` or `*redis.ClusterClient`).

### Queue Options

- `Prefix`: Sets a custom prefix for Redis keys (default is "bull").
- `StreamEventsMaxLen`: Sets the maximum length for the events stream (default 10000).

### Worker Options

- `Concurrency`: The number of concurrent jobs the worker can process.
- `StalledInterval`: The interval (`time.Duration`) for checking stalled jobs.
- `Backoff`: Configure retry backoff behavior (e.g., `{Type: "fixed"|"exponential", Delay: ms}`).

### Important note on Redis clients

- Use three separate Redis clients in your app: one each for `Queue`, `Worker`, and `QueueEvents`. This avoids `CLIENT SETNAME` overwrites, as each component sets a distinct client name.

### QueueEvents Options

- `Autorun`: Whether to automatically start listening for events.
- `Prefix`: Sets a custom prefix for Redis keys.

## Examples

### Adding a Job with Options

```go
jobData := map[string]string{"task": "send_email", "to": "user@example.com"}

job, err := queue.Add(ctx, "emailJob", jobData,
    gobullmq.AddWithPriority(2),
    gobullmq.AddWithDelay(5*time.Second), // Delay 5 seconds
    gobullmq.AddWithAttempts(3),
    gobullmq.AddWithRemoveOnComplete(gobullmq.KeepJobs{Count: 100}), // Keep last 100 completed
)
if err != nil {
    log.Fatalf("Failed to add email job: %v", err)
}
```

### Adding a Repeatable Job

```go
// Add a job that repeats every 10 seconds
_, err = queue.Add(ctx, "myRepeatableJob", jobData,
    gobullmq.AddWithRepeat(gobullmq.JobRepeatOptions{
        Every: 10000, // Repeat every 10000 ms (10 seconds)
    }),
)
if err != nil {
    log.Fatal(err)
}
```

## Contributing

Contributions are welcome! Please open an issue or submit a pull request on GitHub.

## License

This project is licensed under the MIT License. See the LICENSE file for details.
