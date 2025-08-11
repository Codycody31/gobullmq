
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

Create a new queue and add jobs to it:

```go
import (
 "context"
 "log"

 "github.com/redis/go-redis/v9"
 "go.codycody31.dev/gobullmq"
 "go.codycody31.dev/gobullmq/types"
)

func main() {
 ctx := context.Background()
 redisOpts := &redis.Options{
  Addr: "127.0.0.1:6379", // Or your Redis server address
  // Password: "your_password", // Uncomment if needed
  DB: 0, // Default DB
 }

 queue, err := gobullmq.NewQueue(ctx, "myQueue",
  gobullmq.WithRedisOptions(redisOpts),
  // Optional: Set a custom key prefix
  // gobullmq.WithKeyPrefix("myCustomPrefix"),
 )
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
  gobullmq.AddWithDelay(2000), // Delay by 2 seconds
 )
 if err != nil {
  log.Fatalf("Failed to add job: %v", err)
 }
 log.Printf("Added job %s with ID: %s\n", job.Name, job.Id)
}

```

### Worker

Define a worker to process jobs from the queue:

```go
workerProcess := func(ctx context.Context, job *types.Job) (interface{}, error) {
    fmt.Printf("Processing job: %s\n", job.Name)
    return nil, nil
}

worker, err := gobullmq.NewWorker(ctx, "myQueue", gobullmq.WorkerOptions{
    Concurrency:     1,
    StalledInterval: 30000,
}, redis.NewClient(&redis.Options{
    Addr:     "127.0.0.1:6379",
    Password: "",
}), workerProcess)
if err != nil {
    log.Fatal(err)
}

worker.Run()
worker.Wait()
```

### worker in cluster mode 
```go
func main() {
	ctx := context.Background()
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
	workerProcess := func(ctx context.Context, job *types.Job) (interface{}, error) {
		fmt.Printf("job.Data type: %T, value: %v\n", job.Data, job.Data)

		return nil, nil
	}

	// Initialize the worker with Redis cluster connection
	worker, err := gobullmq.NewWorker(ctx, queueName, gobullmq.WorkerOptions{
		Concurrency:     10,
		StalledInterval: 30000,
		Prefix:          "{jobQueue}",
	}, rdb, workerProcess)
	if err != nil {
		log.Fatalf("Failed to create worker: %v", err)
	}

	fmt.Println("Starting gobullmq worker with concurrency 10...")
	fmt.Println("Waiting for 'job' tasks in queue 'jobQueue'...")

	// Set up error handling
	worker.On("error", func(args ...interface{}) {
		fmt.Printf("Worker error: %v\n", args)
	})

	// Handle graceful shutdown
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	// Run the worker in a goroutine
	
		if err := worker.Run(); err != nil {
			log.Printf("Worker error: %v", err)
		}


	// Wait for interrupt signal
	<-c

	fmt.Println("\nShutting down worker...")
	worker.Close()
	rdb.Close()
	fmt.Println("Worker shut down gracefully")
}

```

### QueueEvents

Listen to events emitted by the queue:

```go
events, err := gobullmq.NewQueueEvents(ctx, "myQueue", gobullmq.QueueEventsOptions{
    RedisClient: *redis.NewClient(&redis.Options{
        Addr:     "127.0.0.1:6379",
        Password: "",
        DB:       0,
    }),
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

Configuration is primarily done using functional options passed to `NewQueue`, `NewWorker`, and `NewQueueEvents`.

### Queue Functional Options

- `WithRedisOptions(*redis.Options)`: **Recommended**. Sets the Redis connection options using a `redis.Options` struct.
- `WithKeyPrefix(string)`: Sets a custom prefix for Redis keys (default is "bull").
- `WithStreamsEventsMaxLen(int64)`: Sets the maximum length for the events stream (default 10000).
- `WithLegacyRedisConfig(ip, password, mode)`: **DEPRECATED**. Use `WithRedisOptions` instead.

### Worker Options

- `Concurrency`: The number of concurrent jobs the worker can process.
- `StalledInterval`: The interval for checking stalled jobs.

### QueueEvents Options

- `RedisClient`: The Redis client used for connecting to the Redis server.
- `Autorun`: Whether to automatically start listening for events.

## Examples

### Adding a Job with Options

```go
jobData := map[string]string{"task": "send_email", "to": "user@example.com"}

job, err := queue.Add(ctx, "emailJob", jobData,
    gobullmq.AddWithPriority(2),
    gobullmq.AddWithDelay(5000), // Delay 5 seconds
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
    gobullmq.AddWithRepeat(types.JobRepeatOptions{
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
