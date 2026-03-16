package main

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"go.codycody31.dev/gobullmq"
)

// JobPayload is the typed data for our jobs.
type JobPayload struct {
	TaskID  int    `json:"taskId"`
	Message string `json:"message"`
}

func main() {
	queueName := "test"
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Define Redis connection options
	redisOpts := &redis.Options{
		Addr: "127.0.0.1:6379",
		DB:   0,
	}

	// Create separate redis clients for queue, worker, and events to avoid CLIENT SETNAME clashes
	queueClient := redis.NewClient(redisOpts)
	workerClient := redis.NewClient(redisOpts)
	eventsClient := redis.NewClient(redisOpts)

	// Initialize the queue with typed data (no context in constructor, no I/O)
	queue, err := gobullmq.NewQueue[JobPayload](queueName, queueClient, nil)
	if err != nil {
		fmt.Println("Error initializing queue:", err)
		return
	}

	// Define the worker process function with typed data and result
	workerProcess := func(ctx context.Context, job *gobullmq.Job[JobPayload]) (string, error) {
		fmt.Printf("Processing job: %s\n", job.ID())
		fmt.Printf("Data: %+v\n", job.Data())

		if job.RepeatJobKey() != "" {
			fmt.Printf("Repeat job key: %s\n", job.RepeatJobKey())
		}

		// Update progress and extend lock via job methods
		_ = job.UpdateProgress(ctx, 10)
		_ = job.ExtendLock(ctx, 10*time.Second)

		r, _ := rand.Int(rand.Reader, big.NewInt(100))
		if r.Int64() < 50 {
			return "", errors.New("job failed")
		}

		return "ok", nil
	}

	// Initialize the worker with typed data and result (no context in constructor)
	worker, err := gobullmq.NewWorker[JobPayload, string](queueName, workerClient, workerProcess, &gobullmq.WorkerOptions{
		Concurrency:     1,
		StalledInterval: 30 * time.Second,
		Backoff:         &gobullmq.BackoffOptions{Type: "exponential", Delay: 500},
	})
	if err != nil {
		fmt.Println("Error initializing worker:", err)
		return
	}

	// Initialize queue events
	events, err := gobullmq.NewQueueEvents(ctx, queueName, eventsClient, &gobullmq.QueueEventsOptions{
		Autorun: true,
	})
	if err != nil {
		fmt.Println("Error initializing queue events:", err)
		return
	}

	// Set up typed event listeners on QueueEvents (stream-based)
	events.OnCompleted(func(evt gobullmq.CompletedEvent) {
		fmt.Printf("Job %s completed (stream %s): %v\n", evt.JobID, evt.StreamID, evt.ReturnValue)
	})
	events.OnFailed(func(evt gobullmq.FailedEvent) {
		fmt.Printf("Job %s failed (stream %s): %s\n", evt.JobID, evt.StreamID, evt.FailedReason)
	})
	events.OnActive(func(evt gobullmq.QueueEventData) {
		fmt.Printf("Job %s active (stream %s)\n", evt.JobID, evt.StreamID)
	})
	events.OnError(func(err error) {
		fmt.Println("QueueEvents error:", err)
	})

	// Create job data
	jobPayload := JobPayload{
		Message: "Processing job",
	}

	// Add jobs to the queue
	for i := 0; i < 10; i++ {
		jobPayload.TaskID = i
		if _, err := queue.Add(ctx, "testJob", jobPayload, gobullmq.AddWithAttempts(3)); err != nil {
			fmt.Printf("Error adding job %d: %v\n", i, err)
		}
	}

	// Example of adding a delayed job using time.Duration
	_, err = queue.Add(ctx, "delayedJob", jobPayload,
		gobullmq.AddWithDelay(2*time.Second),
		gobullmq.AddWithAttempts(3),
	)
	if err != nil {
		fmt.Println("Error adding delayed job:", err)
	}

	// Example of adding a repeatable job
	_, err = queue.Add(ctx, "repeatableTest", jobPayload,
		gobullmq.AddWithRepeat(gobullmq.JobRepeatOptions{
			Every: 5000,
		}),
	)
	if err != nil {
		fmt.Println("Error adding repeatable job:", err)
	}

	// Set up typed event listeners on Worker (in-process)
	worker.OnCompleted(func(job *gobullmq.Job[JobPayload], result string) {
		fmt.Printf("Worker: job %s completed with result: %v\n", job.ID(), result)
	})
	worker.OnFailed(func(job *gobullmq.Job[JobPayload], err error) {
		fmt.Printf("Worker: job %s failed: %v\n", job.ID(), err)
	})
	worker.OnActive(func(job *gobullmq.Job[JobPayload]) {
		fmt.Printf("Worker: job %s active\n", job.ID())
	})
	worker.OnError(func(err error) {
		fmt.Println("Worker error:", err)
	})

	// Run the worker (context controls lifetime)
	if err := worker.Run(ctx); err != nil {
		fmt.Println("Error running worker:", err)
	}

	// Wait for shutdown signal (ctx will be cancelled by signal.NotifyContext)
	worker.Wait()

	// Clean up
	if err := worker.Close(); err != nil {
		fmt.Println("Error closing worker:", err)
	}
	if err := events.Close(); err != nil {
		fmt.Println("Error closing events:", err)
	}
	if err := queue.Close(ctx); err != nil {
		fmt.Println("Error closing queue:", err)
	}
}
