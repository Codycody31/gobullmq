package main

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"

	"github.com/redis/go-redis/v9"
	"go.codycody31.dev/gobullmq"
	"go.codycody31.dev/gobullmq/types"
)

func main() {
	queueName := "test"
	ctx := context.Background()

	// Define Redis connection options
	redisOpts := &redis.Options{
		Addr: "127.0.0.1:6379",
		// Password: "", // Add password if needed
		DB: 0, // Default DB
	}

	// Create separate redis clients for queue, worker, and events to avoid CLIENT SETNAME clashes
	queueClient := redis.NewClient(redisOpts)
	workerClient := redis.NewClient(redisOpts)
	eventsClient := redis.NewClient(redisOpts)

	// Initialize the queue using functional options
	queue, err := gobullmq.NewQueue(ctx, queueName, queueClient)
	if err != nil {
		fmt.Println("Error initializing queue:", err)
		return
	}

	// Define the worker process function (V2 with API)
	workerProcess := func(ctx context.Context, job *types.Job, api gobullmq.WorkerProcessAPI) (interface{}, error) {
		fmt.Printf("Processing job: %s\n", job.Id)
		fmt.Printf("Data: %v\n", job.Data)

		if job.RepeatJobKey != "" {
			fmt.Printf("Repeat job key: %s\n", job.RepeatJobKey)
		}

		// Update progress example
		_ = api.UpdateProgress(ctx, job.Id, 10)
		// Extend lock example
		_ = api.ExtendLock(ctx, job)

		r, _ := rand.Int(rand.Reader, big.NewInt(100))
		if r.Int64() < 50 {
			return nil, errors.New("job failed")
		}

		return "ok", nil
	}

	// Initialize the worker (now requires V2 processor)
	worker, err := gobullmq.NewWorker(ctx, queueName, gobullmq.WorkerOptions{
		Concurrency:     1,
		StalledInterval: 30000,
		Backoff:         &gobullmq.BackoffOptions{Type: "exponential", Delay: 500},
	}, workerClient, workerProcess)
	if err != nil {
		fmt.Println("Error initializing worker:", err)
		return
	}

	// Initialize queue events
	events, err := gobullmq.NewQueueEvents(ctx, queueName, gobullmq.QueueEventsOptions{
		RedisClient: eventsClient,
		Autorun:     true,
	})
	if err != nil {
		fmt.Println("Error initializing queue events:", err)
		return
	}

	// Set up event listeners
	events.On("completed", func(args ...interface{}) {
		fmt.Println("Job completed:", args)
	})
	events.On("active", func(args ...interface{}) {
		fmt.Println("Job active:", args)
	})
	events.On("added", func(args ...interface{}) {
		fmt.Println("Job added:", args)
	})
	events.On("error", func(args ...interface{}) {
		fmt.Println("Error event:", args)
	})

	// Create job data struct
	jobPayload := struct {
		TaskID  int    `json:"taskId"`
		Message string `json:"message"`
	}{
		Message: "Processing job",
	}

	// Add jobs to the queue using the new Add signature
	for i := 0; i < 10; i++ { // Reduced loop count for quicker testing
		jobPayload.TaskID = i                                                            // Modify payload for each job
		if _, err := queue.Add(ctx, "testJob", jobPayload, gobullmq.AddWithAttempts(3)); // Pass context and payload struct
		// Example functional options:
		// gobullmq.AddWithDelay(1000*i), // Delay each job slightly differently
		// gobullmq.AddWithPriority(i%3),
		err != nil {
			fmt.Printf("Error adding job %d: %v\n", i, err)
		}
	}

	// Example of adding a repeatable job:
	_, err = queue.Add(ctx, "repeatableTest", jobPayload,
		gobullmq.AddWithRepeat(types.JobRepeatOptions{
			Every: 5000, // Repeat every second
		}),
	)
	if err != nil {
		fmt.Println("Error adding repeatable job:", err)
	}

	worker.On("completed", func(args ...interface{}) {
		fmt.Println("Worker completed:", args)
	})
	worker.On("active", func(args ...interface{}) {
		fmt.Println("Worker active:", args)
	})
	worker.On("added", func(args ...interface{}) {
		fmt.Println("Worker added:", args)
	})
	worker.On("error", func(args ...interface{}) {
		fmt.Println("Worker error:", args)
	})
	worker.On("failed", func(args ...interface{}) {
		fmt.Println("Worker failed:", args)
	})

	// Run the worker
	if err := worker.Run(); err != nil {
		fmt.Println("Error running worker:", err)
	}

	worker.Wait()

	// Clean up
	worker.Close()
	events.Close()
}
