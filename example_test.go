package gobullmq_test

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
	"go.codycody31.dev/gobullmq"
)

// emailPayload is the typed job data used by the examples.
type emailPayload struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

func ExampleNewQueue() {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

	queue, err := gobullmq.NewQueue[emailPayload]("mail", client, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer queue.Close()

	job, err := queue.Add(context.Background(), "welcome", emailPayload{
		To:      "user@example.com",
		Subject: "Welcome!",
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("added job", job.ID())
}

func ExampleNewWorker() {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

	process := func(ctx context.Context, job *gobullmq.Job[emailPayload]) (string, error) {
		fmt.Printf("sending %q to %s\n", job.Data().Subject, job.Data().To)
		return "sent", nil
	}

	worker, err := gobullmq.NewWorker[emailPayload, string]("mail", client, process, &gobullmq.WorkerOptions{
		Concurrency: 4,
	})
	if err != nil {
		log.Fatal(err)
	}

	worker.OnCompleted(func(job *gobullmq.Job[emailPayload], result string) {
		fmt.Println("completed:", job.ID(), result)
	})
	worker.OnFailed(func(job *gobullmq.Job[emailPayload], err error) {
		fmt.Println("failed:", job.ID(), err)
	})

	if err := worker.Run(context.Background()); err != nil {
		log.Fatal(err)
	}

	// Graceful shutdown: stop fetching, drain in-flight jobs, bounded by ctx.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := worker.Shutdown(shutdownCtx); err != nil {
		log.Fatal(err)
	}
}

func ExampleQueue_Add() {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

	queue, err := gobullmq.NewQueue[emailPayload]("mail", client, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer queue.Close()

	ctx := context.Background()
	job, err := queue.Add(ctx, "digest", emailPayload{
		To:      "user@example.com",
		Subject: "Daily digest",
	},
		gobullmq.AddWithPriority(2),
		gobullmq.AddWithDelay(5*time.Second),
		gobullmq.AddWithAttempts(3),
		gobullmq.AddWithBackoff(gobullmq.BackoffOptions{Type: "exponential", Delay: 500}),
		gobullmq.AddWithRemoveOnComplete(gobullmq.KeepJobs{Count: 100}),
	)
	if err != nil {
		log.Fatal(err)
	}

	state, err := queue.JobState(ctx, job.ID())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(state == gobullmq.JobStateDelayed)
}

func ExampleFlowProducer_Add() {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

	flows, err := gobullmq.NewFlowProducer(client, nil)
	if err != nil {
		log.Fatal(err)
	}

	// The parent stays in waiting-children until both children complete.
	tree, err := flows.Add(context.Background(), gobullmq.FlowJob{
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
	fmt.Println("parent", tree.Job.ID(), "with", len(tree.Children), "children")
}
