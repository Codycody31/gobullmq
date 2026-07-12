//go:build interop

package interop_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"go.codycody31.dev/gobullmq"
)

const interopDeadline = 30 * time.Second

type payload struct {
	Source string `json:"source"`
	Value  int    `json:"value"`
	Nested struct {
		OK bool `json:"ok"`
	} `json:"nested"`
}

type result struct {
	HandledBy string `json:"handledBy"`
	Value     int    `json:"value"`
}

type nodeMessage struct {
	Event     string `json:"event"`
	Direction string `json:"direction"`
	State     string `json:"state"`
}

type nodeProcess struct {
	command *exec.Cmd
	stdout  *bufio.Scanner
	stderr  bytes.Buffer
}

func TestNodeProducerGoWorker(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), interopDeadline)
	defer cancel()

	queueName := newQueueName()
	workerClient := newRedisClient(t, ctx)
	defer closeRedis(t, workerClient)

	worker, err := gobullmq.NewWorker[payload, result](queueName, workerClient, func(ctx context.Context, job *gobullmq.Job[payload]) (result, error) {
		data := job.Data()
		if data.Source != "node" || data.Value != 41 || !data.Nested.OK {
			return result{}, fmt.Errorf("unexpected Node payload: %+v", data)
		}
		if err := job.UpdateProgress(ctx, map[string]any{"stage": "go", "value": data.Value}); err != nil {
			return result{}, fmt.Errorf("update progress: %w", err)
		}
		return result{HandledBy: "go", Value: data.Value + 1}, nil
	}, nil)
	if err != nil {
		t.Fatalf("create Go worker: %v", err)
	}
	worker.OnError(func(err error) {
		t.Logf("Go worker error: %v", err)
	})
	if err := worker.Run(ctx); err != nil {
		t.Fatalf("run Go worker: %v", err)
	}
	defer shutdownWorker(t, worker)

	node := startNode(t, ctx, queueName, "producer")
	message := node.readEvent(t, "result")
	if err := node.wait(); err != nil {
		t.Fatalf("Node producer failed: %v\nstderr:\n%s", err, node.stderr.String())
	}
	if message.Direction != "node-to-go" || message.State != "completed" {
		t.Fatalf("unexpected Node producer result: %+v", message)
	}
}

func TestGoProducerNodeWorker(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), interopDeadline)
	defer cancel()

	queueName := newQueueName()
	queueClient := newRedisClient(t, ctx)
	defer closeRedis(t, queueClient)
	eventsClient := newRedisClient(t, ctx)
	defer closeRedis(t, eventsClient)

	queue, err := gobullmq.NewQueue[payload](queueName, queueClient, nil)
	if err != nil {
		t.Fatalf("create Go queue: %v", err)
	}
	defer func() {
		if err := queue.Close(); err != nil {
			t.Logf("close Go queue: %v", err)
		}
	}()
	queueEvents, err := gobullmq.NewQueueEvents(ctx, queueName, eventsClient, &gobullmq.QueueEventsOptions{
		Autorun:     true,
		LastEventId: "0-0",
	})
	if err != nil {
		t.Fatalf("create Go queue events: %v", err)
	}
	defer func() {
		if err := queueEvents.Close(); err != nil {
			t.Logf("close Go queue events: %v", err)
		}
	}()

	progress := make(chan gobullmq.ProgressEvent, 1)
	queueEvents.OnProgress(func(event gobullmq.ProgressEvent) {
		select {
		case progress <- event:
		default:
		}
	})

	node := startNode(t, ctx, queueName, "worker")
	node.readEvent(t, "ready")

	data := payload{Source: "go", Value: 41}
	data.Nested.OK = true
	job, err := queue.Add(
		ctx,
		"go-produced",
		data,
		gobullmq.AddWithAttempts(2),
		gobullmq.AddWithBackoff(gobullmq.BackoffOptions{Type: "fixed", Delay: 25}),
		gobullmq.AddWithRemoveOnComplete(gobullmq.KeepJobs{Count: -1}),
		gobullmq.AddWithRemoveOnFail(gobullmq.KeepJobs{Count: -1}),
	)
	if err != nil {
		t.Fatalf("add Go job: %v", err)
	}

	got, err := job.WaitUntilFinished(ctx, queueEvents)
	if err != nil {
		t.Fatalf("wait for Node worker: %v\nNode stderr:\n%s", err, node.stderr.String())
	}
	assertResult(t, got, "node", 42)

	select {
	case event := <-progress:
		if event.JobID != job.ID() {
			t.Fatalf("progress job ID = %s, want %s", event.JobID, job.ID())
		}
		assertProgress(t, event.Data, "node", 41)
	case <-ctx.Done():
		t.Fatalf("wait for progress: %v\nNode stderr:\n%s", ctx.Err(), node.stderr.String())
	}

	message := node.readEvent(t, "result")
	if err := node.wait(); err != nil {
		t.Fatalf("Node worker failed: %v\nstderr:\n%s", err, node.stderr.String())
	}
	if message.Direction != "go-to-node" || message.State != "completed" {
		t.Fatalf("unexpected Node worker result: %+v", message)
	}
}

func TestClusterKeySlotContract(t *testing.T) {
	if redisMode() != "cluster" {
		t.Skip("Redis Cluster only")
	}
	ctx, cancel := context.WithTimeout(context.Background(), interopDeadline)
	defer cancel()
	client := newRedisClient(t, ctx)
	defer closeRedis(t, client)

	queueName := newQueueName()
	suffixes := []string{
		"", "1", "1:dependencies", "1:lock", "1:logs", "1:processed",
		"active", "completed", "delayed", "events", "failed", "id", "limiter",
		"marker", "meta", "metrics:completed", "metrics:failed", "paused", "pc",
		"prioritized", "repeat", "stalled-check", "wait", "waiting-children",
	}
	var expected int64 = -1
	for _, suffix := range suffixes {
		key := "bull:" + queueName
		if suffix != "" {
			key += ":" + suffix
		}
		slot, err := client.Do(ctx, "CLUSTER", "KEYSLOT", key).Int64()
		if err != nil {
			t.Fatalf("CLUSTER KEYSLOT %s: %v", key, err)
		}
		if expected == -1 {
			expected = slot
		} else if slot != expected {
			t.Fatalf("key %s uses slot %d, want %d", key, slot, expected)
		}
	}
}

func startNode(t *testing.T, ctx context.Context, queueName, mode string) *nodeProcess {
	t.Helper()
	script, err := filepath.Abs(filepath.Join("..", "node", "interop.cjs"))
	if err != nil {
		t.Fatalf("resolve Node harness: %v", err)
	}
	command := exec.CommandContext(ctx, "node", script, mode)
	command.Env = append(os.Environ(),
		"QUEUE_NAME="+queueName,
		"REDIS_MODE="+redisMode(),
		"REDIS_ADDRS="+strings.Join(redisAddresses(), ","),
	)
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatalf("open Node stdout: %v", err)
	}
	process := &nodeProcess{command: command, stdout: bufio.NewScanner(stdout)}
	command.Stderr = &process.stderr
	if err := command.Start(); err != nil {
		t.Fatalf("start Node %s: %v", mode, err)
	}
	return process
}

func (process *nodeProcess) readEvent(t *testing.T, wanted string) nodeMessage {
	t.Helper()
	for process.stdout.Scan() {
		line := process.stdout.Bytes()
		var message nodeMessage
		if err := json.Unmarshal(line, &message); err != nil {
			t.Fatalf("decode Node output %q: %v\nstderr:\n%s", line, err, process.stderr.String())
		}
		if message.Event == wanted {
			return message
		}
	}
	if err := process.stdout.Err(); err != nil {
		t.Fatalf("read Node output: %v\nstderr:\n%s", err, process.stderr.String())
	}
	err := process.wait()
	t.Fatalf("Node exited before %q event: %v\nstderr:\n%s", wanted, err, process.stderr.String())
	return nodeMessage{}
}

func (process *nodeProcess) wait() error {
	if err := process.stdout.Err(); err != nil {
		return err
	}
	return process.command.Wait()
}

func newRedisClient(t *testing.T, ctx context.Context) redis.UniversalClient {
	t.Helper()
	var client redis.UniversalClient
	if redisMode() == "cluster" {
		client = redis.NewClusterClient(&redis.ClusterOptions{Addrs: redisAddresses()})
	} else {
		client = redis.NewClient(&redis.Options{Addr: redisAddresses()[0]})
	}
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		t.Fatalf("ping Redis %s: %v", redisMode(), err)
	}
	return client
}

func closeRedis(t *testing.T, client redis.UniversalClient) {
	t.Helper()
	if err := client.Close(); err != nil {
		t.Logf("close Redis client: %v", err)
	}
}

func shutdownWorker(t *testing.T, worker *gobullmq.Worker[payload, result]) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := worker.Shutdown(ctx); err != nil {
		t.Logf("shutdown Go worker: %v", err)
	}
}

func redisMode() string {
	mode := os.Getenv("REDIS_MODE")
	if mode == "" {
		return "standalone"
	}
	return mode
}

func redisAddresses() []string {
	if value := os.Getenv("REDIS_ADDRS"); value != "" {
		return strings.Split(value, ",")
	}
	if redisMode() == "cluster" {
		addresses := make([]string, 0, 6)
		for port := 7000; port <= 7005; port++ {
			addresses = append(addresses, "127.0.0.1:"+strconv.Itoa(port))
		}
		return addresses
	}
	return []string{"127.0.0.1:6390"}
}

func newQueueName() string {
	return "{gobullmq-ci-" + uuid.NewString() + "}"
}

func assertResult(t *testing.T, value any, handledBy string, number int) {
	t.Helper()
	object, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("result type = %T, want map[string]any", value)
	}
	if object["handledBy"] != handledBy || object["value"] != float64(number) {
		t.Fatalf("result = %#v, want handledBy=%s value=%d", object, handledBy, number)
	}
}

func assertProgress(t *testing.T, value any, stage string, number int) {
	t.Helper()
	object, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("progress type = %T, want map[string]any", value)
	}
	if object["stage"] != stage || object["value"] != float64(number) {
		t.Fatalf("progress = %#v, want stage=%s value=%d", object, stage, number)
	}
}
