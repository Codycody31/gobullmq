package gobullmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"go.codycody31.dev/gobullmq/internal/lua"
)

// FlowJob defines a job within a flow (dependency tree).
type FlowJob struct {
	Name      string
	QueueName string
	Prefix    string // optional per-node key prefix; defaults to the producer's prefix
	Data      any
	Opts      JobOptions
	Children  []FlowJob
}

// FlowJobResult represents a node of an added or fetched flow.
type FlowJobResult struct {
	Job      *Job[any]
	Children []FlowJobResult
}

// FlowGetOptions configures Flow.
type FlowGetOptions struct {
	ID          string
	QueueName   string
	Prefix      string // defaults to the producer's prefix
	Depth       int    // how many levels of children to fetch (default 10)
	MaxChildren int    // maximum children fetched per node (default 20)
}

// FlowProducerOptions configures the FlowProducer.
type FlowProducerOptions struct {
	Prefix string
}

// FlowProducer creates complex job dependency trees (flows).
type FlowProducer struct {
	client redis.Cmdable
	prefix string
}

// NewFlowProducer creates a new FlowProducer instance.
// The Redis client is externally managed: the producer holds no resources of
// its own, so there is nothing to close.
func NewFlowProducer(client redis.Cmdable, opts *FlowProducerOptions) (*FlowProducer, error) {
	if client == nil {
		return nil, fmt.Errorf("redis client must not be nil")
	}
	prefix := "bull"
	if opts != nil && opts.Prefix != "" {
		prefix = strings.Trim(opts.Prefix, ":")
		if prefix == "" {
			return nil, fmt.Errorf("prefix cannot be empty or just colons")
		}
	}
	return &FlowProducer{
		client: client,
		prefix: prefix,
	}, nil
}

// Add creates a job flow (dependency tree). The whole tree is queued in a
// single Redis transaction like upstream, so a partial failure cannot strand
// a parent in waiting-children.
func (fp *FlowProducer) Add(ctx context.Context, flow FlowJob) (*FlowJobResult, error) {
	results, err := fp.addFlows(ctx, []FlowJob{flow})
	if err != nil {
		return nil, err
	}
	return results[0], nil
}

// AddBulk creates multiple job flows atomically in a single transaction.
func (fp *FlowProducer) AddBulk(ctx context.Context, flows []FlowJob) ([]*FlowJobResult, error) {
	if len(flows) == 0 {
		return nil, nil
	}
	return fp.addFlows(ctx, flows)
}

func (fp *FlowProducer) addFlows(ctx context.Context, flows []FlowJob) ([]*FlowJobResult, error) {
	// Load the script up-front so the queued EVALSHAs cannot hit NOSCRIPT.
	if err := lua.AddJobScript.Load(ctx, fp.client).Err(); err != nil {
		return nil, fmt.Errorf("failed to load addJob script: %w", err)
	}

	pipe := fp.client.TxPipeline()
	var pending []*redis.Cmd
	results := make([]*FlowJobResult, 0, len(flows))
	for _, flow := range flows {
		result, err := fp.addNode(ctx, pipe, &pending, flow, nil)
		if err != nil {
			return nil, err
		}
		results = append(results, &result)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("failed to add flow: %w", err)
	}
	for _, cmd := range pending {
		v, err := cmd.Result()
		if err != nil {
			return nil, fmt.Errorf("failed to add flow node: %w", err)
		}
		if _, err := parseAddJobResult(v); err != nil {
			return nil, err
		}
	}
	return results, nil
}

// addNode queues one node (parent before its children, as upstream does) and
// recursively its children onto the transaction.
func (fp *FlowProducer) addNode(ctx context.Context, pipe redis.Pipeliner, pending *[]*redis.Cmd, node FlowJob, parent *ParentOptions) (FlowJobResult, error) {
	if node.QueueName == "" {
		return FlowJobResult{}, fmt.Errorf("flow job must have a QueueName")
	}
	prefix := node.Prefix
	if prefix == "" {
		prefix = fp.prefix
	}
	keyPrefix := prefix + ":" + node.QueueName + ":"

	opts := node.Opts
	// Flow nodes get UUID v4 ids like upstream (jobId must be known before
	// the transaction executes, so the INCR counter cannot be used).
	jobID := opts.JobID
	if jobID == "" {
		jobID = uuid.NewString()
	} else if jobID == "0" || strings.HasPrefix(jobID, "0:") {
		return FlowJobResult{}, fmt.Errorf("jobId cannot be '0' or start with '0:'")
	}
	if parent != nil {
		opts.Parent = parent
	}
	if len(node.Children) > 0 {
		opts.WaitChildren = true
	}

	// A node without data stores "{}" like upstream (asJSON: undefined -> {}).
	jsonData := []byte("{}")
	if node.Data != nil {
		var err error
		jsonData, err = json.Marshal(node.Data)
		if err != nil {
			return FlowJobResult{}, fmt.Errorf("failed to marshal data for flow job %q: %w", node.Name, err)
		}
	}

	raw, err := newJob(node.Name, string(jsonData), opts)
	if err != nil {
		return FlowJobResult{}, fmt.Errorf("failed to create flow job %q: %w", node.Name, err)
	}

	keys, argv, err := buildAddJobArgs(keyPrefix, raw, jobID)
	if err != nil {
		return FlowJobResult{}, err
	}
	*pending = append(*pending, lua.AddJobScript.Run(ctx, pipe, keys, argv...))

	raw.id = jobID
	raw.setJobContext(fp.client, keyPrefix)
	result := FlowJobResult{Job: &Job[any]{raw: &raw, data: node.Data}}

	if len(node.Children) > 0 {
		parentRef := &ParentOptions{
			ID:    jobID,
			Queue: prefix + ":" + node.QueueName,
		}
		result.Children = make([]FlowJobResult, 0, len(node.Children))
		for _, child := range node.Children {
			childResult, err := fp.addNode(ctx, pipe, pending, child, parentRef)
			if err != nil {
				return FlowJobResult{}, fmt.Errorf("failed to add child flow job %q for parent %q: %w", child.Name, node.Name, err)
			}
			result.Children = append(result.Children, childResult)
		}
	}

	return result, nil
}

// Flow fetches a flow tree rooted at the given job, following processed
// and unprocessed dependencies, mirroring upstream FlowProducer.getFlow.
func (fp *FlowProducer) Flow(ctx context.Context, opts FlowGetOptions) (*FlowJobResult, error) {
	prefix := opts.Prefix
	if prefix == "" {
		prefix = fp.prefix
	}
	depth := opts.Depth
	if depth <= 0 {
		depth = 10
	}
	maxChildren := opts.MaxChildren
	if maxChildren <= 0 {
		maxChildren = 20
	}
	return fp.getNode(ctx, prefix, opts.QueueName, opts.ID, depth, maxChildren)
}

func (fp *FlowProducer) getNode(ctx context.Context, prefix, queueName, jobId string, depth, maxChildren int) (*FlowJobResult, error) {
	keyPrefix := prefix + ":" + queueName + ":"
	raw, err := jobFromId(ctx, fp.client, keyPrefix, jobId)
	if err != nil {
		if errors.Is(err, ErrJobNotFound) {
			return nil, nil
		}
		return nil, err
	}
	raw.setJobContext(fp.client, keyPrefix)
	typed, err := wrapRawJob[any](&raw)
	if err != nil {
		return nil, err
	}
	node := &FlowJobResult{Job: typed}

	if depth > 1 {
		jobKey := keyPrefix + jobId
		pipe := fp.client.Pipeline()
		processedCmd := pipe.HKeys(ctx, jobKey+":processed")
		unprocessedCmd := pipe.SMembers(ctx, jobKey+":dependencies")
		if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
			return nil, err
		}
		// maxChildren caps each dependency type separately, like upstream's
		// per-type counts in getDependencies.
		processed := processedCmd.Val()
		if len(processed) > maxChildren {
			processed = processed[:maxChildren]
		}
		unprocessed := unprocessedCmd.Val()
		if len(unprocessed) > maxChildren {
			unprocessed = unprocessed[:maxChildren]
		}
		childKeys := append(processed, unprocessed...)
		for _, childKey := range childKeys {
			// childKey is "<prefix>:<queueName>:<id>".
			lastColon := strings.LastIndex(childKey, ":")
			if lastColon < 0 {
				continue
			}
			childID := childKey[lastColon+1:]
			rest := childKey[:lastColon]
			midColon := strings.LastIndex(rest, ":")
			if midColon < 0 {
				continue
			}
			childQueue := rest[midColon+1:]
			childPrefix := rest[:midColon]
			child, err := fp.getNode(ctx, childPrefix, childQueue, childID, depth-1, maxChildren)
			if err != nil {
				return nil, err
			}
			if child != nil {
				node.Children = append(node.Children, *child)
			}
		}
	}
	return node, nil
}
