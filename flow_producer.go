package gobullmq

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/redis/go-redis/v9"
)

// FlowJob defines a job within a flow (dependency tree).
type FlowJob struct {
	Name      string
	QueueName string
	Data      interface{}
	Opts      JobOptions
	Children  []FlowJob
}

// FlowJobResult represents the result of adding a flow.
type FlowJobResult struct {
	Job      *Job[any]
	Children []FlowJobResult
}

// FlowProducerOptions configures the FlowProducer.
type FlowProducerOptions struct {
	Prefix string
}

// FlowProducer creates complex job dependency trees (flows).
type FlowProducer struct {
	client redis.Cmdable
	prefix string
	mu     sync.Mutex
	queues map[string]*Queue[any]
}

// NewFlowProducer creates a new FlowProducer instance.
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
		queues: make(map[string]*Queue[any]),
	}, nil
}

// Add creates a job flow (dependency tree).
func (fp *FlowProducer) Add(ctx context.Context, flow FlowJob) (*FlowJobResult, error) {
	result, err := fp.addFlow(ctx, flow, nil)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// AddBulk creates multiple job flows.
func (fp *FlowProducer) AddBulk(ctx context.Context, flows []FlowJob) ([]*FlowJobResult, error) {
	results := make([]*FlowJobResult, 0, len(flows))
	for _, flow := range flows {
		result, err := fp.Add(ctx, flow)
		if err != nil {
			return results, err
		}
		results = append(results, result)
	}
	return results, nil
}

// Close cleans up the FlowProducer and its cached queue instances.
func (fp *FlowProducer) Close(ctx context.Context) error {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	var errs []error
	for _, q := range fp.queues {
		if err := q.Close(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	fp.queues = nil
	return errors.Join(errs...)
}

// addFlow recursively adds a flow job and its children.
func (fp *FlowProducer) addFlow(ctx context.Context, flow FlowJob, parentOpts *ParentOpts) (FlowJobResult, error) {
	if flow.QueueName == "" {
		return FlowJobResult{}, fmt.Errorf("flow job must have a QueueName")
	}

	q, err := fp.getQueue(flow.QueueName)
	if err != nil {
		return FlowJobResult{}, err
	}

	if len(flow.Children) == 0 {
		addOpts := buildFlowAddOpts(flow.Opts, parentOpts)
		job, err := q.Add(ctx, flow.Name, flow.Data, addOpts...)
		if err != nil {
			return FlowJobResult{}, fmt.Errorf("failed to add leaf flow job %q: %w", flow.Name, err)
		}
		return FlowJobResult{Job: job}, nil
	}

	parentFlowOpts := flow.Opts
	parentFlowOpts.WaitChildren = true
	addOpts := buildFlowAddOpts(parentFlowOpts, parentOpts)
	parentJob, err := q.Add(ctx, flow.Name, flow.Data, addOpts...)
	if err != nil {
		return FlowJobResult{}, fmt.Errorf("failed to add parent flow job %q: %w", flow.Name, err)
	}

	parentRef := &ParentOpts{
		ID:    parentJob.ID(),
		Queue: fp.prefix + ":" + flow.QueueName,
	}

	childResults := make([]FlowJobResult, 0, len(flow.Children))
	for _, child := range flow.Children {
		childResult, err := fp.addFlow(ctx, child, parentRef)
		if err != nil {
			return FlowJobResult{}, fmt.Errorf("failed to add child flow job %q for parent %q: %w", child.Name, flow.Name, err)
		}
		childResults = append(childResults, childResult)
	}

	return FlowJobResult{
		Job:      parentJob,
		Children: childResults,
	}, nil
}

// buildFlowAddOpts creates AddOption slice from JobOptions and optional parent.
func buildFlowAddOpts(opts JobOptions, parentOpts *ParentOpts) []AddOption {
	addOpts := optsToAddOptions(opts)
	if parentOpts != nil {
		addOpts = append(addOpts, AddWithParent(*parentOpts))
	}
	return addOpts
}

// getQueue returns a cached Queue instance for the given name.
func (fp *FlowProducer) getQueue(name string) (*Queue[any], error) {
	fp.mu.Lock()
	defer fp.mu.Unlock()

	if q, ok := fp.queues[name]; ok {
		return q, nil
	}
	q, err := NewQueue[any](name, fp.client, &QueueOptions{Prefix: fp.prefix})
	if err != nil {
		return nil, fmt.Errorf("failed to create queue %q for flow: %w", name, err)
	}
	fp.queues[name] = q
	return q, nil
}
