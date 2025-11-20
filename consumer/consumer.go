package consumer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

// Consumer handles message consumption from a Kafka topic
type Consumer struct {
	topic         string
	handler       MessageHandler
	reader        Reader
	wg            sync.WaitGroup
	cancel        context.CancelFunc
	once          sync.Once
	brokers       []string
	groupID       string
	gate          *Gate
	lastCommitted map[int]int64
	// processing
	workerCount  int
	backoffBase  time.Duration
	backoffMax   time.Duration
	log          Logger
	pauseDecider PausePredicate
	commitCB     CommitCallback // Callback for when messages are committed
}

// New creates a new consumer.
func New(brokers []string, groupID string, topic string, h MessageHandler, gate *Gate, workerCount int, backoffBase time.Duration, backoffMax time.Duration, log Logger, decider PausePredicate, commitCB CommitCallback) *Consumer {
	// If the handler implements the TopicSetter interface, set the topic
	if ts, ok := h.(TopicSetter); ok {
		ts.SetTopic(topic)
	}

	if workerCount <= 0 {
		workerCount = 1
	}

	if log == nil {
		log = newNopLogger()
	}

	if decider == nil {
		decider = func(err error) bool { return false }
	}

	return &Consumer{
		topic:         topic,
		handler:       h,
		brokers:       brokers,
		groupID:       groupID,
		gate:          gate,
		lastCommitted: make(map[int]int64),
		workerCount:   workerCount,
		backoffBase:   backoffBase,
		backoffMax:    backoffMax,
		log:           log.With("component", "consumer", "topic", topic),
		pauseDecider:  decider,
	}
}

// Start begins consuming messages
func (c *Consumer) Start(ctx context.Context) {
	c.once.Do(func() {
		// Create a cancellable context for this consumer
		ctx, cancel := context.WithCancel(ctx)
		c.cancel = cancel

		c.wg.Add(1)
		go func() {
			defer c.wg.Done()

			for {
				// Respect shutdown
				if ctx.Err() != nil {
					return
				}

				// Wait until gate is healthy
				if c.gate != nil && !c.gate.IsHealthy() {
					if err := c.gate.WaitUntilHealthy(ctx); err != nil {
						return
					}
				}

				c.reader = kafka.NewReader(kafka.ReaderConfig{
					Brokers: c.brokers,
					GroupID: c.groupID,
					Topic:   c.topic,
					// Disable auto-commit; we'll commit only on successful processing
					CommitInterval: 0,
					MinBytes:       10e2,
					MaxBytes:       10e6,
					StartOffset:    kafka.LastOffset,
				})

				// Run consumption until context done or a handler error triggers stop
				c.consume(ctx)

				// Close reader before retrying
				safeCloseReader(c.reader)
				c.reader = nil

				// If shutting down, exit immediately instead of sleeping
				if ctx.Err() != nil {
					return
				}

				// Small backoff to avoid hot loop on persistent errors
				time.Sleep(1 * time.Second)

				// Loop will wait for health again (if unhealthy) and recreate reader
			}
		}()
	})
}

// consume processes messages
func (c *Consumer) consume(ctx context.Context) {
	// Ensure reader is always closed when consumption exits
	defer func() {
		safeCloseReader(c.reader)
		c.reader = nil
	}()

	c.log.Info("consumer start")

	// Create bounded channels to apply backpressure between stages
	jobs := make(chan job, c.workerCount*2)
	results := make(chan result, c.workerCount*2)

	// 1) Start the worker pool that runs the handler concurrently
	var workers sync.WaitGroup
	c.startWorkers(ctx, &workers, jobs, results)

	// 2) Start the commit coordinator which enforces in-order commit per partition
	coord, done, stopCh := c.makeCoordinator(ctx, results)

	// 3) Run the fetch loop (blocks until ctx done or coordinator asks to stop)
	c.fetchLoop(ctx, coord, &workers, jobs, results, done, stopCh)
}

// startWorkers launches goroutines to process jobs and post results
func (c *Consumer) startWorkers(ctx context.Context, workers *sync.WaitGroup, jobs <-chan job, results chan<- result) {
	for i := 0; i < c.workerCount; i++ {
		workers.Add(1)
		go func(id int) {
			defer workers.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case j, ok := <-jobs:
					if !ok {
						return
					}

					func() {
						defer func() {
							if r := recover(); r != nil {
								c.log.Error("handler panicked", "error", r, "topic", c.topic, "partition", j.msg.Partition, "offset", j.msg.Offset)
								results <- result{msg: j.msg, err: fmt.Errorf("handler panicked: %v", r)}
							}
						}()

						// attach trace_id to processing context
						procCtx := withTraceID(ctx, traceID(c.topic, j.msg))
						// Run the actual business handler. The handler itself is responsible
						// for calling the external API and must return api.ErrUnavailable
						// when the API is down so the coordinator can retry/backoff.
						err := c.handler.Handle(procCtx, j.msg.Value)
						if c.pauseDecider != nil && c.pauseDecider(err) && c.gate != nil {
							// Flip the gate to unhealthy to pause new fetches during retry
							c.gate.SetHealthy(false)
						}
						results <- result{msg: j.msg, err: err}
					}()
				}
			}
		}(i)
	}
}

// makeCoordinator creates a Coordinator with a processFn for retry/backoff
func (c *Consumer) makeCoordinator(ctx context.Context, results <-chan result) (*Coordinator, chan struct{}, chan struct{}) {
	// processFn re-invokes the handler during coordinator-managed retries
	processFn := func(procCtx context.Context, msg kafka.Message) error {
		// ensure retries carry the same trace_id
		procCtx = withTraceID(procCtx, traceID(c.topic, msg))
		return c.handler.Handle(procCtx, msg.Value)
	}

	coord := NewCoordinatorWithProcessor(c.topic, c.reader, c.gate, c.cancel, processFn, c.backoffBase, c.backoffMax, c.log, c.pauseDecider, c.commitCB)
	done := make(chan struct{})   // closed when coordinator goroutine exits
	stopCh := make(chan struct{}) // closed to tell fetch loop to stop

	go func() {
		defer close(done)
		for {
			select {
			case <-ctx.Done():
				return
			case r, ok := <-results:
				if !ok {
					return
				}

				// Forward to coordinator; if it returns error, stop this cycle
				if err := coord.Process(ctx, ProcessResult{Msg: r.msg, Err: r.err}); err != nil {
					// Signal fetch loop to unwind; Start loop will recreate the reader
					select {
					case <-stopCh:
					default:
						close(stopCh)
					}
					return
				}
			}
		}
	}()

	return coord, done, stopCh
}

// fetchLoop continuously fetches from Kafka and coordinates message processing
func (c *Consumer) fetchLoop(
	ctx context.Context,
	coord *Coordinator,
	workers *sync.WaitGroup,
	jobs chan<- job,
	results chan result,
	done chan struct{},
	stopCh chan struct{},
) {
	defer func() {
		// Ensure the pipeline drains deterministically
		close(jobs)
		workers.Wait()
		close(results)
		<-done
	}()

	// Track first-seen offsets per partition so the coordinator knows where to start
	initedParts := make(map[int]bool)

	log := c.log

	for {
		select {
		case <-ctx.Done():
			log.Info("consumer-stop", "reason", "ctx_done")
			return
		case <-stopCh:
			// Coordinator requested to stop consumption (e.g., persistent downstream failure);
			// the outer Start loop will recreate the reader after a short backoff.
			log.Info("consumer-stop", "reason", "coordinator_signal")
			return
		default:
			// Pause fetch if gate is unhealthy (set by coordinator on api.ErrUnavailable)
			if c.gate != nil {
				if err := c.gate.WaitUntilHealthy(ctx); err != nil {
					return
				}
			}

			// Fetch next message from Kafka
			msg, err := c.reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Error("fetch-error", "err", err.Error())
				continue
			}

			// Observability: record the exact message we fetched
			log.Info("fetched", "partition", msg.Partition, "offset", msg.Offset, "key", string(msg.Key), "trace_id", traceID(c.topic, msg))

			// Initialize coordinator's expected starting offset for this partition
			if !initedParts[msg.Partition] {
				coord.SetStartOffset(msg.Partition, msg.Offset)
				initedParts[msg.Partition] = true
			}

			// Submit the job to the worker pool (respect shutdown)
			select {
			case <-ctx.Done():
				return
			case jobs <- job{msg: msg}:
			}
		}
	}
}

// Stop shuts down the consumer
func (c *Consumer) Stop() error {
	// Cancel the consumer context first to stop internal loops and in-flight handlers
	if c.cancel != nil {
		c.cancel()
	}

	// Then proactively close reader to unblock any ongoing FetchMessage calls
	if c.reader != nil {
		safeCloseReader(c.reader)
		c.reader = nil
	}

	// Wait for the Start goroutine to exit, but avoid indefinite blocking
	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-time.After(12 * time.Second):
		// Timed out waiting; return an error to signal potential leak
		return fmt.Errorf("consumer stop timeout for topic %s", c.topic)
	}
}
