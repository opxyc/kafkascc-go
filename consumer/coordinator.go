package consumer

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
)

// Coordinator manages message processing and commit ordering
type Coordinator struct {
	topic        string
	committer    MessageCommitter
	gate         *Gate
	cancel       context.CancelFunc
	log          Logger
	pauseDecider PausePredicate

	partitions  map[int]*partState
	starts      map[int]int64
	processFn   func(ctx context.Context, msg kafka.Message) error
	commitCB    CommitCallback // Callback for when messages are committed
	
	// retry/backoff settings
	baseBackoff time.Duration
	maxBackoff  time.Duration
}

// NewCoordinatorWithProcessor creates a coordinator with a custom processing function and commit callback
func NewCoordinatorWithProcessor(topic string, committer MessageCommitter, gate *Gate, cancel context.CancelFunc, processFn func(ctx context.Context, msg kafka.Message) error, baseBackoff time.Duration, maxBackoff time.Duration, log Logger, decider PausePredicate, commitCB CommitCallback) *Coordinator {
	return &Coordinator{
		topic:        topic,
		committer:    committer,
		gate:         gate,
		cancel:       cancel,
		log:          log.With("component", "coordinator", "topic", topic),
		pauseDecider: decider,
		partitions:   make(map[int]*partState),
		starts:       make(map[int]int64),
		processFn:    processFn,
		commitCB:     commitCB,
		baseBackoff:  baseBackoff,
		maxBackoff:   maxBackoff,
	}
}

func NewCoordinator(topic string, committer MessageCommitter, gate *Gate, cancel context.CancelFunc, log Logger, decider PausePredicate, commitCB CommitCallback) *Coordinator {
	return NewCoordinatorWithProcessor(topic, committer, gate, cancel, nil, 500*time.Millisecond, 10*time.Second, log, decider, commitCB)
}

// SetBaseBackoff sets the base retry delay
func (c *Coordinator) SetBaseBackoff(baseBackoff time.Duration) {
	c.baseBackoff = baseBackoff
}

// SetMaxBackoff sets the maximum retry delay
func (c *Coordinator) SetMaxBackoff(maxBackoff time.Duration) {
	c.maxBackoff = maxBackoff
}

// SetStartOffset sets the initial offset for a partition
func (c *Coordinator) SetStartOffset(partition int, offset int64) {
	c.starts[partition] = offset
}

// Process handles a message processing result
func (c *Coordinator) Process(ctx context.Context, r ProcessResult) error {
	p := r.Msg.Partition
	st, exists := c.partitions[p]
	if !exists {
		next := r.Msg.Offset
		if start, ok := c.starts[p]; ok {
			next = start
		}
		st = &partState{next: next, buf: make(map[int64]ProcessResult)}
		c.partitions[p] = st
	}

	st.buf[r.Msg.Offset] = r

	log := c.log.With("partition", p)

	// try commit in-order
	for {
		res, ok := st.buf[st.next]
		if !ok {
			break
		}

		if res.Err != nil {
			// If API is unavailable, pause globally and retry this exact message until success.
			shouldPause := c.pauseDecider != nil && c.pauseDecider(res.Err)

			if shouldPause {
				log.Error("api-down: pausing and retrying", "event", "api_down", "offset", st.next, "trace_id", traceID(c.topic, res.Msg), "err", res.Err.Error(), "err_source", "handler")
				if c.gate != nil {
					c.gate.SetHealthy(false)
				}

				// Exponential backoff retry loop with jitter
				backoff := c.baseBackoff
				attempt := 1

				for {
					// Respect context cancellation
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(applyJitter(backoff)):
					}

					if err := c.processFn(ctx, res.Msg); err == nil {
						// Success: mark healthy and proceed to commit
						if c.gate != nil {
							// Delay flipping healthy using a ctx-aware timer to avoid leaks on shutdown
							t := time.NewTimer(backoff)
							go func() {
								defer t.Stop()
								select {
								case <-ctx.Done():
									return
								case <-t.C:
									c.gate.SetHealthy(true)
								}
							}()
						}

						log.Info("retry-success", "event", "retry_success", "offset", st.next, "attempts", attempt, "trace_id", traceID(c.topic, res.Msg))
						res.Err = nil
						st.buf[st.next] = res
						break
					} else if c.pauseDecider != nil && c.pauseDecider(err) {
						// Keep pausing; increase backoff only for pause-eligible errors
						log.Info("retry", "event", "retry", "offset", st.next, "attempt", attempt, "backoff", backoff.String(), "trace_id", traceID(c.topic, res.Msg))
						if backoff < c.maxBackoff {
							backoff *= 2
							if backoff > c.maxBackoff {
								backoff = c.maxBackoff
							}
						}
						attempt++
						continue
					} else {
						// Non-pausing error and no retry: do NOT stop consumption when predicate is nil/false.
						// Log and proceed to commit this message to avoid blocking the pipeline.
						log.Error("non-pausing-error", "event", "non_pausing_error", "offset", st.next, "trace_id", traceID(c.topic, res.Msg), "err", res.Err.Error())
						res.Err = nil
						st.buf[st.next] = res
						// fall through to commit below
					}
				}
			} else {
				// Non-pausing error and no retry: do NOT stop consumption when predicate is nil/false.
				// Log and proceed to commit this message to avoid blocking the pipeline.
				log.Error("non-pausing-error", "event", "non_pausing_error", "offset", st.next, "trace_id", traceID(c.topic, res.Msg), "err", res.Err.Error())
				res.Err = nil
				st.buf[st.next] = res
				// fall through to commit below
			}
		}

		// commit this offset with acknowledgment
		err := c.committer.CommitMessages(ctx, res.Msg)
		if err != nil {
			log.Error("commit-error", "event", "commit_error", "offset", res.Msg.Offset, "trace_id", traceID(c.topic, res.Msg), "err", err.Error())
			// Call the commit callback with the error
			if c.commitCB != nil {
				c.commitCB(res.Msg, err)
			}
			return err
		}

		log.Info("committed", "event", "committed", "offset", res.Msg.Offset, "trace_id", traceID(c.topic, res.Msg))
		
		// Update last heartbeat timestamp to indicate we're making progress
		if consumer, ok := c.committer.(interface{ UpdateHeartbeat() }); ok {
			consumer.UpdateHeartbeat()
		}
		
		// Call the commit callback on successful commit
		if c.commitCB != nil {
			c.commitCB(res.Msg, nil)
		}
		delete(st.buf, st.next)
		st.next++
	}

	return nil
}
