import type { EachBatchPayload } from "kafkajs";
import { logger } from "@/logger.js";
import type {
  ConsumeContext,
  IKafkaConsumer,
  IMessageHandler,
  ReaderFactory,
  ReaderOptions,
} from "@/kafka/types.js";
import type { Gate } from "@/kafka/gate.js";

const sleep = (ms: number) => new Promise((res) => setTimeout(res, ms));
const jitter = (ms: number) => Math.floor(ms * (0.7 + Math.random() * 0.6));

const nullPausePredicate = (_: unknown, __: ConsumeContext) => false;

/**
 * Reader manages a single topic:
 * - Manual commit with strict per-partition ordering.
 * - Batch-level concurrency per partition.
 * - Local pause/resume via predicate or explicit control.
 * - Responds to global gate by pausing; on gate open, reader is recreated.
 */
export class Reader {
  private consumer: IKafkaConsumer | null = null;
  private runStarted = false;

  // Local pause state
  private readerPaused = false;
  private pauseReason: string | null = null;

  // Restart guards
  private recreateInProgress = false;

  private readonly concurrencyPerPartition: number;
  private readonly pausePredicate: (
    err: unknown,
    ctx: ConsumeContext
  ) => boolean;

  constructor(
    private readonly topic: string,
    private readonly handler: IMessageHandler,
    private readonly makeReader: ReaderFactory,
    private readonly gate: Gate,
    private readonly runConfig?: Omit<
      import("kafkajs").ConsumerRunConfig,
      "eachMessage" | "eachBatch" | "autoCommit"
    >,
    options?: ReaderOptions
  ) {
    this.concurrencyPerPartition = Math.max(
      1,
      options?.concurrencyPerPartition ?? 10
    );
    this.pausePredicate = options?.pausePredicate ?? nullPausePredicate;

    // React to global gate transitions
    this.gate.onClose(() => {
      this.pauseInternal("[gate] closed");
    });
    this.gate.onOpen(() => {
      // On global resume, we recreate the reader to rejoin and rebalance
      void this.recreate("[gate] opened");
    });
  }

  async start(): Promise<void> {
    if (this.runStarted) return;
    await this.createAndRun();
    this.runStarted = true;
    logger.info(
      {
        topic: this.topic,
        concurrencyPerPartition: this.concurrencyPerPartition,
      },
      "[reader] run loop started"
    );
  }

  async stop(): Promise<void> {
    if (!this.consumer) return;
    try {
      await this.consumer.stop();
    } catch (err) {
      logger.error({ err, topic: this.topic }, "[reader] stop() error");
    }
    try {
      await this.consumer.disconnect();
    } catch (err) {
      logger.error({ err, topic: this.topic }, "[reader] disconnect() error");
    }
    this.consumer = null;
    this.runStarted = false;
    logger.info({ topic: this.topic }, "[reader] stopped");
  }

  /** Pauses only this topic; used for local dependency failures */
  pause(reason?: string): void {
    this.pauseReason = reason ?? this.pauseReason;
    this.pauseInternal(this.pauseReason ?? "local pause");
  }

  private pauseInternal(reason: string): void {
    if (this.readerPaused) return;
    this.readerPaused = true;
    try {
      this.consumer?.pause([this.topic]);
      logger.warn({ topic: this.topic, reason }, "[reader] paused");
    } catch (err) {
      logger.error({ err, topic: this.topic }, "[reader] pause error");
    }
  }

  /** Local resume: do NOT recreate; just resume the consumer for this topic. */
  async resume(): Promise<void> {
    if (!this.readerPaused) return;
    try {
      this.consumer?.resume([this.topic]);
      const reason = this.pauseReason;
      this.readerPaused = false;
      this.pauseReason = null;
      logger.info(
        { topic: this.topic, reason },
        "[reader] resumed (no recreate)"
      );
    } catch (err) {
      logger.error({ err, topic: this.topic }, "[reader] resume error");
    }
  }

  /** Global: recreate the reader (fresh join & rebalance) */
  async recreate(reason: string): Promise<void> {
    if (this.recreateInProgress) {
      logger.warn(
        { topic: this.topic, reason },
        "[reader] recreate already in progress"
      );
      return;
    }
    this.recreateInProgress = true;
    logger.info({ topic: this.topic, reason }, "[reader] recreating");

    // Stop the current consumer
    await this.stop();

    // Clear local pause flags before starting fresh
    this.readerPaused = false;
    this.pauseReason = null;

    // Create and run a brand-new consumer
    await this.createAndRun();

    this.recreateInProgress = false;
    logger.info({ topic: this.topic }, "[reader] recreated & running");
  }

  private async createAndRun(): Promise<void> {
    this.consumer = this.makeReader();
    await this.consumer.connect();
    await this.consumer.subscribe({
      topics: [this.topic],
      fromBeginning: false,
    });

    // Fire-and-forget run; KafkaJS manages lifecycle; stop() will end run.
    void this.consumer.run({
      autoCommit: false,
      // Recommended if you manually resolve offsets:
      // eachBatchAutoResolve: false, // include if needed via runConfig
      ...this.runConfig,
      eachBatch: async (payload: EachBatchPayload) => this.eachBatch(payload),
    });
  }

  /** Control for handlers */
  makeControl(): import("@/kafka/types.js").Control {
    return {
      // Global gate control
      closeGate: (reason: string) => this.gate.close(reason),
      openGate: () => this.gate.openGate(),
      isGateOpen: () => this.gate.isOpen(),

      // Local pause/resume
      pauseReader: (reason: string) => this.pause(reason),
      resumeReader: async () => this.resume(),
      isReaderPaused: () => this.readerPaused,
    };
  }

  private normalizeHeaders(
    headers: EachBatchPayload["batch"]["messages"][number]["headers"]
  ) {
    const headersObj: Record<string, string | undefined> = Object.fromEntries(
      Object.entries(headers ?? {}).map(([key, value]) => {
        if (value === undefined) return [key, undefined];
        if (Array.isArray(value)) {
          const parts = value.map((x) =>
            typeof x === "string" ? x : x.toString("utf8")
          );
          return [key, parts.join(",")];
        }
        return [
          key,
          typeof value === "string" ? value : value.toString("utf8"),
        ];
      })
    );
    return headersObj;
  }

  /**
   * High-throughput batch handler with ordered commit:
   * - Run up to `concurrencyPerPartition` handler tasks concurrently.
   * - Each task retries until success (exponential backoff with jitter).
   * - After ALL messages in the batch succeed, resolve offsets IN ORDER and commit.
   *   This guarantees N is committed before N+1.
   * - If autoPauseOnError(err, ctx) returns true, reader pauses (topic-only) and keeps retrying.
   *   On success, reader resumes (no recreate).
   */
  private async eachBatch(payload: EachBatchPayload): Promise<void> {
    const {
      batch,
      resolveOffset,
      heartbeat,
      isRunning,
      isStale,
      commitOffsetsIfNecessary,
    } = payload;
    const { topic, partition } = batch;

    if (!isRunning() || isStale()) return;

    // Sort by ascending offset for ordered commit
    const messagesSorted = [...batch.messages].sort((a, b) =>
      Number(BigInt(a.offset) - BigInt(b.offset))
    );

    // Worker that retries the given message until success.
    const makeWorker =
      (message: (typeof messagesSorted)[number]) => async () => {
        const keyStr = message.key?.toString();
        const headersObj = this.normalizeHeaders(message.headers);

        const ctx = {
          topic,
          partition,
          offset: message.offset,
          headers: headersObj,
          receivedAt: new Date().toISOString(),
          ...(keyStr !== undefined ? { key: keyStr } : {}),
        };

        let attempt = 0;
        const maxBackoffMs = 30000;

        // Retry until success
        while (true) {
          if (!isRunning() || isStale()) return;

          try {
            const raw = message.value?.toString() ?? null;

            await this.handler.handle(raw, ctx, this.makeControl());

            // If locally paused earlier, a success should resume (no recreate)
            if (this.readerPaused) {
              logger.info(
                { topic, partition, offset: message.offset },
                "[reader] success after local pause -> resuming"
              );
              await this.resume();
            }

            await heartbeat();
            return; // success
          } catch (err) {
            attempt++;
            const backoffMs = Math.min(
              maxBackoffMs,
              1000 * Math.pow(2, attempt)
            );
            const waitMs = jitter(backoffMs);

            // Auto local pause based on caller-provided predicate
            const shouldAutoPause =
              typeof this.pausePredicate === "function"
                ? this.pausePredicate(err, ctx)
                : false;

            if (shouldAutoPause) {
              this.pause(
                `auto-pause: ${
                  err instanceof Error ? err.message : String(err)
                }`
              );
            }

            logger.error(
              {
                err,
                attempt,
                topic,
                partition,
                offset: message.offset,
                gateState: this.gate.isOpen() ? "OPEN" : "CLOSED",
                readerPaused: this.readerPaused,
                pauseReason: this.pauseReason,
              },
              "[reader] handler failed; retrying same message"
            );

            // Maintain session
            await heartbeat();
            await sleep(waitMs);

            // Loop continues; do NOT resolve/commit until success.
          }
        }
      };

    // Run batch with bounded concurrency per partition.
    await runWithConcurrency(
      messagesSorted,
      this.concurrencyPerPartition,
      makeWorker
    );

    // After every message in this batch has succeeded, resolve offsets in order and commit.
    for (const m of messagesSorted) {
      resolveOffset(m.offset); // KafkaJS will commit "next" internally
    }
    await commitOffsetsIfNecessary();

    logger.debug(
      {
        topic,
        partition,
        resolvedThrough:
          messagesSorted[messagesSorted.length - 1]?.offset ?? "n/a",
        count: messagesSorted.length,
      },
      "[reader] batch resolved+committed in-order"
    );
  }
}

/** Simple FIFO concurrency runner. */
async function runWithConcurrency<T>(
  items: T[],
  concurrency: number,
  taskFactory: (item: T) => () => Promise<void>
): Promise<void> {
  const queue = items.map((item) => taskFactory(item));
  const workers = Math.max(1, concurrency);
  const running: Promise<void>[] = [];

  // Prime initial workers
  for (let i = 0; i < workers && queue.length > 0; i++) {
    const task = queue.shift()!;
    running.push(
      task().then(async () => {
        // Pull next tasks until queue empties
        while (queue.length > 0) {
          const next = queue.shift()!;
          await next();
        }
      })
    );
  }

  await Promise.all(running);
}
