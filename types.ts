import type { ConsumerRunConfig, ConsumerSubscribeTopics } from "kafkajs";

export interface IKafkaConsumer {
  connect(): Promise<void>;
  subscribe(args: ConsumerSubscribeTopics): Promise<void>;
  run(args: ConsumerRunConfig): Promise<void>;
  stop(): Promise<void>;
  disconnect(): Promise<void>;

  pause(topics: string[]): void;
  resume(topics: string[]): void;

  commitOffsets(
    offsets: Array<{ topic: string; partition: number; offset: string }>
  ): Promise<void>;
}

/**
 * Control surface available to handlers:
 * - Global gate close/open.
 * - Topic-local pause/resume (resume does NOT recreate).
 */
export interface Control {
  // Global gate
  closeGate(reason: string): void;
  openGate(): void;
  isGateOpen(): boolean;

  // Topic-local pause/resume
  pauseReader(reason: string): void;
  resumeReader(): Promise<void>;
  isReaderPaused(): boolean;
}

export type ConsumeContext = {
  topic: string;
  partition: number;
  offset: string;
  key?: string;
  headers: Record<string, string | undefined>;
  receivedAt: string; // ISO time
};

export interface IMessageHandler {
  handle(
    rawValue: string | null,
    ctx: ConsumeContext,
    control: Control
  ): Promise<void>;
}

export type ReaderFactory = () => IKafkaConsumer;

export type ReaderOptions = {
  /**
   * Max concurrent handler executions per partition batch.
   *
   * Default 10.
   */
  concurrencyPerPartition?: number;

  /**
   * Predicate to decide if the reader should auto-pause on a handler error.
   * If returns true, reader pauses (topic-only) and keeps retrying the failing message.
   * On success, reader resumes (no recreation).
   */
  pausePredicate?: (err: unknown, ctx: ConsumeContext) => boolean;
};

export type TopicRegistration = {
  topic: string;
  handler: IMessageHandler;
  makeReader: ReaderFactory;
  /**
   * Optional per-topic run options; we enforce autoCommit=false internally.
   */
  runConfig?: Omit<
    ConsumerRunConfig,
    "eachMessage" | "eachBatch" | "autoCommit"
  >;
  /**
   * Optional reader options for throughput tuning and auto-pause policy.
   */
  options?: ReaderOptions;
};
