import { logger } from "@/logger.js";
import { Gate } from "@/kafka/gate.js";
import { Reader } from "@/kafka/reader.js";
import type { TopicRegistration } from "@/kafka/types.js";

/**
 * Orchestrates multiple topic readers:
 * - Global gate: on close -> pause all readers; on open -> recreate all readers.
 * - Registration must happen before start().
 */
export class ConsumerController {
  private readonly gate = new Gate();
  private readonly readers: Map<string, Reader> = new Map(); // topic -> reader
  private started = false;

  register(reg: TopicRegistration): void {
    if (this.started) throw new Error("Cannot register after start");
    if (this.readers.has(reg.topic))
      throw new Error(`Topic '${reg.topic}' already registered`);

    const reader = new Reader(
      reg.topic,
      reg.handler,
      reg.makeReader,
      this.gate,
      reg.runConfig,
      reg.options
    );
    this.readers.set(reg.topic, reader);
    logger.info({ topic: reg.topic }, "[controller] registered reader");
  }

  async start(): Promise<void> {
    if (this.started) return;
    this.started = true;

    logger.info(
      { topics: [...this.readers.keys()] },
      "[controller] starting all readers"
    );
    for (const [, reader] of this.readers) {
      await reader.start();
    }
    logger.info("[controller] all readers started");
  }

  async shutdown(): Promise<void> {
    logger.info("[controller] shutdown initiated");
    for (const [, reader] of this.readers) {
      await reader.stop();
    }
    logger.info("[controller] shutdown complete");
  }

  /** Expose helpers for external control/testing if needed */
  isGateOpen(): boolean {
    return this.gate.isOpen();
  }
  closeGate(reason: string): void {
    this.gate.close(reason);
  }
  openGate(): void {
    this.gate.openGate();
  }

  /** Optional: direct reader control by topic (useful for tests) */
  async recreateReader(topic: string, reason: string): Promise<void> {
    const reader = this.readers.get(topic);
    if (!reader) throw new Error(`Reader for topic '${topic}' not found`);
    await reader.recreate(reason);
  }
}
