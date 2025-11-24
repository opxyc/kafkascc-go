import { config } from "@/config.js";
import { logger } from "@/logger.js";
import { makeKafka } from "@/kafka/factory.js";
import { KafkajsConsumerAdapter } from "@/kafka/consumer.adapter.js";
import { LoggingMobEventHandler } from "@/processing/mobevent.handler.js";
import { ConsumerController } from "@/kafka/consumer.controller.js";
import {
  DependencyUnavailableError,
  DBUnavailableError,
  APIUnavailableError,
} from "@/kafka/errors.js";

export class App {
  private controller: ConsumerController;

  constructor() {
    this.controller = new ConsumerController();

    const kafka = makeKafka();
    const handler = new LoggingMobEventHandler();

    // Reader factory: caller can tune per-topic consumer configs
    const makeReader = () =>
      new KafkajsConsumerAdapter(
        kafka.consumer({
          groupId: config.kafka.groupId,
          sessionTimeout: config.kafka.sessionTimeoutMs,
          heartbeatInterval: config.kafka.heartbeatIntervalMs,
        })
      );

    // Register the mob-events topic with autoPauseOnError predicate
    this.controller.register({
      topic: config.kafka.topic,
      handler,
      makeReader,
      runConfig: {
        // Recommended when you manually resolve offsets:
        // eachBatchAutoResolve: false,
        // partitionsConsumedConcurrently: 1,
      },
      options: {
        concurrencyPerPartition: Number(
          process.env["CONCURRENCY_PER_PARTITION"] ?? 16
        ),
        pausePredicate: (err, ctx) => {
          // Pause on typed dependency unavailability errors
          return (
            err instanceof DBUnavailableError ||
            err instanceof APIUnavailableError ||
            err instanceof DependencyUnavailableError
          );
        },
      },
    });
  }

  async start(): Promise<void> {
    await this.controller.start();
    logger.info("[app] started");
  }

  async stop(): Promise<void> {
    await this.controller.shutdown();
    logger.info("[app] stopped");
  }
}
