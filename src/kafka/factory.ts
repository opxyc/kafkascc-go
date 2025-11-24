import { Kafka, logLevel, type SASLOptions, type KafkaConfig } from "kafkajs";
import { config } from "@/config.js";

export function makeKafka(): Kafka {
  const sasl: SASLOptions | undefined = config.kafka.sasl
    ? {
        mechanism: config.kafka.sasl.mechanism,
        username: config.kafka.sasl.username,
        password: config.kafka.sasl.password,
      }
    : undefined;

  const base: KafkaConfig = {
    clientId: config.kafka.clientId,
    brokers: config.kafka.brokers,
    ssl: config.kafka.ssl,
    logLevel: logLevel.INFO,
    retry: {
      initialRetryTime: config.kafka.retryInitialMs,
      retries: config.kafka.retryMaxRetries,
    },
  };

  const kafkaConfig: KafkaConfig = sasl ? { ...base, sasl } : base;
  return new Kafka(kafkaConfig);
}
