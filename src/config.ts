import "dotenv/config";

export type KafkaAuthConfig = {
  ssl: boolean;
  sasl?:
    | {
        mechanism: "plain" | "scram-sha-256" | "scram-sha-512";
        username: string;
        password: string;
      }
    | undefined;
};

export type AppConfig = {
  kafka: {
    brokers: string[];
    clientId: string;
    groupId: string;
    topic: string;
    sessionTimeoutMs: number;
    heartbeatIntervalMs: number;
    maxBytesPerPartition: number;
    retryInitialMs: number;
    retryMaxRetries: number;
  } & KafkaAuthConfig;
};

function boolEnv(name: string, def = false): boolean {
  const v = process.env[name];
  if (!v) return def;
  return ["true", "1", "yes", "y"].includes(v.toLowerCase());
}

export const config: AppConfig = {
  kafka: {
    brokers: (process.env["KAFKA_BROKERS"] ?? "localhost:9092")
      .split(",")
      .map((b) => b.trim())
      .filter(Boolean),
    clientId: process.env["KAFKA_CLIENT_ID"] ?? "mob-events-consumer",
    groupId: process.env["KAFKA_GROUP_ID"] ?? "mob-events-consumer-group",
    topic: process.env["KAFKA_TOPIC"] ?? "mob-events",
    sessionTimeoutMs: Number(process.env["KAFKA_SESSION_TIMEOUT_MS"] ?? 30000),
    heartbeatIntervalMs: Number(
      process.env["KAFKA_HEARTBEAT_INTERVAL_MS"] ?? 3000
    ),
    maxBytesPerPartition: Number(
      process.env["KAFKA_MAX_BYTES_PER_PARTITION"] ?? 1048576
    ),
    retryInitialMs: Number(process.env["KAFKA_RETRY_INITIAL_MS"] ?? 1000),
    retryMaxRetries: Number(process.env["KAFKA_RETRY_MAX_RETRIES"] ?? 10),
    ssl: boolEnv("KAFKA_SSL", false),
    sasl: (process.env["KAFKA_SASL_MECHANISM"] ?? "").trim()
      ? {
          mechanism: process.env["KAFKA_SASL_MECHANISM"] as
            | "plain"
            | "scram-sha-256"
            | "scram-sha-512",
          username: process.env["KAFKA_SASL_USERNAME"] ?? "",
          password: process.env["KAFKA_SASL_PASSWORD"] ?? "",
        }
      : undefined,
  },
};
