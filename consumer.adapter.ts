import type {
  Consumer,
  ConsumerRunConfig,
  ConsumerSubscribeTopics,
} from "kafkajs";
import type { IKafkaConsumer } from "@/kafka/types.js";

export class KafkajsConsumerAdapter implements IKafkaConsumer {
  constructor(private readonly consumer: Consumer) {}

  connect() {
    return this.consumer.connect();
  }
  subscribe(args: ConsumerSubscribeTopics) {
    return this.consumer.subscribe(args);
  }
  run(args: ConsumerRunConfig) {
    return this.consumer.run(args);
  }
  stop() {
    return this.consumer.stop();
  }
  disconnect() {
    return this.consumer.disconnect();
  }
  pause(topics: string[]) {
    return this.consumer.pause(topics.map((topic) => ({ topic })));
  }
  resume(topics: string[]) {
    return this.consumer.resume(topics.map((topic) => ({ topic })));
  }
  commitOffsets(
    offsets: Array<{ topic: string; partition: number; offset: string }>
  ) {
    return this.consumer.commitOffsets(offsets);
  }
}
