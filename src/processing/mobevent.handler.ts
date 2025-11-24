import { logger } from "@/logger.js";
import { FedMobileEvent, type MobEvent } from "@/types/fedmobile.js";
import type {
  ConsumeContext,
  IMessageHandler,
  Control,
} from "@/kafka/types.js";
import { DBUnavailableError, APIUnavailableError } from "@/kafka/errors.js";

export class LoggingMobEventHandler implements IMessageHandler {
  async handle(
    rawValue: string | null,
    ctx: ConsumeContext,
    control: Control
  ): Promise<void> {
    let parsed: unknown = rawValue;

    if (rawValue) {
      try {
        parsed = JSON.parse(rawValue);
      } catch {
        // keep as string
      }
    }

    let validated: MobEvent | string | Record<string, unknown> | null = null;

    if (typeof parsed === "object" && parsed !== null) {
      const result = FedMobileEvent.safeParse(parsed);
      validated = result.success
        ? result.data
        : (parsed as Record<string, unknown>);
    } else {
      validated = rawValue;
    }

    // Simulate dependency checks (replace with real logic)
    const dbIsDown = false;
    const apiIsDown = false;
    const sharedDepDown = false;

    if (dbIsDown) throw new DBUnavailableError();
    if (apiIsDown) throw new APIUnavailableError();

    if (sharedDepDown) {
      control.closeGate("shared dependency down");
      throw new Error("Shared downstream unavailable");
    }

    logger.info(
      {
        ctx,
        message: validated,
        gateOpen: control.isGateOpen(),
        readerPaused: control.isReaderPaused(),
      },
      "[consume] mob-events message"
    );
  }
}
