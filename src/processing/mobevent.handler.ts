import { APIUnavailableError } from "@/kafka/errors.js";
import type {
  ConsumeContext,
  Control,
  IMessageHandler,
} from "@/kafka/types.js";
import { logger } from "@/logger.js";
import { FedMobileEvent, type MobEvent } from "@/types/fedmobile.js";
import fetch, { type RequestInit } from "node-fetch";

const API_URL = "http://localhost:8090/events";

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

    try {
      // Send event to API
      const fetchOptions: RequestInit = {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(validated),
      };

      const response = await fetch(API_URL, {
        ...fetchOptions,
      });

      if (!response.ok) {
        throw new Error(`API responded with status ${response.status}`);
      }
    } catch (error) {
      logger.error(
        { error, url: API_URL, event: validated },
        "Failed to send event to API"
      );
      throw new APIUnavailableError("Failed to send event to API");
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
