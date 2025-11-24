// src/logger.ts
import pino from "pino";

export const logger = pino({
  level: process.env["LOG_LEVEL"] ?? "info",
  base: null, // no pid/hostname noise
  timestamp: pino.stdTimeFunctions.isoTime,
});
