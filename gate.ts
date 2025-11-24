import { logger } from "@/logger.js";

type GateListener = () => void;

export class Gate {
  private open = true;
  private reason: string | null = null;

  private onOpenListeners: GateListener[] = [];
  private onCloseListeners: GateListener[] = [];

  isOpen(): boolean {
    return this.open;
  }

  close(reason: string): void {
    if (!this.open) return;
    this.open = false;
    this.reason = reason;
    logger.warn({ reason }, "[gate] CLOSED");
    this.onCloseListeners.forEach((fn) => safeCall(fn));
  }

  openGate(): void {
    if (this.open) return;
    const prevReason = this.reason;
    this.open = true;
    this.reason = null;
    logger.info({ reason: prevReason }, "[gate] OPENED");
    this.onOpenListeners.forEach((fn) => safeCall(fn));
  }

  currentReason(): string | null {
    return this.reason;
  }

  onOpen(fn: GateListener): void {
    this.onOpenListeners.push(fn);
  }

  onClose(fn: GateListener): void {
    this.onCloseListeners.push(fn);
  }
}

function safeCall(fn: GateListener) {
  try {
    fn();
  } catch (err) {
    logger.error({ err }, "[gate] listener error");
  }
}
