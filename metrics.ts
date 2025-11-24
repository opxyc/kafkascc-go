import { createServer, Server } from "http";
import { logger } from "@/logger.js";

type Labels = Record<string, string>;

class Counter {
  private value = 0;
  inc(by = 1) {
    this.value += by;
  }
  get() {
    return this.value;
  }
}

class Gauge {
  private value = 0;
  set(v: number) {
    this.value = v;
  }
  inc(by = 1) {
    this.value += by;
  }
  dec(by = 1) {
    this.value -= by;
  }
  get() {
    return this.value;
  }
}

class Histogram {
  private observations: number[] = [];
  observe(v: number) {
    this.observations.push(v);
  }
  getCount() {
    return this.observations.length;
  }
  getSum() {
    return this.observations.reduce((a, b) => a + b, 0);
  }
  getAvg() {
    const c = this.getCount();
    return c === 0 ? 0 : this.getSum() / c;
  }
}

export class Metrics {
  private counters = new Map<string, Counter>();
  private gauges = new Map<string, Gauge>();
  private histograms = new Map<string, Histogram>();

  private key(name: string, labels?: Labels): string {
    if (!labels || Object.keys(labels).length === 0) return name;
    const parts = Object.keys(labels)
      .sort()
      .map((k) => `${k}="${labels[k]}"`)
      .join(",");
    return `${name}{${parts}}`;
  }

  counter(name: string, labels?: Labels): Counter {
    const k = this.key(name, labels);
    if (!this.counters.has(k)) this.counters.set(k, new Counter());
    return this.counters.get(k)!;
  }

  gauge(name: string, labels?: Labels): Gauge {
    const k = this.key(name, labels);
    if (!this.gauges.has(k)) this.gauges.set(k, new Gauge());
    return this.gauges.get(k)!;
  }

  histogram(name: string, labels?: Labels): Histogram {
    const k = this.key(name, labels);
    if (!this.histograms.has(k)) this.histograms.set(k, new Histogram());
    return this.histograms.get(k)!;
  }

  toPrometheus(): string {
    const lines: string[] = [];

    for (const [k, c] of this.counters.entries()) {
      lines.push(`# TYPE ${metricName(k)} counter`);
      lines.push(`${k} ${c.get()}`);
    }
    for (const [k, g] of this.gauges.entries()) {
      lines.push(`# TYPE ${metricName(k)} gauge`);
      lines.push(`${k} ${g.get()}`);
    }
    for (const [k, h] of this.histograms.entries()) {
      lines.push(`# TYPE ${metricName(k)} summary`);
      lines.push(`${k}_count ${h.getCount()}`);
      lines.push(`${k}_sum ${h.getSum()}`);
      lines.push(`${k}_avg ${h.getAvg()}`);
    }

    return lines.join("\n") + "\n";
  }
}

function metricName(k: string): string {
  const idx = k.indexOf("{");
  return idx === -1 ? k : k.substring(0, idx);
}

export class MetricsServer {
  private server: Server | null = null;
  constructor(
    private readonly metrics: Metrics,
    private readonly port: number
  ) {}

  start(): Promise<void> {
    return new Promise((resolve) => {
      this.server = createServer((req, res) => {
        if (req.url === "/metrics") {
          const body = this.metrics.toPrometheus();
          res.statusCode = 200;
          res.setHeader("Content-Type", "text/plain; version=0.0.4");
          return res.end(body);
        }
        res.statusCode = 404;
        res.end("NOT FOUND");
      });
      this.server.listen(this.port, () => {
        logger.info({ port: this.port }, "[metrics] server started");
        resolve();
      });
    });
  }

  async stop(): Promise<void> {
    if (!this.server) return;
    await new Promise<void>((resolve) => this.server!.close(() => resolve()));
    logger.info("[metrics] server stopped");
  }
}
