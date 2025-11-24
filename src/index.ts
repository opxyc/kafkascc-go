import { App } from "./app.js";
import { logger } from "./logger.js";

const app = new App();

async function main() {
  await app.start();
}

async function shutdown() {
  logger.info("[signal] shutdown requested");
  await app.stop();
  process.exit(0);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

main().catch((err) => {
  logger.error({ err }, "[fatal] app start failed");
  process.exit(1);
});
