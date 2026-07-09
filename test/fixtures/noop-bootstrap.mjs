// A no-op bootstrap: posts a ready-shaped message on startup.
import { parentPort } from 'node:worker_threads';
parentPort.postMessage({ type: 'worker-router/ready' });
