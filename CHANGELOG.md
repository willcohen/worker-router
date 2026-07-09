# Changelog

All notable changes to this project will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.0.1] - 2026-07-09

Initial release.

- `WorkerPool.create({size, bootstrap, handlers})`: a fixed-size pool of
  Web Workers (browser) or `worker_threads` (Node), each loading the same
  handler modules, exposed over Comlink.
- Three routing modes: `worker(i)` by index, `any()` least-loaded, and
  `claim()`/`release()` for durable placement.
- Lifecycle: ready handshake, per-handler `init`/`destroy`, idempotent
  `terminate()`, configurable `bootstrapTimeoutMs`/`shutdownTimeoutMs`.
  A worker that fails to boot rejects `create()`. A worker that dies later
  is routed around; calls to it reject instead of hanging, and an
  unexpected death is logged.
- TypeScript declarations. `comlink` and `squint-cljs/core.js` stay
  external to the published bundle; a bundler resolves them, or map them
  in an import map for browsers without one.
