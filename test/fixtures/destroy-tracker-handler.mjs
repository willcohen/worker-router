// Test fixture for the worker-router destroy lifecycle.
//
// The handler module exports `handler` (a Comlink-published object with
// an `init` method) and a top-level `destroy` sibling. The bootstrap
// discovers `destroy` at handler load time and registers it on the
// per-worker coordinator; `pool.terminate()` invokes it before tearing
// down the underlying worker handle.
//
// Per-worker side effect: `destroy` appends a line to args.logPath so
// the main-thread test can verify both that destroy fired and that the
// per-handler destroy args reached the module.
import { appendFileSync } from 'node:fs';

let initArgs = null;

export const handler = {
  async init(args) {
    initArgs = args;
  },
  // Visible to callers as a method on the Comlink-exposed object —
  // lets the test confirm init landed before terminating.
  async snapshotInit() {
    return initArgs;
  },
};

export function destroy(args) {
  const line = JSON.stringify({
    init: initArgs,
    destroyArgs: args,
    pid: process.pid,
  });
  appendFileSync(args.logPath, line + '\n');
}
