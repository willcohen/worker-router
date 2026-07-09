// Test fixture for shutdownTimeoutMs: destroy never settles, so only
// the pool's per-worker shutdown cap lets terminate() complete.
export const handler = {
  ping() { return 'pong'; },
};

export function destroy() {
  return new Promise(() => {});
}
