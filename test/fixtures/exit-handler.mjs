// Test fixture for dead-worker handling: die() replies first, then hard-
// exits the worker thread shortly after, so the test can observe the
// pool's exit tracking without leaving a never-settling call behind.
export const handler = {
  ping() { return 'pong'; },
  die() {
    setTimeout(() => process.exit(1), 10);
    return 'dying';
  },
};
