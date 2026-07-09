// Test fixture for in-flight-call resilience: `slow` returns a promise that
// resolves after `ms`, long enough for the test to race terminate() against
// a call that is genuinely still in flight.
export const handler = {
  async init() {},
  ping() { return 'pong'; },
  slow(ms) {
    return new Promise((resolve) => setTimeout(() => resolve('slow-done'), ms));
  },
};
