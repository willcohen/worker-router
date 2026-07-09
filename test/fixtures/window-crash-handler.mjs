import { writeFileSync } from 'node:fs';
// Test fixture for the bootstrap-window liveness gap. Every worker loads
// this. The first worker to win the marker file readies immediately and then
// throws asynchronously 150ms later; every other worker blocks its init
// ~500ms, so it is still booting (create()'s ready barrier still pending)
// when the fast worker throws. That crash therefore lands in the window
// between "fast worker posted ready" and "the pool attached its permanent
// liveness listeners" -- the window this fixture exists to exercise.
export async function create() {
  const marker = process.env.WR_WINDOW_MARKER;
  let first = false;
  try {
    writeFileSync(marker, 'x', { flag: 'wx' });
    first = true;
  } catch {
    first = false;
  }
  if (first) {
    setTimeout(() => {
      throw new Error('worker-router-test: post-ready async boom');
    }, 150);
    return { ping() { return 'pong'; } };
  }
  await new Promise((resolve) => setTimeout(resolve, 500));
  return { ping() { return 'pong'; } };
}
