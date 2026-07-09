// Test fixture for bootstrapTimeoutMs: stays alive but never posts a
// ready (or error) message, so only the pool's timeout can settle
// create(). The interval keeps the worker's event loop from draining,
// which would otherwise fire the exit listener instead of the timer.
setInterval(() => {}, 1000);
