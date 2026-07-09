let count = 0;
export const handler = {
  async init(args) {
    count = typeof args?.start === 'number' ? args.start : 0;
  },
  inc() { return ++count; },
  get() { return count; },
};
