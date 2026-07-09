export const handler = {
  async init() {},
  echo(x) { return x; },
  nested: {
    double(x) { return x * 2; },
  },
};
