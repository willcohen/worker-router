let initCalls = [];
export const handler = {
  async init(args) {
    initCalls.push(args);
  },
  calls() { return initCalls.slice(); },
};
