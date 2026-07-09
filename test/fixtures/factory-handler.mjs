export async function create(init) {
  const prefix = init?.prefix ?? 'factory';
  return {
    greet(name) { return `${prefix}:${name}`; },
  };
}
