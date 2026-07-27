// Copyright (c) 2026 Will Cohen
//
// Part of worker-router, under the Apache License v2.0 with LLVM Exceptions.
// See LICENSE for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

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
