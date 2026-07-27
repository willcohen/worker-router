// Copyright (c) 2026 Will Cohen
//
// Part of worker-router, under the Apache License v2.0 with LLVM Exceptions.
// See LICENSE for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

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
