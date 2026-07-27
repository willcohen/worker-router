// Copyright (c) 2026 Will Cohen
//
// Part of worker-router, under the Apache License v2.0 with LLVM Exceptions.
// See LICENSE for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

// Test fixture for in-flight-call resilience under a mid-call crash:
// `slowThenDie` never resolves its promise and hard-exits the worker `ms`
// later, so the test can observe a call that was in flight when the worker
// died. Distinct from exit-handler (which replies before exiting): here the
// call itself never settles from the worker side.
export const handler = {
  ping() { return 'pong'; },
  slowThenDie(ms) {
    setTimeout(() => process.exit(1), ms);
    return new Promise(() => {});
  },
};
