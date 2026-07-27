// Copyright (c) 2026 Will Cohen
//
// Part of worker-router, under the Apache License v2.0 with LLVM Exceptions.
// See LICENSE for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

// Test fixture for shutdownTimeoutMs: destroy never settles, so only
// the pool's per-worker shutdown cap lets terminate() complete.
export const handler = {
  ping() { return 'pong'; },
};

export function destroy() {
  return new Promise(() => {});
}
