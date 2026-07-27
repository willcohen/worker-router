// Copyright (c) 2026 Will Cohen
//
// Part of worker-router, under the Apache License v2.0 with LLVM Exceptions.
// See LICENSE for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

// Test fixture for bootstrapTimeoutMs: stays alive but never posts a
// ready (or error) message, so only the pool's timeout can settle
// create(). The interval keeps the worker's event loop from draining,
// which would otherwise fire the exit listener instead of the timer.
setInterval(() => {}, 1000);
