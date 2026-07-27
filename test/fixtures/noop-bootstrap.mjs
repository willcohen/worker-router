// Copyright (c) 2026 Will Cohen
//
// Part of worker-router, under the Apache License v2.0 with LLVM Exceptions.
// See LICENSE for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

// A no-op bootstrap: posts a ready-shaped message on startup.
import { parentPort } from 'node:worker_threads';
parentPort.postMessage({ type: 'worker-router/ready' });
