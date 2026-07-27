// Copyright (c) 2026 Will Cohen
//
// Part of worker-router, under the Apache License v2.0 with LLVM Exceptions.
// See LICENSE for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

export async function create(init) {
  const prefix = init?.prefix ?? 'factory';
  return {
    greet(name) { return `${prefix}:${name}`; },
  };
}
