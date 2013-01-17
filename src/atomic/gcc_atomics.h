/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#ifndef SRC_ATOMIC_GCC_ATOMICS_H_
#define SRC_ATOMIC_GCC_ATOMICS_H_ 1

#include "config.h"

#define ep_sync_add_and_fetch(a, b) __sync_add_and_fetch(a, b);
#define ep_sync_bool_compare_and_swap(a, b, c) __sync_bool_compare_and_swap(a, b, c)
#define ep_sync_fetch_and_add(a, b) __sync_fetch_and_add(a, b);
#define ep_sync_lock_release(a) __sync_lock_release(a)
#define ep_sync_lock_test_and_set(a, b) __sync_lock_test_and_set(a, b)
#define ep_sync_synchronize() __sync_synchronize()

#endif  // SRC_ATOMIC_GCC_ATOMICS_H_
