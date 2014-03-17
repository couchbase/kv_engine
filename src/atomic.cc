/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2011 Couchbase, Inc
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

#include "config.h"

#include "atomic.h"

SpinLock::SpinLock()
#ifndef USE_CXX11_ATOMICS
    : lock(0)
#endif
{
#ifdef USE_CXX11_ATOMICS
    lock.clear();
#endif
}

SpinLock::~SpinLock() {
#ifndef USE_CXX11_ATOMICS
    cb_assert(lock == 0);
#endif
}

bool SpinLock::tryAcquire(void) {
#ifdef USE_CXX11_ATOMICS
    return !lock.test_and_set(std::memory_order_acquire);
#else
    return ep_sync_lock_test_and_set(&lock, 1) == 0;
#endif
}


void SpinLock::acquire(void) {
   int spin = 0;
   while (!tryAcquire()) {
      ++spin;
      if (spin > 64) {
          sched_yield();
      }
   }
}

void SpinLock::release(void) {
#ifdef USE_CXX11_ATOMICS
    lock.clear(std::memory_order_release);
#else
    ep_sync_lock_release(&lock);
#endif
}
