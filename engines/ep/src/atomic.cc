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
{
    lck.clear();
}

SpinLock::~SpinLock() {}

bool SpinLock::tryAcquire(void) {
    return !lck.test_and_set(std::memory_order_acquire);
}


void SpinLock::lock(void) {
   int spin = 0;
   while (!tryAcquire()) {
      ++spin;
      if (spin > 64) {
          sched_yield();
      }
   }
}

void SpinLock::unlock(void) {
    lck.clear(std::memory_order_release);
}
