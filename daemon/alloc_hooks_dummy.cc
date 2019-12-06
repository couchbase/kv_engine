/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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

/**
 * This file defines the memory allocation hooks used on platforms
 * where we don't have a supported memory alloctor for memory tracking
 */

#include "alloc_hooks_dummy.h"

#include <logger/logger.h>
#include <cstdlib>

void DummyAllocHooks::initialize() {
    // Don't clutter the unit test output with this log message
    if (getenv("MEMCACHED_UNIT_TESTS") == nullptr) {
        LOG_INFO(
                "This version of Couchbase is built without allocator hooks "
                "for accurate memory tracking");
    }
}

size_t DummyAllocHooks::get_allocation_size(const void* ptr) {
    return 0;
}

void DummyAllocHooks::release_free_memory() {
    // empty
}

bool DummyAllocHooks::enable_thread_cache(bool enable) {
    return true;
}

bool DummyAllocHooks::get_allocator_property(const char* name, size_t* value) {
    return false;
}

int DummyAllocHooks::set_allocator_property(const char* name,
                                            void* newp,
                                            size_t newlen) {
    return 1;
}
