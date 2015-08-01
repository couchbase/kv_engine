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

#include "config.h"
#include "alloc_hooks.h"
#include <stdbool.h>

void init_alloc_hooks() {
    get_stderr_logger()->log(EXTENSION_LOG_NOTICE, NULL,
                             "This version of Couchbase is built without "
                             "allocator hooks for accurate memory tracking");
}

bool mc_add_new_hook(void (* hook)(const void* ptr, size_t size)) {
    return false;
}

bool mc_remove_new_hook(void (* hook)(const void* ptr, size_t size)) {
    return false;
}

bool mc_add_delete_hook(void (* hook)(const void* ptr)) {
    return false;
}

bool mc_remove_delete_hook(void (* hook)(const void* ptr)) {
    return false;
}

int mc_get_extra_stats_size() {
    return 0;
}

void mc_get_allocator_stats(allocator_stats* stats) {
}

size_t mc_get_allocation_size(const void* ptr) {
    return 0;
}

void mc_get_detailed_stats(char* buffer, int size) {
    // empty
}

void mc_release_free_memory() {
    // empty
}

bool mc_enable_thread_cache(bool enable) {
    return true;
}

bool mc_get_allocator_property(const char* name, size_t* value) {
    return false;
}

bool mc_set_allocator_property(const char* name, size_t value) {
    return false;
}
