/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include <memcached/visibility.h>
#include <platform/cb_malloc.h>
#include <string>

/*
 * Plugin (shared object / DLL) loaded at runtime by memory_tracking_test
 * to verify memory tracking across shared objects.
 */

extern "C" MEMCACHED_PUBLIC_API void* plugin_malloc(size_t size);
extern "C" MEMCACHED_PUBLIC_API void plugin_free(void* ptr);
extern "C" MEMCACHED_PUBLIC_API char* plugin_new_char_array(size_t len);
extern "C" MEMCACHED_PUBLIC_API void plugin_delete_array(char* ptr);
extern "C" MEMCACHED_PUBLIC_API std::string* plugin_new_string(const char* str);
extern "C" MEMCACHED_PUBLIC_API void plugin_delete_string(std::string* ptr);

void* plugin_malloc(size_t size) {
    return cb_malloc(size);
}

void plugin_free(void* ptr) {
    cb_free(ptr);
}

char* plugin_new_char_array(size_t len) {
    return new char[len];
}

void plugin_delete_array(char* ptr) {
    delete[] ptr;
}

std::string* plugin_new_string(const char* str) {
    return new std::string(str);
}

void plugin_delete_string(std::string* str) {
    delete str;
}
