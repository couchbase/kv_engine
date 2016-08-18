/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
*     Copyright 2016 Couchbase, Inc
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
#include <platform/cb_malloc.h>
#include <platform/visibility.h>
#include <string>

/*
 * Plugin (shared object / DLL) loaded at runtime by memory_tracking_test
 * to verify memory tracking across shared objects.
 */

extern "C" EXPORT_SYMBOL void* plugin_malloc(size_t size);
extern "C" EXPORT_SYMBOL void plugin_free(void* ptr);
extern "C" EXPORT_SYMBOL char* plugin_new_char_array(size_t len);
extern "C" EXPORT_SYMBOL void plugin_delete_array(char* ptr);
extern "C" EXPORT_SYMBOL std::string* plugin_new_string(const char* str);
extern "C" EXPORT_SYMBOL void plugin_delete_string(std::string* ptr);

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
