/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

/*
 * testapp testcases for sub-document API.
 */

#pragma once

#include "testapp.h"

#if defined(__cplusplus)
extern "C" {
#endif

enum test_return test_subdoc_get_binary_raw();
enum test_return test_subdoc_get_binary_compressed();
enum test_return test_subdoc_get_array_simple_raw();
enum test_return test_subdoc_get_array_simple_compressed();
enum test_return test_subdoc_get_dict_simple_raw();
enum test_return test_subdoc_get_dict_simple_compressed();
enum test_return test_subdoc_get_dict_nested_raw();
enum test_return test_subdoc_get_dict_nested_compressed();
enum test_return test_subdoc_get_dict_deep();
enum test_return test_subdoc_get_array_deep();

enum test_return test_subdoc_exists_binary_raw();
enum test_return test_subdoc_exists_binary_compressed();
enum test_return test_subdoc_exists_array_simple_raw();
enum test_return test_subdoc_exists_array_simple_compressed();
enum test_return test_subdoc_exists_dict_simple_raw();
enum test_return test_subdoc_exists_dict_simple_compressed();
enum test_return test_subdoc_exists_dict_nested_raw();
enum test_return test_subdoc_exists_dict_nested_compressed();
enum test_return test_subdoc_exists_dict_deep();
enum test_return test_subdoc_exists_array_deep();

enum test_return test_subdoc_dict_add_simple_raw();
enum test_return test_subdoc_dict_add_simple_compressed();
enum test_return test_subdoc_dict_add_deep();

enum test_return test_subdoc_dict_upsert_simple_raw();
enum test_return test_subdoc_dict_upsert_simple_compressed();
enum test_return test_subdoc_dict_upsert_deep();

enum test_return test_subdoc_delete_simple_raw();
enum test_return test_subdoc_delete_simple_compressed();
enum test_return test_subdoc_delete_array();
enum test_return test_subdoc_delete_array_nested();

enum test_return test_subdoc_replace_simple_dict();
enum test_return test_subdoc_replace_simple_array();
enum test_return test_subdoc_replace_array_deep();

enum test_return test_subdoc_array_push_last_simple();
enum test_return test_subdoc_array_push_last_nested();

enum test_return test_subdoc_array_push_first_simple();
enum test_return test_subdoc_array_push_first_nested();

enum test_return test_subdoc_array_add_unique_simple();

#if defined(__cplusplus)
} // extern "C"
#endif
