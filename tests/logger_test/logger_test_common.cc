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

/*
 * Common helper code for all logger tests.
 */

#include "logger_test_common.h"

#include <platform/cbassert.h>
#include <platform/dirutils.h>

EXTENSION_LOGGER_DESCRIPTOR *logger;

EXTENSION_LOG_LEVEL get_log_level(void)
{
    return EXTENSION_LOG_DETAIL;
}

bool register_extension(extension_type_t type, void *extension)
{
    cb_assert(type == EXTENSION_LOGGER);
    logger = reinterpret_cast<EXTENSION_LOGGER_DESCRIPTOR *>(extension);
    return true;
}

void register_callback(ENGINE_HANDLE *eh,
                       ENGINE_EVENT_TYPE type,
                       EVENT_CALLBACK cb,
                       const void *cb_data)
{
}

SERVER_HANDLE_V1 *get_server_api(void)
{
    static int init;
    static SERVER_CORE_API core_api;
    static SERVER_COOKIE_API server_cookie_api;
    static SERVER_STAT_API server_stat_api;
    static SERVER_LOG_API server_log_api;
    static SERVER_EXTENSION_API extension_api;
    static SERVER_CALLBACK_API callback_api;
    static ALLOCATOR_HOOKS_API hooks_api;
    static SERVER_HANDLE_V1 rv;

    if (!init) {
        init = 1;

        core_api.parse_config = parse_config;
        server_log_api.get_level = get_log_level;
        extension_api.register_extension = register_extension;
        callback_api.register_callback = register_callback;

        rv.interface = 1;
        rv.core = &core_api;
        rv.stat = &server_stat_api;
        rv.extension = &extension_api;
        rv.callback = &callback_api;
        rv.log = &server_log_api;
        rv.cookie = &server_cookie_api;
        rv.alloc_hooks = &hooks_api;
    }

    return &rv;
}

void remove_files(std::vector<std::string> &files) {
    for (std::vector<std::string>::iterator iter = files.begin();
         iter != files.end();
         ++iter) {
        CouchbaseDirectoryUtilities::rmrf(*iter);
    }
}
