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

#pragma once

#include <extensions/protocol_extension.h>

#include <string>
#include <vector>

extern EXTENSION_LOGGER_DESCRIPTOR *logger;

extern "C" {
    EXTENSION_LOG_LEVEL get_log_level(void);
    bool register_extension(extension_type_t type, void *extension);
    void register_callback(ENGINE_HANDLE *eh,
                           ENGINE_EVENT_TYPE type,
                           EVENT_CALLBACK cb,
                           const void *cb_data);
    SERVER_HANDLE_V1 *get_server_api(void);
}

// Removes all files in the specified vector
void remove_files(std::vector<std::string> &files);
