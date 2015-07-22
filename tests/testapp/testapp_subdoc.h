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

#include <memory>

// Class representing a subdoc command; to assist in constructing / encoding one.
struct SubdocCmd {
    // Always need at least a cmd, key and path.
    explicit SubdocCmd(protocol_binary_command cmd_, const std::string& key_,
                       const std::string& path_)
      : cmd(cmd_), key(key_), path(path_), value(""), flags(), cas() {}

    // Constructor including a value.
    explicit SubdocCmd(protocol_binary_command cmd_, const std::string& key_,
                       const std::string& path_, const std::string& value_)
      : cmd(cmd_), key(key_), path(path_), value(value_), flags(), cas() {}

    // Constructor additionally including flags.
    explicit SubdocCmd(protocol_binary_command cmd_, const std::string& key_,
                       const std::string& path_, const std::string& value_,
                       protocol_binary_subdoc_flag flags_)
      : cmd(cmd_), key(key_), path(path_), value(value_), flags(flags_),
        cas() {}

    // Constructor additionally including CAS.
    explicit SubdocCmd(protocol_binary_command cmd_, const std::string& key_,
                       const std::string& path_, const std::string& value_,
                       protocol_binary_subdoc_flag flags_, uint64_t cas_)
      : cmd(cmd_), key(key_), path(path_), value(value_), flags(flags_),
        cas(cas_) {}

    protocol_binary_command cmd;
    std::string key;
    std::string path;
    std::string value;
    protocol_binary_subdoc_flag flags;
    uint64_t cas;
};

/* Encodes and sends a sub-document command with the given parameters, receives
 * the response and validates that the status matches the expected one.
 * If expected_value is non-empty, also verifies that the response value equals
 * expectd_value.
 * @return CAS value (if applicable for the command, else zero).
 */
uint64_t expect_subdoc_cmd(const SubdocCmd& cmd,
                           protocol_binary_response_status expected_status,
                           const std::string& expected_value);

void store_object(const std::string& key,
                  const std::string& value,
                  bool JSON, bool compress);
