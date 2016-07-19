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

#include "subdocument_traits.h"

SubdocCmdTraits get_subdoc_cmd_traits(protocol_binary_command cmd) {
    switch (cmd) {
    case PROTOCOL_BINARY_CMD_SUBDOC_GET:
        return get_traits<PROTOCOL_BINARY_CMD_SUBDOC_GET>();

    case PROTOCOL_BINARY_CMD_SUBDOC_EXISTS:
        return get_traits<PROTOCOL_BINARY_CMD_SUBDOC_EXISTS>();

    case PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD:
        return get_traits<PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD>();

    case PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT:
        return get_traits<PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT>();

    case PROTOCOL_BINARY_CMD_SUBDOC_DELETE:
        return get_traits<PROTOCOL_BINARY_CMD_SUBDOC_DELETE>();

    case PROTOCOL_BINARY_CMD_SUBDOC_REPLACE:
        return get_traits<PROTOCOL_BINARY_CMD_SUBDOC_REPLACE>();

    case PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST:
        return get_traits<PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST>();

    case PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST:
        return get_traits<PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST>();

    case PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT:
        return get_traits<PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT>();

    case PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE:
        return get_traits<PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE>();

    case PROTOCOL_BINARY_CMD_SUBDOC_COUNTER:
        return get_traits<PROTOCOL_BINARY_CMD_SUBDOC_COUNTER>();

    case PROTOCOL_BINARY_CMD_SUBDOC_GET_COUNT:
        return get_traits<PROTOCOL_BINARY_CMD_SUBDOC_GET_COUNT>();

    default:
        return {Subdoc::Command::INVALID,
                SUBDOC_FLAG_NONE,
                false,
                false,
                false,
                false,
                SubdocPath::SINGLE};
    }
}
