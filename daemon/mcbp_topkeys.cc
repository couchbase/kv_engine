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

#include "config.h"
#include "mcbp_topkeys.h"

#include <memcached/protocol_binary.h>

/**
 * Define valid commands to track operations on keys. True commands
 * will be tracked, false will not.
 */
std::array<bool, 0x100>& get_mcbp_topkeys() {
    static std::array<bool, 0x100> commands;

    commands[PROTOCOL_BINARY_CMD_SETQ] = true;
    commands[PROTOCOL_BINARY_CMD_SET] = true;
    commands[PROTOCOL_BINARY_CMD_ADDQ] = true;
    commands[PROTOCOL_BINARY_CMD_ADD] = true;
    commands[PROTOCOL_BINARY_CMD_REPLACEQ] = true;
    commands[PROTOCOL_BINARY_CMD_REPLACE] = true;
    commands[PROTOCOL_BINARY_CMD_APPENDQ] = true;
    commands[PROTOCOL_BINARY_CMD_APPEND] = true;
    commands[PROTOCOL_BINARY_CMD_PREPENDQ] = true;
    commands[PROTOCOL_BINARY_CMD_PREPEND] = true;
    commands[PROTOCOL_BINARY_CMD_GET] = true;
    commands[PROTOCOL_BINARY_CMD_GETQ] = true;
    commands[PROTOCOL_BINARY_CMD_GETK] = true;
    commands[PROTOCOL_BINARY_CMD_GETKQ] = true;
    commands[PROTOCOL_BINARY_CMD_DELETE] = true;
    commands[PROTOCOL_BINARY_CMD_DELETEQ] = true;
    commands[PROTOCOL_BINARY_CMD_INCREMENT] = true;
    commands[PROTOCOL_BINARY_CMD_INCREMENTQ] = true;
    commands[PROTOCOL_BINARY_CMD_DECREMENT] = true;
    commands[PROTOCOL_BINARY_CMD_DECREMENTQ] = true;
    commands[PROTOCOL_BINARY_CMD_SUBDOC_GET] = true;
    commands[PROTOCOL_BINARY_CMD_SUBDOC_EXISTS] = true;
    commands[PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD] = true;
    commands[PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT] = true;
    commands[PROTOCOL_BINARY_CMD_SUBDOC_DELETE] = true;
    commands[PROTOCOL_BINARY_CMD_SUBDOC_REPLACE] = true;
    commands[PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST] = true;
    commands[PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST] = true;
    commands[PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT] = true;
    commands[PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE] = true;
    commands[PROTOCOL_BINARY_CMD_GET_REPLICA] = true;
    commands[PROTOCOL_BINARY_CMD_EVICT_KEY] = true;
    commands[PROTOCOL_BINARY_CMD_GET_LOCKED] = true;
    commands[PROTOCOL_BINARY_CMD_UNLOCK_KEY] = true;
    commands[PROTOCOL_BINARY_CMD_GET_META] = true;
    commands[PROTOCOL_BINARY_CMD_GETQ_META] = true;
    commands[PROTOCOL_BINARY_CMD_SET_WITH_META] = true;
    commands[PROTOCOL_BINARY_CMD_SETQ_WITH_META] = true;
    commands[PROTOCOL_BINARY_CMD_DEL_WITH_META] = true;
    commands[PROTOCOL_BINARY_CMD_DELQ_WITH_META] = true;

    return commands;
}
