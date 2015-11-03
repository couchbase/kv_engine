/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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

/*
 * The mcbp_executors files contains the implementation of the execution
 * callbacks for the memcached binary protocol.
 */

#include <array>
#include <memcached/protocol_binary.h>
#include "connection_mcbp.h"

typedef void (* mcbp_package_execute)(McbpConnection *c, void* packet);

/**
 * Get the memcached binary protocol executors
 *
 * @return the array of 0x100 entries for the package
 *         executors
 */
std::array<mcbp_package_execute, 0x100>& get_mcbp_executors();

void mcbp_complete_nread(McbpConnection *c);

int try_read_mcbp_command(McbpConnection *c);

void initialize_mbcp_lookup_map(void);

void ship_mcbp_tap_log(McbpConnection* c);
void ship_mcbp_dcp_log(McbpConnection* c);
void setup_mcbp_lookup_cmd(
    EXTENSION_BINARY_PROTOCOL_DESCRIPTOR* descriptor,
    uint8_t cmd,
    BINARY_COMMAND_CALLBACK new_handler);

void process_stat_settings(ADD_STAT add_stat_callback, void *c);