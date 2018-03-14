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

#include <memcached/protocol_binary.h>
#include <array>
#include "connection.h"

void mcbp_execute_packet(Cookie& cookie);

void try_read_mcbp_command(Connection& c);

void initialize_mbcp_lookup_map();
