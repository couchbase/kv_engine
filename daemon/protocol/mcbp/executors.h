/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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

#include <daemon/connection_mcbp.h>

/**
 * Implementation of the SELECT BUCKET command
 *
 * @param c the connection which should select the bucket
 * @param packet the incomming packet
 */
void select_bucket_executor(McbpConnection* c, void* packet);

void list_bucket_executor(McbpConnection* c, void*);

void get_cmd_timer_executor(McbpConnection* c, void* packet);

// DCP executor
void dcp_add_stream_executor(McbpConnection* c, void* packet);
void dcp_buffer_acknowledgement_executor(McbpConnection* c, void* packet);
void dcp_close_stream_executor(McbpConnection* c, void* packet);
void dcp_control_executor(McbpConnection* c, void* packet);
void dcp_flush_executor(McbpConnection* c, void* packet);
void dcp_get_failover_log_executor(McbpConnection* c, void* packet);
void dcp_noop_executor(McbpConnection* c, void*);
void dcp_open_executor(McbpConnection* c, void* packet);
void dcp_set_vbucket_state_executor(McbpConnection* c, void* packet);
void dcp_snapshot_marker_executor(McbpConnection* c, void* packet);
void dcp_stream_end_executor(McbpConnection* c, void* packet);
void dcp_stream_req_executor(McbpConnection* c, void* packet);

