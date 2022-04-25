/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <daemon/connection.h>

/**
 * Implementation of the SELECT BUCKET command
 *
 * @param c the connection which should select the bucket
 * @param packet the incomming packet
 */
void select_bucket_executor(Cookie& cookie);

void list_bucket_executor(Cookie& cookie);

void get_cmd_timer_executor(Cookie& cookie);

void process_hello_packet_executor(Cookie& cookie);

// DCP executor
void dcp_add_stream_executor(Cookie& cookie);
void dcp_buffer_acknowledgement_executor(Cookie& cookie);
void dcp_close_stream_executor(Cookie& cookie);
void dcp_control_executor(Cookie& cookie);
void dcp_get_failover_log_executor(Cookie& cookie);
void dcp_noop_executor(Cookie& cookie);
void dcp_open_executor(Cookie& cookie);
void dcp_set_vbucket_state_executor(Cookie& cookie);
void dcp_snapshot_marker_executor(Cookie& cookie);
void dcp_stream_end_executor(Cookie& cookie);
void dcp_stream_req_executor(Cookie& cookie);
void dcp_abort_executor(Cookie& cookie);
void dcp_commit_executor(Cookie& cookie);
void dcp_prepare_executor(Cookie& cookie);
void dcp_seqno_acknowledged_executor(Cookie& cookie);

// Collections
void collections_set_manifest_executor(Cookie& cookie);
void collections_get_manifest_executor(Cookie& cookie);
void collections_get_collection_id_executor(Cookie& cookie);
void collections_get_scope_id_executor(Cookie& cookie);

void drop_privilege_executor(Cookie&);

void get_cluster_config_executor(Cookie&);
void set_cluster_config_executor(Cookie&);

void adjust_timeofday_executor(Cookie&);

// RangeScan
void range_scan_create_executor(Cookie&);
void range_scan_cancel_executor(Cookie&);

/**
 * Handle the status for an executor (update ewouldblock state / disconnect
 * send responses etc)
 *
 * @param cookie The command cookie running the operation
 * @param status code (cannot be rollback)
 */
void handle_executor_status(Cookie& cookie, cb::engine_errc status);
