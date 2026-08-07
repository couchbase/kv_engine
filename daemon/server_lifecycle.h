/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

/// Query the human-readable server version string.
const char* get_server_version();

/// Has the server begun shutting down?
bool is_memcached_shutting_down();

/// Request that the server begin shutting down.
void shutdown_server();

/// Ask the main event loop to stop looping (used during shutdown).
void stop_memcached_main_base();
