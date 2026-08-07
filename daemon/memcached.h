/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <memcached/engine_error.h>
#include <platform/socket.h>
#include <vector>

#include <statistics/prometheus.h>

/** \file
 * The main memcached header holding commonly used data
 * structures and function prototypes.
 */

/** Maximum length of a key. */
#define KEY_MAX_LENGTH 250

/// Safely closes a server socket.
void close_server_socket(SOCKET sfd);
/// Safely closes a client socket.
void close_client_socket(SOCKET sfd);
