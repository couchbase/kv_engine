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

#include <platform/socket.h>

/// Safely closes a server socket.
void close_server_socket(SOCKET sfd);
/// Safely closes a client socket.
void close_client_socket(SOCKET sfd);
/**
 * Close a client socket which was accepted on a listening port but never
 * handed over to a Connection object, and release the connection slots
 * accounted for it (curr_conns, and system_conns if it was accepted on a
 * system port).
 *
 * @param sfd The socket to close
 * @param system true if the socket was accepted on a system port
 */
void close_client_socket(SOCKET sfd, bool system);
