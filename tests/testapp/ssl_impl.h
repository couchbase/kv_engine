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

/*
 * This file hides the #ifdef's and select the appropriate SSL
 * implementation to use. All of the different backends MUST provide
 * the following methods:
 */

/**
 * Create and connect the ssl_socket to the given port
 */
SOCKET create_connect_ssl_socket(in_port_t port);

/**
 * Destroy and clean up the global ssl connection
 */
void destroy_ssl_socket();

/**
 * Send the provided buffer
 *
 * @param buf The data to send
 * @param len The size of the buffer
 * @return the number of bytes sent
 */
ssize_t phase_send_ssl(const void *buf, size_t len);

/**
 * Receive the requested number of bytes
 *
 * @param buf Where to store the data
 * @param len The number of bytes to read
 * @return the number of bytes received
 */
ssize_t phase_recv_ssl(void *buf, size_t len);
