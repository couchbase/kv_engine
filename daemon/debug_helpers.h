/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/*
 * Helper functions for debug output.
 */

#pragma once

#include <cstddef>
#include <cstdint>

/* Insert a char* into a buffer, but replace all non-printable characters with
 * a '.'
 *
 * @param dest where to store the output
 * @param destsz size of destination buffer
 * @param src the string to add to the buffer
 * @param srcsz the number of bytes in src
 * @return true if success, false otherwise
 */
bool buf_to_printable_buffer(char* dest,
                             size_t destsz,
                             const char* src,
                             size_t srcsz);

/**
 * Insert a key into a buffer, but replace all non-printable characters
 * with a '.'.
 *
 * @param dest where to store the output
 * @param destsz size of destination buffer
 * @param prefix string to insert before the data
 * @param client the client we are serving
 * @param from_client set to true if this data is from the client
 * @param key the key to add to the buffer
 * @param nkey the number of bytes in the key
 * @return true if success, false otherwise
 */
bool key_to_printable_buffer(char* dest,
                             size_t destsz,
                             uint32_t client,
                             bool from_client,
                             const char* prefix,
                             const char* key,
                             size_t nkey);

/**
 * Convert a byte array to a text string
 *
 * @param dest where to store the output
 * @param destsz size of destination buffer
 * @param prefix string to insert before the data
 * @param client the client we are serving
 * @param from_client set to true if this data is from the client
 * @param data the data to add to the buffer
 * @param size the number of bytes in data to print
 * @return true if success, false otherwise
 */
bool bytes_to_output_string(char* dest,
                            size_t destsz,
                            uint32_t client,
                            bool from_client,
                            const char* prefix,
                            const char* data,
                            size_t size);
