/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <string_view>

/**
 * Validate that a key is legal according to the XATTR spec
 *
 * The XATTR key limitations:
 * <ul>
 *   <li>An XATTR consists of a key/value pair.</li>
 *   <li>The XATTR key (X-Key) is a Modified UTF-8 string up to 16 bytes in
 *       length.</li>
 *   <li>X-Keys starting with the following characters are reserved and
 *       cannot be used:
 *       <ul>
 *          <li>ispunct(), excluding underscore</li>
 *          <li>iscntrl()</li>
 *       </ul>
 *   <li>X-Keys starting with a leading underscore ('_', 0x5F) are considered
 *       system XATTRs and can only be accessed if the client holds the
 *       SYSTEM_XATTR read / write privilege.</li>
 *   <li>X-keys starting with a leading dollar sign ('$', 0x25) are considered
 *       virtual xattrs</li>
 * </ul>
 *
 * @param path The path to check
 * @param key_length The length of the key component (out)
 * @return true if the key part of the provided path meets the spec
 */
bool is_valid_xattr_key(std::string_view path, size_t& key_length);

// Wrapper function if you couldn't care less about the key length
inline bool is_valid_xattr_key(std::string_view path) {
    size_t len;
    return is_valid_xattr_key(path, len);
}
