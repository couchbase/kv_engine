/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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

#include <platform/sized_buffer.h>
#include <xattr/visibility.h>

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
XATTR_PUBLIC_API
bool is_valid_xattr_key(cb::const_char_buffer path, size_t& key_length);

// Wrapper function if you couldn't care less about the key length
inline bool is_valid_xattr_key(cb::const_char_buffer path) {
    size_t len;
    return is_valid_xattr_key(path, len);
}
