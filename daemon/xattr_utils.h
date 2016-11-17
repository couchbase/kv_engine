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

#include <cstring>
#include "buffer.h"

namespace cb {
/**
 * The XATTR support in Couchbase is implemented in the core by storing them
 * together with the actual data. Their presence is flagged by setting the
 * XATTR bit in the datatype member. The actual layout is a four byte
 * integer value containing the length of the XATTR "blob" and then the
 * actual body.
 */
namespace xattr {
/**
 * Get the offset of the body into the specified payload
 *
 * @param payload the payload to check
 * @return The number of bytes into the payload where the body lives
 *         (the body size == payload.len - the returned value)
 */
uint32_t get_body_offset(const const_char_buffer& payload);

/**
 * Get the segment where the actual body lives
 *
 * @param payload the document blob as it is stored in the engine
 * @return a buffer representing the body blob
 */
const_char_buffer get_body(const const_char_buffer& payload);

/**
 * Get the segment where the xattr lives
 *
 * @param payload the document blob as it is stored in the engine
 * @return a buffer representing the xattr blob
 */
const_char_buffer get_xattr(const const_char_buffer& payload);

}
}
