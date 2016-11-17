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
#include "xattr_utils.h"

uint32_t cb::xattr::get_body_offset(const const_char_buffer& payload) {
    const uint32_t* lenptr = reinterpret_cast<const uint32_t*>(payload.buf);
    return *lenptr + sizeof(uint32_t);
}

const_char_buffer cb::xattr::get_body(const const_char_buffer& payload) {
    auto offset = get_body_offset(payload);
    return {payload.buf + offset, payload.len - offset};
}

const_char_buffer cb::xattr::get_xattr(const const_char_buffer& payload) {
    const uint32_t* lenptr = reinterpret_cast<const uint32_t*>(payload.buf);
    return {payload.buf + sizeof(uint32_t), *lenptr};
}
