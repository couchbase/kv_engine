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

#include <string>

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
 * Validate that the first content of a blob contains a valid XATTR
 * encoded blob. The motivation behind this function is to ensure that
 * we don't crash if someone provides an illegal blob via DCP or SetWithMeta
 *
 * @param blob the blob to validate
 * @return true if the blob contains a valid xattr encoded blob (and
 *              that it is safe to use the rest of the methods in
 *              cb::xattr to access them
 */
XATTR_PUBLIC_API
bool validate(const cb::const_char_buffer& blob);

/**
 * Get the offset of the body into the specified payload
 *
 * @param payload the payload to check
 * @return The number of bytes into the payload where the body lives
 *         (the body size == payload.len - the returned value)
 */
XATTR_PUBLIC_API
uint32_t get_body_offset(const cb::const_char_buffer& payload);

/**
 * Get the segment where the actual body lives
 *
 * @param payload the document blob as it is stored in the engine
 * @return a buffer representing the body blob
 */
XATTR_PUBLIC_API
cb::const_char_buffer get_body(const cb::const_char_buffer& payload);

/**
 * Check to see if the provided attribute represents a system
 * attribute or not.
 *
 * @param attr the attribute to check (CAN'T BE EMPTY!)
 */
static inline bool is_system_xattr(cb::const_char_buffer& attr) {
    return *attr.data() == '_';
}

/**
 * Check if the attribute is a virtual xattr or not
 *
 * @param attr the attribute to check
 */
static inline bool is_vattr(cb::const_char_buffer attr) {
    return !attr.empty() && *attr.data() == '$';
}

namespace macros {
struct macro {
    cb::const_char_buffer name;
    size_t expandedSize;
};
static macro CAS = {"\"${Mutation.CAS}\"", 8};
static macro SEQNO = {"\"${Mutation.seqno}\"", 8};
static macro VALUE_CRC32C = {"\"${Mutation.value_crc32c}\"", 4};
}

namespace vattrs {
static constexpr cb::const_char_buffer DOCUMENT = {"$document", 9};
static constexpr cb::const_char_buffer VBUCKET = {"$vbucket", 8};
static constexpr cb::const_char_buffer XTOC = {"$XTOC", 5};
}

/**
 * Get the number of bytes the system xattrs contains in the provided
 * document (which may not contain xattrs at all)
 *
 * @param datatype the datatype for the provided document
 * @param doc the document to inspect
 * @return the number of bytes of system xattrs
 */
XATTR_PUBLIC_API
size_t get_system_xattr_size(uint8_t datatype, const cb::const_char_buffer doc);

/**
 * Get both the total size (body_offset) and the number of bytes the system
 * xattrs contains in the provided document (which may not contain xattrs at
 * all)
 *
 * @param datatype the datatype for the provided document
 * @param doc the document to inspect
 * @return pair of size, first is the total, second is the system size
 */
XATTR_PUBLIC_API
std::pair<size_t, size_t> get_size_and_system_xattr_size(
        uint8_t datatype, const cb::const_char_buffer doc);

/**
 * Get the size of the body chunk in the provided value, which may not contain
 * any xattr.
 *
 * @param datatype
 * @param value
 * @return the body size
 */
XATTR_PUBLIC_API
size_t get_body_size(uint8_t datatype, const cb::const_char_buffer value);
}
}
