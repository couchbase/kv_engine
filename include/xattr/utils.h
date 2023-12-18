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

#include <string>
#include <unordered_map>

namespace cb {

namespace json {
class SyntaxValidator;
}

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
bool validate(cb::json::SyntaxValidator& validator, std::string_view blob);

/// Same as the above but a new syntax validator gets created every time
bool validate(std::string_view blob);

// Function exists for testing. This runs the validate loop and exposes the
// system size that validate itself checks.
size_t get_system_size(std::string_view blob);

/**
 * Get the offset of the body into the specified payload
 *
 * @param payload the payload to check
 * @return The number of bytes into the payload where the body lives
 *         (the body size == payload.size() - the returned value)
 */
uint32_t get_body_offset(std::string_view payload);

/**
 * Get the segment where the actual body lives
 *
 * @param payload the document blob as it is stored in the engine
 * @return a buffer representing the body blob
 */
std::string_view get_body(std::string_view payload);

/**
 * Check to see if the provided attribute represents a system
 * attribute or not.
 *
 * @param attr the attribute to check (CAN'T BE EMPTY!)
 */
static inline bool is_system_xattr(std::string_view attr) {
    return *attr.data() == '_';
}

/**
 * Check if the attribute is a virtual xattr or not
 *
 * @param attr the attribute to check
 */
static inline bool is_vattr(std::string_view attr) {
    return !attr.empty() && *attr.data() == '$';
}

namespace macros {
struct macro {
    /// The textual name of the macro
    std::string_view name;
    /// The size of the datatype to be expanded
    size_t expandedSize;
};
static constexpr macro CAS = {R"("${Mutation.CAS}")", sizeof(uint64_t)};
static constexpr macro SEQNO = {R"("${Mutation.seqno}")", sizeof(uint64_t)};
static constexpr macro VALUE_CRC32C = {R"("${Mutation.value_crc32c}")",
                                       sizeof(uint32_t)};
}

namespace vattrs {
static std::string_view DOCUMENT = {"$document", 9};
static std::string_view VBUCKET = {"$vbucket", 8};
static std::string_view XTOC = {"$XTOC", 5};
}

/**
 * Get the number of bytes the system xattrs contains in the provided
 * document (which may not contain xattrs at all)
 *
 * @param datatype the datatype for the provided document
 * @param doc the document to inspect
 * @return the number of bytes of system xattrs
 */
size_t get_system_xattr_size(uint8_t datatype, std::string_view doc);

/**
 * Get the size of the body chunk in the provided value, which may not contain
 * any xattr.
 *
 * @param datatype
 * @param value
 * @return the body size
 */
size_t get_body_size(uint8_t datatype, std::string_view value);

/**
 * Make a wire encoded XATTR value in a std::string. This will encode the given
 * 'body' and xattrs as per the mcbp protocol
 *
 * @param body the 'body' of the document
 * @param xattMap map of XATTR key -> value pairs to encode in the output
 *        document/string.
 */
std::string make_wire_encoded_string(
        const std::string& body,
        const std::unordered_map<std::string, std::string>& xattrMap);
}
}
