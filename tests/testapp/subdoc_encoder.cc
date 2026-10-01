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
 * Sub-document API encoding helpers
 *
 */

#include "subdoc_encoder.h"

#include <gsl/gsl-lite.hpp>
#include <iterator>

/**
 * Append the opcode as a single byte to the provided buffer.
 *
 * @param vector The buffer to append to
 * @param opcode The opcode to append
 */
static void append(std::vector<char>& vector, cb::mcbp::ClientOpcode opcode) {
    vector.push_back(static_cast<char>(opcode));
}

/**
 * Append the path flags as a single byte to the provided buffer.
 *
 * @param vector The buffer to append to
 * @param flag The path flags to append
 */
static void append(std::vector<char>& vector, cb::mcbp::subdoc::PathFlag flag) {
    vector.push_back(static_cast<char>(flag));
}

/**
 * Append the document flags as a single byte to the provided buffer.
 *
 * @param vector The buffer to append to
 * @param flag The document flags to append
 */
static void append(std::vector<char>& vector, cb::mcbp::subdoc::DocFlag flag) {
    vector.push_back(static_cast<char>(flag));
}

/**
 * Append a 16 bit integer in network byte order to the provided buffer.
 *
 * @param vector The buffer to append to
 * @param value The value (in host byte order) to append
 */
static void append(std::vector<char>& vector, uint16_t value) {
    const auto val = htons(value);
    const auto* ptr = reinterpret_cast<const char*>(&val);
    vector.insert(vector.end(), ptr, ptr + sizeof(val));
}

/**
 * Append a 32 bit integer in network byte order to the provided buffer.
 *
 * @param vector The buffer to append to
 * @param value The value (in host byte order) to append
 */
static void append(std::vector<char>& vector, uint32_t value) {
    const auto val = htonl(value);
    const auto* ptr = reinterpret_cast<const char*>(&val);
    vector.insert(vector.end(), ptr, ptr + sizeof(val));
}

void SubdocMultiCmd::Spec::encode(std::vector<char>& destination,
                                  bool has_value) const {
    append(destination, opcode);
    append(destination, flags);
    append(destination, gsl::narrow<uint16_t>(path.size()));
    if (has_value) {
        append(destination, gsl::narrow<uint32_t>(value.size()));
    }
    std::ranges::copy(path, back_inserter(destination));
    if (has_value) {
        std::ranges::copy(value, back_inserter(destination));
    }
}

size_t SubdocMultiCmd::Spec::encoded_size(bool has_value) const {
    if (has_value) {
        return sizeof(protocol_binary_subdoc_multi_mutation_spec) +
               path.size() + value.size();
    }
    // A value which isn't encoded would be silently dropped
    Expects(value.empty());
    return sizeof(protocol_binary_subdoc_multi_lookup_spec) + path.size();
}

bool SubdocMultiCmd::spec_has_value(const Spec&) const {
    return command == cb::mcbp::ClientOpcode::SubdocMultiMutation;
}

std::vector<char> SubdocMultiCmd::encode() const {
    size_t specs_size = 0;
    for (const auto& s : specs) {
        specs_size += s.encoded_size(spec_has_value(s));
    }

    // Encode the common elements (key, extras) first.
    std::vector<char> request = encode_common(specs_size);
    const auto expected_size = request.size() + specs_size;

    // Add all specs.
    for (const auto& s : specs) {
        s.encode(request, spec_has_value(s));
    }

    // encoded_size() must match what encode() appends, otherwise the
    // buffer reserved by encode_common() is the wrong size.
    Expects(request.size() == expected_size);

    // Populate the header.
    auto* header = reinterpret_cast<cb::mcbp::Request*>(request.data());
    populate_header(*header, request.size() - sizeof(*header));

    return request;
}

std::vector<char> SubdocMultiCmd::encode_common(const size_t specs_size) const {
    std::vector<char> request;
    request.reserve(sizeof(cb::mcbp::Request) + get_extlen() + key.size() +
                    specs_size);

    // Reserve space for the header and setup pointer to it (we fill in the
    // details once the rest of the packet is encoded).
    request.resize(sizeof(cb::mcbp::Request));

    // Expiry (optional) is encoded in extras. Only include if non-zero or
    // if explicit encoding of zero was requested.
    if (expiry != 0 || encode_zero_expiry_on_wire) {
        append(request, expiry);
    }

    if (!isNone(doc_flags)) {
        append(request, doc_flags);
    }

    // Add the key.
    std::ranges::copy(key, back_inserter(request));

    return request;
}

void SubdocMultiCmd::addDocFlag(const cb::mcbp::subdoc::DocFlag flag) {
    doc_flags |= flag;
}

size_t SubdocMultiCmd::get_extlen() const {
    size_t extlen = 0;
    if (expiry != 0 || encode_zero_expiry_on_wire) {
        extlen += sizeof(expiry);
    }
    if (!isNone(doc_flags)) {
        extlen += sizeof(doc_flags);
    }
    return extlen;
}

void SubdocMultiCmd::populate_header(cb::mcbp::Request& header,
                                     const size_t bodylen) const {
    header.setMagic(cb::mcbp::Magic::ClientRequest);
    header.setOpcode(command);
    header.setKeylen(gsl::narrow<uint16_t>(key.size()));
    header.setExtlen(gsl::narrow<uint8_t>(get_extlen()));
    header.setDatatype(cb::mcbp::Datatype::Raw);
    header.setVBucket(vbid);
    header.setBodylen(gsl::narrow<uint32_t>(bodylen));
    header.setOpaque(0xdeadbeef);
    header.setCas(cas);
}
