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
 * Sub-document API encoding helpers
 *
 */

#include "subdoc_encoder.h"

#include <gsl/gsl-lite.hpp>
#include <iterator>

std::vector<char> SubdocMultiLookupCmd::encode() const {
    // Encode the common elements (key, extras) first.
    std::vector<char> request = encode_common();

    // Add all lookup specs.
    for (const auto& s : specs) {
        union {
            protocol_binary_subdoc_multi_lookup_spec spec;
            char bytes[sizeof(protocol_binary_subdoc_multi_lookup_spec)];
        } encoded;
        encoded.spec.opcode = s.opcode;
        encoded.spec.flags = s.flags;
        encoded.spec.pathlen = htons(gsl::narrow<uint16_t>(s.path.size()));

        std::copy(&encoded.bytes[0],
                  &encoded.bytes[sizeof(encoded.spec)],
                  back_inserter(request));
        std::copy(s.path.begin(), s.path.end(), back_inserter(request));
    }

    // Populate the header.
    auto* header =
            reinterpret_cast<protocol_binary_request_header*>(request.data());
    populate_header(*header, request.size() - sizeof(*header));

    return request;
}

std::vector<char> SubdocMultiMutationCmd::encode() const {
    // Encode the common elements (key, extras) first.
    std::vector<char> request = encode_common();

    // Add all lookup specs.
    for (const auto& s : specs) {
        union {
            protocol_binary_subdoc_multi_mutation_spec spec;
            char bytes[sizeof(protocol_binary_subdoc_multi_mutation_spec)];
        } encoded;
        encoded.spec.opcode = s.opcode;
        encoded.spec.flags = s.flags;
        encoded.spec.pathlen = htons(gsl::narrow<uint16_t>(s.path.size()));
        encoded.spec.valuelen = htonl(gsl::narrow<uint32_t>(s.value.size()));

        std::copy(&encoded.bytes[0],
                  &encoded.bytes[sizeof(encoded.spec)],
                  back_inserter(request));
        std::copy(s.path.begin(), s.path.end(), back_inserter(request));
        std::copy(s.value.begin(), s.value.end(), back_inserter(request));
    }

    // Populate the header.
    auto* header =
            reinterpret_cast<protocol_binary_request_header*>(request.data());

    populate_header(*header, request.size() - sizeof(*header));

    return request;
}

std::vector<char> SubdocMultiCmd::encode_common() const {
    std::vector<char> request;

    // Reserve space for the header and setup pointer to it (we fill in the
    // details once the rest of the packet is encoded).
    request.resize(sizeof(protocol_binary_request_header));

    // Expiry (optional) is encoded in extras. Only include if non-zero or
    // if explicit encoding of zero was requested.
    bool include_expiry = (expiry != 0 || encode_zero_expiry_on_wire);
    if (include_expiry) {
        union {
            uint32_t expiry;
            char bytes[sizeof(uint32_t)];
        } u;
        u.expiry = htonl(expiry);
        std::copy(&u.bytes[0],
                  &u.bytes[sizeof(uint32_t)],
                  back_inserter(request));
    }

    bool include_doc_flags = !isNone(doc_flags);
    if (include_doc_flags) {
        std::copy(reinterpret_cast<const uint8_t*>(&doc_flags),
                  reinterpret_cast<const uint8_t*>(&doc_flags) + 1,
                  back_inserter(request));
    }

    // Add the key.
    std::copy(key.begin(), key.end(), back_inserter(request));

    return request;
}

void SubdocMultiCmd::addDocFlag(cb::mcbp::subdoc::doc_flag flags_) {
    static const cb::mcbp::subdoc::doc_flag validFlags =
            cb::mcbp::subdoc::doc_flag::Mkdoc |
            cb::mcbp::subdoc::doc_flag::AccessDeleted |
            cb::mcbp::subdoc::doc_flag::Add;
    if ((flags_ & ~validFlags) == cb::mcbp::subdoc::doc_flag::None) {
        doc_flags = doc_flags | flags_;
    } else {
        throw std::invalid_argument("addDocFlags: flags_ (which is " +
                                    to_string(flags_) + ") is not a doc flag");
    }
}

void SubdocMultiCmd::populate_header(protocol_binary_request_header& header,
                                     size_t bodylen) const {
    header.request.setMagic(cb::mcbp::Magic::ClientRequest);
    header.request.setOpcode(command);
    header.request.setKeylen(gsl::narrow<uint16_t>(key.size()));
    header.request.setExtlen(((expiry != 0 || encode_zero_expiry_on_wire)
                                      ? sizeof(uint32_t)
                                      : 0) +
                             (!isNone(doc_flags) ? sizeof(doc_flags) : 0));
    header.request.setDatatype(cb::mcbp::Datatype::Raw);
    /* TODO: vbucket */
    header.request.setBodylen(gsl::narrow<uint32_t>(bodylen));
    header.request.setOpaque(0xdeadbeef);
    header.request.setCas(cas);
}
