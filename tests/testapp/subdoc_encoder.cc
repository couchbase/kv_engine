/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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

/*
 * Sub-document API encoding helpers
 *
 */

#include "subdoc_encoder.h"

#include <gsl/gsl>
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

void SubdocMultiCmd::addDocFlag(mcbp::subdoc::doc_flag flags_) {
    static const mcbp::subdoc::doc_flag validFlags =
            mcbp::subdoc::doc_flag::Mkdoc |
            mcbp::subdoc::doc_flag::AccessDeleted | mcbp::subdoc::doc_flag::Add;
    if ((flags_ & ~validFlags) == mcbp::subdoc::doc_flag::None) {
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
