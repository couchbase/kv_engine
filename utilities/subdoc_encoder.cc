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

#include "config.h"

#include "subdoc_encoder.h"

std::vector<char> SubdocMultiLookupCmd::encode() const {
    std::vector<char> request;

    // Reserve space for the header and setup pointer to it (we fill in the
    // details once the rest of the packet is encoded).
    request.resize(sizeof(protocol_binary_request_header));

    size_t bodylen = 0;

    // Add the key.
    std::copy(key.begin(), key.end(), back_inserter(request));
    bodylen += key.size();

    // Add all lookup specs.
    for (const auto& s : specs) {
        union {
            protocol_binary_subdoc_multi_lookup_spec spec;
            char bytes[sizeof(protocol_binary_subdoc_multi_lookup_spec)];
        } encoded;
        encoded.spec.opcode = s.opcode;
        encoded.spec.flags = s.flags;
        encoded.spec.pathlen = htons(s.path.size());

        std::copy(&encoded.bytes[0],
                  &encoded.bytes[sizeof(encoded.spec)],
                  back_inserter(request));
        std::copy(s.path.begin(), s.path.end(), back_inserter(request));
        bodylen += sizeof(encoded.spec) + s.path.size();
    }

    // Populate the header.
    auto* header = reinterpret_cast<protocol_binary_request_header*>
        (request.data());
    populate_header(*header, bodylen);

    return request;
}

std::vector<char> SubdocMultiMutationCmd::encode() const {
    std::vector<char> request;

    // Reserve space for the header and setup pointer to it (we fill in the
    // details once the rest of the packet is encoded).
    request.resize(sizeof(protocol_binary_request_header));

    size_t bodylen = 0;

    // Add the key.
    std::copy(key.begin(), key.end(), back_inserter(request));
    bodylen += key.size();

    // Add all lookup specs.
    for (const auto& s : specs) {
        union {
            protocol_binary_subdoc_multi_mutation_spec spec;
            char bytes[sizeof(protocol_binary_subdoc_multi_mutation_spec)];
        } encoded;
        encoded.spec.opcode = s.opcode;
        encoded.spec.flags = s.flags;
        encoded.spec.pathlen = htons(s.path.size());
        encoded.spec.valuelen = htonl(s.value.size());

        std::copy(&encoded.bytes[0],
                  &encoded.bytes[sizeof(encoded.spec)],
                  back_inserter(request));
        std::copy(s.path.begin(), s.path.end(), back_inserter(request));
        std::copy(s.value.begin(), s.value.end(), back_inserter(request));
        bodylen += sizeof(encoded.spec) + s.path.size() + s.value.size();
    }

    // Populate the header.
    auto* header = reinterpret_cast<protocol_binary_request_header*>
        (request.data());

    populate_header(*header, bodylen);

    return request;
}

void SubdocMultiCmd::populate_header(protocol_binary_request_header& header,
                                     size_t bodylen) const {
    header.request.magic = PROTOCOL_BINARY_REQ;
    header.request.opcode = command;
    header.request.keylen = htons(key.size());
    header.request.extlen = 0;
    header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    /* TODO: vbucket */
    header.request.bodylen = htonl(bodylen);
    header.request.opaque = 0xdeadbeef;
    header.request.cas = cas;
}
