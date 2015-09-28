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
#pragma once

#include "config.h"

#include <memcached/protocol_binary.h>

#include <string>
#include <vector>

/*
 * Sub-document API encoding helpers
 */

// Abstract base class for multi lookup / mutation command encoding.
struct SubdocMultiCmd {

    SubdocMultiCmd(protocol_binary_command command_)
        : cas(0),
          expiry(0),
          encode_zero_expiry_on_wire(false),
          command(command_) {}

    std::string key;
    uint64_t cas;
    uint32_t expiry;

    // If true then a zero expiry will actually be encoded on the wire (as zero),
    // as opposed to the normal behaviour of indicating zero by the absence of
    // the 'expiry' field in extras.
    bool encode_zero_expiry_on_wire;

    protocol_binary_command command;

    virtual std::vector<char> encode() const = 0;

protected:
    // Helper for encode() - fills in elements common to both lookup and
    // mutation.
    std::vector<char> encode_common() const;

    // Fill in the header for this command.
    void populate_header(protocol_binary_request_header& header,
                         size_t bodylen) const;
};

/* Sub-document API MULTI_LOOKUP command */
struct SubdocMultiLookupCmd : public SubdocMultiCmd {

    SubdocMultiLookupCmd()
        : SubdocMultiCmd(PROTOCOL_BINARY_CMD_SUBDOC_MULTI_LOOKUP) {}

    struct LookupSpec {
        protocol_binary_command opcode;
        protocol_binary_subdoc_flag flags;
        std::string path;
    };
    std::vector<LookupSpec> specs;

    /* Takes the current state of object and encodes a
     * protocol_binary_request_subdocument_multi_lookup packet in network order.
     */
    std::vector<char> encode() const;
};

/* Sub-document API MULTI_MUTATION command */
struct SubdocMultiMutationCmd : public SubdocMultiCmd {

    SubdocMultiMutationCmd()
        : SubdocMultiCmd(PROTOCOL_BINARY_CMD_SUBDOC_MULTI_MUTATION) {}

    struct LookupSpec {
        protocol_binary_command opcode;
        protocol_binary_subdoc_flag flags;
        std::string path;
        std::string value;
    };
    std::vector<LookupSpec> specs;

    /* Takes the current state of object and encodes a
     * protocol_binary_request_subdocument_multi_lookup packet in network order.
     */
    std::vector<char> encode() const;
};
