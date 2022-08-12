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
#pragma once

#include <memcached/protocol_binary.h>

#include <string>
#include <vector>

/*
 * Sub-document API encoding helpers
 */

// Abstract base class for multi lookup / mutation command encoding.
struct SubdocMultiCmd {
    explicit SubdocMultiCmd(cb::mcbp::ClientOpcode command_)
        : cas(0),
          expiry(0),
          encode_zero_expiry_on_wire(false),
          command(command_),
          doc_flags(cb::mcbp::subdoc::doc_flag::None) {
    }

    std::string key;
    uint64_t cas;
    uint32_t expiry;

    // If true then a zero expiry will actually be encoded on the wire (as
    // zero), as opposed to the normal behaviour of indicating zero by the
    // absence of the 'expiry' field in extras.
    bool encode_zero_expiry_on_wire;

    cb::mcbp::ClientOpcode command;

    void addDocFlag(cb::mcbp::subdoc::doc_flag doc_flag);

    virtual std::vector<char> encode() const = 0;

protected:
    // Helper for encode() - fills in elements common to both lookup and
    // mutation.
    std::vector<char> encode_common() const;

    // Fill in the header for this command.
    void populate_header(protocol_binary_request_header& header,
                         size_t bodylen) const;
    cb::mcbp::subdoc::doc_flag doc_flags;
};

/* Sub-document API MULTI_LOOKUP command */
struct SubdocMultiLookupCmd : public SubdocMultiCmd {
    SubdocMultiLookupCmd()
        : SubdocMultiCmd(cb::mcbp::ClientOpcode::SubdocMultiLookup) {
    }

    struct LookupSpec {
        cb::mcbp::ClientOpcode opcode;
        protocol_binary_subdoc_flag flags;
        std::string path;
    };
    std::vector<LookupSpec> specs;

    /* Takes the current state of object and encodes a
     * protocol_binary_request_subdocument_multi_lookup packet in network order.
     */
    std::vector<char> encode() const override;
};

/* Sub-document API MULTI_MUTATION command */
struct SubdocMultiMutationCmd : public SubdocMultiCmd {
    SubdocMultiMutationCmd()
        : SubdocMultiCmd(cb::mcbp::ClientOpcode::SubdocMultiMutation) {
    }

    struct LookupSpec {
        cb::mcbp::ClientOpcode opcode;
        protocol_binary_subdoc_flag flags;
        std::string path;
        std::string value;
    };
    std::vector<LookupSpec> specs;

    /* Takes the current state of object and encodes a
     * protocol_binary_request_subdocument_multi_lookup packet in network order.
     */
    std::vector<char> encode() const override;
};
