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

/**
 * Base class for multi lookup / mutation command encoding.
 */
struct SubdocMultiCmd {
    /**
     * A single lookup or mutation spec.
     */
    struct Spec {
        cb::mcbp::ClientOpcode opcode;
        cb::mcbp::subdoc::PathFlag flags;
        std::string path;
        /**
         * The value for the spec. It is only encoded for specs where
         * SubdocMultiCmd::spec_has_value() returns true, and must be empty
         * for all other specs.
         */
        std::string value = {};

        /**
         * Encode this spec in its on-the-wire format and append it to the
         * provided buffer.
         *
         * The spec is encoded as: opcode (1 byte), flags (1 byte), path
         * length (2 bytes), and if has_value is set the value length (4
         * bytes), followed by the path and then the value (if has_value
         * is set). All integer fields are encoded in network byte order.
         *
         * @param destination The buffer to append the encoded spec to
         * @param has_value If the value length and value should be encoded
         * @throws gsl::fail_fast if has_value is false and value isn't empty
         */
        void encode(std::vector<char>& destination, bool has_value) const;
        /**
         * Get the number of bytes encode() appends for this spec.
         *
         * @param has_value If the value length and value should be encoded
         * @return The size of the encoded spec
         */
        size_t encoded_size(bool has_value) const;
    };

    /**
     * The key of the document to operate on.
     */
    std::string key;

    /**
     * The CAS value to put in the request header (0 means no CAS check).
     */
    uint64_t cas = 0;

    /**
     * The expiry time for the document. It is only encoded in the extras
     * if it is non-zero, or if encode_zero_expiry_on_wire is set.
     */
    uint32_t expiry = 0;

    /**
     * The vbucket to put in the request header.
     */
    Vbid vbid{0};

    /**
     * If true then a zero expiry will actually be encoded on the wire (as
     * zero), as opposed to the normal behaviour of indicating zero by the
     * absence of the 'expiry' field in extras.
     */
    bool encode_zero_expiry_on_wire = false;

    /**
     * The opcode to put in the request header.
     */
    cb::mcbp::ClientOpcode command{cb::mcbp::ClientOpcode::Invalid};

    /**
     * The specs to encode in the body of the request.
     */
    std::vector<Spec> specs;

    /**
     * Add a document flag to the command. The flags are encoded in the
     * extras if any flag is set.
     *
     * @param flag The flag to add (it is OR'ed into any flags already set)
     */
    void addDocFlag(cb::mcbp::subdoc::DocFlag flag);

    /**
     * Encode the current state of the object as a complete request packet
     * (header, extras, key and specs) in network byte order.
     *
     * @return The encoded request packet
     */
    std::vector<char> encode() const;

protected:
    /**
     * Create a new command.
     *
     * @param command_ The opcode to put in the request header
     */
    explicit SubdocMultiCmd(cb::mcbp::ClientOpcode command_)
        : command(command_) {
    }

    /**
     * Check if the provided spec should be encoded with a value (and use
     * the protocol_binary_subdoc_multi_mutation_spec layout). This mirrors
     * how the server decodes the specs: all specs in a multi mutation
     * carry a value, whereas specs in a multi lookup currently don't.
     *
     * The decision is made per spec (rather than per command) to allow
     * for lookup specs which carry a value. Currently it only depends on
     * the command.
     *
     * @param spec The spec to check
     * @return true if the value length and value should be encoded
     */
    bool spec_has_value(const Spec& spec) const;

    /**
     * Helper for encode() - fills in elements common to both lookup and
     * mutation.
     *
     * The returned buffer has capacity for the header, extras, key and
     * the specified number of bytes for the specs, so that the caller can
     * append the encoded specs without causing a reallocation.
     *
     * @param specs_size The total number of bytes the caller will append
     *                   for the encoded specs
     * @return A buffer containing space for the header followed by the
     *         extras and the key
     */
    std::vector<char> encode_common(size_t specs_size) const;

    /**
     * Get the length of the extras section for this command.
     *
     * @return The number of bytes of extras (expiry and/or doc flags)
     */
    size_t get_extlen() const;

    /**
     * Fill in the request header for this command.
     *
     * @param header The header to populate
     * @param bodylen The length of the body (extras, key and specs)
     */
    void populate_header(cb::mcbp::Request& header, size_t bodylen) const;

    /**
     * The document flags to encode in the extras (see addDocFlag()).
     */
    cb::mcbp::subdoc::DocFlag doc_flags = cb::mcbp::subdoc::DocFlag::None;
};

/** Sub-document API MULTI_LOOKUP command */
struct SubdocMultiLookupCmd : SubdocMultiCmd {
    SubdocMultiLookupCmd()
        : SubdocMultiCmd(cb::mcbp::ClientOpcode::SubdocMultiLookup) {
    }
};

/** Sub-document API MULTI_MUTATION command */
struct SubdocMultiMutationCmd : SubdocMultiCmd {
    SubdocMultiMutationCmd()
        : SubdocMultiCmd(cb::mcbp::ClientOpcode::SubdocMultiMutation) {
    }
};
