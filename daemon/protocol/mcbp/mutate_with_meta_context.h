/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "steppable_command_context.h"

#include <mcbp/codec/mutate_with_meta_payload.h>
#include <mcbp/codec/with_meta_options.h>
#include <memcached/engine.h>
#include <variant>

/**
 * The MutateWithMetaCommandContext is a state machine used by the memcached
 * core to implement MutateWithMeta command. MutateWithMeta differs from
 * Add/Set/DeleteWithMeta in two ways:
 *
 *  1. The metadata is specified in JSON and not binary encoded where each
 *     value depends on the offset it is located at (and the size of the
 *     extras section determines which values to look for)
 *  2. The command may perform pre-link document processing to replace segments
 *     in xattrs with new values for CAS and sequence number, which is not
 *     supported by the Add/Set/DeleteWithMeta commands and the motivation
 *     for creating the new command.
 *
 * For both CAS and sequence number replacement, the client provides offsets
 * within the document where the values should be written. The pre-link document
 * processing will then write the actual CAS and sequence number values at the
 * specified locations before storing the item.
 *
 * The command goes through the following states:
 * - ValidateInput: Validate the input parameters provided by the user
 * - StoreItem: Store the item in the underlying engine
 * - SendResponse: Send the response back to the user
 *
 * The command may also perform pre-link document processing to replace
 * segments in xattrs with new values for CAS and sequence number.
 *
 * The extras segment of the command contains a 4 byte integer which
 * specifies the number of bytes at the end of the value which contains
 * the metadata for the operation. The metadata is JSON encoded and may
 * contain options for the operation, as well as optional CAS and sequence
 * number pattern strings:
 *
 *     {
 *       "cas": "0x0000000000000001",
 *       "expiration": "0x00000000",
 *       "flags": "0xdeadbeef",
 *       "rev_seqno": "0x0000000000000001",
 *       "command": "add",
 *       "options": "0x0000000c",
 *       "cas_offsets": [10, 50, 100],
 *       "seqno_offsets": [30, 70, 120]
 *     }
 *
 * The "cas", "expiration", "flags", and "rev_seqno" fields specify the metadata
 * for the item to be stored. The values may be in hexadecimal string format
 * (with a 0x prefix, case won't matter), or as decimal strings (or JSON
 * numbers, but beware that 64 bit numbers may overflow the JSON number
 * representation in certain JSON decoders. Use strings to be safe)).
 *
 * The "command" field may be "add", "set", or "delete" and indicates how
 * the item should be stored.
 *
 * The "options" field is a bitmask of:
 *   - REGENERATE_CAS (0x04)
 *   - SKIP_CONFLICT_RESOLUTION_FLAG (0x08)
 *   - FORCE_ACCEPT_WITH_META_OPS (0x20)
 *   - IS_EXPIRATION (0x10) - only valid for delete operations
 *   (this is the same value as passed to Add/Set/DeleteWithMeta commands)
 *
 * The "cas_offsets" and "seqno_offsets" fields are optional arrays of byte
 * offsets within the document where the CAS and sequence number values
 * (as 18-byte hex strings with 0x prefix using lowercase for the any letters)
 * should be written.
 */
class MutateWithMetaCommandContext : public SteppableCommandContext {
public:
    enum class State : uint8_t { ValidateInput, StoreItem, SendResponse };

    MutateWithMetaCommandContext(Cookie& cookie);

    cb::engine_errc pre_link_document(item_info&) override;

protected:
    cb::engine_errc step() override;
    cb::engine_errc validateInput();
    cb::engine_errc storeItem();

private:
    /// Parse the provided JSON and perform basic validation of their values
    /// (that they are in the correct format and that the required fields are
    /// present. Note that this does not perform validation of the actual
    /// values, such as checking that the values provided match the options
    /// specified for the command etc). The error context will be set with
    /// more details if the parsing or validation fails.
    std::variant<cb::engine_errc, cb::mcbp::request::MutateWithMetaPayload>
    parseJson();

    /// Validate that a CAS is only present when GenerateCas is set to NO and
    /// that the CAS pattern and sequence number is only present when
    /// GenerateCas is set to YES. The error context will be set with more
    /// details if the validation fails.
    cb::engine_errc validateGenerateCasOptions(
            const cb::mcbp::request::MutateWithMetaPayload& payload);

    /// Validate that the if a pattern is provided the document isn't
    /// compressed and contains an XATTR section
    cb::engine_errc validateOffsetRequirements(
            std::string_view value, protocol_binary_datatype_t datatype);

    /// Validate that the document don't contain user xattrs and that the
    /// document body is empty
    cb::engine_errc validateDeleteCommandRequirements(
            std::string_view value, protocol_binary_datatype_t datatype);

    /**
     * Get the size of the metadata segment at the end of the value from the
     * extras field of the command.
     */
    std::size_t getMetaSize() const;
    /**
     * Get the document data to store (i.e. the part of the value which is not
     * the metadata segment at the end of the value). This is the part of the
     * value which should be stored as the document value, and the metadata
     * segment at the end of the value should be ignored by the underlying
     * engine.
     */
    std::string_view getDocumentData() const;

    ItemMetaData item_meta;
    cb::mcbp::WithMetaOptions options;
    cb::mcbp::request::MutateWithMetaCommand command =
            cb::mcbp::request::MutateWithMetaCommand::Add;
    std::vector<std::size_t> cas_offsets;
    std::vector<std::size_t> seqno_offsets;

    /// The mutation descriptor for the mutation to return back to the client
    /// upon success
    mutation_descr_t mutation_descr{};

    /// The current state
    State state = State::ValidateInput;
};
