/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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

#include "memcached.h"

#include "memcached/buffer.h"
#include "subdocument_traits.h"
#include "xattr_utils.h"

#include <cstddef>
#include <iomanip>
#include <memory>
#include <platform/compress.h>

/** Subdoc command context. An instance of this exists for the lifetime of
 *  each subdocument command, and it used to hold information which needs to
 *  persist across calls to subdoc_executor; for example when one or more
 *  engine functions return EWOULDBLOCK and hence the executor needs to be
 *  retried.
 */
class SubdocCmdContext : public CommandContext {
public:
    /**
     * All subdoc access happens in two phases... First we'll run through
     * all of the operations on the extended attributes, then we'll run
     * over all of the ones in the body.
     */
    enum class Phase : uint8_t {
        XATTR,
        Body
    };

    class OperationSpec;
    typedef std::vector<OperationSpec> Operations;

    SubdocCmdContext(McbpConnection& connection_, const SubdocCmdTraits traits_)
      : connection(connection_),
        traits(traits_),
        in_doc(),
        in_cas(0),
        in_flags(0),
        in_datatype(PROTOCOL_BINARY_RAW_BYTES),
        in_document_state(DocumentState::Alive),
        executed(false),
        jroot_type(JSONSL_T_ROOT),
        needs_new_doc(false),
        out_doc_len(0),
        out_doc(),
        response_val_len(0),
        do_macro_expansion(false),
        do_allow_deleted_docs(false),
        currentPhase(Phase::XATTR){}

    virtual ~SubdocCmdContext() {
        if (out_doc != NULL) {
            auto engine = connection.getBucketEngine();
            engine->release(reinterpret_cast<ENGINE_HANDLE*>(engine),
                            &connection, out_doc);
        }
    }

    ENGINE_ERROR_CODE pre_link_document(item_info& info) override;

    /**
     * Get the padded value we want to use for values with macro expansion.
     * Note that the macro name must be evaluated elsewhere as this method
     * expect the input value to be one of the legal macros.
     *
     * @param macro the name of the macro to return the padded value for
     * @return the buffer we want to pass on to subdoc instead of the macro
     *         name
     */
    cb::const_char_buffer get_padded_macro(const cb::const_char_buffer macro);

    /**
     * Generate a cas padding we may use to substitute "${Mutation.CAS}" with.
     * it needs to be wide enough so that we can do an in-place replacement
     * with the actual CAS in the pre_link_document callback.
     *
     * We can't really use a hardcoded value (as we would limit the user
     * for what they could inject in their documents, and we don't want to
     * suddenly replace user data with a cas value ;).
     *
     * This method tries to generate a string, and then scans through the
     * supplied payload to ensure that it isn't present there before
     * scanning through all of the values in the xattr modidications to
     * ensure that it isn't part of any of them either.
     *
     * You might think: oh, why don't you just store the pointers to where
     * in the blob we injected the macro? The problem with that is that
     * there isn't any restrictions on the order you may specify the
     * mutations in a multiop, so that you could move the stuff around;
     * replace it; delete it. That means that you would have to go
     * through and relocate all of these offsets after each mutation.
     * Not impossible, but I don't think it would simplify the logic
     * that much ;-)
     *
     * @param payload the JSON value for the xattr to perform macro
     *                substitution in
     */
    void generate_cas_padding(const cb::const_char_buffer payload);

    Operations& getOperations(const Phase phase) {
        switch (phase) {
        case Phase::Body:
            return operations[0];
        case Phase::XATTR:
            return operations[1];
        }
        throw std::invalid_argument("SubdocCmdContext::getOperations() invalid phase");
    }

    Operations& getOperations() {
        return getOperations(currentPhase);
    }

    Phase getCurrentPhase() {
        return currentPhase;
    }

    void setCurrentPhase(Phase phase) {
        currentPhase = phase;
    }

    // Returns the total size of all Operation values (bytes).
    uint64_t getOperationValueBytesTotal() const;

    // Cookie this command is associated with. Needed for the destructor
    // to release items.
    McbpConnection& connection;

    // The traits for this command.
    SubdocCmdTraits traits;

    // The expanded input JSON document. This may either refer to:
    // a). The raw engine item iovec
    // b). The 'inflated_doc_buffer' if the input document had to be
    //     inflated.
    // c). {intermediate_result} member of this object.
    // Either way, it should /not/ be cb_free()d.
    // TODO: Remove (b), and just use intermediate result.
    const_char_buffer in_doc;

    // Temporary buffer to hold the inflated content in case of the
    // document in the engine being compressed
    cb::compression::Buffer inflated_doc_buffer;


    // Temporary buffer used to hold the intermediate result document for
    // multi-path mutations. {in_doc} is then updated to point to this to use
    // as input for the next multi-path mutation.
    std::unique_ptr<char[]> temp_doc;

    // CAS value of the input document. Required to ensure we only store a
    // new document which was derived from the same original input document.
    uint64_t in_cas;

    // Flags of the input document. Required so we can set the same flags to
    // to the new document, so flags are unchanged by subdoc.
    uint32_t in_flags;

    // The datatype for the document currently held in `in_doc`. This
    // is used to set the new documents datatype.
    protocol_binary_datatype_t in_datatype;

    // The state of the document currently held in `in_doc`. This is used
    // to to set the new documents state.
    DocumentState in_document_state;

    // True if this operation has been successfully executed (via subjson)
    // and we have valid result.
    bool executed;

    // [Mutations only] The type of the root element, if flags & FLAG_MKDOC
    jsonsl_type_t jroot_type;
    // [Mutations only] True if the doc does not exist and an insert (rather
    // than replace) is required.
    bool needs_new_doc;

    // Overall status of the entire command.
    // For single-path commands this is simply the same as the first (and only)
    // opetation, for multi-path it's an aggregate status.
    protocol_binary_response_status overall_status;

    // [Mutations only] Mutation sequence number and vBucket UUID. Only set
    // if the calling connection has the MUTATION_SEQNO feature enabled; to be
    // included in the response back to the client.
    uint64_t vbucket_uuid;
    uint64_t sequence_no;

    // [Mutations only] Size in bytes of the new item to store into engine.
    // Held in the context so upon success we can update statistics.
    size_t out_doc_len;

    // [Mutations only] New item to store into engine. _Must_ be released
    // back to the engine using ENGINE_HANDLE_V1::release()
    item* out_doc;

    // Size in bytes of the response value to send back to the client.
    size_t response_val_len;

    // Set to true if one (or more) of the xattr operation wants to do
    // macro expansion.
    bool do_macro_expansion;

    // Set to true if we want to operate on deleted documents
    bool do_allow_deleted_docs;

    /* Specification of a single path operation. Encapsulates both the request
     * parameters, and (later) the result of the operation.
     */
    class OperationSpec {
    public:
        // Constructor for lookup operations (no value).
        OperationSpec(SubdocCmdTraits traits_,
                      protocol_binary_subdoc_flag flags_,
                      const_char_buffer path_);

        // Constructor for operations requiring a value.
        OperationSpec(SubdocCmdTraits traits_,
                      protocol_binary_subdoc_flag flags_,
                      const_char_buffer path_,
                      const_char_buffer value_);

        // Move constructor.
        OperationSpec(OperationSpec&& other);

        // The traits of this individual Operation.
        SubdocCmdTraits traits;

        // The flags set for this individual Operation
        protocol_binary_subdoc_flag flags;

        // Path to operate on. Owned by the original request packet.
        const_char_buffer path;

        // [For mutations only] Value to apply to document. Owned by the
        // original request packet.
        const_char_buffer value;

        // Status code of the operation.
        protocol_binary_response_status status;

        // Result of this operation, to be returned back to the client (for
        // operations which return a result).
        Subdoc::Result result;
    };

    /**
     * Get the xattr key being accessed in this context. Only one
     * xattr key is allowed in each multi op
     *
     * @return the key
     */
    cb::const_byte_buffer get_xattr_key() {
        return xattr_key;
    }

    /**
     * Set the xattr key being accessed in this context. Only one
     * xattr key is allowed in each multi op
     *
     * @param key the key to be accessed
     */
    void set_xattr_key(const cb::const_byte_buffer& key) {
        xattr_key = key;
    }

private:
    // The array containing all of the operations requested by the user.
    // Each element in the array contains the operations which should be
    // run in each phase. Use `getOperations()` to get the correct entry
    // in this array as that method contains the logic of where each
    // phase lives.
    std::array<Operations, 2> operations;

    // The phase we're currently operating in
    Phase currentPhase;

    // The xattr key being accessed in this command
    cb::const_byte_buffer xattr_key;

    std::string padded_cas_macro;
}; // class SubdocCmdContext
