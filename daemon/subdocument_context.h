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

#include "memcached.h"

#include "connection.h"
#include "cookie.h"
#include "subdocument_traits.h"
#include "xattr/utils.h"

#include <memcached/engine.h>
#include <platform/compress.h>
#include <cstddef>
#include <iomanip>
#include <memory>
#include <unordered_map>

/**
 * The MemoryBackedBuffer may be used for a "copy on write" context
 * where we initially use it from a read-only-view, but at a later time
 * we want to reset it to point to a different backing
 */
struct MemoryBackedBuffer {
    MemoryBackedBuffer() = default;
    explicit MemoryBackedBuffer(std::string_view view) : view(view) {
    }

    void reset(std::string&& next) {
        backing.swap(next);
        view = {backing.data(), backing.size()};
        modified = true;
    }

    void swap(MemoryBackedBuffer& next) {
        reset(std::move(next.backing));
        next.view = {};
    }

    std::string_view view;

    /// Check if the buffer was modified since it was created
    bool isModified() const {
        return modified;
    }

protected:
    std::string backing;
    bool modified = false;
};

enum class MutationSemantics : uint8_t { Add, Replace, Set };

// Used to describe which xattr keys the xtoc vattr should return
enum class XtocSemantics : uint8_t { User, All };

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

    SubdocCmdContext(Cookie& cookie_,
                     const SubdocCmdTraits traits_,
                     Vbid vbucket_,
                     mcbp::subdoc::doc_flag doc_flags);

    cb::engine_errc pre_link_document(item_info& info) override;

    /**
     * Get the padded value we want to use for values with macro expansion.
     * Note that the macro name must be evaluated elsewhere as this method
     * expect the input value to be one of the legal macros.
     *
     * @param macro the name of the macro to return the padded value for
     * @return the buffer we want to pass on to subdoc instead of the macro
     *         name
     */
    std::string_view get_padded_macro(std::string_view macro);

    /// Try to expand the virtual macro and return an empty string
    /// if we don't know how to do that
    std::string_view expand_virtual_macro(std::string_view macro);

    /**
     * Generate macro padding we may use to substitute a macro with. E.g. We
     * replace "${Mutation.CAS}" or "${Mutation.seqno}" with the generated
     * padding. It needs to be wide enough so that we can do an in-place
     * replacement with the actual CAS or seqno in the pre_link_document
     * callback.
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
     * @param macro the macro for which we want to generate the padding
     *
     * @throws std::logic_error if the macro expansion size is invalid
     */
    void generate_macro_padding(std::string_view payload,
                                cb::xattr::macros::macro macro);

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

    // Cookie this command is associated with.
    Cookie& cookie;

    Connection& connection;

    // The traits for this command.
    SubdocCmdTraits traits;

    /// The vBucket for this request.
    const Vbid vbucket;

    // The expanded input JSON document. This may either refer to:
    // a). The raw engine item iovec
    // b). The 'inflated_doc_buffer' if the input document had to be
    //     inflated.
    // c). {intermediate_result} member of this object.
    // Either way, it should /not/ be cb_free()d.
    // Note this is *always* in a decompressed form (and hence can safely be
    // read / manipulated directly) - see get_document_for_searching().
    // TODO: Remove (b), and just use intermediate result.
    MemoryBackedBuffer in_doc;

    // Temporary buffer used to hold the xattrs in use, as a get request
    // may hold pointers into the repacked xattr buckets
    std::unique_ptr<char[]> xattr_buffer;

    // CAS value of the input document. Required to ensure we only store a
    // new document which was derived from the same original input document.
    uint64_t in_cas = 0;

    // Flags of the input document. Required so we can set the same flags to
    // to the new document, so flags are unchanged by subdoc.
    uint32_t in_flags = 0;

    // The datatype for the document currently held in `in_doc`. This
    // is used to set the new documents datatype.
    // Note: If the original input was Snappy compressed; it will be
    // decompressed during fetch (by get_document_for_searching()) - as such
    // this field will never have the Snappy bit set.
    protocol_binary_datatype_t in_datatype = PROTOCOL_BINARY_RAW_BYTES;

    // The state of the document currently held in `in_doc`. This is used
    // to to set the new documents state.
    DocumentState in_document_state = DocumentState::Alive;

    // True if this operation has been successfully executed (via subjson)
    // and we have valid result.
    bool executed = false;

    // [Mutations only] The type of the root element, if flags & FLAG_MKDOC
    jsonsl_type_t jroot_type = JSONSL_T_ROOT;

    // [Mutations only] True if the doc does not exist and an insert (rather
    // than replace) is required.
    bool needs_new_doc = false;

    // Overall status of the entire command.
    // For single-path commands this is simply the same as the first (and only)
    // opetation, for multi-path it's an aggregate status.
    cb::mcbp::Status overall_status = cb::mcbp::Status::Success;

    // [Mutations only] Mutation sequence number and vBucket UUID. Only set
    // if the calling connection has the MUTATION_SEQNO feature enabled; to be
    // included in the response back to the client.
    uint64_t vbucket_uuid = 0;
    uint64_t sequence_no = 0;

    // [Mutations only] Size in bytes of the new item to store into engine.
    // Held in the context so upon success we can update statistics.
    size_t out_doc_len = 0;

    // [Mutations only] New item to store into engine.
    cb::unique_item_ptr out_doc;

    // Size in bytes of the response value to send back to the client.
    size_t response_val_len = 0;

    // Set to true if one (or more) of the xattr operation wants to do
    // macro expansion.
    bool do_macro_expansion = false;

    // Set to true if we want to operate on deleted documents
    const bool do_allow_deleted_docs;

    // Set to true if we want to delete the document after modifying it
    bool do_delete_doc = false;

    // true if there are no system xattrs after the operation. In
    // reality this means we do a bucket_remove rather than a bucket_update
    bool no_sys_xattrs = false;

    XtocSemantics xtocSemantics = XtocSemantics::User;

    // If we need to create a new document (Add / Set), what state should
    // that document be created in?
    const DocumentState createState;

    // The semantics to use when mutating the document in the Bucket.
    const MutationSemantics mutationSemantics;

    // Should we try to revive the document as part of operating on
    // a deleted document
    const bool reviveDocument;

    /* Specification of a single path operation. Encapsulates both the request
     * parameters, and (later) the result of the operation.
     */
    class OperationSpec {
    public:
        OperationSpec(SubdocCmdTraits traits_,
                      protocol_binary_subdoc_flag flags_,
                      std::string path_,
                      std::string value_ = {});

        // Move constructor.
        OperationSpec(OperationSpec&& other);

        // The traits of this individual Operation.
        SubdocCmdTraits traits;

        // The flags set for this individual Operation
        protocol_binary_subdoc_flag flags;

        // Path to operate on.
        const std::string path;

        // [For mutations only] Value to apply to document.
        const std::string value;

        // Status code of the operation.
        cb::mcbp::Status status;

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
    std::string_view get_xattr_key() {
        if (xattr_key.empty()) {
            return {};
        }
        return {xattr_key.data(), xattr_key.size()};
    }

    /**
     * Set the xattr key being accessed in this context. Only one
     * xattr key is allowed in each multi op
     *
     * @param key the key to be accessed
     */
    void set_xattr_key(std::string_view key) {
        xattr_key = {key.data(), key.size()};
    }

    /**
     * Get the document containing all of the virtual attributes for
     * the document. The storage is created the first time the method is called,
     * and reused for the rest of the lifetime of the context.
     */
    std::string_view get_document_vattr();

    /**
     * Get the document containing all of the virtual attributes for
     * the vbucket requested.
     * The storage is created the first time the method is called,
     * and reused for the rest of the lifetime of the context.
     */
    std::string_view get_vbucket_vattr();

    /*
     * Get the xtoc document which contains a list of xattr keys that exist for
     * the document.
     * The storage is created the first time the method is called,
     * and reused for the rest of the lifetime of the context.
     */
    std::string_view get_xtoc_vattr();

    // This is the item info for the item we've fetched from the
    // database
    item_info& getInputItemInfo() {
        return input_item_info;
    }

    /**
     * Initialize all of the internal input variables with a
     * flat, uncompressed JSON document ready for performing a subjson
     * operation on it.
     *
     * @param client_cas The CAS provided by the client (which should be
     *                   used for updates to the document
     *
     * @return cb::mcbp::Status::Success for success, otherwise an
     *         error code which should be returned to the client immediately
     *         (and stop executing of the command)
     */
    cb::mcbp::Status get_document_for_searching(uint64_t client_cas);

    /**
     * The result of subdoc_fetch.
     */
    cb::unique_item_ptr fetchedItem;

private:
    // Temporary buffer to hold the inflated content in case of the
    // document in the engine being compressed. It is only kept here
    // to avoid an extra memory allocation and copy (in_doc will reference
    // this section initially until someone tries to modify it.. at that
    // time in_doc points to a temporary buffer)
    cb::compression::Buffer inflated_doc_buffer;

    // The item info representing the input document
    item_info input_item_info = {};

    // The array containing all of the operations requested by the user.
    // Each element in the array contains the operations which should be
    // run in each phase. Use `getOperations()` to get the correct entry
    // in this array as that method contains the logic of where each
    // phase lives.
    std::array<Operations, 2> operations;

    // The phase we're currently operating in
    Phase currentPhase = Phase::XATTR;

    template <typename T>
    std::string macroToString(cb::xattr::macros::macro macro, T macroValue);

    /**
     * Check whether or not the SubdocCmdContext contains a given macro
     * @param macro The macro we are checking for
     * @return True if the macro exists, False otherwise
     */
    bool containsMacro(cb::xattr::macros::macro macro) ;

    void substituteMacro(cb::xattr::macros::macro macroName,
                         const std::string& macroValue,
                         cb::char_buffer& value);

    /**
     * Returns the value CRC32C of the document processed by the current
     * subdoc context
     */
    uint32_t computeValueCRC32C();

    std::string_view expand_virtual_document_macro(std::string_view macro);

    void create_single_path_context(mcbp::subdoc::doc_flag doc_flags);

    void create_multi_path_context(mcbp::subdoc::doc_flag doc_flags);

    // The xattr key being accessed in this command
    std::string xattr_key;

    using MacroPair = std::pair<std::string_view, std::string>;
    std::vector<MacroPair> paddedMacros;

    std::string document_vattr;
    std::string vbucket_vattr;
    std::string xtoc_vattr;

    std::vector<std::string> expandedVirtualMacrosBackingStore;
}; // class SubdocCmdContext
