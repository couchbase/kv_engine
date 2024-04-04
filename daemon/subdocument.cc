/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "subdocument.h"

#include "buckets.h"
#include "concurrency_semaphores.h"
#include "front_end_thread.h"
#include "mcaudit.h"
#include "one_shot_limited_concurrency_task.h"
#include "protocol/mcbp/engine_wrapper.h"
#include "subdocument_context.h"
#include "subdocument_parser.h"
#include "subdocument_traits.h"

#include <gsl/gsl-lite.hpp>
#include <logger/logger.h>
#include <memcached/durability_spec.h>
#include <memcached/types.h>
#include <platform/histogram.h>
#include <platform/scope_timer.h>

static const std::array<SubdocExecutionContext::Phase, 2> phases{
        {SubdocExecutionContext::Phase::XATTR,
         SubdocExecutionContext::Phase::Body}};

using namespace cb::mcbp::subdoc;

/******************************************************************************
 * Subdocument executors
 *****************************************************************************/

/**
 * The SubdocCommandContext class is the command context containing all of
 * the data which is required for the entire duration of the execution of
 * a subdoc command. It holds the old SubdocExecutionContext object which holds
 * all of the state logic needed for a single execution of the subdoc
 * operation (it gets reset every time there is a CAS collision causing the
 * entire operation to be retried). The old SubdocExecutionContext should be
 * renamed, but treat that separately from this patch.
 *
 * It'll eventually be refactored into a steppable command context which
 * utilize the executor pools to perform the actual subdoc transformations
 */
class SubdocCommandContext : public CommandContext {
public:
    SubdocCommandContext(Cookie& cookie, SubdocCmdTraits traits)
        : cookie(cookie),
          traits(std::move(traits)),
          auto_retry_mode(cookie.getRequest().getCas() ==
                          cb::mcbp::cas::Wildcard),
          preserveTtl(getPreserveTtl(cookie)),
          expiration(getExpiration(cookie, this->traits)),
          doc_flags(getDocFlags(cookie, this->traits)) {
    }

    /**
     * Main function which handles execution of all sub-document
     * commands: fetches, operates on, updates and finally responds to the
     * client.
     */
    void drive() {
        auto aio_status = cookie.swapAiostat(cb::engine_errc::success);

        do {
            switch (state) {
            case State::CreateContext:
                do_create_context();
                break;
            case State::FetchItem:
                if (do_fetch_item(aio_status)) {
                    aio_status = cb::engine_errc::success;
                } else {
                    return;
                }
                break;
            case State::ExecuteSpecInFrontendThread:
                do_execute_spec_in_frontend_thread();
                break;
            case State::AllocateNewItem:
                if (do_allocate_new_item(aio_status)) {
                    aio_status = cb::engine_errc::success;
                } else {
                    return;
                }
                break;
            case State::UpdateItem:
                if (do_update_item(aio_status)) {
                    aio_status = cb::engine_errc::success;
                } else {
                    return;
                }
                break;
            case State::SendResponse:
                do_send_response();
                return;

            case State::Done:
                return;

            case State::InitiateShutdown:
                do_initiate_shutdown();
                return;
            }

        } while (true);
    }

    cb::engine_errc pre_link_document(item_info& info) override {
        if (execution_context) {
            return execution_context->pre_link_document(info);
        }
        return cb::engine_errc::success;
    }

protected:
    enum class State {
        CreateContext,
        FetchItem,
        ExecuteSpecInFrontendThread,
        AllocateNewItem,
        UpdateItem,
        SendResponse,
        Done,
        InitiateShutdown
    };
    State state{State::CreateContext};

    /// Callback implementing CreateContext state. Create the execution
    /// context and move to FetchItem
    void do_create_context();

    /// Callback implementing FetchItem. It may be called multiple times
    /// in the case where the engine returned EWB, and aio_status contains the
    /// value provided by the underlying engine.
    ///
    /// The method may terminate the state machinery if an error occurrs
    /// or if there is a logic error (revive a document which isn't deleted for
    /// example).
    ///
    /// Next state:
    ///     ExecuteSpecInFrontEndThread
    bool do_fetch_item(cb::engine_errc aio_status);

    /// Execute the Spec.
    /// The next state would be:
    ///    AllocateNewItem - if this is a mutation which executed successfully
    ///    SendResponse - Errors and lookup
    void do_execute_spec_in_frontend_thread();

    /// Allocate an item to store the result. The method may be called
    /// multiple times if the engine returns EWB.
    /// The state machine will terminate (and send error message) if
    /// allocation fails for any other reason than EWB.
    /// Next state:
    ///     UpdateItem for success
    bool do_allocate_new_item(cb::engine_errc aio_status);

    /// Try to update the item in the engine. The method may be called
    /// multiple times if the engine returns EWB.
    ///
    /// The state machine will terminate upon errors (and an error message
    /// returned to the client).
    ///
    /// Next state:
    ///      SendResponse for success
    ///      CreateContext if the client didn't specify CAS and the document
    ///                    was changed by another actor while we modified the
    ///                    document (and retry count < 100)
    bool do_update_item(cb::engine_errc aio_status);

    /// Send the response back to the client and terminate the state machinery
    void do_send_response();

    /// Initiate shutdown of the connection due to receiving an exception
    /// while running on the thread pool
    void do_initiate_shutdown();

    /// Check to see if we've hit the maximum of allowed retry attempts
    bool may_retry() {
        return auto_retry_mode && ++retries < cb::limits::SubdocMaxAutoRetries;
    }

    /**
     * Fetch the item to operate on from the engine.
     *
     * @return the engine error code. cb::engine_errc::success to continue
     */
    cb::engine_errc fetch_input_document(cb::engine_errc ret);

    /**
     * Update the engine with whatever modifications the subdocument command
     * made to the document.
     *
     * @return the status of the store operation
     */
    cb::engine_errc update_document(cb::engine_errc ret);

    cb::engine_errc allocate_document(cb::engine_errc ret);

    /// Send the appropriate response type back to the client
    void send_response();

    /// The cookie owning the operation
    Cookie& cookie;

    /// The traits for the current command
    const SubdocCmdTraits traits;

    /// The current context object
    std::unique_ptr<SubdocExecutionContext> execution_context;

    /// The current retry count
    size_t retries = 0;

    /// If operations should be retried or not
    const bool auto_retry_mode;

    /// If mutations should preserve TTL or not
    const bool preserveTtl;

    /// The expiration provided by the client (should be const)
    const uint32_t expiration;

    /// The doc_flags provided by the client (should be const)
    const DocFlag doc_flags;

private:
    /// Construct and send a response to a single-path request back to the
    /// client.
    void send_single_response();
    /// Construct and send a response to a multi-path mutation back to the
    /// client.
    void send_multi_mutation_response();
    /// Construct and send a response to a multi-path lookup back to the client.
    void send_multi_lookup_response();

    void execute_subdoc_spec() {
        {
            ScopeTimer2<HdrMicroSecStopwatch, cb::tracing::SpanStopwatch> timer(
                    std::forward_as_tuple(cookie.getConnection()
                                                  .getBucket()
                                                  .subjson_operation_times),
                    std::forward_as_tuple(cookie,
                                          cb::tracing::Code::SubdocOperate));
            execution_context->execute_subdoc_spec();
        }

        state = State::SendResponse;

        if (traits.is_mutator) {
            if (execution_context->overall_status ==
                cb::mcbp::Status::Success) {
                state = State::AllocateNewItem;
            }
        } else if (execution_context->overall_status ==
                           cb::mcbp::Status::Success ||
                   execution_context->overall_status ==
                           cb::mcbp::Status::SubdocMultiPathFailure) {
            // Make sure we use the correct cas in the response.
            cookie.setCas(execution_context->in_cas);
        }
    }

    static bool getPreserveTtl(Cookie& cookie) {
        bool preserveTtl = false;
        cookie.getRequest().parseFrameExtras(
                [&preserveTtl](cb::mcbp::request::FrameInfoId id,
                               cb::const_byte_buffer) -> bool {
                    if (id == cb::mcbp::request::FrameInfoId::PreserveTtl) {
                        preserveTtl = true;
                        return false;
                    }
                    return true;
                });
        return preserveTtl;
    }

    static uint32_t getExpiration(Cookie& cookie,
                                  const SubdocCmdTraits& traits) {
        if (traits.path == SubdocPath::SINGLE) {
            cb::mcbp::request::SubdocPayloadParser parser(
                    cookie.getRequest().getExtdata());
            return parser.getExpiry();
        }

        cb::mcbp::request::SubdocMultiPayloadParser parser(
                cookie.getRequest().getExtdata());
        return parser.getExpiry();
    }

    static DocFlag getDocFlags(Cookie& cookie, const SubdocCmdTraits& traits) {
        if (traits.path == SubdocPath::SINGLE) {
            cb::mcbp::request::SubdocPayloadParser parser(
                    cookie.getRequest().getExtdata());
            return parser.getDocFlag();
        }

        cb::mcbp::request::SubdocMultiPayloadParser parser(
                cookie.getRequest().getExtdata());
        return parser.getDocFlag();
    }
};

void SubdocCommandContext::do_create_context() {
    execution_context = std::make_unique<SubdocExecutionContext>(
            cookie, traits, cookie.getRequest().getVBucket(), doc_flags);
    state = State::FetchItem;
}

bool SubdocCommandContext::do_fetch_item(cb::engine_errc aio_status) {
    auto& ctx = *execution_context;
    aio_status =
            ctx.connection.remapErrorCode(fetch_input_document(aio_status));
    switch (aio_status) {
    case cb::engine_errc::success:
        if (execution_context->reviveDocument &&
            execution_context->in_document_state == DocumentState::Alive) {
            cookie.sendResponse(
                    cb::mcbp::Status::SubdocCanOnlyReviveDeletedDocuments);
            return false;
        }
        break;

    case cb::engine_errc::would_block:
        cookie.setEwouldblock(true);
        return false;

    case cb::engine_errc::disconnect:
        cookie.getConnection().shutdown();
        return false;

    default:
        cookie.sendResponse(aio_status);
        return false;
    }

    state = State::ExecuteSpecInFrontendThread;
    return true;
}

void SubdocCommandContext::do_execute_spec_in_frontend_thread() {
    execute_subdoc_spec();
}

bool SubdocCommandContext::do_allocate_new_item(cb::engine_errc aio_status) {
    aio_status = allocate_document(aio_status);
    switch (cookie.getConnection().remapErrorCode(
            allocate_document(aio_status))) {
    case cb::engine_errc::success:
        state = State::UpdateItem;
        return true;

    case cb::engine_errc::would_block:
        cookie.setEwouldblock(true);
        return false;

    case cb::engine_errc::disconnect:
        cookie.getConnection().shutdown();
        return false;

    default:
        cookie.sendResponse(aio_status);
        return false;
    }
}

bool SubdocCommandContext::do_update_item(cb::engine_errc aio_status) {
    aio_status = update_document(aio_status);
    if (aio_status == cb::engine_errc::success) {
        state = State::SendResponse;
        return true;
    }

    if (aio_status == cb::engine_errc::key_already_exists) {
        if (auto_retry_mode) {
            get_thread_stats(&cookie.getConnection())->subdoc_update_races++;

            // Retry the operation. Reset the command context and
            // related state, so start from the beginning again.
            if (may_retry()) {
                execution_context.reset();
                state = State::CreateContext;

                if (std::chrono::steady_clock::now() <
                    cookie.getConnection().getCurrentTimesliceEnd()) {
                    return true;
                }

                // We've used the entire timeslice. Reschedule
                cookie.setEwouldblock(true);
                cookie.notifyIoComplete(cb::engine_errc::success);
                return false;
            }

            // Hit maximum attempts - this theoretically could happen but
            // shouldn't in reality.
            LOG_WARNING(
                    "{}: Subdoc: Hit maximum number of auto-retry attempts "
                    "({}) when attempting to perform op {} on doc {} - "
                    "returning TMPFAIL",
                    cookie.getConnectionId(),
                    cb::limits::SubdocMaxAutoRetries,
                    to_string(cookie.getRequest().getClientOpcode()),
                    cb::UserDataView(
                            cookie.getRequestKey().toPrintableString()));
            cookie.sendResponse(cb::mcbp::Status::Etmpfail);
            return false;
        }
        // No auto-retry - return status back to client and return.
        cookie.sendResponse(aio_status);
        return false;
    }

    // error handled in update_document
    return false;
}

void SubdocCommandContext::do_send_response() {
    send_response();
    execution_context->update_statistics();
}

void SubdocCommandContext::do_initiate_shutdown() {
    auto& c = cookie.getConnection();
    c.setTerminationReason("Received exception executing subdoc spec");
    c.shutdown();
}

cb::engine_errc SubdocCommandContext::fetch_input_document(
        cb::engine_errc ret) {
    auto& ctx = *execution_context;
    if (ret == cb::engine_errc::success) {
        if (ctx.do_read_replica) {
            // we can't read deleted items from here...
            auto [status, item] = bucket_get_replica(
                    cookie, cookie.getRequestKey(), ctx.vbucket);
            if (status == cb::engine_errc::success) {
                ctx.fetchedItem = std::move(item);
                ret = cb::engine_errc::success;
            } else {
                ret = ctx.connection.remapErrorCode(status);
            }
        } else {
            auto [status, item] = bucket_get(
                    cookie,
                    cookie.getRequestKey(),
                    ctx.vbucket,
                    ctx.do_allow_deleted_docs ? DocStateFilter::AliveOrDeleted
                                              : DocStateFilter::Alive);
            if (status == cb::engine_errc::success) {
                ctx.fetchedItem = std::move(item);
                ret = cb::engine_errc::success;
            } else {
                ret = ctx.connection.remapErrorCode(status);
            }
        }
    }

    switch (ret) {
    case cb::engine_errc::success:
        if (ctx.traits.is_mutator &&
            ctx.mutationSemantics == MutationSemantics::Add) {
            return cb::engine_errc::key_already_exists;
        }
        ctx.needs_new_doc = false;

        // Retrieve the item_info the engine, and if necessary,
        // uncompress it so subjson can parse it.
        return ctx.get_document_for_searching(cookie.getRequest().getCas());

    case cb::engine_errc::no_such_key:
        if (!ctx.traits.is_mutator ||
            ctx.mutationSemantics == MutationSemantics::Replace) {
            // Lookup operation against a non-existent document is not
            // possible

            // or

            // Mutation operation with Replace semantics - cannot continue
            // without an existing document.
            return cb::engine_errc::no_such_key;
        }

        // The item does not exist, but we are performing a mutation
        // and the mutation semantics are Add or Set (i.e. this mutation
        // can create the document during execution) therefore, can
        // continue without an existing document.

        // Assign a template JSON document of the correct type to operate
        // on.
        if (ctx.jroot_type == JSONSL_T_LIST) {
            std::string in = "[]";
            ctx.in_doc.reset(std::move(in));
            ctx.in_datatype = PROTOCOL_BINARY_DATATYPE_JSON;
        } else if (ctx.jroot_type == JSONSL_T_OBJECT) {
            std::string in = "{}";
            ctx.in_doc.reset(std::move(in));
            ctx.in_datatype = PROTOCOL_BINARY_DATATYPE_JSON;
        } else {
            // Otherwise, a wholedoc operation.
        }

        // Indicate that a new document is required:
        ctx.needs_new_doc = true;
        ctx.in_document_state = ctx.createState;
        return cb::engine_errc::success;

    default:
        return ret;
    }
}

cb::engine_errc SubdocCommandContext::allocate_document(cb::engine_errc ret) {
    Expects(traits.is_mutator);
    auto& context = *execution_context;

    // Allocate a new item of this size.
    if (context.out_doc == nullptr &&
        !(context.no_sys_xattrs && context.do_delete_doc)) {
        if (ret == cb::engine_errc::success) {
            context.out_doc_len = context.in_doc.view.size();
            const size_t priv_bytes = cb::xattr::get_system_xattr_size(
                    context.in_datatype, context.in_doc.view);

            // If the user wanted to preserve the item info we need to copy
            // the one from the item we fetched (which is updated every time we
            // run this loop
            const auto exp = (preserveTtl && context.fetchedItem)
                                     ? context.getInputItemInfo().exptime
                                     : expiration;

            // Calculate the updated document length - use the last operation
            // result.
            try {
                context.out_doc = bucket_allocate(cookie,
                                                  cookie.getRequestKey(),
                                                  context.out_doc_len,
                                                  priv_bytes,
                                                  context.in_flags,
                                                  exp,
                                                  context.in_datatype,
                                                  context.vbucket);
                ret = cb::engine_errc::success;
            } catch (const cb::engine_error& e) {
                ret = cb::engine_errc(e.code().value());
                ret = context.connection.remapErrorCode(ret);
            }
        }

        if (ret != cb::engine_errc::success) {
            return ret;
        }

        // To ensure we only replace the version of the document we
        // just appended to; set the CAS to the one retrieved from.
        context.out_doc->setCas(context.in_cas);

        if (!context.in_doc.view.empty()) {
            // Copy the new document into the item.
            std::copy(context.in_doc.view.begin(),
                      context.in_doc.view.end(),
                      context.out_doc->getValueBuffer().begin());
        }
    }

    return cb::engine_errc::success;
}

cb::engine_errc SubdocCommandContext::update_document(cb::engine_errc ret) {
    Expects(traits.is_mutator);

    auto& connection = cookie.getConnection();
    auto& context = *execution_context;

    // And finally, store the new document.
    uint64_t new_cas;
    mutation_descr_t mdt;
    auto new_op =
            context.needs_new_doc ? StoreSemantics::Add : StoreSemantics::CAS;
    if (ret == cb::engine_errc::success) {
        if (context.do_delete_doc && context.no_sys_xattrs) {
            new_cas = context.in_cas;
            ret = bucket_remove(cookie,
                                cookie.getRequestKey(),
                                new_cas,
                                context.vbucket,
                                cookie.getRequest().getDurabilityRequirements(),
                                mdt);
        } else {
            // if the document is deleted (and we're not reviving it) it
            // cannot have a value
            if (context.in_document_state == DocumentState::Deleted &&
                !context.reviveDocument) {
                auto bodysize = context.in_doc.view.size();
                if (cb::mcbp::datatype::is_xattr(context.in_datatype)) {
                    bodysize -= cb::xattr::get_body_offset(context.in_doc.view);
                }
                if (bodysize > 0) {
                    cookie.setErrorContext(
                            "A deleted document can't have a value");
                    cookie.sendResponse(
                            cb::mcbp::Status::
                                    SubdocDeletedDocumentCantHaveValue);
                    return cb::engine_errc::failed;
                }
            }

            DocumentState new_state = context.do_delete_doc
                                              ? DocumentState::Deleted
                                              : context.in_document_state;

            if (context.reviveDocument) {
                // Unfortunately we can't do CAS replace when transitioning
                // from a deleted document to a live document
                // As a workaround for now just clear the cas field and
                // do add (which will fail if someone created a new _LIVE_
                // document since we read the document).
                context.out_doc->setCas(0);
                new_op = StoreSemantics::Add;

                // this is a revive, so we want to create an alive document
                new_state = DocumentState::Alive;
            }

            ret = bucket_store(cookie,
                               *context.out_doc,
                               new_cas,
                               new_op,
                               cookie.getRequest().getDurabilityRequirements(),
                               new_state,
                               false);
        }
        ret = connection.remapErrorCode(ret);
    }
    switch (ret) {
    case cb::engine_errc::success:
        // Record the UUID / Seqno if MUTATION_SEQNO feature is enabled so
        // we can include it in the response.
        if (connection.isSupportsMutationExtras()) {
            if (context.do_delete_doc && context.no_sys_xattrs) {
                context.vbucket_uuid = mdt.vbucket_uuid;
                context.sequence_no = mdt.seqno;
            } else {
                item_info info;
                if (!bucket_get_item_info(connection, *context.out_doc, info)) {
                    LOG_WARNING("{}: Subdoc: Failed to get item info",
                                connection.getId());
                    cookie.sendResponse(cb::mcbp::Status::Einternal);
                    return cb::engine_errc::failed;
                }

                context.vbucket_uuid = info.vbucket_uuid;
                context.sequence_no = info.seqno;
            }
        }

        cookie.setCas(new_cas);
        break;

    case cb::engine_errc::not_stored:
        // If we tried an add for the item (because it didn't exists)
        // we might race with another thread which started to add
        // the document at the same time. (Note that for Set operations we
        // have to use "add" to add the item to avoid race conditions with
        // another thread trying to create the item at the same time.
        //
        // Adding documents will return NOT_STORED if the document already
        // exist in the database. In the context of a Set operation we map
        // the return code to EEXISTS which may cause the operation to be
        // retried.
        if (new_op == StoreSemantics::Add &&
            context.mutationSemantics == MutationSemantics::Set) {
            ret = cb::engine_errc::key_already_exists;
        } else {
            // Otherwise cb::engine_errc::not_stored is terminal - return to
            // client.
            cookie.sendResponse(ret);
        }
        break;

    case cb::engine_errc::key_already_exists:
        // CAS mismatch. Caller may choose to retry this (without necessarily
        // telling the client), so send so response here...
        break;

    case cb::engine_errc::would_block:
        cookie.setEwouldblock(true);
        break;

    case cb::engine_errc::disconnect:
        connection.shutdown();
        break;

    default:
        cookie.sendResponse(ret);
        break;
    }

    return ret;
}

/* Encodes the context's mutation sequence number and vBucket UUID into the
 * given buffer.
 */
static mutation_descr_t encode_mutation_descr(SubdocExecutionContext& context) {
    mutation_descr_t ret;
    ret.seqno = htonll(context.sequence_no);
    ret.vbucket_uuid = htonll(context.vbucket_uuid);
    return ret;
}

/* Encodes the specified multi-mutation result into the given the given buffer.
 * @param index The operation index.
 * @param op Operation spec to encode.
 * @param buffer Buffer to encode into
 * @return The number of bytes written into the buffer.
 */
static std::string_view encode_multi_mutation_result_spec(
        uint8_t index,
        const SubdocExecutionContext::OperationSpec& op,
        cb::char_buffer buffer) {
    if (buffer.size() <
        (sizeof(uint8_t) + sizeof(uint16_t) + sizeof(uint32_t))) {
        throw std::runtime_error(
                "encode_multi_mutation_result_spec: buffer too small");
    }

    char* cursor = buffer.data();

    // Always encode the index and status.
    *reinterpret_cast<uint8_t*>(cursor) = index;
    cursor += sizeof(uint8_t);
    *reinterpret_cast<uint16_t*>(cursor) = htons(uint16_t(op.status));
    cursor += sizeof(uint16_t);

    // Also encode resultlen if status is success.
    if (op.status == cb::mcbp::Status::Success) {
        const auto& mloc = op.result.matchloc();
        *reinterpret_cast<uint32_t*>(cursor) =
                htonl(gsl::narrow<uint32_t>(mloc.length));
        cursor += sizeof(uint32_t);
    }
    return {const_cast<const char*>(buffer.data()),
            size_t(cursor - buffer.data())};
}

void SubdocCommandContext::send_single_response() {
    using cb::mcbp::Status;
    if (execution_context->overall_status != Status::Success) {
        cookie.sendResponse(execution_context->overall_status);
        return;
    }

    auto& context = *execution_context;

    context.response_val_len = 0;
    std::string_view value = {};
    bool binary = false;

    if (traits.responseHasValue()) {
        // The value may have been created in the xattr or the body phase
        // so it should only be one, so if it isn't an xattr it should be
        // in the body
        SubdocExecutionContext::Phase phase =
                SubdocExecutionContext::Phase::XATTR;
        if (context.getOperations(phase).empty()) {
            phase = SubdocExecutionContext::Phase::Body;
        }
        const auto& op = context.getOperations(phase)[0];
        binary = hasBinaryValue(op.flags);

        auto mloc = op.result.matchloc();
        value = {mloc.at, mloc.length};
        context.response_val_len = value.size();
    }

    std::string_view extras = {};
    mutation_descr_t descr = {};
    if (traits.is_mutator) {
        // Add mutation descr to response buffer if requested.
        if (cookie.getConnection().isSupportsMutationExtras()) {
            descr = encode_mutation_descr(context);
            extras = {reinterpret_cast<const char*>(&descr), sizeof(descr)};
        }
    } else {
        // Record a Document Read audit event for non-mutator operations
        // (mutators will have already recorded a Document Modify event when
        // they called bucket_store.
        cb::audit::document::add(cookie,
                                 cb::audit::document::Operation::Read,
                                 cookie.getRequestKey());
    }

    auto status_code = cb::mcbp::Status::Success;
    if (context.in_document_state == DocumentState::Deleted &&
        !context.reviveDocument) {
        status_code = cb::mcbp::Status::SubdocSuccessDeleted;
    }

    cookie.sendResponse(status_code,
                        extras,
                        {},
                        value,
                        binary ? cb::mcbp::Datatype::Raw
                               : traits.responseDatatype(context.in_datatype),
                        cookie.getCas());
}

void SubdocCommandContext::send_multi_mutation_response() {
    auto& context = *execution_context;
    auto& connection = cookie.getConnection();

    // MULTI_MUTATION: On success, zero to N multi_mutation_result_spec objects
    // (one for each spec which wants to return a value), with optional 16byte
    // mutation descriptor in extras if MUTATION_SEQNO is enabled.
    //
    // On failure body indicates the index and status code of the first failing
    // spec.
    mutation_descr_t descr = {};
    std::string_view extras = {};

    // Encode mutation extras into buffer if success & they were requested.
    if (context.overall_status == cb::mcbp::Status::Success &&
        connection.isSupportsMutationExtras()) {
        descr = encode_mutation_descr(context);
        extras = {reinterpret_cast<const char*>(&descr), sizeof(descr)};
    }

    // Calculate total body size to encode into the header.
    size_t iov_len = 0;
    if (context.overall_status == cb::mcbp::Status::Success) {
        // on success, one per each non-zero length result.
        for (auto phase : phases) {
            for (const auto& op : context.getOperations(phase)) {
                const auto mloc = op.result.matchloc();
                if (op.traits.responseHasValue() && mloc.length > 0) {
                    // add the size of the header
                    iov_len += sizeof(uint8_t) + sizeof(uint16_t) +
                               sizeof(uint32_t);
                    // and the size of the actual data
                    iov_len += mloc.length;
                }
            }
        }
    } else {
        // Just one - index and status of first failure.
        iov_len = sizeof(uint8_t) + sizeof(uint16_t);
    }

    auto status_code = context.overall_status;
    if ((status_code == cb::mcbp::Status::Success) &&
        (context.in_document_state == DocumentState::Deleted) &&
        !context.reviveDocument) {
        status_code = cb::mcbp::Status::SubdocSuccessDeleted;
    }

    // Allocated required resource - build the header.
    connection.sendResponseHeaders(cookie,
                                   status_code,
                                   extras,
                                   {},
                                   iov_len,
                                   PROTOCOL_BINARY_RAW_BYTES);

    // Append the iovecs for each operation result.
    uint8_t index = 0;
    auto scratch = context.connection.getThread().getScratchBuffer();

    for (auto phase : phases) {
        for (size_t ii = 0; ii < context.getOperations(phase).size();
             ii++, index++) {
            const auto& op = context.getOperations(phase)[ii];
            // Successful - encode all non-zero length results.
            if (context.overall_status == cb::mcbp::Status::Success) {
                const auto mloc = op.result.matchloc();
                if (op.traits.responseHasValue() && mloc.length > 0) {
                    connection.copyToOutputStream(
                            encode_multi_mutation_result_spec(
                                    index, op, scratch),
                            {mloc.at, mloc.length});
                }
            } else {
                // Failure - encode first unsuccessful path index and status.
                if (op.status != cb::mcbp::Status::Success) {
                    connection.copyToOutputStream(
                            encode_multi_mutation_result_spec(
                                    index, op, scratch));
                    // Only the first unsuccessful op is reported.
                    return;
                }
            }
        }
    }
}

void SubdocCommandContext::send_multi_lookup_response() {
    auto& context = *execution_context;
    auto& connection = cookie.getConnection();

    // Calculate the value length - sum of all the operation results.
    context.response_val_len = 0;
    for (auto phase : phases) {
        for (auto& op : context.getOperations(phase)) {
            // 16bit status, 32bit resultlen, variable-length result.
            size_t result_size = sizeof(uint16_t) + sizeof(uint32_t);
            if (op.traits.responseHasValue()) {
                result_size += op.result.matchloc().length;
            }
            context.response_val_len += result_size;
        }
    }

    // We need two iovecs per operation result:
    // 1. status (uin16_t) & vallen (uint32_t). Use the dynamicBuffer for this
    // 2. actual value - this already resides either in the original document
    //                   (for lookups) or stored in the Subdoc::Result.

    // Allocated required resource - build the header.
    auto status_code = context.overall_status;
    if (status_code == cb::mcbp::Status::Success) {
        cb::audit::document::add(cookie,
                                 cb::audit::document::Operation::Read,
                                 cookie.getRequestKey());
        if (context.in_document_state == DocumentState::Deleted) {
            status_code = cb::mcbp::Status::SubdocSuccessDeleted;
        }
    }

    // Lookups to a deleted document which (partially) succeeded need
    // to be mapped MULTI_PATH_FAILURE_DELETED, so the client knows the found
    // document was in Deleted state.
    if (status_code == cb::mcbp::Status::SubdocMultiPathFailure &&
        (context.in_document_state == DocumentState::Deleted) &&
        !traits.is_mutator) {
        status_code = cb::mcbp::Status::SubdocMultiPathFailureDeleted;
    }

    connection.sendResponseHeaders(cookie,
                                   status_code,
                                   {},
                                   {},
                                   context.response_val_len,
                                   PROTOCOL_BINARY_RAW_BYTES);

    // Append the iovecs for each operation result.
    for (auto phase : phases) {
        for (auto& op : context.getOperations(phase)) {
            auto mloc = op.result.matchloc();

            // Header is always included. Result value included if the response
            // for this command has a value (e.g. not for EXISTS).
#pragma pack(1)
            struct Header {
                explicit Header(cb::mcbp::Status s)
                    : status(htons(uint16_t(s))) {
                }
                void setLength(uint32_t l) {
                    length = htonl(l);
                }
                const uint16_t status;
                uint32_t length = 0;
                std::string_view getBuffer() const {
                    return {reinterpret_cast<const char*>(this), sizeof(*this)};
                }
            };
#pragma pack()
            static_assert(sizeof(Header) == 6, "Incorrect size");
            Header h(op.status);

            if (op.traits.responseHasValue()) {
                h.setLength((mloc.length));
                connection.copyToOutputStream(h.getBuffer(),
                                              {mloc.at, mloc.length});
            } else {
                connection.copyToOutputStream(h.getBuffer());
            }
        }
    }
}

void SubdocCommandContext::send_response() {
    switch (traits.path) {
    case SubdocPath::SINGLE:
        send_single_response();
        return;

    case SubdocPath::MULTI:
        if (traits.is_mutator) {
            send_multi_mutation_response();
        } else {
            send_multi_lookup_response();
        }
        return;
    }

    folly::assume_unreachable();
}

void subdoc_get_executor(Cookie& cookie) {
    cookie.obtainContext<SubdocCommandContext>(
                  cookie, get_traits<cb::mcbp::ClientOpcode::SubdocGet>())
            .drive();
}

void subdoc_exists_executor(Cookie& cookie) {
    cookie.obtainContext<SubdocCommandContext>(
                  cookie, get_traits<cb::mcbp::ClientOpcode::SubdocExists>())
            .drive();
}

void subdoc_dict_add_executor(Cookie& cookie) {
    cookie.obtainContext<SubdocCommandContext>(
                  cookie, get_traits<cb::mcbp::ClientOpcode::SubdocDictAdd>())
            .drive();
}

void subdoc_dict_upsert_executor(Cookie& cookie) {
    cookie.obtainContext<SubdocCommandContext>(
                  cookie,
                  get_traits<cb::mcbp::ClientOpcode::SubdocDictUpsert>())
            .drive();
}

void subdoc_delete_executor(Cookie& cookie) {
    cookie.obtainContext<SubdocCommandContext>(
                  cookie, get_traits<cb::mcbp::ClientOpcode::SubdocDelete>())
            .drive();
}

void subdoc_replace_executor(Cookie& cookie) {
    cookie.obtainContext<SubdocCommandContext>(
                  cookie, get_traits<cb::mcbp::ClientOpcode::SubdocReplace>())
            .drive();
}

void subdoc_array_push_last_executor(Cookie& cookie) {
    cookie.obtainContext<SubdocCommandContext>(
                  cookie,
                  get_traits<cb::mcbp::ClientOpcode::SubdocArrayPushLast>())
            .drive();
}

void subdoc_array_push_first_executor(Cookie& cookie) {
    cookie.obtainContext<SubdocCommandContext>(
                  cookie,
                  get_traits<cb::mcbp::ClientOpcode::SubdocArrayPushFirst>())
            .drive();
}

void subdoc_array_insert_executor(Cookie& cookie) {
    cookie.obtainContext<SubdocCommandContext>(
                  cookie,
                  get_traits<cb::mcbp::ClientOpcode::SubdocArrayInsert>())
            .drive();
}

void subdoc_array_add_unique_executor(Cookie& cookie) {
    cookie.obtainContext<SubdocCommandContext>(
                  cookie,
                  get_traits<cb::mcbp::ClientOpcode::SubdocArrayAddUnique>())
            .drive();
}

void subdoc_counter_executor(Cookie& cookie) {
    cookie.obtainContext<SubdocCommandContext>(
                  cookie, get_traits<cb::mcbp::ClientOpcode::SubdocCounter>())
            .drive();
}

void subdoc_get_count_executor(Cookie& cookie) {
    cookie.obtainContext<SubdocCommandContext>(
                  cookie, get_traits<cb::mcbp::ClientOpcode::SubdocGetCount>())
            .drive();
}

void subdoc_replace_body_with_xattr_executor(Cookie& cookie) {
    cookie.obtainContext<SubdocCommandContext>(
                  cookie,
                  get_traits<
                          cb::mcbp::ClientOpcode::SubdocReplaceBodyWithXattr>())
            .drive();
}

void subdoc_multi_lookup_executor(Cookie& cookie) {
    cookie.obtainContext<SubdocCommandContext>(
                  cookie,
                  get_traits<cb::mcbp::ClientOpcode::SubdocMultiLookup>())
            .drive();
}

void subdoc_multi_mutation_executor(Cookie& cookie) {
    cookie.obtainContext<SubdocCommandContext>(
                  cookie,
                  get_traits<cb::mcbp::ClientOpcode::SubdocMultiMutation>())
            .drive();
}
