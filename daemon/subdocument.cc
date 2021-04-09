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

#include "subdocument.h"

#include "buckets.h"
#include "connections.h"
#include "front_end_thread.h"
#include "mcaudit.h"
#include "protocol/mcbp/engine_wrapper.h"
#include "subdoc/util.h"
#include "subdocument_context.h"
#include "subdocument_parser.h"
#include "subdocument_traits.h"
#include "subdocument_validators.h"
#include "timings.h"
#include "topkeys.h"
#include "utilities/logtags.h"
#include "xattr/key_validator.h"
#include "xattr/utils.h"

#include <logger/logger.h>
#include <memcached/durability_spec.h>
#include <memcached/types.h>
#include <platform/histogram.h>
#include <utilities/engine_errc_2_mcbp.h>
#include <xattr/blob.h>
#include <gsl/gsl>

static const std::array<SubdocCmdContext::Phase, 2> phases{{SubdocCmdContext::Phase::XATTR,
                                                            SubdocCmdContext::Phase::Body}};

using namespace mcbp::subdoc;

/******************************************************************************
 * Subdocument executors
 *****************************************************************************/

/*
 * Declarations
 */
static bool subdoc_fetch(Cookie& cookie,
                         SubdocCmdContext& ctx,
                         cb::engine_errc& ret,
                         cb::const_byte_buffer key,
                         uint64_t cas);

static bool subdoc_operate(SubdocCmdContext& context);

static cb::engine_errc subdoc_update(SubdocCmdContext& context,
                                     cb::engine_errc ret,
                                     cb::const_byte_buffer key,
                                     uint32_t expiration);
static void subdoc_response(Cookie& cookie, SubdocCmdContext& context);

static SubdocCmdContext* subdoc_create_context(Cookie& cookie,
                                               const SubdocCmdTraits traits,
                                               doc_flag doc_flags,
                                               Vbid vbucket) {
    try {
        return std::make_unique<SubdocCmdContext>(
                       cookie, traits, vbucket, doc_flags)
                .release();
    } catch (const std::bad_alloc&) {
        return nullptr;
    }
}

/**
 * Main function which handles execution of all sub-document
 * commands: fetches, operates on, updates and finally responds to the client.
 *
 * Invoked via extern "C" trampoline functions (see later) which populate the
 * subdocument elements of executors[].
 *
 * @param cookie the command context
 * @param traits Traits associated with the specific command.
 */
static void subdoc_executor(Cookie& cookie, const SubdocCmdTraits traits) {
    // 0. Parse the request and log it if debug enabled.
    const auto& request = cookie.getRequest();
    const auto vbucket = request.getVBucket();
    const auto cas = request.getCas();
    const auto key = request.getKey();
    auto extras = request.getExtdata();

    uint32_t expiration;
    doc_flag doc_flags;

    if (traits.path == SubdocPath::SINGLE) {
        cb::mcbp::request::SubdocPayloadParser parser(extras);
        expiration = parser.getExpiry();
        doc_flags = parser.getDocFlag();
    } else {
        cb::mcbp::request::SubdocMultiPayloadParser parser(extras);
        expiration = parser.getExpiry();
        doc_flags = parser.getDocFlag();
    }

    bool preserveTtl = false;
    request.parseFrameExtras([&preserveTtl](cb::mcbp::request::FrameInfoId id,
                                            cb::const_byte_buffer) -> bool {
        if (id == cb::mcbp::request::FrameInfoId::PreserveTtl) {
            preserveTtl = true;
            return false;
        }
        return true;
    });

    // We potentially need to make multiple attempts at this as the engine may
    // return EWOULDBLOCK if not initially resident, hence initialise ret to
    // c->aiostat.
    auto ret = cookie.swapAiostat(cb::engine_errc::success);

    // If client didn't specify a CAS, we still use CAS internally to check
    // that we are updating the same version of the document as was fetched.
    // However in this case we auto-retry in the event of a concurrent update
    // by some other client.
    const bool auto_retry = (cas == 0);

    // We specify a finite number of times to retry; to prevent the (extremely
    // unlikely) event that we are fighting with another client for the
    // correct CAS value for an arbitrary amount of time (and to defend against
    // possible bugs in our code ;)
    const int MAXIMUM_ATTEMPTS = 100;

    cookie.logCommand();

    int attempts = 0;
    do {
        attempts++;

        // 0. If we don't already have a command context, allocate one
        // (we may already have one if this is an auto_retry or a re-execution
        // due to EWOULDBLOCK).
        auto* context =
                dynamic_cast<SubdocCmdContext*>(cookie.getCommandContext());
        if (context == nullptr) {
            context = subdoc_create_context(cookie, traits, doc_flags, vbucket);
            if (context == nullptr) {
                cookie.sendResponse(cb::mcbp::Status::Enomem);
                return;
            }
            cookie.setCommandContext(context);
        }

        // 1. Attempt to fetch from the engine the document to operate on. Only
        // continue if it returned true, otherwise return from this function
        // (which may result in it being called again later in the EWOULDBLOCK
        // case).
        if (!subdoc_fetch(cookie, *context, ret, key, cas)) {
            return;
        }

        if (context->reviveDocument &&
            context->in_document_state == DocumentState::Alive) {
            cookie.sendResponse(
                    cb::mcbp::Status::SubdocCanOnlyReviveDeletedDocuments);
            return;
        }

        // 2. Perform the operation specified by CMD. Again, return if it fails.
        if (!subdoc_operate(*context)) {
            return;
        }

        // If the user wanted to preserve the item info we need to copy
        // the one from the item we fetched (which is updated every time we
        // run this loop
        if (preserveTtl && context->fetchedItem) {
            expiration = context->getInputItemInfo().exptime;
        }

        // 3. Update the document in the engine (mutations only).
        ret = subdoc_update(*context, ret, key, expiration);
        if (ret == cb::engine_errc::key_already_exists) {
            if (auto_retry) {
                // Retry the operation. Reset the command context and related
                // state, so start from the beginning again.
                ret = cb::engine_errc::success;

                cookie.setCommandContext();
                continue;
            } else {
                // No auto-retry - return status back to client and return.
                cookie.sendResponse(cb::engine_errc(ret));
                return;
            }
        } else if (ret != cb::engine_errc::success) {
            return;
        }

        // 4. Form a response and send it back to the client.
        subdoc_response(cookie, *context);

        // Update stats. Treat all mutations as 'cmd_set', all accesses as 'cmd_get',
        // in addition to specific subdoc counters. (This is mainly so we
        // see subdoc commands in the GUI, which used cmd_set / cmd_get).
        auto* thread_stats = get_thread_stats(&cookie.getConnection());
        if (context->traits.is_mutator) {
            thread_stats->cmd_subdoc_mutation++;
            thread_stats->bytes_subdoc_mutation_total += context->out_doc_len;
            thread_stats->bytes_subdoc_mutation_inserted +=
                    context->getOperationValueBytesTotal();

            SLAB_INCR(&cookie.getConnection(), cmd_set);
        } else {
            thread_stats->cmd_subdoc_lookup++;
            thread_stats->bytes_subdoc_lookup_total +=
                    context->in_doc.view.size();
            thread_stats->bytes_subdoc_lookup_extracted += context->response_val_len;

            STATS_HIT(&cookie.getConnection(), get);
        }
        update_topkeys(cookie);
        return;
    } while (auto_retry && attempts < MAXIMUM_ATTEMPTS);

    // Hit maximum attempts - this theoretically could happen but shouldn't
    // in reality.
    const auto mcbp_cmd = request.getClientOpcode();

    auto& c = cookie.getConnection();
    LOG_WARNING(
            "{}: Subdoc: Hit maximum number of auto-retry attempts ({}) when "
            "attempting to perform op {} for client {} - returning TMPFAIL",
            c.getId(),
            MAXIMUM_ATTEMPTS,
            to_string(mcbp_cmd),
            c.getDescription());
    cookie.sendResponse(cb::mcbp::Status::Etmpfail);
}

// Fetch the item to operate on from the engine.
// Returns true if the command was successful (and execution should continue),
// else false.
static bool subdoc_fetch(Cookie& cookie,
                         SubdocCmdContext& ctx,
                         cb::engine_errc& ret,
                         cb::const_byte_buffer key,
                         uint64_t cas) {
    if (!ctx.fetchedItem && !ctx.needs_new_doc) {
        if (ret == cb::engine_errc::success) {
            auto get_key = cookie.getConnection().makeDocKey(key);
            DocStateFilter state = DocStateFilter::Alive;
            if (ctx.do_allow_deleted_docs) {
                state = DocStateFilter::AliveOrDeleted;
            }
            auto r = bucket_get(cookie, get_key, ctx.vbucket, state);
            if (r.first == cb::engine_errc::success) {
                ctx.fetchedItem = std::move(r.second);
                ret = cb::engine_errc::success;
            } else {
                ret = cb::engine_errc(r.first);
                ret = ctx.connection.remapErrorCode(ret);
            }
        }

        switch (ret) {
        case cb::engine_errc::success:
            if (ctx.traits.is_mutator &&
                ctx.mutationSemantics == MutationSemantics::Add) {
                cookie.sendResponse(cb::mcbp::Status::KeyEexists);
                return false;
            }
            ctx.needs_new_doc = false;
            break;

        case cb::engine_errc::no_such_key:
            if (!ctx.traits.is_mutator) {
                // Lookup operation against a non-existent document is not
                // possible.
                cookie.sendResponse(cb::engine_errc(ret));
                return false;
            }

            // Mutation operation with Replace semantics - cannot continue
            // withot an existing document.
            if (ctx.mutationSemantics == MutationSemantics::Replace) {
                cookie.sendResponse(cb::engine_errc(ret));
                return false;
            }

            // The item does not exist, but we are performing a mutation
            // and the mutation semantics are Add or Set (i.e. this mutation
            // can create the document during execution) therefore can
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
                // Otherwise a wholedoc operation.
            }

            // Indicate that a new document is required:
            ctx.needs_new_doc = true;
            ctx.in_document_state = ctx.createState;
            // Change 'ret' back to success - conceptually the fetch did
            // "succeed" and execution should continue.
            ret = cb::engine_errc::success;
            return true;

        case cb::engine_errc::would_block:
            cookie.setEwouldblock(true);
            return false;

        case cb::engine_errc::disconnect:
            cookie.getConnection().shutdown();
            return false;

        default:
            cookie.sendResponse(cb::engine_errc(ret));
            return false;
        }
    }

    if (ctx.in_doc.view.data() == nullptr) {
        // Retrieve the item_info the engine, and if necessary
        // uncompress it so subjson can parse it.
        auto status = ctx.get_document_for_searching(cas);

        if (status != cb::mcbp::Status::Success) {
            // Failed. Note c.item and c.commandContext will both be freed for
            // us as part of preparing for the next command.
            cookie.sendResponse(status);
            return false;
        }
    }

    return true;
}

/**
 * Perform the subjson operation specified by {spec} to one path in the
 * document.
 */
static cb::mcbp::Status subdoc_operate_one_path(
        SubdocCmdContext& context,
        SubdocCmdContext::OperationSpec& spec,
        std::string_view in_doc) {
    // Prepare the specified sub-document command.
    auto& op = context.connection.getThread().subdoc_op;
    op.clear();
    op.set_result_buf(&spec.result);
    op.set_code(spec.traits.subdocCommand);
    op.set_doc(in_doc.data(), in_doc.size());

    if (spec.flags & SUBDOC_FLAG_EXPAND_MACROS) {
        auto padded_macro = context.get_padded_macro(spec.value);
        if (padded_macro.empty()) {
            padded_macro = context.expand_virtual_macro(spec.value);
            if (padded_macro.empty()) {
                return cb::mcbp::Status::SubdocXattrUnknownVattrMacro;
            }
        }
        op.set_value(padded_macro.data(), padded_macro.size());
    } else {
        op.set_value(spec.value.data(), spec.value.size());
    }

    if (context.getCurrentPhase() == SubdocCmdContext::Phase::XATTR &&
        cb::xattr::is_vattr(spec.path)) {
        // path potentially contains elements with in the VATTR, e.g.
        // $document.cas. Need to extract the actual VATTR name prefix.
        auto vattr_key = spec.path;
        auto key_end = spec.path.find_first_of(".[");
        if (key_end != std::string::npos) {
            vattr_key.resize(key_end);
        }

        // Check which VATTR is being accessed:
        std::string_view doc;
        if (vattr_key == cb::xattr::vattrs::DOCUMENT) {
            // This is a call to the "$document" VATTR, so replace the document
            // with the document virtual one..
            doc = context.get_document_vattr();
        } else if (vattr_key == cb::xattr::vattrs::XTOC) {
            doc = context.get_xtoc_vattr();
        } else if (vattr_key == cb::xattr::vattrs::VBUCKET) {
            // replace the document with the vbucket virtual one..
            doc = context.get_vbucket_vattr();
        } else {
            return cb::mcbp::Status::SubdocXattrUnknownVattr;
        }
        op.set_doc(doc.data(), doc.size());
    }

    // ... and execute it.
    const auto subdoc_res = op.op_exec(spec.path.data(), spec.path.size());

    switch (subdoc_res) {
    case Subdoc::Error::SUCCESS:
        return cb::mcbp::Status::Success;

    case Subdoc::Error::PATH_ENOENT:
        return cb::mcbp::Status::SubdocPathEnoent;

    case Subdoc::Error::PATH_MISMATCH:
        return cb::mcbp::Status::SubdocPathMismatch;

    case Subdoc::Error::DOC_ETOODEEP:
        return cb::mcbp::Status::SubdocDocE2deep;

    case Subdoc::Error::PATH_EINVAL:
        return cb::mcbp::Status::SubdocPathEinval;

    case Subdoc::Error::DOC_NOTJSON:
        return cb::mcbp::Status::SubdocDocNotJson;

    case Subdoc::Error::DOC_EEXISTS:
        return cb::mcbp::Status::SubdocPathEexists;

    case Subdoc::Error::PATH_E2BIG:
        return cb::mcbp::Status::SubdocPathE2big;

    case Subdoc::Error::NUM_E2BIG:
        return cb::mcbp::Status::SubdocNumErange;

    case Subdoc::Error::DELTA_EINVAL:
        return cb::mcbp::Status::SubdocDeltaEinval;

    case Subdoc::Error::VALUE_CANTINSERT:
        return cb::mcbp::Status::SubdocValueCantinsert;

    case Subdoc::Error::DELTA_OVERFLOW:
        return cb::mcbp::Status::SubdocValueCantinsert;

    case Subdoc::Error::VALUE_ETOODEEP:
        return cb::mcbp::Status::SubdocValueEtoodeep;

    default:
        // TODO: handle remaining errors.
        LOG_DEBUG("Unexpected response from subdoc: {} ({:x})",
                  subdoc_res.description(),
                  subdoc_res.code());
        return cb::mcbp::Status::Einternal;
    }
}

/**
 * Perform the wholedoc (mcbp) operation defined by spec
 */
static cb::mcbp::Status subdoc_operate_wholedoc(
        SubdocCmdContext& context,
        SubdocCmdContext::OperationSpec& spec,
        std::string_view& doc) {
    switch (spec.traits.mcbpCommand) {
    case cb::mcbp::ClientOpcode::Get:
        if (doc.empty()) {
            // Size of zero indicates the document body ("path") doesn't exist.
            return cb::mcbp::Status::SubdocPathEnoent;
        }
        spec.result.set_matchloc({doc.data(), doc.size()});
        return cb::mcbp::Status::Success;

    case cb::mcbp::ClientOpcode::Set:
        spec.result.push_newdoc({spec.value.data(), spec.value.size()});
        return cb::mcbp::Status::Success;

    case cb::mcbp::ClientOpcode::Delete:
        context.in_datatype &= ~BODY_ONLY_DATATYPE_MASK;
        spec.result.push_newdoc({nullptr, 0});
        return cb::mcbp::Status::Success;

    default:
        return cb::mcbp::Status::Einval;
    }
}

static cb::mcbp::Status subdoc_operate_attributes_and_body(
        SubdocCmdContext& context,
        SubdocCmdContext::OperationSpec& spec,
        MemoryBackedBuffer* xattr,
        MemoryBackedBuffer& body) {
    if (spec.traits.mcbpCommand !=
        cb::mcbp::ClientOpcode::SubdocReplaceBodyWithXattr) {
        return cb::mcbp::Status::NotSupported;
    }

    if (xattr == nullptr) {
        throw std::invalid_argument(
                "subdoc_operate_attributes_and_body: can't be called with "
                "xattr being nullptr");
    }

    // Currently we only support SubdocReplaceBodyWithXattr through this
    // API, and we can implement that by using a SUBDOC-GET to get the location
    // of the data; and then reset the body with the content of the operation.
    auto st = subdoc_operate_one_path(context, spec, xattr->view);
    if (st == cb::mcbp::Status::Success) {
        body.reset(spec.result.matchloc().to_string());
        spec.result.clear();
        if (body.view.empty()) {
            // document body is now empty; clear the json bit
            context.in_datatype &= ~PROTOCOL_BINARY_DATATYPE_JSON;
        } else {
            context.in_datatype |= PROTOCOL_BINARY_DATATYPE_JSON;
        }
    }
    return st;
}

/**
 * Run through all of the subdoc operations for the current phase on
 * the documents content (either the xattr section or the document body,
 * depending on the execution phase)
 *
 * @param context The context object for this operation
 * @param xattr the documents attribute section (MUST be set in the xattr
 *              phase, and should be set to nullptr in the body phase)
 * @param body the documents body section
 * @param doc_datatype The datatype of the document. Updated if a
 *                     wholedoc op changes the datatype.
 * @param modified set to true upon return if any modifications happened
 *                 to the input document.
 * @return true if we should continue processing this request,
 *         false if we've sent the error packet and should terminate
 *               execution for this request
 *
 * @throws std::bad_alloc if allocation fails
 */
static bool operate_single_doc(SubdocCmdContext& context,
                               MemoryBackedBuffer* xattr,
                               MemoryBackedBuffer& body,
                               protocol_binary_datatype_t& doc_datatype,
                               bool& modified) {
    modified = false;
    auto& operations = context.getOperations();

    if (context.getCurrentPhase() == SubdocCmdContext::Phase::XATTR) {
        Expects(xattr);
    } else {
        // The xattr is only provided in the xattr phase as supplying them
        // would require us to rebuild a JSON document for the key (and there
        // is no operations in the BODY phase which require the header)
        Expects(!xattr);
    }

    auto& current = context.getCurrentPhase() == SubdocCmdContext::Phase::XATTR
                            ? *xattr
                            : body;

    // 2. Perform each of the operations on document.
    for (auto& op : operations) {
        switch (op.traits.scope) {
        case CommandScope::SubJSON:
            if (mcbp::datatype::is_json(doc_datatype)) {
                // Got JSON, perform the operation.
                op.status = subdoc_operate_one_path(context, op, current.view);
            } else {
                // No good; need to have JSON.
                op.status = cb::mcbp::Status::SubdocDocNotJson;
            }
            break;

        case CommandScope::WholeDoc:
            op.status = subdoc_operate_wholedoc(context, op, current.view);
            break;

        case CommandScope::AttributesAndBody:
            op.status = subdoc_operate_attributes_and_body(
                    context, op, xattr, body);
            break;
        }

        if (op.status == cb::mcbp::Status::Success) {
            if (context.traits.is_mutator) {
                modified = true;

                if (op.traits.scope != CommandScope::AttributesAndBody) {
                    // Determine how much space we now need.
                    size_t new_doc_len = 0;
                    for (auto& loc : op.result.newdoc()) {
                        new_doc_len += loc.length;
                    }

                    std::string next;
                    next.reserve(new_doc_len);
                    // TODO-PERF: We need to create a contiguous input
                    // region for the next subjson call, from the set of
                    // iovecs in the result. We can't simply write into
                    // the dynamic_buffer, as that may be the underlying
                    // storage for iovecs from the result. Ideally we'd
                    // either permit subjson to take an iovec as input, or
                    // permit subjson to take all the multipaths at once.
                    // For now we make a contiguous region in a temporary
                    // char[], and point in_doc at that.
                    for (auto& loc : op.result.newdoc()) {
                        next.insert(next.end(), loc.at, loc.at + loc.length);
                    }

                    // Copying complete - safe to delete the old temp_doc
                    // (even if it was the source of some of the newdoc
                    // iovecs).
                    current.reset(std::move(next));

                    if (op.traits.scope == CommandScope::WholeDoc) {
                        // the entire document has been replaced as part of a
                        // wholedoc op update the datatype to match
                        JSON_checker::Validator validator;
                        bool isValidJson = validator.validate(current.view);

                        // don't alter context.in_datatype directly here in case
                        // we are in xattrs phase
                        if (isValidJson) {
                            doc_datatype |= PROTOCOL_BINARY_DATATYPE_JSON;
                        } else {
                            doc_datatype &= ~PROTOCOL_BINARY_DATATYPE_JSON;
                        }
                    }
                }
            } else { // lookup
                // nothing to do.
            }
        } else {
            switch (context.traits.path) {
            case SubdocPath::SINGLE:
                // Failure of a (the only) op stops execution and returns an
                // error to the client.
                context.cookie.sendResponse(op.status);
                return false;

            case SubdocPath::MULTI:
                context.overall_status =
                        cb::mcbp::Status::SubdocMultiPathFailure;
                if (context.traits.is_mutator) {
                    // For mutations, this stops the operation - however as
                    // we need to respond with a body indicating the index
                    // which failed we return true indicating 'success'.
                    return true;
                } else {
                    // For lookup; an operation failing doesn't stop us
                    // continuing with the rest of the operations
                    // - continue with the next operation.
                    continue;
                }

                break;
            }
        }
    }

    return true;
}

static cb::engine_errc validate_vattr_privilege(SubdocCmdContext& context,
                                                std::string_view key) {
    // The $document vattr doesn't require any xattr permissions.
    if (key[1] == 'X') {
        // In the xtoc case we want to see which privileges the connection has
        // to determine which XATTRs we tell the user about

        bool xattrRead =
                context.cookie.checkPrivilege(cb::rbac::Privilege::XattrRead)
                        .success();
        bool xattrSysRead =
                context.cookie
                        .checkPrivilege(cb::rbac::Privilege::SystemXattrRead)
                        .success();

        if (xattrRead && xattrSysRead) {
            context.xtocSemantics = XtocSemantics::All;
        } else if (xattrRead) {
            context.xtocSemantics = XtocSemantics::User;
        } else if (xattrSysRead) {
            context.xtocSemantics = XtocSemantics::System;
        } else {
            return cb::engine_errc::no_access;
        }
    }
    return cb::engine_errc::success;
}

static cb::engine_errc validate_xattr_privilege(SubdocCmdContext& context) {
    // Look at all of the operations we've got in there:
    for (const auto& op :
         context.getOperations(SubdocCmdContext::Phase::XATTR)) {
        if (cb::xattr::is_vattr(op.path)) {
            auto ret = validate_vattr_privilege(context, op.path);
            if (ret != cb::engine_errc::success) {
                return ret;
            }
        }
    }

    auto key = context.get_xattr_key();
    if (key.empty()) {
        return cb::engine_errc::success;
    }

    cb::rbac::Privilege privilege;
    // We've got an XATTR..
    if (context.traits.is_mutator) {
        if (cb::xattr::is_system_xattr(key)) {
            privilege = cb::rbac::Privilege::SystemXattrWrite;
        } else {
            privilege = cb::rbac::Privilege::XattrWrite;
        }
    } else {
        if (cb::xattr::is_system_xattr(key)) {
            privilege = cb::rbac::Privilege::SystemXattrRead;
        } else {
            privilege = cb::rbac::Privilege::XattrRead;
        }
    }

    return context.cookie.checkPrivilege(privilege).success()
                   ? cb::engine_errc::success
                   : cb::engine_errc::no_access;
}

/**
 * Replaces the xattrs on the document with the new ones provided
 * @param new_xattr The new xattrs to use
 * @param context The command context for this operation
 * @param bodyoffset The offset in to the body of the xattr section
 * @param bodysize The size of the body (excludes xattrs)
 */
static inline void replace_xattrs(std::string_view new_xattr,
                                  SubdocCmdContext& context,
                                  const size_t bodyoffset,
                                  const size_t bodysize) {
    auto total = new_xattr.size() + bodysize;

    std::string next;
    next.reserve(total);
    next.insert(next.end(), new_xattr.begin(), new_xattr.end());
    next.insert(next.end(),
                context.in_doc.view.data() + bodyoffset,
                context.in_doc.view.data() + bodyoffset + bodysize);
    context.in_doc.reset(std::move(next));

    if (new_xattr.empty()) {
        context.in_datatype &= ~PROTOCOL_BINARY_DATATYPE_XATTR;
        context.no_sys_xattrs = true;

    } else {
        context.in_datatype |= PROTOCOL_BINARY_DATATYPE_XATTR;
    }
}

/**
 * Delete user xattrs from the xattr blob if required.
 * @param context The command context for this operation
 * @return true if success and that we may progress to the
 *              next phase
 */
static bool do_xattr_delete_phase(SubdocCmdContext& context) {
    if (!context.do_delete_doc ||
        !mcbp::datatype::is_xattr(context.in_datatype)) {
        return true;
    }

    // We need to remove the user keys from the Xattrs and rebuild the document

    const auto bodyoffset = cb::xattr::get_body_offset(context.in_doc.view);
    const auto bodysize = context.in_doc.view.size() - bodyoffset;

    cb::char_buffer blob_buffer{(char*)context.in_doc.view.data(),
                                (size_t)bodyoffset};

    const cb::xattr::Blob xattr_blob(
            blob_buffer, mcbp::datatype::is_snappy(context.in_datatype));

    // The backing store for the blob is currently witin the actual
    // document.. create a copy we can use for replace.
    cb::xattr::Blob copy(xattr_blob);

    // Remove the user xattrs so we're just left with system xattrs
    copy.prune_user_keys();

    const auto new_xattr = copy.finalize();
    replace_xattrs(new_xattr, context, bodyoffset, bodysize);

    return true;
}

/**
 * Parse the XATTR blob and only operate on the single xattr
 * requested
 *
 * @param context The command context for this operation
 * @return true if success and that we may progress to the
 *              next phase
 */
static bool do_xattr_phase(SubdocCmdContext& context) {
    context.setCurrentPhase(SubdocCmdContext::Phase::XATTR);
    if (context.getOperations().empty()) {
        return true;
    }

    // Does the user have the permission to perform XATTRs
    auto access = validate_xattr_privilege(context);
    if (access != cb::engine_errc::success) {
        access = context.connection.remapErrorCode(access);
        if (access == cb::engine_errc::disconnect) {
            context.connection.shutdown();
            return false;
        }

        switch (context.traits.path) {
        case SubdocPath::SINGLE:
            // Failure of a (the only) op stops execution and returns an
            // error to the client.
            context.cookie.sendResponse(cb::engine_errc(access));
            return false;

        case SubdocPath::MULTI:
            context.overall_status = cb::mcbp::Status::SubdocMultiPathFailure;
            {
                // Mark all of them as failed..
                auto& operations = context.getOperations();
                for (auto& operation : operations) {
                    operation.status =
                            cb::mcbp::to_status(cb::engine_errc(access));
                }
            }
            return true;
        }
        throw std::logic_error("do_xattr_phase: unknown SubdocPath");
    }

    auto bodysize = context.in_doc.view.size();
    auto bodyoffset = 0;

    if (mcbp::datatype::is_xattr(context.in_datatype)) {
        bodyoffset = cb::xattr::get_body_offset(context.in_doc.view);
        bodysize -= bodyoffset;
    }

    cb::char_buffer blob_buffer{(char*)context.in_doc.view.data(),
                                (size_t)bodyoffset};

    const cb::xattr::Blob xattr_blob(
            blob_buffer, mcbp::datatype::is_snappy(context.in_datatype));
    auto key = context.get_xattr_key();
    auto value_buf = xattr_blob.get(key);

    if (value_buf.size() == 0) {
        context.xattr_buffer.reset(new char[2]);
        context.xattr_buffer[0] = '{';
        context.xattr_buffer[1] = '}';
        value_buf = {context.xattr_buffer.get(), 2};
    } else {
        // To allow the subjson do it's thing with the full xattrs
        // create a full json doc looking like: {\"xattr_key\":\"value\"};
        size_t total = 5 + key.size() + value_buf.size();
        context.xattr_buffer.reset(new char[total]);
        char* ptr = context.xattr_buffer.get();
        memcpy(ptr, "{\"", 2);
        ptr += 2;
        memcpy(ptr, key.data(), key.size());
        ptr += key.size();
        memcpy(ptr, "\":", 2);
        ptr += 2;
        memcpy(ptr, value_buf.data(), value_buf.size());
        ptr += value_buf.size();
        *ptr = '}';
        value_buf = { context.xattr_buffer.get(), total};
    }

    MemoryBackedBuffer body{{context.in_doc.view.data() + bodyoffset,
                             context.in_doc.view.size() - bodyoffset}};
    MemoryBackedBuffer xattr{{value_buf.data(), value_buf.size()}};

    for (const auto& m : {cb::xattr::macros::CAS,
                          cb::xattr::macros::SEQNO,
                          cb::xattr::macros::VALUE_CRC32C}) {
        context.generate_macro_padding(xattr.view, m);
    }

    bool modified;
    auto datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    if (!operate_single_doc(context, &xattr, body, datatype, modified)) {
        // Something failed..
        return false;
    }
    // Xattr doc should always be json
    Expects(datatype == PROTOCOL_BINARY_DATATYPE_JSON);

    if (context.overall_status != cb::mcbp::Status::Success) {
        return true;
    }

    // We didn't change anything in the document so just drop everything
    if (!modified) {
        return true;
    }

    // Time to rebuild the full document.
    // As a temporary solution we did create a full JSON doc for the
    // xattr key, so we should strip off the key and just store the value.

    // The backing store for the blob is currently witin the actual
    // document.. create a copy we can use for replace.
    cb::xattr::Blob copy(xattr_blob);

    if (xattr.view.size() > key.size()) {
        const char* start = strchr(xattr.view.data(), ':') + 1;
        const char* end = xattr.view.data() + xattr.view.size() - 1;

        copy.set(key, {start, size_t(end - start)});
    } else {
        copy.remove(key);
    }
    const auto new_xattr = copy.finalize();

    if (body.isModified()) {
        auto total = new_xattr.size() + body.view.size();
        std::string next;
        next.reserve(total);
        next.insert(next.end(), new_xattr.begin(), new_xattr.end());
        next.insert(next.end(), body.view.begin(), body.view.end());
        context.in_doc.reset(std::move(next));
        if (new_xattr.empty()) {
            context.in_datatype &= ~PROTOCOL_BINARY_DATATYPE_XATTR;
            context.no_sys_xattrs = true;
        } else {
            context.in_datatype |= PROTOCOL_BINARY_DATATYPE_XATTR;
        }
    } else {
        replace_xattrs(new_xattr, context, bodyoffset, bodysize);
    }
    return true;
}

/**
 * Operate on the user body part of the document as specified by the command
 * context.
 * @return true if the command was successful (and execution should continue),
 *         else false.
 */
static bool do_body_phase(SubdocCmdContext& context) {
    context.setCurrentPhase(SubdocCmdContext::Phase::Body);

    if (context.getOperations().empty()) {
        return true;
    }

    size_t xattrsize = 0;
    MemoryBackedBuffer body{context.in_doc};

    if (mcbp::datatype::is_xattr(context.in_datatype)) {
        // We shouldn't have any documents like that!
        xattrsize = cb::xattr::get_body_offset(body.view);
        body.view.remove_prefix(xattrsize);
    }

    bool modified;

    if (!operate_single_doc(
                context, nullptr, body, context.in_datatype, modified)) {
        return false;
    }

    // We didn't change anything in the document so just drop everything
    if (!modified) {
        return true;
    }

    // There isn't any xattrs associated with the document. We shouldn't
    // reallocate and move things around but just reuse the temporary
    // buffer we've already created.
    if (xattrsize == 0) {
        context.in_doc.swap(body);
        return true;
    }

    // Just stitch our new modified value behind the existing xattrs
    auto total = xattrsize + body.view.size();
    std::string next;
    next.reserve(total);
    next.insert(next.end(),
                context.in_doc.view.data(),
                context.in_doc.view.data() + xattrsize);
    next.insert(next.end(), body.view.begin(), body.view.end());
    context.in_doc.reset(std::move(next));

    return true;
}

// Operate on the document as specified by the command context.
// Returns true if the command was successful (and execution should continue),
// else false.
static bool subdoc_operate(SubdocCmdContext& context) {
    if (context.executed) {
        return true;
    }

    HdrMicroSecBlockTimer bt(
            &context.connection.getBucket().subjson_operation_times);

    context.overall_status = cb::mcbp::Status::Success;

    try {
        if (do_xattr_phase(context) && do_xattr_delete_phase(context) &&
            do_body_phase(context)) {
            context.executed = true;
            return true;
        }
    } catch (const std::bad_alloc&) {
        // Insufficient memory - unable to continue.
        context.cookie.sendResponse(cb::mcbp::Status::Enomem);
        return false;
    }

    return false;
}

// Update the engine with whatever modifications the subdocument command made
// to the document.
// Returns true if the update was successful (and execution should continue),
// else false.
static cb::engine_errc subdoc_update(SubdocCmdContext& context,
                                     cb::engine_errc ret,
                                     cb::const_byte_buffer key,
                                     uint32_t expiration) {
    auto& connection = context.connection;
    auto& cookie = context.cookie;

    if (context.getCurrentPhase() == SubdocCmdContext::Phase::XATTR) {
        LOG_WARNING(
                "Internal error: We should not reach subdoc_update in the "
                "xattr phase");
        return cb::engine_errc::failed;
    }

    if (!context.traits.is_mutator) {
        // No update required - just make sure we have the correct cas to use
        // for response.
        cookie.setCas(context.in_cas);
        return cb::engine_errc::success;
    }

    // For multi-mutations, we only want to actually update the engine if /all/
    // paths succeeded - otherwise the document is unchanged (and we continue
    // to subdoc_response() to send information back to the client on what
    // succeeded/failed.
    if (context.overall_status != cb::mcbp::Status::Success) {
        return cb::engine_errc::success;
    }

    // Allocate a new item of this size.
    if (context.out_doc == nullptr &&
        !(context.no_sys_xattrs && context.do_delete_doc)) {
        if (ret == cb::engine_errc::success) {
            context.out_doc_len = context.in_doc.view.size();
            auto allocate_key = cookie.getConnection().makeDocKey(key);
            const size_t priv_bytes = cb::xattr::get_system_xattr_size(
                    context.in_datatype, context.in_doc.view);

            // Calculate the updated document length - use the last operation result.
            try {
                auto r = bucket_allocate_ex(cookie,
                                            allocate_key,
                                            context.out_doc_len,
                                            priv_bytes,
                                            context.in_flags,
                                            expiration,
                                            context.in_datatype,
                                            context.vbucket);
                if (r.first) {
                    // Save the allocated document in the cmd context.
                    context.out_doc = std::move(r.first);
                    ret = cb::engine_errc::success;
                } else {
                    ret = cb::engine_errc::no_memory;
                }
            } catch (const cb::engine_error& e) {
                ret = cb::engine_errc(e.code().value());
                ret = context.connection.remapErrorCode(ret);
            }
        }

        switch (ret) {
        case cb::engine_errc::success:
            // Save the allocated document in the cmd context.
            break;

        case cb::engine_errc::would_block:
            cookie.setEwouldblock(true);
            return ret;

        case cb::engine_errc::disconnect:
            connection.shutdown();
            return ret;

        default:
            cookie.sendResponse(cb::engine_errc(ret));
            return ret;
        }

        // To ensure we only replace the version of the document we
        // just appended to; set the CAS to the one retrieved from.
        bucket_item_set_cas(connection, context.out_doc.get(), context.in_cas);

        // Obtain the item info (and it's iovectors)
        if (!context.in_doc.view.empty()) {
            item_info new_doc_info;
            if (!bucket_get_item_info(
                        connection, context.out_doc.get(), &new_doc_info)) {
                cookie.sendResponse(cb::mcbp::Status::Einternal);
                return cb::engine_errc::failed;
            }

            // Copy the new document into the item.
            char* write_ptr =
                    static_cast<char*>(new_doc_info.value[0].iov_base);
            std::copy(context.in_doc.view.begin(),
                      context.in_doc.view.end(),
                      write_ptr);
        }
    }

    // And finally, store the new document.
    uint64_t new_cas;
    mutation_descr_t mdt;
    auto new_op =
            context.needs_new_doc ? StoreSemantics::Add : StoreSemantics::CAS;
    if (ret == cb::engine_errc::success) {
        if (context.do_delete_doc && context.no_sys_xattrs) {
            new_cas = context.in_cas;
            auto docKey = connection.makeDocKey(key);
            ret = bucket_remove(cookie,
                                docKey,
                                new_cas,
                                context.vbucket,
                                cookie.getRequest().getDurabilityRequirements(),
                                mdt);
        } else {
            if (context.reviveDocument) {
                context.in_document_state = DocumentState::Alive;
                // Unfortunately we can't do CAS replace when transitioning
                // from a deleted document to a live document
                // As a workaround for now just clear the cas field and
                // do add (which will fail if someone created a new _LIVE_
                // document since we read the document).
                bucket_item_set_cas(connection, context.out_doc.get(), 0);
                new_op = StoreSemantics::Add;
            }

            if (context.in_document_state == DocumentState::Deleted) {
                auto bodysize = context.in_doc.view.size();
                if (mcbp::datatype::is_xattr(context.in_datatype)) {
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

            ret = bucket_store(cookie,
                               context.out_doc.get(),
                               new_cas,
                               new_op,
                               cookie.getRequest().getDurabilityRequirements(),
                               context.do_delete_doc
                                       ? DocumentState::Deleted
                                       : context.in_document_state,
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
                if (!bucket_get_item_info(
                            connection, context.out_doc.get(), &info)) {
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
            cookie.sendResponse(cb::engine_errc(ret));
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
        cookie.sendResponse(cb::engine_errc(ret));
        break;
    }

    return ret;
}

/* Encodes the context's mutation sequence number and vBucket UUID into the
 * given buffer.
 */
static void encode_mutation_descr(SubdocCmdContext& context,
                                  mutation_descr_t& descr) {
    descr.seqno = htonll(context.sequence_no);
    descr.vbucket_uuid = htonll(context.vbucket_uuid);
}

/* Encodes the specified multi-mutation result into the given the given buffer.
 * @param index The operation index.
 * @param op Operation spec to encode.
 * @param buffer Buffer to encode into
 * @return The number of bytes written into the buffer.
 */
static std::string_view encode_multi_mutation_result_spec(
        uint8_t index,
        const SubdocCmdContext::OperationSpec& op,
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

/* Construct and send a response to a single-path request back to the client.
 */
static void subdoc_single_response(Cookie& cookie, SubdocCmdContext& context) {
    auto& connection = context.connection;

    context.response_val_len = 0;
    std::string_view value = {};
    if (context.traits.responseHasValue()) {
        // The value may have been created in the xattr or the body phase
        // so it should only be one, so if it isn't an xattr it should be
        // in the body
        SubdocCmdContext::Phase phase = SubdocCmdContext::Phase::XATTR;
        if (context.getOperations(phase).empty()) {
            phase = SubdocCmdContext::Phase::Body;
        }
        auto mloc = context.getOperations(phase)[0].result.matchloc();
        value = {mloc.at, mloc.length};
        context.response_val_len = value.size();
    }

    if (context.traits.is_mutator) {
        cb::audit::document::add(cookie,
                                 cb::audit::document::Operation::Modify);
    } else {
        cb::audit::document::add(cookie, cb::audit::document::Operation::Read);
    }

    // Add mutation descr to response buffer if requested.
    std::string_view extras = {};
    mutation_descr_t descr = {};
    if (connection.isSupportsMutationExtras() && context.traits.is_mutator) {
        encode_mutation_descr(context, descr);
        extras = {reinterpret_cast<const char*>(&descr), sizeof(descr)};
    }

    auto status_code = cb::mcbp::Status::Success;
    if (context.in_document_state == DocumentState::Deleted) {
        status_code = cb::mcbp::Status::SubdocSuccessDeleted;
    }

    cookie.sendResponse(status_code,
                        extras,
                        {},
                        value,
                        context.traits.responseDatatype(context.in_datatype),
                        cookie.getCas());
}

/* Construct and send a response to a multi-path mutation back to the client.
 */
static void subdoc_multi_mutation_response(Cookie& cookie,
                                           SubdocCmdContext& context) {
    auto& connection = context.connection;

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
        encode_mutation_descr(context, descr);
        extras = {reinterpret_cast<const char*>(&descr), sizeof(descr)};
    }

    // Calculate total body size to encode into the header.
    size_t iov_len = 0;
    if (context.overall_status == cb::mcbp::Status::Success) {
        cb::audit::document::add(cookie,
                                 cb::audit::document::Operation::Modify);

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
        (context.in_document_state == DocumentState::Deleted)) {
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
        for (size_t ii = 0; ii < context.getOperations(phase).size(); ii++, index++) {
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

/* Construct and send a response to a multi-path lookup back to the client.
 */
static void subdoc_multi_lookup_response(Cookie& cookie,
                                         SubdocCmdContext& context) {
    auto& connection = context.connection;

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
        cb::audit::document::add(cookie, cb::audit::document::Operation::Read);
        if (context.in_document_state == DocumentState::Deleted) {
            status_code = cb::mcbp::Status::SubdocSuccessDeleted;
        }
    }

    // Lookups to a deleted document which (partially) succeeded need
    // to be mapped MULTI_PATH_FAILURE_DELETED, so the client knows the found
    // document was in Deleted state.
    if (status_code == cb::mcbp::Status::SubdocMultiPathFailure &&
        (context.in_document_state == DocumentState::Deleted) &&
        !context.traits.is_mutator) {
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

            // Header is always included. Result value included if the response for
            // this command has a value (e.g. not for EXISTS).
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

// Respond back to the user as appropriate to the specific command.
static void subdoc_response(Cookie& cookie, SubdocCmdContext& context) {
    switch (context.traits.path) {
    case SubdocPath::SINGLE:
        subdoc_single_response(cookie, context);
        return;

    case SubdocPath::MULTI:
        if (context.traits.is_mutator) {
            subdoc_multi_mutation_response(cookie, context);
        } else {
            subdoc_multi_lookup_response(cookie, context);
        }
        return;
    }

    // Shouldn't get here - invalid traits.path
    cookie.sendResponse(cb::mcbp::Status::Einternal);
    auto& connection = cookie.getConnection();
    LOG_WARNING(
            "{}: subdoc_response - invalid traits.path - closing connection {}",
            connection.getId(),
            connection.getDescription());
    connection.shutdown();
}

void subdoc_get_executor(Cookie& cookie) {
    subdoc_executor(cookie, get_traits<cb::mcbp::ClientOpcode::SubdocGet>());
}

void subdoc_exists_executor(Cookie& cookie) {
    subdoc_executor(cookie, get_traits<cb::mcbp::ClientOpcode::SubdocExists>());
}

void subdoc_dict_add_executor(Cookie& cookie) {
    subdoc_executor(cookie,
                    get_traits<cb::mcbp::ClientOpcode::SubdocDictAdd>());
}

void subdoc_dict_upsert_executor(Cookie& cookie) {
    subdoc_executor(cookie,
                    get_traits<cb::mcbp::ClientOpcode::SubdocDictUpsert>());
}

void subdoc_delete_executor(Cookie& cookie) {
    subdoc_executor(cookie, get_traits<cb::mcbp::ClientOpcode::SubdocDelete>());
}

void subdoc_replace_executor(Cookie& cookie) {
    subdoc_executor(cookie,
                    get_traits<cb::mcbp::ClientOpcode::SubdocReplace>());
}

void subdoc_array_push_last_executor(Cookie& cookie) {
    subdoc_executor(cookie,
                    get_traits<cb::mcbp::ClientOpcode::SubdocArrayPushLast>());
}

void subdoc_array_push_first_executor(Cookie& cookie) {
    subdoc_executor(cookie,
                    get_traits<cb::mcbp::ClientOpcode::SubdocArrayPushFirst>());
}

void subdoc_array_insert_executor(Cookie& cookie) {
    subdoc_executor(cookie,
                    get_traits<cb::mcbp::ClientOpcode::SubdocArrayInsert>());
}

void subdoc_array_add_unique_executor(Cookie& cookie) {
    subdoc_executor(cookie,
                    get_traits<cb::mcbp::ClientOpcode::SubdocArrayAddUnique>());
}

void subdoc_counter_executor(Cookie& cookie) {
    subdoc_executor(cookie,
                    get_traits<cb::mcbp::ClientOpcode::SubdocCounter>());
}

void subdoc_get_count_executor(Cookie& cookie) {
    subdoc_executor(cookie,
                    get_traits<cb::mcbp::ClientOpcode::SubdocGetCount>());
}

void subdoc_replace_body_with_xattr_executor(Cookie& cookie) {
    subdoc_executor(
            cookie,
            get_traits<cb::mcbp::ClientOpcode::SubdocReplaceBodyWithXattr>());
}

void subdoc_multi_lookup_executor(Cookie& cookie) {
    subdoc_executor(cookie,
                    get_traits<cb::mcbp::ClientOpcode::SubdocMultiLookup>());
}

void subdoc_multi_mutation_executor(Cookie& cookie) {
    subdoc_executor(cookie,
                    get_traits<cb::mcbp::ClientOpcode::SubdocMultiMutation>());
}
