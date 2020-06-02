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

#include "subdocument.h"

#include "buckets.h"
#include "connections.h"
#include "debug_helpers.h"
#include "front_end_thread.h"
#include "mcaudit.h"
#include "protocol/mcbp/engine_wrapper.h"
#include "settings.h"
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
                         ENGINE_ERROR_CODE ret,
                         cb::const_byte_buffer key,
                         uint64_t cas);

static bool subdoc_operate(SubdocCmdContext& context);

static ENGINE_ERROR_CODE subdoc_update(SubdocCmdContext& context,
                                       ENGINE_ERROR_CODE ret,
                                       cb::const_byte_buffer key,
                                       uint32_t expiration);
static void subdoc_response(Cookie& cookie, SubdocCmdContext& context);

// Debug - print details of the specified subdocument command.
static void subdoc_print_command(Connection& c,
                                 cb::mcbp::ClientOpcode cmd,
                                 const char* key,
                                 const uint16_t keylen,
                                 const char* path,
                                 const size_t pathlen,
                                 const char* value,
                                 const size_t vallen) {
    char clean_key[KEY_MAX_LENGTH + 32];
    char clean_path[SUBDOC_PATH_MAX_LENGTH];
    char clean_value[80]; // only print the first few characters of the value.
    if ((key_to_printable_buffer(clean_key,
                                 sizeof(clean_key),
                                 c.getId(),
                                 true,
                                 to_string(cb::mcbp::ClientOpcode(cmd)).c_str(),
                                 key,
                                 keylen)) &&
        (buf_to_printable_buffer(
                clean_path, sizeof(clean_path), path, pathlen))) {
        // print key, path & value if there is a value.
        if ((vallen > 0) &&
            (buf_to_printable_buffer(
                    clean_value, sizeof(clean_value), value, vallen))) {
            LOG_DEBUG("{} path:'{}' value:'{}'",
                      cb::UserDataView(clean_key),
                      cb::UserDataView(clean_path),
                      cb::UserDataView(clean_value));

        } else {
            // key & path only
            LOG_DEBUG("{} path:'{}'",
                      cb::UserDataView(clean_key),
                      cb::UserDataView(clean_path));
        }
    }
}

/*
 * Definitions
 */
static void create_single_path_context(SubdocCmdContext& context,
                                       Cookie& cookie,
                                       const SubdocCmdTraits traits,
                                       doc_flag doc_flags) {
    const auto& request = cookie.getRequest();
    const auto extras = request.getExtdata();
    cb::mcbp::request::SubdocPayloadParser parser(extras);
    const auto pathlen = parser.getPathlen();
    auto flags = parser.getSubdocFlags();
    const auto valbuf = request.getValue();
    std::string_view value{reinterpret_cast<const char*>(valbuf.data()),
                           valbuf.size()};
    // Path is the first thing in the value; remainder is the operation
    // value.
    auto path = value.substr(0, pathlen);

    const bool xattr = (flags & SUBDOC_FLAG_XATTR_PATH);
    const SubdocCmdContext::Phase phase = xattr ?
                                          SubdocCmdContext::Phase::XATTR :
                                          SubdocCmdContext::Phase::Body;
    auto& ops = context.getOperations(phase);

    if (xattr) {
        size_t xattr_keylen;
        is_valid_xattr_key({path.data(), path.size()}, xattr_keylen);
        context.set_xattr_key({path.data(), xattr_keylen});
    }

    if (flags & SUBDOC_FLAG_EXPAND_MACROS) {
        context.do_macro_expansion = true;
    }

    if (hasAccessDeleted(doc_flags)) {
        context.do_allow_deleted_docs = true;
    }

    // If Mkdoc or Add is specified, this implies MKDIR_P, ensure that it's set
    // here
    if (impliesMkdir_p(doc_flags)) {
        flags = flags | SUBDOC_FLAG_MKDIR_P;
    }

    context.decodeDocFlags(doc_flags);

    // Decode as single path; add a single operation to the context.
    if (traits.request_has_value) {
        // Adjust value to move past the path.
        value.remove_prefix(pathlen);

        ops.emplace_back(
                SubdocCmdContext::OperationSpec{traits,
                                                flags,
                                                {path.data(), path.size()},
                                                {value.data(), value.size()}});
    } else {
        ops.emplace_back(SubdocCmdContext::OperationSpec{
                traits, flags, {path.data(), path.size()}});
    }

    if (impliesMkdir_p(doc_flags)) {
        context.jroot_type = Subdoc::Util::get_root_type(
                traits.subdocCommand, path.data(), path.size());
    }

    if (Settings::instance().getVerbose() > 1) {
        const auto keybuf = request.getKey();
        const char* key = reinterpret_cast<const char*>(keybuf.data());
        const auto keylen = gsl::narrow<uint16_t>(keybuf.size());

        subdoc_print_command(cookie.getConnection(),
                             request.getClientOpcode(),
                             key,
                             keylen,
                             path.data(),
                             path.size(),
                             value.data(),
                             value.size());
    }
}

static void create_multi_path_context(SubdocCmdContext& context,
                                      Cookie& cookie,
                                      const SubdocCmdTraits traits,
                                      doc_flag doc_flags) {
    // Decode each of lookup specs from the value into our command context.
    const auto& request = cookie.getRequest();
    const auto valbuf = request.getValue();
    std::string_view value{reinterpret_cast<const char*>(valbuf.data()),
                           valbuf.size()};

    context.decodeDocFlags(doc_flags);

    size_t offset = 0;
    while (offset < value.size()) {
        cb::mcbp::ClientOpcode binprot_cmd = cb::mcbp::ClientOpcode::Invalid;
        protocol_binary_subdoc_flag flags;
        size_t headerlen;
        std::string_view path;
        std::string_view spec_value;
        if (traits.is_mutator) {
            auto* spec = reinterpret_cast<
                    const protocol_binary_subdoc_multi_mutation_spec*>(
                    value.data() + offset);
            headerlen = sizeof(*spec);
            binprot_cmd = cb::mcbp::ClientOpcode(spec->opcode);
            flags = protocol_binary_subdoc_flag(spec->flags);
            path = {value.data() + offset + headerlen, htons(spec->pathlen)};
            spec_value = {value.data() + offset + headerlen + path.size(),
                          htonl(spec->valuelen)};

        } else {
            auto* spec = reinterpret_cast<
                    const protocol_binary_subdoc_multi_lookup_spec*>(
                    value.data() + offset);
            headerlen = sizeof(*spec);
            binprot_cmd = cb::mcbp::ClientOpcode(spec->opcode);
            flags = protocol_binary_subdoc_flag(spec->flags);
            path = {value.data() + offset + headerlen, htons(spec->pathlen)};
            spec_value = {nullptr, 0};
        }

        auto traits = get_subdoc_cmd_traits(binprot_cmd);
        if (impliesMkdir_p(doc_flags) &&
            context.jroot_type == 0) {
            // Determine the root type
            context.jroot_type = Subdoc::Util::get_root_type(
                    traits.subdocCommand, path.data(), path.size());
        }

        if (flags & SUBDOC_FLAG_EXPAND_MACROS) {
            context.do_macro_expansion = true;
        }

        if (hasAccessDeleted(doc_flags)) {
            context.do_allow_deleted_docs = true;
        }

        const bool xattr = (flags & SUBDOC_FLAG_XATTR_PATH);
        if (xattr) {
            size_t xattr_keylen;
            is_valid_xattr_key({path.data(), path.size()}, xattr_keylen);
            std::string_view key{path.data(), xattr_keylen};
            if (!cb::xattr::is_vattr(key)) {
                context.set_xattr_key(key);
            }
        }

        const SubdocCmdContext::Phase phase = xattr ?
                                              SubdocCmdContext::Phase::XATTR :
                                              SubdocCmdContext::Phase::Body;

        auto& ops = context.getOperations(phase);

        // Mkdoc and Add imply MKDIR_P, ensure that MKDIR_P is set
        if (impliesMkdir_p(doc_flags)) {
            flags = flags | SUBDOC_FLAG_MKDIR_P;
        }
        if (traits.mcbpCommand == cb::mcbp::ClientOpcode::Delete) {
            context.do_delete_doc = true;
        }
        ops.emplace_back(SubdocCmdContext::OperationSpec{
                traits,
                flags,
                {path.data(), path.size()},
                {spec_value.data(), spec_value.size()}});
        offset += headerlen + path.size() + spec_value.size();
    }

    if (Settings::instance().getVerbose() > 1) {
        const auto keybuf = request.getKey();
        const char* key = reinterpret_cast<const char*>(keybuf.data());
        const auto keylen = gsl::narrow<uint16_t>(keybuf.size());

        const char path[] = "<multipath>";
        subdoc_print_command(cookie.getConnection(),
                             request.getClientOpcode(),
                             key,
                             keylen,
                             path,
                             strlen(path),
                             value.data(),
                             value.size());
    }
}

static SubdocCmdContext* subdoc_create_context(Cookie& cookie,
                                               const SubdocCmdTraits traits,
                                               doc_flag doc_flags,
                                               Vbid vbucket) {
    try {
        std::unique_ptr<SubdocCmdContext> context;
        context = std::make_unique<SubdocCmdContext>(cookie, traits, vbucket);
        switch (traits.path) {
        case SubdocPath::SINGLE:
            create_single_path_context(
                    *context.get(), cookie, traits, doc_flags);
            break;

        case SubdocPath::MULTI:
            create_multi_path_context(
                    *context.get(), cookie, traits, doc_flags);
            break;
        }

        return context.release();
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
    auto ret = cookie.swapAiostat(ENGINE_SUCCESS);

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
        if (ret == ENGINE_KEY_EEXISTS) {
            if (auto_retry) {
                // Retry the operation. Reset the command context and related
                // state, so start from the beginning again.
                ret = ENGINE_SUCCESS;

                cookie.setCommandContext();
                continue;
            } else {
                // No auto-retry - return status back to client and return.
                cookie.sendResponse(cb::engine_errc(ret));
                return;
            }
        } else if (ret != ENGINE_SUCCESS) {
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
            thread_stats->bytes_subdoc_lookup_total += context->in_doc.size();
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
                         ENGINE_ERROR_CODE ret,
                         cb::const_byte_buffer key,
                         uint64_t cas) {
    if (!ctx.fetchedItem && !ctx.needs_new_doc) {
        if (ret == ENGINE_SUCCESS) {
            auto get_key = cookie.getConnection().makeDocKey(key);
            DocStateFilter state = DocStateFilter::Alive;
            if (ctx.do_allow_deleted_docs) {
                state = DocStateFilter::AliveOrDeleted;
            }
            auto r = bucket_get(cookie, get_key, ctx.vbucket, state);
            if (r.first == cb::engine_errc::success) {
                ctx.fetchedItem = std::move(r.second);
                ret = ENGINE_SUCCESS;
            } else {
                ret = ENGINE_ERROR_CODE(r.first);
                ret = ctx.connection.remapErrorCode(ret);
            }
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            if (ctx.traits.is_mutator &&
                ctx.mutationSemantics == MutationSemantics::Add) {
                cookie.sendResponse(cb::mcbp::Status::KeyEexists);
                return false;
            }
            ctx.needs_new_doc = false;
            break;

        case ENGINE_KEY_ENOENT:
            if (ctx.traits.is_mutator &&
                ctx.mutationSemantics == MutationSemantics::Replace) {
                cookie.sendResponse(cb::engine_errc(ret));
                return false;
            }

            // The item does not exist. Check the current command context to
            // determine if we should at all write a new document (i.e. pretend
            // it exists) and defer insert until later.. OR if we should simply
            // bail.

            if (ctx.jroot_type == JSONSL_T_LIST) {
                ctx.in_doc = {"[]", 2};
            } else if (ctx.jroot_type == JSONSL_T_OBJECT) {
                ctx.in_doc = {"{}", 2};
            } else {
                cookie.sendResponse(cb::engine_errc(ret));
                return false;
            }

            // Indicate that a new document is required:
            ctx.needs_new_doc = true;
            ctx.in_datatype = PROTOCOL_BINARY_DATATYPE_JSON;
            ctx.in_document_state = ctx.createState;
            return true;

        case ENGINE_EWOULDBLOCK:
            cookie.setEwouldblock(true);
            return false;

        case ENGINE_DISCONNECT:
            cookie.getConnection().shutdown();
            return false;

        default:
            cookie.sendResponse(cb::engine_errc(ret));
            return false;
        }
    }

    if (ctx.in_doc.data() == nullptr) {
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

/**
 * Run through all of the subdoc operations for the current phase on
 * a single 'document' (either the user document, or a XATTR).
 *
 * @param context The context object for this operation
 * @param doc the document to operate on
 * @param doc_datatype The datatype of the document. Updated if a
 *                     wholedoc op changes the datatype.
 * @param temp_buffer where to store the data for our temporary buffer
 *                    allocations if we need to change the doc.
 * @param modified set to true upon return if any modifications happened
 *                 to the input document.
 * @return true if we should continue processing this request,
 *         false if we've sent the error packet and should temrinate
 *               execution for this request
 *
 * @throws std::bad_alloc if allocation fails
 */
static bool operate_single_doc(SubdocCmdContext& context,
                               std::string_view& doc,
                               protocol_binary_datatype_t& doc_datatype,
                               std::unique_ptr<char[]>& temp_buffer,
                               bool& modified) {
    modified = false;
    auto& operations = context.getOperations();

    // 2. Perform each of the operations on document.
    for (auto& op : operations) {
        switch (op.traits.scope) {
        case CommandScope::SubJSON:
            if (mcbp::datatype::is_json(doc_datatype)) {
                // Got JSON, perform the operation.
                op.status = subdoc_operate_one_path(context, op, doc);
            } else {
                // No good; need to have JSON.
                op.status = cb::mcbp::Status::SubdocDocNotJson;
            }
            break;

        case CommandScope::WholeDoc:
            op.status = subdoc_operate_wholedoc(context, op, doc);
        }

        if (op.status == cb::mcbp::Status::Success) {
            if (context.traits.is_mutator) {
                modified = true;

                // Determine how much space we now need.
                size_t new_doc_len = 0;
                for (auto& loc : op.result.newdoc()) {
                    new_doc_len += loc.length;
                }

                // TODO-PERF: We need to create a contiguous input
                // region for the next subjson call, from the set of
                // iovecs in the result. We can't simply write into
                // the dynamic_buffer, as that may be the underlying
                // storage for iovecs from the result. Ideally we'd
                // either permit subjson to take an iovec as input, or
                // permit subjson to take all the multipaths at once.
                // For now we make a contiguous region in a temporary
                // char[], and point in_doc at that.
                std::unique_ptr<char[]> temp(new char[new_doc_len]);

                size_t offset = 0;
                for (auto& loc : op.result.newdoc()) {
                    std::copy(loc.at, loc.at + loc.length, temp.get() + offset);
                    offset += loc.length;
                }

                // Copying complete - safe to delete the old temp_doc
                // (even if it was the source of some of the newdoc
                // iovecs).
                temp_buffer.swap(temp);
                doc = {temp_buffer.get(), new_doc_len};

                if (op.traits.scope == CommandScope::WholeDoc) {
                    // the entire document has been replaced as part of a
                    // wholedoc op update the datatype to match
                    JSON_checker::Validator validator;
                    bool isValidJson = validator.validate(doc);

                    // don't alter context.in_datatype directly here in case we
                    // are in xattrs phase
                    if (isValidJson) {
                        doc_datatype |= PROTOCOL_BINARY_DATATYPE_JSON;
                    } else {
                        doc_datatype &= ~PROTOCOL_BINARY_DATATYPE_JSON;
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

static ENGINE_ERROR_CODE validate_vattr_privilege(SubdocCmdContext& context,
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
            return ENGINE_EACCESS;
        }
    }
    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE validate_xattr_privilege(SubdocCmdContext& context) {
    // Look at all of the operations we've got in there:
    for (const auto& op :
         context.getOperations(SubdocCmdContext::Phase::XATTR)) {
        if (cb::xattr::is_vattr(op.path)) {
            auto ret = validate_vattr_privilege(context, op.path);
            if (ret != ENGINE_SUCCESS) {
                return ret;
            }
        }
    }

    auto key = context.get_xattr_key();
    if (key.empty()) {
        return ENGINE_SUCCESS;
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

    return context.cookie.checkPrivilege(privilege).success() ? ENGINE_SUCCESS
                                                              : ENGINE_EACCESS;
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

    std::unique_ptr<char[]> full_document(new char[total]);
    std::copy(new_xattr.begin(), new_xattr.end(), full_document.get());
    std::copy(context.in_doc.data() + bodyoffset,
              context.in_doc.data() + bodyoffset + bodysize,
              full_document.get() + new_xattr.size());

    context.temp_doc.swap(full_document);
    context.in_doc = {context.temp_doc.get(), total};

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

    const auto bodyoffset = cb::xattr::get_body_offset(context.in_doc);
    const auto bodysize = context.in_doc.size() - bodyoffset;

    cb::char_buffer blob_buffer{(char*)context.in_doc.data(),
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
    if (access != ENGINE_SUCCESS) {
        access = context.connection.remapErrorCode(access);
        if (access == ENGINE_DISCONNECT) {
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

    auto bodysize = context.in_doc.size();
    auto bodyoffset = 0;

    if (mcbp::datatype::is_xattr(context.in_datatype)) {
        bodyoffset = cb::xattr::get_body_offset(context.in_doc);;
        bodysize -= bodyoffset;
    }

    cb::char_buffer blob_buffer{(char*)context.in_doc.data(),
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

    std::unique_ptr<char[]> temp_doc;
    std::string_view document{value_buf.data(), value_buf.size()};

    context.generate_macro_padding(document, cb::xattr::macros::CAS);
    context.generate_macro_padding(document, cb::xattr::macros::SEQNO);
    context.generate_macro_padding(document, cb::xattr::macros::VALUE_CRC32C);

    bool modified;
    auto datatype = PROTOCOL_BINARY_DATATYPE_JSON;
    if (!operate_single_doc(context, document, datatype, temp_doc, modified)) {
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

    if (document.size() > key.size()) {
        const char* start = strchr(document.data(), ':') + 1;
        const char* end = document.data() + document.size() - 1;

        copy.set(key, {start, size_t(end - start)});
    } else {
        copy.remove(key);
    }
    const auto new_xattr = copy.finalize();
    replace_xattrs(new_xattr, context, bodyoffset, bodysize);

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
    std::string_view document{context.in_doc};

    if (mcbp::datatype::is_xattr(context.in_datatype)) {
        // We shouldn't have any documents like that!
        xattrsize = cb::xattr::get_body_offset(document);
        document.remove_prefix(xattrsize);
    }

    std::unique_ptr<char[]> temp_doc;
    bool modified;

    if (!operate_single_doc(
                context, document, context.in_datatype, temp_doc, modified)) {
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
        context.temp_doc.swap(temp_doc);
        context.in_doc = {context.temp_doc.get(), document.size()};
        return true;
    }

    // Time to rebuild the full document.
    auto total = xattrsize + document.size();
    std::unique_ptr<char[]> full_document(new char[total]);;

    memcpy(full_document.get(), context.in_doc.data(), xattrsize);
    memcpy(full_document.get() + xattrsize, document.data(), document.size());

    context.temp_doc.swap(full_document);
    context.in_doc = { context.temp_doc.get(), total };

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
static ENGINE_ERROR_CODE subdoc_update(SubdocCmdContext& context,
                                       ENGINE_ERROR_CODE ret,
                                       cb::const_byte_buffer key,
                                       uint32_t expiration) {
    auto& connection = context.connection;
    auto& cookie = context.cookie;

    if (context.getCurrentPhase() == SubdocCmdContext::Phase::XATTR) {
        LOG_WARNING(
                "Internal error: We should not reach subdoc_update in the "
                "xattr phase");
        return ENGINE_FAILED;
    }

    if (!context.traits.is_mutator) {
        // No update required - just make sure we have the correct cas to use
        // for response.
        cookie.setCas(context.in_cas);
        return ENGINE_SUCCESS;
    }

    // For multi-mutations, we only want to actually update the engine if /all/
    // paths succeeded - otherwise the document is unchanged (and we continue
    // to subdoc_response() to send information back to the client on what
    // succeeded/failed.
    if (context.overall_status != cb::mcbp::Status::Success) {
        return ENGINE_SUCCESS;
    }

    // Allocate a new item of this size.
    if (context.out_doc == nullptr &&
        !(context.no_sys_xattrs && context.do_delete_doc)) {

        if (ret == ENGINE_SUCCESS) {
            context.out_doc_len = context.in_doc.size();
            auto allocate_key = cookie.getConnection().makeDocKey(key);
            const size_t priv_bytes =
                cb::xattr::get_system_xattr_size(context.in_datatype,
                                                 context.in_doc);

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
                    ret = ENGINE_SUCCESS;
                } else {
                    ret = ENGINE_ENOMEM;
                }
            } catch (const cb::engine_error& e) {
                ret = ENGINE_ERROR_CODE(e.code().value());
                ret = context.connection.remapErrorCode(ret);
            }
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            // Save the allocated document in the cmd context.
            break;

        case ENGINE_EWOULDBLOCK:
            cookie.setEwouldblock(true);
            return ret;

        case ENGINE_DISCONNECT:
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
        item_info new_doc_info;
        if (!bucket_get_item_info(
                    connection, context.out_doc.get(), &new_doc_info)) {
            cookie.sendResponse(cb::mcbp::Status::Einternal);
            return ENGINE_FAILED;
        }

        // Copy the new document into the item.
        char* write_ptr = static_cast<char*>(new_doc_info.value[0].iov_base);
        std::memcpy(write_ptr, context.in_doc.data(), context.in_doc.size());
    }

    // And finally, store the new document.
    uint64_t new_cas;
    mutation_descr_t mdt;
    auto new_op = context.needs_new_doc ? OPERATION_ADD : OPERATION_CAS;
    if (ret == ENGINE_SUCCESS) {
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
    case ENGINE_SUCCESS:
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
                    return ENGINE_FAILED;
                }

                context.vbucket_uuid = info.vbucket_uuid;
                context.sequence_no = info.seqno;
            }
        }

        cookie.setCas(new_cas);
        break;

    case ENGINE_NOT_STORED:
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
        if (new_op == OPERATION_ADD &&
            context.mutationSemantics == MutationSemantics::Set) {
            ret = ENGINE_KEY_EEXISTS;
        } else {
            // Otherwise ENGINE_NOT_STORED is terminal - return to client.
            cookie.sendResponse(cb::engine_errc(ret));
        }
        break;

    case ENGINE_KEY_EEXISTS:
        // CAS mismatch. Caller may choose to retry this (without necessarily
        // telling the client), so send so response here...
        break;

    case ENGINE_EWOULDBLOCK:
        cookie.setEwouldblock(true);
        break;

    case ENGINE_DISCONNECT:
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
                                    index, op, scratch));
                    connection.copyToOutputStream({mloc.at, mloc.length});
                }
            } else {
                // Failure - encode first unsuccessful path index and status.
                if (op.status != cb::mcbp::Status::Success) {
                    connection.copyToOutputStream(
                            encode_multi_mutation_result_spec(
                                    index, op, scratch));
                    // Only the first unsuccessful op is reported.
                    break;
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
                connection.copyToOutputStream(h.getBuffer());
                connection.copyToOutputStream({mloc.at, mloc.length});
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
    return subdoc_executor(cookie,
                           get_traits<cb::mcbp::ClientOpcode::SubdocGet>());
}

void subdoc_exists_executor(Cookie& cookie) {
    return subdoc_executor(cookie,
                           get_traits<cb::mcbp::ClientOpcode::SubdocExists>());
}

void subdoc_dict_add_executor(Cookie& cookie) {
    return subdoc_executor(cookie,
                           get_traits<cb::mcbp::ClientOpcode::SubdocDictAdd>());
}

void subdoc_dict_upsert_executor(Cookie& cookie) {
    return subdoc_executor(
            cookie, get_traits<cb::mcbp::ClientOpcode::SubdocDictUpsert>());
}

void subdoc_delete_executor(Cookie& cookie) {
    return subdoc_executor(cookie,
                           get_traits<cb::mcbp::ClientOpcode::SubdocDelete>());
}

void subdoc_replace_executor(Cookie& cookie) {
    return subdoc_executor(cookie,
                           get_traits<cb::mcbp::ClientOpcode::SubdocReplace>());
}

void subdoc_array_push_last_executor(Cookie& cookie) {
    return subdoc_executor(
            cookie, get_traits<cb::mcbp::ClientOpcode::SubdocArrayPushLast>());
}

void subdoc_array_push_first_executor(Cookie& cookie) {
    return subdoc_executor(
            cookie, get_traits<cb::mcbp::ClientOpcode::SubdocArrayPushFirst>());
}

void subdoc_array_insert_executor(Cookie& cookie) {
    return subdoc_executor(
            cookie, get_traits<cb::mcbp::ClientOpcode::SubdocArrayInsert>());
}

void subdoc_array_add_unique_executor(Cookie& cookie) {
    return subdoc_executor(
            cookie, get_traits<cb::mcbp::ClientOpcode::SubdocArrayAddUnique>());
}

void subdoc_counter_executor(Cookie& cookie) {
    return subdoc_executor(cookie,
                           get_traits<cb::mcbp::ClientOpcode::SubdocCounter>());
}

void subdoc_get_count_executor(Cookie& cookie) {
    return subdoc_executor(
            cookie, get_traits<cb::mcbp::ClientOpcode::SubdocGetCount>());
}

void subdoc_multi_lookup_executor(Cookie& cookie) {
    return subdoc_executor(
            cookie, get_traits<cb::mcbp::ClientOpcode::SubdocMultiLookup>());
}

void subdoc_multi_mutation_executor(Cookie& cookie) {
    return subdoc_executor(
            cookie, get_traits<cb::mcbp::ClientOpcode::SubdocMultiMutation>());
}
