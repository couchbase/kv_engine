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

#include "connections.h"
#include "debug_helpers.h"
#include "mcaudit.h"
#include "mcbp.h"
#include "protocol/mcbp/engine_wrapper.h"
#include "subdoc/util.h"
#include "subdocument_context.h"
#include "subdocument_traits.h"
#include "subdocument_validators.h"
#include "timings.h"
#include "topkeys.h"
#include "utilities/logtags.h"
#include "utilities/protocol2text.h"
#include "xattr/key_validator.h"
#include "xattr/utils.h"

#include <memcached/protocol_binary.h>
#include <memcached/types.h>
#include <platform/histogram.h>
#include <xattr/blob.h>

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
                         const char* key,
                         size_t keylen,
                         uint16_t vbucket,
                         uint64_t cas);

static bool subdoc_operate(SubdocCmdContext& context);

static ENGINE_ERROR_CODE subdoc_update(SubdocCmdContext& context,
                                       ENGINE_ERROR_CODE ret,
                                       const char* key, size_t keylen,
                                       uint16_t vbucket, uint32_t expiration);
static void subdoc_response(Cookie& cookie, SubdocCmdContext& context);

// Debug - print details of the specified subdocument command.
static void subdoc_print_command(Connection& c, protocol_binary_command cmd,
                                 const char* key, const uint16_t keylen,
                                 const char* path, const uint16_t pathlen,
                                 const char* value, const uint32_t vallen) {
    char clean_key[KEY_MAX_LENGTH + 32];
    char clean_path[SUBDOC_PATH_MAX_LENGTH];
    char clean_value[80]; // only print the first few characters of the value.
    if ((key_to_printable_buffer(clean_key, sizeof(clean_key), c.getId(), true,
                                 memcached_opcode_2_text(cmd), key, keylen)
                    != -1)
                    && (buf_to_printable_buffer(clean_path, sizeof(clean_path),
                                                path, pathlen) != -1)) {
        // print key, path & value if there is a value.
        if ((vallen > 0)
                        && (buf_to_printable_buffer(clean_value,
                                                    sizeof(clean_value), value,
                                                    vallen) != -1)) {
            LOG_DEBUG("{} path:'{}' value:'{}'",
                      cb::logtags::tagUserData(clean_key),
                      cb::logtags::tagUserData(clean_path),
                      cb::logtags::tagUserData(clean_value));

        } else {
            // key & path only
            LOG_DEBUG("{} path:'{}'",
                      cb::logtags::tagUserData(clean_key),
                      cb::logtags::tagUserData(clean_path));
        }
    }
}

/*
 * Definitions
 */
static void create_single_path_context(SubdocCmdContext& context,
                                       Cookie& cookie,
                                       const SubdocCmdTraits traits,
                                       const void* packet,
                                       cb::const_char_buffer value,
                                       doc_flag doc_flags) {
    const protocol_binary_request_subdocument *req =
        reinterpret_cast<const protocol_binary_request_subdocument*>(packet);

    auto flags = static_cast<protocol_binary_subdoc_flag>(
            req->message.extras.subdoc_flags);
    const protocol_binary_command mcbp_cmd =
        protocol_binary_command(req->message.header.request.opcode);
    const uint16_t pathlen = ntohs(req->message.extras.pathlen);

    // Path is the first thing in the value; remainder is the operation
    // value.
    auto path = value;
    path.len = pathlen;

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

    context.setMutationSemantics(doc_flags);

    // Decode as single path; add a single operation to the context.
    if (traits.request_has_value) {
        // Adjust value to move past the path.
        value.buf += pathlen;
        value.len -= pathlen;

        ops.emplace_back(SubdocCmdContext::OperationSpec{traits, flags,
                                                         path, value});
    } else {
        ops.emplace_back(SubdocCmdContext::OperationSpec{traits, flags, path});
    }

    if (impliesMkdir_p(doc_flags)) {
        context.jroot_type = Subdoc::Util::get_root_type(
                traits.subdocCommand, path.buf, path.len);
    }

    if (settings.getVerbose() > 1) {
        const uint8_t extlen = req->message.header.request.extlen;
        const char* key = (char*)packet + sizeof(req->message.header) + extlen;
        const uint16_t keylen = ntohs(req->message.header.request.keylen);

        subdoc_print_command(cookie.getConnection(),
                             mcbp_cmd,
                             key,
                             keylen,
                             path.buf,
                             path.len,
                             value.buf,
                             value.len);
    }
}

static void create_multi_path_context(SubdocCmdContext& context,
                                      Cookie& cookie,
                                      const SubdocCmdTraits traits,
                                      const void* packet,
                                      cb::const_char_buffer value,
                                      doc_flag doc_flags) {
    // Decode each of lookup specs from the value into our command context.
    context.setMutationSemantics(doc_flags);
    size_t offset = 0;
    while (offset < value.len) {
        protocol_binary_command binprot_cmd;
        protocol_binary_subdoc_flag flags;
        size_t headerlen;
        cb::const_char_buffer path;
        cb::const_char_buffer spec_value;
        if (traits.is_mutator) {
            auto* spec = reinterpret_cast<const protocol_binary_subdoc_multi_mutation_spec*>
                (value.buf + offset);
            headerlen = sizeof(*spec);
            binprot_cmd = protocol_binary_command(spec->opcode);
            flags = protocol_binary_subdoc_flag(spec->flags);
            path = {value.buf + offset + headerlen,
                    htons(spec->pathlen)};
            spec_value = {value.buf + offset + headerlen + path.len,
                          htonl(spec->valuelen)};

        } else {
            auto* spec = reinterpret_cast<const protocol_binary_subdoc_multi_lookup_spec*>
                (value.buf + offset);
            headerlen = sizeof(*spec);
            binprot_cmd = protocol_binary_command(spec->opcode);
            flags = protocol_binary_subdoc_flag(spec->flags);
            path = {value.buf + offset + headerlen,
                    htons(spec->pathlen)};
            spec_value = {nullptr, 0};
        }

        auto traits = get_subdoc_cmd_traits(binprot_cmd);
        if (impliesMkdir_p(doc_flags) &&
            context.jroot_type == 0) {
            // Determine the root type
            context.jroot_type = Subdoc::Util::get_root_type(
                    traits.subdocCommand, path.buf, path.len);
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
            context.set_xattr_key({path.data(), xattr_keylen});
        }

        const SubdocCmdContext::Phase phase = xattr ?
                                              SubdocCmdContext::Phase::XATTR :
                                              SubdocCmdContext::Phase::Body;

        auto& ops = context.getOperations(phase);

        // Mkdoc and Add imply MKDIR_P, ensure that MKDIR_P is set
        if (impliesMkdir_p(doc_flags)) {
            flags = flags | SUBDOC_FLAG_MKDIR_P;
        }
        if (traits.mcbpCommand == PROTOCOL_BINARY_CMD_DELETE) {
            context.do_delete_doc = true;
        }
        ops.emplace_back(SubdocCmdContext::OperationSpec{traits, flags, path,
                                                         spec_value});
        offset += headerlen + path.len + spec_value.len;
    }

    if (settings.getVerbose() > 1) {
        const protocol_binary_request_subdocument *req =
            reinterpret_cast<const protocol_binary_request_subdocument*>(packet);

        const protocol_binary_command mcbp_cmd =
            protocol_binary_command(req->message.header.request.opcode);

        const uint8_t extlen = req->message.header.request.extlen;
        const char* key = (char*)packet + sizeof(req->message.header) + extlen;
        const uint16_t keylen = ntohs(req->message.header.request.keylen);

        const char path[] = "<multipath>";
        subdoc_print_command(cookie.getConnection(),
                             mcbp_cmd,
                             key,
                             keylen,
                             path,
                             strlen(path),
                             value.buf,
                             value.len);
    }
}

static SubdocCmdContext* subdoc_create_context(Cookie& cookie,
                                               const SubdocCmdTraits traits,
                                               const void* packet,
                                               cb::const_char_buffer value,
                                               doc_flag doc_flags) {
    try {
        std::unique_ptr<SubdocCmdContext> context;
        context.reset(new SubdocCmdContext(cookie, traits));
        switch (traits.path) {
        case SubdocPath::SINGLE:
            create_single_path_context(
                    *context.get(), cookie, traits, packet, value, doc_flags);
            break;

        case SubdocPath::MULTI:
            create_multi_path_context(
                    *context.get(), cookie, traits, packet, value, doc_flags);
            break;
        }

        return context.release();
    } catch (const std::bad_alloc&) {
        return nullptr;
    }
}

/* Decode the specified expiration value for the specified request.
 */
uint32_t subdoc_decode_expiration(const protocol_binary_request_header* header,
                                  const SubdocCmdTraits traits) {
    // Expiration is zero (never expire) unless an (optional) 4-byte expiry
    // value was included in the extras.
    const char* expiration_ptr = nullptr;

    // Single-path and multi-path have different extras encodings:
    switch (traits.path) {
    case SubdocPath::SINGLE:
        if (header->request.extlen == SUBDOC_EXPIRY_EXTRAS_LEN) {
            expiration_ptr = reinterpret_cast<const char*>(header) +
                             sizeof(*header) + SUBDOC_BASIC_EXTRAS_LEN;
        }
        break;

    case SubdocPath::MULTI:
        if ((header->request.extlen == SUBDOC_MULTI_EXPIRY_EXTRAS_LEN) ||
            (header->request.extlen == SUBDOC_MULTI_ALL_EXTRAS_LEN)) {
            expiration_ptr = reinterpret_cast<const char*>(header) +
                             sizeof(*header);
        }
        break;
    }

    if (expiration_ptr != nullptr) {
        return ntohl(*reinterpret_cast<const uint32_t*>(expiration_ptr));
    } else {
        return 0;
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
    auto packet = cookie.getPacket(Cookie::PacketContent::Full);
    const auto* req =
            reinterpret_cast<const protocol_binary_request_subdocument*>(
                    packet.data());
    const protocol_binary_request_header* header = &req->message.header;

    const uint8_t extlen = header->request.extlen;
    const uint16_t keylen = ntohs(header->request.keylen);
    const uint32_t bodylen = ntohl(header->request.bodylen);
    const uint16_t vbucket = ntohs(header->request.vbucket);
    const uint64_t cas = ntohll(header->request.cas);

    const char* key = (char*)packet.data() + sizeof(*header) + extlen;

    const char* value = key + keylen;
    const uint32_t vallen = bodylen - keylen - extlen;

    const uint32_t expiration = subdoc_decode_expiration(header, traits);
    const doc_flag doc_flags = subdoc_decode_doc_flags(header, traits.path);

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
            cb::const_char_buffer value_buf{value, vallen};
            context = subdoc_create_context(
                    cookie, traits, packet.data(), value_buf, doc_flags);
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
        if (!subdoc_fetch(cookie, *context, ret, key, keylen, vbucket, cas)) {
            return;
        }

        // 2. Perform the operation specified by CMD. Again, return if it fails.
        if (!subdoc_operate(*context)) {
            return;
        }

        // 3. Update the document in the engine (mutations only).
        ret = subdoc_update(*context, ret, key, keylen, vbucket, expiration);
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
            thread_stats->bytes_subdoc_lookup_total += context->in_doc.len;
            thread_stats->bytes_subdoc_lookup_extracted += context->response_val_len;

            STATS_HIT(&cookie.getConnection(), get);
        }
        update_topkeys(cookie);
        return;
    } while (auto_retry && attempts < MAXIMUM_ATTEMPTS);

    // Hit maximum attempts - this theoretically could happen but shouldn't
    // in reality.
    const auto mcbp_cmd = protocol_binary_command(header->request.opcode);

    auto& c = cookie.getConnection();
    LOG_WARNING(
            "{}: Subdoc: Hit maximum number of auto-retry attempts ({}) when "
            "attempting to perform op {} for client {} - returning TMPFAIL",
            c.getId(),
            MAXIMUM_ATTEMPTS,
            memcached_opcode_2_text(mcbp_cmd),
            c.getDescription());
    cookie.sendResponse(cb::mcbp::Status::Etmpfail);
}

// Fetch the item to operate on from the engine.
// Returns true if the command was successful (and execution should continue),
// else false.
static bool subdoc_fetch(Cookie& cookie,
                         SubdocCmdContext& ctx,
                         ENGINE_ERROR_CODE ret,
                         const char* key,
                         size_t keylen,
                         uint16_t vbucket,
                         uint64_t cas) {
    if (!ctx.fetchedItem && !ctx.needs_new_doc) {
        if (ret == ENGINE_SUCCESS) {
            DocKey get_key(reinterpret_cast<const uint8_t*>(key),
                           keylen,
                           cookie.getConnection().getDocNamespace());
            DocStateFilter state = DocStateFilter::Alive;
            if (ctx.do_allow_deleted_docs) {
                state = DocStateFilter::AliveOrDeleted;
            }
            auto r = bucket_get(cookie, get_key, vbucket, state);
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
            return true;

        case ENGINE_EWOULDBLOCK:
            cookie.setEwouldblock(true);
            return false;

        case ENGINE_DISCONNECT:
            cookie.getConnection().setState(McbpStateMachine::State::closing);
            return false;

        default:
            cookie.sendResponse(cb::engine_errc(ret));
            return false;
        }
    }

    if (ctx.in_doc.buf == nullptr) {
        // Retrieve the item_info the engine, and if necessary
        // uncompress it so subjson can parse it.
        auto status = ctx.get_document_for_searching(cas);

        if (status != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
            // Failed. Note c.item and c.commandContext will both be freed for
            // us as part of preparing for the next command.
            cookie.sendResponse(cb::mcbp::Status(status));
            return false;
        }
    }

    return true;
}

/**
 * Perform the subjson operation specified by {spec} to one path in the
 * document.
 */
static protocol_binary_response_status
subdoc_operate_one_path(SubdocCmdContext& context, SubdocCmdContext::OperationSpec& spec,
                        const cb::const_char_buffer& in_doc) {

    // Prepare the specified sub-document command.
    auto& op = context.connection.getThread()->subdoc_op;
    op.clear();
    op.set_result_buf(&spec.result);
    op.set_code(spec.traits.subdocCommand);
    op.set_doc(in_doc.buf, in_doc.len);

    if (spec.flags & SUBDOC_FLAG_EXPAND_MACROS) {
        auto padded_macro = context.get_padded_macro(spec.value);
        op.set_value(padded_macro.buf, padded_macro.len);
    } else {
        op.set_value(spec.value.buf, spec.value.len);
    }

    if (context.getCurrentPhase() == SubdocCmdContext::Phase::XATTR &&
        spec.path.buf[0] == '$') {
        if (spec.path.buf[1] == 'd') {
            // This is a call to the "$document" (the validator stopped all of
            // the other ones), so replace the document with
            // the document virtual one..
            auto doc = context.get_document_vattr();
            op.set_doc(doc.data(), doc.size());
        } else if (spec.path.buf[1] == 'X') {
            auto doc = context.get_xtoc_vattr();
            op.set_doc(doc.data(), doc.size());
        }
    }

    // ... and execute it.
    const auto subdoc_res = op.op_exec(spec.path.buf, spec.path.len);

    switch (subdoc_res) {
    case Subdoc::Error::SUCCESS:
        return PROTOCOL_BINARY_RESPONSE_SUCCESS;

    case Subdoc::Error::PATH_ENOENT:
        return PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_ENOENT;

    case Subdoc::Error::PATH_MISMATCH:
        return PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_MISMATCH;

    case Subdoc::Error::DOC_ETOODEEP:
        return PROTOCOL_BINARY_RESPONSE_SUBDOC_DOC_E2DEEP;

    case Subdoc::Error::PATH_EINVAL:
        return PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_EINVAL;

    case Subdoc::Error::DOC_NOTJSON:
        return PROTOCOL_BINARY_RESPONSE_SUBDOC_DOC_NOTJSON;

    case Subdoc::Error::DOC_EEXISTS:
        return PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_EEXISTS;

    case Subdoc::Error::PATH_E2BIG:
        return PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_E2BIG;

    case Subdoc::Error::NUM_E2BIG:
        return PROTOCOL_BINARY_RESPONSE_SUBDOC_NUM_ERANGE;

    case Subdoc::Error::DELTA_EINVAL:
        return PROTOCOL_BINARY_RESPONSE_SUBDOC_DELTA_EINVAL;

    case Subdoc::Error::VALUE_CANTINSERT:
        return PROTOCOL_BINARY_RESPONSE_SUBDOC_VALUE_CANTINSERT;

    case Subdoc::Error::DELTA_OVERFLOW:
        return PROTOCOL_BINARY_RESPONSE_SUBDOC_VALUE_CANTINSERT;

    case Subdoc::Error::VALUE_ETOODEEP:
        return PROTOCOL_BINARY_RESPONSE_SUBDOC_VALUE_ETOODEEP;

    default:
        // TODO: handle remaining errors.
        LOG_DEBUG("Unexpected response from subdoc: {} ({:x})",
                  subdoc_res,
                  subdoc_res);
        return PROTOCOL_BINARY_RESPONSE_EINTERNAL;
    }
}

/**
 * Perform the wholedoc (mcbp) operation defined by spec
 */
static protocol_binary_response_status subdoc_operate_wholedoc(
        SubdocCmdContext& context,
        SubdocCmdContext::OperationSpec& spec,
        cb::const_char_buffer& doc) {
    switch (spec.traits.mcbpCommand) {
    case PROTOCOL_BINARY_CMD_GET:
        if (doc.size() == 0) {
            // Size of zero indicates the document body ("path") doesn't exist.
            return PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_ENOENT;
        }
        spec.result.set_matchloc({doc.buf, doc.len});
        return PROTOCOL_BINARY_RESPONSE_SUCCESS;

    case PROTOCOL_BINARY_CMD_SET:
        spec.result.push_newdoc({spec.value.buf, spec.value.len});
        return PROTOCOL_BINARY_RESPONSE_SUCCESS;

    case PROTOCOL_BINARY_CMD_DELETE:
        context.in_datatype &= ~BODY_ONLY_DATATYPE_MASK;
        spec.result.push_newdoc({nullptr, 0});
        return PROTOCOL_BINARY_RESPONSE_SUCCESS;

    default:
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }
}

/**
 * Run through all of the subdoc operations for the current phase on
 * a single 'document' (either the user document, or a XATTR).
 *
 * @param context The context object for this operation
 * @param doc the document to operate on
 * @param doc_datatype The datatype of the document.
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
                                cb::const_char_buffer& doc,
                                protocol_binary_datatype_t doc_datatype,
                                std::unique_ptr<char[]>& temp_buffer,
                                bool& modified) {
    modified = false;
    auto& operations = context.getOperations();

    // 2. Perform each of the operations on document.
    for (auto op = operations.begin(); op != operations.end(); op++) {
        switch (op->traits.scope) {
        case CommandScope::SubJSON:
            if (mcbp::datatype::is_json(doc_datatype)) {
                // Got JSON, perform the operation.
                op->status = subdoc_operate_one_path(context, *op, doc);
            } else {
                // No good; need to have JSON.
                op->status = PROTOCOL_BINARY_RESPONSE_SUBDOC_DOC_NOTJSON;
            }
            break;

        case CommandScope::WholeDoc:
            op->status = subdoc_operate_wholedoc(context, *op, doc);
        }

        if (op->status == PROTOCOL_BINARY_RESPONSE_SUCCESS) {
            if (context.traits.is_mutator) {
                modified = true;

                // Determine how much space we now need.
                size_t new_doc_len = 0;
                for (auto& loc : op->result.newdoc()) {
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
                // std::vector, and point in_doc at that.

                // Allocate an extra byte to make sure we can zero term it
                // (in case we want to use cJSON_Parse() ;-)
                std::unique_ptr<char[]> temp(new char[new_doc_len + 1]);
                temp[new_doc_len] = '\0';

                size_t offset = 0;
                for (auto& loc : op->result.newdoc()) {
                    std::memcpy(temp.get() + offset, loc.at, loc.length);
                    offset += loc.length;
                }

                // Copying complete - safe to delete the old temp_doc
                // (even if it was the source of some of the newdoc
                // iovecs).
                temp_buffer.swap(temp);
                doc.buf = temp_buffer.get();
                doc.len = new_doc_len;
            } else { // lookup
                // nothing to do.
            }
        } else {
            switch (context.traits.path) {
            case SubdocPath::SINGLE:
                // Failure of a (the only) op stops execution and returns an
                // error to the client.
                context.cookie.sendResponse(cb::mcbp::Status(op->status));
                return false;

            case SubdocPath::MULTI:
                context.overall_status
                    = PROTOCOL_BINARY_RESPONSE_SUBDOC_MULTI_PATH_FAILURE;
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

static ENGINE_ERROR_CODE validate_vattr_privilege(SubdocCmdContext& context) {
    auto key = context.get_xattr_key();

    // The $document vattr doesn't require any xattr permissions.

    if (key.buf[1] == 'X') {
        // In the xtoc case we want to see which privileges the connection has
        // to determine which XATTRs we tell the user about

        bool xattrRead = false;
        auto access = context.connection.checkPrivilege(
                cb::rbac::Privilege::XattrRead, context.cookie);
        switch (access) {
        case cb::rbac::PrivilegeAccess::Ok:
            xattrRead = true;
            break;
        case cb::rbac::PrivilegeAccess::Fail:
            xattrRead = false;
            break;
        case cb::rbac::PrivilegeAccess::Stale:
            return ENGINE_AUTH_STALE;
        }

        bool xattrSysRead = false;
        access = context.connection.checkPrivilege(
                cb::rbac::Privilege::SystemXattrRead, context.cookie);
        switch (access) {
        case cb::rbac::PrivilegeAccess::Ok:
            xattrSysRead = true;
            break;
        case cb::rbac::PrivilegeAccess::Fail:
            xattrSysRead = false;
            break;
        case cb::rbac::PrivilegeAccess::Stale:
            return ENGINE_AUTH_STALE;
        }

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
    auto key = context.get_xattr_key();
    if (key.empty()) {
        return ENGINE_SUCCESS;
    }

    if (cb::xattr::is_vattr(key)) {
        return validate_vattr_privilege(context);
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

    auto access = context.connection.checkPrivilege(privilege, context.cookie);
    switch (access) {
    case cb::rbac::PrivilegeAccess::Ok:
        return ENGINE_SUCCESS;
    case cb::rbac::PrivilegeAccess::Fail:
        return ENGINE_EACCESS;
    case cb::rbac::PrivilegeAccess::Stale:
        return ENGINE_AUTH_STALE;
    }

    throw std::logic_error(
        "validate_xattr_privilege: invalid return value from checkPrivilege");
}

/**
 * Replaces the xattrs on the document with the new ones provided
 * @param new_xattr The new xattrs to use
 * @param context The command context for this operation
 * @param bodyoffset The offset in to the body of the xattr section
 * @param bodysize The size of the body (excludes xattrs)
 */
static inline void replace_xattrs(const cb::char_buffer& new_xattr,
                                  SubdocCmdContext& context,
                                  const size_t bodyoffset,
                                  const size_t bodysize) {
    auto total = new_xattr.len + bodysize;

    std::unique_ptr<char[]> full_document(new char[total]);
    std::copy(
            new_xattr.buf, new_xattr.buf + new_xattr.len, full_document.get());
    std::copy(context.in_doc.buf + bodyoffset,
              context.in_doc.buf + bodyoffset + bodysize,
              full_document.get() + new_xattr.len);

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
    const auto bodysize = context.in_doc.len - bodyoffset;

    cb::char_buffer blob_buffer{(char*)context.in_doc.buf, (size_t)bodyoffset};

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
            context.connection.setState(McbpStateMachine::State::closing);
            return false;
        }

        switch (context.traits.path) {
        case SubdocPath::SINGLE:
            // Failure of a (the only) op stops execution and returns an
            // error to the client.
            context.cookie.sendResponse(cb::engine_errc(access));
            return false;

        case SubdocPath::MULTI:
            context.overall_status
                = PROTOCOL_BINARY_RESPONSE_SUBDOC_MULTI_PATH_FAILURE;

            {
                // Mark all of them as failed..
                auto& operations = context.getOperations();
                for (auto op = operations.begin(); op != operations.end(); op++) {
                    op->status = engine_error_2_mcbp_protocol_error(access);
                }
            }
            return true;
        }
        throw std::logic_error("do_xattr_phase: unknown SubdocPath");
    }

    auto bodysize = context.in_doc.len;
    auto bodyoffset = 0;

    if (mcbp::datatype::is_xattr(context.in_datatype)) {
        bodyoffset = cb::xattr::get_body_offset(context.in_doc);;
        bodysize -= bodyoffset;
    }

    cb::char_buffer blob_buffer{(char*)context.in_doc.buf, (size_t)bodyoffset};

    const cb::xattr::Blob xattr_blob(
            blob_buffer, mcbp::datatype::is_snappy(context.in_datatype));
    auto key = context.get_xattr_key();
    auto value_buf = xattr_blob.get(key);

    if (value_buf.len == 0) {
        context.xattr_buffer.reset(new char[2]);
        context.xattr_buffer[0] = '{';
        context.xattr_buffer[1] = '}';
        value_buf = {context.xattr_buffer.get(), 2};
    } else {
        // To allow the subjson do it's thing with the full xattrs
        // create a full json doc looking like: {\"xattr_key\":\"value\"};
        size_t total = 5 + key.len + value_buf.len;
        context.xattr_buffer.reset(new char[total]);
        char* ptr = context.xattr_buffer.get();
        memcpy(ptr, "{\"", 2);
        ptr += 2;
        memcpy(ptr, key.buf, key.len);
        ptr += key.len;
        memcpy(ptr, "\":", 2);
        ptr += 2;
        memcpy(ptr, value_buf.buf, value_buf.len);
        ptr += value_buf.len;
        *ptr = '}';
        value_buf = { context.xattr_buffer.get(), total};
    }

    std::unique_ptr<char[]> temp_doc;
    cb::const_char_buffer document{value_buf.buf, value_buf.len};

    context.generate_macro_padding(document, cb::xattr::macros::CAS);
    context.generate_macro_padding(document, cb::xattr::macros::SEQNO);

    bool modified;
    if (!operate_single_doc(context,
                            document,
                            PROTOCOL_BINARY_DATATYPE_JSON,
                            temp_doc,
                            modified)) {
        // Something failed..
        return false;
    }

    if (context.overall_status != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
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

    if (document.len > key.len) {
        const char* start = strchr(document.buf, ':') + 1;
        const char* end = document.buf + document.len - 1;

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
    cb::const_char_buffer document {context.in_doc.buf,
                                    context.in_doc.len};

    if (mcbp::datatype::is_xattr(context.in_datatype)) {
        // We shouldn't have any documents like that!
        xattrsize = cb::xattr::get_body_offset(document);
        document.buf += xattrsize;
        document.len -= xattrsize;
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
        context.in_doc = { context.temp_doc.get(), document.len };
        return true;
    }

    // Time to rebuild the full document.
    auto total = xattrsize + document.len;
    std::unique_ptr<char[]> full_document(new char[total]);;

    memcpy(full_document.get(), context.in_doc.buf, xattrsize);
    memcpy(full_document.get() + xattrsize, document.buf, document.len);

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

    GenericBlockTimer<TimingHistogram, 0> bt(
            &all_buckets[context.connection.getBucketIndex()].subjson_operation_times);

    context.overall_status = PROTOCOL_BINARY_RESPONSE_SUCCESS;

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
                                       ENGINE_ERROR_CODE ret, const char* key,
                                       size_t keylen, uint16_t vbucket,
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
    if (context.overall_status != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        return ENGINE_SUCCESS;
    }

    // Allocate a new item of this size.
    if (context.out_doc == NULL &&
        !(context.no_sys_xattrs && context.do_delete_doc)) {

        if (ret == ENGINE_SUCCESS) {
            context.out_doc_len = context.in_doc.len;
            DocKey allocate_key(reinterpret_cast<const uint8_t*>(key),
                                keylen, connection.getDocNamespace());

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
                                            vbucket);
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
            connection.setState(McbpStateMachine::State::closing);
            return ret;

        default:
            cookie.sendResponse(cb::engine_errc(ret));
            return ret;
        }

        // To ensure we only replace the version of the document we
        // just appended to; set the CAS to the one retrieved from.
        bucket_item_set_cas(cookie, context.out_doc.get(), context.in_cas);

        // Obtain the item info (and it's iovectors)
        item_info new_doc_info;
        if (!bucket_get_item_info(
                    cookie, context.out_doc.get(), &new_doc_info)) {
            cookie.sendResponse(cb::mcbp::Status::Einternal);
            return ENGINE_FAILED;
        }

        // Copy the new document into the item.
        char* write_ptr = static_cast<char*>(new_doc_info.value[0].iov_base);
        std::memcpy(write_ptr, context.in_doc.buf, context.in_doc.len);
    }

    // And finally, store the new document.
    uint64_t new_cas;
    mutation_descr_t mdt;
    auto new_op = context.needs_new_doc ? OPERATION_ADD : OPERATION_CAS;
    if (context.do_delete_doc && context.no_sys_xattrs) {
        new_cas = context.in_cas;
        DocKey docKey(reinterpret_cast<const uint8_t*>(key),
                      keylen,
                      connection.getDocNamespace());
        ret = bucket_remove(cookie, docKey, new_cas, vbucket, mdt);
    } else {
        ret = bucket_store(cookie,
                           context.out_doc.get(),
                           new_cas,
                           new_op,
                           context.do_delete_doc ? DocumentState::Deleted
                                                 : context.in_document_state);
    }
    ret = connection.remapErrorCode(ret);
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
                            cookie, context.out_doc.get(), &info)) {
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
        connection.setState(McbpStateMachine::State::closing);
        break;

    default:
        cookie.sendResponse(cb::engine_errc(ret));
        break;
    }

    return ret;
}

/* Encodes the context's mutation sequence number and vBucket UUID into the
 * given buffer.
 * @param descr Buffer to write to. Must be 16 bytes in size.
 */
static void encode_mutation_descr(SubdocCmdContext& context, char* buffer)
{
    mutation_descr_t descr;
    descr.seqno = htonll(context.sequence_no);
    descr.vbucket_uuid = htonll(context.vbucket_uuid);
    std::memcpy(buffer, &descr, sizeof(descr));
}

/* Encodes the specified multi-mutation result into the given the given buffer.
 * @param index The operation index.
 * @param op Operation spec to encode.
 * @param buffer Buffer to encode into
 * @return The number of bytes written into the buffer.
 */
static size_t encode_multi_mutation_result_spec(uint8_t index,
                                                const SubdocCmdContext::OperationSpec& op,
                                                char* buffer)
{
    char* cursor = buffer;

    // Always encode the index and status.
    *reinterpret_cast<uint8_t*>(cursor) = index;
    cursor += sizeof(uint8_t);
    *reinterpret_cast<uint16_t*>(cursor) = htons(op.status);
    cursor += sizeof(uint16_t);

    // Also encode resultlen if status is success.
    if (op.status == PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        const auto& mloc = op.result.matchloc();
        *reinterpret_cast<uint32_t*>(cursor) = htonl(mloc.length);
        cursor += sizeof(uint32_t);
    }
    return cursor - buffer;
}

/* Construct and send a response to a single-path request back to the client.
 */
static void subdoc_single_response(Cookie& cookie, SubdocCmdContext& context) {
    auto& connection = context.connection;

    context.response_val_len = 0;
    cb::const_char_buffer value = {};
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
    cb::const_char_buffer extras = {};
    mutation_descr_t descr = {};
    if (connection.isSupportsMutationExtras() && context.traits.is_mutator) {
        encode_mutation_descr(context, reinterpret_cast<char*>(&descr));
        extras = {reinterpret_cast<const char*>(&descr), sizeof(descr)};
    }

    auto status_code = cb::mcbp::Status::Success;
    if (context.in_document_state == DocumentState::Deleted) {
        status_code = cb::mcbp::Status::SubdocSuccessDeleted;
    }

    cookie.sendResponse(cb::mcbp::Status(status_code),
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
    DynamicBuffer& response_buf = cookie.getDynamicBuffer();
    size_t extlen = 0;
    char* extras_ptr = nullptr;

    // Encode mutation extras into buffer if success & they were requested.
    if (context.overall_status == PROTOCOL_BINARY_RESPONSE_SUCCESS &&
            connection.isSupportsMutationExtras()) {
        extlen = sizeof(mutation_descr_t);
        if (!response_buf.grow(extlen)) {
            // Unable to form complete response.
            cookie.sendResponse(cb::mcbp::Status::Enomem);
            return;
        }
        extras_ptr = response_buf.getCurrent();
        encode_mutation_descr(context, extras_ptr);
        response_buf.moveOffset(extlen);
    }

    // Calculate how much space we need in our dynamic buffer, and total body
    // size to encode into the header.
    size_t response_buf_needed;
    size_t iov_len = 0;
    if (context.overall_status == PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        cb::audit::document::add(cookie,
                                 cb::audit::document::Operation::Modify);

        // on success, one per each non-zero length result.
        response_buf_needed = 0;
        for (auto phase : phases) {
            for (size_t ii = 0;
                 ii < context.getOperations(phase).size(); ii++) {
                const auto& op = context.getOperations(phase)[ii];
                const auto mloc = op.result.matchloc();
                if (op.traits.responseHasValue() && mloc.length > 0) {
                    response_buf_needed += sizeof(uint8_t) + sizeof(uint16_t) +
                                           sizeof(uint32_t);
                    iov_len += mloc.length;
                }
            }
        }
    } else {
        // Just one - index and status of first failure.
        response_buf_needed = sizeof(uint8_t) + sizeof(uint16_t);
    }

    // We need two iovecs per operation result:
    // 1. result_spec header (index, status; resultlen for successful specs).
    //    Use the dynamicBuffer for this.
    // 2. actual value - this already resides in the Subdoc::Result.
    if (!response_buf.grow(response_buf_needed)) {
        // Unable to form complete response.
        cookie.sendResponse(cb::mcbp::Status::Enomem);
        return;
    }

    auto status_code = context.overall_status;
    if ((status_code == PROTOCOL_BINARY_RESPONSE_SUCCESS) &&
        (context.in_document_state == DocumentState::Deleted)) {
        status_code = PROTOCOL_BINARY_RESPONSE_SUBDOC_SUCCESS_DELETED;
    }

    // Allocated required resource - build the header.
    mcbp_add_header(cookie,
                    status_code,
                    extlen,
                    /*keylen*/ 0,
                    extlen + response_buf_needed + iov_len,
                    PROTOCOL_BINARY_RAW_BYTES);

    // Append extras if requested.
    if (extlen > 0) {
        connection.addIov(reinterpret_cast<void*>(extras_ptr), extlen);
    }

    // Append the iovecs for each operation result.
    size_t index = 0;
    for (auto phase : phases) {
        for (size_t ii = 0; ii < context.getOperations(phase).size(); ii++, index++) {
            const auto& op = context.getOperations(phase)[ii];
            // Successful - encode all non-zero length results.
            if (context.overall_status == PROTOCOL_BINARY_RESPONSE_SUCCESS) {
                const auto mloc = op.result.matchloc();
                if (op.traits.responseHasValue() && mloc.length > 0) {
                    char* header = response_buf.getCurrent();
                    size_t header_sz =
                        encode_multi_mutation_result_spec(index, op, header);

                    connection.addIov(reinterpret_cast<void*>(header),
                                      header_sz);
                    connection.addIov(mloc.at, mloc.length);

                    response_buf.moveOffset(header_sz);
                }
            } else {
                // Failure - encode first unsuccessful path index and status.
                if (op.status != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
                    char* header = response_buf.getCurrent();
                    size_t header_sz =
                        encode_multi_mutation_result_spec(index, op, header);

                    connection.addIov(reinterpret_cast<void*>(header),
                                      header_sz);
                    response_buf.moveOffset(header_sz);

                    // Only the first unsuccessful op is reported.
                    break;
                }
            }
        }
    }
    connection.setState(McbpStateMachine::State::send_data);
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
    DynamicBuffer& response_buf = cookie.getDynamicBuffer();
    size_t needed = (sizeof(uint16_t) + sizeof(uint32_t)) *
        (context.getOperations(SubdocCmdContext::Phase::XATTR).size() +
         context.getOperations(SubdocCmdContext::Phase::Body).size());

    if (!response_buf.grow(needed)) {
        // Unable to form complete response.
        cookie.sendResponse(cb::mcbp::Status::Enomem);
        return;
    }

    // Allocated required resource - build the header.
    auto status_code = context.overall_status;
    if (status_code == PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        cb::audit::document::add(cookie, cb::audit::document::Operation::Read);
        if (context.in_document_state == DocumentState::Deleted) {
            status_code = PROTOCOL_BINARY_RESPONSE_SUBDOC_SUCCESS_DELETED;
        }
    }

    // Lookups to a deleted document which (partially) succeeded need
    // to be mapped MULTI_PATH_FAILURE_DELETED, so the client knows the found
    // document was in Deleted state.
    if (status_code == PROTOCOL_BINARY_RESPONSE_SUBDOC_MULTI_PATH_FAILURE &&
            (context.in_document_state == DocumentState::Deleted) &&
            !context.traits.is_mutator) {
        status_code = PROTOCOL_BINARY_RESPONSE_SUBDOC_MULTI_PATH_FAILURE_DELETED;
    }

    mcbp_add_header(cookie,
                    status_code,
                    /*extlen*/ 0, /*keylen*/
                    0,
                    context.response_val_len,
                    PROTOCOL_BINARY_RAW_BYTES);

    // Append the iovecs for each operation result.
    for (auto phase : phases) {
        for (auto& op : context.getOperations(phase)) {
            auto mloc = op.result.matchloc();

            // Header is always included. Result value included if the response for
            // this command has a value (e.g. not for EXISTS).
            char* header = response_buf.getCurrent();
            const size_t header_sz = sizeof(uint16_t) + sizeof(uint32_t);
            *reinterpret_cast<uint16_t*>(header) = htons(op.status);
            uint32_t result_len = 0;
            if (op.traits.responseHasValue()) {
                result_len = htonl(uint32_t(mloc.length));
            }
            *reinterpret_cast<uint32_t*>(header +
                                         sizeof(uint16_t)) = result_len;

            connection.addIov(reinterpret_cast<void*>(header), header_sz);

            if (result_len != 0) {
                connection.addIov(mloc.at, mloc.length);
            }
            response_buf.moveOffset(header_sz);
        }
    }

    connection.setState(McbpStateMachine::State::send_data);
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
    cookie.getConnection().setWriteAndGo(McbpStateMachine::State::closing);
}

void subdoc_get_executor(Cookie& cookie) {
    return subdoc_executor(cookie,
                           get_traits<PROTOCOL_BINARY_CMD_SUBDOC_GET>());
}

void subdoc_exists_executor(Cookie& cookie) {
    return subdoc_executor(cookie,
                           get_traits<PROTOCOL_BINARY_CMD_SUBDOC_EXISTS>());
}

void subdoc_dict_add_executor(Cookie& cookie) {
    return subdoc_executor(cookie,
                           get_traits<PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD>());
}

void subdoc_dict_upsert_executor(Cookie& cookie) {
    return subdoc_executor(
            cookie, get_traits<PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT>());
}

void subdoc_delete_executor(Cookie& cookie) {
    return subdoc_executor(cookie,
                           get_traits<PROTOCOL_BINARY_CMD_SUBDOC_DELETE>());
}

void subdoc_replace_executor(Cookie& cookie) {
    return subdoc_executor(cookie,
                           get_traits<PROTOCOL_BINARY_CMD_SUBDOC_REPLACE>());
}

void subdoc_array_push_last_executor(Cookie& cookie) {
    return subdoc_executor(
            cookie, get_traits<PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST>());
}

void subdoc_array_push_first_executor(Cookie& cookie) {
    return subdoc_executor(
            cookie, get_traits<PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST>());
}

void subdoc_array_insert_executor(Cookie& cookie) {
    return subdoc_executor(
            cookie, get_traits<PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT>());
}

void subdoc_array_add_unique_executor(Cookie& cookie) {
    return subdoc_executor(
            cookie, get_traits<PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE>());
}

void subdoc_counter_executor(Cookie& cookie) {
    return subdoc_executor(cookie,
                           get_traits<PROTOCOL_BINARY_CMD_SUBDOC_COUNTER>());
}

void subdoc_get_count_executor(Cookie& cookie) {
    return subdoc_executor(cookie,
                           get_traits<PROTOCOL_BINARY_CMD_SUBDOC_GET_COUNT>());
}

void subdoc_multi_lookup_executor(Cookie& cookie) {
    return subdoc_executor(
            cookie, get_traits<PROTOCOL_BINARY_CMD_SUBDOC_MULTI_LOOKUP>());
}

void subdoc_multi_mutation_executor(Cookie& cookie) {
    return subdoc_executor(
            cookie, get_traits<PROTOCOL_BINARY_CMD_SUBDOC_MULTI_MUTATION>());
}
