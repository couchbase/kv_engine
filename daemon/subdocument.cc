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

#include <snappy-c.h>

#include "connections.h"
#include "debug_helpers.h"
#include "mcbp.h"
#include "subdoc/util.h"
#include "subdocument_context.h"
#include "subdocument_traits.h"
#include "subdocument_validators.h"
#include "timings.h"
#include "topkeys.h"
#include "utilities/protocol2text.h"

#include <platform/histogram.h>

/******************************************************************************
 * Subdocument executors
 *****************************************************************************/

/*
 * Declarations
 */

static bool subdoc_fetch(McbpConnection* c, SubdocCmdContext& ctx,
                         ENGINE_ERROR_CODE ret, const char* key,
                         size_t keylen, uint16_t vbucket, uint64_t cas);

static bool subdoc_operate(SubdocCmdContext* context);

static ENGINE_ERROR_CODE subdoc_update(SubdocCmdContext* context,
                                       ENGINE_ERROR_CODE ret,
                                       const char* key, size_t keylen,
                                       uint16_t vbucket, uint32_t expiration);
static void subdoc_response(SubdocCmdContext* context);

static protocol_binary_response_status
subdoc_operate_one_path(Connection* c, SubdocCmdContext::OperationSpec& spec,
                        const const_char_buffer& in_doc,
                        uint64_t in_cas);

// Debug - print details of the specified subdocument command.
static void subdoc_print_command(Connection* c, protocol_binary_command cmd,
                                 const char* key, const uint16_t keylen,
                                 const char* path, const uint16_t pathlen,
                                 const char* value, const uint32_t vallen) {
    char clean_key[KEY_MAX_LENGTH + 32];
    char clean_path[SUBDOC_PATH_MAX_LENGTH];
    char clean_value[80]; // only print the first few characters of the value.
    if ((key_to_printable_buffer(clean_key, sizeof(clean_key), c->getId(), true,
                                 memcached_opcode_2_text(cmd), key, keylen)
                    != -1)
                    && (buf_to_printable_buffer(clean_path, sizeof(clean_path),
                                                path, pathlen) != -1)) {
        // print key, path & value if there is a value.
        if ((vallen > 0)
                        && (buf_to_printable_buffer(clean_value,
                                                    sizeof(clean_value), value,
                                                    vallen) != -1)) {
            LOG_DEBUG(c, "%s path:'%s' value:'%s'",
                      clean_key, clean_path, clean_value);
        } else {
            // key & path only
            LOG_DEBUG(c, "%s path:'%s'", clean_key, clean_path);
        }
    }
}

/*
 * Definitions
 */

static SubdocCmdContext*
subdoc_create_context(McbpConnection* c, const SubdocCmdTraits traits,
                      const void* packet, const_char_buffer value) {

    try {
        auto* context = new SubdocCmdContext(c, traits);

        switch (traits.path) {
        case SubdocPath::SINGLE: {
            const protocol_binary_request_subdocument *req =
                    reinterpret_cast<const protocol_binary_request_subdocument*>(packet);

            const protocol_binary_subdoc_flag flags =
                    static_cast<protocol_binary_subdoc_flag>(req->message.extras.subdoc_flags);
            const protocol_binary_command mcbp_cmd =
                    protocol_binary_command(req->message.header.request.opcode);
            const uint16_t pathlen = ntohs(req->message.extras.pathlen);

            // Path is the first thing in the value; remainder is the operation
            // value.
            auto path = value;
            path.len = pathlen;

            // Decode as single path; add a single operation to the context.
            if (traits.request_has_value) {
                // Adjust value to move past the path.
                value.buf += pathlen;
                value.len -= pathlen;

                context->ops.emplace_back
                    (SubdocCmdContext::OperationSpec{traits, flags, path, value});
            } else {
                context->ops.emplace_back
                    (SubdocCmdContext::OperationSpec{traits, flags, path});
            }

            if (flags & SUBDOC_FLAG_MKDOC) {
                context->jroot_type =
                        Subdoc::Util::get_root_type(traits.command, path.buf, path.len);
            }

            if (settings.getVerbose() > 1) {
                const uint8_t extlen = req->message.header.request.extlen;
                const char* key = (char*)packet + sizeof(req->message.header) + extlen;
                const uint16_t keylen = ntohs(req->message.header.request.keylen);

                subdoc_print_command(c, mcbp_cmd, key, keylen,
                                     path.buf, path.len, value.buf, value.len);
            }
            break;
        }

        case SubdocPath::MULTI: {
            // Decode each of lookup specs from the value into our command context.
            size_t offset = 0;

            while (offset < value.len) {
                protocol_binary_command binprot_cmd;
                protocol_binary_subdoc_flag flags;
                size_t headerlen;
                const_char_buffer path;
                const_char_buffer spec_value;
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
                if ((flags & SUBDOC_FLAG_MKDOC) && context->jroot_type == 0) {
                    // Determine the root type
                    context->jroot_type =
                            Subdoc::Util::get_root_type(traits.command, path.buf, path.len);
                }

                context->ops.emplace_back
                    (SubdocCmdContext::OperationSpec{traits, flags, path,
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
                subdoc_print_command(c, mcbp_cmd, key, keylen,
                                     path, strlen(path), value.buf, value.len);
            }
        }}
        return context;
    } catch (std::bad_alloc&) {
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
        if (header->request.extlen == sizeof(uint32_t)) {
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

/* Main function which handles execution of all sub-document
 * commands: fetches, operates on, updates and finally responds to the client.
 *
 * Invoked via extern "C" trampoline functions (see later) which populate the
 * subdocument elements of executors[].
 *
 * @param c      connection object.
 * @param packet request packet.
 * @param traits Traits associated with the specific command.
 */
static void subdoc_executor(McbpConnection *c, const void *packet,
                            const SubdocCmdTraits traits) {

    // 0. Parse the request and log it if debug enabled.
    const protocol_binary_request_subdocument *req =
            reinterpret_cast<const protocol_binary_request_subdocument*>(packet);
    const protocol_binary_request_header* header = &req->message.header;

    const uint8_t extlen = header->request.extlen;
    const uint16_t keylen = ntohs(header->request.keylen);
    const uint32_t bodylen = ntohl(header->request.bodylen);
    const uint16_t vbucket = ntohs(header->request.vbucket);
    const uint64_t cas = ntohll(header->request.cas);

    const char* key = (char*)packet + sizeof(*header) + extlen;

    const char* value = key + keylen;
    const uint32_t vallen = bodylen - keylen - extlen;

    const uint32_t expiration = subdoc_decode_expiration(header, traits);

    // We potentially need to make multiple attempts at this as the engine may
    // return EWOULDBLOCK if not initially resident, hence initialise ret to
    // c->aiostat.
    ENGINE_ERROR_CODE ret = c->getAiostat();
    c->setAiostat(ENGINE_SUCCESS);

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

    int attempts = 0;
    do {
        attempts++;

        // 0. If we don't already have a command context, allocate one
        // (we may already have one if this is an auto_retry or a re-execution
        // due to EWOULDBLOCK).
        auto* context = dynamic_cast<SubdocCmdContext*>(c->getCommandContext());
        if (context == nullptr) {
            const_char_buffer value_buf{value, vallen};
            context = subdoc_create_context(c, traits, packet, value_buf);
            c->setCommandContext(context);
        }

        // 1. Attempt to fetch from the engine the document to operate on. Only
        // continue if it returned true, otherwise return from this function
        // (which may result in it being called again later in the EWOULDBLOCK
        // case).
        if (!subdoc_fetch(c, *context, ret, key, keylen, vbucket, cas)) {
            return;
        }

        // 2. Perform the operation specified by CMD. Again, return if it fails.
        if (!subdoc_operate(context)) {
            return;
        }

        // 3. Update the document in the engine (mutations only).
        ret = subdoc_update(context, ret, key, keylen, vbucket, expiration);
        if (ret == ENGINE_KEY_EEXISTS) {
            if (auto_retry) {
                // Retry the operation. Reset the command context and related
                // state, so start from the beginning again.
                ret = ENGINE_SUCCESS;
                if (c->getItem() != nullptr) {
                    bucket_release_item(c, c->getItem());
                    c->setItem(nullptr);
                }

                c->resetCommandContext();
                continue;
            } else {
                // No auto-retry - return status back to client and return.
                mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ret));
                return;
            }
        } else if (ret != ENGINE_SUCCESS) {
            return;
        }

        // 4. Form a response and send it back to the client.
        subdoc_response(context);

        // Update stats. Treat all mutations as 'cmd_set', all accesses as 'cmd_get',
        // in addition to specific subdoc counters. (This is mainly so we
        // see subdoc commands in the GUI, which used cmd_set / cmd_get).
        auto* thread_stats = get_thread_stats(c);
        if (context->traits.is_mutator) {
            thread_stats->cmd_subdoc_mutation++;
            thread_stats->bytes_subdoc_mutation_total += context->out_doc_len;
            thread_stats->bytes_subdoc_mutation_inserted +=
                    context->getOperationValueBytesTotal();

            SLAB_INCR(c, cmd_set, key, keylen);
        } else {
            thread_stats->cmd_subdoc_lookup++;
            thread_stats->bytes_subdoc_lookup_total += context->in_doc.len;
            thread_stats->bytes_subdoc_lookup_extracted += context->response_val_len;

            STATS_HIT(c, get, key, nkey);
        }
        update_topkeys(key, keylen, c);

        return;
    } while (auto_retry && attempts < MAXIMUM_ATTEMPTS);

    // Hit maximum attempts - this theoretically could happen but shouldn't
    // in reality.
    const protocol_binary_command mcbp_cmd =
            protocol_binary_command(header->request.opcode);

    LOG_WARNING(c,
         "%u: Subdoc: Hit maximum number of auto-retry attempts (%d) when "
         "attempting to perform op %s for client %s - returning TMPFAIL",
         c->getId(), MAXIMUM_ATTEMPTS, memcached_opcode_2_text(mcbp_cmd),
         c->getDescription().c_str());
    mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ENGINE_TMPFAIL));
}

/* Gets a flat, uncompressed JSON document ready for performing a subjson
 * operation on it.
 * @param      c        Connection
 * @param      item     The item referring to the document to retreive.
 * @param[out] document Upon success, the returned document.
 * @param      in_cas   Input CAS to use
 * @param[out] cas      Upon success, the returned document's CAS.
 * @param[out] flags    Upon success, the returned document's flags.
 *
 * Returns true if a buffer could be prepared, updating {document} with the
 * address and size of the document and {cas} with the cas.
 * Otherwise returns an error code indicating why the document could not be
 * obtained.
 */
static protocol_binary_response_status
get_document_for_searching(McbpConnection * c, const item* item,
                           const_char_buffer& document, uint64_t in_cas,
                           uint64_t& cas, uint32_t& flags) {

    item_info_holder info;
    info.info.nvalue = IOV_MAX;

    if (!bucket_get_item_info(c, item, &info.info)) {
        LOG_WARNING(c, "%u: Failed to get item info", c->getId());
        return PROTOCOL_BINARY_RESPONSE_EINTERNAL;
    }

    // Need to have the complete document in a single iovec.
    if (info.info.nvalue != 1) {
        LOG_WARNING(c, "%u: More than one iovec in document", c->getId());
        return PROTOCOL_BINARY_RESPONSE_EINTERNAL;
    }

    // Check CAS matches (if specified by the user)
    if ((in_cas != 0) && in_cas != info.info.cas) {
        return PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS;
    }

    // Set CAS - same irrespective of datatype.
    cas = info.info.cas;
    flags = info.info.flags;

    if (!mcbp::datatype::is_json(info.info.datatype)) {
        // No good; need to have JSON
        return PROTOCOL_BINARY_RESPONSE_SUBDOC_DOC_NOTJSON;
    }

    if (mcbp::datatype::is_compressed(info.info.datatype)) {
        // Need to expand before attempting to extract from it.
        const char* compressed_buf =
            static_cast<const char*>(info.info.value[0].iov_base);
        const size_t compressed_len = info.info.value[0].iov_len;
        size_t uncompressed_len;

        if (snappy_uncompressed_length(compressed_buf, compressed_len,
                                       &uncompressed_len) != SNAPPY_OK) {
            char clean_key[KEY_MAX_LENGTH + 32];
            if (buf_to_printable_buffer(clean_key, sizeof(clean_key),
                                        static_cast<const char*>(info.info.key),
                                        info.info.nkey) != -1) {
                LOG_WARNING(c,
                            "<%u ERROR: Failed to determine inflated body"
                                " size. Key: '%s' may have an "
                                "incorrect datatype of COMPRESSED_JSON.",
                            c->getId(), clean_key);
            }
            return PROTOCOL_BINARY_RESPONSE_EINTERNAL;
        }

        // We use the connections' dynamic buffer to uncompress into; this
        // will later be used as the the send buffer for the subset of the
        // document we send.
        if (!c->growDynamicBuffer(uncompressed_len)) {
            LOG_WARNING(c,
                        "<%u ERROR: Failed to grow dynamic buffer to %"
                            PRIu64 "for uncompressing document.",
                        c->getId(), uncompressed_len);
            return PROTOCOL_BINARY_RESPONSE_E2BIG;
        }

        auto &dbuf = c->getDynamicBuffer();
        char* buffer = dbuf.getCurrent();
        if (snappy_uncompress(compressed_buf, compressed_len, buffer,
                              &uncompressed_len) != SNAPPY_OK) {
            char clean_key[KEY_MAX_LENGTH + 32];
            if (buf_to_printable_buffer(clean_key, sizeof(clean_key),
                                        static_cast<const char*>(info.info.key),
                                        info.info.nkey) != -1) {
                LOG_WARNING(c,
                            "<%u ERROR: Failed to inflate body. Key: '%s'"
                                " may have an incorrect datatype of "
                                "COMPRESSED_JSON.", c->getId(), clean_key);
            }
            return PROTOCOL_BINARY_RESPONSE_EINTERNAL;
        }
        dbuf.moveOffset(uncompressed_len);
        // Update document to point to the uncompressed version in the buffer.
        document.buf = buffer;
        document.len = uncompressed_len;
        return PROTOCOL_BINARY_RESPONSE_SUCCESS;
    } else {
        // Good to go using original buffer.
        document.buf = static_cast<char*>(info.info.value[0].iov_base);
        document.len = info.info.value[0].iov_len;
        return PROTOCOL_BINARY_RESPONSE_SUCCESS;
    }
}

// Fetch the item to operate on from the engine.
// Returns true if the command was successful (and execution should continue),
// else false.
static bool subdoc_fetch(McbpConnection* c, SubdocCmdContext& ctx,
                         ENGINE_ERROR_CODE ret, const char* key,
                         size_t keylen, uint16_t vbucket, uint64_t cas) {

    if (c->getItem() == NULL && !ctx.needs_new_doc) {
        item* initial_item;

        if (ret == ENGINE_SUCCESS) {
            ret = bucket_get(c, &initial_item, key, (int)keylen, vbucket);
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            // We have the item; assign to c->item (so we'll start from step 2
            // next time).
            c->setItem(initial_item);
            ctx.needs_new_doc = false;
            break;

        case ENGINE_KEY_ENOENT:
            // The item does not exist. Check the current command context to
            // determine if we should at all write a new document (i.e. pretend
            // it exists) and defer insert until later.. OR if we should simply
            // bail.

            if (ctx.jroot_type == JSONSL_T_LIST) {
                ctx.in_doc = {"[]", 2};
            } else if (ctx.jroot_type == JSONSL_T_OBJECT) {
                ctx.in_doc = {"{}", 2};
            } else {
                mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ret));
                return false;
            }

            // Indicate that a new document is required:
            ctx.needs_new_doc = true;
            return true;

        case ENGINE_EWOULDBLOCK:
            c->setEwouldblock(true);
            return false;

        case ENGINE_DISCONNECT:
            c->setState(conn_closing);
            return false;

        default:
            mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ret));
            return false;
        }
    }

    if (ctx.in_doc.buf == nullptr) {
        // Retrieve the item_info the engine, and if necessary
        // uncompress it so subjson can parse it.
        uint64_t doc_cas;
        uint32_t doc_flags;
        const_char_buffer doc;
        protocol_binary_response_status status;
        status = get_document_for_searching(c, c->getItem(), doc, cas,
                                            doc_cas, doc_flags);

        if (status != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
            // Failed. Note c->item and c->commandContext will both be freed for
            // us as part of preparing for the next command.
            mcbp_write_packet(c, status);
            return false;
        }

        // Record the input document in the context.
        ctx.in_doc = doc;
        ctx.in_cas = doc_cas;
        ctx.in_flags = doc_flags;
    }

    return true;
}

/** Perform the operation specified by {spec} to one path in the document.
 */
static protocol_binary_response_status
subdoc_operate_one_path(Connection* c, SubdocCmdContext::OperationSpec& spec,
                      const const_char_buffer& in_doc, uint64_t in_cas) {

    // Prepare the specified sub-document command.
    Subdoc::Operation* op = c->getThread()->subdoc_op;
    op->clear();
    op->set_result_buf(&spec.result);
    op->set_code(spec.traits.command);
    op->set_doc(in_doc.buf, in_doc.len);
    op->set_value(spec.value.buf, spec.value.len);

    // ... and execute it.
    Subdoc::Error subdoc_res = op->op_exec(spec.path.buf, spec.path.len);

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
        LOG_DEBUG(c, "Unexpected response from subdoc: %d (0x%x)",
                  subdoc_res, subdoc_res);
        return PROTOCOL_BINARY_RESPONSE_EINTERNAL;
    }
}

// Operate on the document as specified by the command context.
// Returns true if the command was successful (and execution should continue),
// else false.
static bool subdoc_operate(SubdocCmdContext* context) {

    if (!context->executed) {
        GenericBlockTimer<TimingHistogram, 0> bt(
                &all_buckets[context->c->getBucketIndex()].subjson_operation_times);

        context->overall_status = PROTOCOL_BINARY_RESPONSE_SUCCESS;

        // 2. Perform each of the operations on document.
        for (auto op = context->ops.begin(); op != context->ops.end(); op++) {
            op->status = subdoc_operate_one_path(context->c, *op, context->in_doc,
                                                 context->in_cas);

            switch (context->traits.path) {
            case SubdocPath::SINGLE:
                if (op->status != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
                    // Failure of a (the only) op stops execution and returns an
                    // error to the client.
                    mcbp_write_packet(context->c, op->status);
                    return false;
                }
                break;

            case SubdocPath::MULTI:
                if (op->status == PROTOCOL_BINARY_RESPONSE_SUCCESS) {
                    if (context->traits.is_mutator) {
                        // Unless this is the last operation, we need to make
                        // the result document be input for the next operation.
                        auto last_op = std::prev(context->ops.end());
                        if (op == last_op) {
                            continue;
                        }

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
                        std::unique_ptr<char[]> intermediate_doc;
                        try {
                            intermediate_doc.reset(new char[new_doc_len]);
                        } catch (const std::bad_alloc&) {
                            // Insufficient memory - unable to continue.
                            mcbp_write_packet(context->c,
                                              PROTOCOL_BINARY_RESPONSE_ENOMEM);
                            return false;
                        }

                        size_t offset = 0;
                        for (auto& loc : op->result.newdoc()) {
                            std::memcpy(intermediate_doc.get() + offset,
                                        loc.at, loc.length);
                            offset += loc.length;
                        }

                        // Copying complete - safe to delete the old temp_doc
                        // (even if it was the source of some of the newdoc
                        // iovecs).
                        context->temp_doc.swap(intermediate_doc);
                        context->in_doc = {context->temp_doc.get(),
                                           new_doc_len};

                    } else { // lookup
                        // nothing to do.
                    }
                } else {
                    context->overall_status
                        = PROTOCOL_BINARY_RESPONSE_SUBDOC_MULTI_PATH_FAILURE;
                    if (context->traits.is_mutator) {
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
                }
                break;
            }
        }

        context->executed = true;
    }

    return true;
}

// Update the engine with whatever modifications the subdocument command made
// to the document.
// Returns true if the update was successful (and execution should continue),
// else false.
ENGINE_ERROR_CODE subdoc_update(SubdocCmdContext* context,
                                ENGINE_ERROR_CODE ret, const char* key,
                                size_t keylen, uint16_t vbucket,
                                uint32_t expiration) {
    auto* const c = context->c;
    auto* handle = reinterpret_cast<ENGINE_HANDLE*>(c->getBucketEngine());

    if (!context->traits.is_mutator) {
        // No update required - just make sure we have the correct cas to use
        // for response.
        c->setCAS(context->in_cas);
        return ENGINE_SUCCESS;
    }

    // For multi-mutations, we only want to actually update the engine if /all/
    // paths succeeded - otherwise the document is unchanged (and we continue
    // to subdoc_response() to send information back to the client on what
    // succeeded/failed.
    if (context->overall_status != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        return ENGINE_SUCCESS;
    }

    // Calculate the updated document length - use the last operation result.
    context->out_doc_len = 0;
    for (auto& loc : context->ops.back().result.newdoc()) {
        context->out_doc_len += loc.length;
    }

    // Allocate a new item of this size.
    if (context->out_doc == NULL) {
        item *new_doc;

        if (ret == ENGINE_SUCCESS) {
            ret = c->getBucketEngine()->allocate(handle, c->getCookie(),
                                                 &new_doc, key, keylen,
                                                 context->out_doc_len,
                                                 context->in_flags,
                                                 expiration,
                                                 PROTOCOL_BINARY_DATATYPE_JSON);
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            // Save the allocated document in the cmd context.
            context->out_doc = new_doc;
            break;

        case ENGINE_EWOULDBLOCK:
            c->setEwouldblock(true);
            return ret;

        case ENGINE_DISCONNECT:
            c->setState(conn_closing);
            return ret;

        default:
            mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ret));
            return ret;
        }

        // To ensure we only replace the version of the document we
        // just appended to; set the CAS to the one retrieved from.
        bucket_item_set_cas(c, new_doc, context->in_cas);

        // Obtain the item info (and it's iovectors)
        item_info new_doc_info;
        new_doc_info.nvalue = IOV_MAX;
        if (!bucket_get_item_info(c, new_doc, &new_doc_info)) {
            mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL);
            return ENGINE_FAILED;
        }

        // Copy the new document into the item.
        char* write_ptr = static_cast<char*>(new_doc_info.value[0].iov_base);
        for (auto& loc : context->ops.back().result.newdoc()) {
            std::copy(loc.at, loc.at + loc.length, write_ptr);
            write_ptr += loc.length;
        }
    }

    // And finally, store the new document.
    uint64_t new_cas;
    auto new_op = context->needs_new_doc ? OPERATION_ADD : OPERATION_REPLACE;
    ret = bucket_store(c, context->out_doc, &new_cas, new_op, vbucket);
    switch (ret) {
    case ENGINE_SUCCESS:
        // Record the UUID / Seqno if MUTATION_SEQNO feature is enabled so
        // we can include it in the response.
        if (c->isSupportsMutationExtras()) {
            item_info info;
            info.nvalue = 1;
            if (!bucket_get_item_info(c, context->out_doc, &info)) {
                LOG_WARNING(c, "%u: Subdoc: Failed to get item info",
                            c->getId());
                mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL);
                return ENGINE_FAILED;
            }
            context->vbucket_uuid = info.vbucket_uuid;
            context->sequence_no = info.seqno;
        }

        c->setCAS(new_cas);
        break;

    case ENGINE_KEY_EEXISTS:
        // CAS mismatch. Caller may choose to retry this (without necessarily
        // telling the client), so send so response here...
        break;

    case ENGINE_EWOULDBLOCK:
        c->setEwouldblock(true);
        break;

    case ENGINE_DISCONNECT:
        c->setState(conn_closing);
        break;

    default:
        mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ret));
        break;
    }

    return ret;
}

/* Encodes the context's mutation sequence number and vBucket UUID into the
 * given buffer.
 * @param descr Buffer to write to. Must be 16 bytes in size.
 */
static void encode_mutation_descr(SubdocCmdContext* context, char* buffer)
{
    mutation_descr_t descr;
    descr.seqno = htonll(context->sequence_no);
    descr.vbucket_uuid = htonll(context->vbucket_uuid);
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
static void subdoc_single_response(SubdocCmdContext* context) {
    auto* const c = context->c;

    protocol_binary_response_subdocument* rsp =
            reinterpret_cast<protocol_binary_response_subdocument*>(c->write.buf);

    // Calculate extras size
    const bool include_mutation_dscr = (c->isSupportsMutationExtras() &&
                                        context->traits.is_mutator);
    const size_t extlen = include_mutation_dscr ? sizeof(mutation_descr_t)
                                                : 0;

    const char* value = NULL;
    context->response_val_len = 0;
    if (context->traits.response_has_value) {
        auto mloc = context->ops[0].result.matchloc();
        value = mloc.at;
        context->response_val_len = mloc.length;
    }

    if (mcbp_add_header(c, PROTOCOL_BINARY_RESPONSE_SUCCESS, extlen,
        /*keylen*/0, extlen + context->response_val_len,
                        PROTOCOL_BINARY_RAW_BYTES) == -1) {
        c->setState(conn_closing);
        return;
    }
    rsp->message.header.response.cas = htonll(c->getCAS());

    // Add mutation descr to response buffer if requested.
    if (include_mutation_dscr) {
        DynamicBuffer& response_buf = c->getDynamicBuffer();
        if (!response_buf.grow(extlen)) {
            // Unable to form complete response.
            mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM);
            return;
        }
        char* const extras_ptr = response_buf.getCurrent();
        encode_mutation_descr(context, extras_ptr);
        response_buf.moveOffset(extlen);

        c->addIov(extras_ptr, extlen);
    }

    if (context->traits.response_has_value) {
        c->addIov(value, context->response_val_len);
    }

    c->setState(conn_mwrite);
}

/* Construct and send a response to a multi-path mutation back to the client.
 */
static void subdoc_multi_mutation_response(SubdocCmdContext* context) {
    auto* const c = context->c;
    protocol_binary_response_subdocument* rsp =
            reinterpret_cast<protocol_binary_response_subdocument*>(c->write.buf);

    // MULTI_MUTATION: On success, zero to N multi_mutation_result_spec objects
    // (one for each spec which wants to return a value), with optional 16byte
    // mutation descriptor in extras if MUTATION_SEQNO is enabled.
    //
    // On failure body indicates the index and status code of the first failing
    // spec.
    DynamicBuffer& response_buf = c->getDynamicBuffer();
    size_t extlen = 0;
    char* extras_ptr = nullptr;

    // Encode mutation extras into buffer if success & they were requested.
    if (context->overall_status == PROTOCOL_BINARY_RESPONSE_SUCCESS &&
            c->isSupportsMutationExtras()) {
        extlen = sizeof(mutation_descr_t);
        if (!response_buf.grow(extlen)) {
            // Unable to form complete response.
            mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM);
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
    if (context->overall_status == PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        // on success, one per each non-zero length result.
        response_buf_needed = 0;
        for (size_t ii = 0; ii < context->ops.size(); ii++) {
            const auto& op = context->ops[ii];
            const auto mloc = op.result.matchloc();
            if (op.traits.response_has_value && mloc.length > 0) {
                response_buf_needed += sizeof(uint8_t) + sizeof(uint16_t) +
                                       sizeof(uint32_t);
                iov_len += mloc.length;
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
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM);
        return;
    }

    // Allocated required resource - build the header.
    if (mcbp_add_header(c, context->overall_status, extlen, /*keylen*/0,
                        extlen + response_buf_needed + iov_len,
                        PROTOCOL_BINARY_RAW_BYTES)
            == -1) {
        c->setState(conn_closing);
        return;
    }
    rsp->message.header.response.cas = htonll(c->getCAS());

    // Append extras if requested.
    if (extlen > 0) {
        c->addIov(reinterpret_cast<void*>(extras_ptr), extlen);
    }

    // Append the iovecs for each operation result.
    for (size_t ii = 0; ii < context->ops.size(); ii++) {
        const auto& op = context->ops[ii];
        // Successful - encode all non-zero length results.
        if (context->overall_status == PROTOCOL_BINARY_RESPONSE_SUCCESS) {
            const auto mloc = op.result.matchloc();
            if (op.traits.response_has_value && mloc.length > 0) {
                char* header = response_buf.getCurrent();
                size_t header_sz =
                        encode_multi_mutation_result_spec(ii, op, header);

                c->addIov(reinterpret_cast<void*>(header), header_sz);
                c->addIov(mloc.at, mloc.length);

                response_buf.moveOffset(header_sz);
            }
        } else {
            // Failure - encode first unsuccessful path index and status.
            if (op.status != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
                char* header = response_buf.getCurrent();
                size_t header_sz =
                        encode_multi_mutation_result_spec(ii, op, header);

                c->addIov(reinterpret_cast<void*>(header), header_sz);
                response_buf.moveOffset(header_sz);

                // Only the first unsuccessful op is reported.
                break;
            }
        }
    }
    c->setState(conn_mwrite);
}

/* Construct and send a response to a multi-path lookup back to the client.
 */
static void subdoc_multi_lookup_response(SubdocCmdContext* context) {
    auto* const c = context->c;
    protocol_binary_response_subdocument* rsp =
            reinterpret_cast<protocol_binary_response_subdocument*>(c->write.buf);

    // Calculate the value length - sum of all the operation results.
    context->response_val_len = 0;
    for (auto& op : context->ops) {
        // 16bit status, 32bit resultlen, variable-length result.
        size_t result_size = sizeof(uint16_t) + sizeof(uint32_t);
        if (op.traits.response_has_value) {
            result_size += op.result.matchloc().length;
        }
        context->response_val_len += result_size;
    }

    // We need two iovecs per operation result:
    // 1. status (uin16_t) & vallen (uint32_t). Use the dynamicBuffer for this
    // 2. actual value - this already resides either in the original document
    //                   (for lookups) or stored in the Subdoc::Result.
    DynamicBuffer& response_buf = c->getDynamicBuffer();
    size_t needed = (sizeof(uint16_t) + sizeof(uint32_t)) * context->ops.size();
    if (!response_buf.grow(needed)) {
        // Unable to form complete response.
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM);
        return;
    }

    // Allocated required resource - build the header.
    if (mcbp_add_header(c, context->overall_status, /*extlen*/0, /*keylen*/
                        0, context->response_val_len, PROTOCOL_BINARY_RAW_BYTES)
        == -1) {
        c->setState(conn_closing);
        return;
    }
    rsp->message.header.response.cas = htonll(c->getCAS());

    // Append the iovecs for each operation result.
    for (auto& op : context->ops) {
        auto mloc = op.result.matchloc();

        // Header is always included. Result value included if the response for
        // this command has a value (e.g. not for EXISTS).
        char* header = response_buf.getCurrent();
        const size_t header_sz = sizeof(uint16_t) + sizeof(uint32_t);
        *reinterpret_cast<uint16_t*>(header) = htons(op.status);
        uint32_t result_len = 0;
        if (op.traits.response_has_value) {
            result_len = htonl(uint32_t(mloc.length));
        }
        *reinterpret_cast<uint32_t*>(header + sizeof(uint16_t)) = result_len;

        c->addIov(reinterpret_cast<void*>(header), header_sz);

        if (result_len != 0) {
            c->addIov(mloc.at, mloc.length);
        }
        response_buf.moveOffset(header_sz);
    }

    c->setState(conn_mwrite);
}

// Respond back to the user as appropriate to the specific command.
void subdoc_response(SubdocCmdContext* context) {
    switch (context->traits.path) {
    case SubdocPath::SINGLE:
        subdoc_single_response(context);
        return;

    case SubdocPath::MULTI:
        if (context->traits.is_mutator) {
            subdoc_multi_mutation_response(context);
        } else {
            subdoc_multi_lookup_response(context);
        }
        return;
    }

    // Shouldn't get here - invalid traits.path
    auto* const c = context->c;
    mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL);
    c->setState(conn_closing);
}

void subdoc_get_executor(McbpConnection* c, void* packet) {
    return subdoc_executor(c, packet,
                           get_traits<PROTOCOL_BINARY_CMD_SUBDOC_GET>());
}

void subdoc_exists_executor(McbpConnection* c, void* packet) {
    return subdoc_executor(c, packet,
                           get_traits<PROTOCOL_BINARY_CMD_SUBDOC_EXISTS>());
}

void subdoc_dict_add_executor(McbpConnection* c, void *packet) {
    return subdoc_executor(c, packet,
                           get_traits<PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD>());
}

void subdoc_dict_upsert_executor(McbpConnection* c, void *packet) {
    return subdoc_executor(c, packet,
                           get_traits<PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT>());
}

void subdoc_delete_executor(McbpConnection* c, void *packet) {
    return subdoc_executor(c, packet,
                           get_traits<PROTOCOL_BINARY_CMD_SUBDOC_DELETE>());
}

void subdoc_replace_executor(McbpConnection* c, void *packet) {
    return subdoc_executor(c, packet,
                           get_traits<PROTOCOL_BINARY_CMD_SUBDOC_REPLACE>());
}

void subdoc_array_push_last_executor(McbpConnection* c, void *packet) {
    return subdoc_executor(c, packet,
                           get_traits<PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST>());
}

void subdoc_array_push_first_executor(McbpConnection* c, void *packet) {
    return subdoc_executor(c, packet,
                           get_traits<PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST>());
}

void subdoc_array_insert_executor(McbpConnection* c, void *packet) {
    return subdoc_executor(c, packet,
                           get_traits<PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT>());
}

void subdoc_array_add_unique_executor(McbpConnection* c, void *packet) {
    return subdoc_executor(c, packet,
                           get_traits<PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE>());
}

void subdoc_counter_executor(McbpConnection* c, void *packet) {
    return subdoc_executor(c, packet,
                           get_traits<PROTOCOL_BINARY_CMD_SUBDOC_COUNTER>());
}

void subdoc_get_count_executor(McbpConnection *c, void *packet) {
    return subdoc_executor(c, packet,
                           get_traits<PROTOCOL_BINARY_CMD_SUBDOC_GET_COUNT>());
}

void subdoc_multi_lookup_executor(McbpConnection* c, void *packet) {
    return subdoc_executor(c, packet,
                           get_traits<PROTOCOL_BINARY_CMD_SUBDOC_MULTI_LOOKUP>());
}

void subdoc_multi_mutation_executor(McbpConnection* c, void *packet) {
    return subdoc_executor(c, packet,
                           get_traits<PROTOCOL_BINARY_CMD_SUBDOC_MULTI_MUTATION>());
}
