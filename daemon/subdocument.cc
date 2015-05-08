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
#include <subdoc/operations.h>

#include "connections.h"
#include "debug_helpers.h"
#include "timings.h"
#include "utilities/protocol2text.h"

struct sized_buffer {
    char* buf;
    size_t len;
};

/* Convert integers to types, to allow type traits for different
 * protocol_binary_commands, etc.
 */
template <protocol_binary_command C>
struct Cmd2Type
{
  enum { value = C };
};

/* Traits of each of the sub-document commands. These are used to build up
 * the individual implementations using generic building blocks:
 *
 *   optype: The subjson API optype for this command.
 *   request_has_value: Does the command request require a value?
 *   allow_empty_path: Is the path field allowed to be empty (zero-length)?
 *   response_has_value: Does the command response require a value?
 *   is_mutator: Does the command mutate (modify) the document?
 *   valid_flags: What flags are valid for this command.
 */
template <typename T>
struct cmd_traits;

template <>
struct cmd_traits<Cmd2Type<PROTOCOL_BINARY_CMD_SUBDOC_GET> > {
  static const subdoc_OPTYPE optype = SUBDOC_CMD_GET;
  static const bool request_has_value = false;
  static const bool allow_empty_path = false;
  static const bool response_has_value = true;
  static const bool is_mutator = false;
  static const protocol_binary_subdoc_flag valid_flags =
      protocol_binary_subdoc_flag(0);
};

template <>
struct cmd_traits<Cmd2Type<PROTOCOL_BINARY_CMD_SUBDOC_EXISTS> > {
  static const subdoc_OPTYPE optype = SUBDOC_CMD_EXISTS;
  static const bool request_has_value = false;
  static const bool allow_empty_path = false;
  static const bool response_has_value = false;
  static const bool is_mutator = false;
  static const protocol_binary_subdoc_flag valid_flags =
      protocol_binary_subdoc_flag(0);
};

template <>
struct cmd_traits<Cmd2Type<PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD> > {
  static const subdoc_OPTYPE optype = SUBDOC_CMD_DICT_ADD;
  static const bool request_has_value = true;
  static const bool allow_empty_path = false;
  static const bool response_has_value = false;
  static const bool is_mutator = true;
  static const protocol_binary_subdoc_flag valid_flags = SUBDOC_FLAG_MKDIR_P;
};

template <>
struct cmd_traits<Cmd2Type<PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT> > {
  static const subdoc_OPTYPE optype = SUBDOC_CMD_DICT_UPSERT;
  static const bool request_has_value = true;
  static const bool allow_empty_path = false;
  static const bool response_has_value = false;
  static const bool is_mutator = true;
  static const protocol_binary_subdoc_flag valid_flags = SUBDOC_FLAG_MKDIR_P;
};

template <>
struct cmd_traits<Cmd2Type<PROTOCOL_BINARY_CMD_SUBDOC_DELETE> > {
  static const subdoc_OPTYPE optype = SUBDOC_CMD_DELETE;
  static const bool request_has_value = false;
  static const bool allow_empty_path = false;
  static const bool response_has_value = false;
  static const bool is_mutator = true;
  static const protocol_binary_subdoc_flag valid_flags =
          protocol_binary_subdoc_flag(0);
};

template <>
struct cmd_traits<Cmd2Type<PROTOCOL_BINARY_CMD_SUBDOC_REPLACE> > {
  static const subdoc_OPTYPE optype = SUBDOC_CMD_REPLACE;
  static const bool request_has_value = true;
  static const bool allow_empty_path = false;
  static const bool response_has_value = false;
  static const bool is_mutator = true;
  static const protocol_binary_subdoc_flag valid_flags =
          protocol_binary_subdoc_flag(0);
};

template <>
struct cmd_traits<Cmd2Type<PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST> > {
  static const subdoc_OPTYPE optype = SUBDOC_CMD_ARRAY_APPEND;
  static const bool request_has_value = true;
  static const bool allow_empty_path = true;
  static const bool response_has_value = false;
  static const bool is_mutator = true;
  static const protocol_binary_subdoc_flag valid_flags = SUBDOC_FLAG_MKDIR_P;
};

template <>
struct cmd_traits<Cmd2Type<PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST> > {
  static const subdoc_OPTYPE optype = SUBDOC_CMD_ARRAY_PREPEND;
  static const bool request_has_value = true;
  static const bool allow_empty_path = true;
  static const bool response_has_value = false;
  static const bool is_mutator = true;
  static const protocol_binary_subdoc_flag valid_flags = SUBDOC_FLAG_MKDIR_P;
};

/*
 * Subdocument command validators
 */

template<protocol_binary_command CMD>
static int subdoc_validator(void* packet) {
    const protocol_binary_request_subdocument *req =
            reinterpret_cast<protocol_binary_request_subdocument*>(packet);
    const protocol_binary_request_header* header = &req->message.header;
    // Extract the various fields from the header.
    const uint16_t keylen = ntohs(header->request.keylen);
    const uint8_t extlen = header->request.extlen;
    const uint32_t bodylen = ntohl(header->request.bodylen);
    const protocol_binary_subdoc_flag subdoc_flags =
            static_cast<protocol_binary_subdoc_flag>(req->message.extras.subdoc_flags);
    const uint16_t pathlen = ntohs(req->message.extras.pathlen);
    const uint32_t valuelen = bodylen - keylen - extlen - pathlen;

    if ((header->request.magic != PROTOCOL_BINARY_REQ) ||
        (keylen == 0) ||
        (extlen != sizeof(uint16_t) + sizeof(uint8_t)) ||
        (pathlen > SUBDOC_PATH_MAX_LENGTH) ||
        (header->request.datatype != PROTOCOL_BINARY_RAW_BYTES)) {
        return -1;
    }

    // Now command-trait specific stuff:

    // valuelen should be non-zero iff the request has a value.
    if (cmd_traits<Cmd2Type<CMD> >::request_has_value) {
        if (valuelen == 0) {
            return -1;
        }
    } else {
        if (valuelen != 0) {
            return -1;
        }
    }

    // Check only valid flags are specified.
    if ((subdoc_flags & ~cmd_traits<Cmd2Type<CMD> >::valid_flags) != 0) {
        return -1;
    }

    if (!cmd_traits<Cmd2Type<CMD> >::allow_empty_path &&
        (pathlen == 0)) {
        return -1;
    }

    return 0;
}

int subdoc_get_validator(void* packet) {
    return subdoc_validator<PROTOCOL_BINARY_CMD_SUBDOC_GET>(packet);
}

int subdoc_exists_validator(void* packet) {
    return subdoc_validator<PROTOCOL_BINARY_CMD_SUBDOC_EXISTS>(packet);
}

int subdoc_dict_add_validator(void* packet) {
    return subdoc_validator<PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD>(packet);
}

int subdoc_dict_upsert_validator(void* packet) {
    return subdoc_validator<PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT>(packet);
}

int subdoc_delete_validator(void* packet) {
    return subdoc_validator<PROTOCOL_BINARY_CMD_SUBDOC_DELETE>(packet);
}

int subdoc_replace_validator(void* packet) {
    return subdoc_validator<PROTOCOL_BINARY_CMD_SUBDOC_REPLACE>(packet);
}

int subdoc_array_push_last_validator(void* packet) {
    return subdoc_validator<PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST>(packet);
}

int subdoc_array_push_first_validator(void* packet) {
    return subdoc_validator<PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST>(packet);
}

/******************************************************************************
 * Subdocument executors
 *****************************************************************************/

/*
 * Types
 */

/** Subdoc command context. An instance of this exists for the lifetime of
 *  each subdocument command, and it used to hold information which needs to
 *  persist across calls to subdoc_executor; for example when one or more
 *  engine functions return EWOULDBLOCK and hence the executor needs to be
 *  retried.
 */
struct SubdocCmdContext {

    SubdocCmdContext(void* c)
      : cookie(c),
        in_doc({NULL, 0}),
        in_cas(0),
        doc_new_len(0),
        out_doc(NULL) {}

    ~SubdocCmdContext() {
        if (out_doc != NULL) {
            settings.engine.v1->release(settings.engine.v0, cookie, out_doc);
        }
    }

    // Static method passed back to memcached to destroy objects of this class.
    static void dtor(conn* c, void* context);

    // Cookie this command is associated with. Needed for the destructor
    // to release items.
    void* cookie;

    // The expanded input JSON document. This may either refer to the raw engine
    // item iovec; or to the connections' dynamic_buffer if the JSON document
    // had to be decompressed. Either way, it should /not/ be free()d.
    sized_buffer in_doc;

    // CAS value of the input document. Required to ensure we only store a
    // new document which was derived from the same original input document.
    uint64_t in_cas;

    // Location of the fragments consisting of the _new_ value.
    subdoc_LOC doc_new[8];
    // Number of fragments active.
    size_t doc_new_len;

    // [Mutations only] New item to store into engine. _Must_ be released
    // back to the engine using ENGINE_HANDLE_V1::release()
    item* out_doc;
};

/*
 * Declarations
 */

static bool subdoc_fetch(conn* c, ENGINE_ERROR_CODE ret, const char* key,
                         size_t keylen, uint16_t vbucket);
template<protocol_binary_command CMD>
static bool subdoc_operate(conn* c, const char* path, size_t pathlen,
                           const char* value, size_t vallen,
                           protocol_binary_subdoc_flag flags, uint64_t in_cas);
template<protocol_binary_command CMD>
static bool subdoc_update(conn* c, ENGINE_ERROR_CODE ret, const char* key,
                          size_t keylen, uint16_t vbucket);
template<protocol_binary_command CMD>
static void subdoc_response(conn* c);

/*
 * Definitions
 */

/* Main template function which handles execution of all sub-document
 * commands: fetches, operates on, updates and finally responds to the client.
 *
 * Invoked via extern "C" trampoline functions (see later) which populate the
 * subdocument elements of executors[].
 *
 * @param CMD sub-document command the function is templated on.
 * @param c connection object.
 * @param packet request packet.
 */
template<protocol_binary_command CMD>
void subdoc_executor(conn *c, const void *packet) {

    // 0. Parse the request and log it if debug enabled.
    const protocol_binary_request_subdocument *req =
            reinterpret_cast<const protocol_binary_request_subdocument*>(packet);
    const protocol_binary_request_header* header = &req->message.header;

    const uint8_t extlen = header->request.extlen;
    const uint16_t keylen = ntohs(header->request.keylen);
    const uint32_t bodylen = ntohl(header->request.bodylen);
    const uint16_t pathlen = ntohs(req->message.extras.pathlen);
    const protocol_binary_subdoc_flag flags =
            static_cast<protocol_binary_subdoc_flag>(req->message.extras.subdoc_flags);
    const uint16_t vbucket = ntohs(header->request.vbucket);
    const uint64_t cas = ntohll(header->request.cas);

    const char* key = (char*)packet + sizeof(*header) + extlen;
    const char* path = key + keylen;

    const char* value = path + pathlen;
    const uint32_t vallen = bodylen - keylen - extlen - pathlen;

    if (settings.verbose > 1) {
        char clean_key[KEY_MAX_LENGTH + 32];
        char clean_path[SUBDOC_PATH_MAX_LENGTH];
        if ((key_to_printable_buffer(clean_key, sizeof(clean_key), c->sfd, true,
                                     memcached_opcode_2_text(CMD),
                                     key, keylen) != -1) &&
            (buf_to_printable_buffer(clean_path, sizeof(clean_path),
                                     path, pathlen) != -1)) {
            settings.extensions.logger->log(EXTENSION_LOG_DEBUG, c,
                                            "%s path:%s\n", clean_key,
                                            clean_path);
        }
    }

    // We potentially need to make multiple attempts at this as the engine may
    // return EWOULDBLOCK if not initially resident, hence initialise ret to
    // c->aiostat.
    ENGINE_ERROR_CODE ret = c->aiostat;
    c->aiostat = ENGINE_SUCCESS;

    // 1. Attempt to fetch from the engine the document to operate on. Only
    // continue if it returned true, otherwise return from this function (which
    // may result in it being called again later in the EWOULDBLOCK case).
    if (!subdoc_fetch(c, ret, key, keylen, vbucket)) {
        return;
    }

    // 2. Perform the operation specified by CMD. Again, return if it fails.
    if (!subdoc_operate<CMD>(c, path, pathlen, value, vallen, flags, cas)) {
        return;
    }

    // 3. Update the document in the engine (mutations only).
    if (!subdoc_update<CMD>(c, ret, key, keylen, vbucket)) {
        return;
    }

    // 4. Form a response and send it back to the client.
    subdoc_response<CMD>(c);
}

/* Gets a flat, uncompressed JSON document ready for performing a subjson
 * operation on it.
 * Returns true if a buffer could be prepared, updating {buf} with the address
 * and size of the document and {cas} with the cas. Otherwise returns an
 * error code indicating why the document could not be obtained.
 */
static protocol_binary_response_status
get_document_for_searching(conn* c, const item* item,
                           sized_buffer& document, uint64_t in_cas,
                           uint64_t& cas) {

    item_info_holder info;
    info.info.nvalue = IOV_MAX;

    if (!settings.engine.v1->get_item_info(settings.engine.v0, c, item,
                                           &info.info)) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                        "%d: Failed to get item info",
                                        c->sfd);
        return PROTOCOL_BINARY_RESPONSE_EINTERNAL;
    }

    // Need to have the complete document in a single iovec.
    if (info.info.nvalue != 1) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                        "%d: More than one iovec in document",
                                        c->sfd);
        return PROTOCOL_BINARY_RESPONSE_EINTERNAL;
    }

    // Check CAS matches (if specified by the user)
    if ((in_cas != 0) && in_cas != info.info.cas) {
        return PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS;
    }

    // Set CAS - same irrespective of datatype.
    cas = info.info.cas;

    switch (info.info.datatype) {
    case PROTOCOL_BINARY_DATATYPE_JSON:
        // Good to go using original buffer.
        document.buf = static_cast<char*>(info.info.value[0].iov_base);
        document.len = info.info.value[0].iov_len;
        return PROTOCOL_BINARY_RESPONSE_SUCCESS;

    case PROTOCOL_BINARY_DATATYPE_COMPRESSED_JSON:
        {
            // Need to expand before attempting to extract from it.
            const char* compressed_buf =
                    static_cast<char*>(info.info.value[0].iov_base);
            const size_t compressed_len = info.info.value[0].iov_len;
            size_t uncompressed_len;
            if (snappy_uncompressed_length(compressed_buf, compressed_len,
                                           &uncompressed_len) != SNAPPY_OK) {
                char clean_key[KEY_MAX_LENGTH + 32];
                if (buf_to_printable_buffer(clean_key, sizeof(clean_key),
                                            static_cast<const char*>(info.info.key),
                                            info.info.nkey) != -1) {
                    settings.extensions.logger->log(
                            EXTENSION_LOG_WARNING, c, "<%d ERROR: Failed to "
                            "determine inflated body size. Key: '%s' may have an "
                            "incorrect datatype of COMPRESSED_JSON.",
                            c->sfd, clean_key);
                }
                return PROTOCOL_BINARY_RESPONSE_EINTERNAL;
            }

            // We use the connections' dynamic buffer to uncompress into; this
            // will later be used as the the send buffer for the subset of the
            // document we send.
            if (!grow_dynamic_buffer(c, uncompressed_len)) {
                if (settings.verbose > 0) {
                    settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                            "<%d ERROR: Failed to grow dynamic buffer to %" PRIu64
                            "for uncompressing document.",
                            c->sfd, uncompressed_len);
                }
                return PROTOCOL_BINARY_RESPONSE_E2BIG;
            }

            char* buffer = c->dynamic_buffer.buffer + c->dynamic_buffer.offset;
            if (snappy_uncompress(compressed_buf, compressed_len, buffer,
                                  &uncompressed_len) != SNAPPY_OK) {
                char clean_key[KEY_MAX_LENGTH + 32];
                if (buf_to_printable_buffer(clean_key, sizeof(clean_key),
                                            static_cast<const char*>(info.info.key),
                                            info.info.nkey) != -1) {
                    settings.extensions.logger->log(
                            EXTENSION_LOG_WARNING, c, "<%d ERROR: Failed to "
                            "inflate body. Key: '%s' may have an incorrect "
                            "datatype of COMPRESSED_JSON.", c->sfd, clean_key);
                }
                return PROTOCOL_BINARY_RESPONSE_EINTERNAL;
            }

            // Update document to point to the uncompressed version in the buffer.
            document.buf = buffer;
            document.len = uncompressed_len;
            return PROTOCOL_BINARY_RESPONSE_SUCCESS;
        }

    case PROTOCOL_BINARY_RAW_BYTES:
    case PROTOCOL_BINARY_DATATYPE_COMPRESSED:
        // No good; need to have JSON
        return PROTOCOL_BINARY_RESPONSE_SUBDOC_DOC_NOTJSON;

    default:
        {
            // Unhandled datatype - shouldn't occur.
            char clean_key[KEY_MAX_LENGTH + 32];
            if (buf_to_printable_buffer(clean_key, sizeof(clean_key),
                                        static_cast<const char*>(info.info.key),
                                        info.info.nkey) != -1) {
                settings.extensions.logger->log(
                        EXTENSION_LOG_WARNING, c, "<%d ERROR: Unhandled datatype "
                        "'%u' of document '%s'.",
                        c->sfd, info.info.datatype, clean_key);
            }
            return PROTOCOL_BINARY_RESPONSE_EINTERNAL;
        }
    }
}

// Destructor function for SubdocCmdContext objects. Needed so we can cleanup
// any resouces used while executing a subdocument command, by setting
// c->cmd_context and c->cmd_context_dtor.
static void subdoc_context_dtor(void* context) {
    SubdocCmdContext* subdoc_context =
            reinterpret_cast<SubdocCmdContext*>(context);
    delete subdoc_context;
}

// Fetch the item to operate on from the engine.
// Returns true if the command was successful (and execution should continue),
// else false.
static bool subdoc_fetch(conn* c, ENGINE_ERROR_CODE ret, const char* key,
                         size_t keylen, uint16_t vbucket) {
    if (c->item == NULL) {
        item* initial_item;

        if (ret == ENGINE_SUCCESS) {
            ret = settings.engine.v1->get(settings.engine.v0, c, &initial_item,
                                          key, (int)keylen, vbucket);
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            // We have the item; assign to c->item (so we'll start from step 2
            // next time) and create the subdoc_cmd_context for the other
            // information we need to record.
            c->item = initial_item;
            cb_assert(c->cmd_context == NULL);
            c->cmd_context = new SubdocCmdContext(c);
            c->cmd_context_dtor = subdoc_context_dtor;
            break;

        case ENGINE_EWOULDBLOCK:
            c->ewouldblock = true;
            return false;

        case ENGINE_DISCONNECT:
            conn_set_state(c, conn_closing);
            return false;

        default:
            write_bin_packet(c, engine_error_2_protocol_error(ret));
            return false;
        }
    }

    return true;
}

// Operate on the document as specified by the the sub-document CMD template
// parameter.
// Returns true if the command was successful (and execution should continue),
// else false.
template<protocol_binary_command CMD>
static bool subdoc_operate(conn* c, const char* path, size_t pathlen,
                           const char* value, size_t vallen,
                           protocol_binary_subdoc_flag flags, uint64_t in_cas) {
    SubdocCmdContext* context =
            reinterpret_cast<SubdocCmdContext*>(c->cmd_context);
    cb_assert(context != NULL);

    if (context->in_doc.buf == NULL) {
        // Retrieve the item_info the engine, and if necessary
        // uncompress it so subjson can parse it.
        uint64_t doc_cas;
        sized_buffer doc;
        protocol_binary_response_status status =
            get_document_for_searching(c, c->item, doc, in_cas, doc_cas);

        if (status != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
            // Failed. Note c->item and c->cmd_context will both be freed for
            // us as part of preparing for the next command.
            write_bin_packet(c, status);
            return false;
        }

        // Prepare the specified sub-document command.
        Subdoc::Operation* op = c->thread->subdoc_op;
        op->clear();
        subdoc_OPTYPE opcode = cmd_traits<Cmd2Type<CMD>>::optype;
        if ((flags & SUBDOC_FLAG_MKDIR_P) == SUBDOC_FLAG_MKDIR_P) {
            opcode = subdoc_OPTYPE(opcode | SUBDOC_CMD_FLAG_MKDIR_P);
        }
        op->set_code(opcode);
        op->set_doc(doc.buf, doc.len);
        if (cmd_traits<Cmd2Type<CMD>>::request_has_value) {
            op->set_value(value, vallen);
        }

        // ... and execute it.
        Subdoc::Error subdoc_res = op->op_exec(path, pathlen);

        switch (subdoc_res) {
        case SUBDOC_STATUS_SUCCESS:
            // Save the information necessary to construct the result of the
            // subdoc.
            context->in_doc = doc;
            context->in_cas = doc_cas;
            context->doc_new_len = op->doc_new_len;
            for (unsigned int i = 0; i < op->doc_new_len; i++) {
                context->doc_new[i] = op->doc_new[i];
            }
            break;

        case SUBDOC_STATUS_PATH_ENOENT:
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_ENOENT);
            return false;

        case SUBDOC_STATUS_PATH_MISMATCH:
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_MISMATCH);
            return false;

        case SUBDOC_STATUS_DOC_ETOODEEP:
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_SUBDOC_DOC_E2DEEP);
            return false;

        case SUBDOC_STATUS_PATH_EINVAL:
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_EINVAL);
            return false;

        case SUBDOC_STATUS_DOC_EEXISTS:
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_EEXISTS);
            return false;

        case SUBDOC_STATUS_PATH_E2BIG:
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_E2BIG);
            return false;

        case SUBDOC_STATUS_VALUE_CANTINSERT:
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_SUBDOC_VALUE_CANTINSERT);
            return false;

        default:
            // TODO: handle remaining errors.
            settings.extensions.logger->log(EXTENSION_LOG_DEBUG, c,
                                            "Unexpected response from subdoc: %d (0x%x)", subdoc_res, subdoc_res);
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL);
            return false;
        }
    }

    return true;
}

// Update the engine with whatever modifications the subdocument command made
// to the document.
// Returns true if the updare was successful (and execution should continue),
// else false.
template<protocol_binary_command CMD>
bool subdoc_update(conn* c, ENGINE_ERROR_CODE ret, const char* key,
                   size_t keylen, uint16_t vbucket) {
    SubdocCmdContext* context =
            reinterpret_cast<SubdocCmdContext*>(c->cmd_context);
    cb_assert(context != NULL);

    if (!cmd_traits<Cmd2Type<CMD>>::is_mutator) {
        // No update required - just make sure we have the correct cas to use
        // for resposne.
        c->cas = context->in_cas;
        return true;
    }

    // Calculate the updated document length.
    size_t new_doc_len = 0;
    for (size_t ii = 0; ii < context->doc_new_len; ii++) {
        const subdoc_LOC& loc = context->doc_new[ii];
        new_doc_len += loc.length;
    }

    // Allocate a new item of this size.
    if (context->out_doc == NULL) {
        item *new_doc;

        if (ret == ENGINE_SUCCESS) {
            ret = settings.engine.v1->allocate(settings.engine.v0, c, &new_doc,
                                               key, keylen, new_doc_len, 0, 0,
                                               PROTOCOL_BINARY_DATATYPE_JSON);
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            // Save the allocated document in the cmd context.
            context->out_doc = new_doc;
            break;

        case ENGINE_EWOULDBLOCK:
            c->ewouldblock = true;
            return false;

        case ENGINE_DISCONNECT:
            conn_set_state(c, conn_closing);
            return false;

        default:
            write_bin_packet(c, engine_error_2_protocol_error(ret));
            return false;
        }

        // To ensure we only replace the version of the document we
        // just appended to; set the CAS to the one retrieved from.
        settings.engine.v1->item_set_cas(settings.engine.v0, c,
                                         new_doc, context->in_cas);

        // Obtain the item info (and it's iovectors)
        item_info new_doc_info;
        new_doc_info.nvalue = IOV_MAX;
        if (!settings.engine.v1->get_item_info(settings.engine.v0, c,
                                               new_doc, &new_doc_info)) {
            // TODO: free everything!!
            write_bin_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL);
            return false;
        }

        // Copy the new document into the item.
        char* write_ptr = static_cast<char*>(new_doc_info.value[0].iov_base);
        for (size_t ii = 0; ii < context->doc_new_len; ii++) {
            const subdoc_LOC& loc = context->doc_new[ii];
            std::memcpy(write_ptr, loc.at, loc.length);
            write_ptr += loc.length;
        }
    }

    // And finally, store the new document.
    uint64_t new_cas;
    ret = settings.engine.v1->store(settings.engine.v0, c,
                                    context->out_doc, &new_cas,
                                    OPERATION_REPLACE, vbucket);
    switch (ret) {
    case ENGINE_SUCCESS:
        c->cas = new_cas;
        break;

    case ENGINE_EWOULDBLOCK:
        c->ewouldblock = true;
        return false;

    case ENGINE_DISCONNECT:
        c->state = conn_closing;
        return false;
    default:
        write_bin_packet(c, engine_error_2_protocol_error(ret));
        return false;
    }

    return true;
}

// Respond back to the user as appropriate to the specific command.
template<protocol_binary_command CMD>
void subdoc_response(conn* c) {
    SubdocCmdContext* context =
            reinterpret_cast<SubdocCmdContext*>(c->cmd_context);
    cb_assert(context != NULL);

    protocol_binary_response_subdocument* rsp =
            reinterpret_cast<protocol_binary_response_subdocument*>(c->write.buf);

    const char* value = NULL;
    size_t vallen = 0;
    if (cmd_traits<Cmd2Type<CMD>>::response_has_value) {
        value = c->thread->subdoc_op->match.loc_match.at;
        vallen = c->thread->subdoc_op->match.loc_match.length;
    }

    if (add_bin_header(c, 0, /*extlen*/0, /*keylen*/0, vallen,
                       PROTOCOL_BINARY_RAW_BYTES) == -1) {
        conn_set_state(c, conn_closing);
        return;
    }
    rsp->message.header.response.cas = htonll(c->cas);

    if (cmd_traits<Cmd2Type<CMD>>::response_has_value) {
        add_iov(c, value, vallen);
    }
    conn_set_state(c, conn_mwrite);
}

void subdoc_get_executor(conn *c, void* packet) {
    return subdoc_executor<PROTOCOL_BINARY_CMD_SUBDOC_GET>(c, packet);
}

void subdoc_exists_executor(conn *c, void* packet) {
    return subdoc_executor<PROTOCOL_BINARY_CMD_SUBDOC_EXISTS>(c, packet);
}

void subdoc_dict_add_executor(conn *c, void *packet) {
    return subdoc_executor<PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD>(c, packet);
}

void subdoc_dict_upsert_executor(conn *c, void *packet) {
    return subdoc_executor<PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT>(c, packet);
}

void subdoc_delete_executor(conn *c, void *packet) {
    return subdoc_executor<PROTOCOL_BINARY_CMD_SUBDOC_DELETE>(c, packet);
}

void subdoc_replace_executor(conn *c, void *packet) {
    return subdoc_executor<PROTOCOL_BINARY_CMD_SUBDOC_REPLACE>(c, packet);
}

void subdoc_array_push_last_executor(conn *c, void *packet) {
    return subdoc_executor<PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST>(c, packet);
}

void subdoc_array_push_first_executor(conn *c, void *packet) {
    return subdoc_executor<PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST>(c, packet);
}
