/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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
#include "mcbp.h"

#include "debug_helpers.h"
#include "memcached.h"
#include "utilities/protocol2text.h"

static ENGINE_ERROR_CODE get_vb_map_cb(const void* cookie,
                                       const void* map,
                                       size_t mapsize) {
    char* buf;
    Connection* c = (Connection*)cookie;
    protocol_binary_response_header header;
    size_t needed = mapsize + sizeof(protocol_binary_response_header);
    if (!c->growDynamicBuffer(needed)) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                        "<%d ERROR: Failed to allocate memory for response",
                                        c->getId());
        return ENGINE_ENOMEM;
    }

    auto& buffer = c->getDynamicBuffer();
    buf = buffer.getCurrent();
    memset(&header, 0, sizeof(header));

    header.response.magic = (uint8_t)PROTOCOL_BINARY_RES;
    header.response.opcode = c->binary_header.request.opcode;
    header.response.status = (uint16_t)htons(
        PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET);
    header.response.bodylen = htonl((uint32_t)mapsize);
    header.response.opaque = c->getOpaque();

    memcpy(buf, header.bytes, sizeof(header.response));
    buf += sizeof(header.response);
    memcpy(buf, map, mapsize);
    buffer.moveOffset(needed);

    return ENGINE_SUCCESS;
}

void mcbp_write_response(Connection* c,
                         const void* d,
                         int extlen,
                         int keylen,
                         int dlen) {
    if (!c->isNoReply() || c->getCmd() == PROTOCOL_BINARY_CMD_GET ||
        c->getCmd() == PROTOCOL_BINARY_CMD_GETK) {
        if (mcbp_add_header(c, 0, extlen, keylen, dlen,
                            PROTOCOL_BINARY_RAW_BYTES) == -1) {
            c->setState(conn_closing);
            return;
        }
        c->addIov(d, dlen);
        c->setState(conn_mwrite);
        c->setWriteAndGo(conn_new_cmd);
    } else {
        if (c->getStart() != 0) {
            collect_timings(c);
            c->setStart(0);
        }
        c->setState(conn_new_cmd);
    }
}

void mcbp_write_packet(Connection* c, protocol_binary_response_status err) {
    if (err == PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET) {
        ENGINE_ERROR_CODE ret;

        ret = bucket_get_engine_vb_map(c, get_vb_map_cb);
        if (ret == ENGINE_SUCCESS) {
            write_and_free(c, &c->getDynamicBuffer());
        } else {
            c->setState(conn_closing);
        }
    } else {
        ssize_t len = 0;
        const char* errtext = nullptr;

        // Determine the error-code policy for the current opcode & error code.
        // "Legacy" memcached opcodes had the behaviour of sending a textual
        // description of the error in the response body for most errors.
        // We don't want to do that for any new commands.
        if (c->includeErrorStringInResponseBody(err)) {
            errtext = memcached_status_2_text(err);
            if (errtext != nullptr) {
                len = (ssize_t)strlen(errtext);
            }
        }

        if (errtext && settings.verbose > 1) {
            settings.extensions.logger->log(EXTENSION_LOG_DEBUG, c,
                                            ">%u Writing an error: %s",
                                            c->getId(), errtext);
        }

        mcbp_add_header(c, err, 0, 0, len, PROTOCOL_BINARY_RAW_BYTES);
        if (errtext) {
            c->addIov(errtext, len);
        }
        c->setState(conn_mwrite);
        c->setWriteAndGo(conn_new_cmd);
    }
}

int mcbp_add_header(Connection* c,
                    uint16_t err,
                    uint8_t ext_len,
                    uint16_t key_len,
                    uint32_t body_len,
                    uint8_t datatype) {
    protocol_binary_response_header* header;

    if (c == nullptr) {
        throw std::invalid_argument(
            "mcbp_add_header: 'c' must be non-NULL");
    }

    if (!c->addMsgHdr(true)) {
        return -1;
    }

    header = (protocol_binary_response_header*)c->write.buf;

    header->response.magic = (uint8_t)PROTOCOL_BINARY_RES;
    header->response.opcode = c->binary_header.request.opcode;
    header->response.keylen = (uint16_t)htons(key_len);

    header->response.extlen = ext_len;
    header->response.datatype = datatype;
    header->response.status = (uint16_t)htons(err);

    header->response.bodylen = htonl(body_len);
    header->response.opaque = c->getOpaque();
    header->response.cas = htonll(c->getCAS());

    if (settings.verbose > 1) {
        char buffer[1024];
        if (bytes_to_output_string(buffer, sizeof(buffer), c->getId(), false,
                                   "Writing bin response:",
                                   (const char*)header->bytes,
                                   sizeof(header->bytes)) != -1) {
            settings.extensions.logger->log(EXTENSION_LOG_DEBUG, c,
                                            "%s", buffer);
        }
    }

    return c->addIov(c->write.buf, sizeof(header->response)) ? 0 : -1;
}

protocol_binary_response_status engine_error_2_mcbp_protocol_error(
    ENGINE_ERROR_CODE e) {
    protocol_binary_response_status ret;

    switch (e) {
    case ENGINE_SUCCESS:
        return PROTOCOL_BINARY_RESPONSE_SUCCESS;
    case ENGINE_KEY_ENOENT:
        return PROTOCOL_BINARY_RESPONSE_KEY_ENOENT;
    case ENGINE_KEY_EEXISTS:
        return PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS;
    case ENGINE_ENOMEM:
        return PROTOCOL_BINARY_RESPONSE_ENOMEM;
    case ENGINE_TMPFAIL:
        return PROTOCOL_BINARY_RESPONSE_ETMPFAIL;
    case ENGINE_NOT_STORED:
        return PROTOCOL_BINARY_RESPONSE_NOT_STORED;
    case ENGINE_EINVAL:
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    case ENGINE_ENOTSUP:
        return PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED;
    case ENGINE_E2BIG:
        return PROTOCOL_BINARY_RESPONSE_E2BIG;
    case ENGINE_NOT_MY_VBUCKET:
        return PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET;
    case ENGINE_ERANGE:
        return PROTOCOL_BINARY_RESPONSE_ERANGE;
    case ENGINE_ROLLBACK:
        return PROTOCOL_BINARY_RESPONSE_ROLLBACK;
    case ENGINE_NO_BUCKET:
        return PROTOCOL_BINARY_RESPONSE_NO_BUCKET;
    case ENGINE_EBUSY:
        return PROTOCOL_BINARY_RESPONSE_EBUSY;
    default:
        ret = PROTOCOL_BINARY_RESPONSE_EINTERNAL;
    }

    return ret;
}
