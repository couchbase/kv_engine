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

#include <snappy-c.h>

static int get_clustermap_revno(const char *map, size_t mapsize) {
    /* Try to locate the "rev": field in the map. Unfortunately
     * we can't use the function strnstr because it's not available
     * on all platforms
     */
    const std::string prefix = "\"rev\":";

    if (mapsize == 0 || *map != '{' || mapsize < (prefix.length() + 1) ) {
        /* This doesn't look like our cluster map */
        return -1;
    }
    mapsize -= prefix.length();

    for (size_t index = 1; index < mapsize; ++index) {
        if (memcmp(map + index, prefix.data(), prefix.length()) == 0) {
            index += prefix.length();
            /* Found :-) */
            while (isspace(map[index])) {
                ++index;
            }

            if (!isdigit(map[index])) {
                return -1;
            }

            return atoi(map + index);
        }
    }

    /* not found */
    return -1;
}

static ENGINE_ERROR_CODE get_vb_map_cb(const void* void_cookie,
                                       const void* map,
                                       size_t mapsize) {
    char* buf;

    auto* cookie = reinterpret_cast<const Cookie*>(void_cookie);
    if (cookie->connection == nullptr) {
        throw std::runtime_error("get_vb_map_cb: cookie must represent connection");
    }

    McbpConnection* c = reinterpret_cast<McbpConnection*>(cookie->connection);
    protocol_binary_response_header header;
    size_t needed = sizeof(protocol_binary_response_header);

    if (settings.isDedupeNmvbMaps()) {
        int revno = get_clustermap_revno(reinterpret_cast<const char*>(map),
                                         mapsize);
        if (revno == c->getClustermapRevno()) {
            /* The client already have this map... */
            mapsize = 0;
        } else if (revno != -1) {
            c->setClustermapRevno(revno);
        }
    }

    needed += mapsize;
    if (!c->growDynamicBuffer(needed)) {
        LOG_WARNING(c, "<%d ERROR: Failed to allocate memory for response",
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

void mcbp_write_response(McbpConnection* c,
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
            mcbp_collect_timings(reinterpret_cast<McbpConnection*>(c));
            c->setStart(0);
        }
        c->setState(conn_new_cmd);
    }
}

void mcbp_write_and_free(McbpConnection* c, DynamicBuffer* buf) {
    if (buf->getRoot() == nullptr) {
        c->setState(conn_closing);
    } else {
        if (!c->pushTempAlloc(buf->getRoot())) {
            c->setState(conn_closing);
            return;
        }
        c->write.curr = buf->getRoot();
        c->write.bytes = (uint32_t)buf->getOffset();
        c->setState(conn_write);
        c->setWriteAndGo(conn_new_cmd);

        buf->takeOwnership();
    }
}

void mcbp_write_packet(McbpConnection* c, protocol_binary_response_status err) {
    if (err == PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        mcbp_write_response(c, NULL, 0, 0, 0);
        return;
    }
    if ((err == PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET) &&
        (c->getBucketEngine()->get_engine_vb_map != nullptr)) {

        ENGINE_ERROR_CODE ret;

        ret = bucket_get_engine_vb_map(c, get_vb_map_cb);
        if (ret == ENGINE_SUCCESS) {
            mcbp_write_and_free(c, &c->getDynamicBuffer());
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

        if (errtext && settings.getVerbose() > 1) {
            LOG_DEBUG(c, ">%u Writing an error: %s", c->getId(), errtext);
        }

        mcbp_add_header(c, err, 0, 0, len, PROTOCOL_BINARY_RAW_BYTES);
        if (errtext) {
            c->addIov(errtext, len);
        }
        c->setState(conn_mwrite);
        c->setWriteAndGo(conn_new_cmd);
    }
}

int mcbp_add_header(McbpConnection* c,
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

    if (settings.getVerbose() > 1) {
        char buffer[1024];
        if (bytes_to_output_string(buffer, sizeof(buffer), c->getId(), false,
                                   "Writing bin response:",
                                   (const char*)header->bytes,
                                   sizeof(header->bytes)) != -1) {
            LOG_DEBUG(c, "%s", buffer);
        }
    }

    return c->addIov(c->write.buf, sizeof(header->response)) ? 0 : -1;
}

protocol_binary_response_status engine_error_2_mcbp_protocol_error(
    ENGINE_ERROR_CODE e) {
    protocol_binary_response_status ret;

    switch (e) {
    case ENGINE_EACCESS:
        return PROTOCOL_BINARY_RESPONSE_EACCESS;
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
    case ENGINE_AUTH_STALE:
        return PROTOCOL_BINARY_RESPONSE_AUTH_STALE;
    case ENGINE_DELTA_BADVAL:
        return PROTOCOL_BINARY_RESPONSE_DELTA_BADVAL;
    default:
        ret = PROTOCOL_BINARY_RESPONSE_EINTERNAL;
    }

    return ret;
}

bool mcbp_response_handler(const void* key, uint16_t keylen,
                           const void* ext, uint8_t extlen,
                           const void* body, uint32_t bodylen,
                           uint8_t datatype, uint16_t status,
                           uint64_t cas, const void* void_cookie)
{
    protocol_binary_response_header header;
    char *buf;

    auto* cookie = reinterpret_cast<const Cookie*>(void_cookie);
    cookie->validate();
    if (cookie->connection == nullptr) {
        throw std::runtime_error(
            "mcbp_response_handler: cookie must represent connection");
    }

    McbpConnection* c = reinterpret_cast<McbpConnection*>(cookie->connection);
    /* Look at append_bin_stats */
    size_t needed;
    bool need_inflate = false;
    size_t inflated_length;

    if (!c->isSupportsDatatype()) {
        if (mcbp::datatype::is_compressed(datatype)) {
            need_inflate = true;
        }
        /* We may silently drop the knowledge about a JSON item */
        datatype = PROTOCOL_BINARY_RAW_BYTES;
    }

    needed = keylen + extlen + sizeof(protocol_binary_response_header);
    if (need_inflate) {
        if (snappy_uncompressed_length(reinterpret_cast<const char*>(body),
                                       bodylen, &inflated_length) != SNAPPY_OK) {
            LOG_WARNING(c,
                        "<%u ERROR: Failed to inflate body, "
                            "Key: %s may have an incorrect datatype, "
                            "Datatype indicates that document is %s",
                        c->getId(), (const char*)key,
                        (datatype == PROTOCOL_BINARY_DATATYPE_COMPRESSED) ?
                        "RAW_COMPRESSED" : "JSON_COMPRESSED");
            return false;
        }
        needed += inflated_length;
    } else {
        needed += bodylen;
    }

    auto &dbuf = c->getDynamicBuffer();
    if (!dbuf.grow(needed)) {
        LOG_WARNING(c, "<%u ERROR: Failed to allocate memory for response",
                    c->getId());
        return false;
    }

    buf = dbuf.getCurrent();
    memset(&header, 0, sizeof(header));
    header.response.magic = (uint8_t)PROTOCOL_BINARY_RES;
    header.response.opcode = c->binary_header.request.opcode;
    header.response.keylen = (uint16_t)htons(keylen);
    header.response.extlen = extlen;
    header.response.datatype = datatype;
    header.response.status = (uint16_t)htons(status);
    if (need_inflate) {
        header.response.bodylen = htonl((uint32_t)(inflated_length + keylen + extlen));
    } else {
        header.response.bodylen = htonl(bodylen + keylen + extlen);
    }
    header.response.opaque = c->getOpaque();
    header.response.cas = htonll(cas);

    memcpy(buf, header.bytes, sizeof(header.response));
    buf += sizeof(header.response);

    if (extlen > 0) {
        memcpy(buf, ext, extlen);
        buf += extlen;
    }

    if (keylen > 0) {
        cb_assert(key != NULL);
        memcpy(buf, key, keylen);
        buf += keylen;
    }

    if (bodylen > 0) {
        if (need_inflate) {
            if (snappy_uncompress(reinterpret_cast<const char*>(body), bodylen,
                                  buf, &inflated_length) != SNAPPY_OK) {
                LOG_WARNING(c, "<%u Failed to inflate item", c->getId());
                return false;
            }
        } else {
            memcpy(buf, body, bodylen);
        }
    }

    dbuf.moveOffset(needed);
    return true;
}

void mcbp_collect_timings(const McbpConnection* c) {
    hrtime_t now = gethrtime();
    const hrtime_t elapsed_ns = now - c->getStart();
    // aggregated timing for all buckets
    all_buckets[0].timings.collect(c->getCmd(), elapsed_ns);

    // timing for current bucket
    bucket_id_t bucketid = get_bucket_id(c->getCookie());
    /* bucketid will be zero initially before you run sasl auth
     * (unless there is a default bucket), or if someone tries
     * to delete the bucket you're associated with and your're idle.
     */
    if (bucketid != 0) {
        all_buckets[bucketid].timings.collect(c->getCmd(), elapsed_ns);
    }

    // Log operations taking longer than 0.5s
    const hrtime_t elapsed_ms = elapsed_ns / (1000 * 1000);
    c->maybeLogSlowCommand(std::chrono::milliseconds(elapsed_ms));
}
