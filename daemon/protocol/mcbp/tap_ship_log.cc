/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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
#include <daemon/memcached.h>
#include <snappy-c.h>

void ship_mcbp_tap_log(McbpConnection* c) {
    bool more_data = true;
    bool send_data = false;
    bool disconnect = false;
    item* it;
    uint32_t bodylen;
    int ii = 0;

    c->addMsgHdr(true);
    /* @todo add check for buffer overflow of c->write.buf) */
    c->write.bytes = 0;
    c->write.curr = c->write.buf;

    auto tap_iterator = c->getTapIterator();
    do {
        /* @todo fixme! */
        void* engine;
        uint16_t nengine;
        uint8_t ttl;
        uint16_t tap_flags;
        uint32_t seqno;
        uint16_t vbucket;
        tap_event_t event;
        bool inflate = false;
        size_t inflated_length = 0;

        union {
            protocol_binary_request_tap_mutation mutation;
            protocol_binary_request_tap_delete del;
            protocol_binary_request_tap_flush flush;
            protocol_binary_request_tap_opaque opaque;
            protocol_binary_request_noop noop;
        } msg;
        item_info info;

        if (ii++ == 10) {
            break;
        }

        event = tap_iterator(c->getBucketEngineAsV0(), c->getCookie(), &it,
                             &engine, &nengine, &ttl,
                             &tap_flags, &seqno, &vbucket);
        memset(&msg, 0, sizeof(msg));
        msg.opaque.message.header.request.magic = (uint8_t)PROTOCOL_BINARY_REQ;
        msg.opaque.message.header.request.opaque = htonl(seqno);
        msg.opaque.message.body.tap.enginespecific_length = htons(nengine);
        msg.opaque.message.body.tap.ttl = ttl;
        msg.opaque.message.body.tap.flags = htons(tap_flags);
        msg.opaque.message.header.request.extlen = 8;
        msg.opaque.message.header.request.vbucket = htons(vbucket);
        info.nvalue = 1;

        switch (event) {
        case TAP_NOOP :
            send_data = true;
            msg.noop.message.header.request.opcode = PROTOCOL_BINARY_CMD_NOOP;
            msg.noop.message.header.request.extlen = 0;
            msg.noop.message.header.request.bodylen = htonl(0);
            memcpy(c->write.curr, msg.noop.bytes, sizeof(msg.noop.bytes));
            c->addIov(c->write.curr, sizeof(msg.noop.bytes));
            c->write.curr += sizeof(msg.noop.bytes);
            c->write.bytes += sizeof(msg.noop.bytes);
            break;
        case TAP_PAUSE :
            more_data = false;
            break;
        case TAP_CHECKPOINT_START:
        case TAP_CHECKPOINT_END:
        case TAP_MUTATION:
            if (!bucket_get_item_info(c, it, &info)) {
                bucket_release_item(c, it);
                LOG_WARNING(c, "%u: Failed to get item info", c->getId());
                break;
            }

            if (!c->reserveItem(it)) {
                bucket_release_item(c, it);
                LOG_WARNING(c, "%u: Failed to grow item array", c->getId());
                break;
            }
            send_data = true;

            if (event == TAP_CHECKPOINT_START) {
                msg.mutation.message.header.request.opcode =
                    PROTOCOL_BINARY_CMD_TAP_CHECKPOINT_START;
                tap_stats.sent.checkpoint_start++;
            } else if (event == TAP_CHECKPOINT_END) {
                msg.mutation.message.header.request.opcode =
                    PROTOCOL_BINARY_CMD_TAP_CHECKPOINT_END;
                tap_stats.sent.checkpoint_end++;
            } else if (event == TAP_MUTATION) {
                msg.mutation.message.header.request.opcode = PROTOCOL_BINARY_CMD_TAP_MUTATION;
                tap_stats.sent.mutation++;
            }

            msg.mutation.message.header.request.cas = htonll(info.cas);
            msg.mutation.message.header.request.keylen = htons(info.nkey);
            msg.mutation.message.header.request.extlen = 16;
            if (c->isSupportsDatatype()) {
                msg.mutation.message.header.request.datatype = info.datatype;
            } else {
                inflate = mcbp::datatype::is_compressed(info.datatype);
                msg.mutation.message.header.request.datatype = 0;
            }

            bodylen = 16 + info.nkey + nengine;
            if ((tap_flags & TAP_FLAG_NO_VALUE) == 0) {
                if (inflate) {
                    if (snappy_uncompressed_length
                            (reinterpret_cast<const char*>(info.value[0].iov_base),
                             info.nbytes, &inflated_length) == SNAPPY_OK) {
                        bodylen += (uint32_t)inflated_length;
                    } else {
                        LOG_WARNING(c,
                                    "<%u Failed to determine inflated size. "
                                        "Sending as compressed",
                                    c->getId());
                        inflate = false;
                        bodylen += info.nbytes;
                    }
                } else {
                    bodylen += info.nbytes;
                }
            }
            msg.mutation.message.header.request.bodylen = htonl(bodylen);

            if ((tap_flags & TAP_FLAG_NETWORK_BYTE_ORDER) == 0) {
                msg.mutation.message.body.item.flags = htonl(info.flags);
            } else {
                msg.mutation.message.body.item.flags = info.flags;
            }
            msg.mutation.message.body.item.expiration = htonl(
                info.exptime);
            msg.mutation.message.body.tap.enginespecific_length = htons(
                nengine);
            msg.mutation.message.body.tap.ttl = ttl;
            msg.mutation.message.body.tap.flags = htons(tap_flags);
            memcpy(c->write.curr, msg.mutation.bytes,
                   sizeof(msg.mutation.bytes));

            c->addIov(c->write.curr, sizeof(msg.mutation.bytes));
            c->write.curr += sizeof(msg.mutation.bytes);
            c->write.bytes += sizeof(msg.mutation.bytes);

            if (nengine > 0) {
                memcpy(c->write.curr, engine, nengine);
                c->addIov(c->write.curr, nengine);
                c->write.curr += nengine;
                c->write.bytes += nengine;
            }

            c->addIov(info.key, info.nkey);
            if ((tap_flags & TAP_FLAG_NO_VALUE) == 0) {
                if (inflate) {
                    char* buf = reinterpret_cast<char*>(cb_malloc(
                        inflated_length));
                    if (buf == NULL) {
                        LOG_WARNING(c,
                                    "%u: FATAL: failed to allocate buffer "
                                        "of size %" PRIu64
                                        " to inflate object into. Shutting "
                                        "down connection",
                                    c->getId(), inflated_length);
                        c->setState(conn_closing);
                        return;
                    }
                    const char* body = reinterpret_cast<const char*>(info.value[0].iov_base);
                    size_t input_length = info.value[0].iov_len;
                    if (snappy_uncompress(body, input_length,
                                          buf, &inflated_length) == SNAPPY_OK) {
                        if (!c->pushTempAlloc(buf)) {
                            cb_free(buf);
                            LOG_WARNING(c,
                                        "%u: FATAL: failed to allocate space "
                                            "to keep temporary buffer",
                                        c->getId());
                            c->setState(conn_closing);
                            return;
                        }
                        c->addIov(buf, inflated_length);
                    } else {
                        cb_free(buf);
                        LOG_WARNING(c,
                                    "%u: FATAL: failed to inflate object. "
                                        "shutting down connection",
                                    c->getId());
                        c->setState(conn_closing);
                        return;
                    }
                } else {
                    c->addIov(info.value[0].iov_base, info.value[0].iov_len);
                }
            }

            break;
        case TAP_DELETION:
            /* This is a delete */
            if (!bucket_get_item_info(c, it, &info)) {
                bucket_release_item(c, it);
                LOG_WARNING(c, "%u: Failed to get item info", c->getId());
                break;
            }

            if (!c->reserveItem(it)) {
                bucket_release_item(c, it);
                LOG_WARNING(c, "%u: Failed to grow item array", c->getId());
                break;
            }
            send_data = true;
            msg.del.message.header.request.opcode = PROTOCOL_BINARY_CMD_TAP_DELETE;
            msg.del.message.header.request.cas = htonll(info.cas);
            msg.del.message.header.request.keylen = htons(info.nkey);

            bodylen = 8 + info.nkey + nengine;
            if ((tap_flags & TAP_FLAG_NO_VALUE) == 0) {
                bodylen += info.nbytes;
            }
            msg.del.message.header.request.bodylen = htonl(bodylen);

            memcpy(c->write.curr, msg.del.bytes, sizeof(msg.del.bytes));
            c->addIov(c->write.curr, sizeof(msg.del.bytes));
            c->write.curr += sizeof(msg.del.bytes);
            c->write.bytes += sizeof(msg.del.bytes);

            if (nengine > 0) {
                memcpy(c->write.curr, engine, nengine);
                c->addIov(c->write.curr, nengine);
                c->write.curr += nengine;
                c->write.bytes += nengine;
            }

            c->addIov(info.key, info.nkey);
            if ((tap_flags & TAP_FLAG_NO_VALUE) == 0) {
                c->addIov(info.value[0].iov_base, info.value[0].iov_len);
            }

            tap_stats.sent.del++;
            break;

        case TAP_DISCONNECT:
            disconnect = true;
            more_data = false;
            break;
        case TAP_VBUCKET_SET:
        case TAP_FLUSH:
        case TAP_OPAQUE:
            send_data = true;

            if (event == TAP_OPAQUE) {
                msg.flush.message.header.request.opcode = PROTOCOL_BINARY_CMD_TAP_OPAQUE;
                tap_stats.sent.opaque++;
            } else if (event == TAP_FLUSH) {
                msg.flush.message.header.request.opcode = PROTOCOL_BINARY_CMD_TAP_FLUSH;
                tap_stats.sent.flush++;
            } else if (event == TAP_VBUCKET_SET) {
                msg.flush.message.header.request.opcode = PROTOCOL_BINARY_CMD_TAP_VBUCKET_SET;
                msg.flush.message.body.tap.flags = htons(tap_flags);
                tap_stats.sent.vbucket_set++;
            }

            msg.flush.message.header.request.bodylen = htonl(8 + nengine);
            memcpy(c->write.curr, msg.flush.bytes, sizeof(msg.flush.bytes));
            c->addIov(c->write.curr, sizeof(msg.flush.bytes));
            c->write.curr += sizeof(msg.flush.bytes);
            c->write.bytes += sizeof(msg.flush.bytes);
            if (nengine > 0) {
                memcpy(c->write.curr, engine, nengine);
                c->addIov(c->write.curr, nengine);
                c->write.curr += nengine;
                c->write.bytes += nengine;
            }
            break;
        default:
            LOG_WARNING(c,
                        "%u: ship_tap_log: event (which is %d) is not a valid "
                            "tap_event_t - closing connection", event);
            c->setState(conn_closing);
            return;
        }
    } while (more_data);

    c->setEwouldblock(false);
    if (send_data) {
        c->setState(conn_mwrite);
        if (disconnect) {
            c->setWriteAndGo(conn_closing);
        } else {
            c->setWriteAndGo(conn_ship_log);
        }
    } else {
        if (disconnect) {
            c->setState(conn_closing);
        } else {
            /* No more items to ship to the slave at this time.. suspend.. */
            LOG_DEBUG(c, "%u: No more items in tap log.. waiting", c->getId());
            c->setEwouldblock(true);
        }
    }
}