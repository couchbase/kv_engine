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

#include "mcbp_executors.h"

#include "mcbp.h"
#include "memcached.h"
#include "session_cas.h"
#include "buckets.h"
#include "config_parse.h"
#include "ioctl.h"
#include "runtime.h"
#include "debug_helpers.h"
#include "mcaudit.h"
#include "subdocument.h"
#include "mc_time.h"
#include "connections.h"
#include "mcbp_validators.h"
#include "mcbp_topkeys.h"
#include "enginemap.h"
#include "mcbpdestroybuckettask.h"
#include "sasl_tasks.h"
#include "mcbp_privileges.h"
#include "protocol/mcbp/appendprepend_context.h"
#include "protocol/mcbp/arithmetic_context.h"
#include "protocol/mcbp/get_context.h"
#include "protocol/mcbp/get_locked_context.h"
#include "protocol/mcbp/dcp_deletion.h"
#include "protocol/mcbp/dcp_expiration.h"
#include "protocol/mcbp/dcp_mutation.h"
#include "protocol/mcbp/engine_wrapper.h"
#include "protocol/mcbp/executors.h"
#include "protocol/mcbp/mutation_context.h"
#include "protocol/mcbp/remove_context.h"
#include "protocol/mcbp/stats_context.h"
#include "protocol/mcbp/steppable_command_context.h"
#include "protocol/mcbp/unlock_context.h"
#include "protocol/mcbp/utilities.h"
#include "settings.h"

#include <cctype>
#include <memcached/audit_interface.h>
#include <memcached/rbac.h>
#include <platform/cb_malloc.h>
#include <platform/checked_snprintf.h>
#include <platform/compress.h>
#include <snappy-c.h>
#include <utilities/protocol2text.h>

struct tap_stats tap_stats;

std::array<bool, 0x100>&  topkey_commands = get_mcbp_topkeys();
std::array<mcbp_package_execute, 0x100>& executors = get_mcbp_executors();

static bool authenticated(McbpConnection* c) {
    bool rv;

    switch (c->getCmd()) {
    case PROTOCOL_BINARY_CMD_SASL_LIST_MECHS: /* FALLTHROUGH */
    case PROTOCOL_BINARY_CMD_SASL_AUTH:       /* FALLTHROUGH */
    case PROTOCOL_BINARY_CMD_SASL_STEP:       /* FALLTHROUGH */
    case PROTOCOL_BINARY_CMD_VERSION:         /* FALLTHROUGH */
    case PROTOCOL_BINARY_CMD_HELLO:
        rv = true;
        break;
    default:
        rv = c->isAuthenticated();
    }

    if (settings.getVerbose() > 1) {
        LOG_DEBUG(c, "%u: authenticated() in cmd 0x%02x is %s",
                  c->getId(), c->getCmd(), rv ? "true" : "false");
    }

    return rv;
}

static void bin_read_chunk(McbpConnection* c, uint32_t chunk) {
    ptrdiff_t offset;
    c->setRlbytes(chunk);

    /* Ok... do we have room for everything in our buffer? */
    offset =
        c->read.curr + sizeof(protocol_binary_request_header) - c->read.buf;
    if (c->getRlbytes() > c->read.size - offset) {
        size_t nsize = c->read.size;
        size_t size = c->getRlbytes() + sizeof(protocol_binary_request_header);

        while (size > nsize) {
            nsize *= 2;
        }

        if (nsize != c->read.size) {
            char* newm;
            LOG_DEBUG(c, "%u: Need to grow buffer from %lu to %lu",
                      c->getId(), (unsigned long)c->read.size,
                      (unsigned long)nsize);
            newm = reinterpret_cast<char*>(cb_realloc(c->read.buf, nsize));
            if (newm == NULL) {
                LOG_WARNING(c, "%u: Failed to grow buffer.. closing connection",
                            c->getId());
                c->setState(conn_closing);
                return;
            }

            c->read.buf = newm;
            /* rcurr should point to the same offset in the packet */
            c->read.curr =
                c->read.buf + offset - sizeof(protocol_binary_request_header);
            c->read.size = (int)nsize;
        }
        if (c->read.buf != c->read.curr) {
            memmove(c->read.buf, c->read.curr, c->read.bytes);
            c->read.curr = c->read.buf;
            LOG_DEBUG(c, "%u: Repack input buffer", c->getId());
        }
    }

    /* preserve the header in the buffer.. */
    c->setRitem(c->read.curr + sizeof(protocol_binary_request_header));
    c->setState(conn_nread);
}

/* Just write an error message and disconnect the client */
static void handle_binary_protocol_error(McbpConnection* c) {
    mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_EINVAL);
    LOG_NOTICE(c, "%u: Protocol error (opcode %02x), close connection",
               c->getId(), c->binary_header.request.opcode);
    c->setWriteAndGo(conn_closing);
}

/**
 * Triggers topkeys_update (i.e., increments topkeys stats) if called by a
 * valid operation.
 */
void update_topkeys(const DocKey& key, McbpConnection* c) {

    if (topkey_commands[c->binary_header.request.opcode]) {
        if (all_buckets[c->getBucketIndex()].topkeys != nullptr) {
            all_buckets[c->getBucketIndex()].topkeys->updateKey(key.data(),
                                                                key.size(),
                                                                mc_time_get_current_time());
        }
    }
}

static void process_bin_get(McbpConnection* c, void* packet) {
    auto* req = reinterpret_cast<protocol_binary_request_get*>(packet);
    c->obtainContext<GetCommandContext>(*c, req).drive();
}

static void get_locked_executor(McbpConnection* c, void* packet) {
    auto* req = reinterpret_cast<protocol_binary_request_getl*>(packet);
    c->obtainContext<GetLockedCommandContext>(*c, req).drive();
}

static void unlock_executor(McbpConnection* c, void* packet) {
    auto* req = reinterpret_cast<protocol_binary_request_no_extras*>(packet);
    c->obtainContext<UnlockCommandContext>(*c, req).drive();
}

static ENGINE_ERROR_CODE default_unknown_command(
    EXTENSION_BINARY_PROTOCOL_DESCRIPTOR*,
    ENGINE_HANDLE*,
    const void* void_cookie,
    protocol_binary_request_header* request,
    ADD_RESPONSE response) {

    auto* cookie = reinterpret_cast<const Cookie*>(void_cookie);
    if (cookie->connection == nullptr) {
        throw std::logic_error("default_unknown_command: connection can't be null");
    }
    // Using dynamic cast to ensure a coredump when we implement this for
    // Greenstack and fix it
    auto* c = dynamic_cast<McbpConnection*>(cookie->connection);
    return bucket_unknown_command(c, request, response);
}

struct request_lookup {
    EXTENSION_BINARY_PROTOCOL_DESCRIPTOR* descriptor;
    BINARY_COMMAND_CALLBACK callback;
};

static struct request_lookup request_handlers[0x100];

typedef void (* RESPONSE_HANDLER)(McbpConnection*);

/**
 * A map between the response packets op-code and the function to handle
 * the response message.
 */
static std::array<RESPONSE_HANDLER, 0x100> response_handlers;

void setup_mcbp_lookup_cmd(
    EXTENSION_BINARY_PROTOCOL_DESCRIPTOR* descriptor,
    uint8_t cmd,
    BINARY_COMMAND_CALLBACK new_handler) {
    request_handlers[cmd].descriptor = descriptor;
    request_handlers[cmd].callback = new_handler;
}

static void process_bin_unknown_packet(McbpConnection* c) {
    char* packet = c->read.curr -
                   (c->binary_header.request.bodylen +
                    sizeof(c->binary_header));

    auto* req = reinterpret_cast<protocol_binary_request_header*>(packet);
    ENGINE_ERROR_CODE ret = c->getAiostat();
    c->setAiostat(ENGINE_SUCCESS);
    c->setEwouldblock(false);

    if (ret == ENGINE_SUCCESS) {
        struct request_lookup* rq =
            request_handlers + c->binary_header.request.opcode;
        ret = rq->callback(rq->descriptor,
                           c->getBucketEngineAsV0(), c->getCookie(), req,
                           mcbp_response_handler);
    }


    switch (ret) {
    case ENGINE_SUCCESS: {
        if (c->getDynamicBuffer().getRoot() != nullptr) {
            mcbp_write_and_free(c, &c->getDynamicBuffer());
        } else {
            c->setState(conn_new_cmd);
        }
        update_topkeys(DocKey(reinterpret_cast<uint8_t*>(packet +
                              sizeof(c->binary_header.request) +
                              c->binary_header.request.extlen),
                              c->binary_header.request.keylen,
                              DocNamespace::DefaultCollection), c);
        break;
    }
    case ENGINE_EWOULDBLOCK:
        c->setEwouldblock(true);
        break;
    case ENGINE_DISCONNECT:
        c->setState(conn_closing);
        break;
    default:
        /* Release the dynamic buffer.. it may be partial.. */
        c->clearDynamicBuffer();
        mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ret));
    }
}

static void process_bin_tap_connect(McbpConnection* c) {
    TAP_ITERATOR iterator;
    char* packet = (c->read.curr - (c->binary_header.request.bodylen +
                                    sizeof(c->binary_header)));
    auto* req = reinterpret_cast<protocol_binary_request_tap_connect*>(packet);
    const char* key = packet + sizeof(req->bytes);
    const char* data = key + c->binary_header.request.keylen;
    uint32_t flags = 0;
    size_t ndata = c->binary_header.request.bodylen -
                   c->binary_header.request.extlen -
                   c->binary_header.request.keylen;

    if (c->binary_header.request.extlen == 4) {
        flags = ntohl(req->message.body.flags);

        if (flags & TAP_CONNECT_FLAG_BACKFILL) {
            /* the userdata has to be at least 8 bytes! */
            if (ndata < 8) {
                LOG_WARNING(c, "%u: ERROR: Invalid tap connect message",
                            c->getId());
                c->setState(conn_closing);
                return;
            }
        }
    } else {
        data -= 4;
        key -= 4;
    }

    if (settings.getVerbose() && c->binary_header.request.keylen > 0) {
        char buffer[1024];
        size_t len = c->binary_header.request.keylen;
        if (len >= sizeof(buffer)) {
            len = sizeof(buffer) - 1;
        }
        memcpy(buffer, key, len);
        buffer[len] = '\0';
        LOG_DEBUG(c, "%u: Trying to connect with named tap connection: <%s>",
                  c->getId(), buffer);
    }

    iterator = c->getBucketEngine()->get_tap_iterator(c->getBucketEngineAsV0(),
                                                      c->getCookie(), key,
                                                      c->binary_header.request.keylen,
                                                      flags, data, ndata);

    if (iterator == NULL) {
        LOG_WARNING(c, "%u: FATAL: The engine does not support tap",
                    c->getId());
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
        c->setWriteAndGo(conn_closing);
    } else {
        c->setPriority(Connection::Priority::High);
        c->setTapIterator(iterator);
        c->setCurrentEvent(EV_WRITE);
        c->setState(conn_ship_log);
    }
}

static void process_bin_tap_packet(tap_event_t event, McbpConnection* c) {
    char* packet;
    uint16_t nengine;
    uint16_t tap_flags;
    uint32_t seqno;
    uint8_t ttl;
    char* engine_specific;
    char* key;
    uint16_t nkey;
    char* data;
    uint32_t flags;
    uint32_t exptime;
    uint32_t ndata;
    ENGINE_ERROR_CODE ret;

    packet = (c->read.curr - (c->binary_header.request.bodylen +
                              sizeof(c->binary_header)));
    auto* tap = reinterpret_cast<protocol_binary_request_tap_no_extras*>(packet);
    nengine = ntohs(tap->message.body.tap.enginespecific_length);
    tap_flags = ntohs(tap->message.body.tap.flags);
    seqno = ntohl(tap->message.header.request.opaque);
    ttl = tap->message.body.tap.ttl;
    engine_specific = packet + sizeof(tap->bytes);
    key = engine_specific + nengine;
    nkey = c->binary_header.request.keylen;
    data = key + nkey;
    flags = 0;
    exptime = 0;
    ndata = c->binary_header.request.bodylen - nengine - nkey - 8;
    ret = c->getAiostat();

    if (ttl == 0) {
        ret = ENGINE_EINVAL;
    } else {
        if (event == TAP_MUTATION || event == TAP_CHECKPOINT_START ||
            event == TAP_CHECKPOINT_END) {
            auto* mutation =
                reinterpret_cast<protocol_binary_request_tap_mutation*>(tap);

            /* engine_specific data in protocol_binary_request_tap_mutation is */
            /* at a different offset than protocol_binary_request_tap_no_extras */
            engine_specific = packet + sizeof(mutation->bytes);

            flags = mutation->message.body.item.flags;
            if ((tap_flags & TAP_FLAG_NETWORK_BYTE_ORDER) == 0) {
                flags = ntohl(flags);
            }

            exptime = ntohl(mutation->message.body.item.expiration);
            key += 8;
            data += 8;
            ndata -= 8;
        }

        if (ret == ENGINE_SUCCESS) {
            uint8_t datatype = c->binary_header.request.datatype;
            if (event == TAP_MUTATION && !c->isSupportsDatatype()) {
                auto* validator = c->getThread()->validator;
                try {
                    if (validator->validate(reinterpret_cast<uint8_t*>(data),
                                            ndata)) {
                        datatype = PROTOCOL_BINARY_DATATYPE_JSON;
                    }
                } catch (std::bad_alloc&) {
                    // @todo send ENOMEM
                    c->setState(conn_closing);
                    return;
                }
            }

            ret = c->getBucketEngine()->tap_notify(c->getBucketEngineAsV0(),
                                                   c->getCookie(),
                                                   engine_specific, nengine,
                                                   ttl - 1, tap_flags,
                                                   event, seqno,
                                                   key, nkey,
                                                   flags, exptime,
                                                   ntohll(
                                                       tap->message.header.request.cas),
                                                   datatype,
                                                   data, ndata,
                                                   c->binary_header.request.vbucket);
        }
    }

    switch (ret) {
    case ENGINE_DISCONNECT:
        c->setState(conn_closing);
        break;
    case ENGINE_EWOULDBLOCK:
        c->setEwouldblock(true);
        break;
    default:
        if ((tap_flags & TAP_FLAG_ACK) || (ret != ENGINE_SUCCESS)) {
            mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ret));
        } else {
            c->setState(conn_new_cmd);
        }
    }
}

static void process_bin_tap_ack(McbpConnection* c) {
    char* packet;
    uint32_t seqno;
    uint16_t status;
    char* key;
    ENGINE_ERROR_CODE ret = ENGINE_DISCONNECT;

    packet = (c->read.curr -
              (c->binary_header.request.bodylen + sizeof(c->binary_header)));
    auto* rsp = reinterpret_cast<protocol_binary_response_no_extras*>(packet);
    seqno = ntohl(rsp->message.header.response.opaque);
    status = ntohs(rsp->message.header.response.status);
    key = packet + sizeof(rsp->bytes);

    if (c->getBucketEngine()->tap_notify != NULL) {
        ret = c->getBucketEngine()->tap_notify(c->getBucketEngineAsV0(),
                                               c->getCookie(),
                                               NULL, 0,
                                               0, status,
                                               TAP_ACK, seqno, key,
                                               c->binary_header.request.keylen,
                                               0, 0,
                                               0,
                                               c->binary_header.request.datatype,
                                               NULL,
                                               0, 0);
    }

    if (ret == ENGINE_DISCONNECT) {
        c->setState(conn_closing);
    } else {
        c->setState(conn_ship_log);
    }
}

/**
 * We received a noop response.. just ignore it
 */
static void process_bin_noop_response(McbpConnection* c) {
    c->setState(conn_new_cmd);
}

/*******************************************************************************
 **                             DCP MESSAGE PRODUCERS                         **
 ******************************************************************************/

static ENGINE_ERROR_CODE dcp_message_get_failover_log(const void* void_cookie,
                                                      uint32_t opaque,
                                                      uint16_t vbucket) {
    auto* c = cookie2mcbp(void_cookie, __func__);

    c->setCmd(PROTOCOL_BINARY_CMD_DCP_GET_FAILOVER_LOG);

    protocol_binary_request_dcp_get_failover_log packet;
    if (c->write.bytes + sizeof(packet.bytes) >= c->write.size) {
        /* We don't have room in the buffer */
        return ENGINE_E2BIG;
    }

    memset(packet.bytes, 0, sizeof(packet.bytes));
    packet.message.header.request.magic = (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode = (uint8_t)PROTOCOL_BINARY_CMD_DCP_GET_FAILOVER_LOG;
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.vbucket = htons(vbucket);

    memcpy(c->write.curr, packet.bytes, sizeof(packet.bytes));
    c->addIov(c->write.curr, sizeof(packet.bytes));
    c->write.curr += sizeof(packet.bytes);
    c->write.bytes += sizeof(packet.bytes);

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE dcp_message_stream_req(const void* void_cookie,
                                                uint32_t opaque,
                                                uint16_t vbucket,
                                                uint32_t flags,
                                                uint64_t start_seqno,
                                                uint64_t end_seqno,
                                                uint64_t vbucket_uuid,
                                                uint64_t snap_start_seqno,
                                                uint64_t snap_end_seqno) {
    auto* c = cookie2mcbp(void_cookie, __func__);
    c->setCmd(PROTOCOL_BINARY_CMD_DCP_STREAM_REQ);

    protocol_binary_request_dcp_stream_req packet;
    if (c->write.bytes + sizeof(packet.bytes) >= c->write.size) {
        /* We don't have room in the buffer */
        return ENGINE_E2BIG;
    }

    memset(packet.bytes, 0, sizeof(packet.bytes));
    packet.message.header.request.magic = (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode = (uint8_t)PROTOCOL_BINARY_CMD_DCP_STREAM_REQ;
    packet.message.header.request.extlen = 48;
    packet.message.header.request.bodylen = htonl(48);
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.vbucket = htons(vbucket);

    packet.message.body.flags = ntohl(flags);
    packet.message.body.start_seqno = ntohll(start_seqno);
    packet.message.body.end_seqno = ntohll(end_seqno);
    packet.message.body.vbucket_uuid = ntohll(vbucket_uuid);
    packet.message.body.snap_start_seqno = ntohll(snap_start_seqno);
    packet.message.body.snap_end_seqno = ntohll(snap_end_seqno);

    memcpy(c->write.curr, packet.bytes, sizeof(packet.bytes));
    c->addIov(c->write.curr, sizeof(packet.bytes));
    c->write.curr += sizeof(packet.bytes);
    c->write.bytes += sizeof(packet.bytes);

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE dcp_message_add_stream_response(const void* void_cookie,
                                                         uint32_t opaque,
                                                         uint32_t dialogopaque,
                                                         uint8_t status) {
    auto* c = cookie2mcbp(void_cookie, __func__);

    c->setCmd(PROTOCOL_BINARY_CMD_DCP_ADD_STREAM);
    protocol_binary_response_dcp_add_stream packet;
    if (c->write.bytes + sizeof(packet.bytes) >= c->write.size) {
        /* We don't have room in the buffer */
        return ENGINE_E2BIG;
    }

    memset(packet.bytes, 0, sizeof(packet.bytes));
    packet.message.header.response.magic = (uint8_t)PROTOCOL_BINARY_RES;
    packet.message.header.response.opcode = (uint8_t)PROTOCOL_BINARY_CMD_DCP_ADD_STREAM;
    packet.message.header.response.extlen = 4;
    packet.message.header.response.status = htons(status);
    packet.message.header.response.bodylen = htonl(4);
    packet.message.header.response.opaque = opaque;
    packet.message.body.opaque = ntohl(dialogopaque);

    memcpy(c->write.curr, packet.bytes, sizeof(packet.bytes));
    c->addIov(c->write.curr, sizeof(packet.bytes));
    c->write.curr += sizeof(packet.bytes);
    c->write.bytes += sizeof(packet.bytes);

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE dcp_message_marker_response(const void* void_cookie,
                                                     uint32_t opaque,
                                                     uint8_t status) {
    auto* c = cookie2mcbp(void_cookie, __func__);

    c->setCmd(PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER);
    protocol_binary_response_dcp_snapshot_marker packet;
    if (c->write.bytes + sizeof(packet.bytes) >= c->write.size) {
        /* We don't have room in the buffer */
        return ENGINE_E2BIG;
    }

    memset(packet.bytes, 0, sizeof(packet.bytes));
    packet.message.header.response.magic = (uint8_t)PROTOCOL_BINARY_RES;
    packet.message.header.response.opcode = (uint8_t)PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER;
    packet.message.header.response.extlen = 0;
    packet.message.header.response.status = htons(status);
    packet.message.header.response.bodylen = 0;
    packet.message.header.response.opaque = opaque;

    memcpy(c->write.curr, packet.bytes, sizeof(packet.bytes));
    c->addIov(c->write.curr, sizeof(packet.bytes));
    c->write.curr += sizeof(packet.bytes);
    c->write.bytes += sizeof(packet.bytes);

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE dcp_message_set_vbucket_state_response(
    const void* void_cookie,
    uint32_t opaque,
    uint8_t status) {

    auto* c = cookie2mcbp(void_cookie, __func__);

    c->setCmd(PROTOCOL_BINARY_CMD_DCP_SET_VBUCKET_STATE);
    protocol_binary_response_dcp_set_vbucket_state packet;
    if (c->write.bytes + sizeof(packet.bytes) >= c->write.size) {
        /* We don't have room in the buffer */
        return ENGINE_E2BIG;
    }

    memset(packet.bytes, 0, sizeof(packet.bytes));
    packet.message.header.response.magic = (uint8_t)PROTOCOL_BINARY_RES;
    packet.message.header.response.opcode = (uint8_t)PROTOCOL_BINARY_CMD_DCP_SET_VBUCKET_STATE;
    packet.message.header.response.extlen = 0;
    packet.message.header.response.status = htons(status);
    packet.message.header.response.bodylen = 0;
    packet.message.header.response.opaque = opaque;

    memcpy(c->write.curr, packet.bytes, sizeof(packet.bytes));
    c->addIov(c->write.curr, sizeof(packet.bytes));
    c->write.curr += sizeof(packet.bytes);
    c->write.bytes += sizeof(packet.bytes);

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE dcp_message_stream_end(const void* void_cookie,
                                                uint32_t opaque,
                                                uint16_t vbucket,
                                                uint32_t flags) {
    auto* c = cookie2mcbp(void_cookie, __func__);

    c->setCmd(PROTOCOL_BINARY_CMD_DCP_STREAM_END);
    protocol_binary_request_dcp_stream_end packet;
    if (c->write.bytes + sizeof(packet.bytes) >= c->write.size) {
        /* We don't have room in the buffer */
        return ENGINE_E2BIG;
    }

    memset(packet.bytes, 0, sizeof(packet.bytes));
    packet.message.header.request.magic = (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode = (uint8_t)PROTOCOL_BINARY_CMD_DCP_STREAM_END;
    packet.message.header.request.extlen = 4;
    packet.message.header.request.bodylen = htonl(4);
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.vbucket = htons(vbucket);
    packet.message.body.flags = ntohl(flags);

    memcpy(c->write.curr, packet.bytes, sizeof(packet.bytes));
    c->addIov(c->write.curr, sizeof(packet.bytes));
    c->write.curr += sizeof(packet.bytes);
    c->write.bytes += sizeof(packet.bytes);

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE dcp_message_marker(const void* void_cookie,
                                            uint32_t opaque,
                                            uint16_t vbucket,
                                            uint64_t start_seqno,
                                            uint64_t end_seqno,
                                            uint32_t flags) {
    auto* c = cookie2mcbp(void_cookie, __func__);

    c->setCmd(PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER);
    protocol_binary_request_dcp_snapshot_marker packet;
    if (c->write.bytes + sizeof(packet.bytes) >= c->write.size) {
        /* We don't have room in the buffer */
        return ENGINE_E2BIG;
    }

    memset(packet.bytes, 0, sizeof(packet.bytes));
    packet.message.header.request.magic = (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode = (uint8_t)PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER;
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.vbucket = htons(vbucket);
    packet.message.header.request.extlen = 20;
    packet.message.header.request.bodylen = htonl(20);
    packet.message.body.start_seqno = htonll(start_seqno);
    packet.message.body.end_seqno = htonll(end_seqno);
    packet.message.body.flags = htonl(flags);

    memcpy(c->write.curr, packet.bytes, sizeof(packet.bytes));
    c->addIov(c->write.curr, sizeof(packet.bytes));
    c->write.curr += sizeof(packet.bytes);
    c->write.bytes += sizeof(packet.bytes);

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE dcp_message_flush(const void* void_cookie,
                                           uint32_t opaque,
                                           uint16_t vbucket) {
    auto* c = cookie2mcbp(void_cookie, __func__);
    c->setCmd(PROTOCOL_BINARY_CMD_DCP_FLUSH);
    protocol_binary_request_dcp_flush packet;
    if (c->write.bytes + sizeof(packet.bytes) >= c->write.size) {
        /* We don't have room in the buffer */
        return ENGINE_E2BIG;
    }

    memset(packet.bytes, 0, sizeof(packet.bytes));
    packet.message.header.request.magic = (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode = (uint8_t)PROTOCOL_BINARY_CMD_DCP_FLUSH;
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.vbucket = htons(vbucket);

    memcpy(c->write.curr, packet.bytes, sizeof(packet.bytes));
    c->addIov(c->write.curr, sizeof(packet.bytes));
    c->write.curr += sizeof(packet.bytes);
    c->write.bytes += sizeof(packet.bytes);

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE dcp_message_set_vbucket_state(const void* void_cookie,
                                                       uint32_t opaque,
                                                       uint16_t vbucket,
                                                       vbucket_state_t state) {
    auto* c = cookie2mcbp(void_cookie, __func__);
    c->setCmd(PROTOCOL_BINARY_CMD_DCP_SET_VBUCKET_STATE);
    protocol_binary_request_dcp_set_vbucket_state packet;
    if (c->write.bytes + sizeof(packet.bytes) >= c->write.size) {
        /* We don't have room in the buffer */
        return ENGINE_E2BIG;
    }

    if (!is_valid_vbucket_state_t(state)) {
        return ENGINE_EINVAL;
    }

    memset(packet.bytes, 0, sizeof(packet.bytes));
    packet.message.header.request.magic = (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode = (uint8_t)PROTOCOL_BINARY_CMD_DCP_SET_VBUCKET_STATE;
    packet.message.header.request.extlen = 1;
    packet.message.header.request.bodylen = htonl(1);
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.vbucket = htons(vbucket);
    packet.message.body.state = uint8_t(state);

    memcpy(c->write.curr, packet.bytes, sizeof(packet.bytes));
    c->addIov(c->write.curr, sizeof(packet.bytes));
    c->write.curr += sizeof(packet.bytes);
    c->write.bytes += sizeof(packet.bytes);

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE dcp_message_noop(const void* void_cookie,
                                          uint32_t opaque) {
    auto* c = cookie2mcbp(void_cookie, __func__);
    c->setCmd(PROTOCOL_BINARY_CMD_DCP_NOOP);
    protocol_binary_request_dcp_noop packet;
    if (c->write.bytes + sizeof(packet.bytes) >= c->write.size) {
        /* We don't have room in the buffer */
        return ENGINE_E2BIG;
    }

    memset(packet.bytes, 0, sizeof(packet.bytes));
    packet.message.header.request.magic = (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode = (uint8_t)PROTOCOL_BINARY_CMD_DCP_NOOP;
    packet.message.header.request.opaque = opaque;

    memcpy(c->write.curr, packet.bytes, sizeof(packet.bytes));
    c->addIov(c->write.curr, sizeof(packet.bytes));
    c->write.curr += sizeof(packet.bytes);
    c->write.bytes += sizeof(packet.bytes);

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE dcp_message_buffer_acknowledgement(const void* void_cookie,
                                                            uint32_t opaque,
                                                            uint16_t vbucket,
                                                            uint32_t buffer_bytes) {
    auto* c = cookie2mcbp(void_cookie, __func__);
    c->setCmd(PROTOCOL_BINARY_CMD_DCP_BUFFER_ACKNOWLEDGEMENT);
    protocol_binary_request_dcp_buffer_acknowledgement packet;
    if (c->write.bytes + sizeof(packet.bytes) >= c->write.size) {
        /* We don't have room in the buffer */
        return ENGINE_E2BIG;
    }

    memset(packet.bytes, 0, sizeof(packet.bytes));
    packet.message.header.request.magic = (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode = (uint8_t)PROTOCOL_BINARY_CMD_DCP_BUFFER_ACKNOWLEDGEMENT;
    packet.message.header.request.extlen = 4;
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.vbucket = htons(vbucket);
    packet.message.header.request.bodylen = ntohl(4);
    packet.message.body.buffer_bytes = ntohl(buffer_bytes);

    memcpy(c->write.curr, packet.bytes, sizeof(packet.bytes));
    c->addIov(c->write.curr, sizeof(packet.bytes));
    c->write.curr += sizeof(packet.bytes);
    c->write.bytes += sizeof(packet.bytes);

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE dcp_message_control(const void* void_cookie,
                                             uint32_t opaque,
                                             const void* key,
                                             uint16_t nkey,
                                             const void* value,
                                             uint32_t nvalue) {
    auto* c = cookie2mcbp(void_cookie, __func__);
    c->setCmd(PROTOCOL_BINARY_CMD_DCP_CONTROL);
    protocol_binary_request_dcp_control packet;
    if (c->write.bytes + sizeof(packet.bytes) + nkey + nvalue >=
        c->write.size) {
        /* We don't have room in the buffer */
        return ENGINE_E2BIG;
    }

    memset(packet.bytes, 0, sizeof(packet.bytes));
    packet.message.header.request.magic = (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode = (uint8_t)PROTOCOL_BINARY_CMD_DCP_CONTROL;
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.keylen = ntohs(nkey);
    packet.message.header.request.bodylen = ntohl(nvalue + nkey);

    c->addIov(c->write.curr, sizeof(packet.bytes) + nkey + nvalue);
    memcpy(c->write.curr, packet.bytes, sizeof(packet.bytes));
    c->write.curr += sizeof(packet.bytes);
    c->write.bytes += sizeof(packet.bytes);

    memcpy(c->write.curr, key, nkey);
    c->write.curr += nkey;
    c->write.bytes += nkey;

    memcpy(c->write.curr, value, nvalue);
    c->write.curr += nvalue;
    c->write.bytes += nvalue;

    return ENGINE_SUCCESS;
}

void ship_mcbp_dcp_log(McbpConnection* c) {
    static struct dcp_message_producers producers = {
        dcp_message_get_failover_log,
        dcp_message_stream_req,
        dcp_message_add_stream_response,
        dcp_message_marker_response,
        dcp_message_set_vbucket_state_response,
        dcp_message_stream_end,
        dcp_message_marker,
        dcp_message_mutation,
        dcp_message_deletion,
        dcp_message_expiration,
        dcp_message_flush,
        dcp_message_set_vbucket_state,
        dcp_message_noop,
        dcp_message_buffer_acknowledgement,
        dcp_message_control
    };
    ENGINE_ERROR_CODE ret;

    // Begin timing DCP, each dcp callback needs to set the c->cmd for the timing
    // to be recorded.
    c->setStart(gethrtime());

    c->addMsgHdr(true);
    c->write.bytes = 0;
    c->write.curr = c->write.buf;
    c->setEwouldblock(false);
    ret = c->getBucketEngine()->dcp.step(c->getBucketEngineAsV0(), c->getCookie(),
                                         &producers);
    if (ret == ENGINE_SUCCESS) {
        /* the engine don't have more data to send at this moment */
        c->setEwouldblock(true);
    } else if (ret == ENGINE_WANT_MORE) {
        /* The engine got more data it wants to send */
        ret = ENGINE_SUCCESS;
        c->setState(conn_mwrite);
        c->setWriteAndGo(conn_ship_log);
    }

    if (ret != ENGINE_SUCCESS) {
        c->setState(conn_closing);
    }
}

/******************************************************************************
 *                        TAP packet executors                                *
 ******************************************************************************/
static void tap_connect_executor(McbpConnection* c, void* packet) {
    (void)packet;
    if (c->getBucketEngine()->get_tap_iterator == NULL) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        tap_stats.received.connect++;
        c->setState(conn_setup_tap_stream);
    }
}

static void tap_mutation_executor(McbpConnection* c, void* packet) {
    (void)packet;
    if (c->getBucketEngine()->tap_notify == NULL) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        tap_stats.received.mutation++;
        process_bin_tap_packet(TAP_MUTATION, c);
    }
}

static void tap_delete_executor(McbpConnection* c, void* packet) {
    (void)packet;
    if (c->getBucketEngine()->tap_notify == NULL) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        tap_stats.received.del++;
        process_bin_tap_packet(TAP_DELETION, c);
    }
}

static void tap_flush_executor(McbpConnection* c, void* packet) {
    (void)packet;
    if (c->getBucketEngine()->tap_notify == NULL) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        tap_stats.received.flush++;
        process_bin_tap_packet(TAP_FLUSH, c);
    }
}

static void tap_opaque_executor(McbpConnection* c, void* packet) {
    (void)packet;
    if (c->getBucketEngine()->tap_notify == NULL) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        tap_stats.received.opaque++;
        process_bin_tap_packet(TAP_OPAQUE, c);
    }
}

static void tap_vbucket_set_executor(McbpConnection* c, void* packet) {
    (void)packet;
    if (c->getBucketEngine()->tap_notify == NULL) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        tap_stats.received.vbucket_set++;
        process_bin_tap_packet(TAP_VBUCKET_SET, c);
    }
}

static void tap_checkpoint_start_executor(McbpConnection* c, void* packet) {
    (void)packet;
    if (c->getBucketEngine()->tap_notify == NULL) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        tap_stats.received.checkpoint_start++;
        process_bin_tap_packet(TAP_CHECKPOINT_START, c);
    }
}

static void tap_checkpoint_end_executor(McbpConnection* c, void* packet) {
    (void)packet;
    if (c->getBucketEngine()->tap_notify == NULL) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
    } else {
        tap_stats.received.checkpoint_end++;
        process_bin_tap_packet(TAP_CHECKPOINT_END, c);
    }
}

static void add_set_replace_executor(McbpConnection* c, void* packet,
                                     ENGINE_STORE_OPERATION store_op) {
    auto* req = reinterpret_cast<protocol_binary_request_set*>(packet);
    c->obtainContext<MutationCommandContext>(*c, req, store_op).drive();
}


static void add_executor(McbpConnection* c, void* packet) {
    c->setNoReply(false);
    add_set_replace_executor(c, packet, OPERATION_ADD);
}

static void addq_executor(McbpConnection* c, void* packet) {
    c->setNoReply(true);
    add_set_replace_executor(c, packet, OPERATION_ADD);
}

static void set_executor(McbpConnection* c, void* packet) {
    c->setNoReply(false);
    add_set_replace_executor(c, packet, OPERATION_SET);
}

static void setq_executor(McbpConnection* c, void* packet) {
    c->setNoReply(true);
    add_set_replace_executor(c, packet, OPERATION_SET);
}

static void replace_executor(McbpConnection* c, void* packet) {
    c->setNoReply(false);
    add_set_replace_executor(c, packet, OPERATION_REPLACE);
}

static void replaceq_executor(McbpConnection* c, void* packet) {
    c->setNoReply(true);
    add_set_replace_executor(c, packet, OPERATION_REPLACE);
}

static void append_prepend_executor(McbpConnection* c,
                                    void* packet,
                                    const AppendPrependCommandContext::Mode mode) {
    auto* req = reinterpret_cast<protocol_binary_request_append*>(packet);
    c->obtainContext<AppendPrependCommandContext>(*c, req, mode).drive();
}

static void append_executor(McbpConnection* c, void* packet) {
    c->setNoReply(false);
    append_prepend_executor(c, packet,
                            AppendPrependCommandContext::Mode::Append);
}

static void appendq_executor(McbpConnection* c, void* packet) {
    c->setNoReply(true);
    append_prepend_executor(c, packet,
                            AppendPrependCommandContext::Mode::Append);
}

static void prepend_executor(McbpConnection* c, void* packet) {
    c->setNoReply(false);
    append_prepend_executor(c, packet,
                            AppendPrependCommandContext::Mode::Prepend);
}

static void prependq_executor(McbpConnection* c, void* packet) {
    c->setNoReply(true);
    append_prepend_executor(c, packet,
                            AppendPrependCommandContext::Mode::Prepend);
}


static void get_executor(McbpConnection* c, void* packet) {
    switch (c->getCmd()) {
    case PROTOCOL_BINARY_CMD_GETQ:
        c->setNoReply(true);
        break;
    case PROTOCOL_BINARY_CMD_GET:
        c->setNoReply(false);
        break;
    case PROTOCOL_BINARY_CMD_GETKQ:
        c->setNoReply(true);
        break;
    case PROTOCOL_BINARY_CMD_GETK:
        c->setNoReply(false);
        break;
    default:
        LOG_WARNING(c,
                    "%u: get_executor: cmd (which is %d) is not a valid GET "
                        "variant - closing connection", c->getCmd());
        c->setState(conn_closing);
        return;
    }

    process_bin_get(c, packet);
}


static void stat_executor(McbpConnection* c, void* packet) {
    auto* req = reinterpret_cast<protocol_binary_request_stats*>(packet);
    c->obtainContext<StatsCommandContext>(*c, *req).drive();
}

static void isasl_refresh_executor(McbpConnection* c, void* packet) {
    ENGINE_ERROR_CODE ret = c->getAiostat();
    (void)packet;

    c->setAiostat(ENGINE_SUCCESS);
    c->setEwouldblock(false);

    if (ret == ENGINE_SUCCESS) {
        ret = refresh_cbsasl(c);
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
        break;
    case ENGINE_EWOULDBLOCK:
        c->setEwouldblock(true);
        c->setState(conn_refresh_cbsasl);
        break;
    case ENGINE_DISCONNECT:
        c->setState(conn_closing);
        break;
    default:
        mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ret));
    }
}

static void ssl_certs_refresh_executor(McbpConnection* c, void* packet) {
    ENGINE_ERROR_CODE ret = c->getAiostat();
    (void)packet;

    c->setAiostat(ENGINE_SUCCESS);
    c->setEwouldblock(false);

    if (ret == ENGINE_SUCCESS) {
        ret = refresh_ssl_certs(c);
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
        break;
    case ENGINE_EWOULDBLOCK:
        c->setEwouldblock(true);
        c->setState(conn_refresh_ssl_certs);
        break;
    case ENGINE_DISCONNECT:
        c->setState(conn_closing);
        break;
    default:
        mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ret));
    }
}

static void verbosity_executor(McbpConnection* c, void* packet) {
    auto* req = reinterpret_cast<protocol_binary_request_verbosity*>(packet);
    uint32_t level = (uint32_t)ntohl(req->message.body.level);
    if (level > MAX_VERBOSITY_LEVEL) {
        level = MAX_VERBOSITY_LEVEL;
    }
    settings.setVerbose(static_cast<int>(level));
    perform_callbacks(ON_LOG_LEVEL, NULL, NULL);
    mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
}


static void process_hello_packet_executor(McbpConnection* c, void* packet) {
    auto* req = reinterpret_cast<protocol_binary_request_hello*>(packet);
    std::string log_buffer;
    log_buffer.reserve(512);
    log_buffer.append("HELO ");

    const cb::const_char_buffer key{
        reinterpret_cast<const char*>(req->bytes + sizeof(req->bytes)),
        ntohs(req->message.header.request.keylen)};

    const cb::sized_buffer<const uint16_t> input{
        reinterpret_cast<const uint16_t*>(key.data() + key.size()),
        (ntohl(req->message.header.request.bodylen) - key.size()) / 2};

    std::vector<uint16_t> out;
    bool tcpdelay_handled = false;

    /*
     * Disable all features the hello packet may enable, so that
     * the client can toggle features on/off during a connection
     */
    c->setSupportsDatatype(false);
    c->setSupportsMutationExtras(false);
    c->setXattrSupport(false);
    c->setXerrorSupport(false);

    if (!key.empty()) {
        log_buffer.append("[");
        if (key.size() > 256) {
            log_buffer.append(key.data(), 256);
            log_buffer.append("...");
        } else {
            log_buffer.append(key.data(), key.size());
        }
        log_buffer.append("] ");
    }

    for (const auto& value : input) {
        bool added = false;
        const uint16_t in = ntohs(value);
        const auto feature = mcbp::Feature(in);

        switch (feature) {
        case mcbp::Feature::TLS:
            /* Not implemented */
            break;
        case mcbp::Feature::DATATYPE:
            if (settings.isDatatypeSupport() && !c->isSupportsDatatype()) {
                c->setSupportsDatatype(true);
                added = true;
            }
            break;

        case mcbp::Feature::TCPNODELAY:
        case mcbp::Feature::TCPDELAY:
            if (!tcpdelay_handled) {
                c->setTcpNoDelay(feature == mcbp::Feature::TCPNODELAY);
                tcpdelay_handled = true;
                added = true;
            }
            break;

        case mcbp::Feature::MUTATION_SEQNO:
            if (!c->isSupportsMutationExtras()) {
                c->setSupportsMutationExtras(true);
                added = true;
            }
            break;
        case mcbp::Feature::XATTR:
            if ((settings.isXattrEnabled() || c->isInternal()) && !c->isXattrSupport()) {
                c->setXattrSupport(true);
                added = true;
            }
            break;
        case mcbp::Feature::XERROR:
            if (!c->isXerrorSupport()) {
                c->setXerrorSupport(true);
                added = true;
            }
            break;
        }

        if (added) {
            out.push_back(value);
            log_buffer.append(mcbp::to_string(feature));
            log_buffer.append(", ");
        }
    }

    if (out.empty()) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
    } else {
        mcbp_response_handler(nullptr, 0, nullptr, 0,
                              out.data(),
                              uint32_t(2 * out.size()),
                              PROTOCOL_BINARY_RAW_BYTES,
                              PROTOCOL_BINARY_RESPONSE_SUCCESS,
                              0, c->getCookie());
        mcbp_write_and_free(c, &c->getDynamicBuffer());
    }

    // Trim off the trailing whitespace (and potentially comma)
    log_buffer.resize(log_buffer.size() - 1);
    if (log_buffer.back() == ',') {
        log_buffer.resize(log_buffer.size() - 1);
    }

    LOG_NOTICE(c, "%u: %s %s", c->getId(), log_buffer.c_str(),
               c->getDescription().c_str());
}

static void version_executor(McbpConnection* c, void*) {
    mcbp_write_response(c, get_server_version(), 0, 0,
                        (uint32_t)strlen(get_server_version()));
}

static void quit_executor(McbpConnection* c, void*) {
    mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
    c->setWriteAndGo(conn_closing);
}

static void quitq_executor(McbpConnection* c, void*) {
    c->setState(conn_closing);
}

static void sasl_list_mech_executor(McbpConnection* c, void*) {
    if (c->isSslEnabled() && settings.has.ssl_sasl_mechanisms) {
        const auto& mechs = settings.getSslSaslMechanisms();
        mcbp_write_response(c, mechs.data(), 0, 0, mechs.size());
    } else if (!c->isSslEnabled() && settings.has.sasl_mechanisms) {
        const auto& mechs = settings.getSaslMechanisms();
        mcbp_write_response(c, mechs.data(), 0, 0, mechs.size());
    } else {
        /*
         * The administrator did not configure any SASL mechanisms.
         * Go ahead and use whatever we've got in cbsasl
         */
        const char* result_string = NULL;
        unsigned int string_length = 0;

        auto ret = cbsasl_listmech(c->getSaslConn(), nullptr, nullptr, " ",
                                   nullptr, &result_string, &string_length,
                                   nullptr);

        if (ret == CBSASL_OK) {
            mcbp_write_response(c, (char*)result_string, 0, 0, string_length);
        } else {
            /* Perhaps there's a better error for this... */
            LOG_WARNING(c, "%u: Failed to list SASL mechanisms: %s", c->getId(),
                        cbsasl_strerror(c->getSaslConn(), ret));
            mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_AUTH_ERROR);
            return;
        }
    }
}

static void sasl_auth_executor(McbpConnection* c, void* packet) {
    auto* req = reinterpret_cast<protocol_binary_request_no_extras*>(packet);
    int nkey = c->binary_header.request.keylen;
    int vlen = c->binary_header.request.bodylen - nkey;

    const char *ptr = reinterpret_cast<char*>(req->bytes) + sizeof(req->bytes);

    std::string mechanism(ptr, nkey);
    std::string challenge(ptr + nkey, vlen);

    LOG_DEBUG(c, "%u: SASL auth with mech: '%s' with %d bytes of data",
              c->getId(), mechanism.c_str(), vlen);

    std::shared_ptr<Task> task;

    if (c->getCmd() == PROTOCOL_BINARY_CMD_SASL_AUTH) {
        task = std::make_shared<StartSaslAuthTask>(c->getCookieObject(), *c, mechanism, challenge);
    } else {
        task = std::make_shared<StepSaslAuthTask>(c->getCookieObject(), *c, mechanism, challenge);
    }
    c->setCommandContext(new SaslCommandContext(task));

    c->setEwouldblock(true);
    c->setState(conn_sasl_auth);
    std::lock_guard<std::mutex> guard(task->getMutex());
    executorPool->schedule(task, true);
}

static void noop_executor(McbpConnection* c, void*) {
    mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
}

static void flush_executor(McbpConnection* c, void*) {
    ENGINE_ERROR_CODE ret;
    if (c->getCmd() == PROTOCOL_BINARY_CMD_FLUSHQ) {
        c->setNoReply(true);
    }

    LOG_NOTICE(c, "%u: flush b:%s", c->getId(), c->getBucket().name);

    ret = c->getBucketEngine()->flush(c->getBucketEngineAsV0(), c->getCookie());
    ret = c->remapErrorCode(ret);

    switch (ret) {
    case ENGINE_SUCCESS:
        audit_bucket_flush(c, all_buckets[c->getBucketIndex()].name);
        get_thread_stats(c)->cmd_flush++;
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
        break;
    case ENGINE_EWOULDBLOCK:
        c->setEwouldblock(true);
        c->setState(conn_flush);
        break;
    case ENGINE_DISCONNECT:
        c->setState(conn_closing);
        break;
    default:
        mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ret));
    }
}

static void delete_executor(McbpConnection* c, void* packet) {
    if (c->getCmd() == PROTOCOL_BINARY_CMD_DELETEQ) {
        c->setNoReply(true);
    }

    auto* req = reinterpret_cast<protocol_binary_request_delete*>(packet);
    c->obtainContext<RemoveCommandContext>(*c, req).drive();
}

static void arithmetic_executor(McbpConnection* c, void* packet) {
    auto* req = reinterpret_cast<protocol_binary_request_incr*>(packet);
    c->obtainContext<ArithmeticCommandContext>(*c, *req).drive();
}

static void arithmeticq_executor(McbpConnection* c, void* packet) {
    c->setNoReply(true);
    arithmetic_executor(c, packet);
}

static void get_cmd_timer_executor(McbpConnection* c, void* packet) {
    std::string str;
    auto* req = reinterpret_cast<protocol_binary_request_get_cmd_timer*>(packet);
    const char* key = (const char*)(req->bytes + sizeof(req->bytes));
    size_t keylen = ntohs(req->message.header.request.keylen);
    int index = c->getBucketIndex();
    std::string bucket(key, keylen);

    if (bucket == "/all/") {
        index = 0;
        keylen = 0;
    }

    if (keylen == 0) {
        if (index == 0 && !c->isInternal()) {
            // We're not connected to a bucket, and we didn't
            // authenticate to a bucket.. Don't leak the
            // global stats...
            mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_EACCESS);
            return;
        }
        str = all_buckets[index].timings.generate(req->message.body.opcode);
        mcbp_response_handler(NULL, 0, NULL, 0, str.data(),
                              uint32_t(str.length()),
                              PROTOCOL_BINARY_RAW_BYTES,
                              PROTOCOL_BINARY_RESPONSE_SUCCESS,
                              0, c->getCookie());
        mcbp_write_and_free(c, &c->getDynamicBuffer());
    } else if (c->isInternal()) {
        bool found = false;
        for (size_t ii = 1; ii < all_buckets.size() && !found; ++ii) {
            // Need the lock to get the bucket state and name
            cb_mutex_enter(&all_buckets[ii].mutex);
            if ((all_buckets[ii].state == BucketState::Ready) &&
                (bucket == all_buckets[ii].name)) {
                str = all_buckets[ii].timings.generate(
                    req->message.body.opcode);
                found = true;
            }
            cb_mutex_exit(&all_buckets[ii].mutex);
        }
        if (found) {
            mcbp_response_handler(NULL, 0, NULL, 0, str.data(),
                                  uint32_t(str.length()),
                                  PROTOCOL_BINARY_RAW_BYTES,
                                  PROTOCOL_BINARY_RESPONSE_SUCCESS,
                                  0, c->getCookie());
            mcbp_write_and_free(c, &c->getDynamicBuffer());
        } else {
            mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);
        }
    } else {
        // non-privileged connections can't specify bucket
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_EACCESS);
    }
}

static void set_ctrl_token_executor(McbpConnection* c, void* packet) {
    auto* req = reinterpret_cast<protocol_binary_request_set_ctrl_token*>(packet);
    uint64_t casval = ntohll(req->message.header.request.cas);
    uint64_t newval = ntohll(req->message.body.new_cas);
    uint64_t value;

    auto ret = session_cas.cas(newval, casval, value);
    mcbp_response_handler(NULL, 0, NULL, 0, NULL, 0,
                          PROTOCOL_BINARY_RAW_BYTES,
                          engine_error_2_mcbp_protocol_error(ret),
                          value, c->getCookie());

    mcbp_write_and_free(c, &c->getDynamicBuffer());
}

static void get_ctrl_token_executor(McbpConnection* c, void*) {
    mcbp_response_handler(NULL, 0, NULL, 0, NULL, 0,
                          PROTOCOL_BINARY_RAW_BYTES,
                          PROTOCOL_BINARY_RESPONSE_SUCCESS,
                          session_cas.getCasValue(), c->getCookie());
    mcbp_write_and_free(c, &c->getDynamicBuffer());
}

static void init_complete_executor(McbpConnection* c, void* packet) {
    auto* init = reinterpret_cast<protocol_binary_request_init_complete*>(packet);
    uint64_t cas = ntohll(init->message.header.request.cas);;

    if (session_cas.increment_session_counter(cas)) {
        set_server_initialized(true);
        session_cas.decrement_session_counter();
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
        perform_callbacks(ON_INIT_COMPLETE, nullptr, nullptr);
    } else {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS);
    }
}

static void ioctl_get_executor(McbpConnection* c, void* packet) {
    auto* req = reinterpret_cast<protocol_binary_request_ioctl_set*>(packet);

    const char* key_ptr = reinterpret_cast<const char*>(
        req->bytes + sizeof(req->bytes));
    size_t keylen = ntohs(req->message.header.request.keylen);
    const std::string key(key_ptr, keylen);

    std::string value;

    ENGINE_ERROR_CODE status = ioctl_get_property(c, key, value);

    if (status == ENGINE_SUCCESS) {
        try {
            if (mcbp_response_handler(NULL, 0, NULL, 0,
                                      value.data(), value.size(),
                                      PROTOCOL_BINARY_RAW_BYTES,
                                      PROTOCOL_BINARY_RESPONSE_SUCCESS, 0,
                                      c->getCookie())) {
                mcbp_write_and_free(c, &c->getDynamicBuffer());
            } else {
                mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM);
            }
        } catch (std::exception& e) {
            LOG_WARNING(c, "ioctl_get_executor: Failed to format response: %s",
                        e.what());
            mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM);
        }
    } else {
        mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(status));
    }
}

static void ioctl_set_executor(McbpConnection* c, void* packet) {
    auto* req = reinterpret_cast<protocol_binary_request_ioctl_set*>(packet);

    const char* key_ptr = reinterpret_cast<const char*>(
        req->bytes + sizeof(req->bytes));
    size_t keylen = ntohs(req->message.header.request.keylen);
    const std::string key(key_ptr, keylen);

    const char* val_ptr = key_ptr + keylen;
    size_t vallen = ntohl(req->message.header.request.bodylen) - keylen;
    const std::string value(val_ptr, vallen);


    ENGINE_ERROR_CODE status = ioctl_set_property(c, key, value);

    mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(status));
}

static void config_validate_executor(McbpConnection* c, void* packet) {
    const char* val_ptr = NULL;
    cJSON* errors = NULL;
    auto* req = reinterpret_cast<protocol_binary_request_ioctl_set*>(packet);

    size_t keylen = ntohs(req->message.header.request.keylen);
    size_t vallen = ntohl(req->message.header.request.bodylen) - keylen;

    /* Key not yet used, must be zero length. */
    if (keylen != 0) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_EINVAL);
        return;
    }

    /* must have non-zero length config */
    if (vallen == 0 || vallen > CONFIG_VALIDATE_MAX_LENGTH) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_EINVAL);
        return;
    }

    val_ptr = (const char*)(req->bytes + sizeof(req->bytes)) + keylen;

    /* null-terminate value, and convert to integer */
    try {
        std::string val_buffer(val_ptr, vallen);
        errors = cJSON_CreateArray();

        if (validate_proposed_config_changes(val_buffer.c_str(), errors)) {
            mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
        } else {
            /* problem(s). Send the errors back to the client. */
            char* error_string = cJSON_PrintUnformatted(errors);
            if (mcbp_response_handler(NULL, 0, NULL, 0, error_string,
                                      (uint32_t)strlen(error_string),
                                      PROTOCOL_BINARY_RAW_BYTES,
                                      PROTOCOL_BINARY_RESPONSE_EINVAL, 0,
                                      c->getCookie())) {
                mcbp_write_and_free(c, &c->getDynamicBuffer());
            } else {
                mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_ENOMEM);
            }
            cJSON_Free(error_string);
        }

        cJSON_Delete(errors);
    } catch (const std::bad_alloc&) {
        LOG_WARNING(c,
                    "%u: Failed to allocate buffer of size %"
                        PRIu64 " to validate config. Shutting down connection",
                    c->getId(), vallen + 1);
        c->setState(conn_closing);
        return;
    }

}

static void config_reload_executor(McbpConnection* c, void*) {
    // We need to audit that the privilege debug mode changed and
    // in order to do that we need the "connection" object so we can't
    // do this by using the common "changed_listener"-interface.
    bool old_priv_debug = settings.isPrivilegeDebug();
    reload_config_file();
    if (settings.isPrivilegeDebug() != old_priv_debug) {
        audit_set_privilege_debug_mode(c, settings.isPrivilegeDebug());
    }
    mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
}

static void audit_config_reload_executor(McbpConnection* c, void*) {
    if (settings.getAuditFile().empty()) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
    } else {
        if (configure_auditdaemon(get_audit_handle(),
                                  settings.getAuditFile().c_str(),
                                  c->getCookie()) == AUDIT_EWOULDBLOCK) {
            c->setEwouldblock(true);
            c->setState(conn_audit_configuring);
        } else {
            LOG_WARNING(NULL,
                        "configuration of audit daemon failed with config "
                            "file: %s",
                        settings.getAuditFile().c_str());
            mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL);
        }
    }
}

static void audit_put_executor(McbpConnection* c, void* packet) {

    auto* req = reinterpret_cast<const protocol_binary_request_audit_put*>(packet);
    const void* payload = req->bytes + sizeof(req->message.header) +
                          req->message.header.request.extlen;

    const size_t payload_length = ntohl(req->message.header.request.bodylen) -
                                  req->message.header.request.extlen;

    if (put_audit_event(get_audit_handle(),
                        ntohl(req->message.body.id),
                        payload,
                        payload_length) == AUDIT_SUCCESS) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
    } else {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL);
    }
}

/**
 * Override the CreateBucketTask so that we can have our own notification
 * mechanism to kickstart the clients thread
 */
class McbpCreateBucketTask : public Task {
public:
    McbpCreateBucketTask(const std::string& name_,
                         const std::string& config_,
                         const BucketType& type_,
                         McbpConnection& connection_)
        : thread(name_, config_, type_, connection_, this),
          mcbpconnection(connection_) { }

    // start the bucket deletion
    // May throw std::bad_alloc if we're failing to start the thread
    void start() {
        thread.start();
    }

    virtual bool execute() override {
        return true;
    }

    virtual void notifyExecutionComplete() override {
        notify_io_complete(mcbpconnection.getCookie(), thread.getResult());
    }

    CreateBucketThread thread;
    McbpConnection& mcbpconnection;
};

/**
 * The create bucket contains message have the following format:
 *    key: bucket name
 *    body: module\nconfig
 */
static void create_bucket_executor(McbpConnection* c, void* packet) {
    ENGINE_ERROR_CODE ret = c->getAiostat();

    c->setAiostat(ENGINE_SUCCESS);
    c->setEwouldblock(false);

    if (ret == ENGINE_SUCCESS) {
        auto* req = reinterpret_cast<protocol_binary_request_create_bucket*>(packet);
        /* decode packet */
        uint16_t klen = ntohs(req->message.header.request.keylen);
        uint32_t blen = ntohl(req->message.header.request.bodylen);
        blen -= klen;

        try {
            std::string name((char*)(req + 1), klen);
            std::string value((char*)(req + 1) + klen, blen);
            std::string config;

            // Check if (optional) config was included after the value.
            auto marker = value.find('\0');
            if (marker != std::string::npos) {
                config.assign(&value[marker + 1]);
            }

            std::string errors;
            BucketType type = module_to_bucket_type(value.c_str());
            std::shared_ptr<Task> task = std::make_shared<McbpCreateBucketTask>(
                name, config, type, *c);
            std::lock_guard<std::mutex> guard(task->getMutex());
            reinterpret_cast<McbpCreateBucketTask*>(task.get())->start();
            ret = ENGINE_EWOULDBLOCK;
            executorPool->schedule(task, false);
        } catch (const std::bad_alloc&) {
            ret = ENGINE_ENOMEM;
        }
        ret = c->remapErrorCode(ret);
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
        break;
    case ENGINE_EWOULDBLOCK:
        c->setEwouldblock(true);
        c->setState(conn_create_bucket);
        break;
    case ENGINE_DISCONNECT:
        c->setState(conn_closing);
        break;
    default:
        mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ret));
    }
}

static void delete_bucket_executor(McbpConnection* c, void* packet) {
    ENGINE_ERROR_CODE ret = c->getAiostat();
    (void)packet;

    c->setAiostat(ENGINE_SUCCESS);
    c->setEwouldblock(false);

    if (ret == ENGINE_SUCCESS) {
        McbpDestroyBucketTask* task = nullptr;
        try {
            auto* req = reinterpret_cast<protocol_binary_request_delete_bucket*>(packet);
            /* decode packet */
            uint16_t klen = ntohs(req->message.header.request.keylen);
            uint32_t blen = ntohl(req->message.header.request.bodylen);
            blen -= klen;

            std::string name((char*)(req + 1), klen);
            std::string config((char*)(req + 1) + klen, blen);
            bool force = false;

            struct config_item items[2];
            memset(&items, 0, sizeof(items));
            items[0].key = "force";
            items[0].datatype = DT_BOOL;
            items[0].value.dt_bool = &force;
            items[1].key = NULL;

            if (parse_config(config.c_str(), items, stderr) == 0) {
                std::shared_ptr<Task> task = std::make_shared<McbpDestroyBucketTask>(
                    name, force, c);
                std::lock_guard<std::mutex> guard(task->getMutex());
                reinterpret_cast<McbpDestroyBucketTask*>(task.get())->start();
                ret = ENGINE_EWOULDBLOCK;
                executorPool->schedule(task, false);
            } else {
                ret = ENGINE_EINVAL;
            }
        } catch (std::bad_alloc&) {
            ret = ENGINE_ENOMEM;
            delete task;
        }
        ret = c->remapErrorCode(ret);
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
        break;
    case ENGINE_EWOULDBLOCK:
        c->setEwouldblock(true);
        c->setState(conn_delete_bucket);
        break;
    case ENGINE_DISCONNECT:
        c->setState(conn_closing);
        break;
    default:
        mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ret));
    }
}

static void get_errmap_executor(McbpConnection *c, void *packet) {
    auto const *req = reinterpret_cast<protocol_binary_request_get_errmap*>(packet);
    uint16_t version = ntohs(req->message.body.version);
    auto const& ss = settings.getErrorMap(version);
    if (ss.empty()) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);
    } else {
        mcbp_response_handler(NULL, 0, NULL, 0, ss.data(), ss.size(),
                              PROTOCOL_BINARY_RAW_BYTES,
                              PROTOCOL_BINARY_RESPONSE_SUCCESS,
                              0, c->getCookie());
        mcbp_write_and_free(c, &c->getDynamicBuffer());
    }
}

static void shutdown_executor(McbpConnection* c, void* packet) {
    auto req = reinterpret_cast<protocol_binary_request_shutdown*>(packet);
    uint64_t cas = ntohll(req->message.header.request.cas);

    if (session_cas.increment_session_counter(cas)) {
        shutdown_server();
        session_cas.decrement_session_counter();
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
    } else {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS);
    }
}

/**
 * A small task used to reload the RBAC configuration data (it cannot run
 * in the frontend threads as it use file io.
 */
class RbacConfigReloadTask : public Task {
public:
    RbacConfigReloadTask(McbpConnection& connection_)
        : connection(connection_),
          status(ENGINE_SUCCESS) {
        // Empty
    }

    virtual bool execute() override {
        try {
            LOG_NOTICE(nullptr, "%u: Loading RBAC configuration from [%s] %s",
                       connection.getId(),
                       settings.getRbacFile().c_str(),
                       connection.getDescription().c_str());
            cb::rbac::loadPrivilegeDatabase(settings.getRbacFile());
            LOG_NOTICE(nullptr, "%u: RBAC configuration updated %s",
                       connection.getId(), connection.getDescription().c_str());
        } catch (const std::runtime_error& error) {
            LOG_WARNING(nullptr,
                        "%u: RbacConfigReloadTask(): An error occured while loading RBAC configuration from [%s] %s: %s",
                        connection.getId(), settings.getRbacFile().c_str(),
                        connection.getDescription().c_str(), error.what());
            status = ENGINE_FAILED;
        }

        return true;
    }

    virtual void notifyExecutionComplete() override {
        notify_io_complete(connection.getCookie(), status);
    }

private:
    McbpConnection& connection;
    ENGINE_ERROR_CODE status;
};

static void rbac_refresh_executor(McbpConnection* c, void*) {
    ENGINE_ERROR_CODE ret = c->getAiostat();

    c->setAiostat(ENGINE_SUCCESS);
    c->setEwouldblock(false);

    if (ret == ENGINE_SUCCESS) {
        std::shared_ptr<Task> task = std::make_shared<RbacConfigReloadTask>(*c);
        std::lock_guard<std::mutex> guard(task->getMutex());
        ret = ENGINE_EWOULDBLOCK;
        executorPool->schedule(task);
    }

    switch (ret) {
    case ENGINE_EWOULDBLOCK:
        c->setEwouldblock(true);
        c->setState(conn_rbac_reload);
        break;
    case ENGINE_DISCONNECT:
        c->setState(conn_closing);
        break;
    default:
        mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ret));
    }
}

std::array<mcbp_package_execute, 0x100>& get_mcbp_executors(void) {
    static std::array<mcbp_package_execute, 0x100> executors;
    std::fill(executors.begin(), executors.end(), nullptr);

    executors[PROTOCOL_BINARY_CMD_DCP_OPEN] = dcp_open_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_ADD_STREAM] = dcp_add_stream_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_CLOSE_STREAM] = dcp_close_stream_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER] = dcp_snapshot_marker_executor;
    executors[PROTOCOL_BINARY_CMD_TAP_CHECKPOINT_END] = tap_checkpoint_end_executor;
    executors[PROTOCOL_BINARY_CMD_TAP_CHECKPOINT_START] = tap_checkpoint_start_executor;
    executors[PROTOCOL_BINARY_CMD_TAP_CONNECT] = tap_connect_executor;
    executors[PROTOCOL_BINARY_CMD_TAP_DELETE] = tap_delete_executor;
    executors[PROTOCOL_BINARY_CMD_TAP_FLUSH] = tap_flush_executor;
    executors[PROTOCOL_BINARY_CMD_TAP_MUTATION] = tap_mutation_executor;
    executors[PROTOCOL_BINARY_CMD_TAP_OPAQUE] = tap_opaque_executor;
    executors[PROTOCOL_BINARY_CMD_TAP_VBUCKET_SET] = tap_vbucket_set_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_DELETION] = dcp_deletion_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_EXPIRATION] = dcp_expiration_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_FLUSH] = dcp_flush_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_GET_FAILOVER_LOG] = dcp_get_failover_log_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_MUTATION] = dcp_mutation_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_SET_VBUCKET_STATE] = dcp_set_vbucket_state_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_NOOP] = dcp_noop_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_BUFFER_ACKNOWLEDGEMENT] = dcp_buffer_acknowledgement_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_CONTROL] = dcp_control_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_STREAM_END] = dcp_stream_end_executor;
    executors[PROTOCOL_BINARY_CMD_DCP_STREAM_REQ] = dcp_stream_req_executor;
    executors[PROTOCOL_BINARY_CMD_ISASL_REFRESH] = isasl_refresh_executor;
    executors[PROTOCOL_BINARY_CMD_SSL_CERTS_REFRESH] = ssl_certs_refresh_executor;
    executors[PROTOCOL_BINARY_CMD_VERBOSITY] = verbosity_executor;
    executors[PROTOCOL_BINARY_CMD_HELLO] = process_hello_packet_executor;
    executors[PROTOCOL_BINARY_CMD_VERSION] = version_executor;
    executors[PROTOCOL_BINARY_CMD_QUIT] = quit_executor;
    executors[PROTOCOL_BINARY_CMD_QUITQ] = quitq_executor;
    executors[PROTOCOL_BINARY_CMD_SASL_LIST_MECHS] = sasl_list_mech_executor;
    executors[PROTOCOL_BINARY_CMD_SASL_AUTH] = sasl_auth_executor;
    executors[PROTOCOL_BINARY_CMD_SASL_STEP] = sasl_auth_executor;
    executors[PROTOCOL_BINARY_CMD_NOOP] = noop_executor;
    executors[PROTOCOL_BINARY_CMD_FLUSH] = flush_executor;
    executors[PROTOCOL_BINARY_CMD_FLUSHQ] = flush_executor;
    executors[PROTOCOL_BINARY_CMD_SETQ] = setq_executor;
    executors[PROTOCOL_BINARY_CMD_SET] = set_executor;
    executors[PROTOCOL_BINARY_CMD_ADDQ] = addq_executor;
    executors[PROTOCOL_BINARY_CMD_ADD] = add_executor;
    executors[PROTOCOL_BINARY_CMD_REPLACEQ] = replaceq_executor;
    executors[PROTOCOL_BINARY_CMD_REPLACE] = replace_executor;
    executors[PROTOCOL_BINARY_CMD_APPENDQ] = appendq_executor;
    executors[PROTOCOL_BINARY_CMD_APPEND] = append_executor;
    executors[PROTOCOL_BINARY_CMD_PREPENDQ] = prependq_executor;
    executors[PROTOCOL_BINARY_CMD_PREPEND] = prepend_executor;
    executors[PROTOCOL_BINARY_CMD_GET] = get_executor;
    executors[PROTOCOL_BINARY_CMD_GETQ] = get_executor;
    executors[PROTOCOL_BINARY_CMD_GETK] = get_executor;
    executors[PROTOCOL_BINARY_CMD_GETKQ] = get_executor;
    executors[PROTOCOL_BINARY_CMD_DELETE] = delete_executor;
    executors[PROTOCOL_BINARY_CMD_DELETEQ] = delete_executor;
    executors[PROTOCOL_BINARY_CMD_STAT] = stat_executor;
    executors[PROTOCOL_BINARY_CMD_INCREMENT] = arithmetic_executor;
    executors[PROTOCOL_BINARY_CMD_INCREMENTQ] = arithmeticq_executor;
    executors[PROTOCOL_BINARY_CMD_DECREMENT] = arithmetic_executor;
    executors[PROTOCOL_BINARY_CMD_DECREMENTQ] = arithmeticq_executor;
    executors[PROTOCOL_BINARY_CMD_GET_CMD_TIMER] = get_cmd_timer_executor;
    executors[PROTOCOL_BINARY_CMD_SET_CTRL_TOKEN] = set_ctrl_token_executor;
    executors[PROTOCOL_BINARY_CMD_GET_CTRL_TOKEN] = get_ctrl_token_executor;
    executors[PROTOCOL_BINARY_CMD_INIT_COMPLETE] = init_complete_executor;
    executors[PROTOCOL_BINARY_CMD_IOCTL_GET] = ioctl_get_executor;
    executors[PROTOCOL_BINARY_CMD_IOCTL_SET] = ioctl_set_executor;
    executors[PROTOCOL_BINARY_CMD_CONFIG_VALIDATE] = config_validate_executor;
    executors[PROTOCOL_BINARY_CMD_CONFIG_RELOAD] = config_reload_executor;
    executors[PROTOCOL_BINARY_CMD_AUDIT_PUT] = audit_put_executor;
    executors[PROTOCOL_BINARY_CMD_AUDIT_CONFIG_RELOAD] = audit_config_reload_executor;
    executors[PROTOCOL_BINARY_CMD_SHUTDOWN] = shutdown_executor;
    executors[PROTOCOL_BINARY_CMD_SUBDOC_GET] = subdoc_get_executor;
    executors[PROTOCOL_BINARY_CMD_SUBDOC_EXISTS] = subdoc_exists_executor;
    executors[PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD] = subdoc_dict_add_executor;
    executors[PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT] = subdoc_dict_upsert_executor;
    executors[PROTOCOL_BINARY_CMD_SUBDOC_DELETE] = subdoc_delete_executor;
    executors[PROTOCOL_BINARY_CMD_SUBDOC_REPLACE] = subdoc_replace_executor;
    executors[PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST] = subdoc_array_push_last_executor;
    executors[PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST] = subdoc_array_push_first_executor;
    executors[PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT] = subdoc_array_insert_executor;
    executors[PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE] = subdoc_array_add_unique_executor;
    executors[PROTOCOL_BINARY_CMD_SUBDOC_COUNTER] = subdoc_counter_executor;
    executors[PROTOCOL_BINARY_CMD_SUBDOC_MULTI_LOOKUP] = subdoc_multi_lookup_executor;
    executors[PROTOCOL_BINARY_CMD_SUBDOC_MULTI_MUTATION] = subdoc_multi_mutation_executor;
    executors[PROTOCOL_BINARY_CMD_SUBDOC_GET_COUNT] = subdoc_get_count_executor;

    executors[PROTOCOL_BINARY_CMD_CREATE_BUCKET] = create_bucket_executor;
    executors[PROTOCOL_BINARY_CMD_LIST_BUCKETS] = list_bucket_executor;
    executors[PROTOCOL_BINARY_CMD_DELETE_BUCKET] = delete_bucket_executor;
    executors[PROTOCOL_BINARY_CMD_SELECT_BUCKET] = select_bucket_executor;
    executors[PROTOCOL_BINARY_CMD_GET_ERROR_MAP] = get_errmap_executor;
    executors[PROTOCOL_BINARY_CMD_GET_LOCKED] = get_locked_executor;
    executors[PROTOCOL_BINARY_CMD_UNLOCK_KEY] = unlock_executor;

    executors[PROTOCOL_BINARY_CMD_RBAC_REFRESH] = rbac_refresh_executor;


    return executors;
}

static void process_bin_dcp_response(McbpConnection* c) {
    ENGINE_ERROR_CODE ret = ENGINE_DISCONNECT;

    c->setSupportsDatatype(true);

    if (c->getBucketEngine()->dcp.response_handler != NULL) {
        auto* header = reinterpret_cast<protocol_binary_response_header*>
        (c->read.curr - (c->binary_header.request.bodylen +
                         sizeof(c->binary_header)));
        ret = c->getBucketEngine()->dcp.response_handler
            (c->getBucketEngineAsV0(), c->getCookie(), header);
        ret = c->remapErrorCode(ret);
    }

    if (ret == ENGINE_DISCONNECT) {
        c->setState(conn_closing);
    } else {
        c->setState(conn_ship_log);
    }
}


void initialize_mbcp_lookup_map(void) {
    int ii;
    for (ii = 0; ii < 0x100; ++ii) {
        request_handlers[ii].descriptor = NULL;
        request_handlers[ii].callback = default_unknown_command;
    }

    response_handlers[PROTOCOL_BINARY_CMD_NOOP] = process_bin_noop_response;
    response_handlers[PROTOCOL_BINARY_CMD_TAP_MUTATION] = process_bin_tap_ack;
    response_handlers[PROTOCOL_BINARY_CMD_TAP_DELETE] = process_bin_tap_ack;
    response_handlers[PROTOCOL_BINARY_CMD_TAP_FLUSH] = process_bin_tap_ack;
    response_handlers[PROTOCOL_BINARY_CMD_TAP_OPAQUE] = process_bin_tap_ack;
    response_handlers[PROTOCOL_BINARY_CMD_TAP_VBUCKET_SET] = process_bin_tap_ack;
    response_handlers[PROTOCOL_BINARY_CMD_TAP_CHECKPOINT_START] = process_bin_tap_ack;
    response_handlers[PROTOCOL_BINARY_CMD_TAP_CHECKPOINT_END] = process_bin_tap_ack;

    response_handlers[PROTOCOL_BINARY_CMD_DCP_OPEN] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_ADD_STREAM] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_CLOSE_STREAM] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_STREAM_REQ] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_GET_FAILOVER_LOG] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_STREAM_END] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_MUTATION] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_DELETION] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_EXPIRATION] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_FLUSH] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_SET_VBUCKET_STATE] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_NOOP] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_BUFFER_ACKNOWLEDGEMENT] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_CONTROL] = process_bin_dcp_response;
    response_handlers[PROTOCOL_BINARY_CMD_DCP_RESERVED4] = process_bin_dcp_response;
}

bool conn_setup_tap_stream(McbpConnection* c) {
    process_bin_tap_connect(c);
    return true;
}

/**
 * Check if the current packet use an invalid datatype value. It may be
 * considered invalid for two reasons:
 *
 *    1) it is using an unknown value
 *    2) The connected client has not enabled the support for datatype
 *
 * @param c - the connected client
 * @return true if the packet is considered invalid in this context,
 *         false otherwise
 */
static bool invalid_datatype(McbpConnection* c) {
    if (mcbp::datatype::is_raw(c->binary_header.request.datatype) ||
        c->isSupportsDatatype() || c->isXattrSupport()) {
        // Additional bits may have been set
        if (!mcbp::datatype::is_valid(c->binary_header.request.datatype)) {
            return true;
        }

        return false;
    }

    return true;
}

static protocol_binary_response_status validate_bin_header(McbpConnection* c) {
    if (c->binary_header.request.bodylen >=
        (c->binary_header.request.keylen + c->binary_header.request.extlen)) {
        return PROTOCOL_BINARY_RESPONSE_SUCCESS;
    } else {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }
}

static void process_bin_packet(McbpConnection* c) {
    static McbpPrivilegeChains privilegeChains;
    protocol_binary_response_status result;

    char* packet = (c->read.curr - (c->binary_header.request.bodylen +
                                    sizeof(c->binary_header)));

    auto opcode = static_cast<protocol_binary_command>(c->binary_header.request.opcode);
    auto executor = executors[opcode];

    const auto res = privilegeChains.invoke(opcode, c->getCookieObject());
    switch (res) {
    case cb::rbac::PrivilegeAccess::Fail:
        LOG_WARNING(c,
                    "%u %s: no access to command %s",
                    c->getId(), c->getDescription().c_str(),
                    memcached_opcode_2_text(opcode));
        audit_command_access_failed(c);

        if (c->remapErrorCode(ENGINE_EACCESS) == ENGINE_DISCONNECT) {
            c->setState(conn_closing);
            return;
        } else {
            mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_EACCESS);
        }

        return;
    case cb::rbac::PrivilegeAccess::Ok:
        result = validate_bin_header(c);
        if (result == PROTOCOL_BINARY_RESPONSE_SUCCESS) {
            result = c->validateCommand(opcode);
        }

        if (result != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
            LOG_NOTICE(c,
                       "%u: Invalid format specified for %s - %d - "
                           "closing connection",
                       c->getId(), memcached_opcode_2_text(opcode), result);
            audit_invalid_packet(c);
            mcbp_write_packet(c, result);
            c->setWriteAndGo(conn_closing);
            return;
        }

        if (executor != NULL) {
            executor(c, packet);
        } else {
            process_bin_unknown_packet(c);
        }
        return;
    case cb::rbac::PrivilegeAccess::Stale:
        if (c->remapErrorCode(ENGINE_AUTH_STALE) == ENGINE_DISCONNECT) {
            c->setState(conn_closing);
        } else {
            mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_AUTH_STALE);
        }
        return;
    }

    LOG_WARNING(c,
                "%u: process_bin_packet: res (which is %d) is not a valid "
                    "AuthResult - closing connection", res);
    c->setState(conn_closing);
}


static inline bool is_initialized(McbpConnection* c, uint8_t opcode) {
    if (c->isInternal() || is_server_initialized()) {
        return true;
    }

    switch (opcode) {
    case PROTOCOL_BINARY_CMD_SASL_LIST_MECHS:
    case PROTOCOL_BINARY_CMD_SASL_AUTH:
    case PROTOCOL_BINARY_CMD_SASL_STEP:
        return true;
    default:
        return false;
    }
}

static void dispatch_bin_command(McbpConnection* c) {
    uint16_t keylen = c->binary_header.request.keylen;

    /* @trond this should be in the Connection-connect part.. */
    /*        and in the select bucket */
    if (c->getBucketEngine() == NULL) {
        c->setBucketEngine(all_buckets[c->getBucketIndex()].engine);
    }

    if (!is_initialized(c, c->binary_header.request.opcode)) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_NOT_INITIALIZED);
        c->setWriteAndGo(conn_closing);
        return;
    }

    if (settings.isRequireSasl() && !authenticated(c)) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_AUTH_ERROR);
        c->setWriteAndGo(conn_closing);
        return;
    }

    if (invalid_datatype(c)) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_EINVAL);
        c->setWriteAndGo(conn_closing);
        return;
    }

    if (c->getStart() == 0) {
        c->setStart(gethrtime());
    }

    MEMCACHED_PROCESS_COMMAND_START(c->getId(), c->read.curr, c->read.bytes);

    /* binprot supports 16bit keys, but internals are still 8bit */
    if (keylen > KEY_MAX_LENGTH) {
        handle_binary_protocol_error(c);
        return;
    }

    c->setNoReply(false);

    /*
     * Protect ourself from someone trying to kill us by sending insanely
     * large packets.
     */
    if (c->binary_header.request.bodylen > settings.getMaxPacketSize()) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_EINVAL);
        c->setWriteAndGo(conn_closing);
    } else {
        bin_read_chunk(c, c->binary_header.request.bodylen);
    }
}

void mcbp_complete_nread(McbpConnection* c) {
    if (c->binary_header.request.magic == PROTOCOL_BINARY_RES) {
        RESPONSE_HANDLER handler;
        handler = response_handlers[c->binary_header.request.opcode];
        if (handler) {
            handler(c);
        } else {
            LOG_WARNING(c, "%u: Unsupported response packet received: %u",
                        c->getId(),
                        (unsigned int)c->binary_header.request.opcode);
            c->setState(conn_closing);
        }
    } else {
        process_bin_packet(c);
    }
}

int try_read_mcbp_command(McbpConnection* c) {
    if (c == nullptr) {
        throw std::runtime_error("Internal eror, connection is not mcbp");
    }
    cb_assert(c->read.curr <= (c->read.buf + c->read.size));
    cb_assert(c->read.bytes > 0);

    /* Do we have the complete packet header? */
    if (c->read.bytes < sizeof(c->binary_header)) {
        /* need more data! */
        return 0;
    } else {
#ifdef NEED_ALIGN
        if (((long)(c->read.curr)) % 8 != 0) {
            /* must realign input buffer */
            memmove(c->read.buf, c->read.curr, c->read.bytes);
            c->read.curr = c->read.buf;
            LOG_DEBUG(c, "%d: Realign input buffer", c->sfd);
        }
#endif
        protocol_binary_request_header* req;
        req = (protocol_binary_request_header*)c->read.curr;

        if (settings.getVerbose() > 1) {
            /* Dump the packet before we convert it to host order */
            char buffer[1024];
            ssize_t nw;
            nw = bytes_to_output_string(buffer, sizeof(buffer), c->getId(),
                                        true, "Read binary protocol data:",
                                        (const char*)req->bytes,
                                        sizeof(req->bytes));
            if (nw != -1) {
                LOG_DEBUG(c, "%s", buffer);
            }
        }

        c->binary_header = *req;
        c->binary_header.request.keylen = ntohs(req->request.keylen);
        c->binary_header.request.bodylen = ntohl(req->request.bodylen);
        c->binary_header.request.vbucket = ntohs(req->request.vbucket);
        c->binary_header.request.cas = ntohll(req->request.cas);

        if (c->binary_header.request.magic != PROTOCOL_BINARY_REQ &&
            !(c->binary_header.request.magic == PROTOCOL_BINARY_RES &&
              response_handlers[c->binary_header.request.opcode])) {
            if (c->binary_header.request.magic != PROTOCOL_BINARY_RES) {
                LOG_WARNING(c, "%u: Invalid magic: %x, closing connection",
                            c->getId(), c->binary_header.request.magic);
            } else {
                LOG_WARNING(c,
                            "%u: Unsupported response packet received: %u, "
                                "closing connection",
                            c->getId(),
                            (unsigned int)c->binary_header.request.opcode);

            }
            c->setState(conn_closing);
            return -1;
        }

        c->addMsgHdr(true);
        c->setCmd(c->binary_header.request.opcode);
        /* clear the returned cas value */
        c->setCAS(0);

        dispatch_bin_command(c);

        c->read.bytes -= sizeof(c->binary_header);
        c->read.curr += sizeof(c->binary_header);
    }

    return 1;
}
