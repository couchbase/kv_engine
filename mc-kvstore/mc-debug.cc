/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2011 Couchbase, Inc
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
#include "config.h"

#include "vbucket.hh"
#include "mc-kvstore/mc-debug.hh"
#include "mc-kvstore/mc-engine.hh"

using namespace std;

static const char *getErrorCode(uint16_t rcode) {
    switch (ntohs(rcode)) {
    case PROTOCOL_BINARY_RESPONSE_SUCCESS:
        return "SUCCESS";
    case PROTOCOL_BINARY_RESPONSE_KEY_ENOENT:
        return "KEY_ENOENT";
    case PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS:
        return "KEY_EEXISTS";
    case PROTOCOL_BINARY_RESPONSE_E2BIG:
        return "E2BIG";
    case PROTOCOL_BINARY_RESPONSE_EINVAL:
        return "EINVAL";
    case PROTOCOL_BINARY_RESPONSE_NOT_STORED:
        return "NOT_STORED";
    case PROTOCOL_BINARY_RESPONSE_DELTA_BADVAL:
        return "DELTA_BADVAL";
    case PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET:
        return "NOT_MY_VBUCKET";
    case PROTOCOL_BINARY_RESPONSE_AUTH_ERROR:
        return "AUTH_ERROR";
    case PROTOCOL_BINARY_RESPONSE_AUTH_CONTINUE:
        return "AUTH_CONTINUE";
    case PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND:
        return "UNKNOWN_COMMAND";
    case PROTOCOL_BINARY_RESPONSE_ENOMEM:
        return "ENOMEM";
    case PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED:
        return "NOT_SUPPORTED";
    case PROTOCOL_BINARY_RESPONSE_EINTERNAL:
        return "EINTERNAL";
    case PROTOCOL_BINARY_RESPONSE_EBUSY:
        return "EBUSY";
    case PROTOCOL_BINARY_RESPONSE_ETMPFAIL:
        return "ETMPFAIL";
    default:
        return "";
    }
}

static const char *getOpcode(uint8_t opcode) {
    switch (opcode) {
    case PROTOCOL_BINARY_CMD_GET:
        return "get";
    case PROTOCOL_BINARY_CMD_SET:
        return "set";
    case PROTOCOL_BINARY_CMD_ADD:
        return "add";
    case PROTOCOL_BINARY_CMD_REPLACE:
        return "replace";
    case PROTOCOL_BINARY_CMD_DELETE:
        return "delete";
    case PROTOCOL_BINARY_CMD_INCREMENT:
        return "increment";
    case PROTOCOL_BINARY_CMD_DECREMENT:
        return "decrement";
    case PROTOCOL_BINARY_CMD_QUIT:
        return "quit";
    case PROTOCOL_BINARY_CMD_FLUSH:
        return "flush";
    case PROTOCOL_BINARY_CMD_GETQ:
        return "getq";
    case PROTOCOL_BINARY_CMD_NOOP:
        return "noop";
    case PROTOCOL_BINARY_CMD_VERSION:
        return "version";
    case PROTOCOL_BINARY_CMD_GETK:
        return "getk";
    case PROTOCOL_BINARY_CMD_GETKQ:
        return "getkq";
    case PROTOCOL_BINARY_CMD_APPEND:
        return "append";
    case PROTOCOL_BINARY_CMD_PREPEND:
        return "prepend";
    case PROTOCOL_BINARY_CMD_STAT:
        return "stat";
    case PROTOCOL_BINARY_CMD_SETQ:
        return "setq";
    case PROTOCOL_BINARY_CMD_ADDQ:
        return "addq";
    case PROTOCOL_BINARY_CMD_REPLACEQ:
        return "replaceq";
    case PROTOCOL_BINARY_CMD_DELETEQ:
        return "deleteq";
    case PROTOCOL_BINARY_CMD_INCREMENTQ:
        return "incrementq";
    case PROTOCOL_BINARY_CMD_DECREMENTQ:
        return "decrementq";
    case PROTOCOL_BINARY_CMD_QUITQ:
        return "quitq";
    case PROTOCOL_BINARY_CMD_FLUSHQ:
        return "flushq";
    case PROTOCOL_BINARY_CMD_APPENDQ:
        return "appendq";
    case PROTOCOL_BINARY_CMD_PREPENDQ:
        return "prependq";
    case PROTOCOL_BINARY_CMD_VERBOSITY:
        return "verbosity";
    case PROTOCOL_BINARY_CMD_SASL_LIST_MECHS:
        return "sasl list mechs";
    case PROTOCOL_BINARY_CMD_SASL_AUTH:
        return "sasl auth";
    case PROTOCOL_BINARY_CMD_SASL_STEP:
        return "sasl step";
    case PROTOCOL_BINARY_CMD_RGET:
        return "rget";
    case PROTOCOL_BINARY_CMD_RSET:
        return "rset";
    case PROTOCOL_BINARY_CMD_RSETQ:
        return "rsetq";
    case PROTOCOL_BINARY_CMD_RAPPEND:
        return "rappend";
    case PROTOCOL_BINARY_CMD_RAPPENDQ:
        return "rappendq";
    case PROTOCOL_BINARY_CMD_RPREPEND:
        return "rprepend";
    case PROTOCOL_BINARY_CMD_RPREPENDQ:
        return "rprependq";
    case PROTOCOL_BINARY_CMD_RDELETE:
        return "rdelete";
    case PROTOCOL_BINARY_CMD_RDELETEQ:
        return "rdeleteq";
    case PROTOCOL_BINARY_CMD_RINCR:
        return "rincrement";
    case PROTOCOL_BINARY_CMD_RINCRQ:
        return "rincrementq";
    case PROTOCOL_BINARY_CMD_RDECR:
        return "rdecrement";
    case PROTOCOL_BINARY_CMD_RDECRQ:
        return "rdecrementq";
    case PROTOCOL_BINARY_CMD_SET_VBUCKET:
        return "set vbucket";
    case PROTOCOL_BINARY_CMD_GET_VBUCKET:
        return "get vbucket";
    case PROTOCOL_BINARY_CMD_DEL_VBUCKET:
        return "del vbucket";
    case PROTOCOL_BINARY_CMD_TAP_CONNECT:
        return "tap connect";
    case PROTOCOL_BINARY_CMD_TAP_MUTATION:
        return "tap mutation";
    case PROTOCOL_BINARY_CMD_TAP_DELETE:
        return "tap delete";
    case PROTOCOL_BINARY_CMD_TAP_FLUSH:
        return "tap flush";
    case PROTOCOL_BINARY_CMD_TAP_OPAQUE:
        return "tap opaque";
    case PROTOCOL_BINARY_CMD_TAP_VBUCKET_SET:
        return "tap vbucket set";
    case PROTOCOL_BINARY_CMD_LAST_RESERVED:
        return "last reserved";
    case PROTOCOL_BINARY_CMD_SCRUB:
        return "scrub";

    case 0x85:
        return "create bucket";
    case 0x86:
        return "delete bucket";
    case 0x89:
        return "select bucket";

    default:
        return "";
    }
}

struct BinaryProtocolPacket {
    BinaryProtocolPacket(size_t sz, const uint8_t *dta) :
        size(sz), data(dta) {
    }
    size_t size;
    const uint8_t *data;
};

static ostream& operator<<(ostream& out, const BinaryProtocolPacket &packet) {
    size_t len = packet.size;
    const uint8_t *ptr = packet.data;
    out
            << "  Byte/     0       |       1       |       2       |       3       |"
            << endl
            << "     /              |               |               |               |"
            << endl
            << "    |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|";

    for (size_t ii = 0; ii < len; ++ii) {
        if (ii % 4 == 0) {
            out << endl
                    << "    +---------------+---------------+---------------+---------------+\n";
            if (ii < 10) {
                out << " ";
            }

            if (ii < 100) {
                out << " ";
            }

            out << " " << ii << "|";
        }
        char bb[20];
        if (ii >= sizeof(protocol_binary_request_header) && isgraph(ptr[ii])) {
            sprintf(bb, "    %02x ('%c')   |", ptr[ii], ptr[ii]);
        } else {
            sprintf(bb, "       %02x      |", ptr[ii]);
        }
        out << bb;
    }
    out << endl;
    return out;
}

static ostream& operator<<(ostream& out, uint8_t v) {
    char val[5];
    sprintf(val, "0x%02x", v);
    out << val;
    return out;
}

static inline ostream& operator<<(ostream& out, vbucket_state_t state) {
    switch (state) {
        case vbucket_state_active:
        out << "active";
        break;
        case vbucket_state_replica:
        out << "replica";
        break;
        case vbucket_state_pending:
        out << "pending";
        break;
        case vbucket_state_dead:
        out << "dead";
        break;
        default:
        out << "unknown: " << static_cast<unsigned int> (state);
    }

    return out;
}

static ostream& operator<<(ostream& out,
        const protocol_binary_request_set_vbucket *req) {
    vbucket_state_t state;
    memcpy(&state, &req->message.body.state, sizeof(state));
    state = (vbucket_state_t)ntohl(state);
    out << "Extras:" << endl << "  State             : " << state << endl;
    if (req->message.header.request.extlen != 4) {
        out << "WARNING: Extlen should be 4 bytes for set_vbucket" << endl;
    }
    return out;
}

static ostream& operator<<(ostream& out,
        const protocol_binary_request_tap_connect *req) {
    const uint8_t *ptr = req->bytes + sizeof(req->bytes);
    const uint8_t *end = ptr + ntohl(req->message.header.request.bodylen)
            - req->message.header.request.extlen;
    uint32_t flags;
    memcpy(&flags, &req->message.body.flags, sizeof(flags));
    flags = ntohl(flags);
    out << "Extras" << endl;
    if (flags != 0) {
        out << "  Flags             : " << endl;
        if (flags & TAP_CONNECT_FLAG_BACKFILL) {
            uint64_t bftime;
            memcpy(&bftime, ptr, sizeof(bftime));
            ptr += sizeof(bftime);
            out << "    Backfill: [" << bftime << "]" << endl;
        }

        if (flags & TAP_CONNECT_FLAG_DUMP) {
            out << "    Dump" << endl;
        }

        if (flags & TAP_CONNECT_FLAG_LIST_VBUCKETS) {
            uint16_t nv;
            memcpy(&nv, ptr, sizeof(nv));
            ptr += sizeof(nv);
            nv = ntohs(nv);
            out << "    List vbuckets   : " << nv << " { ";
            for (uint16_t ii = 0; ii < nv; ++ii) {
                uint16_t vb;
                memcpy(&vb, ptr, sizeof(vb));
                ptr += sizeof(vb);
                vb = ntohs(vb);
                out << vb;
                if (ii + 1 < nv) {
                    out << ", ";
                }
            }
            out << "}" << endl;
        }

        if (flags & TAP_CONNECT_FLAG_TAKEOVER_VBUCKETS) {
            out << "    Takeover vbuckets";
        }

        if (flags & TAP_CONNECT_SUPPORT_ACK) {
            out << "    Support ack";
        }

        if (flags & TAP_CONNECT_REQUEST_KEYS_ONLY) {
            out << "    Request keys only";
        }

        if (flags & TAP_CONNECT_CHECKPOINT) {
            // @todo figure out how this is encoded!
            out << "    Checkpoint";
        }

        if (flags & TAP_CONNECT_REGISTERED_CLIENT) {
            out << "    Registered client";
        }
    } else {
        out << "  Flags             : None" << endl;
    }

    if (ptr < end) {
        std::string name((const char*)ptr, (end - ptr));
        out << "  Name              : " << name << end;
    }

    return out;
}

static ostream& operator<<(ostream& out,
        const protocol_binary_request_tap_mutation *req) {
    uint16_t keylen = ntohs(req->message.header.request.keylen);
    const char *data;
    data = (const char*)req->bytes + sizeof(req->bytes) + ntohs(req->message.body.tap.enginespecific_length);
    string key(data, (size_t)keylen);
    uint32_t vallen = ntohl(req->message.header.request.bodylen);
    vallen -= keylen;
    vallen -= req->message.header.request.extlen;
    vallen -=  ntohs(req->message.body.tap.enginespecific_length);
//    string value(data + keylen, vallen);

    out << "Extras" << endl;
    out << "  TAP               : " << endl;
    out << "    Engine specific : " << ntohs(req->message.body.tap.enginespecific_length) << endl;
    out << "    Flags           : " << ntohs(req->message.body.tap.flags) << endl;
    out << "    TTL             : " << (unsigned int)req->message.body.tap.ttl << endl;
    out << "    RES[1, 2, 3]    : [" << (unsigned int)req->message.body.tap.res1 << "," << (unsigned int)req->message.body.tap.res2 << ","<< (unsigned int)req->message.body.tap.res3 << "]" <<endl;
    out << "  Item              : " << endl;
    out << "    Flags           : " << req->message.body.item.flags << endl;
    out << "    Expiration      : " << req->message.body.item.expiration << endl;
    out << "    Key             : [" << key << "]" << endl;
//    out << "    Value           : [" << value << "]" << endl;
    out << "    vallen          : " << vallen << endl;
    return out;
}

ostream& operator<<(ostream& out, const protocol_binary_request_header *req) {
    using namespace std;
    BinaryProtocolPacket packet(
            ntohl(req->request.bodylen) + sizeof(req->bytes), req->bytes);

    out << packet;
    assert(req->request.magic == PROTOCOL_BINARY_REQ);
    out << "Packet breakdown" << endl << "Field        (offset) (value)"
            << endl << "Magic            (0): PROTOCOL_BINARY_REQ" << endl
            << "Opcode           (1): " << req->request.opcode << " ["
            << getOpcode(req->request.opcode) << "]" << endl
            << "Key length     (2-3): " << ntohs(req->request.keylen) << endl
            << "Extra length     (4): " << (unsigned int)req->request.extlen
            << endl << "Data type        (5): "
            << (unsigned int)req->request.datatype << endl
            << "VBucket        (6-7): " << ntohs(req->request.vbucket) << endl
            << "Total body    (8-11): " << ntohl(req->request.bodylen) << endl
            << "Opaque       (12-15): " << req->request.opaque << endl
            << "CAS          (16-23): " << req->request.cas << endl;

    switch (req->request.opcode) {
    case PROTOCOL_BINARY_CMD_SET_VBUCKET:
        out
                << reinterpret_cast<const protocol_binary_request_set_vbucket*> (req);
        return out;

    case PROTOCOL_BINARY_CMD_TAP_CONNECT:
        out
                << reinterpret_cast<const protocol_binary_request_tap_connect*> (req);
        return out;

    case PROTOCOL_BINARY_CMD_TAP_MUTATION:
        out     << reinterpret_cast<const protocol_binary_request_tap_mutation*> (req);
        return out;


    default:
        ;
    }

    // we don't have a specialized parser for this
    if (req->request.extlen > 0) {
        out << "Extras:" << endl << "  Don't know how to decode extra fields..";
    }

    uint16_t len = ntohs(req->request.keylen);
    if (len != 0) {
        string key(
                (const char*)req->bytes + sizeof(*req) + req->request.extlen,
                (size_t)len);
        out << "Key                 : [" << key << "]" << endl;
    }

    out << endl;
    return out;
}

ostream& operator<<(ostream& out, const protocol_binary_response_header *res) {
    using namespace std;
    BinaryProtocolPacket packet(
            ntohl(res->response.bodylen) + sizeof(res->bytes), res->bytes);

    out << packet;
    assert(res->response.magic == PROTOCOL_BINARY_RES);

    out << "Packet breakdown" << endl << "Field        (offset) (value)"
            << endl << "Magic            (0): PROTOCOL_BINARY_RES" << endl
            << "Opcode           (1): " << res->response.opcode << " ["
            << getOpcode(res->response.opcode) << "]" << endl
            << "Key length     (2-3): " << ntohs(res->response.keylen) << endl
            << "Extra length     (4): " << (unsigned int)res->response.extlen
            << endl << "Data type        (5): "
            << (unsigned int)res->response.datatype << endl
            << "Status         (6-7): " << ntohs(res->response.status) << " "
            << getErrorCode(res->response.status) << endl
            << "Total body    (8-11): " << ntohl(res->response.bodylen) << endl
            << "Opaque       (12-15): " << res->response.opaque << endl
            << "CAS          (16-23): " << res->response.cas << endl;

    const char *offset = (const char*)res->bytes + sizeof(*res)
            + res->response.extlen;

    uint16_t keylen = ntohs(res->response.keylen);
    string key(offset, (size_t)keylen);
    offset += keylen;
    uint32_t vallen = ntohl(res->response.bodylen) - keylen;
    string value(offset, (size_t)vallen);

    if (ntohs(res->response.status) == PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        if (keylen > 0) {
            out << "Key                 : [" << key << "]" << endl;
        }
        if (vallen > 0) {
            out << "Value               : [" << value << "]" << endl;
        }
    } else if (vallen > 0) {
        out << "Error message       : [" << value << "]" << endl;
    }

    out << endl;

    return out;
}
