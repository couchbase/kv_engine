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
#include "config.h"

#include <stdexcept>
#include <string>
#include <libmcbp/mcbp.h>
#include <iomanip>
#include <include/memcached/protocol_binary.h>
#include <utilities/protocol2text.h>

// This file contains the functionality to generate a packet dump
// of a memcached frame

/**
 * The Frame class represents a complete frame as it is being sent on the
 * wire in the Memcached Binary Protocol.
 */
class Frame {
public:
    Frame(const uint8_t* bytes, size_t len)
        : root(bytes),
          length(len) {
        // empty
    }

    void dump(std::ostream& out) const {
        dumpFrame(out);
        dumpPacketInfo(out);
        dumpExtras(out);
        dumpKey(out);
        dumpValue(out);
    }

protected:
    virtual void dumpPacketInfo(std::ostream& out) const = 0;
    virtual void dumpExtras(std::ostream &out) const = 0;
    virtual void dumpKey(std::ostream &out) const = 0;
    virtual void dumpValue(std::ostream&out) const = 0;

    void dumpExtras(const uint8_t* location,
                    uint8_t nbytes,
                    std::ostream& out) const {
        if (nbytes != 0) {
            out << "    Extra               : " << (uint16_t(nbytes) & 0xff)
                << " bytes of binary data" << std::endl;
        }
    }

    void dumpKey(const uint8_t* location, uint16_t nbytes,
                 std::ostream& out) const {
        if (nbytes != 0) {
            const ptrdiff_t first = location - root;
            const ptrdiff_t last = first + nbytes - 1;
            out << "    Key          (" << first << "-" << last << "): ";

            std::string str((const char*)location, nbytes);

            for (const auto&c : str) {
                if (!isprint(c)) {
                    out << nbytes
                        << " bytes of binary data" << std::endl;
                    return;
                }
            }
            out << "The textual string \"" << str
                << "\"" << std::endl;
        }
    }


private:
    void printByte(uint8_t b, bool inBody, std::ostream& out) const {
        uint32_t val = b & 0xff;
        if (b < 0x10) {
            out << " 0x0";
        } else {
            out << " 0x";
        }

        out.flags(std::ios::hex);
        out << val;
        out.flags(std::ios::dec);

        if (inBody && isprint((int)b)) {
            out << " ('" << (char)b << "')";
        } else {
            out << "      ";
        }
        out << "    |";
    }

    void dumpFrame(std::ostream& out) const {
        out << std::endl;
        out <<
        "      Byte/     0       |       1       |       2       |       3       |" <<
        std::endl;
        out <<
        "         /              |               |               |               |" <<
        std::endl;
        out <<
        "        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|";

        size_t ii = 0;
        for (; ii < length; ++ii) {
            if (ii % 4 == 0) {
                out << std::endl;
                out <<
                "        +---------------+---------------+---------------+---------------+" <<
                std::endl;
                out.setf(std::ios::right);
                out << std::setw(8) << ii << "|";
                out.setf(std::ios::fixed);
            }
            printByte(root[ii], ii > 23, out);
        }

        out << std::endl;
        if (ii % 4 != 0) {
            out << "        ";
            for (size_t jj = 0; jj < ii % 4; ++jj) {
                out << "+---------------";
            }
            out << "+" << std::endl;
        } else {
            out << "        +---------------+---------------+---------------+---------------+"
            << std::endl;
        }
    }

    const uint8_t *root;
    size_t length;
};

std::ostream& operator<<(std::ostream& out, const Frame& frame) {
    frame.dump(out);
    return out;
}

class Request : public Frame {
public:
    Request(const protocol_binary_request_header& req)
        : Frame(req.bytes, ntohl(req.request.bodylen) + sizeof(req.bytes)),
          request(req) {
        // nothing
    }

protected:

    virtual void dumpPacketInfo(std::ostream& out) const override {
        uint32_t bodylen = ntohl(request.request.bodylen);

        out << "        Total " << sizeof(request) + bodylen << " bytes";
        if (bodylen > 0) {
            out << " (" << sizeof(request) << " bytes header";
            if (request.request.extlen != 0) {
                out << ", " << (unsigned int)request.request.extlen << " byte extras ";
            }
            uint16_t keylen = ntohs(request.request.keylen);
            if (keylen > 0) {
                out << ", " << keylen << " bytes key";
            }
            bodylen -= keylen;
            bodylen -= request.request.extlen;
            if (bodylen > 0) {
                out << " and " << bodylen << " value";
            }
            out << ")";
        }

        out << std::endl << std::endl;
        out.flags(std::ios::hex);
        out << std::setfill('0');
        out << "    Field        (offset) (value)" << std::endl;
        out << "    Magic        (0)    : 0x" << (uint32_t(request.bytes[0]) & 0xff) << std::endl;
        out << "    Opcode       (1)    : 0x" << std::setw(2) << (uint32_t(request.bytes[1]) & 0xff) << std::endl;
        out << "    Key length   (2,3)  : 0x" << std::setw(4) << (ntohs(request.request.keylen) & 0xffff) << std::endl;
        out << "    Extra length (4)    : 0x" << std::setw(2) << (uint32_t(request.bytes[4]) & 0xff) << std::endl;
        out << "    Data type    (5)    : 0x" << std::setw(2) << (uint32_t(request.bytes[5]) & 0xff) << std::endl;
        out << "    Vbucket      (6,7)  : 0x" << std::setw(4) << (ntohs(request.request.vbucket) & 0xffff) << std::endl;
        out << "    Total body   (8-11) : 0x" << std::setw(8) << (uint64_t(ntohl(request.request.bodylen)) & 0xffff) << std::endl;
        out << "    Opaque       (12-15): 0x" << std::setw(8) << ntohl(request.request.opaque) << std::endl;
        out << "    CAS          (16-23): 0x" << std::setw(16) << ntohll(request.request.cas) << std::endl;
        out << std::setfill(' ');
        out.flags(std::ios::dec);
    }

    virtual void dumpExtras(std::ostream &out) const override {
        Frame::dumpExtras(request.bytes + sizeof(request.bytes),
                          request.request.extlen,
                          out);
    }

    virtual void dumpKey(std::ostream &out) const override {
        Frame::dumpKey(
            request.bytes + sizeof(request.bytes) + request.request.extlen,
            ntohs(request.request.keylen),
            out);
    }

    virtual void dumpValue(std::ostream &out) const override {

    }

    const protocol_binary_request_header& request;
};

class HelloRequest : public Request {
public:
    HelloRequest(const protocol_binary_request_header& req)
        : Request(req) {

    }

protected:
    virtual void dumpExtras(std::ostream& out) const override {
        if (request.request.extlen != 0) {
            throw std::logic_error(
                "HelloRequest::dumpExtras(): extlen must be 0");
        }
    }

    virtual void dumpValue(std::ostream& out) const override {
        uint32_t bodylen = ntohl(request.request.bodylen);
        uint16_t keylen = ntohs(request.request.keylen);
        bodylen -= keylen;

        if ((bodylen % 2) != 0) {
            throw std::logic_error(
                "HelloRequest::dumpValue(): bodylen must be in words");
        }

        if (bodylen == 0) {
            return;
        }
        uint32_t first = sizeof(request.bytes) + keylen;
        out << "    Body                :" << std::endl;

        uint32_t total = bodylen / 2;
        uint32_t offset = sizeof(request.bytes) + keylen;
        for (uint32_t ii = 0; ii < total; ++ii) {
            uint16_t feature;
            memcpy(&feature, request.bytes + offset, 2);
            offset += 2;
            feature = ntohs(feature);
            const char *txt = protocol_feature_2_text(feature);
            out << "                 (" << first << "-"
                << first + 1 <<"): " << txt << std::endl;
            first += 2;
        }
    }
};

class Response : public Frame {
public:
    Response(const protocol_binary_response_header& res)
        : Frame(res.bytes, ntohl(res.response.bodylen) + sizeof(res.bytes)),
          response(res) {
        // nothing
    }

protected:

    virtual void dumpPacketInfo(std::ostream& out) const override {
        uint32_t bodylen = ntohl(response.response.bodylen);
        out << "        Total " << sizeof(response) + bodylen << " bytes";
        if (bodylen > 0) {
            out << " (" << sizeof(response) << " bytes header";
            if (response.response.extlen != 0) {
                out << ", " << (unsigned int)response.response.extlen << " byte extras ";
            }
            uint16_t keylen = ntohs(response.response.keylen);
            if (keylen > 0) {
                out << ", " << keylen << " bytes key";
            }
            bodylen -= keylen;
            bodylen -= response.response.extlen;
            if (bodylen > 0) {
                out << " and " << bodylen << " value";
            }
            out << ")";
        }
        out << std::endl << std::endl;
        out.flags(std::ios::hex);
        out << std::setfill('0');
        out << "    Field        (offset) (value)" << std::endl;
        out << "    Magic        (0)    : 0x" << (uint32_t(response.bytes[0]) & 0xff) << std::endl;
        out << "    Opcode       (1)    : 0x" << std::setw(2) << (uint32_t(response.bytes[1]) & 0xff) << std::endl;
        out << "    Key length   (2,3)  : 0x" << std::setw(4) << (ntohs(response.response.keylen) & 0xffff) << std::endl;
        out << "    Extra length (4)    : 0x" << std::setw(2) << (uint32_t(response.bytes[4]) & 0xff) << std::endl;
        out << "    Data type    (5)    : 0x" << std::setw(2) << (uint32_t(response.bytes[5]) & 0xff) << std::endl;
        out << "    Status       (6,7)  : 0x" << std::setw(4) << (ntohs(response.response.status) & 0xffff) << std::endl;
        out << "    Total body   (8-11) : 0x" << std::setw(8) << (uint64_t(ntohl(response.response.bodylen)) & 0xffff) << std::endl;
        out << "    Opaque       (12-15): 0x" << std::setw(8) << response.response.opaque << std::endl;
        out << "    CAS          (16-23): 0x" << std::setw(16) << response.response.cas << std::endl;
        out << std::setfill(' ');
        out.flags(std::ios::dec);
    }

    virtual void dumpExtras(std::ostream &out) const override {
        Frame::dumpExtras(response.bytes + sizeof(response.bytes),
                          response.response.extlen,
                          out);
    }

    virtual void dumpKey(std::ostream &out) const override {
        Frame::dumpKey(
            response.bytes + sizeof(response.bytes) + response.response.extlen,
            ntohs(response.response.keylen),
            out);
    }

    virtual void dumpValue(std::ostream &out) const override {

    }

    const protocol_binary_response_header& response;
};

class HelloResponse : public Response {
public:
    HelloResponse(const protocol_binary_response_header& res)
        : Response(res) {

    }

protected:
    virtual void dumpExtras(std::ostream& out) const override {
        if (response.response.extlen != 0) {
            throw std::logic_error(
                "HelloResponse::dumpExtras(): extlen must be 0");
        }
    }

    virtual void dumpKey(std::ostream& out) const override {
        if (response.response.keylen != 0) {
            throw std::logic_error(
                "HelloResponse::dumpKey(): keylen must be 0");
        }
    }

    virtual void dumpValue(std::ostream& out) const override {
        uint32_t bodylen = ntohl(response.response.bodylen);

        if ((bodylen % 2) != 0) {
            throw std::logic_error(
                "HelloRequest::dumpValue(): bodylen must be in words");
        }

        if (bodylen == 0) {
            return;
        }
        uint32_t first = sizeof(response.bytes);
        out << "    Body                :" << std::endl;

        uint32_t total = bodylen / 2;
        uint32_t offset = sizeof(response.bytes);
        for (uint32_t ii = 0; ii < total; ++ii) {
            uint16_t feature;
            memcpy(&feature, response.bytes + offset, 2);
            offset += 2;
            feature = ntohs(feature);
            const char *txt = protocol_feature_2_text(feature);
            out << "                 (" << first << "-"
            << first + 1 <<"): " << txt << std::endl;
            first += 2;
        }
    }
};

static void dump_request(const protocol_binary_request_header* req,
                         std::ostream& out) {
    switch (req->request.opcode) {
    case PROTOCOL_BINARY_CMD_HELLO:
        out << HelloRequest(*req) << std::endl;
        break;
    default:
        out << Request(*req) << std::endl;
    }
}

static void dump_response(const protocol_binary_response_header* res,
                          std::ostream& out) {
    switch (res->response.opcode) {
    case PROTOCOL_BINARY_CMD_HELLO:
        out << HelloResponse(*res) << std::endl;
        break;
    default:
        out << Response(*res) << std::endl;
    }
}

void Couchbase::MCBP::dump(const uint8_t* packet, std::ostream& out) {
    auto* req = reinterpret_cast<const protocol_binary_request_header*>(packet);
    auto* res = reinterpret_cast<const protocol_binary_response_header*>(packet);

    switch (*packet) {
    case PROTOCOL_BINARY_REQ:
        dump_request(req, out);
        break;
    case PROTOCOL_BINARY_RES:
        dump_response(res, out);
        break;
    default:
        throw std::invalid_argument("Couchbase::MCBP::dump: Invalid magic");
    }
}
