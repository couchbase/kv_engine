/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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
#include <mcbp/mcbp.h>
#include <memcached/dcp_stream_id.h>
#include <memcached/durability_spec.h>
#include <memcached/protocol_binary.h>
#include <platform/byte_buffer_dump.h>
#include <platform/string_hex.h>
#include <utilities/logtags.h>

#include <cctype>
#include <cstring>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <string>

// This file contains the functionality to generate a packet dump
// of a memcached frame
namespace cb {
namespace mcbp {

void dumpBytes(cb::const_byte_buffer buffer, std::ostream& out, size_t offset) {
    out << std::hex;
    size_t nbytes = 0;
    while (nbytes < buffer.size()) {
        out << "0x" << std::setfill('0') << std::setw(8) << offset << "\t";
        offset += 8;
        std::string chars;
        for (int ii = 0; (ii < 8) && (nbytes < buffer.size()); ++ii, ++nbytes) {
            unsigned int val = buffer.data()[nbytes];
            if (std::isprint(val)) {
                chars.push_back(char(val));
            } else {
                chars.push_back('.');
            }
            out << "0x" << std::setw(2) << val << " ";
        }

        // we need to pad out to align the last buffer
        for (auto ii = chars.size(); ii < 8; ++ii) {
            out << "     ";
        }

        out << "    " << chars << std::endl;
    }

    out << std::dec << std::setfill(' ');
}

} // namespace mcbp
} // namespace cb

/**
 * The Frame class represents a complete frame as it is being sent on the
 * wire in the Memcached Binary Protocol.
 */
class McbpFrame {
public:
    explicit McbpFrame(const cb::mcbp::Header& header) : header(header) {
        // empty
    }

    virtual ~McbpFrame() = default;

    void dump(std::ostream& out) const {
        out << cb::const_byte_buffer{reinterpret_cast<const uint8_t*>(&header),
                                     sizeof(header) + header.getBodylen()};
        uint32_t bodylen = header.getBodylen();
        out << "        Total " << sizeof(header) + bodylen << " bytes";
        if (bodylen > 0) {
            out << " (" << sizeof(header) << " bytes header";
            auto fe = header.getFramingExtras();
            if (!fe.empty()) {
                out << ", " << fe.size() << " bytes frame extras";
            }
            auto ext = header.getExtdata();
            if (!ext.empty()) {
                out << ", " << ext.size() << " bytes extras";
            }
            auto key = header.getKey();
            if (!key.empty()) {
                out << ", " << key.size() << " bytes key";
            }
            auto value = header.getValue();
            if (!value.empty()) {
                out << ", " << value.size() << " bytes value";
            }
            out << ")";
        }
        out << std::endl << std::endl;

        dumpPacketInfo(out);
        dumpFrameExtras(out);
        dumpExtras(out);
        dumpKey(out);
        dumpValue(out);
    }

protected:
    virtual void dumpPacketInfo(std::ostream& out) const = 0;

    virtual void dumpFrameExtras(std::ostream& out) const {
        auto fe = header.getFramingExtras();
        if (!fe.empty()) {
            out << "    Framing Extra       : " << fe.size()
                << " bytes of binary data" << std::endl;
        }
    }

    virtual void dumpExtras(std::ostream& out) const {
        auto ext = header.getExtdata();
        if (!ext.empty()) {
            out << "    Extra               : " << ext.size()
                << " bytes of binary data" << std::endl;
        }
    }

    virtual void dumpKey(std::ostream& out) const {
        auto key = header.getKey();
        if (!key.empty()) {
            const ptrdiff_t first =
                    key.data() - reinterpret_cast<const uint8_t*>(&header);
            const ptrdiff_t last = first + key.size() - 1;
            out << "    Key          (" << first << "-" << last << "): ";

            std::string str((const char*)key.data(), key.size());

            for (const auto& c : str) {
                if (!isprint(c)) {
                    out << key.size() << " bytes of binary data" << std::endl;
                    return;
                }
            }
            out << "The textual string \"" << str << "\"" << std::endl;
        }
    }

    virtual void dumpValue(std::ostream& out) const {
        auto value = header.getValue();
        if (!value.empty()) {
            out << "    Value               : " << value.size()
                << " bytes of binary data" << std::endl;
        }
    }

protected:
    const cb::mcbp::Header& header;
};

std::ostream& operator<<(std::ostream& out, const McbpFrame& frame) {
    frame.dump(out);
    return out;
}

class Request : public McbpFrame {
public:
    explicit Request(const cb::mcbp::Request& request)
        : McbpFrame(reinterpret_cast<const cb::mcbp::Header&>(request)),
          request(request) {
        // nothing
    }

protected:
    void dumpPacketInfo(std::ostream& out) const override {
        const auto* bytes = reinterpret_cast<const uint8_t*>(&request);
        out << "    Field        (offset) (value)" << std::endl;
        out << "    Magic        (0)    : " << cb::to_hex(bytes[0]) << " ("
            << to_string(request.getMagic()) << ")" << std::endl;
        out << "    Opcode       (1)    : " << cb::to_hex(bytes[1]) << " (";
        if (cb::mcbp::is_client_magic(request.getMagic())) {
            out << to_string(request.getClientOpcode()) << ")" << std::endl;
        } else {
            out << to_string(request.getServerOpcode()) << ")" << std::endl;
        }

        if (cb::mcbp::is_alternative_encoding(request.getMagic())) {
            out << "    Framing extlen (2)  : "
                << cb::to_hex(request.getFramingExtraslen()) << std::endl;
            out << "    Key length     (3)  : "
                << cb::to_hex(request.getKeylen()) << std::endl;
        } else {
            out << "    Key length   (2,3)  : "
                << cb::to_hex(request.getKeylen()) << std::endl;
        }
        out << "    Extra length (4)    : " << cb::to_hex(bytes[4])
            << std::endl;
        out << "    Data type    (5)    : " << cb::to_hex(bytes[5])
            << std::endl;
        out << "    Vbucket      (6,7)  : "
            << cb::to_hex(request.getVBucket().get()) << std::endl;
        out << "    Total body   (8-11) : " << cb::to_hex(request.getBodylen())
            << std::endl;
        out << "    Opaque       (12-15): " << cb::to_hex(request.getOpaque())
            << std::endl;
        out << "    CAS          (16-23): " << cb::to_hex(request.getCas())
            << std::endl;
    }

    void dumpFrameExtras(std::ostream& out) const override {
        try {
            std::vector<std::string> vector;

            request.parseFrameExtras([&vector](
                                             cb::mcbp::request::FrameInfoId id,
                                             cb::const_byte_buffer payload)
                                             -> bool {
                std::stringstream ss;
                ss << to_string(id);
                switch (id) {
                case cb::mcbp::request::FrameInfoId::Barrier:
                case cb::mcbp::request::FrameInfoId::PreserveTtl:
                    break;
                case cb::mcbp::request::FrameInfoId::DurabilityRequirement:
                    ss << " Level=";
                    {
                        cb::durability::Requirements requirements(payload);
                        switch (requirements.getLevel()) {
                        case cb::durability::Level::None:
                            ss << "None";
                            break;
                        case cb::durability::Level::Majority:
                            ss << "Majority";
                            break;
                        case cb::durability::Level::MajorityAndPersistOnMaster:
                            ss << "MajorityAndPersistOnMaster";
                            break;
                        case cb::durability::Level::PersistToMajority:
                            ss << "PersistToMajority";
                            break;
                        }
                        ss << ", Timeout="
                           << to_string(requirements.getTimeout());
                    }
                    break;
                case cb::mcbp::request::FrameInfoId::DcpStreamId:
                    ss << " DcpStreamId="
                       << ntohs(*reinterpret_cast<const uint16_t*>(
                                  payload.data()));
                    break;
                case cb::mcbp::request::FrameInfoId::OpenTracingContext:
                    ss << " " << cb::to_hex(payload);
                    break;
                case cb::mcbp::request::FrameInfoId::Impersonate:
                    ss << " euid="
                       << std::string{
                                  reinterpret_cast<const char*>(payload.data()),
                                  payload.size()};
                    break;
                }

                vector.emplace_back(ss.str());
                return false;
            });

            if (vector.empty()) {
                return;
            }
            out << "    Framing Extra       : " << std::endl;
            for (const auto& str : vector) {
                out << "                          " << str << std::endl;
            }
        } catch (const std::runtime_error&) {
            McbpFrame::dumpExtras(out);
        }
    }

    const cb::mcbp::Request& request;
};

class HelloRequest : public Request {
public:
    explicit HelloRequest(const cb::mcbp::Request& request) : Request(request) {
        // nothing
    }

protected:
    void dumpValue(std::ostream& out) const override {
        auto value = request.getValue();
        if (value.empty()) {
            return;
        }

        if ((value.size() % 2) != 0) {
            throw std::logic_error(
                    "HelloRequest::dumpValue(): value must be in words");
        }

        cb::sized_buffer<const uint16_t> features = {
                reinterpret_cast<const uint16_t*>(value.data()),
                value.size() / 2};
        ptrdiff_t first =
                value.data() - reinterpret_cast<const uint8_t*>(&header);
        out << "    Body                :" << std::endl;
        for (auto& val : features) {
            std::string text;
            try {
                text = to_string(cb::mcbp::Feature(ntohs(val)));
            } catch (...) {
                text = std::to_string(ntohs(val));
            }

            out << "                 (" << first << "-" << first + 1
                << "): " << text << std::endl;
            first += 2;
        }
    }
};

class SetWithMetaRequest : public Request {
public:
    explicit SetWithMetaRequest(const cb::mcbp::Request& request)
        : Request(request) {
    }

protected:
    std::string decodeOptions(const uint32_t* ptr) const {
        uint32_t value = ntohl(*ptr);

        std::string options;
        if (value & SKIP_CONFLICT_RESOLUTION_FLAG) {
            options.append("skip conflict resolution, ");
        }

        if (value & FORCE_ACCEPT_WITH_META_OPS) {
            options.append("force accept, ");
        }

        if (value & REGENERATE_CAS) {
            options.append("regenerate cas, ");
        }

        // Should I look for others?

        // Remove trailing ", "
        options.pop_back();
        options.pop_back();

        return options;
    }

    void dumpExtras(std::ostream& out) const override {
        auto ext = request.getExtdata();
        if (ext.size() < 24) {
            throw std::logic_error(
                    "SetWithMetaRequest::dumpExtras(): extlen must be at lest "
                    "24 bytes");
        }

        const auto* extras =
                reinterpret_cast<const cb::mcbp::request::SetWithMetaPayload*>(
                        ext.data());

        out << "    Extras" << std::endl;
        out << "        flags    (24-27): "
            << cb::to_hex(extras->getFlagsInNetworkByteOrder()) << std::endl;
        out << "        exptime  (28-31): "
            << cb::to_hex(extras->getExpiration()) << std::endl;
        out << "        seqno    (32-39): " << cb::to_hex(extras->getSeqno())
            << std::endl;
        out << "        cas      (40-47): " << cb::to_hex(extras->getCas())
            << std::endl;

        const uint16_t* nmeta_ptr;
        const uint32_t* options_ptr;

        switch (ext.size()) {
        case 24: // no nmeta and no options
            break;
        case 26: // nmeta
            nmeta_ptr = reinterpret_cast<const uint16_t*>(ext.data() + 24);
            out << "        nmeta     (48-49): "
                << cb::to_hex(uint16_t(ntohs(*nmeta_ptr))) << std::endl;
            break;
        case 28: // options (4-byte field)
        case 30: // options and nmeta (options followed by nmeta)
            options_ptr = reinterpret_cast<const uint32_t*>(ext.data() + 24);
            out << "        options  (48-51): "
                << cb::to_hex(uint32_t(ntohl(*options_ptr))) << std::endl;
            if (*options_ptr != 0) {
                out << " (" << decodeOptions(options_ptr) << ")";
            }
            out << std::endl;
            if (ext.size() == 28) {
                break;
            }
            nmeta_ptr = reinterpret_cast<const uint16_t*>(ext.data() + 28);
            out << "        nmeta     (52-53): "
                << cb::to_hex(uint16_t(ntohs(*nmeta_ptr))) << std::endl;
            break;
        default:
            throw std::logic_error(
                    "SetWithMetaRequest::dumpExtras(): Invalid extlen");
        }

        out << std::setfill(' ');
    }
};

class Response : public McbpFrame {
public:
    explicit Response(const cb::mcbp::Response& response)
        : McbpFrame(reinterpret_cast<const cb::mcbp::Header&>(response)),
          response(response) {
        // nothing
    }

protected:
    void dumpPacketInfo(std::ostream& out) const override {
        const auto* bytes = reinterpret_cast<const uint8_t*>(&response);
        out << "    Field        (offset) (value)" << std::endl;
        out << "    Magic        (0)    : " << cb::to_hex(bytes[0]) << " ("
            << to_string(response.getMagic()) << ")" << std::endl;
        out << "    Opcode       (1)    : " << cb::to_hex(bytes[1]) << " (";
        if (cb::mcbp::is_client_magic(response.getMagic())) {
            out << to_string(response.getClientOpcode()) << ")" << std::endl;
        } else {
            out << to_string(response.getServerOpcode()) << ")" << std::endl;
        }
        if (cb::mcbp::is_alternative_encoding(response.getMagic())) {
            out << "    Framing extlen (2)  : "
                << cb::to_hex(response.getFramingExtraslen()) << std::endl;
            out << "    Key length     (3)  : "
                << cb::to_hex(response.getKeylen()) << std::endl;
        } else {
            out << "    Key length   (2,3)  : "
                << cb::to_hex(response.getKeylen()) << std::endl;
        }
        out << "    Extra length (4)    : " << cb::to_hex(bytes[4])
            << std::endl;
        out << "    Data type    (5)    : " << cb::to_hex(bytes[5])
            << std::endl;
        out << "    Status       (6,7)  : "
            << cb::to_hex(uint16_t(response.getStatus())) << " ("
            << to_string(response.getStatus()) << ")" << std::endl;
        out << "    Total body   (8-11) : " << cb::to_hex(response.getBodylen())
            << std::endl;
        out << "    Opaque       (12-15): " << cb::to_hex(response.getOpaque())
            << std::endl;
        out << "    CAS          (16-23): " << cb::to_hex(response.getCas())
            << std::endl;
    }

    const cb::mcbp::Response& response;
};

class HelloResponse : public Response {
public:
    explicit HelloResponse(const cb::mcbp::Response& response)
        : Response(response) {
    }

protected:
    void dumpValue(std::ostream& out) const override {
        if (cb::mcbp::isStatusSuccess(response.getStatus())) {
            dumpSuccessValue(out);
        } else {
            dumpFailureValue(out);
        }
    }

    void dumpSuccessValue(std::ostream& out) const {
        auto value = response.getValue();
        if (value.empty()) {
            return;
        }

        if ((value.size() % 2) != 0) {
            throw std::logic_error(
                    "HelloResponse::dumpValue(): value must be in words");
        }

        cb::sized_buffer<const uint16_t> features = {
                reinterpret_cast<const uint16_t*>(value.data()),
                value.size() / 2};
        ptrdiff_t first =
                value.data() - reinterpret_cast<const uint8_t*>(&header);
        out << "    Body                :" << std::endl;
        for (auto& val : features) {
            std::string text;
            try {
                text = to_string(cb::mcbp::Feature(ntohs(val)));
            } catch (...) {
                text = std::to_string(ntohs(val));
            }

            out << "                 (" << first << "-" << first + 1
                << "): " << text << std::endl;
            first += 2;
        }
    }

    void dumpFailureValue(std::ostream& out) const {
        auto value = response.getValue();
        const std::string val = {reinterpret_cast<const char*>(value.data()),
                                 value.size()};
        out << "    Body                : The textual string \"" << val << "\""
            << std::endl;
    }
};

class ListBucketsResponse : public Response {
public:
    explicit ListBucketsResponse(const cb::mcbp::Response& res)
        : Response(res) {
    }

protected:
    void dumpValue(std::ostream& out) const override {
        const auto value = response.getValue();
        std::string buckets(reinterpret_cast<const char*>(value.data()),
                            value.size());
        out << "    Body                :" << std::endl;

        std::string::size_type start = 0;
        std::string::size_type end;

        while ((end = buckets.find(' ', start)) != std::string::npos) {
            const auto b = buckets.substr(start, end - start);
            out << "                        : " << b << std::endl;
            start = end + 1;
        }
        if (start < buckets.size()) {
            out << "                        : " << buckets.substr(start)
                << std::endl;
        }
    }
};

static void dump_request(const cb::mcbp::Request& request, std::ostream& out) {
    switch (request.getClientOpcode()) {
    case cb::mcbp::ClientOpcode::Hello:
        out << HelloRequest(request) << std::endl;
        break;
    case cb::mcbp::ClientOpcode::SetWithMeta:
    case cb::mcbp::ClientOpcode::SetqWithMeta:
        out << SetWithMetaRequest(request) << std::endl;
        break;
    default:
        out << Request(request) << std::endl;
    }
}

static void dump_response(const cb::mcbp::Response& response,
                          std::ostream& out) {
    switch (response.getClientOpcode()) {
    case cb::mcbp::ClientOpcode::Hello:
        out << HelloResponse(response) << std::endl;
        break;
    case cb::mcbp::ClientOpcode::ListBuckets:
        out << ListBucketsResponse(response) << std::endl;
        break;
    default:
        out << Response(response) << std::endl;
    }
}

void cb::mcbp::dump(const cb::mcbp::Header& header, std::ostream& out) {
    if (header.isValid()) {
        if (header.isRequest()) {
            dump_request(header.getRequest(), out);
        } else {
            dump_response(header.getResponse(), out);
        }
    } else {
        throw std::invalid_argument("cb::mcbp::dump: Invalid magic");
    }
}

void cb::mcbp::dump(const uint8_t* packet, std::ostream& out) {
    dump(*reinterpret_cast<const cb::mcbp::Header*>(packet), out);
}

void cb::mcbp::dumpStream(cb::const_byte_buffer buffer, std::ostream& out) {
    size_t offset = 0;

    while ((offset + sizeof(Header)) <= buffer.size()) {
        // Check to see if we've got the entire next packet available
        auto* header = reinterpret_cast<const Header*>(buffer.data() + offset);
        if (!header->isValid()) {
            std::stringstream ss;
            ss << "Invalid magic at offset: " << offset
               << ". Dumping next header" << std::endl;
            for (int ii = 0; ii < 68; ++ii) {
                ss << "-";
            }
            ss << std::endl;
            dumpBytes({buffer.data() + offset, 24}, ss, offset);
            for (int ii = 0; ii < 68; ++ii) {
                ss << "-";
            }
            ss << std::endl;

            throw std::invalid_argument(ss.str());
        }
        offset += sizeof(*header) + header->getBodylen();
        if (offset > buffer.size()) {
            out << "Last frame truncated..." << std::endl;
            return;
        }

        dump(*header, out);
    }
}
