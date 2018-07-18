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
#include "client_mcbp_commands.h"
#include <mcbp/mcbp.h>
#include <array>
#include <gsl/gsl>
#include "tracing/tracer.h"

void BinprotCommand::encode(std::vector<uint8_t>& buf) const {
    protocol_binary_request_header header;
    fillHeader(header);
    buf.insert(
            buf.end(), header.bytes, &header.bytes[0] + sizeof(header.bytes));
}

BinprotCommand::Encoded BinprotCommand::encode() const {
    Encoded bufs;
    encode(bufs.header);
    return bufs;
}

void BinprotCommand::fillHeader(protocol_binary_request_header& header,
                                size_t payload_len,
                                size_t extlen) const {
    header.request.magic = PROTOCOL_BINARY_REQ;
    header.request.opcode = opcode;
    header.request.keylen = htons(gsl::narrow<uint16_t>(key.size()));
    header.request.extlen = gsl::narrow<uint8_t>(extlen);
    header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    header.request.vbucket = htons(vbucket);
    header.request.bodylen =
            htonl(gsl::narrow<uint32_t>(key.size() + extlen + payload_len));
    header.request.opaque = 0xdeadbeef;
    header.request.cas = cas;
}

void BinprotCommand::writeHeader(std::vector<uint8_t>& buf,
                                 size_t payload_len,
                                 size_t extlen) const {
    protocol_binary_request_header* hdr;
    buf.resize(sizeof(hdr->bytes));
    hdr = reinterpret_cast<protocol_binary_request_header*>(buf.data());
    fillHeader(*hdr, payload_len, extlen);
}

void BinprotGenericCommand::encode(std::vector<uint8_t>& buf) const {
    writeHeader(buf, value.size(), extras.size());
    buf.insert(buf.end(), extras.begin(), extras.end());
    buf.insert(buf.end(), key.begin(), key.end());
    buf.insert(buf.end(), value.begin(), value.end());
}

BinprotSubdocCommand::BinprotSubdocCommand(protocol_binary_command cmd_,
                                           const std::string& key_,
                                           const std::string& path_,
                                           const std::string& value_,
                                           protocol_binary_subdoc_flag pathFlags_,
                                           mcbp::subdoc::doc_flag docFlags_,
                                           uint64_t cas_)
    : BinprotCommandT() {
    setOp(cmd_);
    setKey(key_);
    setPath(path_);
    setValue(value_);
    addPathFlags(pathFlags_);
    addDocFlags(docFlags_);
    setCas(cas_);
}

BinprotSubdocCommand& BinprotSubdocCommand::setPath(const std::string& path_) {
    if (path.size() > std::numeric_limits<uint16_t>::max()) {
        throw std::out_of_range("BinprotSubdocCommand::setPath: Path too big");
    }
    path = path_;
    return *this;
}

void BinprotSubdocCommand::encode(std::vector<uint8_t>& buf) const {
    if (key.empty()) {
        throw std::logic_error("BinprotSubdocCommand::encode: Missing a key");
    }

    protocol_binary_request_subdocument request;
    memset(&request, 0, sizeof(request));

    // Expiry (optional) is encoded in extras. Only include if non-zero or
    // if explicit encoding of zero was requested.
    const bool include_expiry = (expiry.getValue() != 0 || expiry.isSet());
    const bool include_doc_flags = !isNone(doc_flags);

    // Populate the header.
    const size_t extlen = sizeof(uint16_t) + // Path length
                          1 + // flags
                          (include_expiry ? sizeof(uint32_t) : 0) +
                          (include_doc_flags ? sizeof(uint8_t) : 0);

    fillHeader(request.message.header, path.size() + value.size(), extlen);

    // Add extras: pathlen, flags, optional expiry
    request.message.extras.pathlen = htons(gsl::narrow<uint16_t>(path.size()));
    request.message.extras.subdoc_flags = flags;
    buf.insert(buf.end(),
               request.bytes,
               &request.bytes[0] + sizeof(request.bytes));

    if (include_expiry) {
        // As expiry is optional (and immediately follows subdoc_flags,
        // i.e. unaligned) there's no field in the struct; so use low-level
        // memcpy to populate it.
        uint32_t encoded_expiry = htonl(expiry.getValue());
        char* expbuf = reinterpret_cast<char*>(&encoded_expiry);
        buf.insert(buf.end(), expbuf, expbuf + sizeof encoded_expiry);
    }

    if (include_doc_flags) {
        const uint8_t* doc_flag_ptr =
                reinterpret_cast<const uint8_t*>(&doc_flags);
        buf.insert(buf.end(), doc_flag_ptr, doc_flag_ptr + sizeof(uint8_t));
    }

    // Add Body: key; path; value if applicable.
    buf.insert(buf.end(), key.begin(), key.end());
    buf.insert(buf.end(), path.begin(), path.end());
    buf.insert(buf.end(), value.begin(), value.end());
}

void BinprotResponse::assign(std::vector<uint8_t>&& srcbuf) {
    payload = std::move(srcbuf);
}

boost::optional<std::chrono::microseconds> BinprotResponse::getTracingData()
        const {
    auto framingExtrasLen = getFramingExtraslen();
    if (framingExtrasLen == 0) {
        return boost::optional<std::chrono::microseconds>{};
    }
    const auto& framingExtras = getResponse().getFramingExtras();
    const auto& data = framingExtras.data();
    size_t offset = 0;

    // locate the tracing info
    while (offset < framingExtrasLen) {
        const uint8_t id = data[offset] & 0xF0;
        const uint8_t len = data[offset] & 0x0F;
        if (0 == id) {
            uint16_t micros = ntohs(
                    reinterpret_cast<const uint16_t*>(data + offset + 1)[0]);
            return cb::tracing::Tracer::decodeMicros(micros);
        }
        offset += 1 + len;
    }

    return boost::optional<std::chrono::microseconds>{};
}

void BinprotSubdocResponse::assign(std::vector<uint8_t>&& srcbuf) {
    BinprotResponse::assign(std::move(srcbuf));
    if (getBodylen() - getExtlen() - getFramingExtraslen() > 0) {
        value.assign(payload.data() + sizeof(protocol_binary_response_header) +
                             getExtlen() + getFramingExtraslen(),
                     payload.data() + payload.size());
    }
}

void BinprotSaslAuthCommand::encode(std::vector<uint8_t>& buf) const {
    if (key.empty()) {
        throw std::logic_error("BinprotSaslAuthCommand: Missing mechanism (setMechanism)");
    }

    writeHeader(buf, challenge.size(), 0);
    buf.insert(buf.end(), key.begin(), key.end());
    buf.insert(buf.end(), challenge.begin(), challenge.end());
}

void BinprotSaslStepCommand::encode(std::vector<uint8_t>& buf) const {
    if (key.empty()) {
        throw std::logic_error("BinprotSaslStepCommand::encode: Missing mechanism (setMechanism");
    }
    if (challenge.empty()) {
        throw std::logic_error("BinprotSaslStepCommand::encode: Missing challenge response");
    }

    writeHeader(buf, challenge.size(), 0);
    buf.insert(buf.end(), key.begin(), key.end());
    buf.insert(buf.end(), challenge.begin(), challenge.end());
}

void BinprotCreateBucketCommand::setConfig(const std::string& module,
                                           const std::string& config) {
    module_config.assign(module.begin(), module.end());
    module_config.push_back(0x00);
    module_config.insert(module_config.end(), config.begin(), config.end());
}

void BinprotCreateBucketCommand::encode(std::vector<uint8_t>& buf) const {
    if (module_config.empty()) {
        throw std::logic_error("BinprotCreateBucketCommand::encode: Missing bucket module and config");
    }
    writeHeader(buf, module_config.size(), 0);
    buf.insert(buf.end(), key.begin(), key.end());
    buf.insert(buf.end(), module_config.begin(), module_config.end());
}

void BinprotGetCommand::encode(std::vector<uint8_t>& buf) const {
    writeHeader(buf, 0, 0);
    buf.insert(buf.end(), key.begin(), key.end());
}

void BinprotGetAndLockCommand::encode(std::vector<uint8_t>& buf) const {
    writeHeader(buf, 0, sizeof(lock_timeout));
    protocol_binary_request_getl *req;
    buf.resize(sizeof(req->bytes));
    req = reinterpret_cast<protocol_binary_request_getl*>(buf.data());
    req->message.body.expiration = htonl(lock_timeout);
    buf.insert(buf.end(), key.begin(), key.end());
}

void BinprotGetAndTouchCommand::encode(std::vector<uint8_t>& buf) const {
    writeHeader(buf, 0, sizeof(expirytime));
    protocol_binary_request_gat *req;
    buf.resize(sizeof(req->bytes));
    req = reinterpret_cast<protocol_binary_request_gat*>(buf.data());
    req->message.body.expiration = htonl(expirytime);
    buf.insert(buf.end(), key.begin(), key.end());
}

void BinprotTouchCommand::encode(std::vector<uint8_t>& buf) const {
    writeHeader(buf, 0, sizeof(expirytime));
    protocol_binary_request_touch *req;
    buf.resize(sizeof(req->bytes));
    req = reinterpret_cast<protocol_binary_request_touch*>(buf.data());
    req->message.body.expiration = htonl(expirytime);
    buf.insert(buf.end(), key.begin(), key.end());
}

void BinprotUnlockCommand::encode(std::vector<uint8_t>& buf) const {
    writeHeader(buf, 0, 0);
    buf.insert(buf.end(), key.begin(), key.end());
}

uint32_t BinprotGetResponse::getDocumentFlags() const {
    if (!isSuccess()) {
        return 0;
    }
    return ntohl(*reinterpret_cast<const uint32_t*>(getPayload()));
}

BinprotMutationCommand& BinprotMutationCommand::setMutationType(
    MutationType type) {
    switch (type) {
    case MutationType::Add:
        setOp(PROTOCOL_BINARY_CMD_ADD);
        return *this;
    case MutationType::Set:
        setOp(PROTOCOL_BINARY_CMD_SET);
        return *this;
    case MutationType::Replace:
        setOp(PROTOCOL_BINARY_CMD_REPLACE);
        return *this;
    case MutationType::Append:
        setOp(PROTOCOL_BINARY_CMD_APPEND);
        return *this;
    case MutationType::Prepend:
        setOp(PROTOCOL_BINARY_CMD_PREPEND);
        return *this;
    }

    throw std::invalid_argument(
        "BinprotMutationCommand::setMutationType: Mutation type not supported: " +
        std::to_string(int(type)));
}

std::string to_string(MutationType type) {
    switch (type) {
    case MutationType::Add: return "ADD";
    case MutationType::Set: return "SET";
    case MutationType::Replace: return "REPLACE";
    case MutationType::Append: return "APPEND";
    case MutationType::Prepend: return "PREPEND";
    }

    return "to_string(MutationType type) Unknown type: " +
           std::to_string(int(type));
}

BinprotMutationCommand& BinprotMutationCommand::setDocumentInfo(
        const DocumentInfo& info) {
    if (!info.id.empty()) {
        setKey(info.id);
    }

    setDocumentFlags(info.flags);
    setCas(info.cas);
    setExpiry(info.expiration);

    datatype = uint8_t(info.datatype);
    return *this;
}

void BinprotMutationCommand::encodeHeader(std::vector<uint8_t>& buf) const {
    if (key.empty()) {
        throw std::invalid_argument("BinprotMutationCommand::encode: Key is missing!");
    }
    if (!value.empty() && !value_refs.empty()) {
        throw std::invalid_argument("BinprotMutationCommand::encode: Both value and value_refs have items!");
    }

    uint8_t extlen = 8;

    protocol_binary_request_header *header;
    buf.resize(sizeof(header->bytes));
    header = reinterpret_cast<protocol_binary_request_header*>(buf.data());

    if (getOp() == PROTOCOL_BINARY_CMD_APPEND ||
        getOp() == PROTOCOL_BINARY_CMD_PREPEND) {
        if (expiry.getValue() != 0) {
            throw std::invalid_argument("BinprotMutationCommand::encode: Expiry invalid with append/prepend");
        }
        extlen = 0;
    }

    size_t value_size = value.size();
    for (const auto& vbuf : value_refs) {
        value_size += vbuf.size();
    }

    fillHeader(*header, value_size, extlen);
    header->request.datatype = datatype;

    if (extlen != 0) {
        // Write the extras:

        protocol_binary_request_set *req;
        buf.resize(sizeof(req->bytes));
        req = reinterpret_cast<protocol_binary_request_set*>(buf.data());

        req->message.body.expiration = htonl(expiry.getValue());
        req->message.body.flags = htonl(flags);
    }
}

void BinprotMutationCommand::encode(std::vector<uint8_t>& buf) const {
    encodeHeader(buf);
    buf.insert(buf.end(), key.begin(), key.end());
    buf.insert(buf.end(), value.begin(), value.end());
    for (const auto& vbuf : value_refs) {
        buf.insert(buf.end(), vbuf.begin(), vbuf.end());
    }
}

BinprotCommand::Encoded BinprotMutationCommand::encode() const {
    Encoded ret;
    auto& hdrbuf = ret.header;
    encodeHeader(hdrbuf);
    hdrbuf.insert(hdrbuf.end(), key.begin(), key.end());
    hdrbuf.insert(hdrbuf.end(), value.begin(), value.end());

    ret.bufs.assign(value_refs.begin(), value_refs.end());
    return ret;
}

void BinprotMutationResponse::assign(std::vector<uint8_t>&& buf) {
    BinprotResponse::assign(std::move(buf));

    if (!isSuccess()) {
        // No point parsing the other info..
        return;
    }

    mutation_info.cas = getCas();
    mutation_info.size = 0; // TODO: what's this?

    if (getExtlen() == 0) {
        mutation_info.vbucketuuid = 0;
        mutation_info.seqno = 0;
    } else if (getExtlen() == 16) {
        auto const* bufs = reinterpret_cast<const uint64_t*>(
                getPayload() + getFramingExtraslen());
        mutation_info.vbucketuuid = ntohll(bufs[0]);
        mutation_info.seqno = ntohll(bufs[1]);
    } else {
        throw std::runtime_error("BinprotMutationResponse::assign: Bad extras length");
    }
}

void BinprotHelloCommand::encode(std::vector<uint8_t>& buf) const {
    writeHeader(buf, features.size() * 2, 0);
    buf.insert(buf.end(), key.begin(), key.end());

    for (auto f : features) {
        uint16_t enc = htons(f);
        const char* p = reinterpret_cast<const char*>(&enc);
        buf.insert(buf.end(), p, p + 2);
    }
}

void BinprotHelloResponse::assign(std::vector<uint8_t>&& buf) {
    BinprotResponse::assign(std::move(buf));

    if (isSuccess()) {
        // Ensure body length is even
        if (((getBodylen() - getFramingExtraslen()) & 1) != 0) {
            throw std::runtime_error(
                    "BinprotHelloResponse::assign: "
                    "Invalid response returned. "
                    "Uneven body length");
        }

        auto const* end =
                reinterpret_cast<const uint16_t*>(getPayload() + getBodylen());
        auto const* cur = reinterpret_cast<const uint16_t*>(
                begin() + getResponse().getValueOffset());

        for (; cur != end; ++cur) {
            features.push_back(cb::mcbp::Feature(htons(*cur)));
        }
    }
}

void BinprotIncrDecrCommand::encode(std::vector<uint8_t>& buf) const {
    writeHeader(buf, 0, 20);
    if (getOp() != PROTOCOL_BINARY_CMD_DECREMENT &&
        getOp() != PROTOCOL_BINARY_CMD_INCREMENT) {
        throw std::invalid_argument(
                "BinprotIncrDecrCommand::encode: Invalid opcode. Need INCREMENT or DECREMENT");
    }

    // Write the delta
    for (auto n : std::array<uint64_t, 2>{{delta, initial}}) {
        uint64_t tmp = htonll(n);
        auto const* p = reinterpret_cast<const char*>(&tmp);
        buf.insert(buf.end(), p, p + 8);
    }

    uint32_t exptmp = htonl(expiry.getValue());
    auto const* p = reinterpret_cast<const char*>(&exptmp);
    buf.insert(buf.end(), p, p + 4);
    buf.insert(buf.end(), key.begin(), key.end());
}

void BinprotIncrDecrResponse::assign(std::vector<uint8_t>&& buf) {
    BinprotMutationResponse::assign(std::move(buf));
    // Assign the value:
    if (isSuccess()) {
        value = htonll(*reinterpret_cast<const uint64_t*>(getData().data()));
    } else {
        value = 0;
    }
}

void BinprotRemoveCommand::encode(std::vector<uint8_t>& buf) const {
    writeHeader(buf, 0, 0);
    buf.insert(buf.end(), key.begin(), key.end());
}

void BinprotGetErrorMapCommand::encode(std::vector<uint8_t>& buf) const {
    writeHeader(buf, 2, 0);
    uint16_t encversion = htons(version);
    const char* p = reinterpret_cast<const char*>(&encversion);
    buf.insert(buf.end(), p, p + 2);
}

void BinprotSubdocMultiMutationCommand::encode(std::vector<uint8_t>& buf) const {
    // Calculate the size of the payload
    size_t total = 0;
    for (const auto& spec : specs) {
        // Accorrding to the spec the payload should be encoded as:
        //  1 @0         : Opcode
        //  1 @1         : Flags
        //  2 @2         : Path Length
        //  4 @4         : Value Length
        //  pathlen @8         : Path
        //  vallen @8+pathlen  : Value
        total += 1 + 1 + 2 + 4 + spec.path.size() + spec.value.size();
    }

    const uint8_t extlen =
            (expiry.isSet() ? 4 : 0) + (!isNone(docFlags) ? 1 : 0);
    writeHeader(buf, total, extlen);
    if (expiry.isSet()) {
        uint32_t expbuf = htonl(expiry.getValue());
        const char* p = reinterpret_cast<const char*>(&expbuf);
        buf.insert(buf.end(), p, p + 4);
    }
    if (!isNone(docFlags)) {
        const uint8_t* doc_flag_ptr =
                reinterpret_cast<const uint8_t*>(&docFlags);
        buf.insert(buf.end(), doc_flag_ptr, doc_flag_ptr + sizeof(uint8_t));
    }

    buf.insert(buf.end(), key.begin(), key.end());

    // Time to add the data:
    for (const auto& spec : specs) {
        buf.push_back(uint8_t(spec.opcode));
        buf.push_back(uint8_t(spec.flags));
        uint16_t pathlen = ntohs(gsl::narrow<uint16_t>(spec.path.size()));
        const char* p = reinterpret_cast<const char*>(&pathlen);
        buf.insert(buf.end(), p, p + 2);
        uint32_t vallen = ntohl(gsl::narrow<uint32_t>(spec.value.size()));
        p = reinterpret_cast<const char*>(&vallen);
        buf.insert(buf.end(), p, p + 4);
        buf.insert(buf.end(), spec.path.begin(), spec.path.end());
        buf.insert(buf.end(), spec.value.begin(), spec.value.end());
    }
}

void BinprotSubdocMultiMutationResponse::assign(std::vector<uint8_t>&& buf) {
    BinprotResponse::assign(std::move(buf));
    switch (getStatus()) {
    case PROTOCOL_BINARY_RESPONSE_SUCCESS:
    case PROTOCOL_BINARY_RESPONSE_SUBDOC_MULTI_PATH_FAILURE:
        break;
    default:
        return;
    }

    const uint8_t* bufcur = getData().data();
    const uint8_t* bufend = getData().data() + getData().size();

    // Result spec is:
    // 1@0          : Request Index
    // 2@1          : Status
    // 4@3          : Value length -- ONLY if status is success
    // $ValueLen@7  : Value

    while (bufcur < bufend) {
        uint8_t index = *bufcur;
        bufcur += 1;

        uint16_t cur_status = ntohs(*reinterpret_cast<const uint16_t*>(bufcur));
        bufcur += 2;

        if (cur_status == PROTOCOL_BINARY_RESPONSE_SUCCESS) {
            uint32_t cur_len =
                    ntohl(*reinterpret_cast<const uint32_t*>(bufcur));
            bufcur += 4;
            if (cur_len > bufend - bufcur) {
                throw std::runtime_error(
                        "BinprotSubdocMultiMutationResponse::assign(): "
                        "Invalid value length received");
            }
            results.emplace_back(MutationResult{
                    index,
                    protocol_binary_response_status(cur_status),
                    std::string(reinterpret_cast<const char*>(bufcur),
                                cur_len)});
            bufcur += cur_len;
        } else {
            results.emplace_back(MutationResult{
                    index, protocol_binary_response_status(cur_status)});
        }
    }
}

void BinprotSubdocMultiLookupCommand::encode(std::vector<uint8_t>& buf) const {
    size_t total = 0;
    // Payload is to be encoded as:
    // 1 @0         : Opcode
    // 1 @1         : Flags
    // 2 @2         : Path Length
    // $pathlen @4  : Path
    for (const auto& spec : specs) {
        total += 1 + 1 + 2 + spec.path.size();
    }

    const uint8_t extlen =
            (expiry.isSet() ? 4 : 0) + (!isNone(docFlags) ? 1 : 0);
    writeHeader(buf, total, extlen);

    // Note: Expiry isn't supported for multi lookups, but we specifically
    // test for it, and therefore allowed at the API level
    if (expiry.isSet()) {
        uint32_t expbuf = htonl(expiry.getValue());
        const char* p = reinterpret_cast<const char*>(&expbuf);
        buf.insert(buf.end(), p, p + 4);
    }
    if (!isNone(docFlags)) {
        const uint8_t* doc_flag_ptr =
                reinterpret_cast<const uint8_t*>(&docFlags);
        buf.insert(buf.end(), doc_flag_ptr, doc_flag_ptr + sizeof(uint8_t));
    }

    buf.insert(buf.end(), key.begin(), key.end());

    // Add the lookup specs themselves:
    for (const auto& spec : specs) {
        buf.push_back(uint8_t(spec.opcode));
        buf.push_back(uint8_t(spec.flags));

        uint16_t pathlen = ntohs(gsl::narrow<uint16_t>(spec.path.size()));
        const char* p = reinterpret_cast<const char*>(&pathlen);
        buf.insert(buf.end(), p, p + 2);
        buf.insert(buf.end(), spec.path.begin(), spec.path.end());
    }
}

void BinprotSubdocMultiLookupResponse::assign(std::vector<uint8_t>&& buf) {
    BinprotResponse::assign(std::move(buf));
    // Check if this is a success - either full or partial.
    switch (getStatus()) {
    case PROTOCOL_BINARY_RESPONSE_SUCCESS:
    case PROTOCOL_BINARY_RESPONSE_SUBDOC_MULTI_PATH_FAILURE:
    case PROTOCOL_BINARY_RESPONSE_SUBDOC_MULTI_PATH_FAILURE_DELETED:
        break;
    default:
        return;
    }

    const uint8_t* bufcur = getData().data();
    const uint8_t* bufend = getData().data() + getData().size();

    // Result spec is:
    // 2@0          : Status
    // 4@0          : Value Length
    // $ValueLen@6  : Value

    while (bufcur < bufend) {
        uint16_t cur_status = ntohs(*reinterpret_cast<const uint16_t*>(bufcur));
        bufcur += 2;

        uint32_t cur_len = ntohl(*reinterpret_cast<const uint32_t*>(bufcur));
        bufcur += 4;

        results.emplace_back(LookupResult{
                protocol_binary_response_status(cur_status),
                std::string(reinterpret_cast<const char*>(bufcur), cur_len)});
        bufcur += cur_len;
    }
}

void BinprotGetCmdTimerCommand::encode(std::vector<uint8_t>& buf) const {
    writeHeader(buf, 0, 1);
    buf.push_back(opcode);
    buf.insert(buf.end(), key.begin(), key.end());
}

void BinprotGetCmdTimerResponse::assign(std::vector<uint8_t>&& buf) {
    BinprotResponse::assign(std::move(buf));
    if (isSuccess()) {
        timings.reset(cJSON_Parse(getDataString().c_str()));
        if (!timings) {
            throw std::runtime_error("BinprotGetCmdTimerResponse::assign: Invalid payload returned");
        }
    }
}

void BinprotVerbosityCommand::encode(std::vector<uint8_t>& buf) const {
    writeHeader(buf, 0, 4);
    uint32_t value = ntohl(level);
    auto const* p = reinterpret_cast<const char*>(&value);
    buf.insert(buf.end(), p, p + 4);
}

/**
 * Append a 16 bit integer to the buffer in network byte order
 *
 * @param buf the buffer to add the data to
 * @param value The value (in host local byteorder) to add.
 */
static void append(std::vector<uint8_t>& buf, uint16_t value) {
    uint16_t vallen = ntohs(value);
    auto p = reinterpret_cast<const char*>(&vallen);
    buf.insert(buf.end(), p, p + 2);
}

/**
 * Append a 32 bit integer to the buffer in network byte order
 *
 * @param buf the buffer to add the data to
 * @param value The value (in host local byteorder) to add.
 */
static void append(std::vector<uint8_t>& buf, uint32_t value) {
    uint32_t vallen = ntohl(value);
    auto p = reinterpret_cast<const char*>(&vallen);
    buf.insert(buf.end(), p, p + 4);
}

static uint8_t netToHost(uint8_t x) {
    return x;
}

static uint16_t netToHost(uint16_t x) {
    return ntohs(x);
}

static uint64_t netToHost(uint64_t x) {
    return ntohll(x);
}

/**
 * Extract the specified type from the buffer position. Returns an iterator
 * to the next element after the type extracted.
 */
template <typename T>
static std::vector<uint8_t>::iterator extract(
        std::vector<uint8_t>::iterator pos, T& value) {
    auto* p = reinterpret_cast<T*>(&*pos);
    value = netToHost(*p);
    return pos + sizeof(T);
}

/**
 * Append a 64 bit integer to the buffer in network byte order
 *
 * @param buf the buffer to add the data to
 * @param value The value (in host local byteorder) to add.
 */
void append(std::vector<uint8_t>& buf, uint64_t value) {
    uint64_t vallen = htonll(value);
    auto p = reinterpret_cast<const char*>(&vallen);
    buf.insert(buf.end(), p, p + 8);
}

void BinprotDcpOpenCommand::encode(std::vector<uint8_t>& buf) const {
    writeHeader(buf, 0, 8);
    append(buf, seqno);
    append(buf, flags);
    buf.insert(buf.end(), key.begin(), key.end());
}

void BinprotDcpStreamRequestCommand::encode(std::vector<uint8_t>& buf) const {
    writeHeader(buf, 0, 48);
    append(buf, dcp_flags);
    append(buf, dcp_reserved);
    append(buf, dcp_start_seqno);
    append(buf, dcp_end_seqno);
    append(buf, dcp_vbucket_uuid);
    append(buf, dcp_snap_start_seqno);
    append(buf, dcp_snap_end_seqno);
}

void BinprotDcpMutationCommand::reset(const std::vector<uint8_t>& packet) {
    clear();
    const auto* cmd = reinterpret_cast<const protocol_binary_request_dcp_mutation*>(packet.data());
    if (cmd->message.header.request.magic != uint8_t(PROTOCOL_BINARY_REQ)) {
        throw std::invalid_argument(
            "BinprotDcpMutationCommand::reset: packet is not a request");
    }

    by_seqno = ntohll(cmd->message.body.by_seqno);
    rev_seqno = ntohll(cmd->message.body.rev_seqno);
    flags = ntohl(cmd->message.body.flags);
    expiration = ntohl(cmd->message.body.expiration);
    lock_time = ntohl(cmd->message.body.lock_time);
    nmeta = ntohs(cmd->message.body.nmeta);
    nru = cmd->message.body.nru;

    setOp(PROTOCOL_BINARY_CMD_DCP_MUTATION);
    setVBucket(cmd->message.header.request.vbucket);
    setCas(cmd->message.header.request.cas);

    const char* ptr = reinterpret_cast<const char*>(cmd->bytes);
    // Non-collection aware DCP mutation, so pass false to getHeaderLength
    ptr += protocol_binary_request_dcp_mutation::getHeaderLength();

    const auto keylen = cmd->message.header.request.keylen;
    const auto bodylen = cmd->message.header.request.bodylen;
    const auto vallen = bodylen - keylen - cmd->message.header.request.extlen;

    setKey(std::string{ptr, keylen});
    ptr += keylen;
    setValue(std::string{ptr, vallen});
}

void BinprotDcpMutationCommand::encode(std::vector<uint8_t>& buf) const {
    throw std::runtime_error(
        "BinprotDcpMutationCommand::encode: not implemented");
}

void BinprotSetParamCommand::encode(std::vector<uint8_t>& buf) const {
    writeHeader(buf, value.size(), 4);
    append(buf, uint32_t(type));
    buf.insert(buf.end(), key.begin(), key.end());
    buf.insert(buf.end(), value.begin(), value.end());
}

void BinprotSetWithMetaCommand::encode(std::vector<uint8_t>& buf) const {
    protocol_binary_request_header* hdr;
    buf.resize(sizeof(hdr->bytes));
    hdr = reinterpret_cast<protocol_binary_request_header*>(buf.data());

    size_t extlen = 24;

    if (options) {
        extlen += 4;
    }

    if (!meta.empty()) {
        extlen += 2;
    }

    fillHeader(*hdr, doc.value.size(), extlen);

    hdr->request.datatype = uint8_t(doc.info.datatype);
    append(buf, getFlags());
    append(buf, getExptime());
    append(buf, seqno);
    append(buf, getMetaCas());

    if (options) {
        append(buf, options);
    }

    if (!meta.empty()) {
        append(buf, uint16_t(htons(gsl::narrow<uint16_t>(meta.size()))));
    }

    buf.insert(buf.end(), key.begin(), key.end());
    buf.insert(buf.end(), doc.value.begin(), doc.value.end());
    buf.insert(buf.end(), meta.begin(), meta.end());
}

void BinprotSetControlTokenCommand::encode(std::vector<uint8_t>& buf) const {
    writeHeader(buf, 0, sizeof(token));
    append(buf, token);
}

void BinprotSetClusterConfigCommand::encode(std::vector<uint8_t>& buf) const {
    writeHeader(buf, config.size(), 0);
    buf.insert(buf.end(), config.begin(), config.end());
}

BinprotObserveSeqnoCommand::BinprotObserveSeqnoCommand(uint16_t vbid,
                                                       uint64_t uuid)
    : BinprotGenericCommand(PROTOCOL_BINARY_CMD_OBSERVE_SEQNO), uuid(uuid) {
    setVBucket(vbid);
}

void BinprotObserveSeqnoCommand::encode(std::vector<uint8_t>& buf) const {
    writeHeader(buf, sizeof(uuid), 0);
    append(buf, uuid);
}

void BinprotObserveSeqnoResponse::assign(std::vector<uint8_t>&& buf) {
    BinprotResponse::assign(std::move(buf));
    if (!isSuccess()) {
        return;
    }

    if ((getBodylen() != 43) && (getBodylen() != 27)) {
        throw std::runtime_error(
                "BinprotObserveSeqnoResponse::assign: Invalid payload size - "
                "expected:43 or 27, actual:" +
                std::to_string(getBodylen()));
    }

    auto it = payload.begin() + getHeaderLen();
    it = extract(it, info.formatType);
    it = extract(it, info.vbId);
    it = extract(it, info.uuid);
    it = extract(it, info.lastPersistedSeqno);
    it = extract(it, info.currentSeqno);

    switch (info.formatType) {
    case 0:
        // No more fields for format 0.
        break;

    case 1:
        // Add in hard failover information
        it = extract(it, info.failoverUUID);
        it = extract(it, info.failoverSeqno);
        break;

    default:
        throw std::runtime_error(
                "BinprotObserveSeqnoResponse::assign: Unexpected formatType:" +
                std::to_string(info.formatType));
    }
}
