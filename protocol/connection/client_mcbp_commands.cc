/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "client_mcbp_commands.h"
#include "frameinfo.h"
#include <gsl/gsl-lite.hpp>
#include <mcbp/mcbp.h>
#include <memcached/tracer.h>
#include <array>
#include <utility>

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

void BinprotCommand::encode(std::vector<uint8_t>& buf) const {
    writeHeader(buf);
}

BinprotCommand::Encoded BinprotCommand::encode() const {
    Encoded bufs;
    encode(bufs.header);
    return bufs;
}

void BinprotCommand::fillHeader(cb::mcbp::Request& header,
                                size_t payload_len,
                                size_t extlen) const {
    header.setMagic(cb::mcbp::Magic::ClientRequest);
    header.setOpcode(opcode);
    // The server refuse to use a key > 0xff so just narrow it here to make
    // sure that we don't use the frame extras field if we want to inject
    // that at a later time
    header.setKeylen(gsl::narrow<uint8_t>(key.size()));
    header.setExtlen(gsl::narrow<uint8_t>(extlen));
    header.setDatatype(cb::mcbp::Datatype::Raw);
    header.setVBucket(vbucket);
    header.setBodylen(gsl::narrow<uint32_t>(key.size() + extlen + payload_len +
                                            frame_info.size()));
    header.setOpaque(opaque);
    // @todo fix this to use the setter. There is still some dependency
    // in other tests which use expects this to be in the same byte order
    // as the server sent it..
    header.setCas(cas);
}

void BinprotCommand::writeHeader(std::vector<uint8_t>& buf,
                                 size_t payload_len,
                                 size_t extlen) const {
    buf.resize(sizeof(cb::mcbp::Request));
    auto& hdr = *reinterpret_cast<cb::mcbp::Request*>(buf.data());
    fillHeader(hdr, payload_len, extlen);

    if (!frame_info.empty()) {
        hdr.setMagic(cb::mcbp::Magic::AltClientRequest);
        hdr.setFramingExtraslen(gsl::narrow<uint8_t>(frame_info.size()));
        std::copy(frame_info.cbegin(),
                  frame_info.cend(),
                  std::back_inserter(buf));
    }
}

BinprotCommand& BinprotCommand::setKey(std::string key_) {
    key = std::move(key_);
    return *this;
}

BinprotCommand& BinprotCommand::setCas(uint64_t cas_) {
    cas = cas_;
    return *this;
}

BinprotCommand& BinprotCommand::setOp(cb::mcbp::ClientOpcode cmd_) {
    opcode = cmd_;
    return *this;
}

void BinprotCommand::clear() {
    opcode = cb::mcbp::ClientOpcode::Invalid;
    key.clear();
    cas = 0;
    vbucket = Vbid(0);
}

uint64_t BinprotCommand::getCas() const {
    return cas;
}

const std::string& BinprotCommand::getKey() const {
    return key;
}

cb::mcbp::ClientOpcode BinprotCommand::getOp() const {
    return opcode;
}

BinprotCommand& BinprotCommand::setVBucket(Vbid vbid) {
    vbucket = vbid;
    return *this;
}

BinprotCommand& BinprotCommand::setOpaque(uint32_t opaq) {
    opaque = opaq;
    return *this;
}

BinprotCommand& BinprotCommand::addFrameInfo(const FrameInfo& fi) {
    auto encoded = fi.encode();
    return addFrameInfo({encoded.data(), encoded.size()});
}

BinprotCommand& BinprotCommand::addFrameInfo(cb::const_byte_buffer section) {
    std::copy(section.cbegin(), section.cend(), std::back_inserter(frame_info));
    return *this;
}

void BinprotCommand::ExpiryValue::assign(uint32_t value_) {
    value = value_;
    set = true;
}

void BinprotCommand::ExpiryValue::clear() {
    set = false;
}

bool BinprotCommand::ExpiryValue::isSet() const {
    return set;
}

uint32_t BinprotCommand::ExpiryValue::getValue() const {
    return value;
}

BinprotCommandResponse::BinprotCommandResponse(cb::mcbp::ClientOpcode opcode,
                                               uint32_t opaque,
                                               cb::mcbp::Status status) {
    setOp(opcode);
    setOpaque(opaque);
    setStatus(status);
}

BinprotCommandResponse& BinprotCommandResponse::setStatus(
        cb::mcbp::Status status) {
    this->status = status;
    return *this;
}

void BinprotCommandResponse::encode(std::vector<uint8_t>& buf) const {
    BinprotGenericCommand::encode(buf);
    auto& rsp = *reinterpret_cast<cb::mcbp::Response*>(buf.data());
    rsp.setMagic(cb::mcbp::Magic::ClientResponse);
    rsp.setStatus(status);
    rsp.setOpaque(opaque);
    rsp.setOpcode(opcode);
}

void BinprotGenericCommand::encode(std::vector<uint8_t>& buf) const {
    writeHeader(buf, value.size(), extras.size());
    buf.insert(buf.end(), extras.begin(), extras.end());
    buf.insert(buf.end(), key.begin(), key.end());
    buf.insert(buf.end(), value.begin(), value.end());
}

BinprotGenericCommand::BinprotGenericCommand(cb::mcbp::ClientOpcode opcode,
                                             const std::string& key_,
                                             const std::string& value_)
    : BinprotCommandT() {
    setOp(opcode);
    setKey(key_);
    setValue(value_);
}

BinprotGenericCommand::BinprotGenericCommand(cb::mcbp::ClientOpcode opcode,
                                             const std::string& key_)
    : BinprotCommandT() {
    setOp(opcode);
    setKey(key_);
}

BinprotGenericCommand::BinprotGenericCommand(cb::mcbp::ClientOpcode opcode)
    : BinprotCommandT() {
    setOp(opcode);
}

BinprotGenericCommand::BinprotGenericCommand() : BinprotCommandT() {
}

BinprotGenericCommand& BinprotGenericCommand::setValue(std::string value_) {
    value = std::move(value_);
    return *this;
}

BinprotGenericCommand& BinprotGenericCommand::setExtras(
        const std::vector<uint8_t>& buf) {
    extras.assign(buf.begin(), buf.end());
    return *this;
}

BinprotGenericCommand& BinprotGenericCommand::setExtras(std::string_view buf) {
    const auto* data = reinterpret_cast<const uint8_t*>(buf.data());
    this->extras.assign(data, data + buf.size());
    return *this;
}

void BinprotGenericCommand::clear() {
    BinprotCommand::clear();
    value.clear();
    extras.clear();
}

BinprotSubdocCommand::BinprotSubdocCommand(
        cb::mcbp::ClientOpcode cmd_,
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

BinprotSubdocCommand& BinprotSubdocCommand::setPath(std::string path_) {
    if (path_.size() > std::numeric_limits<uint16_t>::max()) {
        throw std::out_of_range("BinprotSubdocCommand::setPath: Path too big");
    }
    path = std::move(path_);
    return *this;
}

void BinprotSubdocCommand::encode(std::vector<uint8_t>& buf) const {
    if (key.empty()) {
        throw std::logic_error("BinprotSubdocCommand::encode: Missing a key");
    }

    // Expiry (optional) is encoded in extras. Only include if non-zero or
    // if explicit encoding of zero was requested.
    const bool include_expiry = (expiry.getValue() != 0 || expiry.isSet());
    const bool include_doc_flags = !isNone(doc_flags);

    // Populate the header.
    const size_t extlen = sizeof(uint16_t) + // Path length
                          1 + // flags
                          (include_expiry ? sizeof(uint32_t) : 0) +
                          (include_doc_flags ? sizeof(uint8_t) : 0);

    writeHeader(buf, path.size() + value.size(), extlen);

    // Add extras: pathlen, flags, optional expiry
    append(buf, gsl::narrow<uint16_t>(path.size()));
    buf.push_back(flags);

    if (include_expiry) {
        // As expiry is optional (and immediately follows subdoc_flags,
        // i.e. unaligned) there's no field in the struct; so use low-level
        // memcpy to populate it.
        append(buf, expiry.getValue());
    }

    if (include_doc_flags) {
        buf.push_back(
                std::underlying_type<mcbp::subdoc::doc_flag>::type(doc_flags));
    }

    // Add Body: key; path; value if applicable.
    buf.insert(buf.end(), key.begin(), key.end());
    buf.insert(buf.end(), path.begin(), path.end());
    buf.insert(buf.end(), value.begin(), value.end());
}
BinprotSubdocCommand::BinprotSubdocCommand() : BinprotCommandT() {
}
BinprotSubdocCommand::BinprotSubdocCommand(cb::mcbp::ClientOpcode cmd_) {
    setOp(cmd_);
}
BinprotSubdocCommand::BinprotSubdocCommand(cb::mcbp::ClientOpcode cmd_,
                                           const std::string& key_,
                                           const std::string& path_)
    : BinprotSubdocCommand(cmd_,
                           key_,
                           path_,
                           "",
                           SUBDOC_FLAG_NONE,
                           mcbp::subdoc::doc_flag::None,
                           0) {
}
BinprotSubdocCommand& BinprotSubdocCommand::setValue(std::string value_) {
    value = std::move(value_);
    return *this;
}
BinprotSubdocCommand& BinprotSubdocCommand::addPathFlags(
        protocol_binary_subdoc_flag flags_) {
    static protocol_binary_subdoc_flag validFlags = SUBDOC_FLAG_XATTR_PATH |
                                                    SUBDOC_FLAG_MKDIR_P |
                                                    SUBDOC_FLAG_EXPAND_MACROS;
    if ((flags_ & ~validFlags) == 0) {
        flags = flags | flags_;
    } else {
        throw std::invalid_argument("addPathFlags: flags_ (which is " +
                                    std::to_string(flags_) +
                                    ") is not a path flag");
    }
    return *this;
}
BinprotSubdocCommand& BinprotSubdocCommand::addDocFlags(
        mcbp::subdoc::doc_flag flags_) {
    using namespace mcbp::subdoc;
    constexpr doc_flag validFlags = doc_flag::Mkdoc | doc_flag::AccessDeleted |
                                    doc_flag::Add | doc_flag::CreateAsDeleted;
    if ((flags_ & ~validFlags) == mcbp::subdoc::doc_flag::None) {
        doc_flags = doc_flags | flags_;
    } else {
        throw std::invalid_argument("addDocFlags: flags_ (which is " +
                                    to_string(flags_) + ") is not a doc flag");
    }
    return *this;
}
BinprotSubdocCommand& BinprotSubdocCommand::setExpiry(uint32_t value_) {
    expiry.assign(value_);
    return *this;
}
const std::string& BinprotSubdocCommand::getPath() const {
    return path;
}
const std::string& BinprotSubdocCommand::getValue() const {
    return value;
}
protocol_binary_subdoc_flag BinprotSubdocCommand::getFlags() const {
    return flags;
}

bool BinprotResponse::isSuccess() const {
    return cb::mcbp::isStatusSuccess(getStatus());
}

void BinprotResponse::assign(std::vector<uint8_t>&& srcbuf) {
    payload = std::move(srcbuf);
}

std::optional<std::chrono::microseconds> BinprotResponse::getTracingData()
        const {
    auto framingExtrasLen = getFramingExtraslen();
    if (framingExtrasLen == 0) {
        return std::optional<std::chrono::microseconds>{};
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

    return std::optional<std::chrono::microseconds>{};
}

cb::mcbp::ClientOpcode BinprotResponse::getOp() const {
    return cb::mcbp::ClientOpcode(getResponse().getClientOpcode());
}

cb::mcbp::Status BinprotResponse::getStatus() const {
    return getResponse().getStatus();
}

size_t BinprotResponse::getExtlen() const {
    return getResponse().getExtlen();
}

size_t BinprotResponse::getBodylen() const {
    return getResponse().getBodylen();
}

size_t BinprotResponse::getFramingExtraslen() const {
    return getResponse().getFramingExtraslen();
}

size_t BinprotResponse::getHeaderLen() {
    return sizeof(cb::mcbp::Response);
}

uint64_t BinprotResponse::getCas() const {
    return getResponse().getCas();
}

protocol_binary_datatype_t BinprotResponse::getDatatype() const {
    return protocol_binary_datatype_t(getResponse().getDatatype());
}

std::string_view BinprotResponse::getKey() const {
    const auto buf = getResponse().getKey();
    return {reinterpret_cast<const char*>(buf.data()), buf.size()};
}

std::string BinprotResponse::getKeyString() const {
    const auto buf = getKey();
    return {buf.data(), buf.size()};
}

cb::const_byte_buffer BinprotResponse::getData() const {
    return getResponse().getValue();
}

std::string BinprotResponse::getDataString() const {
    const auto buf = getData();
    return {reinterpret_cast<const char*>(buf.data()), buf.size()};
}

nlohmann::json BinprotResponse::getDataJson() const {
    const auto buf = getData();
    const auto view = std::string_view{
            reinterpret_cast<const char*>(buf.data()), buf.size()};
    return nlohmann::json::parse(view);
}

const cb::mcbp::Response& BinprotResponse::getResponse() const {
    return getHeader().getResponse();
}

void BinprotResponse::clear() {
    payload.clear();
}

const cb::mcbp::Header& BinprotResponse::getHeader() const {
    return *reinterpret_cast<const cb::mcbp::Header*>(payload.data());
}

void BinprotSubdocResponse::assign(std::vector<uint8_t>&& srcbuf) {
    BinprotResponse::assign(std::move(srcbuf));
    if (getBodylen() - getExtlen() - getFramingExtraslen() > 0) {
        value.assign(payload.data() + sizeof(cb::mcbp::Response) + getExtlen() +
                             getFramingExtraslen(),
                     payload.data() + payload.size());
    }
}
const std::string& BinprotSubdocResponse::getValue() const {
    return value;
}
void BinprotSubdocResponse::clear() {
    BinprotResponse::clear();
    value.clear();
}
bool BinprotSubdocResponse::operator==(
        const BinprotSubdocResponse& other) const {
    bool rv = getStatus() == other.getStatus();

    if (getStatus() == cb::mcbp::Status::Success) {
        rv = getValue() == other.getValue();
    }
    return rv;
}

void BinprotSaslAuthCommand::encode(std::vector<uint8_t>& buf) const {
    if (key.empty()) {
        throw std::logic_error("BinprotSaslAuthCommand: Missing mechanism (setMechanism)");
    }

    writeHeader(buf, challenge.size(), 0);
    buf.insert(buf.end(), key.begin(), key.end());
    buf.insert(buf.end(), challenge.begin(), challenge.end());
}
void BinprotSaslAuthCommand::setMechanism(const std::string& mech_) {
    setKey(mech_);
}
void BinprotSaslAuthCommand::setChallenge(std::string_view data) {
    challenge = data;
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
void BinprotSaslStepCommand::setMechanism(const std::string& mech) {
    setKey(mech);
}
void BinprotSaslStepCommand::setChallenge(std::string_view data) {
    challenge = data;
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
BinprotCreateBucketCommand::BinprotCreateBucketCommand(const char* name) {
    setKey(name);
}

void BinprotGetCommand::encode(std::vector<uint8_t>& buf) const {
    writeHeader(buf, 0, 0);
    buf.insert(buf.end(), key.begin(), key.end());
}

void BinprotGetAndLockCommand::encode(std::vector<uint8_t>& buf) const {
    writeHeader(buf, 0, sizeof(lock_timeout));
    cb::mcbp::request::GetLockedPayload payload;
    payload.setExpiration(lock_timeout);
    auto extras = payload.getBuffer();
    buf.insert(buf.end(), extras.begin(), extras.end());
    buf.insert(buf.end(), key.begin(), key.end());
}
BinprotGetAndLockCommand::BinprotGetAndLockCommand()
    : BinprotCommandT(), lock_timeout(0) {
}
BinprotGetAndLockCommand& BinprotGetAndLockCommand::setLockTimeout(
        uint32_t timeout) {
    lock_timeout = timeout;
    return *this;
}

void BinprotGetAndTouchCommand::encode(std::vector<uint8_t>& buf) const {
    writeHeader(buf, 0, sizeof(expirytime));
    cb::mcbp::request::GatPayload extras;
    extras.setExpiration(expirytime);
    auto buffer = extras.getBuffer();
    buf.insert(buf.end(), buffer.begin(), buffer.end());
    buf.insert(buf.end(), key.begin(), key.end());
}
BinprotGetAndTouchCommand::BinprotGetAndTouchCommand(std::string key,
                                                     uint32_t exp)
    : BinprotCommandT(), expirytime(exp) {
    setKey(std::move(key));
}
bool BinprotGetAndTouchCommand::isQuiet() const {
    return getOp() == cb::mcbp::ClientOpcode::Gatq;
}
BinprotGetAndTouchCommand& BinprotGetAndTouchCommand::setQuiet(bool quiet) {
    setOp(quiet ? cb::mcbp::ClientOpcode::Gatq : cb::mcbp::ClientOpcode::Gat);
    return *this;
}
BinprotGetAndTouchCommand& BinprotGetAndTouchCommand::setExpirytime(
        uint32_t timeout) {
    expirytime = timeout;
    return *this;
}

BinprotTouchCommand::BinprotTouchCommand(std::string key, uint32_t exp)
    : BinprotCommandT(), expirytime(exp) {
    setKey(std::move(key));
}

void BinprotTouchCommand::encode(std::vector<uint8_t>& buf) const {
    writeHeader(buf, 0, sizeof(expirytime));
    cb::mcbp::request::TouchPayload extras;
    extras.setExpiration(expirytime);
    auto buffer = extras.getBuffer();
    buf.insert(buf.end(), buffer.begin(), buffer.end());
    buf.insert(buf.end(), key.begin(), key.end());
}
BinprotTouchCommand& BinprotTouchCommand::setExpirytime(uint32_t timeout) {
    expirytime = timeout;
    return *this;
}

void BinprotUnlockCommand::encode(std::vector<uint8_t>& buf) const {
    writeHeader(buf, 0, 0);
    buf.insert(buf.end(), key.begin(), key.end());
}

uint32_t BinprotGetResponse::getDocumentFlags() const {
    if (!isSuccess()) {
        return 0;
    }

    auto extras = getResponse().getExtdata();
    if (extras.size() < sizeof(uint32_t)) {
        throw std::runtime_error(
                "BinprotGetResponse::getDocumentFlags(): extras does not "
                "contain flags");
    }

    return ntohl(*reinterpret_cast<const uint32_t*>(extras.data()));
}

BinprotMutationCommand& BinprotMutationCommand::setMutationType(
    MutationType type) {
    switch (type) {
    case MutationType::Add:
        setOp(cb::mcbp::ClientOpcode::Add);
        return *this;
    case MutationType::Set:
        setOp(cb::mcbp::ClientOpcode::Set);
        return *this;
    case MutationType::Replace:
        setOp(cb::mcbp::ClientOpcode::Replace);
        return *this;
    case MutationType::Append:
        setOp(cb::mcbp::ClientOpcode::Append);
        return *this;
    case MutationType::Prepend:
        setOp(cb::mcbp::ClientOpcode::Prepend);
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
    if (getOp() == cb::mcbp::ClientOpcode::Append ||
        getOp() == cb::mcbp::ClientOpcode::Prepend) {
        if (expiry.getValue() != 0) {
            throw std::invalid_argument("BinprotMutationCommand::encode: Expiry invalid with append/prepend");
        }
        extlen = 0;
    }

    size_t value_size = value.size();
    for (const auto& vbuf : value_refs) {
        value_size += vbuf.size();
    }

    writeHeader(buf, value_size, extlen);
    auto* header = reinterpret_cast<cb::mcbp::Request*>(buf.data());
    header->setDatatype(cb::mcbp::Datatype(datatype));

    if (extlen != 0) {
        // Write the extras:
        cb::mcbp::request::MutationPayload mp;
        mp.setFlags(flags);
        mp.setExpiration(expiry.getValue());
        auto buffer = mp.getBuffer();
        buf.insert(buf.end(), buffer.begin(), buffer.end());
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
BinprotMutationCommand& BinprotMutationCommand::setValue(
        std::vector<uint8_t>&& value_) {
    value = std::move(value_);
    return *this;
}
template <typename T>
BinprotMutationCommand& BinprotMutationCommand::setValue(const T& value_) {
    value.assign(value_.begin(), value_.end());
    return *this;
}
BinprotMutationCommand& BinprotMutationCommand::addValueBuffer(
        cb::const_byte_buffer buf) {
    value_refs.emplace_back(buf);
    return *this;
}
BinprotMutationCommand& BinprotMutationCommand::setDatatype(uint8_t datatype_) {
    datatype = datatype_;
    return *this;
}
BinprotMutationCommand& BinprotMutationCommand::setDatatype(
        cb::mcbp::Datatype datatype_) {
    return setDatatype(uint8_t(datatype_));
}
BinprotMutationCommand& BinprotMutationCommand::setDocumentFlags(
        uint32_t flags_) {
    flags = flags_;
    return *this;
}
BinprotMutationCommand& BinprotMutationCommand::setExpiry(uint32_t expiry_) {
    expiry.assign(expiry_);
    return *this;
}

BinprotMutationResponse::BinprotMutationResponse(BinprotResponse&& other)
    : BinprotResponse(other) {
    decode();
}

void BinprotMutationResponse::assign(std::vector<uint8_t>&& buf) {
    BinprotResponse::assign(std::move(buf));
    decode();
}

const MutationInfo& BinprotMutationResponse::getMutationInfo() const {
    return mutation_info;
}

void BinprotMutationResponse::decode() {
    if (!isSuccess()) {
        // No point parsing the other info..
        return;
    }

    mutation_info.cas = getCas();
    mutation_info.size = 0; // TODO: what's this?

    const auto extras = getResponse().getExtdata();

    if (extras.empty()) {
        mutation_info.vbucketuuid = 0;
        mutation_info.seqno = 0;
    } else if (extras.size() == 16) {
        auto const* bufs = reinterpret_cast<const uint64_t*>(extras.data());
        mutation_info.vbucketuuid = ntohll(bufs[0]);
        mutation_info.seqno = ntohll(bufs[1]);
    } else {
        throw std::runtime_error(
                "BinprotMutationResponse::decode: Bad extras length");
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
BinprotHelloCommand::BinprotHelloCommand(const std::string& client_id)
    : BinprotCommandT() {
    setKey(client_id);
}
BinprotHelloCommand& BinprotHelloCommand::enableFeature(
        cb::mcbp::Feature feature, bool enabled) {
    if (enabled) {
        features.insert(static_cast<uint8_t>(feature));
    } else {
        features.erase(static_cast<uint8_t>(feature));
    }
    return *this;
}

BinprotHelloResponse::BinprotHelloResponse(BinprotResponse&& other)
    : BinprotResponse(other) {
    decode();
}

void BinprotHelloResponse::assign(std::vector<uint8_t>&& buf) {
    BinprotResponse::assign(std::move(buf));
    decode();
}
const std::vector<cb::mcbp::Feature>& BinprotHelloResponse::getFeatures()
        const {
    return features;
}

void BinprotHelloResponse::decode() {
    if (isSuccess()) {
        // Ensure body length is even
        auto value = getResponse().getValue();

        if ((value.size() & 1) != 0) {
            throw std::runtime_error(
                    "BinprotHelloResponse::assign: "
                    "Invalid response returned. "
                    "Uneven body length");
        }

        auto const* end =
                reinterpret_cast<const uint16_t*>(value.data() + value.size());
        auto const* cur = reinterpret_cast<const uint16_t*>(value.data());

        for (; cur != end; ++cur) {
            features.push_back(cb::mcbp::Feature(htons(*cur)));
        }
    }
}

void BinprotIncrDecrCommand::encode(std::vector<uint8_t>& buf) const {
    writeHeader(buf, 0, 20);
    if (getOp() != cb::mcbp::ClientOpcode::Decrement &&
        getOp() != cb::mcbp::ClientOpcode::Increment) {
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
BinprotIncrDecrCommand& BinprotIncrDecrCommand::setDelta(uint64_t delta_) {
    delta = delta_;
    return *this;
}
BinprotIncrDecrCommand& BinprotIncrDecrCommand::setInitialValue(
        uint64_t initial_) {
    initial = initial_;
    return *this;
}
BinprotIncrDecrCommand& BinprotIncrDecrCommand::setExpiry(uint32_t expiry_) {
    expiry.assign(expiry_);
    return *this;
}

BinprotIncrDecrResponse::BinprotIncrDecrResponse(BinprotResponse&& other)
    : BinprotMutationResponse(std::move(other)) {
    decode();
}

void BinprotIncrDecrResponse::assign(std::vector<uint8_t>&& buf) {
    BinprotMutationResponse::assign(std::move(buf));
    decode();
}

uint64_t BinprotIncrDecrResponse::getValue() const {
    return value;
}

void BinprotIncrDecrResponse::decode() {
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
void BinprotGetErrorMapCommand::setVersion(uint16_t version_) {
    version = version_;
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
        const auto* doc_flag_ptr =
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
BinprotSubdocMultiMutationCommand::BinprotSubdocMultiMutationCommand()
    : BinprotCommandT<BinprotSubdocMultiMutationCommand,
                      cb::mcbp::ClientOpcode::SubdocMultiMutation>(),
      docFlags(mcbp::subdoc::doc_flag::None) {
}

BinprotSubdocMultiMutationCommand::BinprotSubdocMultiMutationCommand(
        std::string key,
        std::vector<MutationSpecifier> specs,
        mcbp::subdoc::doc_flag docFlags,
        const std::optional<cb::durability::Requirements>& durReqs)
    : specs(std::move(specs)), docFlags(docFlags) {
    setKey(key);
    if (durReqs) {
        setDurabilityReqs(*durReqs);
    }
}

BinprotSubdocMultiMutationCommand&
BinprotSubdocMultiMutationCommand::addDocFlag(mcbp::subdoc::doc_flag docFlag) {
    using namespace mcbp::subdoc;
    constexpr doc_flag validFlags = doc_flag::Mkdoc | doc_flag::AccessDeleted |
                                    doc_flag::Add | doc_flag::CreateAsDeleted |
                                    doc_flag::ReviveDocument;
    if ((docFlag & ~validFlags) == doc_flag::None) {
        docFlags = docFlags | docFlag;
    } else {
        throw std::invalid_argument("addDocFlag: docFlag (Which is " +
                                    to_string(docFlag) + ") is not a doc flag");
    }
    return *this;
}
BinprotSubdocMultiMutationCommand&
BinprotSubdocMultiMutationCommand::addMutation(
        const BinprotSubdocMultiMutationCommand::MutationSpecifier& spec) {
    specs.push_back(spec);
    return *this;
}
BinprotSubdocMultiMutationCommand&
BinprotSubdocMultiMutationCommand::addMutation(
        cb::mcbp::ClientOpcode opcode,
        protocol_binary_subdoc_flag flags,
        const std::string& path,
        const std::string& value) {
    specs.emplace_back(MutationSpecifier{opcode, flags, path, value});
    return *this;
}
BinprotSubdocMultiMutationCommand& BinprotSubdocMultiMutationCommand::setExpiry(
        uint32_t expiry_) {
    expiry.assign(expiry_);
    return *this;
}

BinprotSubdocMultiMutationCommand&
BinprotSubdocMultiMutationCommand::setDurabilityReqs(
        const cb::durability::Requirements& durReqs) {
    addFrameInfo(DurabilityFrameInfo(durReqs.getLevel(), durReqs.getTimeout()));
    return *this;
}

BinprotSubdocMultiMutationCommand::MutationSpecifier&
BinprotSubdocMultiMutationCommand::at(size_t index) {
    return specs.at(index);
}
BinprotSubdocMultiMutationCommand::MutationSpecifier&
        BinprotSubdocMultiMutationCommand::operator[](size_t index) {
    return specs[index];
}
bool BinprotSubdocMultiMutationCommand::empty() const {
    return specs.empty();
}
size_t BinprotSubdocMultiMutationCommand::size() const {
    return specs.size();
}
void BinprotSubdocMultiMutationCommand::clearMutations() {
    specs.clear();
}
void BinprotSubdocMultiMutationCommand::clearDocFlags() {
    docFlags = mcbp::subdoc::doc_flag::None;
}

void BinprotSubdocMultiMutationResponse::assign(std::vector<uint8_t>&& buf) {
    BinprotResponse::assign(std::move(buf));
    switch (getStatus()) {
    case cb::mcbp::Status::Success:
    case cb::mcbp::Status::SubdocMultiPathFailure:
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

        auto cur_status = cb::mcbp::Status(ntohs(*reinterpret_cast<const uint16_t*>(bufcur)));
        bufcur += 2;

        if (cur_status == cb::mcbp::Status::Success) {
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
                    cur_status,
                    std::string(reinterpret_cast<const char*>(bufcur),
                                cur_len)});
            bufcur += cur_len;
        } else {
            results.emplace_back(MutationResult{index, cur_status, {}});
        }
    }
}
void BinprotSubdocMultiMutationResponse::clear() {
    BinprotResponse::clear();
    results.clear();
}
const std::vector<BinprotSubdocMultiMutationResponse::MutationResult>&
BinprotSubdocMultiMutationResponse::getResults() const {
    return results;
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
        const auto* doc_flag_ptr =
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
BinprotSubdocMultiLookupCommand::BinprotSubdocMultiLookupCommand()
    : BinprotCommandT<BinprotSubdocMultiLookupCommand,
                      cb::mcbp::ClientOpcode::SubdocMultiLookup>(),
      docFlags(mcbp::subdoc::doc_flag::None) {
}

BinprotSubdocMultiLookupCommand::BinprotSubdocMultiLookupCommand(
        std::string key,
        std::vector<LookupSpecifier> specs,
        mcbp::subdoc::doc_flag docFlags)
    : specs(std::move(specs)), docFlags(docFlags) {
    setKey(key);
}

BinprotSubdocMultiLookupCommand& BinprotSubdocMultiLookupCommand::addLookup(
        const BinprotSubdocMultiLookupCommand::LookupSpecifier& spec) {
    specs.push_back(spec);
    return *this;
}
BinprotSubdocMultiLookupCommand& BinprotSubdocMultiLookupCommand::addLookup(
        const std::string& path,
        cb::mcbp::ClientOpcode opcode,
        protocol_binary_subdoc_flag flags) {
    return addLookup({opcode, flags, path});
}
BinprotSubdocMultiLookupCommand& BinprotSubdocMultiLookupCommand::addGet(
        const std::string& path, protocol_binary_subdoc_flag flags) {
    return addLookup(path, cb::mcbp::ClientOpcode::SubdocGet, flags);
}
BinprotSubdocMultiLookupCommand& BinprotSubdocMultiLookupCommand::addExists(
        const std::string& path, protocol_binary_subdoc_flag flags) {
    return addLookup(path, cb::mcbp::ClientOpcode::SubdocExists, flags);
}
BinprotSubdocMultiLookupCommand& BinprotSubdocMultiLookupCommand::addGetcount(
        const std::string& path, protocol_binary_subdoc_flag flags) {
    return addLookup(path, cb::mcbp::ClientOpcode::SubdocGetCount, flags);
}
BinprotSubdocMultiLookupCommand& BinprotSubdocMultiLookupCommand::addDocFlag(
        mcbp::subdoc::doc_flag docFlag) {
    static const mcbp::subdoc::doc_flag validFlags =
            mcbp::subdoc::doc_flag::Mkdoc |
            mcbp::subdoc::doc_flag::AccessDeleted | mcbp::subdoc::doc_flag::Add;
    if ((docFlag & ~validFlags) == mcbp::subdoc::doc_flag::None) {
        docFlags = docFlags | docFlag;
    } else {
        throw std::invalid_argument("addDocFlag: docFlag (Which is " +
                                    to_string(docFlag) + ") is not a doc flag");
    }
    return *this;
}
void BinprotSubdocMultiLookupCommand::clearLookups() {
    specs.clear();
}
BinprotSubdocMultiLookupCommand::LookupSpecifier&
BinprotSubdocMultiLookupCommand::at(size_t index) {
    return specs.at(index);
}
BinprotSubdocMultiLookupCommand::LookupSpecifier&
        BinprotSubdocMultiLookupCommand::operator[](size_t index) {
    return specs[index];
}
bool BinprotSubdocMultiLookupCommand::empty() const {
    return specs.empty();
}
size_t BinprotSubdocMultiLookupCommand::size() const {
    return specs.size();
}
void BinprotSubdocMultiLookupCommand::clearDocFlags() {
    docFlags = mcbp::subdoc::doc_flag::None;
}
BinprotSubdocMultiLookupCommand&
BinprotSubdocMultiLookupCommand::setExpiry_Unsupported(uint32_t expiry_) {
    expiry.assign(expiry_);
    return *this;
}

void BinprotSubdocMultiLookupResponse::assign(std::vector<uint8_t>&& buf) {
    BinprotResponse::assign(std::move(buf));
    decode();
}
const std::vector<BinprotSubdocMultiLookupResponse::LookupResult>&
BinprotSubdocMultiLookupResponse::getResults() const {
    return results;
}
void BinprotSubdocMultiLookupResponse::clear() {
    BinprotResponse::clear();
    results.clear();
}

void BinprotSubdocMultiLookupResponse::decode() {
    // Check if this is a success - either full or partial.
    switch (getStatus()) {
    case cb::mcbp::Status::Success:
    case cb::mcbp::Status::SubdocSuccessDeleted:
    case cb::mcbp::Status::SubdocMultiPathFailure:
    case cb::mcbp::Status::SubdocMultiPathFailureDeleted:
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
                cb::mcbp::Status(cur_status),
                std::string(reinterpret_cast<const char*>(bufcur), cur_len)});
        bufcur += cur_len;
    }
}

BinprotSubdocMultiLookupResponse::BinprotSubdocMultiLookupResponse(
        BinprotResponse&& other)
    : BinprotResponse(other) {
    decode();
}

void BinprotGetCmdTimerCommand::encode(std::vector<uint8_t>& buf) const {
    writeHeader(buf, 0, 1);
    buf.push_back(std::underlying_type<cb::mcbp::ClientOpcode>::type(opcode));
    buf.insert(buf.end(), key.begin(), key.end());
}
BinprotGetCmdTimerCommand::BinprotGetCmdTimerCommand(
        cb::mcbp::ClientOpcode opcode)
    : BinprotCommandT(), opcode(opcode) {
}
BinprotGetCmdTimerCommand::BinprotGetCmdTimerCommand(
        const std::string& bucket, cb::mcbp::ClientOpcode opcode)
    : BinprotCommandT(), opcode(opcode) {
    setKey(bucket);
}
void BinprotGetCmdTimerCommand::setOpcode(cb::mcbp::ClientOpcode opcode) {
    BinprotGetCmdTimerCommand::opcode = opcode;
}
void BinprotGetCmdTimerCommand::setBucket(const std::string& bucket) {
    setKey(bucket);
}

void BinprotGetCmdTimerResponse::assign(std::vector<uint8_t>&& buf) {
    BinprotResponse::assign(std::move(buf));
    if (isSuccess()) {
        try {
            timings = nlohmann::json::parse(getDataString());
        } catch(nlohmann::json::exception& e) {
            std::string msg("BinprotGetCmdTimerResponse::assign: Invalid payload returned");
            msg += (std::string(" Reason: ") + e.what());
            throw std::runtime_error(msg);
        }
    }
}
nlohmann::json BinprotGetCmdTimerResponse::getTimings() const {
    return timings;
}

void BinprotVerbosityCommand::encode(std::vector<uint8_t>& buf) const {
    writeHeader(buf, 0, 4);
    uint32_t value = ntohl(level);
    auto const* p = reinterpret_cast<const char*>(&value);
    buf.insert(buf.end(), p, p + 4);
}
void BinprotVerbosityCommand::setLevel(int level) {
    BinprotVerbosityCommand::level = level;
}

static uint8_t netToHost(uint8_t x) {
    return x;
}

static uint64_t netToHost(uint64_t x) {
    return ntohll(x);
}

static Vbid netToHost(Vbid x) {
    return x.ntoh();
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
    const uint32_t reserved = 0;
    if (payload.empty()) {
        writeHeader(buf, 0, 8);
        append(buf, reserved);
        append(buf, flags);
        buf.insert(buf.end(), key.begin(), key.end());
    } else {
        const auto json = payload.dump();
        writeHeader(buf, json.size(), 8);
        append(buf, reserved);
        append(buf, flags);
        buf.insert(buf.end(), key.begin(), key.end());
        buf.insert(buf.end(), json.cbegin(), json.cend());
        auto& req = *reinterpret_cast<cb::mcbp::Request*>(buf.data());
        req.setDatatype(cb::mcbp::Datatype::JSON);
    }
}

void BinprotDcpOpenCommand::setConsumerName(std::string name) {
    payload["consumer_name"] = name;
}

BinprotDcpOpenCommand::BinprotDcpOpenCommand(const std::string& name,
                                             uint32_t flags_)
    : BinprotGenericCommand(cb::mcbp::ClientOpcode::DcpOpen, name, {}),
      flags(flags_) {
}
BinprotDcpOpenCommand& BinprotDcpOpenCommand::makeProducer() {
    flags |= cb::mcbp::request::DcpOpenPayload::Producer;
    return *this;
}
BinprotDcpOpenCommand& BinprotDcpOpenCommand::makeConsumer() {
    if (flags & cb::mcbp::request::DcpOpenPayload::Producer) {
        throw std::invalid_argument(
                "BinprotDcpOpenCommand::makeConsumer: a stream can't be both a "
                "consumer and producer");
    }
    return *this;
}
BinprotDcpOpenCommand& BinprotDcpOpenCommand::makeIncludeXattr() {
    flags |= cb::mcbp::request::DcpOpenPayload::IncludeXattrs;
    return *this;
}
BinprotDcpOpenCommand& BinprotDcpOpenCommand::makeNoValue() {
    flags |= cb::mcbp::request::DcpOpenPayload::NoValue;
    return *this;
}
BinprotDcpOpenCommand& BinprotDcpOpenCommand::setFlags(uint32_t flags) {
    BinprotDcpOpenCommand::flags = flags;
    return *this;
}

void BinprotDcpStreamRequestCommand::encode(std::vector<uint8_t>& buf) const {
    writeHeader(buf, value.size(), 48);
    append(buf, dcp_flags);
    append(buf, dcp_reserved);
    append(buf, dcp_start_seqno);
    append(buf, dcp_end_seqno);
    append(buf, dcp_vbucket_uuid);
    append(buf, dcp_snap_start_seqno);
    append(buf, dcp_snap_end_seqno);
    buf.insert(buf.end(), value.begin(), value.end());
}
BinprotDcpStreamRequestCommand::BinprotDcpStreamRequestCommand()
    : BinprotGenericCommand(cb::mcbp::ClientOpcode::DcpStreamReq, {}, {}),
      dcp_flags(0),
      dcp_reserved(0),
      dcp_start_seqno(std::numeric_limits<uint64_t>::min()),
      dcp_end_seqno(std::numeric_limits<uint64_t>::max()),
      dcp_vbucket_uuid(0),
      dcp_snap_start_seqno(std::numeric_limits<uint64_t>::min()),
      dcp_snap_end_seqno(std::numeric_limits<uint64_t>::max()) {
}

BinprotDcpStreamRequestCommand::BinprotDcpStreamRequestCommand(
        Vbid vbid,
        uint32_t flags,
        uint64_t startSeq,
        uint64_t endSeq,
        uint64_t vbUuid,
        uint64_t snapStart,
        uint64_t snapEnd)
    : BinprotGenericCommand(cb::mcbp::ClientOpcode::DcpStreamReq, {}, {}),
      dcp_flags(flags),
      dcp_reserved(0),
      dcp_start_seqno(startSeq),
      dcp_end_seqno(endSeq),
      dcp_vbucket_uuid(vbUuid),
      dcp_snap_start_seqno(snapStart),
      dcp_snap_end_seqno(snapEnd) {
    setVBucket(vbid);
}

BinprotDcpStreamRequestCommand::BinprotDcpStreamRequestCommand(
        Vbid vbid,
        uint32_t flags,
        uint64_t startSeq,
        uint64_t endSeq,
        uint64_t vbUuid,
        uint64_t snapStart,
        uint64_t snapEnd,
        const nlohmann::json& value)
    : BinprotGenericCommand(
              cb::mcbp::ClientOpcode::DcpStreamReq, {}, value.dump()),
      dcp_flags(flags),
      dcp_reserved(0),
      dcp_start_seqno(startSeq),
      dcp_end_seqno(endSeq),
      dcp_vbucket_uuid(vbUuid),
      dcp_snap_start_seqno(snapStart),
      dcp_snap_end_seqno(snapEnd) {
    setVBucket(vbid);
}

BinprotDcpStreamRequestCommand& BinprotDcpStreamRequestCommand::setDcpFlags(
        uint32_t value) {
    BinprotDcpStreamRequestCommand::dcp_flags = value;
    return *this;
}
BinprotDcpStreamRequestCommand& BinprotDcpStreamRequestCommand::setDcpReserved(
        uint32_t value) {
    BinprotDcpStreamRequestCommand::dcp_reserved = value;
    return *this;
}
BinprotDcpStreamRequestCommand&
BinprotDcpStreamRequestCommand::setDcpStartSeqno(uint64_t value) {
    BinprotDcpStreamRequestCommand::dcp_start_seqno = value;
    return *this;
}
BinprotDcpStreamRequestCommand& BinprotDcpStreamRequestCommand::setDcpEndSeqno(
        uint64_t value) {
    BinprotDcpStreamRequestCommand::dcp_end_seqno = value;
    return *this;
}
BinprotDcpStreamRequestCommand&
BinprotDcpStreamRequestCommand::setDcpVbucketUuid(uint64_t value) {
    BinprotDcpStreamRequestCommand::dcp_vbucket_uuid = value;
    return *this;
}
BinprotDcpStreamRequestCommand&
BinprotDcpStreamRequestCommand::setDcpSnapStartSeqno(uint64_t value) {
    BinprotDcpStreamRequestCommand::dcp_snap_start_seqno = value;
    return *this;
}
BinprotDcpStreamRequestCommand&
BinprotDcpStreamRequestCommand::setDcpSnapEndSeqno(uint64_t value) {
    BinprotDcpStreamRequestCommand::dcp_snap_end_seqno = value;
    return *this;
}

BinprotDcpStreamRequestCommand& BinprotDcpStreamRequestCommand::setValue(
        const nlohmann::json& value) {
    BinprotDcpStreamRequestCommand::value = value.dump();
    return *this;
}

BinprotDcpStreamRequestCommand& BinprotDcpStreamRequestCommand::setValue(
        std::string_view value) {
    BinprotDcpStreamRequestCommand::value = value;
    return *this;
}

void BinprotSetParamCommand::encode(std::vector<uint8_t>& buf) const {
    writeHeader(buf, value.size(), 4);
    cb::mcbp::request::SetParamPayload payload;
    payload.setParamType(type);
    auto extra = payload.getBuffer();
    buf.insert(buf.end(), extra.begin(), extra.end());
    buf.insert(buf.end(), key.begin(), key.end());
    buf.insert(buf.end(), value.begin(), value.end());
}
BinprotSetParamCommand::BinprotSetParamCommand(
        cb::mcbp::request::SetParamPayload::Type type_,
        const std::string& key_,
        std::string value_)
    : BinprotGenericCommand(cb::mcbp::ClientOpcode::SetParam),
      type(type_),
      value(std::move(value_)) {
    setKey(key_);
}

void BinprotSetWithMetaCommand::encode(std::vector<uint8_t>& buf) const {
    size_t extlen = 24;
    if (options) {
        extlen += 4;
    }

    if (!meta.empty()) {
        extlen += 2;
    }

    writeHeader(buf, doc.value.size(), extlen);
    auto& request = *reinterpret_cast<cb::mcbp::Request*>(buf.data());
    request.setDatatype(doc.info.datatype);
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

BinprotSetWithMetaCommand::BinprotSetWithMetaCommand(const Document& doc,
                                                     Vbid vbucket,
                                                     uint64_t operationCas,
                                                     uint64_t seqno,
                                                     uint32_t options,
                                                     std::vector<uint8_t>& meta)
    : BinprotGenericCommand(cb::mcbp::ClientOpcode::SetWithMeta),
      doc(doc),
      seqno(seqno),
      operationCas(operationCas),
      options(options),
      meta(meta) {
    setVBucket(vbucket);
    setCas(operationCas);
    setKey(doc.info.id);
    setMeta(meta);
}
BinprotSetWithMetaCommand& BinprotSetWithMetaCommand::setQuiet(bool quiet) {
    if (quiet) {
        setOp(cb::mcbp::ClientOpcode::SetqWithMeta);
    } else {
        setOp(cb::mcbp::ClientOpcode::SetWithMeta);
    }
    return *this;
}
uint32_t BinprotSetWithMetaCommand::getFlags() const {
    return doc.info.flags;
}
BinprotSetWithMetaCommand& BinprotSetWithMetaCommand::setFlags(uint32_t flags) {
    doc.info.flags = flags;
    return *this;
}
uint32_t BinprotSetWithMetaCommand::getExptime() const {
    return doc.info.expiration;
}
BinprotSetWithMetaCommand& BinprotSetWithMetaCommand::setExptime(
        uint32_t exptime) {
    doc.info.expiration = exptime;
    return *this;
}
uint64_t BinprotSetWithMetaCommand::getSeqno() const {
    return seqno;
}
BinprotSetWithMetaCommand& BinprotSetWithMetaCommand::setSeqno(uint64_t seqno) {
    BinprotSetWithMetaCommand::seqno = seqno;
    return *this;
}
uint64_t BinprotSetWithMetaCommand::getMetaCas() const {
    return doc.info.cas;
}
BinprotSetWithMetaCommand& BinprotSetWithMetaCommand::setMetaCas(uint64_t cas) {
    doc.info.cas = cas;
    return *this;
}
const std::vector<uint8_t>& BinprotSetWithMetaCommand::getMeta() {
    return meta;
}
BinprotSetWithMetaCommand& BinprotSetWithMetaCommand::setMeta(
        const std::vector<uint8_t>& meta) {
    std::copy(meta.begin(),
              meta.end(),
              std::back_inserter(BinprotSetWithMetaCommand::meta));
    return *this;
}

void BinprotSetControlTokenCommand::encode(std::vector<uint8_t>& buf) const {
    writeHeader(buf, 0, sizeof(token));
    append(buf, token);
}
BinprotSetControlTokenCommand::BinprotSetControlTokenCommand(uint64_t token_,
                                                             uint64_t oldtoken)
    : BinprotGenericCommand(cb::mcbp::ClientOpcode::SetCtrlToken),
      token(token_) {
    setCas(oldtoken);
}

void BinprotSetClusterConfigCommand::encode(std::vector<uint8_t>& buf) const {
    writeHeader(buf, config.size(), sizeof(revision) + sizeof(epoch));
    append(buf, uint64_t(epoch));
    append(buf, uint64_t(revision));
    buf.insert(buf.end(), key.begin(), key.end());
    buf.insert(buf.end(), config.begin(), config.end());
}

BinprotSetClusterConfigCommand::BinprotSetClusterConfigCommand(
        uint64_t token_,
        std::string config,
        int64_t epoch,
        int64_t revision,
        std::string bucket)
    : BinprotGenericCommand(cb::mcbp::ClientOpcode::SetClusterConfig,
                            std::move(bucket)),
      config(std::move(config)),
      epoch(epoch),
      revision(revision) {
    setCas(token_);
}

BinprotObserveSeqnoCommand::BinprotObserveSeqnoCommand(Vbid vbid, uint64_t uuid)
    : BinprotGenericCommand(cb::mcbp::ClientOpcode::ObserveSeqno), uuid(uuid) {
    setVBucket(vbid);
}

void BinprotObserveSeqnoCommand::encode(std::vector<uint8_t>& buf) const {
    writeHeader(buf, sizeof(uuid), 0);
    append(buf, uuid);
}

BinprotObserveSeqnoResponse::BinprotObserveSeqnoResponse(
        BinprotResponse&& other)
    : BinprotResponse(other) {
    decode();
}

void BinprotObserveSeqnoResponse::decode() {
    if (!isSuccess()) {
        return;
    }

    if ((getBodylen() != 43) && (getBodylen() != 27)) {
        throw std::runtime_error(
                "BinprotObserveSeqnoResponse::decode: Invalid payload size - "
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
                "BinprotObserveSeqnoResponse::decode: Unexpected formatType:" +
                std::to_string(info.formatType));
    }
}

void BinprotObserveSeqnoResponse::assign(std::vector<uint8_t>&& buf) {
    BinprotResponse::assign(std::move(buf));
    decode();
}

BinprotUpdateUserPermissionsCommand::BinprotUpdateUserPermissionsCommand(
        std::string payload)
    : BinprotGenericCommand(
              cb::mcbp::ClientOpcode::UpdateExternalUserPermissions),
      payload(std::move(payload)) {
}

void BinprotUpdateUserPermissionsCommand::encode(
        std::vector<uint8_t>& buf) const {
    writeHeader(buf, payload.size(), 0);
    buf.insert(buf.end(), key.begin(), key.end());
    buf.insert(buf.end(), payload.begin(), payload.end());
}

BinprotAuditPutCommand::BinprotAuditPutCommand(uint32_t id, std::string payload)
    : BinprotGenericCommand(cb::mcbp::ClientOpcode::AuditPut),
      id(id),
      payload(std::move(payload)) {
}

void BinprotAuditPutCommand::encode(std::vector<uint8_t>& buf) const {
    writeHeader(buf, payload.size(), sizeof(id));
    append(buf, id);
    buf.insert(buf.end(), payload.begin(), payload.end());
}

BinprotSetVbucketCommand::BinprotSetVbucketCommand(Vbid vbid,
                                                   vbucket_state_t state,
                                                   nlohmann::json payload)
    : BinprotGenericCommand(cb::mcbp::ClientOpcode::SetVbucket),
      state(state),
      payload(std::move(payload)) {
    setVBucket(vbid);
}

void BinprotSetVbucketCommand::encode(std::vector<uint8_t>& buf) const {
    if (payload.empty()) {
        writeHeader(buf, 0, 1);
        buf.push_back(uint8_t(state));
    } else {
        std::string json = payload.dump();
        writeHeader(buf, json.size(), 1);
        buf.push_back(uint8_t(state));
        buf.insert(buf.end(), json.begin(), json.end());
        auto& hdr = *reinterpret_cast<cb::mcbp::Request*>(buf.data());
        hdr.setDatatype(cb::mcbp::Datatype::JSON);
    }
}

BinprotDcpAddStreamCommand::BinprotDcpAddStreamCommand(uint32_t flags)
    : BinprotGenericCommand(cb::mcbp::ClientOpcode::DcpAddStream),
      flags(flags) {
}

void BinprotDcpAddStreamCommand::encode(std::vector<uint8_t>& buf) const {
    writeHeader(buf, 0, sizeof(flags));
    append(buf, flags);
}

BinprotDcpControlCommand::BinprotDcpControlCommand()
    : BinprotGenericCommand(cb::mcbp::ClientOpcode::DcpControl) {
}

BinprotDcpMutationCommand::BinprotDcpMutationCommand(std::string key,
                                                     std::string_view value,
                                                     uint32_t opaque,
                                                     uint8_t datatype,
                                                     uint32_t expiry,
                                                     uint64_t cas,
                                                     uint64_t seqno,
                                                     uint64_t revSeqno,
                                                     uint32_t flags,
                                                     uint32_t lockTime,
                                                     uint8_t nru) {
    setOp(cb::mcbp::ClientOpcode::DcpMutation);
    setKey(std::move(key));
    setValue(value);
    setOpaque(opaque);
    setDatatype(uint8_t(datatype));
    setExpiry(expiry);
    setCas(cas);
    setBySeqno(seqno);
    setRevSeqno(revSeqno);
    setDocumentFlags(flags);
    setLockTime(lockTime);
    setNru(nru);
}

BinprotDcpMutationCommand& BinprotDcpMutationCommand::setBySeqno(
        uint64_t seqno) {
    bySeqno = seqno;
    return *this;
}

BinprotDcpMutationCommand& BinprotDcpMutationCommand::setRevSeqno(
        uint64_t seqno) {
    revSeqno = seqno;
    return *this;
}

BinprotDcpMutationCommand& BinprotDcpMutationCommand::setNru(uint8_t nru) {
    this->nru = nru;
    return *this;
}

BinprotDcpMutationCommand& BinprotDcpMutationCommand::setLockTime(
        uint32_t lockTime) {
    this->lockTime = lockTime;
    return *this;
}

void BinprotDcpMutationCommand::encode(std::vector<uint8_t>& buf) const {
    encodeHeader(buf);
    buf.insert(buf.end(), key.begin(), key.end());
    buf.insert(buf.end(), value.begin(), value.end());
    for (const auto& vbuf : value_refs) {
        buf.insert(buf.end(), vbuf.begin(), vbuf.end());
    }
}

BinprotCommand::Encoded BinprotDcpMutationCommand::encode() const {
    Encoded ret;
    auto& hdrbuf = ret.header;
    encodeHeader(hdrbuf);

    hdrbuf.insert(hdrbuf.end(), key.begin(), key.end());
    hdrbuf.insert(hdrbuf.end(), value.begin(), value.end());

    ret.bufs.assign(value_refs.begin(), value_refs.end());
    return ret;
}

void BinprotDcpMutationCommand::encodeHeader(std::vector<uint8_t>& buf) const {
    if (key.empty()) {
        throw std::invalid_argument(
                "BinprotDcpMutationCommand::encode: Key is missing!");
    }
    if (!value.empty() && !value_refs.empty()) {
        throw std::invalid_argument(
                "BinprotDcpMutationCommand::encode: Both value and value_refs "
                "have items!");
    }

    size_t value_size = value.size();
    for (const auto& vbuf : value_refs) {
        value_size += vbuf.size();
    }

    writeHeader(buf, value_size, sizeof(cb::mcbp::request::DcpMutationPayload));
    auto* header = reinterpret_cast<cb::mcbp::Request*>(buf.data());
    header->setDatatype(cb::mcbp::Datatype(datatype));

    cb::mcbp::request::DcpMutationPayload payload;
    payload.setBySeqno(bySeqno);
    payload.setRevSeqno(revSeqno);
    payload.setFlags(flags);
    payload.setExpiration(expiry.getValue());
    payload.setLockTime(lockTime);
    payload.setNru(nru);

    auto buffer = payload.getBuffer();
    buf.insert(buf.end(), buffer.begin(), buffer.end());
}

BinprotDcpDeletionV2Command::BinprotDcpDeletionV2Command(std::string key,
                                                         std::string_view value,
                                                         uint32_t opaque,
                                                         uint8_t datatype,
                                                         uint64_t cas,
                                                         uint64_t seqno,
                                                         uint64_t revSeqno,
                                                         uint32_t deleteTime) {
    setOp(cb::mcbp::ClientOpcode::DcpDeletion);
    setKey(std::move(key));
    setValue(value);
    setOpaque(opaque);
    setDatatype(uint8_t(datatype));
    setCas(cas);
    setBySeqno(seqno);
    setRevSeqno(revSeqno);
    setDeleteTime(deleteTime);
}

BinprotDcpDeletionV2Command& BinprotDcpDeletionV2Command::setBySeqno(
        uint64_t seqno) {
    bySeqno = seqno;
    return *this;
}

BinprotDcpDeletionV2Command& BinprotDcpDeletionV2Command::setRevSeqno(
        uint64_t seqno) {
    revSeqno = seqno;
    return *this;
}

BinprotDcpDeletionV2Command& BinprotDcpDeletionV2Command::setDeleteTime(
        uint32_t deleteTime) {
    this->deleteTime = deleteTime;
    return *this;
}

void BinprotDcpDeletionV2Command::encode(std::vector<uint8_t>& buf) const {
    encodeHeader(buf);
    buf.insert(buf.end(), key.begin(), key.end());
    buf.insert(buf.end(), value.begin(), value.end());
    for (const auto& vbuf : value_refs) {
        buf.insert(buf.end(), vbuf.begin(), vbuf.end());
    }
}

BinprotCommand::Encoded BinprotDcpDeletionV2Command::encode() const {
    Encoded ret;
    auto& hdrbuf = ret.header;
    encodeHeader(hdrbuf);

    hdrbuf.insert(hdrbuf.end(), key.begin(), key.end());
    hdrbuf.insert(hdrbuf.end(), value.begin(), value.end());

    ret.bufs.assign(value_refs.begin(), value_refs.end());
    return ret;
}

void BinprotDcpDeletionV2Command::encodeHeader(
        std::vector<uint8_t>& buf) const {
    if (key.empty()) {
        throw std::invalid_argument(
                "BinprotDcpDeletionV2Command::encode: Key is missing!");
    }
    if (!value.empty() && !value_refs.empty()) {
        throw std::invalid_argument(
                "BinprotDcpDeletionV2Command::encode: Both value and "
                "value_refs have items!");
    }

    size_t value_size = value.size();
    for (const auto& vbuf : value_refs) {
        value_size += vbuf.size();
    }

    writeHeader(
            buf, value_size, sizeof(cb::mcbp::request::DcpDeletionV2Payload));
    auto* header = reinterpret_cast<cb::mcbp::Request*>(buf.data());
    header->setDatatype(cb::mcbp::Datatype(datatype));

    cb::mcbp::request::DcpDeletionV2Payload payload{
            bySeqno, revSeqno, deleteTime};

    auto buffer = payload.getBuffer();
    buf.insert(buf.end(), buffer.begin(), buffer.end());
}

BinprotDelWithMetaCommand::BinprotDelWithMetaCommand(Document doc,
                                                     Vbid vbucket,
                                                     uint32_t flags,
                                                     uint32_t delete_time,
                                                     uint64_t seqno,
                                                     uint64_t operationCas,
                                                     bool quiet)
    : BinprotGenericCommand(quiet ? cb::mcbp::ClientOpcode::DelqWithMeta
                                  : cb::mcbp::ClientOpcode::DelWithMeta,
                            doc.info.id),
      doc(std::move(doc)),
      flags(flags),
      delete_time(delete_time),
      seqno(seqno),
      operationCas(operationCas) {
    setVBucket(vbucket);
    setCas(operationCas);
}

void BinprotDelWithMetaCommand::encode(std::vector<uint8_t>& buf) const {
    cb::mcbp::request::DelWithMetaPayload extras(
            flags, delete_time, seqno, doc.info.cas);
    writeHeader(buf, doc.value.size(), sizeof(extras));
    auto& request = *reinterpret_cast<cb::mcbp::Request*>(buf.data());
    request.setDatatype(doc.info.datatype);
    auto extraBuf = extras.getBuffer();
    buf.insert(buf.end(), extraBuf.begin(), extraBuf.end());
    buf.insert(buf.end(), key.begin(), key.end());
    buf.insert(buf.end(), doc.value.begin(), doc.value.end());
}

void BinprotObserveCommand::encode(std::vector<uint8_t>& buf) const {
    BinprotGenericCommand::encode(buf);

    size_t total = 0;
    for (const auto& k : keys) {
        // 2 byte vbid
        // 2 byte klen
        total += 4 + k.second.size();
    }

    writeHeader(buf, total, 0);
    for (const auto& k : keys) {
        append(buf, k.first.get());
        append(buf, uint16_t(k.second.size()));
        buf.insert(buf.end(), k.second.begin(), k.second.end());
    }
}

std::vector<BinprotObserveResponse::Result>
BinprotObserveResponse::getResults() {
    if (!isSuccess()) {
        throw std::logic_error(
                "BinprotObserveResponse::getResults: not a success");
    }

    std::vector<Result> ret;
    auto value = getData();
    auto* ptr = value.begin();

    do {
        Result r;

        if (ptr + 2 > value.end()) {
            throw std::runtime_error("No vbid present");
        }
        r.vbid = Vbid{ntohs(*reinterpret_cast<const uint16_t*>(ptr))};
        ptr += 2;

        if (ptr + 2 > value.end()) {
            throw std::runtime_error("No keylen present");
        }
        uint16_t klen = ntohs(*reinterpret_cast<const uint16_t*>(ptr));
        ptr += 2;
        if (ptr + klen > value.end()) {
            throw std::runtime_error("no key present");
        }
        r.key.assign(reinterpret_cast<const char*>(ptr), klen);
        ptr += klen;
        if (ptr + 1 > value.end()) {
            throw std::runtime_error("no status present");
        }
        r.status = *ptr;
        ++ptr;
        if (ptr + sizeof(uint64_t) > value.end()) {
            throw std::runtime_error("no cas present");
        }
        r.cas = ntohll(*reinterpret_cast<const uint64_t*>(ptr));
        ptr += sizeof(uint64_t);
        ret.emplace_back(r);
    } while (ptr < value.end());

    return ret;
}
BinprotEWBCommand::BinprotEWBCommand(EWBEngineMode mode,
                                     cb::engine_errc err_code,
                                     uint32_t value,
                                     const std::string& key)
    : BinprotGenericCommand(cb::mcbp::ClientOpcode::EwouldblockCtl, key) {
    extras.setMode(uint32_t(mode));
    extras.setValue(uint32_t(value));
    extras.setInjectError(uint32_t(err_code));
}

void BinprotEWBCommand::encode(std::vector<uint8_t>& buf) const {
    writeHeader(buf, 0, sizeof(extras));
    auto extraBuf = extras.getBuffer();
    buf.insert(buf.end(), extraBuf.begin(), extraBuf.end());
    buf.insert(buf.end(), key.begin(), key.end());
}

BinprotCompactDbCommand::BinprotCompactDbCommand()
    : BinprotGenericCommand(cb::mcbp::ClientOpcode::CompactDb) {
}

void BinprotCompactDbCommand::encode(std::vector<uint8_t>& buf) const {
    writeHeader(buf, 0, sizeof(extras));
    auto extraBuf = extras.getBuffer();
    buf.insert(buf.end(), extraBuf.begin(), extraBuf.end());
}
