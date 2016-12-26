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
#include "client_mcbp_connection.h"

#include <array>
#include <cbsasl/cbsasl.h>
#include <engines/ewouldblock_engine/ewouldblock_engine.h>
#include <iostream>
#include <iterator>
#include <libmcbp/mcbp.h>
#include <memcached/protocol_binary.h>
#include <platform/strerror.h>
#include <sstream>
#include <stdexcept>
#include <string>
#include <include/memcached/protocol_binary.h>

static const bool packet_dump = getenv("COUCHBASE_PACKET_DUMP") != nullptr;

/////////////////////////////////////////////////////////////////////////
// SASL related functions
/////////////////////////////////////////////////////////////////////////
struct my_sasl_ctx {
    const char* username;
    cbsasl_secret_t* secret;
};

static int sasl_get_username(void* context, int id, const char** result,
                             unsigned int* len) {
    struct my_sasl_ctx* ctx = reinterpret_cast<struct my_sasl_ctx*>(context);
    if (!context || !result ||
        (id != CBSASL_CB_USER && id != CBSASL_CB_AUTHNAME)) {
        return CBSASL_BADPARAM;
    }

    *result = ctx->username;
    if (len) {
        *len = (unsigned int)strlen(*result);
    }

    return CBSASL_OK;
}

static int sasl_get_password(cbsasl_conn_t* conn, void* context, int id,
                             cbsasl_secret_t** psecret) {
    struct my_sasl_ctx* ctx = reinterpret_cast<struct my_sasl_ctx*>(context);
    if (!conn || !psecret || id != CBSASL_CB_PASS || ctx == NULL) {
        return CBSASL_BADPARAM;
    }

    *psecret = ctx->secret;
    return CBSASL_OK;
}

static Frame to_frame(const BinprotCommand& command) {
    Frame frame;
    command.encode(frame.payload);
    return frame;
}

/////////////////////////////////////////////////////////////////////////
// Implementation of the MemcachedBinaryConnection class
/////////////////////////////////////////////////////////////////////////

std::unique_ptr<MemcachedConnection> MemcachedBinprotConnection::clone() {
    auto* result = new MemcachedBinprotConnection(this->host,
                                                  this->port,
                                                  this->family,
                                                  this->ssl);
    return std::unique_ptr<MemcachedConnection>{result};
}

void MemcachedBinprotConnection::sendFrame(const Frame& frame) {
    MemcachedConnection::sendFrame(frame);
    if (packet_dump) {
        Couchbase::MCBP::dump(frame.payload.data(), std::cerr);
    }
}


void MemcachedBinprotConnection::recvFrame(Frame& frame) {
    frame.reset();
    // A memcached packet starts with a 24 byte fixed header
    MemcachedConnection::read(frame, 24);

    // Following the header is the full payload specified in the field
    // bodylen. Luckily for us the bodylen is located at the same offset in
    // both a request and a response message..
    auto* req = reinterpret_cast<protocol_binary_request_header*>(frame.payload.data());
    const uint32_t bodylen = ntohl(req->request.bodylen);
    const uint8_t magic = frame.payload.at(0);
    const uint8_t REQUEST = uint8_t(PROTOCOL_BINARY_REQ);
    const uint8_t RESPONSE = uint8_t(PROTOCOL_BINARY_RES);

    if (magic != REQUEST && magic != RESPONSE) {
        throw std::runtime_error("Invalid magic received: " +
                                 std::to_string(magic));
    }

    MemcachedConnection::read(frame, bodylen);
    if (packet_dump) {
        Couchbase::MCBP::dump(frame.payload.data(), std::cerr);
    }

    // fixup the length bits in the header to be in host local order:
    if (magic == REQUEST) {
        // The underlying buffer may hage been reallocated as part of read
        req = reinterpret_cast<protocol_binary_request_header*>(frame.payload.data());
        req->request.keylen = ntohs(req->request.keylen);
        req->request.bodylen = bodylen;
    } else {
        // The underlying buffer may hage been reallocated as part of read
        auto* res = reinterpret_cast<protocol_binary_response_header*>(frame.payload.data());
        res->response.keylen = ntohs(res->response.keylen);
        res->response.bodylen = bodylen;
        res->response.status = ntohs(res->response.status);
    }
}

void MemcachedBinprotConnection::sendCommand(const BinprotCommand& command) {
    sendFrame(to_frame(command));
}

void MemcachedBinprotConnection::recvResponse(BinprotResponse& response) {
    Frame frame;
    recvFrame(frame);
    response.assign(std::move(frame.payload));
}

void MemcachedBinprotConnection::authenticate(const std::string& username,
                                              const std::string& password,
                                              const std::string& mech) {
    cbsasl_error_t err;
    const char* data;
    unsigned int len;
    const char* chosenmech;
    struct my_sasl_ctx context;
    cbsasl_callback_t sasl_callbacks[4];
    cbsasl_conn_t* client;

    sasl_callbacks[0].id = CBSASL_CB_USER;
    sasl_callbacks[0].proc = (int (*)(void))&sasl_get_username;
    sasl_callbacks[0].context = &context;
    sasl_callbacks[1].id = CBSASL_CB_AUTHNAME;
    sasl_callbacks[1].proc = (int (*)(void))&sasl_get_username;
    sasl_callbacks[1].context = &context;
    sasl_callbacks[2].id = CBSASL_CB_PASS;
    sasl_callbacks[2].proc = (int (*)(void))&sasl_get_password;
    sasl_callbacks[2].context = &context;
    sasl_callbacks[3].id = CBSASL_CB_LIST_END;
    sasl_callbacks[3].proc = NULL;
    sasl_callbacks[3].context = NULL;

    context.username = username.c_str();
    std::vector<uint8_t> buffer(
        sizeof(context.secret->len) + password.length() + 10);
    context.secret = reinterpret_cast<cbsasl_secret_t*>(buffer.data());
    memcpy(context.secret->data, password.c_str(), password.length());
    context.secret->len = password.length();

    err = cbsasl_client_new(NULL, NULL, NULL, NULL, sasl_callbacks, 0, &client);
    if (err != CBSASL_OK) {
        throw std::runtime_error(
            std::string("cbsasl_client_new: ") + std::to_string(err));
    }
    err = cbsasl_client_start(client, mech.c_str(), NULL, &data, &len,
                              &chosenmech);
    if (err != CBSASL_OK) {
        throw std::runtime_error(
            std::string("cbsasl_client_start (") + std::string(chosenmech) +
            std::string("): ") + std::to_string(err));
    }

    BinprotSaslAuthCommand authCommand;
    authCommand.setChallenge(data, len);
    authCommand.setMechanism(chosenmech);
    sendCommand(authCommand);

    BinprotResponse response;
    recvResponse(response);

    while (response.getStatus() == PROTOCOL_BINARY_RESPONSE_AUTH_CONTINUE) {
        auto respdata = response.getData();
        err = cbsasl_client_step(client,
                                 reinterpret_cast<const char *>(respdata.data()),
                                 respdata.size(),
                                 NULL, &data, &len);
        if (err != CBSASL_OK && err != CBSASL_CONTINUE) {
            reconnect();
            throw std::runtime_error(
                std::string("cbsasl_client_step: ") + std::to_string(err));
        }

        BinprotSaslStepCommand stepCommand;
        stepCommand.setMechanism(chosenmech);
        stepCommand.setChallengeResponse(data, len);
        sendCommand(stepCommand);
        recvResponse(response);
    }

    cbsasl_dispose(&client);

    if (!response.isSuccess()) {
        throw BinprotConnectionError("Authentication failed: ", response);
    }
}

void MemcachedBinprotConnection::createBucket(const std::string& name,
                                              const std::string& config,
                                              const Greenstack::BucketType& type) {
    std::string module;
    switch (type) {
    case Greenstack::BucketType::Memcached:
        module.assign("default_engine.so");
        break;
    case Greenstack::BucketType::EWouldBlock:
        module.assign("ewouldblock_engine.so");
        break;
    case Greenstack::BucketType::Couchbase:
        module.assign("ep.so");
        break;
    default:
        throw std::runtime_error("Not implemented");
    }

    BinprotCreateBucketCommand command(name.c_str());
    command.setConfig(module, config);
    sendCommand(command);

    BinprotResponse response;
    recvResponse(response);

    if (!response.isSuccess()) {
        throw BinprotConnectionError("Create bucket failed: ", response);
    }
}

void MemcachedBinprotConnection::deleteBucket(const std::string& name) {
    BinprotGenericCommand command(PROTOCOL_BINARY_CMD_DELETE_BUCKET, name);
    sendCommand(command);
    BinprotResponse response;
    recvResponse(response);

    if (!response.isSuccess()) {
        throw BinprotConnectionError("Delete bucket failed: ", response);
    }
}

void MemcachedBinprotConnection::selectBucket(const std::string& name) {
    BinprotGenericCommand command(PROTOCOL_BINARY_CMD_SELECT_BUCKET, name);
    sendCommand(command);
    BinprotResponse response;
    recvResponse(response);

    if (!response.isSuccess()) {
        throw BinprotConnectionError("Select bucket failed: ", response);
    }
}

std::string MemcachedBinprotConnection::to_string() {
    std::string ret("Memcached connection ");
    ret.append(std::to_string(port));
    if (family == AF_INET6) {
        ret.append("[::!]:");
    } else {
        ret.append("127.0.0.1:");
    }

    ret.append(std::to_string(port));

    if (ssl) {
        ret.append(" ssl");
    }

    return ret;
}

std::vector<std::string> MemcachedBinprotConnection::listBuckets() {
    BinprotGenericCommand command(PROTOCOL_BINARY_CMD_LIST_BUCKETS);
    sendCommand(command);

    BinprotResponse response;
    recvResponse(response);

    if (!response.isSuccess()) {
        throw BinprotConnectionError("List bucket failed: ", response);
    }

    std::vector<std::string> ret;

    // the value contains a list of bucket names separated by space.
    std::istringstream iss(response.getDataString());
    std::copy(std::istream_iterator<std::string>(iss),
              std::istream_iterator<std::string>(),
              std::back_inserter(ret));

    return ret;
}

Document MemcachedBinprotConnection::get(const std::string& id,
                                         uint16_t vbucket) {
    BinprotGetCommand command;
    command.setKey(id);
    command.setVBucket(vbucket);
    sendCommand(command);

    BinprotGetResponse response;
    recvResponse(response);

    if (!response.isSuccess()) {
        throw BinprotConnectionError("Failed to get: " + id,
                                     response.getStatus());
    }

    Document ret;
    ret.info.flags = response.getDocumentFlags();
    ret.info.cas = response.getCas();
    ret.info.id = id;
    if (response.getDatatype() & PROTOCOL_BINARY_DATATYPE_JSON) {
        ret.info.datatype = Greenstack::Datatype::Json;
    } else {
        ret.info.datatype = Greenstack::Datatype::Raw;
    }

    if (response.getDatatype() & PROTOCOL_BINARY_DATATYPE_COMPRESSED) {
        ret.info.compression = Greenstack::Compression::Snappy;
    } else {
        ret.info.compression = Greenstack::Compression::None;
    }
    ret.value.assign(response.getData().data(),
                     response.getData().data() + response.getData().size());
    return ret;
}

Frame MemcachedBinprotConnection::encodeCmdGet(const std::string& id,
                                               uint16_t vbucket) {
    BinprotGetCommand command;
    command.setKey(id);
    command.setVBucket(vbucket);
    return to_frame(command);
}

/* Convenience function which will insert (copy) T into the given container. Only
 * safe if T is trivially copyable (i.e. save to use memcpy on).
 */
template<typename T>
void encode_to(std::vector<uint8_t>& container, const T& element) {
    const auto* elem_ptr = reinterpret_cast<const uint8_t*>(&element);
    container.insert(container.end(), elem_ptr, elem_ptr + sizeof(element));
}

Frame MemcachedBinprotConnection::encodeCmdDcpOpen() {
    // Encode extras
    std::vector<uint8_t> extras;
    encode_to(extras, htonl(0));
    encode_to(extras, uint32_t{DCP_OPEN_PRODUCER});

    return to_frame(BinprotGenericCommand(PROTOCOL_BINARY_CMD_DCP_OPEN)
                    .setKey("dcp")
                    .setExtras(extras));
}

Frame MemcachedBinprotConnection::encodeCmdDcpStreamReq() {

    // Encode extras
    std::vector<uint8_t> extras;
    encode_to(extras, htonl(0));  // flags
    encode_to(extras, uint32_t{});  // reserved
    encode_to(extras, htonll(std::numeric_limits<uint64_t>::min()));  // start_seqno
    encode_to(extras, htonll(std::numeric_limits<uint64_t>::max()));  // end_seqno
    encode_to(extras, uint64_t{});  // VB UUID
    encode_to(extras, htonll(std::numeric_limits<uint64_t>::min()));  // snap_start
    encode_to(extras, htonll(std::numeric_limits<uint64_t>::max()));  // snap_end

    return to_frame(BinprotGenericCommand(PROTOCOL_BINARY_CMD_DCP_STREAM_REQ)
                    .setExtras(extras));
}


MutationInfo MemcachedBinprotConnection::mutate(const Document& doc,
                                                uint16_t vbucket,
                                                const Greenstack::mutation_type_t type) {

    BinprotMutationCommand command;
    command.setDocumentInfo(doc.info);
    command.setValue(doc.value);
    command.setVBucket(vbucket);
    command.setMutationType(type);
    sendCommand(command);

    BinprotMutationResponse response;
    recvResponse(response);
    if (!response.isSuccess()) {
        throw BinprotConnectionError("Failed to store " + doc.info.id,
                                     response.getStatus());
    }

    return response.getMutationInfo();
}

unique_cJSON_ptr MemcachedBinprotConnection::stats(const std::string& subcommand) {
    BinprotGenericCommand command(PROTOCOL_BINARY_CMD_STAT, subcommand);
    sendCommand(command);

    unique_cJSON_ptr ret(cJSON_CreateObject());
    int counter = 0;

    while (true) {
        BinprotResponse response;
        recvResponse(response);

        if (!response.isSuccess()) {
            throw BinprotConnectionError("Stats failed", response);
        }

        if (!response.getBodylen()) {
            break;
        }

        std::string key = response.getKeyString();
        std::string value = response.getDataString();

        if (key.empty()) {
            key = std::to_string(counter++);
        }

        if (value == "false") {
            cJSON_AddFalseToObject(ret.get(), key.c_str());
        } else if (value == "true") {
            cJSON_AddTrueToObject(ret.get(), key.c_str());
        } else {
            try {
                int64_t val = std::stoll(value);
                cJSON_AddNumberToObject(ret.get(), key.c_str(), val);
            } catch (...) {
                cJSON_AddStringToObject(ret.get(), key.c_str(), value.c_str());
            }
        }
    }

    return ret;
}

void MemcachedBinprotConnection::configureEwouldBlockEngine(
    const EWBEngineMode& mode, ENGINE_ERROR_CODE err_code, uint32_t value,
    const std::string& key) {

    request_ewouldblock_ctl request;
    memset(request.bytes, 0, sizeof(request.bytes));
    request.message.header.request.magic = 0x80;
    request.message.header.request.opcode = PROTOCOL_BINARY_CMD_EWOULDBLOCK_CTL;
    request.message.header.request.extlen = 12;
    request.message.header.request.keylen = ntohs((short)key.size());
    request.message.header.request.bodylen = htonl(12 + key.size());
    request.message.body.inject_error = htonl(err_code);
    request.message.body.mode = htonl(static_cast<uint32_t>(mode));
    request.message.body.value = htonl(value);

    Frame frame;
    frame.payload.resize(sizeof(request.bytes) + key.size());
    memcpy(frame.payload.data(), request.bytes, sizeof(request.bytes));
    memcpy(frame.payload.data() + sizeof(request.bytes), key.data(),
           key.size());
    sendFrame(frame);

    recvFrame(frame);
    auto* bytes = frame.payload.data();
    auto* rsp = reinterpret_cast<protocol_binary_response_no_extras*>(bytes);
    auto& header = rsp->message.header.response;
    if (header.status != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        throw BinprotConnectionError("Failed to configure ewouldblock engine",
                                     header.status);
    }
}

void MemcachedBinprotConnection::reloadAuditConfiguration() {
    BinprotGenericCommand command(PROTOCOL_BINARY_CMD_AUDIT_CONFIG_RELOAD);
    sendCommand(command);
    BinprotResponse response;
    recvResponse(response);

    if (!response.isSuccess()) {
        throw BinprotConnectionError("Failed to reload audit configuration",
                                     response);
    }
}

void MemcachedBinprotConnection::hello(const std::string& userAgent,
                                       const std::string& userAgentVersion,
                                       const std::string& comment) {

    applyFeatures(userAgent + " " + userAgentVersion, effective_features);

    BinprotGenericCommand command(PROTOCOL_BINARY_CMD_SASL_LIST_MECHS);
    sendCommand(command);

    BinprotResponse response;
    recvResponse(response);
    if (!response.isSuccess()) {
        throw BinprotConnectionError("Failed to fetch sasl mechanisms",
                                     response);
    }

    saslMechanisms.assign(reinterpret_cast<const char *>(response.getPayload()),
                          response.getBodylen());
}

void MemcachedBinprotConnection::applyFeatures(const std::string& agent,
                                               const Featureset& featureset) {
    BinprotHelloCommand command(agent);
    for (const auto& feature : featureset) {
        command.enableFeature(mcbp::Feature(feature), true);
    }

    sendCommand(command);

    BinprotHelloResponse response;
    recvResponse(response);

    if (!response.isSuccess()) {
        throw BinprotConnectionError("Failed to say hello", response);
    }

    effective_features.clear();
    for (const auto& feature : response.getFeatures()) {
        effective_features.insert(uint16_t(feature));
    }
}

void MemcachedBinprotConnection::setFeature(mcbp::Feature feature,
                                            bool enabled) {
    Featureset currFeatures = effective_features;
    if (enabled) {
        currFeatures.insert(uint16_t(feature));
    } else {
        currFeatures.erase(uint16_t(feature));
    }

    applyFeatures("mcbp", currFeatures);

    if (enabled && !hasFeature(feature)) {
        throw std::runtime_error("Failed to enable " +
                                 mcbp::to_string(feature));
    } else if (!enabled && hasFeature(feature)) {
        throw std::runtime_error("Failed to disable " +
                                 mcbp::to_string(feature));
    }
}

std::string MemcachedBinprotConnection::ioctl_get(const std::string& key) {
    BinprotGenericCommand command(PROTOCOL_BINARY_CMD_IOCTL_GET, key);
    sendCommand(command);

    BinprotResponse response;
    recvResponse(response);
    if (!response.isSuccess()) {
        throw BinprotConnectionError("ioctl_get \"" + key + "\" failed.",
                                     response.getStatus());
    }
    return std::string(reinterpret_cast<const char *>(response.getPayload()),
                       response.getBodylen());
}

void MemcachedBinprotConnection::ioctl_set(const std::string& key,
                                           const std::string& value) {
    BinprotGenericCommand command(PROTOCOL_BINARY_CMD_IOCTL_SET, key, value);
    sendCommand(command);

    BinprotResponse response;
    recvResponse(response);
    if (!response.isSuccess()) {
        throw BinprotConnectionError("ioctl_set \"" + key + "\" failed.",
                                     response.getStatus());
    }
}

uint64_t MemcachedBinprotConnection::increment(const std::string& key,
                                               uint64_t delta,
                                               uint64_t initial,
                                               rel_time_t exptime,
                                               MutationInfo* info) {
    return incr_decr(PROTOCOL_BINARY_CMD_INCREMENT, key, delta, initial,
                     exptime, info);
}

uint64_t MemcachedBinprotConnection::decrement(const std::string& key,
                                               uint64_t delta,
                                               uint64_t initial,
                                               rel_time_t exptime,
                                               MutationInfo* info) {
    return incr_decr(PROTOCOL_BINARY_CMD_DECREMENT, key, delta, initial,
                     exptime, info);
}

uint64_t MemcachedBinprotConnection::incr_decr(protocol_binary_command opcode,
                                               const std::string& key,
                                               uint64_t delta,
                                               uint64_t initial,
                                               rel_time_t exptime,
                                               MutationInfo* info) {

    BinprotIncrDecrCommand command;
    command.setOp(opcode)
            .setKey(key)
            .setDelta(delta)
            .setInitialValue(initial)
            .setExpiry(exptime);

    sendCommand(command);

    BinprotIncrDecrResponse response;
    recvResponse(response);

    if (!response.isSuccess()) {
        if (opcode == PROTOCOL_BINARY_CMD_INCREMENT) {
            throw BinprotConnectionError("incr \"" + key + "\" failed.",
                                         response.getStatus());
        } else {
            throw BinprotConnectionError("decr \"" + key + "\" failed.",
                                         response.getStatus());
        }
    }

    if (info != nullptr) {
        *info = response.getMutationInfo();
    }
    return response.getValue();
}

MutationInfo MemcachedBinprotConnection::remove(const std::string& key,
                                                uint16_t vbucket,
                                                uint64_t cas) {
    BinprotRemoveCommand command;
    command.setKey(key).setVBucket(vbucket);
    command.setVBucket(vbucket);
    sendCommand(command);

    BinprotRemoveResponse response;
    recvResponse(response);

    if (!response.isSuccess()) {
        throw BinprotConnectionError("Failed to remove: " + key,
                                     response.getStatus());
    }

    return response.getMutationInfo();
}
