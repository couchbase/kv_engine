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
#include "testapp_mcbp_connection.h"
#include "testapp_binprot.h"

#include <cbsasl/cbsasl.h>
#include <extensions/protocol/testapp_extension.h>
#include <gtest/gtest.h>
#include <iostream>
#include <memcached/protocol_binary.h>
#include <platform/strerror.h>
#include <sstream>
#include <stdexcept>
#include <string>
#include <memcached/protocol_binary.h>
#include <array>
#include <include/memcached/protocol_binary.h>

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

/////////////////////////////////////////////////////////////////////////
// Implementation of the MemcachedBinaryConnection class
/////////////////////////////////////////////////////////////////////////
void MemcachedBinprotConnection::recvFrame(Frame& frame) {
    frame.reset();
    // A memcached packet starts with a 24 byte fixed header
    MemcachedConnection::read(frame, 24);

    // Following the header is the full payload specified in the field
    // bodylen.
    uint32_t bodylen;

    // fixup the header:
    if (frame.payload.at(0) == uint8_t(PROTOCOL_BINARY_REQ)) {
        auto* packet = reinterpret_cast<protocol_binary_request_header*>(frame.payload.data());

        packet->request.keylen = ntohs(packet->request.keylen);
        bodylen = packet->request.bodylen = ntohl(packet->request.bodylen);
    } else if (frame.payload.at(0) == uint8_t(PROTOCOL_BINARY_RES)) {
        auto* packet = reinterpret_cast<protocol_binary_response_header*>(frame.payload.data());

        packet->response.keylen = ntohs(packet->response.keylen);
        bodylen = packet->response.bodylen = ntohl(packet->response.bodylen);
        packet->response.status = ntohs(packet->response.status);
    } else {
        throw std::runtime_error("Invalid magic received");
    }
    MemcachedConnection::read(frame, bodylen);
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

    Frame request;
    mcbp_raw_command(request,
                     PROTOCOL_BINARY_CMD_SASL_AUTH,
                     chosenmech, strlen(chosenmech),
                     data, len);
    sendFrame(request);
    Frame response;
    recvFrame(response);
    auto* rsp = reinterpret_cast<protocol_binary_response_no_extras*>(response.payload.data());

    bool stepped = false;
    while (rsp->message.header.response.status ==
           PROTOCOL_BINARY_RESPONSE_AUTH_CONTINUE) {
        stepped = true;
        int datalen = rsp->message.header.response.bodylen -
                      rsp->message.header.response.keylen -
                      rsp->message.header.response.extlen;

        int dataoffset = sizeof(rsp->bytes) +
                         rsp->message.header.response.keylen +
                         rsp->message.header.response.extlen;

        err = cbsasl_client_step(client,
                                 reinterpret_cast<char*>(rsp->bytes +
                                                         dataoffset),
                                 datalen,
                                 NULL, &data, &len);
        if (err != CBSASL_OK && err != CBSASL_CONTINUE) {
            reconnect();
            throw std::runtime_error(
                std::string("cbsasl_client_step: ") + std::to_string(err));
        }
        request.reset();
        mcbp_raw_command(request,
                         PROTOCOL_BINARY_CMD_SASL_STEP,
                         chosenmech, strlen(chosenmech), data, len);

        sendFrame(request);
        recvFrame(response);
        rsp = reinterpret_cast<protocol_binary_response_no_extras*>(response.payload.data());
    }

    if (stepped) {
        mcbp_validate_response_header(rsp,
                                      PROTOCOL_BINARY_CMD_SASL_STEP,
                                      rsp->message.header.response.status);
    } else {
        mcbp_validate_response_header(rsp,
                                      PROTOCOL_BINARY_CMD_SASL_AUTH,
                                      rsp->message.header.response.status);
    }
    cbsasl_dispose(&client);

    if (rsp->message.header.response.status !=
        PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        throw std::runtime_error(
            "SASL_AUTH " + std::to_string(rsp->message.header.response.status));
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

    std::vector<uint8_t> payload;
    payload.resize(module.length() + 1 + config.length());
    memcpy(payload.data(), module.data(), module.length());
    memcpy(payload.data() + module.length() + 1, config.data(),
           config.length());

    Frame frame;
    mcbp_raw_command(frame, PROTOCOL_BINARY_CMD_CREATE_BUCKET,
                     name.c_str(), name.length(), payload.data(),
                     payload.size());

    sendFrame(frame);
    recvFrame(frame);
    auto* rsp = reinterpret_cast<protocol_binary_response_no_extras*>(frame.payload.data());

    if (rsp->message.header.response.status !=
        PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        throw ConnectionError("Create bucket failed: ", Protocol::Memcached,
                              rsp->message.header.response.status);
    }
}

void MemcachedBinprotConnection::deleteBucket(const std::string& name) {
    Frame frame;
    mcbp_raw_command(frame, PROTOCOL_BINARY_CMD_DELETE_BUCKET,
                     name.c_str(), name.length(), nullptr, 0);
    sendFrame(frame);
    recvFrame(frame);
    auto* rsp = reinterpret_cast<protocol_binary_response_no_extras*>(frame.payload.data());

    if (rsp->message.header.response.status !=
        PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        throw ConnectionError("Delete bucket failed: ", Protocol::Memcached,
                              rsp->message.header.response.status);
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
    Frame frame;
    mcbp_raw_command(frame, PROTOCOL_BINARY_CMD_LIST_BUCKETS,
                     nullptr, 0, nullptr, 0);
    sendFrame(frame);
    recvFrame(frame);
    auto* rsp = reinterpret_cast<protocol_binary_response_no_extras*>(frame.payload.data());

    if (rsp->message.header.response.status !=
        PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        throw ConnectionError("Delete bucket failed: ", Protocol::Memcached,
                              rsp->message.header.response.status);
    }

    std::vector<std::string> ret;
    std::string value((char*)(rsp + 1), rsp->message.header.response.bodylen);
    // the value contains a list of bucket names separated by space.
    std::istringstream iss(value);
    std::copy(std::istream_iterator<std::string>(iss),
              std::istream_iterator<std::string>(),
              std::back_inserter(ret));

    return ret;
}

Document MemcachedBinprotConnection::get(const std::string& id,
                                         uint16_t vbucket) {
    Frame frame;
    mcbp_raw_command(frame, PROTOCOL_BINARY_CMD_GET,
                     id.data(), id.length(), nullptr, 0);
    auto* req = reinterpret_cast<protocol_binary_request_no_extras*>(frame.payload.data());
    req->message.header.request.vbucket = htons(vbucket);
    sendFrame(frame);

    recvFrame(frame);
    auto* rsp = reinterpret_cast<protocol_binary_response_get*>(frame.payload.data());

    if (rsp->message.header.response.status !=
        PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        throw ConnectionError("Failed to get: " + id, Protocol::Memcached,
                              rsp->message.header.response.status);
    }

    Document ret;
    ret.info.flags = ntohl(rsp->message.body.flags);
    ret.info.cas = rsp->message.header.response.cas;
    ret.info.id = id;
    if (rsp->message.header.response.datatype & PROTOCOL_BINARY_DATATYPE_JSON) {
        ret.info.datatype = Greenstack::Datatype::Json;
    } else {
        ret.info.datatype = Greenstack::Datatype::Raw;
    }

    if (rsp->message.header.response.datatype &
        PROTOCOL_BINARY_DATATYPE_COMPRESSED) {
        ret.info.compression = Greenstack::Compression::Snappy;
    } else {
        ret.info.compression = Greenstack::Compression::None;
    }

    ret.value.resize(rsp->message.header.response.bodylen - 4);
    memcpy(ret.value.data(), rsp->bytes + 28, ret.value.size());

    return ret;
}

MutationInfo MemcachedBinprotConnection::mutate(const Document& doc,
                                                uint16_t vbucket,
                                                const Greenstack::mutation_type_t type) {
    protocol_binary_command cmd;
    switch (type) {
    case Greenstack::MutationType::Add:
        cmd = PROTOCOL_BINARY_CMD_ADD;
        break;
    case Greenstack::MutationType::Set:
        cmd = PROTOCOL_BINARY_CMD_SET;
        break;
    case Greenstack::MutationType::Replace:
        cmd = PROTOCOL_BINARY_CMD_REPLACE;
        break;
    case Greenstack::MutationType::Append:
        cmd = PROTOCOL_BINARY_CMD_APPEND;
        break;
    case Greenstack::MutationType::Prepend:
        cmd = PROTOCOL_BINARY_CMD_PREPEND;
        break;

    default:
        throw std::runtime_error(
            "Not implemented for MBCP: " + std::to_string(type));
    }

    Frame frame;
    // @todo fix expirations
    mcbp_storage_command(frame, cmd, doc.info.id, doc.value, doc.info.flags, 0);

    auto* req = reinterpret_cast<protocol_binary_request_set*>(frame.payload.data());
    if (doc.info.compression != Greenstack::Compression::None) {
        if (doc.info.compression != Greenstack::Compression::Snappy) {
            throw ConnectionError("Invalid compression for MCBP",
                                  Protocol::Memcached,
                                  PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);
        }
        req->message.header.request.datatype = PROTOCOL_BINARY_DATATYPE_COMPRESSED;
    }

    if (doc.info.datatype != Greenstack::Datatype::Raw) {
        req->message.header.request.datatype |= PROTOCOL_BINARY_DATATYPE_JSON;
    }

    req->message.header.request.cas = doc.info.cas;
    sendFrame(frame);

    recvFrame(frame);
    auto* rsp = reinterpret_cast<protocol_binary_response_set*>(frame.payload.data());
    if (rsp->message.header.response.status !=
        PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        throw ConnectionError("Failed to store " + doc.info.id,
                              Protocol::Memcached,
                              rsp->message.header.response.status);
    }

    MutationInfo info;
    info.cas = rsp->message.header.response.cas;
    // @todo add the rest of the fields
    return info;
}

void MemcachedBinprotConnection::setDatatypeSupport(bool enable) {
    std::vector<uint16_t> feat;
    if (enable) {
        feat.push_back(htons(PROTOCOL_BINARY_FEATURE_DATATYPE));
    }

    if (features[1]) {
        feat.push_back(htons(PROTOCOL_BINARY_FEATURE_TCPNODELAY));
    }

    if (features[2]) {
        feat.push_back(htons(PROTOCOL_BINARY_FEATURE_MUTATION_SEQNO));
    }

    Frame frame;
    mcbp_raw_command(frame,
                     PROTOCOL_BINARY_CMD_HELLO,
                     "mcbp", 4, feat.data(),
                     feat.size() * 2);

    sendFrame(frame);
    recvFrame(frame);
    auto* rsp = reinterpret_cast<protocol_binary_response_set*>(frame.payload.data());
    if (rsp->message.header.response.status !=
        PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        throw ConnectionError("Failed to enable features",
                              Protocol::Memcached,
                              rsp->message.header.response.status);
    }

    // Validate the result!
    if ((rsp->message.header.response.bodylen & 1) != 0) {
        throw ConnectionError("Invalid response returned", Protocol::Memcached,
                              PROTOCOL_BINARY_RESPONSE_EINVAL);
    }

    std::vector<uint16_t> enabled;
    enabled.resize(rsp->message.header.response.bodylen / 2);
    memcpy(enabled.data(), (rsp + 1), rsp->message.header.response.bodylen);
    for (auto val : enabled) {
        val = ntohs(val);
        switch (val) {
        case PROTOCOL_BINARY_FEATURE_DATATYPE:
            features[0] = true;
            break;
        case PROTOCOL_BINARY_FEATURE_TCPNODELAY:
            features[1] = true;
            break;
        case PROTOCOL_BINARY_FEATURE_MUTATION_SEQNO:
            features[2] = true;
            break;
        default:
            throw std::runtime_error("Unsupported version returned");
        }
    }

    if (enable && !features[0]) {
        throw std::runtime_error("Failed to enable datatype");
    }
}

void MemcachedBinprotConnection::setMutationSeqnoSupport(bool enable) {
    std::vector<uint16_t> feat;
    if (features[0]) {
        feat.push_back(htons(PROTOCOL_BINARY_FEATURE_DATATYPE));
    }

    if (features[1]) {
        feat.push_back(htons(PROTOCOL_BINARY_FEATURE_TCPNODELAY));
    }

    if (enable) {
        feat.push_back(htons(PROTOCOL_BINARY_FEATURE_MUTATION_SEQNO));
    }

    Frame frame;
    mcbp_raw_command(frame,
                     PROTOCOL_BINARY_CMD_HELLO,
                     "mcbp", 4, feat.data(),
                     feat.size() * 2);

    sendFrame(frame);
    recvFrame(frame);
    auto* rsp = reinterpret_cast<protocol_binary_response_set*>(frame.payload.data());
    if (rsp->message.header.response.status !=
        PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        throw ConnectionError("Failed to enable features",
                              Protocol::Memcached,
                              rsp->message.header.response.status);
    }

    // Validate the result!
    if ((rsp->message.header.response.bodylen & 1) != 0) {
        throw ConnectionError("Invalid response returned", Protocol::Memcached,
                              PROTOCOL_BINARY_RESPONSE_EINVAL);
    }

    std::vector<uint16_t> enabled;
    enabled.resize(rsp->message.header.response.bodylen / 2);
    memcpy(enabled.data(), (rsp + 1), rsp->message.header.response.bodylen);
    for (auto val : enabled) {
        val = ntohs(val);
        switch (val) {
        case PROTOCOL_BINARY_FEATURE_DATATYPE:
            features[0] = true;
            break;
        case PROTOCOL_BINARY_FEATURE_TCPNODELAY:
            features[1] = true;
            break;
        case PROTOCOL_BINARY_FEATURE_MUTATION_SEQNO:
            features[2] = true;
            break;
        default:
            throw std::runtime_error("Unsupported version returned");
        }
    }

    if (enable && !features[2]) {
        throw std::runtime_error("Failed to enable datatype");
    }

}
