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
                                              Greenstack::Bucket::bucket_type_t type) {

    std::string module;
    switch (type) {
    case Greenstack::Bucket::Memcached:
        module.assign("default_engine.so");
        break;
    case Greenstack::Bucket::EWouldBlock:
        module.assign("ewouldblock_engine.so");
        break;
    case Greenstack::Bucket::Couchbase:
        module.assign("ep.so");
        break;
    default:
        throw std::runtime_error("Not implemented");
    }

    std::vector<uint8_t> payload;
    payload.resize(module.length() + 1 + config.length());
    memcpy(payload.data(), module.data(), module.length());
    memcpy(payload.data() + module.length() + 1, config.data(), config.length());

    Frame frame;
    mcbp_raw_command(frame, PROTOCOL_BINARY_CMD_CREATE_BUCKET,
                     name.c_str(), name.length(), payload.data(), payload.size());

    sendFrame(frame);
    recvFrame(frame);
    auto* rsp = reinterpret_cast<protocol_binary_response_no_extras*>(frame.payload.data());

    if (rsp->message.header.response.status != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        throw ConnectionError("Create bucket failed: " + std::to_string(rsp->message.header.response.status));
    }
}

void MemcachedBinprotConnection::deleteBucket(const std::string& name) {
    Frame frame;
    mcbp_raw_command(frame, PROTOCOL_BINARY_CMD_DELETE_BUCKET,
                     name.c_str(), name.length(), nullptr, 0);
    sendFrame(frame);
    recvFrame(frame);
    auto* rsp = reinterpret_cast<protocol_binary_response_no_extras*>(frame.payload.data());

    if (rsp->message.header.response.status != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        throw ConnectionError("Delete bucket failed: " + std::to_string(rsp->message.header.response.status));
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
    throw std::runtime_error("Not implemented for MCBP");
}
