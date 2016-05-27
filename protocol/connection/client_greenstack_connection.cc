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
#include "client_connection.h"
#include "client_greenstack_connection.h"

#include <cbsasl/cbsasl.h>
#include <extensions/protocol/testapp_extension.h>
#include <iostream>
#include <libgreenstack/Greenstack.h>
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
// Implementation of the MemcachedGreenstackConnection class
/////////////////////////////////////////////////////////////////////////
typedef std::unique_ptr<Greenstack::Message> UniqueMessagePtr;

std::unique_ptr<MemcachedConnection> MemcachedGreenstackConnection::clone() {
    return std::unique_ptr<MemcachedConnection>(
            new MemcachedGreenstackConnection(this->port, this->family,
                                              this->ssl));
}

void MemcachedGreenstackConnection::authenticate(const std::string& username,
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

    Frame frame;
    {
        Greenstack::SaslAuthRequest request(chosenmech, std::string(data, len));
        Greenstack::Frame::encode(request, frame.payload);
        sendFrame(frame);
    }

    bool authenticated = false;
    do {
        UniqueMessagePtr msg = recvMessage();
        if (msg.get() == nullptr) {
            cbsasl_dispose(&client);
            throw std::runtime_error("Failed to decode packet");
        }
        auto* rsp = dynamic_cast<Greenstack::SaslAuthResponse*>(msg.get());
        if (rsp == nullptr) {
            cbsasl_dispose(&client);
            throw std::runtime_error("Received unexpected packet");
        }

        if (rsp->getStatus() != Greenstack::Status::Success) {
            // @todo fixme
            cbsasl_dispose(&client);
            throw std::runtime_error("Received unexpected status code: ");
        }

        if (rsp->getChallenge().empty()) {
            authenticated = true;
        } else {
            err = cbsasl_client_step(client,
                                     rsp->getChallenge().data(),
                                     rsp->getChallenge().size(),
                                     NULL, &data, &len);
            if (err != CBSASL_OK && err != CBSASL_CONTINUE) {
                reconnect();
                cbsasl_dispose(&client);
                throw std::runtime_error(
                    std::string("cbsasl_client_step: ") + std::to_string(err));
            }

            frame.reset();
            Greenstack::SaslAuthRequest request("", std::string(data, len));
            Greenstack::Frame::encode(request, frame.payload);
            sendFrame(frame);
        }
    } while (!authenticated);
    cbsasl_dispose(&client);
}

/**
 * Read the next fame off the network. The format of the greenstack
 * frame is a 4 byte header containing the size of the frame.
 */
void MemcachedGreenstackConnection::recvFrame(Frame& frame) {
    frame.reset();
    MemcachedConnection::read(frame, 4);

    uint32_t bodylen;
    memcpy(&bodylen, frame.payload.data(), sizeof(bodylen));
    bodylen = ntohl(bodylen);
    MemcachedConnection::read(frame, bodylen);
}

std::string MemcachedGreenstackConnection::to_string() {
    std::string ret("Greenstack connection ");
    ret.append(std::to_string(port));
    if (family == AF_INET6) {
        ret.append("[::1]:");
    } else {
        ret.append("127.0.0.1:");
    }

    ret.append(std::to_string(port));

    if (ssl) {
        ret.append(" ssl");
    }

    return ret;
}

void MemcachedGreenstackConnection::hello(const std::string& userAgent,
                                          const std::string& userAgentVersion,
                                          const std::string& comment) {
    Greenstack::HelloRequestBuilder builder;
    builder.setUserAgent(userAgent);
    builder.setUserAgentVersion(userAgentVersion);
    builder.setComment(comment);
    auto message = builder.Finish();
    Frame request;

    Greenstack::Frame::encode(message, request.payload);
    sendFrame(request);

    UniqueMessagePtr rsp = recvMessage();
    if (rsp.get() == nullptr) {
        throw std::runtime_error("Failed to create message");
    }
    auto* msg = dynamic_cast<Greenstack::HelloResponse*>(rsp.get());
    if (msg == nullptr) {
        throw std::runtime_error("Received message was not hello response");
    }
    if (msg->getStatus() != Greenstack::Status::Success) {
        throw GreenstackError("Received hello response with ",
                              msg->getStatus());
    }

    saslMechanisms.assign(msg->getSaslMechanisms());
}

void MemcachedGreenstackConnection::createBucket(const std::string& name,
                                                 const std::string& config,
                                                 const Greenstack::BucketType& type) {
    Greenstack::CreateBucketRequest request(name, config, type);
    Frame frame;
    Greenstack::Frame::encode(request, frame.payload);
    sendFrame(frame);

    UniqueMessagePtr rsp = recvMessage();
    if (rsp.get() == nullptr) {
        throw std::runtime_error("Failed to create message");
    }

    auto* msg = dynamic_cast<Greenstack::CreateBucketResponse*>(rsp.get());
    if (msg == nullptr) {
        throw std::runtime_error(
            "Received message was not create bucket response: " +
            Greenstack::to_string(rsp->getOpcode()));
    }

    if (msg->getStatus() != Greenstack::Status::Success) {
        std::string error("Create bucket [");
        error.append(name);
        error.append("] failed");
        throw GreenstackError(error, msg->getStatus());
    }
}

void MemcachedGreenstackConnection::deleteBucket(const std::string& name) {
    Greenstack::DeleteBucketRequest request(name);
    Frame frame;
    Greenstack::Frame::encode(request, frame.payload);
    sendFrame(frame);

    UniqueMessagePtr rsp = recvMessage();
    if (rsp.get() == nullptr) {
        throw std::runtime_error("Failed to create message");
    }
    auto* msg = dynamic_cast<Greenstack::DeleteBucketResponse*>(rsp.get());
    if (msg == nullptr) {
        throw std::runtime_error(
            "Received message was not delete bucket response");
    }

    if (msg->getStatus() != Greenstack::Status::Success) {
        throw GreenstackError("Delete bucket [" + name + "] failed",
                              msg->getStatus());
    }
}

void MemcachedGreenstackConnection::selectBucket(const std::string& name) {
    throw std::runtime_error(
        "Not implemented yet");
}

std::vector<std::string> MemcachedGreenstackConnection::listBuckets() {
    Greenstack::ListBucketsRequest request;
    Frame frame;
    Greenstack::Frame::encode(request, frame.payload);
    sendFrame(frame);

    UniqueMessagePtr rsp = recvMessage();
    if (rsp.get() == nullptr) {
        throw std::runtime_error("Failed to create message");
    }
    auto* msg = dynamic_cast<Greenstack::ListBucketsResponse*>(rsp.get());
    if (msg == nullptr) {
        throw std::runtime_error(
            "Received message was not list bucket response");
    }

    if (msg->getStatus() != Greenstack::Status::Success) {
        throw GreenstackError("Failed to list buckets",
                              msg->getStatus());
    }

    return msg->getBuckets();
}

Document MemcachedGreenstackConnection::get(const std::string& id,
                                            uint16_t vbucket) {
    Frame frame = encodeCmdGet(id, vbucket);
    sendFrame(frame);

    UniqueMessagePtr rsp = recvMessage();
    if (rsp.get() == nullptr) {
        throw std::runtime_error("Failed to create message");
    }

    auto* msg = dynamic_cast<Greenstack::GetResponse*>(rsp.get());
    if (msg == nullptr) {
        throw std::runtime_error("Received message was not get response");
    }

    if (msg->getStatus() == Greenstack::Status::Success) {
        Document ret;
        msg->disassemble();

        auto info = msg->getDocumentInfo();
        auto d = info.get();
        ret.info.id = d->getId();
        ret.info.cas = d->getCas();
        ret.info.compression = d->getCompression();
        ret.info.datatype = d->getDatatype();
        ret.info.expiration = d->getExpiration();
        ret.info.flags = d->getFlags();
        ret.value.resize(msg->getValue()->getSize());
        memcpy(ret.value.data(), msg->getValue()->getData(), ret.value.size());

        return ret;
    } else {
        throw GreenstackError("Get document [" + id + "] failed",
                              msg->getStatus());
    }
}

Frame MemcachedGreenstackConnection::encodeCmdGet(const std::string& id,
                                                    uint16_t vbucket) {
    Frame frame;
    Greenstack::GetRequest request(id);
    request.getFlexHeader().setVbucketId(vbucket);
    Greenstack::Frame::encode(request, frame.payload);
    return frame;
}

MutationInfo MemcachedGreenstackConnection::mutate(const Document& doc,
                                                   uint16_t vbucket,
                                                   const Greenstack::mutation_type_t type) {

    Greenstack::MutationRequest request;
    request.setMutationType(type);

    auto docinfo = std::make_shared<Greenstack::DocumentInfo>();

    docinfo->setId(doc.info.id);
    docinfo->setCompression(doc.info.compression);
    docinfo->setDatatype(doc.info.datatype);
    docinfo->setFlags(doc.info.flags);
    docinfo->setCas(doc.info.cas);
    docinfo->setExpiration(doc.info.expiration);
    request.setDocumentInfo(docinfo);

    auto value = std::make_shared<Greenstack::FixedByteArrayBuffer>(
        (uint8_t*)doc.value.data(),
        (size_t)doc.value.size());
    request.setValue(value);

    request.assemble();
    request.getFlexHeader().setVbucketId(vbucket);

    Frame frame;

    Greenstack::Frame::encode(request, frame.payload);
    sendFrame(frame);

    UniqueMessagePtr rsp = recvMessage();
    if (rsp.get() == nullptr) {
        throw std::runtime_error("Failed to create message");
    }
    auto* msg = dynamic_cast<Greenstack::MutationResponse*>(rsp.get());
    if (msg == nullptr) {
        throw std::runtime_error("Received message was not mutation response");
    }

    if (msg->getStatus() == Greenstack::Status::Success) {
        MutationInfo ret;
        ret.cas = msg->getCas();
        ret.size = msg->getSize();
        ret.seqno = msg->getSeqno();
        ret.vbucketuuid = msg->getVbucketUuid();
        return ret;
    }

    throw GreenstackError("Mutate [" + doc.info.id + "] failed",
                          msg->getStatus());
}

Greenstack::UniqueMessagePtr MemcachedGreenstackConnection::recvMessage() {
    Frame frame;
    recvFrame(frame);
    Greenstack::ByteArrayReader reader(frame.payload);
    return Greenstack::Frame::createUnique(reader);
}

unique_cJSON_ptr MemcachedGreenstackConnection::stats(const std::string& subcommand) {
    throw std::runtime_error("Not implemented for Greenstack");
}

void MemcachedGreenstackConnection::configureEwouldBlockEngine(
    const EWBEngineMode& mode, ENGINE_ERROR_CODE err_code, uint32_t value) {
    throw std::runtime_error("Not implemented for Greenstack");
}
