/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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

// This file contains the implementation of the Greenstack protocol.
// There is currently a few known problems we need to handle in the future
//   * Memory copying. We're copying the data into way too many temporary
//     buffers instead of using an iovector
//
//
#include "config.h"

#include <libgreenstack/Greenstack.h>
#include <stdexcept>
#include <map>
#include <sstream>
#include <platform/strerror.h>

#include "memcached.h"
#include "connections.h"
#include "greenstack.h"
#include "runtime.h"
#include "mcaudit.h"

#if 0

Greenstack::Status engineCode2GreenstackCode(ENGINE_ERROR_CODE code);

class GreenstackCommandContext : public CommandContext {
public:
    GreenstackCommandContext(Greenstack::UniqueMessagePtr msg)
        : message(std::move(msg)) { }

    ~GreenstackCommandContext() {
    }

    Greenstack::UniqueMessagePtr message;
};

static void handleCreateBucket(GreenstackConnection* c, Greenstack::Message* message);

static void handleDeleteBucket(GreenstackConnection* c, Greenstack::Message* message);

static void handleListBucket(GreenstackConnection* c, Greenstack::Message* message);

static void handleGet(GreenstackConnection* c, Greenstack::Message* message);

static void handleHello(GreenstackConnection* c, Greenstack::Message* message);

static void handleMutation(GreenstackConnection* c, Greenstack::Message* message);

static void handleSaslAuth(GreenstackConnection* c, Greenstack::Message* message);

typedef void (* PacketHandler)(GreenstackConnection* c, Greenstack::Message* message);

static std::map<Greenstack::Opcode, PacketHandler> handlers = {
    {Greenstack::Opcode::CreateBucket, handleCreateBucket},
    {Greenstack::Opcode::DeleteBucket, handleDeleteBucket},
    {Greenstack::Opcode::ListBuckets,  handleListBucket},
    {Greenstack::Opcode::Get,          handleGet},
    {Greenstack::Opcode::Hello,        handleHello},
    {Greenstack::Opcode::Mutation,     handleMutation},
    {Greenstack::Opcode::SaslAuth,     handleSaslAuth}
};

static void sendResponse(GreenstackConnection* c, Greenstack::Message& message,
                         Greenstack::Message* request) {
    message.setOpaque(request->getOpaque());
    if (!message.getFlexHeader().isEmpty()) {
        throw std::runtime_error("Flex header not implemented.");
    }

    // @todo optimize me. We don't want to copy between all of these
    //       temporary buffers.
    std::vector<uint8_t> data;
    Greenstack::Frame::encode(message, data);

    if (!c->growDynamicBuffer(data.size())) {
        if (settings.verbose > 0) {
            LOG_INFO(c, "<%d ERROR: Failed to allocate memory for response",
                     c->getId());
        }
        throw std::bad_alloc();
    }

    auto& buffer = c->getDynamicBuffer();
    char* buf = buffer.getCurrent();

    memcpy(buf, data.data(), data.size());
    buffer.moveOffset(data.size());
    write_and_free(c, &c->getDynamicBuffer());
}

static std::string getMechs() {
    const char* cbmechs;
    unsigned int cbmechlen;
    cbsasl_list_mechs(&cbmechs, &cbmechlen);
    return std::string(cbmechs, cbmechlen);
}

static void handleHello(GreenstackConnection* c, Greenstack::Message* message) {
    auto request = dynamic_cast<Greenstack::HelloRequest*>(message);
    std::stringstream ss;
    ss << c->getId() << "> HELLO " << request->getUserAgent() << " ("
    << request->getUserAgentVersion() << ")";

    std::string comment = request->getComment();
    if (!comment.empty()) {
        ss << " (" << comment << ")";
    }
    LOG_INFO(c, "%s", ss.str().c_str());

    Greenstack::HelloResponse response("memcached", get_server_version(),
                                       getMechs());
    LOG_INFO(c, "%d< HELLO %s [%s]", c->getId(),
             response.getUserAgent().c_str(),
             response.getSaslMechanisms().c_str());
    sendResponse(c, response, message);
    if (c->getConnectionState() == ConnectionState::ESTABLISHED) {
        c->setConnectionState(ConnectionState::OPEN);
    }
}

static void handleSaslAuth(GreenstackConnection* c, Greenstack::Message* message) {
    using Greenstack::SaslAuthRequest;
    using Greenstack::SaslAuthResponse;

    auto auth = dynamic_cast<SaslAuthRequest*>(message);
    std::string mechanism = auth->getMechanism();
    std::string challenge = auth->getChallenge();

    std::stringstream ss;
    if (mechanism.empty()) {
        ss << c->getId() << "> SASL STEP ";
    } else {
        ss << c->getId() << "> SASL AUTH " << mechanism << " ";
    }

    ss << "([";
    for (auto ch : challenge) {
        if (ch) {
            ss << ch;
        } else {
            ss << "\\0";
        }
    }
    ss << "])";
    LOG_INFO(c, "%s", ss.str().c_str());

    const char* out = NULL;
    unsigned int outlen = 0;

    int result;

    if (mechanism.empty()) {
        result = cbsasl_server_step(c->getSaslConn(),
                                    challenge.data(),
                                    challenge.size(),
                                    &out, &outlen);
    } else {
        cbsasl_conn_t* conn = c->getSaslConn();
        result = cbsasl_server_start(&conn,
                                     mechanism.c_str(),
                                     challenge.data(),
                                     challenge.size(),
                                     (unsigned char**)&out, &outlen);
        c->setSaslConn(conn);
    }

    switch (result) {
    case CBSASL_OK :
        LOG_INFO(c, "%d: Authenticated as [%s]",
                 c->getId(), c->getUsername());

        c->setAuthenticated(true);
        /*
         * We've successfully changed our user identity.
         * Update the authentication context
         */
        c->setAuthContext(auth_create(c->getUsername(),
                                      c->getPeername().c_str(),
                                      c->getSockname().c_str()));

        if (settings.disable_admin) {
            /* "everyone is admins" */
            c->setAdmin(true);
        } else if (settings.admin != NULL) {
            if (strcmp(settings.admin, c->getUsername()) == 0) {
                c->setAdmin(true);
            }
        }

        {
            SaslAuthResponse response(Greenstack::Status::Success);
            sendResponse(c, response, message);
        }
        break;

    case CBSASL_CONTINUE:
        challenge.assign(out, outlen);
        ss.str("");
        ss << c->getId() << "< SASL CONTINUE ";

        ss << "([";
        for (auto ch : challenge) {
            if (ch) {
                ss << ch;
            } else {
                ss << "\\0";
            }
        }
        ss << "])";
        LOG_INFO(c, "%s", ss.str().c_str());
        {
            SaslAuthResponse response(challenge);
            sendResponse(c, response, message);
        }
        break;

    case CBSASL_BADPARAM:
        LOG_WARNING(c, "%d: Bad sasl params: %d",
                    c->getId(), result);
        {
            SaslAuthResponse response(Greenstack::Status::InvalidArguments);
            sendResponse(c, response, message);
        }
        break;

    case CBSASL_PWERR:
    case CBSASL_NOUSER:
        if (!is_server_initialized()) {
            LOG_WARNING(c,
                        "%d: SASL AUTH failure during initialization",
                        c->getId());

            {
                SaslAuthResponse response(Greenstack::Status::NotInitialized);
                sendResponse(c, response, message);
            }
            c->setWriteAndGo(conn_closing);
            return;
        }

        audit_auth_failure(c, result == CBSASL_NOUSER ?
                              "Unknown user" : "Incorrect password");
        LOG_WARNING(c,
                    "%d: Invalid username/password combination",
                    c->getId());

        {
            SaslAuthResponse response(Greenstack::Status::AuthenticationError);
            sendResponse(c, response, message);
        }
        break;
    default: {
        SaslAuthResponse response(Greenstack::Status::InternalError);
        sendResponse(c, response, message);
    }
        LOG_WARNING(c,
                    "%d: Unknown sasl response: %d",
                    c->getId(), result);
        break;
    }
}

static void handleMutation(GreenstackConnection* c, Greenstack::Message* message) {
    auto request = dynamic_cast<Greenstack::MutationRequest*>(message);
    request->disassemble();

    auto docInfo = request->getDocumentInfo();
    auto value = request->getValue();

    if (settings.verbose > 0) {
        std::stringstream ss;
        ss << c->getId() << "> "
        << Greenstack::MutationType::to_string(request->getMutationType())
        << " Key: [" << docInfo->getId() << "] "
        << "(size: " << value->getSize();
        if (docInfo->getCas() != Greenstack::CAS::Wildcard) {
            ss << " cas: " << docInfo->getCas();
        }
        if (!docInfo->getExpiration().empty()) {
            ss << " expiration: " << docInfo->getExpiration();
        }
        ss << ")";
        LOG_INFO(c, "%s", ss.str().c_str());
    }

    if (request->getMutationType() == Greenstack::MutationType::Patch) {
        Greenstack::MutationResponse response(
            Greenstack::Status::NotImplemented);
        sendResponse(c, response, message);
        return;
    } else if (request->getMutationType() == Greenstack::MutationType::Add) {
        if (docInfo->getCas() != Greenstack::CAS::Wildcard) {
            Greenstack::MutationResponse response(
                Greenstack::Status::InvalidArguments);
            sendResponse(c, response, message);
            return;
        }
    }

    item_info iteminfo;
    memset(&iteminfo, 0, sizeof(iteminfo));
    iteminfo.nvalue = 1;
    ENGINE_ERROR_CODE ret = c->getAiostat();

    if (c->getItem() == nullptr) {
        item* it;

        if (ret == ENGINE_SUCCESS) {
            // @todo I need to fix these two
            rel_time_t expiration = 0;
            uint8_t datatype = 0;
            // @todo fix compression and datatype
            if (docInfo->getDatatype() == Greenstack::Datatype::Json) {
                datatype = PROTOCOL_BINARY_DATATYPE_JSON;
            }

            if (docInfo->getCompression() == Greenstack::Compression::Snappy) {
                datatype |= PROTOCOL_BINARY_DATATYPE_COMPRESSED;
            }

            ret = c->getBucketEngine()->allocate(c->getBucketEngineAsV0(),
                                                 c,
                                                 &it,
                                                 docInfo->getId().data(),
                                                 docInfo->getId().length(),
                                                 value->getSize(),
                                                 docInfo->getFlags(),
                                                 expiration,
                                                 datatype);
        }

        switch (ret) {
        case ENGINE_SUCCESS:
            break;
        case ENGINE_EWOULDBLOCK:
            c->setEwouldblock(true);
            return;
        case ENGINE_DISCONNECT:
            c->setState(conn_closing);
            return;
        default:
            // @todo I need to fix this
            abort();
            return;
        }

        c->getBucketEngine()->item_set_cas(c->getBucketEngineAsV0(), c,
                                           it, docInfo->getCas());

        // Get the real address and copy the data over
        if (!c->getBucketEngine()->get_item_info(c->getBucketEngineAsV0(),
                                                 c, it,
                                                 &iteminfo)) {
            LOG_WARNING(c, "%d < Internal error", c->getId());
            c->getBucketEngine()->release(c->getBucketEngineAsV0(),
                                          c, it);
            Greenstack::MutationResponse response(
                Greenstack::Status::InternalError);
            sendResponse(c, response, message);
            return;
        }

        c->setItem(it);
        cb_assert(iteminfo.value[0].iov_len >= value->getSize());
        memcpy(iteminfo.value[0].iov_base, value->getData(), value->getSize());
    }

    ENGINE_STORE_OPERATION op;
    switch (request->getMutationType()) {
    case Greenstack::MutationType::Add:
        op = OPERATION_ADD;
        break;
    case Greenstack::MutationType::Set:
        if (docInfo->getCas() == Greenstack::CAS::Wildcard) {
            op = OPERATION_SET;
        } else {
            op = OPERATION_CAS;
        }
        break;
    case Greenstack::MutationType::Replace:
        if (docInfo->getCas() == Greenstack::CAS::Wildcard) {
            op = OPERATION_REPLACE;
        } else {
            op = OPERATION_CAS;
        }
        break;
    case Greenstack::MutationType::Append:
        op = OPERATION_APPEND;
        break;
    case Greenstack::MutationType::Prepend:
        op = OPERATION_PREPEND;
        break;
    case Greenstack::MutationType::Patch:
        throw std::logic_error("Not implemented yet");
    default:
        throw std::invalid_argument("Invalid mutation type " +
                                    std::to_string(
                                        int(request->getMutationType())));
    }

    if (ret == ENGINE_SUCCESS) {
        uint64_t cas = c->getCAS();
        // @todo fix default engine so that we don't have to do the mapping
        //       above
        ret = c->getBucketEngine()->store(c->getBucketEngineAsV0(),
                                          c, c->getItem(), &cas, op,
                                          request->getFlexHeader().getVbucketId());
        if (ret == ENGINE_SUCCESS) {
            c->setCAS(cas);
        }
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        /* Stored */
        memset(&iteminfo, 0, sizeof(iteminfo));
        iteminfo.nvalue = 1;
        if (!c->getBucketEngine()->get_item_info(c->getBucketEngineAsV0(),
                                                 c, c->getItem(), &iteminfo)) {
            c->getBucketEngine()->release(c->getBucketEngineAsV0(),
                                          c, c->getItem());
            LOG_WARNING(c, "%d < Internal error", c->getId());
            Greenstack::MutationResponse response(
                Greenstack::Status::InternalError);
            sendResponse(c, response, message);
            return;
        } else {
            Greenstack::MutationResponse response(c->getCAS(), iteminfo.nbytes,
                                                  iteminfo.seqno,
                                                  iteminfo.vbucket_uuid);
            sendResponse(c, response, message);

            if (settings.verbose > 0) {
                std::stringstream ss;
                ss << c->getId() << "< "
                <<
                Greenstack::MutationType::to_string(request->getMutationType())
                << " Key: [" << docInfo->getId() << "] "
                << "(size: " << iteminfo.nbytes
                << " cas: " << c->getCAS()
                << " seqno: " << iteminfo.seqno
                << " uuid: " << iteminfo.vbucket_uuid
                << ")";
                LOG_INFO(c, "%s", ss.str().c_str());
            }
        }
        break;
    case ENGINE_EWOULDBLOCK:
        c->setEwouldblock(true);
        return;

    case ENGINE_DISCONNECT:
        c->setState(conn_closing);
        break;

    case ENGINE_NOT_STORED:
        // remap some of the errors
        if (op == OPERATION_ADD) {
            ret = ENGINE_KEY_EEXISTS;
        } else if (op == OPERATION_REPLACE) {
            ret = ENGINE_KEY_ENOENT;
        }

    default:
        do {
            Greenstack::Response response;
            response.setOpcode(request->getOpcode());
            response.setStatus(engineCode2GreenstackCode(ret));
            sendResponse(c, response, request);
        } while (0);
    }

    /* release the c->item reference */
    c->getBucketEngine()->release(c->getBucketEngineAsV0(), c, c->getItem());
    c->setItem(nullptr);
}


static void handleGet(GreenstackConnection* c, Greenstack::Message* message) {
    auto request = dynamic_cast<Greenstack::GetRequest*>(message);
    std::string id = request->getId();

    if (settings.verbose > 0) {
        LOG_INFO(c, "%d > GET %s", c->getId(), id.c_str());
    }

    item* it;
    ENGINE_ERROR_CODE ret = c->getAiostat();
    c->setAiostat(ENGINE_SUCCESS);
    if (ret == ENGINE_SUCCESS) {
        ret = c->getBucketEngine()->get(c->getBucketEngineAsV0(), c, &it,
                                        id.c_str(), id.length(),
                                        request->getFlexHeader().getVbucketId());
    }

    item_info info;
    memset(&info, 0, sizeof(info));
    // @todo support more later on!
    info.nvalue = 1;

    switch (ret) {
    case ENGINE_SUCCESS:
        if (c->getBucketEngine()->get_item_info(c->getBucketEngineAsV0(),
                                                c, it, &info)) {
            auto docinfo = std::make_shared<Greenstack::DocumentInfo>();
            auto value = std::make_shared<Greenstack::FixedByteArrayBuffer>(
                static_cast<uint8_t*>(info.value[0].iov_base),
                static_cast<size_t>(info.value[0].iov_len));
            docinfo->setId(id);
            if (info.datatype & PROTOCOL_BINARY_DATATYPE_COMPRESSED) {
                docinfo->setCompression(Greenstack::Compression::Snappy);
            }

            if (info.datatype & PROTOCOL_BINARY_DATATYPE_JSON) {
                docinfo->setDatatype(Greenstack::Datatype::Json);
            }

            docinfo->setFlags(info.flags);
            docinfo->setCas(info.cas);

            Greenstack::GetResponse response;
            response.setDocumentInfo(docinfo);
            response.setValue(value);
            response.assemble();

            sendResponse(c, response, message);
        } else {
            LOG_WARNING(c, "%d: Failed to get item info", c->getId());
            c->getBucketEngine()->release(c->getBucketEngineAsV0(), c, it);
            Greenstack::GetResponse response(Greenstack::Status::InternalError);
            sendResponse(c, response, message);
        }
        // release allocated resources
        // @todo refactor so I don't have to memcpy..
        c->getBucketEngine()->release(c->getBucketEngineAsV0(), c, it);
        break;

    case ENGINE_KEY_ENOENT: {
        Greenstack::GetResponse response(Greenstack::Status::NotFound);
        sendResponse(c, response, message);
    }
        break;
    case ENGINE_EWOULDBLOCK:
        c->setEwouldblock(true);
        break;
    case ENGINE_DISCONNECT:
        c->setState(conn_closing);
        break;
    default:
        // @todo update me@@
        abort();
    }
}

static void handleCreateBucket(GreenstackConnection* c, Greenstack::Message* message) {
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    CreateBucketContext* context = nullptr;
    try {
        auto* request = dynamic_cast<Greenstack::CreateBucketRequest*>(message);
        context = new CreateBucketContext(*c);
        context->name = request->getName();
        context->config = request->getConfig();

        switch (request->getType()) {
        case Greenstack::BucketType::Memcached:
            context->type = BucketType::Memcached;
            break;
        case Greenstack::BucketType::Couchbase:
            context->type = BucketType::Couchstore;
            break;
        case Greenstack::BucketType::EWouldBlock:
            context->type = BucketType::EWouldBlock;
            break;
        default:
            context->type = BucketType::Unknown;
        }

        if (context->type == BucketType::Unknown) {
            ret = ENGINE_EINVAL;
        }
    } catch (std::bad_alloc&) {
        LOG_WARNING(c, "%d !! Create Bucket ENOMEM", c->getId());
        ret = ENGINE_ENOMEM;
        delete context;
    }

    if (ret == ENGINE_SUCCESS) {
        std::string buckettype = to_string(context->type);
        LOG_NOTICE(c, "%d > Create %s Bucket %s [%s]", c->getId(),
                   buckettype.c_str(), context->name.c_str(),
                   context->config.c_str());
        c->setAiostat(ENGINE_SUCCESS);
        c->setEwouldblock(false);
        cb_thread_t tid;
        int err = cb_create_thread(&tid, create_bucket_main, context, 1);

        if (err == 0) {
            ret = ENGINE_EWOULDBLOCK;
        } else {
            std::string msg = cb_strerror(err);
            LOG_WARNING(c, "Failed to create thread to create bucket: %s",
                        msg.c_str());
            ret = ENGINE_FAILED;
            delete context;
        }
    }

    if (ret == ENGINE_EWOULDBLOCK) {
        c->setEwouldblock(true);
        c->setState(conn_create_bucket);
    } else {
        // @todo remap error code
        Greenstack::CreateBucketResponse response(
            Greenstack::Status::InternalError);
        sendResponse(c, response, message);
    }
}

static void handleDeleteBucket(GreenstackConnection* c, Greenstack::Message* message) {
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    DeleteBucketContext* context = nullptr;
    try {
        auto* request = dynamic_cast<Greenstack::DeleteBucketRequest*>(message);
        context = new DeleteBucketContext(*c);
        context->name = request->getName();
        context->force = request->isForce();
    } catch (std::bad_alloc&) {
        LOG_WARNING(c, "%d !! Delete Bucket ENOMEM", c->getId());

        ret = ENGINE_ENOMEM;
        delete context;
    }

    if (ret == ENGINE_SUCCESS) {
        LOG_NOTICE(c, "%d > Delete Bucket [%s]%s",
                   c->getId(), context->name.c_str(),
                   context->force ? " [force]" : "");

        c->setAiostat(ENGINE_SUCCESS);
        c->setEwouldblock(false);
        cb_thread_t tid;
        int err = cb_create_thread(&tid, delete_bucket_main, context, 1);

        if (err == 0) {
            ret = ENGINE_EWOULDBLOCK;
        } else {
            std::string msg = cb_strerror(err);
            LOG_WARNING(c, "Failed to create thread to delete bucket: %s",
                        msg.c_str());
            ret = ENGINE_FAILED;
            delete context;
        }
    }

    if (ret == ENGINE_EWOULDBLOCK) {
        c->setEwouldblock(true);
        c->setState(conn_delete_bucket);
    } else {
        Greenstack::CreateBucketResponse response(
            Greenstack::Status::InternalError);
        sendResponse(c, response, message);
    }
}

static void handleListBucket(GreenstackConnection* c, Greenstack::Message* message) {
    LOG_INFO(c, "%d > LIST BUCKETS", c->getId());
    std::vector<std::string> buckets;
    bool mem_error = false;

    for (auto& bucket : all_buckets) {
        cb_mutex_enter(&bucket.mutex);
        if (bucket.state == BucketState::Ready &&
            bucket.type != BucketType::NoBucket) {
            try {
                buckets.push_back(bucket.name);
            } catch (std::bad_alloc&) {
                mem_error = true;
            }
        }
        cb_mutex_exit(&bucket.mutex);
    }

    if (mem_error) {
        Greenstack::ListBucketsResponse response(Greenstack::Status::NoMemory);
        sendResponse(c, response, message);
    } else {
        Greenstack::ListBucketsResponse response(buckets);
        sendResponse(c, response, message);
    }
    LOG_INFO(c, "%d < LIST BUCKETS", c->getId());
}


// @todo I should send back a untagged server message
// if I'm failing to disconnect
int try_read_greenstack_command(GreenstackConnection* c) {
    if (c == nullptr) {
        throw std::runtime_error("Internal error, read called on non-greens"
                                     "tack connection");
    }
    while (c->read.bytes > 4) {
        Greenstack::FixedByteArrayBuffer buffer(
            reinterpret_cast<uint8_t*>(c->read.curr),
            c->read.bytes);
        Greenstack::ByteArrayReader reader(buffer);

        uint32_t frameLength;
        reader.read(frameLength);

        if (frameLength > settings.max_packet_size) {
            // @todo Create an untagged server command saying that
            //       we'll shut down the connection
            LOG_WARNING(c, "%d: Frame exceeds max limit. "
                "Shutting down connection", c->getId());
            c->setState(conn_closing);
            return -1;
        }

        if (frameLength > reader.getRemainder()) {
            // We don't have the entire frame!
            LOG_DETAIL(c,
                       "%d: Not enough data.. wait for more data (%u < %u)",
                       c->getId(), frameLength, reader.getRemainder());
            return 0;
        }

        // "undo" the read of the 4 header bytes
        reader.seek(-4);
        try {
            auto message = Greenstack::Frame::createUnique(reader);
            uint32_t packetSize = frameLength + 4;
            c->read.curr += packetSize;
            c->read.bytes -= packetSize;

            if (message.get() == nullptr) {
                LOG_WARNING(c, "%d: Failed to decode Frame", c->getId());
                c->setState(conn_closing);
                return -1;
            }

            // execute the command!!
            auto iter = handlers.find(message->getOpcode());
            if (iter == handlers.end()) {
                // @todo send back unknown command message
                LOG_WARNING(c, "%d: No command handler",
                            c->getId());
            } else {
                if (!c->addMsgHdr(true)) {
                    if (settings.verbose) {
                        LOG_WARNING(c,
                                    "%d: Failed to create output headers. Shutting down connection",
                                    c->getId());
                    }
                    c->setState(conn_closing);
                    return 1;
                }

                // In Greenstack all connections needs to start with sending HELLO
                if (c->getConnectionState() == ConnectionState::ESTABLISHED) {
                    if (message->getOpcode() != Greenstack::Opcode::Hello) {
                        LOG_WARNING(c,
                                    "%d: Invalid state, waiting for HELLO",
                                    c->getId());

                        // Create a reply saying its not allowed
                        Greenstack::Response response;
                        response.setStatus(Greenstack::Status::InvalidState);
                        sendResponse(c, response, message.get());
                        return 1;
                    }
                }

                switch (c->checkAccess(message->getOpcode())) {
                case AuthResult::FAIL: {
                    /* @TODO Should go to audit */
                    std::string opcode = Greenstack::to_string(
                        message->getOpcode());

                    std::stringstream ss;
                    ss << c->getId() << " (" << c->getPeername() << " <=> "
                    << c->getSockname() << ") [" << c->getUsername()
                    << "] No access to command: " << opcode;
                    const char* txt = ss.str().c_str();

                    LOG_WARNING(c, "%s", txt);

                    // Create a reply saying its not allowed
                    Greenstack::Response response;
                    response.setStatus(Greenstack::Status::NoAccess);
                    sendResponse(c, response, message.get());
                    return 1;
                }
                    break;
                case AuthResult::OK:
                    iter->second(c, message.get());
                    break;
                case AuthResult::STALE: {
                    // Create a reply saying its not allowed
                    Greenstack::Response response;
                    response.setStatus(Greenstack::Status::AuthenticationStale);
                    sendResponse(c, response, message.get());
                    return 1;
                }
                    break;
                default:
                    throw std::logic_error(
                        "Invalid response returned for auth_check");
                }

                if (c->isEwouldblock()) {
                    c->setCommandContext(new GreenstackCommandContext(std::move(message)));
                    return 1;
                }

                if (c->getState() == conn_write) {
                    return 1;
                }
            }
        } catch (std::runtime_error& exp) {
            LOG_WARNING(c, "%d: An exception occured during parsing: [%s]",
                        c->getId(), exp.what());
            // @todo send a untagged server command telling why the connection is terminated
            c->setState(conn_closing);
            return -1;
        }
    }

    // we need more memory
    return 0;
}

Greenstack::Status engineCode2GreenstackCode(ENGINE_ERROR_CODE code) {
    switch (code) {
    case ENGINE_SUCCESS:
        return Greenstack::Status::Success;
    case ENGINE_KEY_ENOENT:
        return Greenstack::Status::NotFound;
    case ENGINE_KEY_EEXISTS:
        return Greenstack::Status::AlreadyExists;
    case ENGINE_ENOMEM:
        return Greenstack::Status::NoMemory;
    case ENGINE_NOT_STORED:
        return Greenstack::Status::NotStored;
    case ENGINE_EINVAL:
        return Greenstack::Status::InvalidArguments;
    case ENGINE_ENOTSUP:
        return Greenstack::Status::NotImplemented;
    case ENGINE_EWOULDBLOCK:
        throw std::logic_error("impossible to map EWOULDBLOCK");
    case ENGINE_E2BIG:
        return Greenstack::Status::ObjectTooBig;
    case ENGINE_WANT_MORE:
        throw std::logic_error("impossible to map WANT_MORE");
    case ENGINE_DISCONNECT:
        throw std::logic_error("impossible to map DISCONNECT");
    case ENGINE_EACCESS:
        return Greenstack::Status::NoAccess;
    case ENGINE_NOT_MY_VBUCKET:
        return Greenstack::Status::NotMyVBucket;
    case ENGINE_TMPFAIL:
        return Greenstack::Status::TmpFailure;
    case ENGINE_ERANGE:
        return Greenstack::Status::IllegalRange;
    case ENGINE_ROLLBACK:
        return Greenstack::Status::Rollback;
    case ENGINE_NO_BUCKET:
        return Greenstack::Status::NoBucket;
    case ENGINE_EBUSY:
        return Greenstack::Status::TooBusy;
    case ENGINE_FAILED:
        return Greenstack::Status::InternalError;
    default:
        throw std::logic_error(
            "Error message for " + std::to_string(int(code)) +
            " is not implemented yet");
    }
}

void greenstack_write_response(GreenstackConnection* c, ENGINE_ERROR_CODE code) {
    auto* ctx = dynamic_cast<GreenstackCommandContext*>(c->getCommandContext());
    if (ctx == nullptr) {
        throw std::logic_error(
            "greenstack_write_response expects command context to be set");
    }

    Greenstack::Response response;
    response.setOpcode(ctx->message->getOpcode());
    response.setStatus(engineCode2GreenstackCode(code));
    sendResponse(c, response, ctx->message.get());
}

#endif
