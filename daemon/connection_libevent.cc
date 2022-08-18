/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "connection_libevent.h"

#include "buckets.h"
#include "connections.h"
#include "cookie.h"
#include "front_end_thread.h"
#include "listening_port.h"
#include "mcaudit.h"
#include "sendbuffer.h"
#include "settings.h"
#include "tracing.h"

#include <event2/bufferevent.h>
#include <event2/bufferevent_ssl.h>
#include <logger/logger.h>
#include <mcbp/protocol/header.h>
#include <phosphor/phosphor.h>
#include <platform/string_hex.h>

LibeventConnection::LibeventConnection(SOCKET sfd,
                                       FrontEndThread& thr,
                                       std::shared_ptr<ListeningPort> descr,
                                       uniqueSslPtr sslStructure)
    : Connection(sfd, thr, std::move(descr), sslStructure ? true : false) {
    // We need to use BEV_OPT_UNLOCK_CALLBACKS (which again require
    // BEV_OPT_DEFER_CALLBACKS) to avoid lock ordering problem (and
    // potential deadlock) because otherwise we'll hold the internal mutex
    // in libevent as part of the callback and later on we acquire the
    // worker threads mutex, but when we try to signal another cookie we
    // hold the worker thread mutex when we try to acquire the mutex inside
    // libevent.
    const auto options = BEV_OPT_THREADSAFE | BEV_OPT_UNLOCK_CALLBACKS |
                         BEV_OPT_CLOSE_ON_FREE | BEV_OPT_DEFER_CALLBACKS;
    if (ssl) {
        bev.reset(
                bufferevent_openssl_socket_new(thr.eventBase.getLibeventBase(),
                                               sfd,
                                               sslStructure.release(),
                                               BUFFEREVENT_SSL_ACCEPTING,
                                               options));
        bufferevent_setcb(bev.get(),
                          LibeventConnection::ssl_read_callback,
                          LibeventConnection::write_callback,
                          LibeventConnection::event_callback,
                          static_cast<void*>(this));
    } else {
        bev.reset(bufferevent_socket_new(
                thr.eventBase.getLibeventBase(), sfd, options));
        bufferevent_setcb(bev.get(),
                          LibeventConnection::read_callback,
                          LibeventConnection::write_callback,
                          LibeventConnection::event_callback,
                          static_cast<void*>(this));
    }

    bufferevent_enable(bev.get(), EV_READ);
}

LibeventConnection::~LibeventConnection() {
    if (bev) {
        bev.reset();
        stats.curr_conns.fetch_sub(1, std::memory_order_relaxed);
    }
}

std::string LibeventConnection::getOpenSSLErrors() {
    unsigned long err;
    auto buffer = thread.getScratchBuffer();
    std::vector<std::string> messages;
    while ((err = bufferevent_get_openssl_error(bev.get())) != 0) {
        std::stringstream ss;
        ERR_error_string_n(err, buffer.data(), buffer.size());
        ss << "{" << buffer.data() << "},";
        messages.emplace_back(ss.str());
    }

    if (messages.empty()) {
        return {};
    }

    if (messages.size() == 1) {
        // remove trailing ,
        messages.front().pop_back();
        return messages.front();
    }

    std::reverse(messages.begin(), messages.end());
    std::string ret = "[";
    for (const auto& a : messages) {
        ret.append(a);
    }
    ret.back() = ']';
    return ret;
}

void LibeventConnection::read_callback() {
    if (isSslEnabled()) {
        const auto ssl_errors = getOpenSSLErrors();
        if (!ssl_errors.empty()) {
            LOG_INFO("{} - OpenSSL errors reported: {}",
                     this->getId(),
                     ssl_errors);
        }
    }

    TRACE_LOCKGUARD_TIMED(thread.mutex,
                          "mutex",
                          "Connection::read_callback::threadLock",
                          SlowMutexThreshold);

    if (!executeCommandsCallback()) {
        conn_destroy(this);
    }
}

void LibeventConnection::read_callback(bufferevent*, void* ctx) {
    reinterpret_cast<LibeventConnection*>(ctx)->read_callback();
}

void LibeventConnection::write_callback() {
    if (isSslEnabled()) {
        const auto ssl_errors = getOpenSSLErrors();
        if (!ssl_errors.empty()) {
            LOG_INFO("{} - OpenSSL errors reported: {}", getId(), ssl_errors);
        }
    }

    TRACE_LOCKGUARD_TIMED(thread.mutex,
                          "mutex",
                          "Connection::rw_callback::threadLock",
                          SlowMutexThreshold);

    if (!executeCommandsCallback()) {
        conn_destroy(this);
    }
}

void LibeventConnection::write_callback(bufferevent*, void* ctx) {
    reinterpret_cast<LibeventConnection*>(ctx)->write_callback();
}

static nlohmann::json BevEvent2Json(short event) {
    if (!event) {
        return {};
    }
    nlohmann::json err = nlohmann::json::array();

    if ((event & BEV_EVENT_READING) == BEV_EVENT_READING) {
        err.push_back("reading");
    }
    if ((event & BEV_EVENT_WRITING) == BEV_EVENT_WRITING) {
        err.push_back("writing");
    }
    if ((event & BEV_EVENT_EOF) == BEV_EVENT_EOF) {
        err.push_back("EOF");
    }
    if ((event & BEV_EVENT_ERROR) == BEV_EVENT_ERROR) {
        err.push_back("error");
    }
    if ((event & BEV_EVENT_TIMEOUT) == BEV_EVENT_TIMEOUT) {
        err.push_back("timeout");
    }
    if ((event & BEV_EVENT_CONNECTED) == BEV_EVENT_CONNECTED) {
        err.push_back("connected");
    }

    const short known = BEV_EVENT_READING | BEV_EVENT_WRITING | BEV_EVENT_EOF |
                        BEV_EVENT_ERROR | BEV_EVENT_TIMEOUT |
                        BEV_EVENT_CONNECTED;

    if (event & ~known) {
        err.push_back(cb::to_hex(uint16_t(event)));
    }

    return err;
}

void LibeventConnection::event_callback(bufferevent* bev,
                                        short event,
                                        void* ctx) {
    auto& instance = *reinterpret_cast<LibeventConnection*>(ctx);
    bool term = false;

    std::string ssl_errors;
    if (instance.isSslEnabled()) {
        ssl_errors = instance.getOpenSSLErrors();
    }

    if ((event & BEV_EVENT_EOF) == BEV_EVENT_EOF) {
        LOG_DEBUG("{}: Socket EOF", instance.getId());
        instance.setTerminationReason("Client closed connection");
        term = true;
    } else if ((event & BEV_EVENT_ERROR) == BEV_EVENT_ERROR) {
        // Note: SSL connections may fail for reasons different than socket
        // error, so we avoid to dump errno:0 (ie, socket operation success).
        const auto sockErr = EVUTIL_SOCKET_ERROR();
        if (sockErr != 0) {
            const auto errStr = evutil_socket_error_to_string(sockErr);
            if (sockErr == ECONNRESET) {
                LOG_INFO(
                        "{}: Unrecoverable error encountered: {}, "
                        "socket_error: {}:{}, shutting down connection",
                        instance.getId(),
                        BevEvent2Json(event).dump(),
                        sockErr,
                        errStr);
            } else {
                LOG_WARNING(
                        "{}: Unrecoverable error encountered: {}, "
                        "socket_error: {}:{}, shutting down connection",
                        instance.getId(),
                        BevEvent2Json(event).dump(),
                        sockErr,
                        errStr);
            }
            instance.setTerminationReason(
                    "socket_error: " + std::to_string(sockErr) + ":" + errStr);
        } else if (!ssl_errors.empty()) {
            LOG_WARNING(
                    "{}: Unrecoverable error encountered: {}, ssl_error: "
                    "{}, shutting down connection",
                    instance.getId(),
                    BevEvent2Json(event).dump(),
                    ssl_errors);
            instance.setTerminationReason("ssl_error: " + ssl_errors);
        } else {
            LOG_WARNING(
                    "{}: Unrecoverable error encountered: {}, shutting down "
                    "connection",
                    instance.getId(),
                    BevEvent2Json(event).dump());
            instance.setTerminationReason("Network error");
        }

        term = true;
    }

    if (term) {
        auto& thread = instance.getThread();
        TRACE_LOCKGUARD_TIMED(thread.mutex,
                              "mutex",
                              "Connection::event_callback::threadLock",
                              SlowMutexThreshold);
        // MB-44460: If a connection disconnects before all of the data
        //           was moved to the kernels send buffer we would still
        //           wait for the send buffer to be drained before closing
        //           the connection. Given that the other side hung up that
        //           will never happen so we should just terminate the
        //           send queue immediately.
        //           note: This extra complexity was added so that we could
        //           send error messages back to the client and then
        //           disconnect the socket once all data was sent to the
        //           client (and bufferevent performs the actual send/recv
        //           on the socket after the callback returned)
        instance.sendQueueInfo.term = true;

        if (instance.state == State::running) {
            instance.shutdown();
        }

        if (!instance.executeCommandsCallback()) {
            conn_destroy(&instance);
        }
    }
}

void LibeventConnection::ssl_read_callback(bufferevent* bev, void* ctx) {
    auto& instance = *reinterpret_cast<LibeventConnection*>(ctx);

    const auto ssl_errors = instance.getOpenSSLErrors();
    if (!ssl_errors.empty()) {
        LOG_INFO("{} - OpenSSL errors reported: {}",
                 instance.getId(),
                 ssl_errors);
    }

    // Let's inspect the certificate before we'll do anything further
    auto* ssl_st = bufferevent_openssl_get_ssl(bev);
    const auto verifyMode = SSL_get_verify_mode(ssl_st);
    const auto enabled = ((verifyMode & SSL_VERIFY_PEER) == SSL_VERIFY_PEER);

    bool disconnect = false;
    cb::openssl::unique_x509_ptr cert(SSL_get_peer_certificate(ssl_st));
    if (enabled) {
        const auto mandatory =
                ((verifyMode & SSL_VERIFY_FAIL_IF_NO_PEER_CERT) ==
                 SSL_VERIFY_FAIL_IF_NO_PEER_CERT);
        // Check certificate
        if (cert) {
            class ServerAuthMapper {
            public:
                static std::pair<cb::x509::Status, std::string> lookup(
                        X509* cert) {
                    static ServerAuthMapper inst;
                    return inst.mapper->lookupUser(cert);
                }

            protected:
                ServerAuthMapper() {
                    mapper = cb::x509::ClientCertConfig::create(R"({
"prefixes": [
    {
        "path": "san.email",
        "prefix": "",
        "delimiter": "",
        "suffix":"@internal.couchbase.com"
    }
]
})"_json);
                }

                std::unique_ptr<cb::x509::ClientCertConfig> mapper;
            };

            auto [status, name] = ServerAuthMapper::lookup(cert.get());
            if (status == cb::x509::Status::Success) {
                if (name == "internal") {
                    name = "@internal";
                } else {
                    status = cb::x509::Status::NoMatch;
                }
            } else {
                auto pair = Settings::instance().lookupUser(cert.get());
                status = pair.first;
                name = std::move(pair.second);
            }

            switch (status) {
            case cb::x509::Status::NoMatch:
                audit_auth_failure(instance,
                                   {"unknown", cb::sasl::Domain::Local},
                                   "Failed to map a user from the client "
                                   "provided X.509 certificate");
                instance.setTerminationReason(
                        "Failed to map a user from the client provided X.509 "
                        "certificate");
                LOG_WARNING(
                        "{}: Failed to map a user from the "
                        "client provided X.509 certificate: [{}]",
                        instance.getId(),
                        name);
                disconnect = true;
                break;
            case cb::x509::Status::Error:
                audit_auth_failure(
                        instance,
                        {"unknown", cb::sasl::Domain::Local},
                        "Failed to use client provided X.509 certificate");
                instance.setTerminationReason(
                        "Failed to use client provided X.509 certificate");
                LOG_WARNING(
                        "{}: Disconnection client due to error with the X.509 "
                        "certificate [{}]",
                        instance.getId(),
                        name);
                disconnect = true;
                break;
            case cb::x509::Status::NotPresent:
                // Note: NotPresent in this context is that there is no
                //       mapper present in the _configuration_ which is
                //       allowed in "Enabled" mode as it just means that we'll
                //       try to verify the peer.
                if (mandatory) {
                    const char* reason =
                            "The server does not have any mapping rules "
                            "configured for certificate authentication";
                    audit_auth_failure(instance,
                                       {"unknown", cb::sasl::Domain::Local},
                                       reason);
                    instance.setTerminationReason(reason);
                    disconnect = true;
                    LOG_WARNING("{}: Disconnecting client: {}",
                                instance.getId(),
                                reason);
                }
                break;
            case cb::x509::Status::Success:
                if (!instance.tryAuthFromSslCert(name,
                                                 SSL_get_cipher_name(ssl_st))) {
                    // Already logged
                    const std::string reason =
                            "User [" + name + "] not defined in Couchbase";
                    audit_auth_failure(instance,
                                       {name, cb::sasl::Domain::Local},
                                       reason.c_str());
                    instance.setTerminationReason(reason.c_str());
                    disconnect = true;
                }
            }
        }
    }

    if (disconnect) {
        instance.shutdown();
    } else if (!instance.authenticated) {
        // tryAuthFromSslCertificate logged the cipher
        LOG_INFO("{}: Using cipher '{}', peer certificate {}provided",
                 instance.getId(),
                 SSL_get_cipher_name(ssl_st),
                 cert ? "" : "not ");
    }

    // update the callback to call the normal read callback
    bufferevent_setcb(bev,
                      LibeventConnection::read_callback,
                      LibeventConnection::write_callback,
                      LibeventConnection::event_callback,
                      ctx);

    // and let's call it to make sure we step through the state machinery
    LibeventConnection::read_callback(bev, ctx);
}

void LibeventConnection::triggerCallback() {
    const auto opt = BEV_TRIG_IGNORE_WATERMARKS | BEV_TRIG_DEFER_CALLBACKS;
    bufferevent_trigger(bev.get(), EV_READ, opt);
}

void LibeventConnection::copyToOutputStream(std::string_view data) {
    if (data.empty()) {
        return;
    }

    if (bufferevent_write(bev.get(), data.data(), data.size()) == -1) {
        throw std::bad_alloc();
    }

    updateSendBytes(data.size());
}

void LibeventConnection::copyToOutputStream(gsl::span<std::string_view> data) {
    size_t nb = 0;
    for (const auto& d : data) {
        if (bufferevent_write(bev.get(), d.data(), d.size()) == -1) {
            throw std::bad_alloc();
        }
        nb += d.size();
    }
    updateSendBytes(nb);
}

static void sendbuffer_cleanup_cb(const void*, size_t, void* extra) {
    delete reinterpret_cast<SendBuffer*>(extra);
}

void LibeventConnection::chainDataToOutputStream(
        std::unique_ptr<SendBuffer> buffer) {
    if (!buffer || buffer->getPayload().empty()) {
        throw std::logic_error(
                "Connection::chainDataToOutputStream: buffer must be set");
    }

    auto data = buffer->getPayload();
    if (evbuffer_add_reference(bufferevent_get_output(bev.get()),
                               data.data(),
                               data.size(),
                               sendbuffer_cleanup_cb,
                               buffer.get()) == -1) {
        throw std::bad_alloc();
    }

    // Buffer successfully added to libevent and the callback
    // (sendbuffer_cleanup_cb) will free the memory.
    // Move the ownership of the buffer!
    (void)buffer.release();
    updateSendBytes(data.size());
}

bool LibeventConnection::isPacketAvailable() const {
    auto* event = bev.get();
    auto* input = bufferevent_get_input(event);
    auto size = evbuffer_get_length(input);
    if (size < sizeof(cb::mcbp::Header)) {
        return false;
    }

    const auto* header = reinterpret_cast<const cb::mcbp::Header*>(
            evbuffer_pullup(input, sizeof(cb::mcbp::Header)));
    if (header == nullptr) {
        throw std::runtime_error(
                "Connection::isPacketAvailable(): Failed to reallocate event "
                "input buffer: " +
                std::to_string(sizeof(cb::mcbp::Header)));
    }

    if (!header->isValid()) {
        audit_invalid_packet(*this, getAvailableBytes());
        throw std::runtime_error(
                "Connection::isPacketAvailable(): Invalid packet header "
                "detected");
    }

    const auto framesize = sizeof(*header) + header->getBodylen();
    if (size >= framesize) {
        // We've got the entire buffer available.. make sure it is continuous
        if (evbuffer_pullup(input, framesize) == nullptr) {
            throw std::runtime_error(
                    "Connection::isPacketAvailable(): Failed to reallocate "
                    "event input buffer: " +
                    std::to_string(framesize));
        }
        return true;
    }

    // We don't have the entire frame available.. Are we receiving an
    // incredible big packet so that we want to disconnect the client?
    if (framesize > Settings::instance().getMaxPacketSize()) {
        throw std::runtime_error(
                "Connection::isPacketAvailable(): The packet size " +
                std::to_string(framesize) +
                " exceeds the max allowed packet size " +
                std::to_string(Settings::instance().getMaxPacketSize()));
    }

    return false;
}

const cb::mcbp::Header& LibeventConnection::getPacket() const {
    // Drain all the data available in bufferevent into the
    // socket read buffer
    auto* event = bev.get();
    auto* input = bufferevent_get_input(event);
    auto nb = evbuffer_get_length(input);
    if (nb < sizeof(cb::mcbp::Header)) {
        throw std::runtime_error(
                "Connection::getPacket(): packet not available");
    }

    return *reinterpret_cast<const cb::mcbp::Header*>(
            evbuffer_pullup(input, sizeof(cb::mcbp::Header)));
}

void LibeventConnection::drainInputPipe(size_t bytes) {
    auto* event = bev.get();
    auto* input = bufferevent_get_input(event);
    if (evbuffer_drain(input, bytes) == -1) {
        throw std::runtime_error(
                "LibeventConnection::drainInputPipe(): Failed to drain buffer");
    }
}

cb::const_byte_buffer LibeventConnection::getAvailableBytes(size_t max) const {
    auto* input = bufferevent_get_input(bev.get());
    auto nb = std::min(evbuffer_get_length(input), max);
    return {evbuffer_pullup(input, nb), nb};
}

void LibeventConnection::disableReadEvent() {
    if ((bufferevent_get_enabled(bev.get()) & EV_READ) == EV_READ) {
        if (bufferevent_disable(bev.get(), EV_READ) == -1) {
            throw std::runtime_error(
                    "Connection::disableReadEvent: Failed to disable read "
                    "events");
        }
    }
}

void LibeventConnection::enableReadEvent() {
    if ((bufferevent_get_enabled(bev.get()) & EV_READ) == 0) {
        if (bufferevent_enable(bev.get(), EV_READ) == -1) {
            throw std::runtime_error(
                    "Connection::enableReadEvent: Failed to enable read "
                    "events");
        }
    }
}

size_t LibeventConnection::getSendQueueSize() const {
    return evbuffer_get_length(bufferevent_get_output(bev.get()));
}
