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
#include "cookie.h"
#include "front_end_thread.h"
#include "listening_port.h"
#include "mcaudit.h"
#include "platform/backtrace.h"
#include "sendbuffer.h"
#include "settings.h"
#include "tracing.h"

#include <event2/bufferevent.h>
#include <event2/bufferevent_ssl.h>
#include <logger/logger.h>
#include <mcbp/protocol/header.h>
#include <openssl/err.h>
#include <phosphor/phosphor.h>
#include <platform/string_hex.h>

/// Don't allow unauthenticated clients send large packets to
/// consume memory on the server (for instance send everything except
/// the last byte of a request and let the server be stuck waiting
/// for the last byte of a 20MB command)
static constexpr size_t MaxUnauthenticatedFrameSize = 16 * 1024;

LibeventConnection::LibeventConnection(SOCKET sfd,
                                       FrontEndThread& thr,
                                       std::shared_ptr<ListeningPort> descr,
                                       uniqueSslPtr sslStructure)
    : Connection(sfd, thr, std::move(descr)) {
    // We need to use BEV_OPT_UNLOCK_CALLBACKS (which again require
    // BEV_OPT_DEFER_CALLBACKS) to avoid lock ordering problem (and
    // potential deadlock) because otherwise we'll hold the internal mutex
    // in libevent as part of the callback and later on we acquire the
    // worker threads mutex, but when we try to signal another cookie we
    // hold the worker thread mutex when we try to acquire the mutex inside
    // libevent.
    constexpr auto options = BEV_OPT_THREADSAFE | BEV_OPT_UNLOCK_CALLBACKS |
                             BEV_OPT_CLOSE_ON_FREE | BEV_OPT_DEFER_CALLBACKS;
    if (sslStructure) {
        bev.reset(
                bufferevent_openssl_socket_new(thr.eventBase.getLibeventBase(),
                                               sfd,
                                               sslStructure.release(),
                                               BUFFEREVENT_SSL_ACCEPTING,
                                               options));
        bufferevent_setcb(bev.get(),
                          LibeventConnection::ssl_read_callback,
                          LibeventConnection::rw_callback,
                          LibeventConnection::event_callback,
                          this);
    } else {
        bev.reset(bufferevent_socket_new(
                thr.eventBase.getLibeventBase(), sfd, options));
        bufferevent_setcb(bev.get(),
                          LibeventConnection::rw_callback,
                          LibeventConnection::rw_callback,
                          LibeventConnection::event_callback,
                          this);
    }

    bufferevent_enable(bev.get(), EV_READ);
    bufferevent_setwatermark(bev.get(), EV_READ, sizeof(cb::mcbp::Header), 0);

    try {
        max_send_watermark_size =
                2 * cb::net::getSocketOption<int>(sfd, SOL_SOCKET, SO_SNDBUF);
    } catch (const std::exception&) {
        auto& settings = Settings::instance();
        max_send_watermark_size = std::min(
                static_cast<std::size_t>(settings.getMaxSoSndbufSize()),
                settings.getMaxSendQueueSize());
    }
}

LibeventConnection::~LibeventConnection() {
    if (isConnectedToSystemPort()) {
        LOG_INFO_CTX("Delete connection connected to system port",
                     {"conn_id", getId()},
                     {"reason", terminationReason});
    }
    if (isDCP() && getSendQueueSizeImpl() > 0) {
        LOG_INFO_CTX("Releasing DCP connection",
                     {"conn_id", socketDescriptor},
                     {"description", to_json_tcp()});
    }
}

std::vector<unsigned long> LibeventConnection::getOpenSslErrorCodes() {
    std::vector<unsigned long> ret;
    unsigned long err;
    while ((err = bufferevent_get_openssl_error(bev.get())) != 0) {
        ret.push_back(err);
    }

    return ret;
}

std::string LibeventConnection::formatOpenSslErrorCodes(
        const std::vector<unsigned long>& codes) {
    if (codes.empty()) {
        return {};
    }

    auto buffer = thread.getScratchBuffer();
    std::vector<std::string> messages;
    for (const auto& err : codes) {
        ERR_error_string_n(err, buffer.data(), buffer.size());
        messages.emplace_back(fmt::format("{{{}}},", buffer.data()));
    }

    if (messages.size() == 1) {
        // remove trailing ,
        messages.front().pop_back();
        return messages.front();
    }

    std::ranges::reverse(messages);
    std::string ret = "[";
    for (const auto& a : messages) {
        ret.append(a);
    }
    ret.back() = ']';
    return ret;
}

std::string LibeventConnection::getOpenSSLErrors() {
    return formatOpenSslErrorCodes(getOpenSslErrorCodes());
}

void LibeventConnection::rw_callback() {
    if (isTlsEnabled()) {
        const auto ssl_errors = getOpenSSLErrors();
        if (!ssl_errors.empty()) {
            LOG_INFO_CTX("rw_callback OpenSSL errors reported",
                         {"conn_id", getId()},
                         {"error", ssl_errors});
        }
    }

    TRACE_LOCKGUARD_TIMED(thread.mutex,
                          "mutex",
                          "LibeventConnection::rw_callback::threadLock",
                          SlowMutexThreshold);

    // reset the write watermark
    bufferevent_setwatermark(
            bev.get(), EV_WRITE, 0, std::numeric_limits<std::size_t>::max());
    if (executeCommandsCallback()) {
        // Increase the write watermark to provide a callback once we've
        // transferred at least 64k to getting notified *too* often.
        const auto length = getSendQueueSize();
        if (length) {
            const size_t chunk = 64 * 1024;
            if (length > chunk) {
                const auto watermark =
                        std::min(length - chunk, max_send_watermark_size);
                bufferevent_setwatermark(
                        bev.get(),
                        EV_WRITE,
                        watermark,
                        std::numeric_limits<std::size_t>::max());
            }
        }
    } else {
        thread.destroy_connection(*this);
    }
}

void LibeventConnection::rw_callback(bufferevent*, void* ctx) {
    reinterpret_cast<LibeventConnection*>(ctx)->rw_callback();
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

    constexpr short known = BEV_EVENT_READING | BEV_EVENT_WRITING |
                            BEV_EVENT_EOF | BEV_EVENT_ERROR |
                            BEV_EVENT_TIMEOUT | BEV_EVENT_CONNECTED;

    if (event & ~known) {
        err.push_back(cb::to_hex(uint16_t(event)));
    }

    return err;
}

void LibeventConnection::event_callback(bufferevent*, short event, void* ctx) {
    auto& instance = *static_cast<LibeventConnection*>(ctx);
    std::string details;

    auto term = (event & BEV_EVENT_EOF) == BEV_EVENT_EOF;
    if (term) {
        details = "EOF,";
    }
    if ((event & BEV_EVENT_ERROR) == BEV_EVENT_ERROR &&
        EVUTIL_SOCKET_ERROR() == ECONNRESET) {
        details.append("ECONNRESET,");
        term = true;
    }

    std::string ssl_errors;
    if (instance.isTlsEnabled() && !term) {
        auto errors = instance.getOpenSslErrorCodes();
        for (const auto& err : errors) {
            if (ERR_GET_REASON(err) == SSL_R_UNEXPECTED_EOF_WHILE_READING) {
                details.append("TLS unexpected EOF,");
                term = true;
                break;
            }
        }

        if (!term) {
            ssl_errors = instance.formatOpenSslErrorCodes(errors);
        }
    }

    if (term) {
        if (!details.empty() && details.back() == ',') {
            details.pop_back();
        }
        instance.setTerminationReason(
                fmt::format("Client closed connection: {}", details));
    } else if ((event & BEV_EVENT_ERROR) == BEV_EVENT_ERROR) {
        // Note: SSL connections may fail for reasons different than socket
        // error, so we avoid to dump errno:0 (ie, socket operation success).
        const auto sockErr = EVUTIL_SOCKET_ERROR();
        if (sockErr != 0) {
            const auto errStr = evutil_socket_error_to_string(sockErr);
            LOG_WARNING_CTX(
                    "Unrecoverable error encountered, socket_error, "
                    "shutting down connection",
                    {"conn_id", instance.getId()},
                    {"description", instance.getDescription()},
                    {"event", BevEvent2Json(event)},
                    {"error_code", sockErr},
                    {"error", errStr});
            instance.setTerminationReason(
                    fmt::format("socket_error: {}: {}", sockErr, errStr));
        } else if (!ssl_errors.empty()) {
            LOG_WARNING_CTX(
                    "Unrecoverable error encountered, ssl_error, shutting "
                    "down connection",
                    {"conn_id", instance.getId()},
                    {"description", instance.getDescription()},
                    {"event", BevEvent2Json(event)},
                    {"error", ssl_errors});
            instance.setTerminationReason("ssl_error: " + ssl_errors);
        } else {
            LOG_WARNING_CTX(
                    "Unrecoverable error encountered, shutting down "
                    "connection",
                    {"conn_id", instance.getId()},
                    {"description", instance.getDescription()},
                    {"event", BevEvent2Json(event)});
            instance.setTerminationReason("Network error");
        }

        term = true;
    }

    if (term) {
        auto& thread = instance.getThread();
        TRACE_LOCKGUARD_TIMED(thread.mutex,
                              "mutex",
                              "LibeventConnection::event_callback::threadLock",
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
            thread.destroy_connection(instance);
        }
    }
}

void LibeventConnection::ssl_read_callback(bufferevent* bev, void* ctx) {
    auto& instance = *reinterpret_cast<LibeventConnection*>(ctx);

    const auto ssl_errors = instance.getOpenSSLErrors();
    if (!ssl_errors.empty()) {
        LOG_INFO_CTX("ssl_read_callback OpenSSL errors reported",
                     {"conn_id", instance.getId()},
                     {"error", ssl_errors});
    }

    // Let's inspect the certificate before we'll do anything further
    auto* ssl_st = bufferevent_openssl_get_ssl(bev);
    instance.onTlsConnect(ssl_st);

    // update the callback to call the normal read callback
    bufferevent_setcb(bev,
                      LibeventConnection::rw_callback,
                      LibeventConnection::rw_callback,
                      LibeventConnection::event_callback,
                      ctx);

    // and let's call it to make sure we step through the state machinery
    LibeventConnection::rw_callback(bev, ctx);
}

void LibeventConnection::triggerCallback(bool force) {
    if (!force && getSendQueueSize() != 0) {
        // The framework will send a notification once the data is sent
        return;
    }
    constexpr auto opt = BEV_TRIG_IGNORE_WATERMARKS | BEV_TRIG_DEFER_CALLBACKS;
    bufferevent_trigger(bev.get(), EV_READ, opt);
}

void LibeventConnection::copyToOutputStream(std::string_view data) {
    Expects(getThread().eventBase.isInEventBaseThread());
    if (bufferevent_write(bev.get(), data.data(), data.size()) == -1) {
        throw std::bad_alloc();
    }

    updateSendBytes(data.size());
}

void LibeventConnection::copyToOutputStream(gsl::span<std::string_view> data) {
    Expects(getThread().eventBase.isInEventBaseThread());
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
    Expects(getThread().eventBase.isInEventBaseThread());
    if (!buffer || buffer->getPayload().empty()) {
        throw std::logic_error(
                "LibeventConnection::chainDataToOutputStream: buffer must be "
                "set");
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

cb::engine_errc LibeventConnection::sendFile(int fd,
                                             off_t offset,
                                             off_t length) {
    if (evbuffer_add_file(
                bufferevent_get_output(bev.get()), fd, offset, length) == 0) {
        return cb::engine_errc::success;
    }
    throw std::bad_alloc();
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
                fmt::format("LibeventConnection::isPacketAvailable(): Failed "
                            "to reallocate event input buffer: {}",
                            sizeof(cb::mcbp::Header)));
    }

    if (!header->isValid()) {
        audit_invalid_packet(*this, getAvailableBytes());
        throw std::runtime_error(
                fmt::format("LibeventConnection::isPacketAvailable(): Invalid "
                            "packet header detected: ({}) totalRecv:{}",
                            *header,
                            totalRecv));
    }

    const auto framesize = sizeof(*header) + header->getBodylen();

    if (!isAuthenticated() && framesize > MaxUnauthenticatedFrameSize) {
        throw std::runtime_error(fmt::format(
                "LibeventConnection::isPacketAvailable(): The packet size {} "
                "exceeds the max allowed packet size for unauthenticated "
                "connections {}",
                framesize,
                MaxUnauthenticatedFrameSize));
    }

    // Are we receiving an incredible big packet so that we want to
    // disconnect the client?
    if (framesize > Settings::instance().getMaxPacketSize()) {
        throw std::runtime_error(fmt::format(
                "LibeventConnection::isPacketAvailable(): The packet size {} "
                "exceeds the max allowed packet size {}",
                framesize,
                Settings::instance().getMaxPacketSize()));
    }

    if (size >= framesize) {
        // We've got the entire buffer available... make sure it is continuous
        if (evbuffer_pullup(input, framesize) == nullptr) {
            throw std::runtime_error(
                    fmt::format("LibeventConnection::isPacketAvailable(): "
                                "Failed to reallocate event input buffer: {}",
                                framesize));
        }

        bufferevent_setwatermark(
                bev.get(), EV_READ, sizeof(cb::mcbp::Header), 0);
        return true;
    }

    bufferevent_setwatermark(bev.get(), EV_READ, framesize, 0);
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
                "LibeventConnection::getPacket(): packet not available");
    }

    return *reinterpret_cast<const cb::mcbp::Header*>(
            evbuffer_pullup(input, sizeof(cb::mcbp::Header)));
}

void LibeventConnection::nextPacket() {
    const size_t bytes = getPacket().getFrame().size();
    auto* event = bev.get();
    auto* input = bufferevent_get_input(event);
    if (evbuffer_drain(input, bytes) == -1) {
        throw std::runtime_error(
                "LibeventConnection::drainInputPipe(): Failed to drain buffer");
    }
}

cb::const_byte_buffer LibeventConnection::getAvailableBytes() const {
    constexpr size_t max = 1024;
    auto* input = bufferevent_get_input(bev.get());
    auto nb = std::min(evbuffer_get_length(input), max);
    return {evbuffer_pullup(input, nb), nb};
}

void LibeventConnection::disableReadEvent() {
    if ((bufferevent_get_enabled(bev.get()) & EV_READ) == EV_READ) {
        if (bufferevent_disable(bev.get(), EV_READ) == -1) {
            throw std::runtime_error(
                    "LibeventConnection::disableReadEvent: Failed to disable "
                    "read events");
        }
    }
}

void LibeventConnection::enableReadEvent() {
    if ((bufferevent_get_enabled(bev.get()) & EV_READ) == 0) {
        if (bufferevent_enable(bev.get(), EV_READ) == -1) {
            throw std::runtime_error(
                    "LibeventConnection::enableReadEvent: Failed to enable "
                    "read events");
        }
    }
}

size_t LibeventConnection::getSendQueueSize() const {
    return getSendQueueSizeImpl();
}

size_t LibeventConnection::getSendQueueSizeImpl() const {
    return evbuffer_get_length(bufferevent_get_output(bev.get()));
}
