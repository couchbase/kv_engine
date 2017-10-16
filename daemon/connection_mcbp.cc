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
#include "config.h"
#include "connections.h"
#include "mc_time.h"
#include "memcached.h"
#include "runtime.h"
#include "server_event.h"
#include "statemachine_mcbp.h"

#include <mcbp/mcbp.h>
#include <phosphor/phosphor.h>
#include <platform/cb_malloc.h>
#include <platform/checked_snprintf.h>
#include <platform/strerror.h>
#include <platform/timeutils.h>
#include <utilities/protocol2text.h>
#include <cctype>
#include <exception>

bool McbpConnection::unregisterEvent() {
    if (!registered_in_libevent) {
        LOG_WARNING(NULL,
                    "Connection::unregisterEvent: Not registered in libevent - "
                        "ignoring unregister attempt");
        return false;
    }

    cb_assert(socketDescriptor != INVALID_SOCKET);

    if (event_del(&event) == -1) {
        log_system_error(EXTENSION_LOG_WARNING,
                         NULL,
                         "Failed to remove connection to libevent: %s");
        return false;
    }

    registered_in_libevent = false;
    return true;
}

bool McbpConnection::registerEvent() {
    if (registered_in_libevent) {
        LOG_WARNING(NULL, "Connection::registerEvent: Already registered in"
            " libevent - ignoring register attempt");
        return false;
    }

    struct timeval tv;
    struct timeval* tp = nullptr;

    if (settings.getConnectionIdleTime() == 0 || isInternal() || isDCP()) {
        tp = nullptr;
        ev_timeout_enabled = false;
    } else {
        tv.tv_sec = settings.getConnectionIdleTime();
        tv.tv_usec = 0;
        tp = &tv;
        ev_timeout_enabled = true;
        ev_timeout = settings.getConnectionIdleTime();
    }

    ev_insert_time = mc_time_get_current_time();

    if (event_add(&event, tp) == -1) {
        log_system_error(EXTENSION_LOG_WARNING, nullptr,
                         "Failed to add connection to libevent: %s");
        return false;
    }

    registered_in_libevent = true;
    return true;
}

bool McbpConnection::updateEvent(const short new_flags) {
    struct event_base* base = event.ev_base;

    if (ssl.isEnabled() && ssl.isConnected() && (new_flags & EV_READ)) {
        /*
         * If we want more data and we have SSL, that data might be inside
         * SSL's internal buffers rather than inside the socket buffer. In
         * that case signal an EV_READ event without actually polling the
         * socket.
         */
        if (ssl.havePendingInputData()) {
            // signal a call to the handler
            event_active(&event, EV_READ, 0);
            return true;
        }
    }

    if (ev_flags == new_flags) {
        // We do "cache" the current libevent state (using EV_PERSIST) to avoid
        // having to re-register it when it doesn't change (which it mostly don't).
        // In order to avoid having clients to falsely "time out" due to that they
        // never update their libevent state we'll forcibly re-enter it half way
        // into the timeout.

        if (ev_timeout_enabled && (isInternal() || isDCP())) {
            LOG_DEBUG(this,
                      "%u: Forcibly reset the event connection flags to"
                          " disable timeout", getId());
        } else {
            rel_time_t now = mc_time_get_current_time();
            const int reinsert_time = settings.getConnectionIdleTime() / 2;

            if ((ev_insert_time + reinsert_time) > now) {
                return true;
            } else {
                LOG_DEBUG(this,
                          "%u: Forcibly reset the event connection flags to"
                              " avoid premature timeout", getId());
            }
        }
    }

    LOG_DEBUG(NULL, "%u: Updated event to read=%s, write=%s\n",
              getId(), (new_flags & EV_READ ? "yes" : "no"),
              (new_flags & EV_WRITE ? "yes" : "no"));

    if (!unregisterEvent()) {
        LOG_WARNING(this,
                    "Failed to remove connection from event notification "
                    "library. Shutting down connection %s",
                    getDescription().c_str());
        return false;
    }

    if (event_assign(&event, base, socketDescriptor, new_flags, event_handler,
              reinterpret_cast<void*>(this)) == -1) {
        LOG_WARNING(this,
                    "Failed to set up event notification. "
                    "Shutting down connection %s",
                    getDescription().c_str());
        return false;
    }
    ev_flags = new_flags;

    if (!registerEvent()) {
        LOG_WARNING(this,
                    "Failed to add connection to the event notification "
                    "library. Shutting down connection %s",
                    getDescription().c_str());
        return false;
    }

    return true;
}

bool McbpConnection::reapplyEventmask() {
    return updateEvent(ev_flags);
}

bool McbpConnection::initializeEvent() {
    short event_flags = (EV_READ | EV_PERSIST);

    if (event_assign(&event, base, socketDescriptor, event_flags, event_handler,
                     reinterpret_cast<void*>(this)) == -1) {
        return false;
    }
    ev_flags = event_flags;

    return registerEvent();
}

void McbpConnection::shrinkBuffers() {
    // We share the buffers with the thread, so we don't need to worry
    // about the read and write buffer.

    if (msglist.size() > MSG_LIST_HIGHWAT) {
        try {
            msglist.resize(MSG_LIST_INITIAL);
            msglist.shrink_to_fit();
        } catch (const std::bad_alloc&) {
            LOG_WARNING(this,
                        "%u: Failed to shrink msglist down to %"
                            PRIu64
                            " elements.", getId(),
                        MSG_LIST_INITIAL);
        }
    }

    if (iov.size() > IOV_LIST_HIGHWAT) {
        try {
            iov.resize(IOV_LIST_INITIAL);
            iov.shrink_to_fit();
        } catch (const std::bad_alloc&) {
            LOG_WARNING(this,
                        "%u: Failed to shrink iov down to %"
                            PRIu64
                            " elements.", getId(),
                        IOV_LIST_INITIAL);
        }
    }

    // The DynamicBuffer is only occasionally used - free the whole thing
    // if it's still allocated.
    dynamicBuffer.clear();
}

bool McbpConnection::tryAuthFromSslCert(const std::string& userName) {
    username.assign(userName);
    domain = cb::sasl::Domain::Local;

    try {
        auto context =
                cb::rbac::createInitialContext(getUsername(), getDomain());
        setAuthenticated(true);
        setInternal(context.second);
        LOG_INFO(this, "%u: Client %s authenticated as '%s' via X509 certificate",
                 getId(), getPeername().c_str(), getUsername());
        // Connections authenticated by using X.509 certificates should not
        // be able to use SASL to change it's identity.
        saslAuthEnabled = false;
    } catch (const cb::rbac::NoSuchUserException& e) {
        setAuthenticated(false);
        LOG_WARNING(this,
                    "%u: User [%s] is not defined as a user in Couchbase",
                    getId(),
                    e.what());
        return false;
    }
    return true;
}

int McbpConnection::sslPreConnection() {
    int r = ssl.accept();
    if (r == 1) {
        ssl.drainBioSendPipe(socketDescriptor);
        ssl.setConnected();
        auto certResult = ssl.getCertUserName();
        bool disconnect = false;
        switch (certResult.first) {
        case ClientCertUser::Status::Error:
            disconnect = true;
            break;
        case ClientCertUser::Status::NotPresent:
            if (settings.getClientCertAuth() ==
                ClientCertAuth::Mode::Mandatory) {
                disconnect = true;
            } else if (is_default_bucket_enabled()) {
                associate_bucket(this, "default");
            }
            break;
        case ClientCertUser::Status::Success:
            if (!tryAuthFromSslCert(certResult.second)) {
                disconnect = true;
                // Don't print an error message... already logged
                certResult.second.resize(0);
            }
        }
        if (disconnect) {
            set_econnreset();
            if (!certResult.second.empty()) {
                LOG_WARNING(this,
                            "%u: SslPreConnection: disconnection client due to"
                                " error [%s]", getId(),
                            certResult.second.c_str());
            }
            return -1;
        }
    } else {
        if (ssl.getError(r) == SSL_ERROR_WANT_READ) {
            ssl.drainBioSendPipe(socketDescriptor);
            set_ewouldblock();
            return -1;
        } else {
            try {
                std::string errmsg("SSL_accept() returned " +
                                   std::to_string(r) +
                                   " with error " +
                                   std::to_string(ssl.getError(r)));

                std::vector<char> ssl_err(1024);
                ERR_error_string_n(ERR_get_error(), ssl_err.data(),
                                   ssl_err.size());

                LOG_WARNING(this, "%u: %s: %s",
                            getId(), errmsg.c_str(), ssl_err.data());
            } catch (const std::bad_alloc&) {
                // unable to print error message; continue.
            }

            set_econnreset();
            return -1;
        }
    }

    return 0;
}

int McbpConnection::recv(char* dest, size_t nbytes) {
    if (nbytes == 0) {
        throw std::logic_error("McbpConnection::recv: Can't read 0 bytes");
    }

    int res;
    if (ssl.isEnabled()) {
        ssl.drainBioRecvPipe(socketDescriptor);

        if (ssl.hasError()) {
            set_econnreset();
            return -1;
        }

        if (!ssl.isConnected()) {
            res = sslPreConnection();
            if (res == -1) {
                return -1;
            }
        }

        /* The SSL negotiation might be complete at this time */
        if (ssl.isConnected()) {
            res = sslRead(dest, nbytes);
        }
    } else {
        res = (int)::recv(socketDescriptor, dest, nbytes, 0);
        if (res > 0) {
            totalRecv += res;
        }
    }

    return res;
}

int McbpConnection::sendmsg(struct msghdr* m) {
    int res = 0;
    if (ssl.isEnabled()) {
        for (int ii = 0; ii < int(m->msg_iovlen); ++ii) {
            int n = sslWrite(reinterpret_cast<char*>(m->msg_iov[ii].iov_base),
                             m->msg_iov[ii].iov_len);
            if (n > 0) {
                res += n;
            } else {
                return res > 0 ? res : -1;
            }
        }

        /* @todo figure out how to drain the rest of the data if we
         * failed to send all of it...
         */
        ssl.drainBioSendPipe(socketDescriptor);
        return res;
    } else {
        res = int(::sendmsg(socketDescriptor, m, 0));
        if (res > 0) {
            totalSend += res;
        }
    }

    return res;
}

/**
 * Adjust the msghdr by "removing" n bytes of data from it.
 *
 * @param m the msgheader to update
 * @param nbytes
 * @return the number of bytes left in the current iov entry
 */
size_t adjust_msghdr(cb::Pipe& pipe, struct msghdr* m, ssize_t nbytes) {
    auto rbuf = pipe.rdata();

    // We've written some of the data. Remove the completed
    // iovec entries from the list of pending writes.
    while (m->msg_iovlen > 0 && nbytes >= ssize_t(m->msg_iov->iov_len)) {
        if (rbuf.data() == static_cast<const uint8_t*>(m->msg_iov->iov_base)) {
            pipe.consumed(m->msg_iov->iov_len);
            rbuf = pipe.rdata();
        }
        nbytes -= (ssize_t)m->msg_iov->iov_len;
        m->msg_iovlen--;
        m->msg_iov++;
    }

    // Might have written just part of the last iovec entry;
    // adjust it so the next write will do the rest.
    if (nbytes > 0) {
        if (rbuf.data() == static_cast<const uint8_t*>(m->msg_iov->iov_base)) {
            pipe.consumed(nbytes);
        }
        m->msg_iov->iov_base =
                (void*)((unsigned char*)m->msg_iov->iov_base + nbytes);
        m->msg_iov->iov_len -= nbytes;
    }

    return m->msg_iov->iov_len;
}

McbpConnection::TransmitResult McbpConnection::transmit() {
    if (ssl.isEnabled()) {
        // We use OpenSSL to write data into a buffer before we send it
        // over the wire... Lets go ahead and drain that BIO pipe before
        // we may do anything else.
        ssl.drainBioSendPipe(socketDescriptor);
        if (ssl.morePendingOutput()) {
            if (ssl.hasError() || !updateEvent(EV_WRITE | EV_PERSIST)) {
                setState(conn_closing);
                return TransmitResult::HardError;
            }
            return TransmitResult::SoftError;
        }

        // The output buffer is completely drained (well, put in the kernel
        // buffer to send to the client). Go ahead and send more data
    }

    while (msgcurr < msglist.size() &&
           msglist[msgcurr].msg_iovlen == 0) {
        /* Finished writing the current msg; advance to the next. */
        msgcurr++;
    }

    if (msgcurr < msglist.size()) {
        ssize_t res;
        struct msghdr* m = &msglist[msgcurr];

        res = sendmsg(m);
        auto error = GetLastNetworkError();
        if (res > 0) {
            get_thread_stats(this)->bytes_written += res;

            if (adjust_msghdr(*write, m, res) == 0) {
                msgcurr++;
                if (msgcurr == msglist.size()) {
                    // We sent the final chunk of data.. In our SSL connections
                    // we might however have data spooled in the SSL buffers
                    // which needs to be drained before we may consider the
                    // transmission complete (note that our sendmsg tried
                    // to drain the buffers before returning).
                    if (ssl.isEnabled() && ssl.morePendingOutput()) {
                        if (ssl.hasError() ||
                            !updateEvent(EV_WRITE | EV_PERSIST)) {
                            setState(conn_closing);
                            return TransmitResult::HardError;
                        }
                        return TransmitResult::SoftError;
                    }
                    return TransmitResult::Complete;
                }
            }

            return TransmitResult::Incomplete;
        }

        if (res == -1 && is_blocking(error)) {
            if (!updateEvent(EV_WRITE | EV_PERSIST)) {
                setState(conn_closing);
                return TransmitResult::HardError;
            }
            return TransmitResult::SoftError;
        }

        // if res == 0 or res == -1 and error is not EAGAIN or EWOULDBLOCK,
        // we have a real error, on which we close the connection
        if (res == -1) {
            if (is_closed_conn(error)) {
                LOG_NOTICE(nullptr,
                           "%u: Failed to send data; peer closed the connection",
                           getId());
            } else {
                log_socket_error(EXTENSION_LOG_WARNING, this,
                                 "Failed to write, and not due to blocking: %s");
            }
        } else {
            // sendmsg should return the number of bytes written, but we
            // sent 0 bytes. That shouldn't be possible unless we
            // requested to write 0 bytes (otherwise we should have gotten
            // -1 with EWOULDBLOCK)
            // Log the request buffer so that we can look into this
            LOG_WARNING(this, "%d - sendmsg returned 0\n",
                        socketDescriptor);
            for (int ii = 0; ii < int(m->msg_iovlen); ++ii) {
                LOG_WARNING(this, "\t%d - %zu\n",
                            socketDescriptor, m->msg_iov[ii].iov_len);
            }
        }

        setState(conn_closing);
        return TransmitResult::HardError;
    } else {
        return TransmitResult::Complete;
    }
}

/**
 * To protect us from someone flooding a connection with bogus data causing
 * the connection to eat up all available memory, break out and start
 * looking at the data I've got after a number of reallocs...
 */
McbpConnection::TryReadResult McbpConnection::tryReadNetwork() {
    // When we get here we've either got an empty buffer, or we've got
    // a buffer with less than a packet header filled in.
    //
    // Verify that assumption!!!
    if (read->rsize() >= sizeof(cb::mcbp::Request)) {
        // The above don't hold true ;)
        throw std::logic_error(
                "tryReadNetwork: Expected the input buffer to be empty or "
                "contain a partial header");
    }

    // Make sure we can fit the header into the input buffer
    try {
        read->ensureCapacity(sizeof(cb::mcbp::Request) - read->rsize());
    } catch (const std::bad_alloc&) {
        return TryReadResult::MemoryError;
    }

    McbpConnection* c = this;
    const auto res = read->produce([c](cb::byte_buffer buffer) -> ssize_t {
        return c->recv(reinterpret_cast<char*>(buffer.data()), buffer.size());
    });

    if (res > 0) {
        get_thread_stats(this)->bytes_read += res;
        return TryReadResult::DataReceived;
    }

    if (res == 0) {
        LOG_INFO(this,
                 "%u Closing connection as the other side closed the "
                 "connection %s",
                 getId(),
                 getDescription().c_str());
        return TryReadResult::SocketClosed;
    }

    const auto error = GetLastNetworkError();
    if (is_blocking(error)) {
        return TryReadResult::NoDataReceived;
    }

    std::string errormsg = cb_strerror(error);
    LOG_WARNING(this,
                "%u Closing connection (%p) %s due to read "
                "error: %s",
                getId(),
                getCookie(),
                getDescription().c_str(),
                errormsg.c_str());
    return TryReadResult::SocketError;
}

int McbpConnection::sslRead(char* dest, size_t nbytes) {
    int ret = 0;

    while (ret < int(nbytes)) {
        int n;
        ssl.drainBioRecvPipe(socketDescriptor);
        if (ssl.hasError()) {
            set_econnreset();
            return -1;
        }
        n = ssl.read(dest + ret, (int)(nbytes - ret));
        if (n > 0) {
            ret += n;
        } else {
            /* n < 0 and n == 0 require a check of SSL error*/
            int error = ssl.getError(n);

            switch (error) {
            case SSL_ERROR_WANT_READ:
                /*
                 * Drain the buffers and retry if we've got data in
                 * our input buffers
                 */
                if (ssl.moreInputAvailable()) {
                    /* our recv buf has data feed the BIO */
                    ssl.drainBioRecvPipe(socketDescriptor);
                } else if (ret > 0) {
                    /* nothing in our recv buf, return what we have */
                    return ret;
                } else {
                    set_ewouldblock();
                    return -1;
                }
                break;

            case SSL_ERROR_ZERO_RETURN:
                /* The TLS/SSL connection has been closed (cleanly). */
                return 0;

            default:
                /*
                 * @todo I don't know how to gracefully recover from this
                 * let's just shut down the connection
                 */
                LOG_WARNING(this,
                            "%u: ERROR: SSL_read returned -1 with error %d",
                            getId(), error);
                set_econnreset();
                return -1;
            }
        }
    }

    return ret;
}

int McbpConnection::sslWrite(const char* src, size_t nbytes) {
    int ret = 0;

    int chunksize = settings.getBioDrainBufferSize();

    while (ret < int(nbytes)) {
        int n;
        int chunk;

        ssl.drainBioSendPipe(socketDescriptor);
        if (ssl.hasError()) {
            set_econnreset();
            return -1;
        }

        chunk = (int)(nbytes - ret);
        if (chunk > chunksize) {
            chunk = chunksize;
        }

        n = ssl.write(src + ret, chunk);
        if (n > 0) {
            ret += n;
        } else {
            if (ret > 0) {
                /* We've sent some data.. let the caller have them */
                return ret;
            }

            if (n < 0) {
                int error = ssl.getError(n);
                switch (error) {
                case SSL_ERROR_WANT_WRITE:
                    set_ewouldblock();
                    return -1;

                default:
                    /*
                     * @todo I don't know how to gracefully recover from this
                     * let's just shut down the connection
                     */
                    LOG_WARNING(this,
                                "%u: ERROR: SSL_write returned -1 with error %d",
                                getId(), error);
                    set_econnreset();
                    return -1;
                }
            }
        }
    }

    return ret;
}

void McbpConnection::addMsgHdr(bool reset) {
    if (reset) {
        msgcurr = 0;
        msglist.clear();
        iovused = 0;
    }

    msglist.emplace_back();

    struct msghdr& msg = msglist.back();

    /* this wipes msg_iovlen, msg_control, msg_controllen, and
       msg_flags, the last 3 of which aren't defined on solaris: */
    memset(&msg, 0, sizeof(struct msghdr));

    msg.msg_iov = &iov.data()[iovused];

    msgbytes = 0;
    STATS_MAX(this, msgused_high_watermark, msglist.size());
}

void McbpConnection::addIov(const void* buf, size_t len) {
    if (len == 0) {
        return;
    }

    struct msghdr* m = &msglist.back();

    /* We may need to start a new msghdr if this one is full. */
    if (m->msg_iovlen == IOV_MAX) {
        addMsgHdr(false);
    }

    ensureIovSpace();

    // Update 'm' as we may have added an additional msghdr
    m = &msglist.back();

    m->msg_iov[m->msg_iovlen].iov_base = (void*)buf;
    m->msg_iov[m->msg_iovlen].iov_len = len;

    msgbytes += len;
    ++iovused;
    STATS_MAX(this, iovused_high_watermark, getIovUsed());
    m->msg_iovlen++;
}

void McbpConnection::ensureIovSpace() {
    if (iovused < iov.size()) {
        // There is still size in the list
        return;
    }

    // Try to double the size of the array
    iov.resize(iov.size() * 2);

    /* Point all the msghdr structures at the new list. */
    size_t ii;
    int iovnum;
    for (ii = 0, iovnum = 0; ii < msglist.size(); ii++) {
        msglist[ii].msg_iov = &iov[iovnum];
        iovnum += msglist[ii].msg_iovlen;
    }
}

McbpConnection::McbpConnection()
    : Connection(INVALID_SOCKET, nullptr),
      stateMachine(new McbpStateMachine(*this, conn_new_cmd)),
      cookie(*this) {
}

McbpConnection::McbpConnection(SOCKET sfd,
                               event_base* b,
                               const ListeningPort& ifc)
    : Connection(sfd, b, ifc),
      stateMachine(new McbpStateMachine(*this, conn_new_cmd)),
      cookie(*this) {
    if (ifc.protocol != Protocol::Memcached) {
        throw std::logic_error("Incorrect object for MCBP");
    }
    msglist.reserve(MSG_LIST_INITIAL);
    iov.resize(IOV_LIST_INITIAL);

    if (ifc.ssl.enabled) {
        if (!enableSSL(ifc.ssl.cert, ifc.ssl.key)) {
            throw std::runtime_error(std::to_string(getId()) +
                                     " Failed to enable SSL");
        }
    }

    if (!initializeEvent()) {
        throw std::runtime_error("Failed to initialize event structure");
    }
}

McbpConnection::~McbpConnection() {
    releaseReservedItems();
    for (auto* ptr : temp_alloc) {
        cb_free(ptr);
    }
}

void McbpConnection::setState(TaskFunction next_state) {
    stateMachine->setCurrentTask(next_state);
}

void McbpConnection::runStateMachinery() {
    if (isTraceEnabled()) {
        do {
            // @todo we should have a TRACE scope!!
            LOGGER(EXTENSION_LOG_NOTICE, this, "%u - Running task: (%s)",
                   getId(), stateMachine->getCurrentTaskName());
        } while (stateMachine->execute());
    } else {
        do {
            LOG_DEBUG(this, "%u - Running task: (%s)", getId(),
                      stateMachine->getCurrentTaskName());
        } while (stateMachine->execute());
    }
}

/**
 * Get a JSON representation of an event mask
 *
 * @param mask the mask to convert to JSON
 * @return the json representation. Caller is responsible for calling
 *         cJSON_Delete()
 */
static cJSON* event_mask_to_json(const short mask) {
    cJSON* ret = cJSON_CreateObject();
    cJSON* array = cJSON_CreateArray();

    cJSON_AddUintPtrToObject(ret, "raw", mask);
    if (mask & EV_READ) {
        cJSON_AddItemToArray(array, cJSON_CreateString("read"));
    }
    if (mask & EV_WRITE) {
        cJSON_AddItemToArray(array, cJSON_CreateString("write"));
    }
    if (mask & EV_PERSIST) {
        cJSON_AddItemToArray(array, cJSON_CreateString("persist"));
    }
    if (mask & EV_TIMEOUT) {
        cJSON_AddItemToArray(array, cJSON_CreateString("timeout"));
    }

    cJSON_AddItemToObject(ret, "decoded", array);
    return ret;
}

unique_cJSON_ptr McbpConnection::toJSON() const {
    auto ret = Connection::toJSON();
    cJSON* obj = ret.get();
    if (obj != nullptr) {
        cJSON_AddBoolToObject(obj, "sasl_enabled", saslAuthEnabled);
        cJSON_AddBoolToObject(obj, "dcp", isDCP());
        cJSON_AddBoolToObject(obj, "dcp_xattr_aware", isDcpXattrAware());
        cJSON_AddBoolToObject(obj, "dcp_no_value", isDcpNoValue());
        cJSON_AddUintPtrToObject(obj, "opaque", getOpaque());
        cJSON_AddNumberToObject(obj, "max_reqs_per_event",
                                max_reqs_per_event);
        cJSON_AddNumberToObject(obj, "nevents", numEvents);
        cJSON_AddStringToObject(obj, "state", getStateName());

        const char* cmd_name = memcached_opcode_2_text(cmd);
        if (cmd_name == nullptr) {
            cJSON_AddUintPtrToObject(obj, "cmd", cmd);
        } else {
            cJSON_AddStringToObject(obj, "cmd", cmd_name);
        }

        {
            cJSON* o = cJSON_CreateObject();
            cJSON_AddBoolToObject(o, "registered",
                                    isRegisteredInLibevent());
            cJSON_AddItemToObject(o, "ev_flags", event_mask_to_json(ev_flags));
            cJSON_AddItemToObject(o, "which", event_mask_to_json(currentEvent));

            if (ev_timeout_enabled) {
                cJSON* timeout = cJSON_CreateObject();
                cJSON_AddNumberToObject(timeout, "value", ev_timeout);
                cJSON_AddNumberToObject(timeout, "remaining",
                                        ev_insert_time + ev_timeout -
                                        mc_time_get_current_time());
                cJSON_AddItemToObject(o, "timeout", timeout);
            }

            cJSON_AddItemToObject(obj, "libevent", o);
        }

        if (read) {
            cJSON_AddItemToObject(obj, "read", read->to_json().release());
        }

        if (write) {
            cJSON_AddItemToObject(obj, "write", write->to_json().release());
        }

        if (write_and_go != nullptr) {
            cJSON_AddStringToObject(obj, "write_and_go",
                                    stateMachine->getTaskName(write_and_go));

        }

        {
            cJSON* iovobj = cJSON_CreateObject();
            cJSON_AddNumberToObject(iovobj, "size", iov.size());
            cJSON_AddNumberToObject(iovobj, "used", iovused);

            cJSON* array = cJSON_CreateArray();
            for (size_t ii = 0; ii < iovused; ++ii) {
                cJSON* o = cJSON_CreateObject();
                cJSON_AddUintPtrToObject(o, "base", (uintptr_t)iov[ii].iov_base);
                cJSON_AddUintPtrToObject(o, "len", (uintptr_t)iov[ii].iov_len);
                cJSON_AddItemToArray(array, o);
            }
            if (cJSON_GetArraySize(array) > 0) {
                cJSON_AddItemToObject(iovobj, "vector", array);
            } else {
                cJSON_Delete(array);
            }
            cJSON_AddItemToObject(obj, "iov", iovobj);
        }

        {
            cJSON* msg = cJSON_CreateObject();
            cJSON_AddNumberToObject(msg, "size", msglist.capacity());
            cJSON_AddNumberToObject(msg, "used", msglist.size());
            cJSON_AddNumberToObject(msg, "curr", msgcurr);
            cJSON_AddNumberToObject(msg, "bytes", msgbytes);

            cJSON_AddItemToObject(obj, "msglist", msg);
        }
        {
            cJSON* ilist = cJSON_CreateObject();
            cJSON_AddNumberToObject(ilist, "size", reservedItems.size());
            cJSON_AddItemToObject(obj, "itemlist", ilist);
        }
        {
            cJSON* talloc = cJSON_CreateObject();
            cJSON_AddNumberToObject(talloc, "size", temp_alloc.size());
            cJSON_AddItemToObject(obj, "temp_alloc_list", talloc);
        }
        cJSON_AddBoolToObject(obj, "noreply", noreply);
        {
            cJSON* dy_buf = cJSON_CreateObject();
            cJSON_AddUintPtrToObject(dy_buf, "buffer",
                                       (uintptr_t)dynamicBuffer.getRoot());
            cJSON_AddNumberToObject(dy_buf, "size",
                                    (double)dynamicBuffer.getSize());
            cJSON_AddNumberToObject(dy_buf, "offset",
                                    (double)dynamicBuffer.getOffset());

            cJSON_AddItemToObject(obj, "DynamicBuffer", dy_buf);
        }

        /* @todo we should decode the binary header */
        cJSON_AddUintPtrToObject(obj, "cas", cas);
        cJSON_AddNumberToObject(obj, "aiostat", aiostat);
        cJSON_AddBoolToObject(obj, "ewouldblock", ewouldblock);
        cJSON_AddItemToObject(obj, "ssl", ssl.toJSON());
        cJSON_AddNumberToObject(obj, "total_recv", totalRecv);
        cJSON_AddNumberToObject(obj, "total_send", totalSend);
        cJSON_AddStringToObject(
                obj,
                "datatype",
                mcbp::datatype::to_string(datatype.getRaw()).c_str());
    }

    return ret;
}

const Protocol McbpConnection::getProtocol() const {
    return Protocol::Memcached;
}

void McbpConnection::maybeLogSlowCommand(
    const std::chrono::milliseconds& elapsed) const {
    auto opcode = cb::mcbp::ClientOpcode(cmd);
    const auto limit =
            cb::mcbp::sla::getSlowOpThreshold(cb::mcbp::ClientOpcode(cmd));

    if (elapsed > limit) {
        std::chrono::nanoseconds timings(elapsed);
        std::string command;
        try {
            command = to_string(opcode);
        } catch (const std::exception& e) {
            char opcode_s[16];
            checked_snprintf(opcode_s, sizeof(opcode_s), "0x%X", cmd);
            command.assign(opcode_s);
        }

        std::string details;
        if (opcode == cb::mcbp::ClientOpcode::Stat) {
            // Log which stat command took a long time
            details.append(", key: ");
            auto key = getKey();

            if (strncmp(key.buf, "key ",
                        std::min(key.len, static_cast<size_t>(4LU))) == 0) {
                // stat key username1324423e; truncate the actual item key
                details.append("key <TRUNCATED>");
            } else if (key.len > 0) {
                details.append(key.buf, key.len);
            } else {
                // requests all stats
                details.append("<EMPTY>");
            }
        }

        TRACE_INSTANT2("memcached/slow", "Slow cmd", "opcode", cmd, "connection_id", getId());
        LOG_WARNING(NULL,
                    "%u: Slow %s operation on connection: %s (%s)%s"
                    " opaque:0x%08x",
                    getId(),
                    command.c_str(),
                    cb::time2text(timings).c_str(),
                    getDescription().c_str(),
                    details.c_str(),
                    getOpaque());
    }
}

bool McbpConnection::shouldDelete() {
    return getState() == conn_destroyed;
}

bool McbpConnection::processServerEvents() {
    if (server_events.empty()) {
        return false;
    }

    const auto before = getState();

    // We're waiting for the next command to arrive from the client
    // and we've got a server event to process. Let's start
    // processing the server events (which might toggle our state)
    if (server_events.front()->execute(*this)) {
        server_events.pop();
    }

    return getState() != before;
}

void McbpConnection::runEventLoop(short which) {
    conn_loan_buffers(this);
    currentEvent = which;
    numEvents = max_reqs_per_event;

    try {
        runStateMachinery();
    } catch (const std::exception& e) {
        LOG_WARNING(this,
                    "%d: exception occurred in runloop - closing connection: %s",
                    getId(), e.what());
        setState(conn_closing);
        /*
         * In addition to setting the state to conn_closing
         * we need to move execution foward by executing
         * conn_closing() and the subsequent functions
         * i.e. conn_pending_close() or conn_immediate_close()
         */
        try {
            runStateMachinery();
        } catch (const std::exception& e) {
            LOG_WARNING(this,
                    "%d: exception occurred in runloop whilst"
                    "attempting to close connection: %s",
                    getId(), e.what());
        }
    }

    conn_return_buffers(this);
    if (write && !dcp) {
        // Add a simple sanity check. We should only have a write buffer
        // connected when we're in:
        //    * execute (we may have started to create data in the buffer)
        //    * conn_send_data (we're currently sending the data)
        //    * closing (we jumped directly from the above)
        auto state = getState();
        if (!(state == conn_execute || state == conn_send_data ||
              state == conn_closing || state == conn_immediate_close)) {
            // MB-26180: Change this to logging before releasing
            throw std::logic_error(
                    "McbpConnection::runEventLoop: " + std::to_string(getId()) +
                    ": Expected write buffer to be released, "
                    "but it's not. Current state: " +
                    stateMachine->getCurrentTaskName());
        }
    }
}

void McbpConnection::initiateShutdown() {
    setState(conn_closing);
}

void McbpConnection::signalIfIdle(bool logbusy, int workerthread) {
    auto state = getState();
    if (!isEwouldblock() &&
        (state == conn_read_packet_header || state == conn_read_packet_body ||
         state == conn_waiting || state == conn_new_cmd ||
         state == conn_ship_log || state == conn_send_data)) {
        // Raise a 'fake' write event to ensure the connection has an
        // event delivered (for example if its sendQ is full).
        if (!registered_in_libevent) {
            ev_flags = EV_READ | EV_WRITE | EV_PERSIST;
            if (!registerEvent()) {
                LOG_WARNING(this, "McbpConnection::signalIfIdle: Unable to "
                                  "registerEvent.  Setting state to conn_closing");
                setState(conn_closing);
            }
        } else if (!updateEvent(EV_READ | EV_WRITE | EV_PERSIST)) {
            LOG_WARNING(this, "McbpConnection::signalIfIdle: Unable to "
                              "updateEvent.  Setting state to conn_closing");
            setState(conn_closing);
        }
        event_active(&event, EV_WRITE, 0);
    } else if (logbusy) {
        unique_cJSON_ptr json(toJSON());
        auto details = to_string(json, false);
        LOG_NOTICE(
                nullptr, "Worker thread %u: %s", workerthread, details.c_str());
    }
}

void McbpConnection::setPriority(const Connection::Priority& priority) {
    Connection::setPriority(priority);
    switch (priority) {
    case Priority::High:
        max_reqs_per_event = settings.getRequestsPerEventNotification(EventPriority::High);
        return;
    case Priority::Medium:
        max_reqs_per_event = settings.getRequestsPerEventNotification(EventPriority::Medium);
        return;
    case Priority::Low:
        max_reqs_per_event = settings.getRequestsPerEventNotification(EventPriority::Low);
        return;
    }
    throw std::invalid_argument(
        "Unkown priority: " + std::to_string(int(priority)));
}

protocol_binary_response_status McbpConnection::validateCommand(protocol_binary_command command) {
    return Bucket::validateMcbpCommand(this, command, cookie);
}

void McbpConnection::logCommand() const {
    if (settings.getVerbose() == 0) {
        // Info is not enabled.. we don't want to try to format
        // output
        return;
    }

    LOG_INFO(this,
             "%u> %s %s",
             getId(),
             memcached_opcode_2_text(getCmd()),
             getPrintableKey().c_str());
}

void McbpConnection::logResponse(const char* reason) const {
    LOG_INFO(this,
             "%u< %s %s - %s",
             getId(),
             memcached_opcode_2_text(getCmd()),
             getPrintableKey().c_str(),
             reason);
}

void McbpConnection::logResponse(ENGINE_ERROR_CODE code) const {
    if (settings.getVerbose() == 0) {
        // Info is not enabled.. we don't want to try to format
        // output
        return;
    }

    if (code == ENGINE_EWOULDBLOCK || code == ENGINE_WANT_MORE) {
        // These are temporary states
        return;
    }

    logResponse(cb::to_string(cb::engine_errc(code)).c_str());
}

std::string McbpConnection::getPrintableKey() const {
    const auto key = getKey();

    std::string buffer{key.data(), key.size()};
    for (auto& ii : buffer) {
        if (!std::isgraph(ii)) {
            ii = '.';
        }
    }

    return buffer;
}

bool McbpConnection::selectedBucketIsXattrEnabled() const {
    if (bucketEngine) {
        return settings.isXattrEnabled() &&
               bucketEngine->isXattrEnabled(getBucketEngineAsV0());
    }
    return settings.isXattrEnabled();
}
