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
#include "memcached.h"
#include "runtime.h"
#include "statemachine_mcbp.h"
#include "mc_time.h"

#include <exception>
#include <utilities/protocol2text.h>
#include <platform/cb_malloc.h>
#include <platform/checked_snprintf.h>
#include <platform/strerror.h>
#include <platform/timeutils.h>

/* cJSON uses double for all numbers, so only has 53 bits of precision.
 * Therefore encode 64bit integers as string.
 */
static cJSON* json_create_uintptr(uintptr_t value) {
    try {
        char buffer[32];
        checked_snprintf(buffer, sizeof(buffer), "0x%" PRIxPTR, value);
        return cJSON_CreateString(buffer);
    } catch (std::exception& e) {
        return cJSON_CreateString("<Failed to convert pointer>");
    }
}

static void json_add_uintptr_to_object(cJSON* obj, const char* name,
                                       uintptr_t value) {
    cJSON_AddItemToObject(obj, name, json_create_uintptr(value));
}

static void json_add_bool_to_object(cJSON* obj, const char* name, bool value) {
    if (value) {
        cJSON_AddTrueToObject(obj, name);
    } else {
        cJSON_AddFalseToObject(obj, name);
    }
}


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

    if (settings.getConnectionIdleTime() == 0 || isAdmin() || isDCP() || isTAP()) {
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
        char dummy;
        /* SSL_pending() will not work here despite the name */
        int rv = ssl.peek(&dummy, 1);
        if (rv > 0) {
            /* signal a call to the handler */
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

        if (ev_timeout_enabled && (isAdmin() || isDCP() || isTAP())) {
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
    if (read.size > READ_BUFFER_HIGHWAT && read.bytes < DATA_BUFFER_SIZE) {
        if (read.curr != read.buf) {
            /* Pack the buffer */
            memmove(read.buf, read.curr, (size_t)read.bytes);
        }

        void* ptr = cb_realloc(read.buf, DATA_BUFFER_SIZE);
        char* newbuf = reinterpret_cast<char*>(ptr);
        if (newbuf) {
            read.buf = newbuf;
            read.size = DATA_BUFFER_SIZE;
        } else {
            LOG_WARNING(this,
                        "%u: Failed to shrink read buffer down to %"
                            PRIu64
                            " bytes.", getId(), DATA_BUFFER_SIZE);
        }
        read.curr = read.buf;
    }

    if (msglist.size() > MSG_LIST_HIGHWAT) {
        try {
            msglist.resize(MSG_LIST_INITIAL);
            msglist.shrink_to_fit();
        } catch (std::bad_alloc) {
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
        } catch (std::bad_alloc) {
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


int McbpConnection::sslPreConnection() {
    int r = ssl.accept();
    if (r == 1) {
        ssl.drainBioSendPipe(socketDescriptor);
        ssl.setConnected();
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

                LOG_WARNING(this, "%u: ERROR: %s\n%s",
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

            /* We've written some of the data. Remove the completed
               iovec entries from the list of pending writes. */
            while (m->msg_iovlen > 0 && res >= ssize_t(m->msg_iov->iov_len)) {
                res -= (ssize_t)m->msg_iov->iov_len;
                m->msg_iovlen--;
                m->msg_iov++;
            }

            /* Might have written just part of the last iovec entry;
               adjust it so the next write will do the rest. */
            if (res > 0) {
                m->msg_iov->iov_base = (void*)(
                    (unsigned char*)m->msg_iov->iov_base + res);
                m->msg_iov->iov_len -= res;
            }

            if (m->msg_iov->iov_len == 0) {
               msgcurr++;
               if (msgcurr == msglist.size()) {
                   // We sent the final chunk of data.. In our SSL connections
                   // we might however have data spooled in the SSL buffers
                   // which needs to be drained before we may consider the
                   // transmission complete (note that our sendmsg tried
                   // to drain the buffers before returning).
                   if (ssl.isEnabled() && ssl.morePendingOutput()) {
                       if (ssl.hasError() || !updateEvent(EV_WRITE | EV_PERSIST)) {
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
            log_socket_error(EXTENSION_LOG_WARNING, this,
                             "Failed to write, and not due to blocking: %s");
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
    TryReadResult gotdata = TryReadResult::NoDataReceived;
    int res;
    int num_allocs = 0;

    if (read.curr != read.buf) {
        if (read.bytes != 0) { /* otherwise there's nothing to copy */
            memmove(read.buf, read.curr, read.bytes);
        }
        read.curr = read.buf;
    }

    while (1) {
        int avail;
        if (read.bytes >= read.size) {
            if (num_allocs == 4) {
                return gotdata;
            }
            ++num_allocs;
            char* new_rbuf = reinterpret_cast<char*>(cb_realloc(read.buf,
                                                             read.size * 2));
            if (!new_rbuf) {
                LOG_WARNING(this, "Couldn't realloc input buffer");
                read.bytes = 0; /* ignore what we read */
                setState(conn_closing);
                return TryReadResult::MemoryError;
            }
            read.curr = read.buf = new_rbuf;
            read.size *= 2;
        }

        avail = read.size - read.bytes;
        res = recv(read.buf + read.bytes, avail);
        if (res > 0) {
            get_thread_stats(this)->bytes_read += res;
            gotdata = TryReadResult::DataReceived;
            read.bytes += res;
            if (res == avail) {
                continue;
            } else {
                break;
            }
        }
        if (res == 0) {
            return isPipeConnection() ?
                   TryReadResult::NoDataReceived : TryReadResult::SocketError;
        }
        if (res == -1) {
            auto error = GetLastNetworkError();

            if (is_blocking(error)) {
                break;
            }

            std::string errormsg = cb_strerror();
            LOG_WARNING(this, "%u Closing connection %s due to read error: %s",
                        getId(), getDescription().c_str(), errormsg.c_str());
            return TryReadResult::SocketError;
        }
    }
    return gotdata;
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

SslContext::~SslContext() {
    if (enabled) {
        disable();
    }
}

bool SslContext::enable(const std::string& cert, const std::string& pkey) {
    ctx = SSL_CTX_new(SSLv23_server_method());
    set_ssl_ctx_protocol_mask(ctx);

    /* @todo don't read files, but use in-memory-copies */
    if (!SSL_CTX_use_certificate_chain_file(ctx, cert.c_str()) ||
        !SSL_CTX_use_PrivateKey_file(ctx, pkey.c_str(), SSL_FILETYPE_PEM)) {
        LOG_WARNING(nullptr, "Failed to use SSL cert %s and pkey %s",
                    cert.c_str(), pkey.c_str());
        return false;
    }

    set_ssl_ctx_cipher_list(ctx);

    enabled = true;
    error = false;
    client = NULL;

    try {
        in.buffer.resize(settings.getBioDrainBufferSize());
        out.buffer.resize(settings.getBioDrainBufferSize());
    } catch (std::bad_alloc) {
        return false;
    }

    BIO_new_bio_pair(&application, in.buffer.size(),
                     &network, out.buffer.size());

    client = SSL_new(ctx);
    SSL_set_bio(client, application, application);

    return true;
}

void SslContext::disable() {
    if (network != nullptr) {
        BIO_free_all(network);
    }
    if (client != nullptr) {
        SSL_free(client);
    }
    error = false;
    if (ctx != nullptr) {
        SSL_CTX_free(ctx);
    }
    enabled = false;
}

void SslContext::drainBioRecvPipe(SOCKET sfd) {
    int n;
    bool stop = false;

    do {
        if (in.current < in.total) {
            n = BIO_write(network, in.buffer.data() + in.current,
                          int(in.total - in.current));
            if (n > 0) {
                in.current += n;
                if (in.current == in.total) {
                    in.current = in.total = 0;
                }
            } else {
                /* Our input BIO is full, no need to grab more data from
                 * the network at this time..
                 */
                return;
            }
        }

        if (in.total < in.buffer.size()) {
            n = recv(sfd, in.buffer.data() + in.total,
                     in.buffer.size() - in.total, 0);
            if (n > 0) {
                in.total += n;
                totalRecv += n;
            } else {
                stop = true;
                if (n == 0) {
                    error = true; /* read end shutdown */
                } else {
                    if (!is_blocking(GetLastNetworkError())) {
                        error = true;
                    }
                }
            }
        }
    } while (!stop);
}

void SslContext::drainBioSendPipe(SOCKET sfd) {
    int n;
    bool stop = false;

    do {
        if (out.current < out.total) {
            n = send(sfd, out.buffer.data() + out.current,
                     out.total - out.current, 0);
            if (n > 0) {
                out.current += n;
                if (out.current == out.total) {
                    out.current = out.total = 0;
                }
                totalSend += n;
            } else {
                if (n == -1) {
                    if (!is_blocking(GetLastNetworkError())) {
                        log_socket_error(EXTENSION_LOG_WARNING, this,
                                         "Failed to write, and not due to blocking: %s");
                        error = true;
                    }
                }
                return;
            }
        }

        if (out.total == 0) {
            n = BIO_read(network, out.buffer.data(), int(out.buffer.size()));
            if (n > 0) {
                out.total = n;
            } else {
                stop = true;
            }
        }
    } while (!stop);
}

void SslContext::dumpCipherList(uint32_t id) const {
    LOG_DEBUG(NULL, "%u: Using SSL ciphers:", id);
    int ii = 0;
    const char* cipher;
    while ((cipher = SSL_get_cipher_list(client, ii++)) != NULL) {
        LOG_DEBUG(NULL, "%u    %s", id, cipher);
    }
}

cJSON* SslContext::toJSON() const {
    cJSON* obj = cJSON_CreateObject();
    json_add_bool_to_object(obj, "enabled", enabled);
    if (enabled) {
        json_add_bool_to_object(obj, "connected", connected);
        json_add_bool_to_object(obj, "error", error);
        cJSON_AddNumberToObject(obj, "total_recv", totalRecv);
        cJSON_AddNumberToObject(obj, "total_send", totalSend);
        cJSON_AddNumberToObject(obj, "input_buff_total", in.total);
        cJSON_AddNumberToObject(obj, "input_buff_current", in.current);
        cJSON_AddNumberToObject(obj, "output_buff_total", out.total);
        cJSON_AddNumberToObject(obj, "output_buff_current", out.current);
    }

    return obj;
}

bool McbpConnection::addMsgHdr(bool reset) {
    if (reset) {
        msgcurr = 0;
        msglist.clear();
        iovused = 0;
    }

    try {
        msglist.emplace_back();
    } catch (std::bad_alloc&) {
        return false;
    }

    struct msghdr& msg = msglist.back();

    /* this wipes msg_iovlen, msg_control, msg_controllen, and
       msg_flags, the last 3 of which aren't defined on solaris: */
    memset(&msg, 0, sizeof(struct msghdr));

    msg.msg_iov = &iov.data()[iovused];

    msgbytes = 0;
    STATS_MAX(this, msgused_high_watermark, msglist.size());

    return true;
}

bool McbpConnection::addIov(const void* buf, size_t len) {

    size_t leftover;
    bool limit_to_mtu;

    if (len == 0) {
        return true;
    }

    do {
        struct msghdr* m = &msglist.back();

        /*
         * Limit the first payloads of TCP replies, to
         * UDP_MAX_PAYLOAD_SIZE bytes.
         */
        limit_to_mtu = (1 == msglist.size());

        /* We may need to start a new msghdr if this one is full. */
        if (m->msg_iovlen == IOV_MAX ||
            (limit_to_mtu && msgbytes >= UDP_MAX_PAYLOAD_SIZE)) {
            if (!addMsgHdr(false)) {
                return false;
            }
        }

        if (!ensureIovSpace()) {
            return false;
        }

        /* If the fragment is too big to fit in the datagram, split it up */
        if (limit_to_mtu && len + msgbytes > UDP_MAX_PAYLOAD_SIZE) {
            leftover = len + msgbytes - UDP_MAX_PAYLOAD_SIZE;
            len -= leftover;
        } else {
            leftover = 0;
        }

        // Update 'm' as we may have added an additional msghdr
        m = &msglist.back();

        m->msg_iov[m->msg_iovlen].iov_base = (void*)buf;
        m->msg_iov[m->msg_iovlen].iov_len = len;

        msgbytes += len;
        ++iovused;
        STATS_MAX(this, iovused_high_watermark, getIovUsed());
        m->msg_iovlen++;

        buf = ((char*)buf) + len;
        len = leftover;
    } while (leftover > 0);

    return true;
}

bool McbpConnection::ensureIovSpace() {
    if (iovused < iov.size()) {
        // There is still size in the list
        return true;
    }

    // Try to double the size of the array
    try {
        iov.resize(iov.size() * 2);
    } catch (std::bad_alloc) {
        return false;
    }

    /* Point all the msghdr structures at the new list. */
    size_t ii;
    int iovnum;
    for (ii = 0, iovnum = 0; ii < msglist.size(); ii++) {
        msglist[ii].msg_iov = &iov[iovnum];
        iovnum += msglist[ii].msg_iovlen;
    }

    return true;
}

McbpConnection::McbpConnection(SOCKET sfd, event_base *b)
    : Connection(sfd, b),
      stateMachine(new McbpStateMachine(conn_immediate_close)),
      tap_iterator(nullptr),
      dcp(false),
      max_reqs_per_event(settings.getRequestsPerEventNotification(EventPriority::Default)),
      numEvents(0),
      cmd(PROTOCOL_BINARY_CMD_INVALID),
      registered_in_libevent(false),
      ev_flags(0),
      currentEvent(0),
      ev_timeout_enabled(false),
      write_and_go(conn_new_cmd),
      ritem(nullptr),
      rlbytes(0),
      item(nullptr),
      iov(IOV_LIST_INITIAL),
      iovused(0),
      msglist(),
      msgcurr(0),
      msgbytes(0),
      noreply(false),
      supports_datatype(false),
      supports_mutation_extras(false),
      start(0),
      cas(0),
      aiostat(ENGINE_SUCCESS),
      ewouldblock(false),
      commandContext(nullptr),
      totalRecv(0),
      totalSend(0),
      cookie(this) {
    memset(&binary_header, 0, sizeof(binary_header));
    memset(&event, 0, sizeof(event));
    memset(&read, 0, sizeof(read));
    memset(&write, 0, sizeof(write));
    msglist.reserve(MSG_LIST_INITIAL);

    if (!initializeEvent()) {
        throw std::runtime_error("Failed to initialize event structure");
    }
}

McbpConnection::McbpConnection(SOCKET sfd,
                               event_base* b,
                               const struct listening_port& ifc)
    : Connection(sfd, b, ifc),
      stateMachine(new McbpStateMachine(conn_new_cmd)),
      tap_iterator(nullptr),
      dcp(false),
      max_reqs_per_event(settings.getRequestsPerEventNotification(EventPriority::Default)),
      numEvents(0),
      cmd(PROTOCOL_BINARY_CMD_INVALID),
      registered_in_libevent(false),
      ev_flags(0),
      currentEvent(0),
      ev_timeout_enabled(false),
      write_and_go(conn_new_cmd),
      ritem(nullptr),
      rlbytes(0),
      item(nullptr),
      iov(IOV_LIST_INITIAL),
      iovused(0),
      msglist(),
      msgcurr(0),
      msgbytes(0),
      noreply(false),
      supports_datatype(false),
      supports_mutation_extras(false),
      start(0),
      cas(0),
      aiostat(ENGINE_SUCCESS),
      ewouldblock(false),
      commandContext(nullptr),
      totalRecv(0),
      totalSend(0),
      cookie(this) {

    if (ifc.protocol != Protocol::Memcached) {
        throw std::logic_error("Incorrect object for MCBP");
    }
    memset(&binary_header, 0, sizeof(binary_header));
    memset(&event, 0, sizeof(event));
    memset(&read, 0, sizeof(read));
    memset(&write, 0, sizeof(write));
    msglist.reserve(MSG_LIST_INITIAL);

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
    cb_free(read.buf);
    cb_free(write.buf);

    releaseReservedItems();
    for (auto* ptr : temp_alloc) {
        cb_free(ptr);
    }
}

void McbpConnection::setState(TaskFunction next_state) {
    stateMachine->setCurrentTask(*this, next_state);
}

void McbpConnection::runStateMachinery() {
    if (isTraceEnabled()) {
        do {
            // @todo we should have a TRACE scope!!
            LOGGER(EXTENSION_LOG_NOTICE, this, "%u - Running task: (%s)",
                   getId(), stateMachine->getCurrentTaskName());
        } while (stateMachine->execute(*this));
    } else {
        do {
            LOG_DEBUG(this, "%u - Running task: (%s)", getId(),
                      stateMachine->getCurrentTaskName());
        } while (stateMachine->execute(*this));
    }
}

/**
 * Convert a JSON representation of a net_buf
 *
 * @param buffer the buffer to convert
 * @return the json representation of the buffer (caller is responsible
 *         for calling cJSON_Delete()
 */
static cJSON* to_json(const struct net_buf &buffer) {
    cJSON* json = cJSON_CreateObject();
    json_add_uintptr_to_object(json, "buf", (uintptr_t)buffer.buf);
    json_add_uintptr_to_object(json, "curr", (uintptr_t)buffer.curr);
    cJSON_AddNumberToObject(json, "size", buffer.size);
    cJSON_AddNumberToObject(json, "bytes", buffer.bytes);
    return json;
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

    json_add_uintptr_to_object(ret, "raw", mask);
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

cJSON* McbpConnection::toJSON() const {
    cJSON* obj = Connection::toJSON();
    if (obj != nullptr) {
        json_add_bool_to_object(obj, "tap", isTAP());
        json_add_bool_to_object(obj, "dcp", isDCP());
        json_add_uintptr_to_object(obj, "opaque", getOpaque());
        cJSON_AddNumberToObject(obj, "max_reqs_per_event",
                                max_reqs_per_event);
        cJSON_AddNumberToObject(obj, "nevents", numEvents);
        cJSON_AddStringToObject(obj, "state", getStateName());

        const char* cmd_name = memcached_opcode_2_text(cmd);
        if (cmd_name == nullptr) {
            json_add_uintptr_to_object(obj, "cmd", cmd);
        } else {
            cJSON_AddStringToObject(obj, "cmd", cmd_name);
        }

        {
            cJSON* o = cJSON_CreateObject();
            json_add_bool_to_object(o, "registered",
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

        cJSON_AddItemToObject(obj, "read", to_json(read));
        cJSON_AddItemToObject(obj, "write", to_json(write));

        if (write_and_go != nullptr) {
            cJSON_AddStringToObject(obj, "write_and_go",
                                    stateMachine->getTaskName(write_and_go));

        }
        json_add_uintptr_to_object(obj, "ritem", (uintptr_t)ritem);
        cJSON_AddNumberToObject(obj, "rlbytes", rlbytes);
        json_add_uintptr_to_object(obj, "item", (uintptr_t)item);

        {
            cJSON* iovobj = cJSON_CreateObject();
            cJSON_AddNumberToObject(iovobj, "size", iov.size());
            cJSON_AddNumberToObject(iovobj, "used", iovused);

            cJSON* array = cJSON_CreateArray();
            for (size_t ii = 0; ii < iovused; ++ii) {
                cJSON* o = cJSON_CreateObject();
                json_add_uintptr_to_object(o, "base", (uintptr_t)iov[ii].iov_base);
                json_add_uintptr_to_object(o, "len", (uintptr_t)iov[ii].iov_len);
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
        json_add_bool_to_object(obj, "noreply", noreply);
        {
            cJSON* dy_buf = cJSON_CreateObject();
            json_add_uintptr_to_object(dy_buf, "buffer",
                                       (uintptr_t)dynamicBuffer.getRoot());
            cJSON_AddNumberToObject(dy_buf, "size",
                                    (double)dynamicBuffer.getSize());
            cJSON_AddNumberToObject(dy_buf, "offset",
                                    (double)dynamicBuffer.getOffset());

            cJSON_AddItemToObject(obj, "DynamicBuffer", dy_buf);
        }

        /* @todo we should decode the binary header */
        json_add_uintptr_to_object(obj, "cas", cas);
        cJSON_AddNumberToObject(obj, "aiostat", aiostat);
        json_add_bool_to_object(obj, "ewouldblock", ewouldblock);
        cJSON_AddItemToObject(obj, "ssl", ssl.toJSON());
        cJSON_AddNumberToObject(obj, "total_recv", totalRecv);
        cJSON_AddNumberToObject(obj, "total_send", totalSend);
    }

    return obj;
}

const Protocol McbpConnection::getProtocol() const {
    return Protocol::Memcached;
}

void McbpConnection::maybeLogSlowCommand(
    const std::chrono::milliseconds& elapsed) const {

    std::chrono::milliseconds limit(500);

    switch (cmd) {
    case PROTOCOL_BINARY_CMD_COMPACT_DB:
        // We have no idea how slow this is, but just set a 30 minute
        // threshold for now to avoid it popping up in the logs all of
        // the times
        limit = std::chrono::milliseconds(1800 * 1000);
        break;
    case PROTOCOL_BINARY_CMD_SEQNO_PERSISTENCE:
        // This can also be slow (given it requires waiting for disk). Set
        // to 30s for now.
        limit = std::chrono::seconds(30);
        break;
    case PROTOCOL_BINARY_CMD_DELETE_BUCKET:
        // All clients needs to be disconnected, and all internal tasks
        // in the underlying bucket needs to be stopped. This may be
        // slow, so lets bump the limit to 10 sec.
        limit = std::chrono::seconds(10);
        break;
    }

    if (elapsed > limit) {
        hrtime_t timings((hrtime_t)elapsed.count());
        timings *= 1000 * 1000; // convert from ms to ns

        const char* opcode = memcached_opcode_2_text(cmd);
        char opcode_s[16];
        std::string details;

        if (opcode == nullptr) {
            checked_snprintf(opcode_s, sizeof(opcode_s), "0x%X", cmd);
            opcode = opcode_s;
        }

        if (cmd == PROTOCOL_BINARY_CMD_STAT) {
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

        LOG_WARNING(NULL, "%u: Slow %s operation on connection: %s (%s)%s",
                    getId(), opcode,
                    Couchbase::hrtime2text(timings).c_str(),
                    getDescription().c_str(), details.c_str());
    }
}

bool McbpConnection::includeErrorStringInResponseBody(
    protocol_binary_response_status err) const {
    // Maintain backwards compatibility - return true for older commands which
    // have for some time returned included the error string. For newer
    // commands where there is no backwards compat issue, return false.

    // Note: skipping the error string is currently "opt-in", as I'm not
    // sure which commands other than these very new ones we can safely skip
    // the string and not cause client incompatibilities.
    switch (binary_header.request.opcode) {
    case PROTOCOL_BINARY_CMD_SUBDOC_GET:
    case PROTOCOL_BINARY_CMD_SUBDOC_EXISTS:
    case PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD:
    case PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT:
    case PROTOCOL_BINARY_CMD_SUBDOC_DELETE:
    case PROTOCOL_BINARY_CMD_SUBDOC_REPLACE:
    case PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST:
    case PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST:
    case PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT:
    case PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE:
    case PROTOCOL_BINARY_CMD_SUBDOC_COUNTER:
    case PROTOCOL_BINARY_CMD_SUBDOC_MULTI_LOOKUP:
    case PROTOCOL_BINARY_CMD_SUBDOC_MULTI_MUTATION:
        return false;

    default:
        // Some legacy commands don't return the error string for specifie
        // error codes:
        switch (err) {
        case PROTOCOL_BINARY_RESPONSE_SUCCESS:
        case PROTOCOL_BINARY_RESPONSE_NOT_INITIALIZED:
        case PROTOCOL_BINARY_RESPONSE_AUTH_STALE:
        case PROTOCOL_BINARY_RESPONSE_NO_BUCKET:
            return false;

        default:
            return true;
        }
    }
}

PipeConnection::PipeConnection(SOCKET sfd, event_base* b)
    : McbpConnection(sfd, b) {
    peername = "pipe";
    sockname = "pipe";
}

PipeConnection::~PipeConnection() {
    if (settings.isExitOnConnectionClose()) {
        exit(0);
    }
}

bool McbpConnection::shouldDelete() {
    return getState() == conn_destroyed;
}

void McbpConnection::runEventLoop(short which) {
    conn_loan_buffers(this);
    currentEvent = which;
    numEvents = max_reqs_per_event;
    try {
        runStateMachinery();
    } catch (std::exception& e) {
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
        } catch (std::exception& e) {
            LOG_WARNING(this,
                    "%d: exception occurred in runloop whilst"
                    "attempting to close connection: %s",
                    getId(), e.what());
        }
    }

    conn_return_buffers(this);
}

void McbpConnection::initateShutdown() {
    setState(conn_closing);
}

void McbpConnection::signalIfIdle(bool logbusy, int workerthread) {
    auto state = getState();
    if (!isEwouldblock() && (state == conn_read ||
                             state == conn_nread ||
                             state == conn_waiting ||
                             state == conn_new_cmd ||
                             state == conn_ship_log)) {

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
        auto* js = toJSON();
        char* details = cJSON_PrintUnformatted(js);

        LOG_NOTICE(NULL, "Worker thread %u: %s", workerthread, details);
        cJSON_Free(details);
        cJSON_Delete(js);
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

int PipeConnection::sendmsg(struct msghdr* m) {
    int res = 0;
    // Windows and POSIX safe, manually write the scatter/gather
    for (size_t ii = 0; ii < size_t(m->msg_iovlen); ii++) {
        auto nw = ::write(fileno(stdout),
                          m->msg_iov[ii].iov_base,
                          m->msg_iov[ii].iov_len);
        if (nw == -1) {
            if (res == 0) {
                return -1;
            }
            break;
        } else {
            res += nw;
        }
    }

    return res;
}

int PipeConnection::recv(char* dest, size_t nbytes) {
    return (int)::read(socketDescriptor, dest, nbytes);
}
