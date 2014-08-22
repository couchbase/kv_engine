/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2012 Couchbase, Inc
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

#include <string.h>

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <map>

#include "couch-kvstore/couch-notifier.h"
#include "ep_engine.h"
#include "locks.h"
#define STATWRITER_NAMESPACE couch_notifier
#include "statwriter.h"
#undef STATWRITER_NAMESPACE


#ifdef _MSC_VER
static inline int poll(struct pollfd fds[], int nfds, int timeout) {
    return WSAPoll(fds, nfds, timeout);
}

static inline std::string getErrorString(DWORD err) {
    std::string ret;
    char* win_msg = NULL;
    FormatMessageA(FORMAT_MESSAGE_ALLOCATE_BUFFER |
                   FORMAT_MESSAGE_FROM_SYSTEM |
                   FORMAT_MESSAGE_IGNORE_INSERTS,
                   NULL, err,
                   MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
                   (LPTSTR)&win_msg,
                   0, NULL);
    ret.assign(win_msg);
    LocalFree(win_msg);
    return ret;
}
#endif

/*
 * The various response handlers
 */
class BinaryPacketHandler {
public:
    BinaryPacketHandler(uint32_t sno) :
        seqno(sno), start(0)
    {
        start = gethrtime();
    }

    virtual ~BinaryPacketHandler() { /* EMPTY */ }

    virtual void request(protocol_binary_request_header *) {
        unsupported();
    }
    virtual void response(protocol_binary_response_header *) {
        unsupported();
    }

    virtual void implicitResponse() {
        LOG(EXTENSION_LOG_WARNING, "Unsupported implicitResponse");
    }

    virtual void connectionReset() {
        LOG(EXTENSION_LOG_WARNING, "Unsupported connectionReset");
    }

    uint32_t seqno;

    hrtime_t getDelta() {
        return (gethrtime() - start) / 1000;
    }

private:
    void unsupported() {
        LOG(EXTENSION_LOG_WARNING, "Unsupported packet received");
    }

protected:
    hrtime_t start;
};

class DelVBucketResponseHandler: public BinaryPacketHandler {
public:
    DelVBucketResponseHandler(uint32_t sno, Callback<bool> &cb) :
        BinaryPacketHandler(sno), callback(cb) {
    }

    virtual void response(protocol_binary_response_header *res) {
        uint16_t rcode = ntohs(res->response.status);
        bool success = true;

        if (rcode != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
            success = false;
        }

        callback.callback(success);
    }

    virtual void connectionReset() {
        bool value = false;
        callback.callback(value);
    }

private:
    Callback<bool> &callback;
};

class FlushResponseHandler: public BinaryPacketHandler {
public:
    FlushResponseHandler(uint32_t sno, Callback<bool> &cb) :
        BinaryPacketHandler(sno), callback(cb) {
    }

    virtual void response(protocol_binary_response_header *res) {
        uint16_t rcode = ntohs(res->response.status);
        bool success = true;

        if (rcode != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
            success = false;
        }

        callback.callback(success);
    }

    virtual void connectionReset() {
        bool value = false;
        callback.callback(value);
    }

private:
    Callback<bool> &callback;
};

class SelectBucketResponseHandler: public BinaryPacketHandler {
public:
    SelectBucketResponseHandler(uint32_t sno) :
        BinaryPacketHandler(sno) {
    }

    virtual void response(protocol_binary_response_header *res) {
        uint16_t rcode = ntohs(res->response.status);

        if (rcode != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
            // This should _NEVER_ happen!!!!
            abort();
        }
    }

    virtual void connectionReset() {
        // ignore
    }
};

class NotifyVbucketUpdateResponseHandler: public BinaryPacketHandler {
public:
    NotifyVbucketUpdateResponseHandler(uint32_t sno, Callback<uint16_t> &cb) :
        BinaryPacketHandler(sno), callback(cb) {
    }

    virtual void response(protocol_binary_response_header *res) {
        uint16_t rcode = ntohs(res->response.status);
        callback.callback(rcode);
    }

    virtual void connectionReset() {
        uint16_t rcode = PROTOCOL_BINARY_RESPONSE_ETMPFAIL;
        callback.callback(rcode);
    }

private:
    Callback<uint16_t> &callback;
};

Mutex CouchNotifier::initMutex;
std::map<std::string, CouchNotifier *> CouchNotifier::instances;
uint16_t CouchNotifier::refCount = 0;
/*
 * Implementation of the member functions in the CouchNotifier class
 */
CouchNotifier::CouchNotifier(EPStats &st, Configuration &config) :
    sock(INVALID_SOCKET), stats(st), bucketName(config.getCouchBucket()),
    responseTimeOut(config.getCouchResponseTimeout()),
    reconnectSleepTime(config.getCouchReconnectSleeptime()),
    port(config.getCouchPort()), host(config.getCouchHost()),
    allowDataLoss(config.isAllowDataLossDuringShutdown()),
    configurationError(true), seqno(0),
    currentCommand(0xff), lastSentCommand(0xff), lastReceivedCommand(0xff),
    connected(false), inSelectBucket(false)
{
    memset(&sendMsg, 0, sizeof(sendMsg));
    sendMsg.msg_iov = sendIov;

    // Select the bucket (will be sent immediately when we connect)
    selectBucket();
}

void CouchNotifier::resetConnection() {
    LOG(EXTENSION_LOG_WARNING,
        "Resetting connection to mccouch, lastReceivedCommand = %s"
        " lastSentCommand = %s currentCommand =%s\n",
        cmd2str(lastReceivedCommand), cmd2str(lastSentCommand),
        cmd2str(currentCommand));
    lastReceivedCommand = 0xff;
    lastSentCommand = 0xff;
    currentCommand = 0xff;

    EVUTIL_CLOSESOCKET(sock);
    sock = INVALID_SOCKET;
    connected = false;
    input.avail = 0;

    std::list<BinaryPacketHandler*>::iterator iter;
    for (iter = responseHandler.begin(); iter != responseHandler.end(); ++iter) {
        (*iter)->connectionReset();
        delete *iter;
    }

    responseHandler.clear();

    // insert the select vbucket command, if necessary
    if (!inSelectBucket) {
        selectBucket();
    }
}

void CouchNotifier::handleResponse(protocol_binary_response_header *res) {
    std::list<BinaryPacketHandler*>::iterator iter;
    for (iter = responseHandler.begin(); iter != responseHandler.end()
            && (*iter)->seqno < res->response.opaque; ++iter) {
        delete *iter;
    }

    if (iter == responseHandler.end() || (*iter)->seqno != res->response.opaque) {
        if (iter != responseHandler.begin()) {
            responseHandler.erase(responseHandler.begin(), iter);
        }
        return;
    }

    int cmdId = commandId(res->response.opcode);
    if (ntohs(res->response.status) == PROTOCOL_BINARY_RESPONSE_SUCCESS){
        commandStats[cmdId].numSuccess++;
    } else {
        LOG(EXTENSION_LOG_WARNING,
            "Received error[%X] from mccouch for %s\n",
            ntohs(res->response.status), cmd2str(res->response.opcode));
        commandStats[cmdId].numError++;
    }
    (*iter)->response(res);
    if (res->response.opcode == PROTOCOL_BINARY_CMD_STAT
            && res->response.bodylen != 0) {
        // a stats command is terminated with an empty packet..
    } else {
        delete *iter;
        ++iter;
    }

    responseHandler.erase(responseHandler.begin(), iter);
}

bool CouchNotifier::connect() {
    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_flags = AI_PASSIVE;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_family = AF_UNSPEC;

    const char *hptr = host.c_str();
    if (host.empty()) {
        hptr = NULL;
    }

    std::stringstream ss;
    ss << port;

    struct addrinfo *ainfo = NULL;
    int error = getaddrinfo(hptr, ss.str().c_str(), &hints, &ainfo);
    if (error != 0) {
        std::stringstream msg;
        msg << "Failed to look up address information for: \"";
        if (hptr != NULL) {
            msg << hptr << ":";
        }
        msg << port << "\"";
        LOG(EXTENSION_LOG_WARNING, "%s", msg.str().c_str());
        configurationError = true;
        ainfo = NULL;
        return false;
    }

    struct addrinfo *ai = ainfo;
    while (ai != NULL) {
        sock = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);

        if (sock != -1) {
            if (::connect(sock, ai->ai_addr, ai->ai_addrlen) != -1 &&
                evutil_make_socket_nonblocking(sock) == 0) {
                break;
            }
            EVUTIL_CLOSESOCKET(sock);
            sock = INVALID_SOCKET;
        }
        ai = ai->ai_next;
    }

    freeaddrinfo(ainfo);
    if (sock == INVALID_SOCKET) {
        std::stringstream msg;
        msg << "Failed to connect to: \"";
        if (hptr != NULL) {
            msg << hptr << ":";
        }
        msg << port << "\"";
        LOG(EXTENSION_LOG_WARNING, "%s\n", msg.str().c_str());
        configurationError = true;
        return false;
    }

    // @fixme
    connected = true;

    return true;
}

void CouchNotifier::ensureConnection()
{
    if (!connected) {
        // I need to connect!!!
        std::stringstream rv;
        rv << "Trying to connect to mccouch: \""
           << host.c_str() << ":"
           << port << "\"";

        LOG(EXTENSION_LOG_WARNING, "%s\n", rv.str().c_str());
        while (!connect()) {
            if (stats.forceShutdown && stats.isShutdown) {
                return ;
            }

            if (allowDataLoss && getppid() == 1) {
                LOG(EXTENSION_LOG_WARNING,
                    "Parent process is gone and you allow data loss during"
                    "shutdown. Terminating without without syncing all data.");
                _exit(1);
            }
            if (configurationError) {
                rv.str(std::string());
                rv << "Failed to connect to: \""
                   << host.c_str() << ":"
                   << port << "\"";
                LOG(EXTENSION_LOG_WARNING, "%s", rv.str().c_str());

                usleep(5000);
                // we might have new configuration parameters...
                configurationError = false;
            } else {
                rv.str(std::string());
                rv << "Connection refused: \""
                   << host.c_str() << ":"
                   << port << "\"";
                LOG(EXTENSION_LOG_WARNING, "%s", rv.str().c_str());
                usleep(reconnectSleepTime);
            }
        }
        rv.str(std::string());
        rv << "Connected to mccouch: \""
           << host.c_str() << ":"
           << port << "\"";
        LOG(EXTENSION_LOG_WARNING, "%s", rv.str().c_str());
    }
}

bool CouchNotifier::waitForWritable()
{
    size_t timeout = 1000;
    size_t waitTime = 0;

    while (connected) {
        struct pollfd fds;
        fds.fd = sock;
        fds.events = POLLIN | POLLOUT;
        fds.revents = 0;

        int ret = poll(&fds, 1, timeout);
        if (ret > 0) {
            if (fds.revents & POLLIN) {
                maybeProcessInput();
            }

            if (fds.revents & POLLOUT) {
                return true;
            }
        } else if (ret == SOCKET_ERROR) {
#ifdef _MSC_VER
            LOG(EXTENSION_LOG_WARNING, "WSAPoll() failed: \"%s\"",
                getErrorString(WSAGetLastError()));
#else
            LOG(EXTENSION_LOG_WARNING, "poll() failed: \"%s\"",
                strerror(errno));
#endif
            resetConnection();
        }  else if ((waitTime += timeout) >= responseTimeOut) {
            // Poll failed due to timeouts multiple times and is above timeout threshold.
            LOG(EXTENSION_LOG_WARNING,
                "No response for mccouch in %ld seconds. Resetting connection.",
                waitTime);
            resetConnection();
        }
    }

    return false;
}

void CouchNotifier::sendSingleChunk(const char *ptr, size_t nb)
{
    while (nb > 0) {
        ssize_t nw = send(sock, ptr, nb, 0);
        if (nw == -1) {
            int error = errno;
            std::string errmsg(strerror(errno));

#ifdef _MSC_VER
            DWORD err = GetLastError();
            if (err == WSAEWOULDBLOCK) {
                error = EWOULDBLOCK;
            } else {
                errmsg = getErrorString(err);
            }
#endif

            switch (error) {
            case EINTR:
                // retry
                LOG(EXTENSION_LOG_WARNING,
                    "Failed to send data to mccouch for %s: \"%s\" "
                    "will retry immediately\n",
                    cmd2str(currentCommand), errmsg.c_str());
                break;

            case EWOULDBLOCK:
                if (!waitForWritable()) {
                    return;
                }
                break;

            default:
                LOG(EXTENSION_LOG_WARNING,
                    "Failed to send data to mccouch: \"%s\"", errmsg.c_str());
                abort();
                return ;
            }
        } else {
            ptr += (size_t)nw;
            nb -= nw;
            if (nb != 0) {
                // We failed to send all of the data... we should take a
                // short break to let the receiving side get a chance
                // to drain the buffer..
                LOG(EXTENSION_LOG_WARNING,
                    "Failed to send all data. Wait a while until mccouch "
                    "is ready to receive more data, reamins = %ld\n", nb);
                usleep(10);
            }
        }
    }
}

void CouchNotifier::sendCommand(BinaryPacketHandler *rh)
{
    currentCommand = reinterpret_cast<uint8_t*>(sendIov[0].iov_base)[1];
    int cmdId = commandId(currentCommand);
    ensureConnection();
    if (!connected) {
        // we might have been disconnected
        LOG(EXTENSION_LOG_WARNING,
            "Failed to send data for %s: connection to mccouch is "
            "not established successfully, shutdown in progress %s",
            cmd2str(currentCommand), stats.isShutdown ? "yes" : "no");
        commandStats[cmdId].numError++;
        delete rh;
        return;
    }

    responseHandler.push_back(rh);
    long backoffSleep = 10;

    do {
        sendMsg.msg_iovlen = numiovec;
        ssize_t nw = sendmsg(sock, &sendMsg, 0);
        if (nw == -1) {
            int error = errno;
            std::string errmsg(strerror(errno));

#ifdef _MSC_VER
            DWORD err = GetLastError();
            if (err == WSAEWOULDBLOCK) {
                error = EWOULDBLOCK;
            } else if (err == WSAEMSGSIZE) {
                error = EMSGSIZE;
            } else {
                errmsg = getErrorString(err);
            }
#endif
            switch (error) {
            case EMSGSIZE:
                // Too big.. try to use send instead..
                for (int ii = 0; ii < numiovec; ++ii) {
                    sendSingleChunk((const char*)(sendIov[ii].iov_base), sendIov[ii].iov_len);
                }
                break;

            case EINTR:
                // retry
                LOG(EXTENSION_LOG_WARNING,
                    "Failed to sendmsg data to mccouch for %s: \"%s\" "
                    "will retry immediately\n",
                    cmd2str(currentCommand), errmsg.c_str());
                break;

            case EWOULDBLOCK:
                if (!waitForWritable()) {
                    return;
                }
                break;
            default:
                LOG(EXTENSION_LOG_WARNING,
                    "Failed to send data to mccouch for %s: \"%s\"",
                    cmd2str(currentCommand), errmsg.c_str());
                resetConnection();
                return;
            }
        } else {
            size_t towrite = 0;
            for (int ii = 0; ii < numiovec; ++ii) {
                towrite += sendIov[ii].iov_len;
            }

            if (towrite == static_cast<size_t>(nw)) {
                // Everything successfully sent!
                lastSentCommand = currentCommand;
                commandStats[cmdId].numSent++;
                currentCommand = static_cast<uint8_t>(0xff);
                LOG(EXTENSION_LOG_DEBUG,
                    "Successfully sending to mccouch: cmd=%s, bytes=%ld",
                    cmd2str(lastSentCommand), towrite);
                return;
            } else {
                size_t rms = towrite - static_cast<size_t>(nw);
                LOG(EXTENSION_LOG_WARNING,
                    "Failed to send all data. Wait a while until mccouch "
                    "is ready to receive more data, sent %lu remains = %lu\n",
                    static_cast<size_t>(nw), rms);

                // We failed to send all of the data... we should take a
                // short break to let the receiving side get a chance
                // to drain the buffer.. Back off as we go to avoid
                // just jamming down the system
                usleep(backoffSleep);
                if (backoffSleep < 1000000) {
                    backoffSleep *= 2;
                }

                // Figure out how much we sent, and repack the stuff
                for (int ii = 0; ii < numiovec && nw > 0; ++ii) {
                    if (sendIov[ii].iov_len <= (size_t)nw) {
                        nw -= sendIov[ii].iov_len;
                        sendIov[ii].iov_len = 0;
                    } else {
                        // only parts of this iovector was sent..
                        sendIov[ii].iov_base = static_cast<char*>(sendIov[ii].iov_base) + nw;
                        sendIov[ii].iov_len -= nw;
                        nw = 0;
                    }
                }

                // Do I need to fix the iovector...
                int index = 0;
                for (int ii = 0; ii < numiovec; ++ii) {
                    if (sendIov[ii].iov_len != 0) {
                        if (index == ii) {
                            index = numiovec;
                            break;
                        }
                        sendIov[index].iov_len = sendIov[ii].iov_len;
                        sendIov[index].iov_base = sendIov[ii].iov_base;
                        ++index;
                    }
                }
                numiovec = index;
            }
        }
    } while (true);
}

void CouchNotifier::maybeProcessInput()
{
    struct pollfd fds;
    fds.fd = sock;
    fds.events = POLLIN;
    fds.revents = 0;

    // @todo check for the #msg sent to avoid a shitload
    // extra syscalls
    int ret = poll(&fds, 1, 0);
    if (ret > 0 && (fds.revents & POLLIN) == POLLIN) {
        processInput();
    }
}

bool CouchNotifier::processInput() {
    // we don't want to block unless there is a message there..
    // this will unfortunately increase the overhead..
    cb_assert(sock != INVALID_SOCKET);

    size_t processed;
    protocol_binary_response_header *res;

    input.grow(8192);
    res = (protocol_binary_response_header *)input.data;

    do {
        ssize_t nr;
        while (input.avail >= sizeof(*res) && input.avail >= (ntohl(
                res->response.bodylen) + sizeof(*res))) {

            lastReceivedCommand = res->response.opcode;
            switch (res->response.magic) {
            case PROTOCOL_BINARY_RES:
                LOG(EXTENSION_LOG_DEBUG,
                    "Successfully received response (%s) from mccouch for %s"
                    ", bytes=%ld",
                    cmd2str(lastReceivedCommand),
                    cmd2str(lastSentCommand),
                    input.avail);
                handleResponse(res);
                break;
            default:
                LOG(EXTENSION_LOG_WARNING,
                    "Rubbish received on the backend stream. closing it");
                resetConnection();
                return false;
            }

            processed = ntohl(res->response.bodylen) + sizeof(*res);
            memmove(input.data, input.data + processed, input.avail - processed);
            input.avail -= processed;
        }

        if (input.size - input.avail == 0) {
            input.grow();
        }

        nr = recv(sock, input.data + input.avail, input.size - input.avail, 0);
        if (nr == -1) {
            int error = errno;
            std::string errmsg(strerror(errno));

#ifdef _MSC_VER
            DWORD err = GetLastError();
            if (err == WSAEWOULDBLOCK || err == ERROR_SUCCESS) {
                error = EWOULDBLOCK;
            } else if (err == WSAEMSGSIZE) {
                error = EMSGSIZE;
            } else {
                errmsg = getErrorString(err);
            }
#endif
            switch (error) {
            case EINTR:
                LOG(EXTENSION_LOG_WARNING,
                    "Failed to receive data from mccouch for %s: \"%s\" "
                    "will retry immediately\n",
                    cmd2str(lastSentCommand), errmsg.c_str());
                break;
            case EWOULDBLOCK:
                return true;
            default:
                LOG(EXTENSION_LOG_WARNING,
                    "Failed to read from mccouch for %s: \"%s\"",
                    cmd2str(lastSentCommand), errmsg.c_str());
                resetConnection();
                return false;
            }
        } else if (nr == 0) {
            LOG(EXTENSION_LOG_WARNING, "Connection closed by mccouch");
            resetConnection();
            return false;
        } else {
            input.avail += (size_t)nr;
        }
    } while (true);

    return true;
}

bool CouchNotifier::waitForReadable(bool tryOnce)
{
    size_t timeout = 1000;
    size_t waitTime = 0;

    while (connected) {
        bool reconnect = false;
        struct pollfd fds;
        fds.fd = sock;
        fds.events = POLLIN;
        fds.revents = 0;

        // @todo do not block forever.. but allow shutdown..
        int ret = poll(&fds, 1, timeout);
        if (ret > 0) {
            if (fds.revents & POLLIN) {
                return true;
            }
        } else if (ret == SOCKET_ERROR) {
#ifdef _MSC_VER
            LOG(EXTENSION_LOG_WARNING, "WSAPoll() failed: \"%s\"",
                getErrorString(WSAGetLastError()));
#else
            LOG(EXTENSION_LOG_WARNING,
                             "poll() failed: \"%s\"",
                             strerror(errno));
#endif
            reconnect = true;
        } else if ((waitTime += timeout) >= responseTimeOut) {
            // Poll failed due to timeouts multiple times and is above timeout threshold.
            LOG(EXTENSION_LOG_WARNING,
                "No response for mccouch in %ld seconds. Resetting connection.",
                waitTime);
            reconnect = true;
        }

        if (reconnect) {
            resetConnection();
            if (tryOnce) {
                return false;
            }
        }
    }

    return false;
}

void CouchNotifier::wait()
{
    std::list<BinaryPacketHandler*> *handler = &responseHandler;

    while (!handler->empty() && waitForReadable()) {
        // We don't want to busy-loop, so wait until there is something
        // there...
        processInput();
    }
}

bool CouchNotifier::waitOnce()
{
    std::list<BinaryPacketHandler*> *handler = &responseHandler;

    bool succeed = false;
    while (!handler->empty() && (succeed = waitForReadable(true))) {
        succeed = processInput();
    }
    return succeed;
}

void CouchNotifier::delVBucket(uint16_t vb, Callback<bool> &cb) {
    protocol_binary_request_del_vbucket req;
    LockHolder lh(mutex);
    // delete vbucket must wait for a response
    do {
        memset(req.bytes, 0, sizeof(req.bytes));
        req.message.header.request.magic = PROTOCOL_BINARY_REQ;
        req.message.header.request.opcode = PROTOCOL_BINARY_CMD_DEL_VBUCKET;
        req.message.header.request.vbucket = ntohs(vb);
        req.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
        req.message.header.request.opaque = seqno;

        sendIov[0].iov_base = (char*)req.bytes;
        sendIov[0].iov_len = sizeof(req.bytes);
        numiovec = 1;

        sendCommand(new DelVBucketResponseHandler(seqno++, cb));
    } while (!waitOnce());
}

void CouchNotifier::flush(Callback<bool> &cb) {
    protocol_binary_request_flush req;
    // flush must wait for a response
    LockHolder lh(mutex);
    do {
        memset(req.bytes, 0, sizeof(req.bytes));
        req.message.header.request.magic = PROTOCOL_BINARY_REQ;
        req.message.header.request.opcode = PROTOCOL_BINARY_CMD_FLUSH;
        req.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
        req.message.header.request.extlen = 4;
        req.message.header.request.bodylen = ntohl(4);
        req.message.header.request.opaque = seqno;
        sendIov[0].iov_base = (char*)req.bytes;
        sendIov[0].iov_len = sizeof(req.bytes);
        numiovec = 1;

        sendCommand(new FlushResponseHandler(seqno++, cb));
    } while (!waitOnce());
}

void CouchNotifier::selectBucket() {
    protocol_binary_request_no_extras req;
    // select bucket must succeed
    do {
        memset(req.bytes, 0, sizeof(req.bytes));
        req.message.header.request.magic = PROTOCOL_BINARY_REQ;
        req.message.header.request.opcode = 0x89;
        req.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
        req.message.header.request.opaque = seqno;
        req.message.header.request.keylen = ntohs((uint16_t)bucketName.length());
        req.message.header.request.bodylen = ntohl((uint32_t)bucketName.length());

        sendIov[0].iov_base = (char*)req.bytes;
        sendIov[0].iov_len = sizeof(req.bytes);
        sendIov[1].iov_base = const_cast<char*>(bucketName.c_str());
        sendIov[1].iov_len = bucketName.length();
        numiovec = 2;

        inSelectBucket = true;
        sendCommand(new SelectBucketResponseHandler(seqno++));
    } while (!waitOnce());

    inSelectBucket = false;
}

void CouchNotifier::notify_update(const VBStateNotification &vbs,
                                  uint64_t file_version,
                                  uint64_t header_offset,
                                  Callback<uint16_t> &cb)
{
    protocol_binary_request_notify_vbucket_update req;
    LockHolder lh(mutex);
    // notify_bucket must wait for a response
    do {
        memset(req.bytes, 0, sizeof(req.bytes));
        req.message.header.request.magic = PROTOCOL_BINARY_REQ;
        req.message.header.request.opcode = PROTOCOL_BINARY_CMD_NOTIFY_VBUCKET_UPDATE;
        req.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
        req.message.header.request.vbucket = ntohs(vbs.vbucket);
        req.message.header.request.opaque = seqno;
        req.message.header.request.bodylen = ntohl(32);

        req.message.body.file_version = ntohll(file_version);
        req.message.body.header_offset = ntohll(header_offset);
        req.message.body.vbucket_state_updated = ntohl(vbs.updateType);
        req.message.body.state = ntohl(vbs.state);
        req.message.body.checkpoint = ntohll(vbs.checkpoint);

        sendIov[0].iov_base = (char*)req.bytes;
        sendIov[0].iov_len = sizeof(req.bytes);
        numiovec = 1;

        sendCommand(new NotifyVbucketUpdateResponseHandler(seqno++, cb));
    } while(!waitOnce());
}

void CouchNotifier::addStats(const std::string &prefix,
                             ADD_STAT add_stat,
                             const void *c)
{
    for (uint8_t ii = 0; ii < MAX_NUM_NOTIFIER_CMD; ++ii) {
        commandStats[ii].addStats(prefix, cmdId2str(ii), add_stat, c);
    }
    add_prefixed_stat(prefix, "current_command", cmd2str(currentCommand), add_stat, c);
    add_prefixed_stat(prefix, "last_sent_command", cmd2str(lastSentCommand), add_stat, c);
    add_prefixed_stat(prefix, "last_received_command", cmd2str(lastReceivedCommand),
            add_stat, c);
}

const char *CouchNotifier::cmd2str(uint8_t cmd)
{
    switch(cmd) {
    case PROTOCOL_BINARY_CMD_DEL_VBUCKET:
        return "del_vbucket";
    case PROTOCOL_BINARY_CMD_FLUSH:
        return "flush";
    case PROTOCOL_BINARY_CMD_NOTIFY_VBUCKET_UPDATE:
        return "notify_vbucket_update";
    case 0x89 :
        return "select_bucket";
    default:
        return "unknown";
    }
}

int CouchNotifier::commandId(uint8_t opcode) {
    switch(opcode) {
    case PROTOCOL_BINARY_CMD_DEL_VBUCKET:
        return del_vbucket_cmd;
    case PROTOCOL_BINARY_CMD_FLUSH:
        return flush_vbucket_cmd;
    case PROTOCOL_BINARY_CMD_NOTIFY_VBUCKET_UPDATE:
        return update_vbucket_cmd;
    case 0x89 :
        return select_bucket_cmd;
    default:
        return unknown_cmd;
    }
}

const char *CouchNotifier::cmdId2str(int id) {
    switch(id) {
    case del_vbucket_cmd:
        return cmd2str(PROTOCOL_BINARY_CMD_DEL_VBUCKET);
    case flush_vbucket_cmd:
        return cmd2str(PROTOCOL_BINARY_CMD_FLUSH);
    case update_vbucket_cmd:
        return cmd2str(PROTOCOL_BINARY_CMD_NOTIFY_VBUCKET_UPDATE);
    case select_bucket_cmd:
        return cmd2str(0x89);
     default:
        return "unknown";
    }
}
