/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <string.h>
#include <cstdlib>
#include <cctype>
#include <algorithm>
#include "locks.hh"

#include "mc-kvstore/mc-engine.hh"
#include "ep_engine.h"

/*
 * The various response handlers
 */
class BinaryPacketHandler {
public:
    BinaryPacketHandler(uint32_t sno, EPStats *st) :
        seqno(sno), stats(st), start(0)
    {
        if (stats) {
            start = gethrtime();
        }
    }

    virtual ~BinaryPacketHandler() { /* EMPTY */ }

    virtual void request(protocol_binary_request_header *) {
        unsupported();
    }
    virtual void response(protocol_binary_response_header *) {
        unsupported();
    }

    virtual void implicitResponse() {
        // by default we don't use quiet commands..
        abort();
    }

    virtual void connectionReset() {
        abort();
    }

    uint32_t seqno;

    hrtime_t getDelta() {
        return (gethrtime() - start) / 1000;
    }

private:
    void unsupported() {
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                "Unsupported packet received");
        abort();
    }

protected:
    EPStats *stats;
    hrtime_t start;
};

class DelResponseHandler: public BinaryPacketHandler {
public:
    DelResponseHandler(uint32_t sno, EPStats *st, Callback<int> &cb) :
        BinaryPacketHandler(sno, st), callback(cb) {
        // EMPTY
    }

    virtual void response(protocol_binary_response_header *res) {
        updateHistogram();
        uint16_t rcode = ntohs(res->response.status);
        int value = 1;

        if (rcode != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
            value = -1;
        }

        callback.callback(value);
    }

    virtual void connectionReset() {
        int value = -1;
        callback.callback(value);
    }

    virtual void implicitResponse() {
        updateHistogram();
        int value = 1;
        callback.callback(value);
    }

    void updateHistogram() {
        if (stats) {
            stats->couchDelqHisto.add(getDelta());
        }
    }

private:
    hrtime_t start;
    Callback<int> &callback;
    EPStats *epStats;
};

class GetResponseHandler: public BinaryPacketHandler {
public:
    GetResponseHandler(uint32_t sno, EPStats *st, const std::string &k, uint16_t vb,
            Callback<GetValue> &cb) :
        BinaryPacketHandler(sno, st), key(k), vbucket(vb), callback(cb) { /* EMPTY */
    }

    virtual void response(protocol_binary_response_header *res) {
        uint16_t rcode = ntohs(res->response.status);
        if (rcode == PROTOCOL_BINARY_RESPONSE_SUCCESS) {
            updateHistogram(ntohl(res->response.bodylen) - 4, true);
            protocol_binary_response_get *gres;
            gres = (protocol_binary_response_get*)res;

            // @todo I need to get the exptime somehow, or is that
            // overwritten with the value we've got in the cache??
            Item *it = new Item(key, gres->message.body.flags, 0,
                    gres->bytes + sizeof(gres->bytes),
                    ntohl(res->response.bodylen) - 4,
                    ntohll(res->response.cas), -1, vbucket);
            GetValue rv(it);
            callback.callback(rv);
        } else {
            updateHistogram(0, false);
            GetValue rv;
            callback.callback(rv);
        }
    }

    virtual void connectionReset() {
        // There isn't a good way to signal back an error!!!
        // a cache miss may confuse the core :S
        GetValue rv;
        callback.callback(rv);
    }

    void updateHistogram(size_t size, bool success) {
        if (stats) {
            if (success) {
                if (size == 0) {
                    size = 1;
                }

                stats->couchGetHisto.add(getDelta() / size);
            } else {
                stats->couchGetFailHisto.add(getDelta());
            }
        }
    }

private:
    std::string key;
    uint16_t vbucket;
    Callback<GetValue> &callback;
};

class SetResponseHandler: public BinaryPacketHandler {
public:
    SetResponseHandler(uint32_t sno, EPStats *st,
                       bool cr,
                       size_t nb,
                       Callback<mutation_result> &cb) :
        BinaryPacketHandler(sno, st), newId(cr ? 1 : 0), nbytes(nb),
        callback(cb) { }

    virtual void response(protocol_binary_response_header *res) {
        uint16_t rcode = ntohs(res->response.status);
        int rv = 1;

        if (rcode != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
            newId = -1;
            rv = 0;
            updateHistogram(false);
        } else {
            updateHistogram(true);
        }

        mutation_result p(rv, newId);
        callback.callback(p);
    }

    virtual void connectionReset() {
        mutation_result p(0, -1);
        callback.callback(p);
    }

    virtual void implicitResponse() {
        updateHistogram(true);
        mutation_result p(1, newId);
        callback.callback(p);
    }

    void updateHistogram(bool success) {
        if (stats) {
            if (success) {
                if (nbytes == 0) {
                    nbytes = 1;
                }

                stats->couchSetHisto.add(getDelta() / nbytes);
            } else {
                stats->couchSetFailHisto.add(getDelta());
            }
        }
    }

private:
    int64_t newId;
    size_t nbytes;
    Callback<mutation_result> &callback;
};

class StatsResponseHandler: public BinaryPacketHandler {
public:
    StatsResponseHandler(uint32_t sno, EPStats *st, Callback<std::map<std::string, std::string> > &cb) :
        BinaryPacketHandler(sno, st), callback(cb) {
    }

    virtual void response(protocol_binary_response_header *res) {
        uint16_t keylen = ntohs(res->response.keylen);
        uint32_t vallen = ntohl(res->response.bodylen) - keylen;

        if (res->response.keylen == 0) {
            callback.callback(stats);
        } else {
            std::string key((const char *)res->bytes + sizeof(res->bytes),
                    keylen);
            std::string value(
                    (const char *)res->bytes + sizeof(res->bytes) + keylen,
                    vallen);
            stats[key] = value;
        }
    }

    virtual void connectionReset() {
        callback.callback(stats);
    }

private:
    std::map<std::string, std::string> stats;
    Callback<std::map<std::string, std::string> > &callback;
};

class SetVBucketResponseHandler: public BinaryPacketHandler {
public:
    SetVBucketResponseHandler(uint32_t sno, EPStats *st, Callback<bool> &cb) :
        BinaryPacketHandler(sno, st), callback(cb) {
    }

    virtual void response(protocol_binary_response_header *res) {
        uint16_t rcode = ntohs(res->response.status);
        bool success = true;

        if (rcode != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
            abort();
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

class DelVBucketResponseHandler: public BinaryPacketHandler {
public:
    DelVBucketResponseHandler(uint32_t sno, EPStats *st, Callback<bool> &cb) :
        BinaryPacketHandler(sno, st), callback(cb) {
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
    FlushResponseHandler(uint32_t sno, EPStats *st, Callback<bool> &cb) :
        BinaryPacketHandler(sno, st), callback(cb) {
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
    SelectBucketResponseHandler(uint32_t sno, EPStats *st) :
        BinaryPacketHandler(sno, st) {
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

class TapResponseHandler: public BinaryPacketHandler {
public:
    TapResponseHandler(uint32_t sno, EPStats *st, shared_ptr<TapCallback> &cb,
                       bool keys_only, bool is_warmup) :
        BinaryPacketHandler(sno, st), callback(cb),
        num(0), keysOnly(keys_only), isWarmup(is_warmup) { }

    ~TapResponseHandler() {
        if (keysOnly) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Preloaded %zu keys (with metadata)",
                             num);
        }
        if (!keysOnly && isWarmup) {
            stats->warmupTime.set(getDelta());
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "The second phase of warmup took %ld (us).",
                             stats->warmupTime.get());
        }
    }

    virtual void response(protocol_binary_response_header *res) {
        uint16_t rcode = ntohs(res->response.status);
        bool success = false;
        assert(rcode != PROTOCOL_BINARY_RESPONSE_SUCCESS);
        callback->complete->callback(success);

        uint32_t bodylen = ntohl(res->response.bodylen);
        if (bodylen > 0 && bodylen < 512) {
            char buffer[512];
            memcpy(buffer, res->bytes + sizeof(res->bytes), bodylen);
            buffer[bodylen] = '\0';
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Dump failed: \"%s\"", buffer);
        } else {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Failed to dump data from couch");
        }
    }

    virtual void request(protocol_binary_request_header *req) {
        if (req->request.opcode == PROTOCOL_BINARY_CMD_TAP_OPAQUE) {
            bool success = true;
            callback->complete->callback(success);
        } else {
            protocol_binary_request_tap_mutation *mreq;
            mreq = (protocol_binary_request_tap_mutation *)req;

            uint16_t tap_flags = ntohs(mreq->message.body.tap.flags);
            bool partial = tap_flags & TAP_FLAG_NO_VALUE;

            uint16_t nes = ntohs(mreq->message.body.tap.enginespecific_length);
            uint8_t *es = mreq->bytes + sizeof(mreq->bytes);
            uint8_t *keyptr = es + nes;
            uint16_t keylen = ntohs(req->request.keylen);
            uint32_t vallen = ntohl(req->request.bodylen) - keylen
                    - req->request.extlen - nes;
            uint8_t *valptr = keyptr + keylen;

            // @todo I need to get the exptime somehow, or is that
            // overwritten with the value we've got in the cache??
            Item *it = new Item(keyptr, keylen,
                                mreq->message.body.item.flags,
                                mreq->message.body.item.expiration, valptr,
                                vallen,
                                req->request.cas, 1,
                                ntohs(req->request.vbucket));
            if (nes == 0) {
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "FATAL: Object returned from mccouch without revid");
                abort();
            }
            uint32_t s;
            uint64_t c;
            uint32_t l;
            uint32_t f;

            if (!Item::decodeMeta(es, s, c, l, f)) {
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "FATAL: Object returned from mccouch with CAS == 0");
                abort();
            }
            it->setCas(c);
            it->setSeqno(s);
            GetValue rv(it, ENGINE_SUCCESS, -1, -1, NULL, partial);
            callback->cb->callback(rv);
            ++num;
        }
    }

    virtual void connectionReset() {
        bool value = false;
        callback->complete->callback(value);
    }

private:
    shared_ptr<TapCallback> callback;
    size_t num;
    bool keysOnly;
    bool isWarmup;
};

class NoopResponseHandler: public BinaryPacketHandler {
public:
    NoopResponseHandler(uint32_t sno, EPStats *st, Callback<bool> &cb) :
        BinaryPacketHandler(sno, st), callback(cb) {
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

/*
 * Implementation of the mmeber functions in the MemcachedEngine class
 */
MemcachedEngine::MemcachedEngine(EventuallyPersistentEngine *e, Configuration &config) :
    sock(INVALID_SOCKET), configuration(config), configurationError(true),
    shutdown(false), seqno(0),
    currentCommand(0xff), lastSentCommand(0xff), lastReceivedCommand(0xff),
    engine(e), epStats(NULL), connected(false)
{
    memset(&sendMsg, 0, sizeof(sendMsg));
    sendMsg.msg_iov = sendIov;

    if (engine != NULL) {
        epStats = &engine->getEpStats();
    }

    // Select the bucket (will be sent immediately when we connect)
    selectBucket();
}

void MemcachedEngine::resetConnection(void) {
    LockHolder lh(mutex);
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

    for (iter = tapHandler.begin(); iter != tapHandler.end(); ++iter) {
        (*iter)->connectionReset();
        delete *iter;
    }

    responseHandler.clear();
    tapHandler.clear();

    // Now insert the select vbucket command..
    selectBucket();
}

void MemcachedEngine::handleResponse(protocol_binary_response_header *res) {
    LockHolder lh(mutex);
    std::list<BinaryPacketHandler*>::iterator iter;
    for (iter = responseHandler.begin(); iter != responseHandler.end()
            && (*iter)->seqno < res->response.opaque; ++iter) {

        // TROND
        // Buffer *b = (*iter)->getCommandBuffer();
        // commandStats[static_cast<uint8_t>(b->data[1])].numImplicit++;
        (*iter)->implicitResponse();
        delete *iter;
    }

    if (iter == responseHandler.end() || (*iter)->seqno != res->response.opaque) {
        if (iter != responseHandler.begin()) {
            responseHandler.erase(responseHandler.begin(), iter);
        }

        // this could be the response for a tap connect...
        for (iter = tapHandler.begin(); iter != tapHandler.end() && (*iter)->seqno
                 != res->response.opaque; ++iter) {
            // empty
        }

        if (iter != tapHandler.end()) {
            if (ntohs(res->response.status) == PROTOCOL_BINARY_RESPONSE_SUCCESS){
                commandStats[res->response.opcode].numSuccess++;
            } else {
                commandStats[res->response.opcode].numError++;
            }
            (*iter)->response(res);
            delete *iter;
            tapHandler.erase(iter);
        }

        return ;
    }

    if (ntohs(res->response.status) == PROTOCOL_BINARY_RESPONSE_SUCCESS){
        commandStats[res->response.opcode].numSuccess++;
    } else {
        commandStats[res->response.opcode].numError++;
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

void MemcachedEngine::handleRequest(protocol_binary_request_header *req) {
    LockHolder lh(mutex);

    std::list<BinaryPacketHandler*>::iterator iter;
    for (iter = tapHandler.begin(); iter != tapHandler.end() && (*iter)->seqno
            != req->request.opaque; ++iter) {
        // empty
    }

    if (req->request.opcode != PROTOCOL_BINARY_CMD_TAP_MUTATION
            && req->request.opcode != PROTOCOL_BINARY_CMD_TAP_OPAQUE) {
        std::cerr << "Incorrect opcode!" << std::endl;
        abort();
    }
    if (iter == tapHandler.end()) {
        std::cerr << "Failed to map the correct server" << req->request.opcode
                << std::endl;
        abort();
    }

    (*iter)->request(req);
    if (req->request.opcode == PROTOCOL_BINARY_CMD_TAP_OPAQUE) {
        delete *iter;
        tapHandler.erase(iter);
    }
}

bool MemcachedEngine::connect() {
    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_flags = AI_PASSIVE;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_family = AF_UNSPEC;

    size_t port = configuration.getCouchPort();
    std::string host = configuration.getCouchHost();
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
        getLogger()->log(EXTENSION_LOG_WARNING, this, msg.str().c_str());
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
        getLogger()->log(EXTENSION_LOG_WARNING, this, msg.str().c_str());
        configurationError = true;
        return false;
    }

    // @fixme
    connected = true;

    return true;
}

void MemcachedEngine::ensureConnection()
{
    if (!connected) {
        // I need to connect!!!
        std::stringstream rv;
        rv << "Trying to connect to mccouch: \""
           << configuration.getCouchHost().c_str() << ":"
           << configuration.getCouchPort() << "\"";

        getLogger()->log(EXTENSION_LOG_WARNING, this, rv.str().c_str());
        while (!connect()) {
            if (shutdown) {
                return ;
            }

            if (configuration.isAllowDataLossDuringShutdown() && getppid() == 1) {
                getLogger()->log(EXTENSION_LOG_WARNING, this,
                                 "Parent process is gone and you allow "
                                 "data loss during shutdown.\n"
                                 "Terminating without without syncing "
                                 "all data.");
                _exit(1);
            }
            if (configurationError) {
                rv.str(std::string());
                rv << "Failed to connect to: \""
                   << configuration.getCouchHost().c_str() << ":"
                   << configuration.getCouchPort() << "\"";
                getLogger()->log(EXTENSION_LOG_WARNING, this, rv.str().c_str());

                usleep(5000);
                // we might have new configuration parameters...
                configurationError = false;
            } else {
                rv.str(std::string());
                rv << "Connection refused: \""
                   << configuration.getCouchHost().c_str() << ":"
                   << configuration.getCouchPort() << "\"";
                getLogger()->log(EXTENSION_LOG_WARNING, this, rv.str().c_str());
                usleep(configuration.getCouchReconnectSleeptime());
            }
        }
        rv.str(std::string());
        rv << "Connected to mccouch: \""
           << configuration.getCouchHost().c_str() << ":"
           << configuration.getCouchPort() << "\"";
        getLogger()->log(EXTENSION_LOG_WARNING, this, rv.str().c_str());
    }
}

bool MemcachedEngine::waitForWritable()
{
    while (connected) {
        struct pollfd fds;
        fds.fd = sock;
        fds.events = POLLIN | POLLOUT;
        fds.revents = 0;

        // @todo do not block forever.. but allow shutdown..
        int ret = poll(&fds, 1, 1000);
        if (ret > 0) {
            if (fds.revents & POLLIN) {
                maybeProcessInput();
            }

            if (fds.revents & POLLOUT) {
                return true;
            }
        } else if (ret < 0) {
            getLogger()->log(EXTENSION_LOG_WARNING, this,
                             "poll() failed: \"%s\"",
                             strerror(errno));
            resetConnection();
        }
    }

    return false;
}

void MemcachedEngine::sendSingleChunk(const unsigned char *ptr, size_t nb)
{
    while (nb > 0) {
        ssize_t nw = send(sock, ptr, nb, 0);
        if (nw == -1) {
            switch (errno) {
            case EINTR:
                // retry
                break;

            case EWOULDBLOCK:
                if (!waitForWritable()) {
                    return;
                }
                break;

            default:
                getLogger()->log(EXTENSION_LOG_WARNING, this,
                                 "Failed to send data to mccouch: \"%s\"",
                                 strerror(errno));
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
                usleep(10);
            }
        }
    }
}

void MemcachedEngine::sendCommand(BinaryPacketHandler *rh)
{
    currentCommand = reinterpret_cast<uint8_t*>(sendIov[0].iov_base)[1];
    ensureConnection();

    if (dynamic_cast<TapResponseHandler*>(rh)) {
        tapHandler.push_back(rh);
    } else {
        responseHandler.push_back(rh);
    }
    maybeProcessInput();
    if (!connected) {
        // we might have been disconnected
        return;
    }

    do {
        sendMsg.msg_iovlen = numiovec;
        ssize_t nw = sendmsg(sock, &sendMsg, 0);
        if (nw == -1) {
            switch (errno) {
            case EMSGSIZE:
                // Too big.. try to use send instead..
                for (int ii = 0; ii < numiovec; ++ii) {
                    sendSingleChunk((const unsigned char*)(sendIov[ii].iov_base), sendIov[ii].iov_len);
                }
                break;

            case EINTR:
                // retry
                break;

            case EWOULDBLOCK:
                if (!waitForWritable()) {
                    return;
                }
                break;
            default:
                getLogger()->log(EXTENSION_LOG_WARNING, this,
                                 "Failed to send data to mccouch: \"%s\"",
                                 strerror(errno));
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
                commandStats[currentCommand].numSent++;
                currentCommand = static_cast<uint8_t>(0xff);
                return;
            } else {
                // We failed to send all of the data... we should take a
                // short break to let the receiving side get a chance
                // to drain the buffer..
                usleep(10);

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


void MemcachedEngine::maybeProcessInput()
{
    struct pollfd fds;
    fds.fd = sock;
    fds.events = POLLIN;
    fds.revents = 0;

    // @todo check for the #msg sent to avoid a shitload
    // extra syscalls
    int ret= poll(&fds, 1, 0);
    if (ret > 0 && (fds.revents & POLLIN) == POLLIN) {
        processInput();
    }
}

void MemcachedEngine::processInput() {
    // we don't want to block unless there is a message there..
    // this will unfortunately increase the overhead..
    assert(sock != INVALID_SOCKET);

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
                handleResponse(res);
                break;
            case PROTOCOL_BINARY_REQ:
                commandStats[res->response.opcode].numSuccess++;
                handleRequest((protocol_binary_request_header *)res);
                break;
            default:
                getLogger()->log(EXTENSION_LOG_WARNING, this,
                        "Rubbish received on the backend stream. closing it");
                resetConnection();
                return;
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
            switch (errno) {
            case EINTR:
                break;
            case EWOULDBLOCK:
                return;
            default:
                getLogger()->log(EXTENSION_LOG_WARNING, this,
                                 "Failed to read from mccouch: \"%s\"",
                                 strerror(errno));
                resetConnection();
                return;
            }
        } else if (nr == 0) {
            getLogger()->log(EXTENSION_LOG_WARNING, this,
                             "Connection closed by mccouch");
            resetConnection();
            return;
        } else {
            input.avail += (size_t)nr;
        }
    } while (true);
}

bool MemcachedEngine::waitForReadable()
{
    while (connected) {
        struct pollfd fds;
        fds.fd = sock;
        fds.events = POLLIN;
        fds.revents = 0;

        // @todo do not block forever.. but allow shutdown..
        int ret = poll(&fds, 1, 1000);
        if (ret > 0) {
            if (fds.revents & POLLIN) {
                return true;
            }

        } else if (ret < 0) {
            getLogger()->log(EXTENSION_LOG_WARNING, this,
                             "poll() failed: \"%s\"",
                             strerror(errno));
            resetConnection();
        }
    }

    return false;
}

void MemcachedEngine::wait()
{
    std::list<BinaryPacketHandler*> *handler;

    if (tapHandler.size() > 0) {
        handler = &tapHandler;
    } else {
        handler = &responseHandler;
    }

    while (handler->size() > 0 && waitForReadable()) {
        // We don't want to busy-loop, so wait until there is something
        // there...
        processInput();
    }
}

void MemcachedEngine::delq(const std::string &key, uint16_t vb,
                           Callback<int> &cb)
{
    protocol_binary_request_delete req;
    memset(req.bytes, 0, sizeof(req.bytes));
    req.message.header.request.magic = PROTOCOL_BINARY_REQ;
    req.message.header.request.opcode = PROTOCOL_BINARY_CMD_DELETEQ;
    req.message.header.request.keylen = ntohs((uint16_t)key.length());
    req.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    req.message.header.request.vbucket = ntohs(vb);
    req.message.header.request.bodylen = ntohl((uint32_t)key.length());
    req.message.header.request.opaque = seqno;

    sendIov[0].iov_base = (char*)req.bytes;
    sendIov[0].iov_len = sizeof(req.bytes);
    sendIov[1].iov_base = const_cast<char*>(key.c_str());
    sendIov[1].iov_len = key.length();

    numiovec = 2;
    sendCommand(new DelResponseHandler(seqno, epStats, cb));
}

void MemcachedEngine::setmq(const Item &it, Callback<mutation_result> &cb) {
    protocol_binary_request_set_with_meta req;
    memset(req.bytes, 0, sizeof(req.bytes));
    req.message.header.request.magic = PROTOCOL_BINARY_REQ;
    req.message.header.request.opcode = CMD_SETQ_WITH_META;
    req.message.header.request.extlen = 12;
    req.message.header.request.keylen = ntohs((uint16_t)it.getNKey());
    req.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    req.message.header.request.vbucket = ntohs(it.getVBucketId());
    uint32_t bodylen = req.message.header.request.extlen + it.getNKey()
        + it.getNBytes() + it.getNMetaBytes();
    req.message.header.request.bodylen = ntohl(bodylen);
    req.message.body.nmeta_bytes = ntohl(it.getNMetaBytes());
    req.message.body.flags = it.getFlags();
    req.message.body.expiration = htonl((uint32_t)it.getExptime());

    uint8_t meta[30];
    size_t nmeta = it.getNMetaBytes();
    Item::encodeMeta(it, meta, nmeta);

    sendIov[0].iov_base = (char*)req.bytes;
    sendIov[0].iov_len = sizeof(req.bytes);
    sendIov[1].iov_base = const_cast<char*>(it.getKey().c_str());
    sendIov[1].iov_len = it.getNKey();
    sendIov[2].iov_base = const_cast<char*>(it.getData());
    sendIov[2].iov_len = it.getNBytes();
    sendIov[3].iov_base = reinterpret_cast<char*>(meta);
    sendIov[3].iov_len = nmeta;

    numiovec = 4;
    sendCommand(new SetResponseHandler(seqno++, epStats, it.getId() <= 0,
                                       it.getNBytes(), cb));
}

void MemcachedEngine::get(const std::string &key, uint16_t vb,
        Callback<GetValue> &cb) {
    protocol_binary_request_get req;
    memset(req.bytes, 0, sizeof(req.bytes));
    req.message.header.request.magic = PROTOCOL_BINARY_REQ;
    req.message.header.request.opcode = PROTOCOL_BINARY_CMD_GET;
    req.message.header.request.keylen = ntohs((uint16_t)key.length());
    req.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    req.message.header.request.vbucket = ntohs(vb);
    req.message.header.request.bodylen = ntohl((uint32_t)key.length());
    req.message.header.request.opaque = seqno;

    sendIov[0].iov_base = (char*)req.bytes;
    sendIov[0].iov_len = sizeof(req.bytes);
    sendIov[1].iov_base = const_cast<char*>(key.c_str());
    sendIov[1].iov_len = key.length();
    numiovec = 2;
    sendCommand(new GetResponseHandler(seqno, epStats, key, vb, cb));
    wait();
}

void MemcachedEngine::stats(const std::string &key,
                            Callback<std::map<std::string,
                            std::string> > &cb)
{
    protocol_binary_request_stats req;
    memset(req.bytes, 0, sizeof(req.bytes));
    req.message.header.request.magic = PROTOCOL_BINARY_REQ;
    req.message.header.request.opcode = PROTOCOL_BINARY_CMD_STAT;
    req.message.header.request.keylen = ntohs((uint16_t)key.length());
    req.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    req.message.header.request.bodylen = ntohl((uint32_t)key.length());
    req.message.header.request.opaque = seqno;

    sendIov[0].iov_base = (char*)req.bytes;
    sendIov[0].iov_len = sizeof(req.bytes);
    sendIov[1].iov_base = const_cast<char*>(key.c_str());
    sendIov[1].iov_len = key.length();
    numiovec = 2;
    sendCommand(new StatsResponseHandler(seqno++, epStats, cb));
    wait();
}

void MemcachedEngine::setVBucket(uint16_t vb, vbucket_state_t state,
                                 Callback<bool> &cb) {
    protocol_binary_request_set_vbucket req;
    memset(req.bytes, 0, sizeof(req.bytes));
    req.message.header.request.magic = PROTOCOL_BINARY_REQ;
    req.message.header.request.opcode = PROTOCOL_BINARY_CMD_SET_VBUCKET;
    req.message.header.request.extlen = 4;
    req.message.header.request.vbucket = ntohs(vb);
    req.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    req.message.header.request.bodylen = ntohl((uint32_t)4);
    req.message.body.state = (vbucket_state_t)htonl((uint32_t)state);
    req.message.header.request.opaque = seqno;

    sendIov[0].iov_base = (char*)req.bytes;
    sendIov[0].iov_len = sizeof(req.bytes);
    numiovec = 1;
    sendCommand(new SetVBucketResponseHandler(seqno++, epStats, cb));
    wait();
}

void MemcachedEngine::snapshotVBuckets(const vbucket_map_t &m, Callback<bool> &cb) {
    protocol_binary_request_noop *req;
    const size_t tuplesize = sizeof(uint16_t) + sizeof(uint64_t) + sizeof(uint32_t);
    size_t bodysize = tuplesize * m.size();

    Buffer *buffer = new Buffer(sizeof(req->bytes) + bodysize);
    req = (protocol_binary_request_noop *)buffer->data;
    memset(buffer->data, 0, buffer->size);

    req->message.header.request.magic = PROTOCOL_BINARY_REQ;
    req->message.header.request.opcode = CMD_SNAPSHOT_VB_STATES;
    req->message.header.request.extlen = 0;
    req->message.header.request.vbucket = 0;
    req->message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    req->message.header.request.bodylen = ntohl((uint32_t)bodysize);
    req->message.header.request.opaque = seqno;

    uint8_t *dest = req->bytes + sizeof(req->bytes);
    vbucket_map_t::const_iterator iter;
    for (iter = m.begin(); iter != m.end(); ++iter) {
        std::pair<uint16_t, uint16_t> first = iter->first;
        const vbucket_state vbstate = iter->second;
        uint16_t vbid = ntohs(first.first);
        uint32_t state = ntohl((uint32_t)vbstate.state);
        uint64_t checkpointId = ntohll(vbstate.checkpointId);

        memcpy(dest, &vbid, sizeof(vbid));
        dest += sizeof(vbid);
        memcpy(dest, &state, sizeof(state));
        dest += sizeof(state);
        memcpy(dest, &checkpointId, sizeof(checkpointId));
        dest += sizeof(checkpointId);
    }
    buffer->avail = buffer->size;

    sendIov[0].iov_base = buffer->data;
    sendIov[0].iov_len = buffer->size;
    numiovec = 1;
    sendCommand(new SetVBucketResponseHandler(seqno++, epStats, cb));
    wait();
    delete buffer;
}

void MemcachedEngine::delVBucket(uint16_t vb, Callback<bool> &cb) {
    protocol_binary_request_del_vbucket req;
    memset(req.bytes, 0, sizeof(req.bytes));
    req.message.header.request.magic = PROTOCOL_BINARY_REQ;
    req.message.header.request.opcode = PROTOCOL_BINARY_CMD_DEL_VBUCKET;
    req.message.header.request.vbucket = ntohs(vb);
    req.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    req.message.header.request.opaque = seqno;

    sendIov[0].iov_base = (char*)req.bytes;
    sendIov[0].iov_len = sizeof(req.bytes);
    numiovec = 1;
    sendCommand(new DelVBucketResponseHandler(seqno++, epStats, cb));
    wait();
}

void MemcachedEngine::flush(Callback<bool> &cb) {
    protocol_binary_request_flush req;
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
    sendCommand(new FlushResponseHandler(seqno++, epStats, cb));
    wait();
}

void MemcachedEngine::selectBucket() {
    std::string name = configuration.getCouchBucket();
    protocol_binary_request_no_extras req;
    memset(req.bytes, 0, sizeof(req.bytes));
    req.message.header.request.magic = PROTOCOL_BINARY_REQ;
    req.message.header.request.opcode = 0x89;
    req.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    req.message.header.request.keylen = ntohs((uint16_t)name.length());
    req.message.header.request.bodylen = ntohl((uint32_t)name.length());
    req.message.header.request.opaque = seqno;

    sendIov[0].iov_base = (char*)req.bytes;
    sendIov[0].iov_len = sizeof(req.bytes);
    sendIov[1].iov_base = const_cast<char*>(name.c_str());
    sendIov[1].iov_len = name.length();
    numiovec = 2;

    sendCommand(new SelectBucketResponseHandler(seqno++, epStats));
}

void MemcachedEngine::tap(shared_ptr<TapCallback> cb) {
    protocol_binary_request_tap_connect req;
    memset(req.bytes, 0, sizeof(req.bytes));
    req.message.header.request.magic = PROTOCOL_BINARY_REQ;
    req.message.header.request.opcode = PROTOCOL_BINARY_CMD_TAP_CONNECT;
    req.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    req.message.header.request.extlen = 4;
    req.message.header.request.bodylen = ntohl(4);
    req.message.header.request.opaque = seqno;
    req.message.body.flags = ntohl(TAP_CONNECT_FLAG_DUMP);
    sendIov[0].iov_base = (char*)req.bytes;
    sendIov[0].iov_len = sizeof(req.bytes);
    numiovec = 1;

    sendCommand(new TapResponseHandler(seqno++, epStats, cb, false, true));
    wait();
}

void MemcachedEngine::tapKeys(shared_ptr<TapCallback> cb) {
    protocol_binary_request_tap_connect req;
    memset(req.bytes, 0, sizeof(req.bytes));
    req.message.header.request.magic = PROTOCOL_BINARY_REQ;
    req.message.header.request.opcode = PROTOCOL_BINARY_CMD_TAP_CONNECT;
    req.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    req.message.header.request.extlen = 4;
    req.message.header.request.bodylen = ntohl(4);
    req.message.header.request.opaque = seqno;
    req.message.body.flags = ntohl(TAP_CONNECT_FLAG_DUMP | TAP_CONNECT_REQUEST_KEYS_ONLY);

    sendIov[0].iov_base = (char*)req.bytes;
    sendIov[0].iov_len = sizeof(req.bytes);
    numiovec = 1;

    sendCommand(new TapResponseHandler(seqno++, epStats, cb, true, true));
    wait();
}

void MemcachedEngine::tap(const std::vector<uint16_t> &vbids,
                          bool full, shared_ptr<TapCallback> cb)
{
    protocol_binary_request_tap_connect *req;

    Buffer *buffer = new Buffer(sizeof(req->bytes) + 2 + (2 * vbids.size()));
    req = (protocol_binary_request_tap_connect*)buffer->data;

    memset(buffer->data, 0, buffer->size);
    req->message.header.request.magic = PROTOCOL_BINARY_REQ;
    req->message.header.request.opcode = PROTOCOL_BINARY_CMD_TAP_CONNECT;
    req->message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    req->message.header.request.extlen = 4;
    req->message.header.request.bodylen = ntohl(6 + (2 * vbids.size()));
    req->message.header.request.opaque = seqno;

    uint32_t flags = TAP_CONNECT_FLAG_DUMP | TAP_CONNECT_FLAG_LIST_VBUCKETS;
    if (!full) {
        flags |= TAP_CONNECT_REQUEST_KEYS_ONLY;
    }

    req->message.body.flags = ntohl(flags);
    uint16_t nv = htons(vbids.size());
    memcpy(req->bytes + sizeof(req->bytes), &nv, sizeof(nv));
    uint8_t *ptr = req->bytes + sizeof(req->bytes) + sizeof(nv);

    std::vector<uint16_t>::const_iterator ii;
    for (ii = vbids.begin(); ii != vbids.end(); ++ii) {
        uint16_t vb = ntohs(*ii);
        memcpy(ptr, &vb, sizeof(vb));
        ptr += sizeof(vb);
    }

    buffer->avail = buffer->size;
    sendIov[0].iov_base = buffer->data;
    sendIov[0].iov_len = buffer->size;
    numiovec = 1;

    sendCommand(new TapResponseHandler(seqno++, epStats, cb, !full, false));
    delete buffer;
    wait();
}

void MemcachedEngine::noop(Callback<bool> &cb)
{
    protocol_binary_request_noop req;
    memset(req.bytes, 0, sizeof(req.bytes));
    req.message.header.request.magic = PROTOCOL_BINARY_REQ;
    req.message.header.request.opcode = PROTOCOL_BINARY_CMD_NOOP;
    req.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    req.message.header.request.opaque = seqno;

    sendIov[0].iov_base = (char*)req.bytes;
    sendIov[0].iov_len = sizeof(req.bytes);
    numiovec = 1;

    sendCommand(new NoopResponseHandler(seqno++, epStats, cb));
    // Wait for response!!
    wait();
}

void MemcachedEngine::addStats(const std::string &prefix,
                               ADD_STAT add_stat,
                               const void *c)
{
    addStat(prefix, "type", "mccouch", add_stat, c);
    for (uint8_t ii = 0; ii < 0xff; ++ii) {
        commandStats[ii].addStats(prefix, cmd2str(ii), add_stat, c);
    }
    addStat(prefix, "current_command", cmd2str(currentCommand), add_stat, c);
    addStat(prefix, "last_sent_command", cmd2str(lastSentCommand), add_stat, c);
    addStat(prefix, "last_received_command", cmd2str(lastReceivedCommand),
            add_stat, c);
}

const char *MemcachedEngine::cmd2str(uint8_t cmd)
{
    switch(cmd) {
    case PROTOCOL_BINARY_CMD_DELETEQ:
        return "delq";
    case CMD_SETQ_WITH_META:
        return "setq_with_meta";
    case CMD_SNAPSHOT_VB_STATES:
        return "snapshot_vb_states";
    case PROTOCOL_BINARY_CMD_DEL_VBUCKET:
        return "del_vbucket";
    case PROTOCOL_BINARY_CMD_FLUSH:
        return "flush";
    case PROTOCOL_BINARY_CMD_GET:
        return "get";
    case PROTOCOL_BINARY_CMD_NOOP:
        return "noop";
    case PROTOCOL_BINARY_CMD_SET_VBUCKET:
        return "set_vbucket";
    case PROTOCOL_BINARY_CMD_STAT:
        return "stat";
    case PROTOCOL_BINARY_CMD_TAP_CONNECT:
        return "tap_connect";
    case PROTOCOL_BINARY_CMD_TAP_MUTATION:
        return "tap_mutation";
    case PROTOCOL_BINARY_CMD_TAP_OPAQUE:
        return "tap_opaque";
    case 0x89 :
        return "select_vbucket";

    default:
        return "unknown";
    }
}
