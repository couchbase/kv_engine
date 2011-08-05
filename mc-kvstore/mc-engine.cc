/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <string.h>
#include <cstdlib>
#include <cctype>
#include <algorithm>
#include "locks.hh"

#include "mc-kvstore/mc-engine.hh"
#include "ep_engine.h"

#ifdef HAVE_MEMCACHED_PACKET_DEBUG_H
#include "mc-kvstore/mc-debug.hh"
static bool packet_debug = true;
std::ostream& operator<<(std::ostream& out, const Buffer *buffer) {
    switch ((uint8_t)buffer->data[0]) {
    case PROTOCOL_BINARY_REQ:
        out
                << reinterpret_cast<const protocol_binary_request_header *> (buffer->data);
        break;
    case PROTOCOL_BINARY_RES:
        out
                << reinterpret_cast<const protocol_binary_response_header *> (buffer->data);
        break;
    default:
        out << "Invalid packet" << std::endl;
    }

    return out;
}
#else
static bool packet_debug = false;
#endif


/*
 * C interface wrapping back into the Engine class
 */
void *start_memcached_engine(void *arg) {
    static_cast<MemcachedEngine*> (arg)->run();
    return NULL;
}

void memcached_engine_libevent_callback(evutil_socket_t sock, short which,
        void *arg) {
    static_cast<MemcachedEngine*> (arg)->libeventCallback(sock, which);
}

void memcached_engine_notify_callback(evutil_socket_t sock, short which,
        void *arg) {
    static_cast<MemcachedEngine*> (arg)->notifyHandler(sock, which);
}

/*
 * The various response handlers
 */
class BinaryPacketHandler {
public:
    BinaryPacketHandler(Buffer *theRequest, EPStats *st) :
        seqno(0), message(theRequest), stats(st), start(0)
    {
        if (stats) {
            start = gethrtime();
        }
    }

    virtual ~BinaryPacketHandler() {
        delete message;
    }

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

    Buffer *getCommandBuffer() {
        return message;
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

    Buffer *message;

protected:
    EPStats *stats;
    hrtime_t start;
};

class DelResponseHandler: public BinaryPacketHandler {
public:
    DelResponseHandler(Buffer *theRequest, EPStats *st, Callback<int> &cb) :
        BinaryPacketHandler(theRequest, st), callback(cb) {
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
    GetResponseHandler(Buffer *theRequest, EPStats *st, const std::string &k, uint16_t vb,
            Callback<GetValue> &cb) :
        BinaryPacketHandler(theRequest, st), key(k), vbucket(vb), callback(cb) { /* EMPTY */
    }

    virtual void response(protocol_binary_response_header *res) {
        uint16_t rcode = ntohs(res->response.status);
        if (rcode == PROTOCOL_BINARY_RESPONSE_SUCCESS) {
            updateHistogram(ntohl(res->response.bodylen) - 4, true);
            protocol_binary_response_get *gres;
            gres = (protocol_binary_response_get*)res;

            // @todo I need to get the exptime somehow, or is that
            // overwritten with the value we've got in the cache??
            Item *item = new Item(key, gres->message.body.flags, 0,
                    gres->bytes + sizeof(gres->bytes),
                    ntohl(res->response.bodylen) - 4,
                    ntohll(res->response.cas), -1, vbucket);
            GetValue rv(item);
            callback.callback(rv);
        } else {
            updateHistogram(0, false);
            GetValue rv;
            callback.callback(rv);
        }
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
    SetResponseHandler(Buffer *theRequest, EPStats *st,
                       bool cr,
                       size_t nb,
                       Callback<mutation_result> &cb) :
        BinaryPacketHandler(theRequest, st), newId(cr ? 1 : 0), nbytes(nb),
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
    StatsResponseHandler(Buffer *theRequest, EPStats *st, Callback<std::map<std::string, std::string> > &cb) :
        BinaryPacketHandler(theRequest, st), callback(cb) {
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

private:
    std::map<std::string, std::string> stats;
    Callback<std::map<std::string, std::string> > &callback;
};

class SetVBucketResponseHandler: public BinaryPacketHandler {
public:
    SetVBucketResponseHandler(Buffer *theRequest, EPStats *st, Callback<bool> &cb) :
        BinaryPacketHandler(theRequest, st), callback(cb) {
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

private:
    Callback<bool> &callback;
};

class DelVBucketResponseHandler: public BinaryPacketHandler {
public:
    DelVBucketResponseHandler(Buffer *theRequest, EPStats *st, Callback<bool> &cb) :
        BinaryPacketHandler(theRequest, st), callback(cb) {
    }

    virtual void response(protocol_binary_response_header *res) {
        uint16_t rcode = ntohs(res->response.status);
        bool success = true;

        if (rcode != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
            success = false;
        }

        callback.callback(success);
    }

private:
    Callback<bool> &callback;
};

class FlushResponseHandler: public BinaryPacketHandler {
public:
    FlushResponseHandler(Buffer *theRequest, EPStats *st, Callback<bool> &cb) :
        BinaryPacketHandler(theRequest, st), callback(cb) {
    }

    virtual void response(protocol_binary_response_header *res) {
        uint16_t rcode = ntohs(res->response.status);
        bool success = true;

        if (rcode != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
            success = false;
        }

        callback.callback(success);
    }

private:
    Callback<bool> &callback;
};

class SelectBucketResponseHandler: public BinaryPacketHandler {
public:
    SelectBucketResponseHandler(Buffer *theRequest, EPStats *st) :
        BinaryPacketHandler(theRequest, st) {
    }

    virtual void response(protocol_binary_response_header *res) {
        uint16_t rcode = ntohs(res->response.status);

        if (rcode != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
            // This should _NEVER_ happen!!!!
            abort();
        }
    }
};

class TapResponseHandler: public BinaryPacketHandler {
public:
    TapResponseHandler(Buffer *theRequest, EPStats *st, TapCallback &cb) :
        BinaryPacketHandler(theRequest, st), callback(cb) {
    }

    virtual void response(protocol_binary_response_header *res) {
        uint16_t rcode = ntohs(res->response.status);
        bool success = false;
        assert(rcode != PROTOCOL_BINARY_RESPONSE_SUCCESS);
        callback.complete.callback(success);
    }

    virtual void request(protocol_binary_request_header *req) {
        if (req->request.opcode == PROTOCOL_BINARY_CMD_TAP_OPAQUE) {
            bool success = true;
            callback.complete.callback(success);
        } else {
            protocol_binary_request_tap_mutation *mreq;
            mreq = (protocol_binary_request_tap_mutation *)req;

            uint16_t tap_flags = ntohs(mreq->message.body.tap.flags);
            bool partial = tap_flags & TAP_FLAG_NO_VALUE;
            uint8_t *keyptr = mreq->bytes + sizeof(mreq->bytes);
            uint16_t keylen = ntohs(req->request.keylen);
            uint32_t vallen = ntohl(req->request.bodylen) - keylen
                    - req->request.extlen;
            uint8_t *valptr = keyptr + keylen;

            // @todo I need to get the exptime somehow, or is that
            // overwritten with the value we've got in the cache??
            Item *item = new Item(keyptr, keylen,
                    mreq->message.body.item.flags,
                    mreq->message.body.item.expiration, valptr, vallen,
                    req->request.cas, -1, ntohs(req->request.vbucket));

            GetValue rv(item, ENGINE_SUCCESS, -1, -1, NULL, partial);
            callback.cb.callback(rv);
        }
    }

private:
    TapCallback &callback;
};

class NoopResponseHandler: public BinaryPacketHandler {
public:
    NoopResponseHandler(Buffer *theRequest, EPStats *st, Callback<bool> &cb) :
        BinaryPacketHandler(theRequest, st), callback(cb) {
    }

    virtual void response(protocol_binary_response_header *res) {
        uint16_t rcode = ntohs(res->response.status);
        bool success = true;

        if (rcode != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
            success = false;
        }

        callback.callback(success);
    }

private:
    Callback<bool> &callback;
};

/*
 * Implementation of the mmeber functions in the MemcachedEngine class
 */
MemcachedEngine::MemcachedEngine(EventuallyPersistentEngine *e, Configuration &config) :
    sock(INVALID_SOCKET), configuration(config), configurationError(true),
    shutdown(false), ev_base(event_base_new()),
    ev_flags(0), seqno(0), output(NULL), engine(e), epStats(NULL)
{
    if (engine != NULL) {
        epStats = &engine->getEpStats();
    }

    if (!createNotificationPipe()) {
        throw std::runtime_error("Failed to create notification pipe");
    }

    event_set(&ev_notify, notifyPipe[0], EV_READ | EV_PERSIST,
            memcached_engine_notify_callback, this);
    event_base_set(ev_base, &ev_notify);
    if (event_add(&ev_notify, NULL) == -1) {
        throw std::runtime_error("Failed to create notification pipe");
    }

    // Select the bucket (will be sent immediately when we connect)
    doSelectBucket();
}

void MemcachedEngine::start() {
    if (pthread_create(&threadid, NULL, start_memcached_engine, this) != 0) {
        throw std::runtime_error("Error creating memcached engine thread");
    }
}

MemcachedEngine::~MemcachedEngine() {
    shutdown = true;
    notify();
    int ret = pthread_join(threadid, NULL);
    if (ret != 0) {
        getLogger()->log(EXTENSION_LOG_WARNING, this,
                         "Failed to join thread: %d %s\n", ret, strerror(ret));
        abort();
    }

    EVUTIL_CLOSESOCKET(notifyPipe[0]);
    EVUTIL_CLOSESOCKET(notifyPipe[1]);
    notifyPipe[0] = notifyPipe[1] = INVALID_SOCKET;

    if (ev_flags != 0 && event_del(&ev_event) == -1) {
        getLogger()->log(EXTENSION_LOG_WARNING, this,
                         "Failed to delete event\n");
        abort();
    }
    if (event_del(&ev_notify) == -1) {
        getLogger()->log(EXTENSION_LOG_WARNING, this,
                         "Failed to delete notify event\n");
        abort();
    }


    event_base_free(ev_base);
    ev_base = NULL;
}

void MemcachedEngine::run() {
    // Set up the notification pipe..

    if (engine != NULL) {
        ObjectRegistry::onSwitchThread(engine);
    }

    while (!shutdown) {
        if (sock == INVALID_SOCKET) {
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

        updateEvent(sock);
        event_base_loop(ev_base, 0);
        event_del(&ev_event);
    }
}

void MemcachedEngine::libeventCallback(evutil_socket_t s, short which) {
    if ((which & EV_WRITE) || (ev_flags & EV_WRITE) == 0) {
        sendData(s);
    }

    if (which & EV_READ) {
        receiveData(s);
    }

    updateEvent(s);
}

void MemcachedEngine::notifyHandler(evutil_socket_t s, short which) {
    if (shutdown) {
        event_base_loopbreak(ev_base);
    }
    assert(which & EV_READ);
    char devnull[1024];

    if (recv(s, devnull, sizeof(devnull), 0) == -1) {
        abort();
    }

    // Someone added a new command... just invoke the callback
    // it will automatically send the command if we isn't blocked
    // on write already...
    libeventCallback(sock, 0);
}

void MemcachedEngine::updateEvent(evutil_socket_t s) {
    if (sock == INVALID_SOCKET) {
        return;
    }
    assert(s == sock);
    int flags = EV_READ | EV_PERSIST;
    if (output != NULL) {
        flags |= EV_WRITE;
    }

    if (flags == ev_flags) {
        // no change in flags!
        return;
    }

    if (ev_flags != 0) {
        event_del(&ev_event);
    }

    ev_flags = flags;
    event_set(&ev_event, sock, ev_flags, memcached_engine_libevent_callback,
            this);
    event_base_set(ev_base, &ev_event);
    if (event_add(&ev_event, NULL) == -1) {
        abort();
    }
}

void MemcachedEngine::resetConnection(void) {
    LockHolder lh(mutex);
    event_del(&ev_event);
    ev_flags = 0;
    EVUTIL_CLOSESOCKET(sock);
    sock = INVALID_SOCKET;
    output = NULL;
    input.avail = 0;
    while (!commandQueue.empty()) {
        commandQueue.pop();
    }

    std::list<BinaryPacketHandler*> s1(responseHandler);
    std::list<BinaryPacketHandler*> s2(tapHandler);

    responseHandler.clear();
    tapHandler.clear();

    // Now insert the select vbucket command..
    doSelectBucket();
    // Reschedule all of the commands...
    reschedule(s1);
    reschedule(s2);
    event_base_loopbreak(ev_base);
}

void MemcachedEngine::reschedule(std::list<BinaryPacketHandler*> &packets) {
    std::list<BinaryPacketHandler*> p(packets);
    packets.clear();

    std::list<BinaryPacketHandler*>::iterator iter;
    for (iter = p.begin(); iter != p.end(); ++iter) {
        doInsertCommand(*iter);
    }
}

void MemcachedEngine::receiveData(evutil_socket_t s) {
    if (sock == -1) {
        return;
    }
    assert(s == sock);

    size_t processed;
    protocol_binary_response_header *res;

    input.grow(8192);
    res = (protocol_binary_response_header *)input.data;

    do {
        ssize_t nr;
        while (input.avail >= sizeof(*res) && input.avail >= (ntohl(
                res->response.bodylen) + sizeof(*res))) {

            switch (res->response.magic) {
            case PROTOCOL_BINARY_RES:
                handleResponse(res);
                break;
            case PROTOCOL_BINARY_REQ:
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

        nr = recv(s, input.data + input.avail, input.size - input.avail, 0);
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

void MemcachedEngine::sendData(evutil_socket_t s) {
    if (sock == -1) {
        return ;
    }
    assert(s == sock);
    do {
        if (!output) {
            output = nextToSend();
            if (!output) {
                // we're out of data to send
                return;
            }
        }

        ssize_t nw = send(s, output->data + output->curr,
                          output->avail - output->curr, 0);
        if (nw == -1) {
            switch (errno) {
            case EINTR:
                // retry
                break;
            case EWOULDBLOCK:
                // The network can't accept more..
                return;
            default:
                getLogger()->log(EXTENSION_LOG_WARNING, this,
                                 "Failed to send data to mccouch: \"%s\"",
                                 strerror(errno));
                resetConnection();
                return;
            }
        } else {
            output->curr += (size_t)nw;
            if (output->curr == output->avail) {
                // all data sent!
                output = NULL;
            }
        }
    } while (true);
}

Buffer *MemcachedEngine::nextToSend() {
    LockHolder lh(mutex);
    if (commandQueue.empty()) {
        return NULL;
    }
    Buffer *ret = commandQueue.front();
    commandQueue.pop();

    if (packet_debug) {
        std::cout << ret;
    }
    protocol_binary_response_header *res;
    res = (protocol_binary_response_header *)ret->data;
    return ret;
}

void MemcachedEngine::handleResponse(protocol_binary_response_header *res) {
    if (packet_debug) {
        std::cout << res;
    }

    LockHolder lh(mutex);
    std::list<BinaryPacketHandler*>::iterator iter;
    for (iter = responseHandler.begin(); iter != responseHandler.end()
            && (*iter)->seqno < res->response.opaque; ++iter) {
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
            (*iter)->response(res);
            delete *iter;
            tapHandler.erase(iter);
        }

        return ;
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

    if (packet_debug) {
        std::cout << req;
    }

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

void MemcachedEngine::notify() {
    ssize_t nw = send(notifyPipe[1], "", 1, 0);
    if (nw != 1) {
        std::stringstream ss;
        ss << "Failed to send notification message to the engine. "
           << "send() returned " << nw << " but we expected 1.";
        if (nw == -1) {
            ss << std::endl << "Error: " << strerror(errno);
        }
        getLogger()->log(EXTENSION_LOG_WARNING, this, ss.str().c_str());
        // @todo for now let's just abort... I'd like to figure out if / why
        // this happens, and people don't pay enough attention to log messages
        // ;)
        abort();
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
            if (::connect(sock, ai->ai_addr, ai->ai_addrlen) != -1
                    && evutil_make_socket_nonblocking(sock) == 0) {
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

    return true;
}

bool MemcachedEngine::createNotificationPipe() {
    if (evutil_socketpair(SOCKETPAIR_AF, SOCK_STREAM, 0, notifyPipe)
            == SOCKET_ERROR) {
        return false;
    }

    for (int j = 0; j < 2; ++j) {
        int flags = 1;
        setsockopt(notifyPipe[j], IPPROTO_TCP, TCP_NODELAY, (void *)&flags,
                sizeof(flags));
        setsockopt(notifyPipe[j], SOL_SOCKET, SO_REUSEADDR, (void *)&flags,
                sizeof(flags));

        if (evutil_make_socket_nonblocking(notifyPipe[j]) == -1) {
            EVUTIL_CLOSESOCKET(notifyPipe[0]);
            EVUTIL_CLOSESOCKET(notifyPipe[1]);
            notifyPipe[0] = notifyPipe[1] = INVALID_SOCKET;
            return false;
        }
    }

    return true;
}

/**
 * Assign a new sequence number and link command and the response handler
 * to it's lists.
 */
void MemcachedEngine::insertCommand(BinaryPacketHandler *rh) {
    LockHolder lh(mutex);
    doInsertCommand(rh);
}

void MemcachedEngine::doInsertCommand(BinaryPacketHandler *rh) {
    protocol_binary_request_no_extras *req;
    Buffer *buffer = rh->getCommandBuffer();
    buffer->curr = 0;
    req = (protocol_binary_request_no_extras *)buffer->data;
    req->message.header.request.opaque = rh->seqno = seqno++;

    bool doNotify = commandQueue.empty();
    commandQueue.push(buffer);
    if (dynamic_cast<TapResponseHandler*>(rh)) {
        tapHandler.push_back(rh);
    } else {
        responseHandler.push_back(rh);
    }

    if (doNotify) {
        notify();
    }
}

void MemcachedEngine::delq(const std::string &key, uint16_t vb,
        Callback<int> &cb) {
    protocol_binary_request_delete *req;
    Buffer *buffer = new Buffer(sizeof(req->bytes) + key.length());
    req = (protocol_binary_request_delete *)buffer->data;

    memset(buffer->data, 0, buffer->size);
    req->message.header.request.magic = PROTOCOL_BINARY_REQ;
    req->message.header.request.opcode = PROTOCOL_BINARY_CMD_DELETEQ;
    req->message.header.request.keylen = ntohs((uint16_t)key.length());
    req->message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    req->message.header.request.vbucket = ntohs(vb);
    req->message.header.request.bodylen = ntohl((uint32_t)key.length());
    memcpy(buffer->data + sizeof(req->bytes), key.c_str(), key.length());
    buffer->avail = buffer->size;

    insertCommand(new DelResponseHandler(buffer, epStats, cb));
}

void MemcachedEngine::setq(const Item &item, Callback<mutation_result> &cb) {

    protocol_binary_request_set *req;
    Buffer *buffer = new Buffer(
            sizeof(req->bytes) + item.getNKey() + item.getNBytes());
    req = (protocol_binary_request_set *)buffer->data;

    memset(buffer->data, 0, buffer->size);
    req->message.header.request.magic = PROTOCOL_BINARY_REQ;
    req->message.header.request.opcode = PROTOCOL_BINARY_CMD_SETQ;
    req->message.header.request.extlen = 8;
    req->message.header.request.keylen = ntohs((uint16_t)item.getNKey());
    req->message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    req->message.header.request.vbucket = ntohs(item.getVBucketId());
    // @todo we should probably pos the CAS in the store (not that
    //       couch chould verify it, but be able to store it)
    // req->message.header.request.cas = ntohll(item.getCas());
    uint32_t bodylen = req->message.header.request.extlen + item.getNKey()
            + item.getNBytes();
    req->message.header.request.bodylen = ntohl(bodylen);

    req->message.body.flags = item.getFlags();
    req->message.body.expiration = htonl((uint32_t)item.getExptime());
    memcpy(buffer->data + sizeof(req->bytes), item.getKey().c_str(),
            item.getNKey());
    memcpy(buffer->data + sizeof(req->bytes) + item.getNKey(), item.getData(),
            item.getNBytes());

    buffer->avail = buffer->size;

    insertCommand(new SetResponseHandler(buffer, epStats, item.getId() <= 0,
                                         item.getNBytes(), cb));
}

void MemcachedEngine::get(const std::string &key, uint16_t vb,
        Callback<GetValue> &cb) {
    protocol_binary_request_get *req;
    Buffer *buffer = new Buffer(sizeof(req->bytes) + key.length());
    req = (protocol_binary_request_get *)buffer->data;

    memset(buffer->data, 0, buffer->size);
    req->message.header.request.magic = PROTOCOL_BINARY_REQ;
    req->message.header.request.opcode = PROTOCOL_BINARY_CMD_GET;
    req->message.header.request.keylen = ntohs((uint16_t)key.length());
    req->message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    req->message.header.request.vbucket = ntohs(vb);
    req->message.header.request.bodylen = ntohl((uint32_t)key.length());
    memcpy(buffer->data + sizeof(req->bytes), key.c_str(), key.length());
    buffer->avail = buffer->size;

    insertCommand(new GetResponseHandler(buffer, epStats, key, vb, cb));
}

void MemcachedEngine::stats(const std::string &key,
        Callback<std::map<std::string, std::string> > &cb) {

    protocol_binary_request_stats *req;
    Buffer *buffer = new Buffer(sizeof(req->bytes) + key.length());
    req = (protocol_binary_request_stats *)buffer->data;

    memset(buffer->data, 0, buffer->size);
    req->message.header.request.magic = PROTOCOL_BINARY_REQ;
    req->message.header.request.opcode = PROTOCOL_BINARY_CMD_STAT;
    req->message.header.request.keylen = ntohs((uint16_t)key.length());
    req->message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    req->message.header.request.bodylen = ntohl((uint32_t)key.length());
    memcpy(buffer->data + sizeof(req->bytes), key.c_str(), key.length());
    buffer->avail = buffer->size;

    insertCommand(new StatsResponseHandler(buffer, epStats, cb));
}

void MemcachedEngine::setVBucket(uint16_t vb, vbucket_state_t state,
        Callback<bool> &cb) {
    protocol_binary_request_set_vbucket *req;
    Buffer *buffer = new Buffer(sizeof(req->bytes));
    req = (protocol_binary_request_set_vbucket *)buffer->data;

    memset(buffer->data, 0, buffer->size);
    req->message.header.request.magic = PROTOCOL_BINARY_REQ;
    req->message.header.request.opcode = PROTOCOL_BINARY_CMD_SET_VBUCKET;
    req->message.header.request.extlen = 4;
    req->message.header.request.vbucket = ntohs(vb);
    req->message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    req->message.header.request.bodylen = ntohl((uint32_t)4);
    req->message.body.state = (vbucket_state_t)htonl((uint32_t)state);
    buffer->avail = buffer->size;

    insertCommand(new SetVBucketResponseHandler(buffer, epStats, cb));
}

void MemcachedEngine::delVBucket(uint16_t vb, Callback<bool> &cb) {
    protocol_binary_request_del_vbucket *req;
    Buffer *buffer = new Buffer(sizeof(req->bytes));
    req = (protocol_binary_request_del_vbucket *)buffer->data;

    memset(buffer->data, 0, buffer->size);
    req->message.header.request.magic = PROTOCOL_BINARY_REQ;
    req->message.header.request.opcode = PROTOCOL_BINARY_CMD_DEL_VBUCKET;
    req->message.header.request.vbucket = ntohs(vb);
    req->message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    buffer->avail = buffer->size;

    insertCommand(new DelVBucketResponseHandler(buffer, epStats, cb));
}

void MemcachedEngine::flush(Callback<bool> &cb) {
    protocol_binary_request_flush *req;
    Buffer *buffer = new Buffer(sizeof(req->bytes));
    req = (protocol_binary_request_flush *)buffer->data;

    memset(buffer->data, 0, buffer->size);
    req->message.header.request.magic = PROTOCOL_BINARY_REQ;
    req->message.header.request.opcode = PROTOCOL_BINARY_CMD_FLUSH;
    req->message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    req->message.header.request.extlen = 4;
    req->message.header.request.bodylen = ntohl(4);
    buffer->avail = buffer->size;

    insertCommand(new FlushResponseHandler(buffer, epStats, cb));
}

void MemcachedEngine::doSelectBucket() {
    protocol_binary_request_no_extras *req;

    std::string name = configuration.getCouchBucket();
    Buffer *buffer = new Buffer(sizeof(req->bytes) + name.length());
    req = (protocol_binary_request_no_extras*)buffer->data;

    memset(buffer->data, 0, buffer->size);
    req->message.header.request.magic = PROTOCOL_BINARY_REQ;
    req->message.header.request.opcode = 0x89;
    req->message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    req->message.header.request.keylen = ntohs((uint16_t)name.length());
    req->message.header.request.bodylen = ntohl((uint32_t)name.length());
    buffer->avail = buffer->size;
    memcpy(buffer->data + sizeof(req->bytes), name.c_str(), name.length());

    doInsertCommand(new SelectBucketResponseHandler(buffer, epStats));
}



void MemcachedEngine::tap(TapCallback &cb) {
    protocol_binary_request_tap_connect *req;

    Buffer *buffer = new Buffer(sizeof(req->bytes));
    req = (protocol_binary_request_tap_connect*)buffer->data;

    memset(buffer->data, 0, buffer->size);
    req->message.header.request.magic = PROTOCOL_BINARY_REQ;
    req->message.header.request.opcode = PROTOCOL_BINARY_CMD_TAP_CONNECT;
    req->message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    req->message.header.request.extlen = 4;
    req->message.header.request.bodylen = ntohl(4);
    req->message.body.flags = ntohl(TAP_CONNECT_FLAG_DUMP);
    buffer->avail = buffer->size;
    insertCommand(new TapResponseHandler(buffer, epStats, cb));
}

void MemcachedEngine::tap(const std::vector<uint16_t> &vbids,
                          bool full, TapCallback &cb)
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
    insertCommand(new TapResponseHandler(buffer, epStats, cb));
}

void MemcachedEngine::noop(Callback<bool> &cb) {
    protocol_binary_request_noop *req;
    Buffer *buffer = new Buffer(sizeof(req->bytes));
    req = (protocol_binary_request_noop *)buffer->data;

    memset(buffer->data, 0, buffer->size);
    req->message.header.request.magic = PROTOCOL_BINARY_REQ;
    req->message.header.request.opcode = PROTOCOL_BINARY_CMD_NOOP;
    req->message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    buffer->avail = buffer->size;
    insertCommand(new NoopResponseHandler(buffer, epStats, cb));
}
