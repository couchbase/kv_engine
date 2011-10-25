/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef MC_KVSTORE_MC_ENGINE_HH
#define MC_KVSTORE_MC_ENGINE_HH

#include <vector>
#include <queue>
#include <event.h>
#include "mutex.hh"
#include "configuration.hh"
#include "callbacks.hh"
#include "kvstore.hh"

/*
 * libevent2 define evutil_socket_t so that it'll automagically work
 * on windows
 */
#ifndef evutil_socket_t
#define evutil_socket_t int
#endif

extern "C" {
void *start_memcached_engine(void *);
void memcached_engine_libevent_callback(evutil_socket_t , short, void *);
void memcached_engine_notify_callback(evutil_socket_t , short, void *);
}

class Buffer {
public:
    char *data;
    size_t size;
    size_t avail;
    size_t curr;

    Buffer() : data(NULL), size(0), avail(0), curr(0) { /* EMPTY */ }

    Buffer(size_t s) : data((char*)malloc(s)), size(s), avail(s), curr(0)
    {
        assert(data != NULL);
    }

    Buffer(const Buffer &other) :
        data((char*)malloc(other.size)), size(other.size), avail(other.avail),
        curr(other.curr)
    {
        assert(data != NULL);
        memcpy(data, other.data, size);
    }

    ~Buffer() {
        free(data);
    }

    bool grow() {
        return grow(8192);
    }

    bool grow(size_t minFree) {
        if (minFree == 0) {
            // no minimum size requested, just ensure that there is at least
            // one byte there...
            minFree = 1;
        }

        if (size - avail < minFree) {
            size_t next = size ? size << 1 : 8192;
            char *ptr;

            while ((next - avail) < minFree) {
                next <<= 1;
            }

            ptr = (char*)realloc(data, next);
            if (ptr == NULL) {
                return false;
            }
            data = ptr;
            size = next;
        }

        return true;
    }
};

class BinaryPacketHandler;
class SelectBucketResponseHandler;

class TapCallback {
public:
    TapCallback(shared_ptr<Callback<GetValue> > &data, shared_ptr<RememberingCallback<bool> > &w) :
        cb(data), complete(w) {
    }

    shared_ptr<Callback<GetValue> > cb;
    shared_ptr<RememberingCallback<bool> > complete;
};

class MemcachedEngine {
public:
    MemcachedEngine(EventuallyPersistentEngine *engine, Configuration &config);

    void start();

    ~MemcachedEngine();

    void flush(Callback<bool> &cb);
    void setmq(const Item &item, Callback<mutation_result> &cb);
    void get(const std::string &key, uint16_t vb, Callback<GetValue> &cb);
    void delq(const std::string &key, uint16_t vb, Callback<int> &cb);
    void stats(const std::string &key,
               Callback<std::map<std::string, std::string> > &cb);
    void setVBucket(uint16_t vb, vbucket_state_t state, Callback<bool> &cb);
    void delVBucket(uint16_t vb, Callback<bool> &cb);

    // Set a bunch of vbuckets in a single operation
    void snapshotVBuckets(const vbucket_map_t &m, Callback<bool> &cb);

    void tap(shared_ptr<TapCallback> cb);
    void tapKeys(shared_ptr<TapCallback> cb);
    void tap(const std::vector<uint16_t> &vbids, bool full, shared_ptr<TapCallback> cb);
    void noop(Callback<bool> &cb);

    void addStats(const std::string &prefix,
                  ADD_STAT add_stat,
                  const void *c);

protected:
    friend class SelectBucketResponseHandler;
    friend void *start_memcached_engine(void *arg);
    friend void memcached_engine_libevent_callback(evutil_socket_t sock,
                                                   short which, void *arg);
    friend void memcached_engine_notify_callback(evutil_socket_t sock,
                                                 short which, void *arg);
    void run();
    void libeventCallback(evutil_socket_t s, short which);
    void notifyHandler(evutil_socket_t s, short which);

private:
    template <typename T>
    void addStat(const std::string &prefix, const char *nm, T val, ADD_STAT add_stat, const void *c) {
        std::stringstream name;
        name << prefix << ":" << nm;
        std::stringstream value;
        value << val;
        std::string n = name.str();
        add_stat(n.data(), static_cast<uint16_t>(n.length()),
                 value.str().data(),
                 static_cast<uint32_t>(value.str().length()),
                 c);
    }

    void doSelectBucket(void);
    void reschedule(std::list<BinaryPacketHandler*> &packets);
    void resetConnection();

    void updateEvent(evutil_socket_t s);
    void receiveData(evutil_socket_t s);
    void sendData(evutil_socket_t s);

    Buffer *nextToSend();
    void insertCommand(BinaryPacketHandler *rh);
    void doInsertCommand(BinaryPacketHandler *rh);

    void handleResponse(protocol_binary_response_header *res);
    void handleRequest(protocol_binary_request_header *req);

    void notify();

    bool connect();

    bool createNotificationPipe();

    evutil_socket_t notifyPipe[2];
    evutil_socket_t sock;

    pthread_t threadid;
    Configuration &configuration;
    bool configurationError;
    bool shutdown;

    /** The event base this instance is connected to */
    struct event_base *ev_base;
    /** The event item representing the memcached server object */
    struct event ev_event;
    /** The current set of flags to the memcached server */
    short ev_flags;

    /** The event item representing the notify pipe */
    struct event ev_notify;

    uint32_t seqno;
    Buffer *output;
    Buffer input;

    /**
     * The current command in transit (set to 0xff when no command is in
     * transit. Please note that this is read and written completely
     * dirty, so you may not trust the value ;)
     */
    volatile uint8_t currentCommand;
    volatile uint8_t lastSentCommand;
    volatile uint8_t lastReceivedCommand;

    const char *cmd2str(uint8_t cmd);

    /**
     * Structure used "per command"
     */
    class CommandStats {
    public:
        CommandStats() : numSent(0), numSuccess(0),
                         numImplicit(), numError(0) { }
        volatile size_t numSent;
        volatile size_t numSuccess;
        volatile size_t numImplicit;
        volatile size_t numError;

        void addStat(const std::string &prefix, const char *nm, size_t val, ADD_STAT add_stat, const void *c) {
            std::stringstream name;
            name << prefix << ":" << nm;
            std::stringstream value;
            value << val;
            std::string n = name.str();
            add_stat(n.data(), static_cast<uint16_t>(n.length()),
                     value.str().data(),
                     static_cast<uint32_t>(value.str().length()),
                     c);
        }

        void addStats(const std::string &prefix,
                      const char *cmd,
                      ADD_STAT add_stat,
                      const void *c) {
            if (numSent > 0 || numSuccess > 0 ||
                numImplicit != 0 || numError != 0)
            {
                if (strcmp(cmd, "unknown") == 0) {
                    abort();
                };

                std::stringstream name;
                name << prefix << ":" << cmd;
                addStat(name.str(), "sent", numSent, add_stat, c);
                addStat(name.str(), "success", numSuccess, add_stat, c);
                addStat(name.str(), "implicit", numImplicit, add_stat, c);
                addStat(name.str(), "error", numError, add_stat, c);
            }
        }
    } commandStats[0xff]; // @todo make this map smaller.. we only use
    // a subset of the packets...

    Mutex mutex;
    std::queue<Buffer*> commandQueue;
    std::list<BinaryPacketHandler*> responseHandler;
    std::list<BinaryPacketHandler*> tapHandler;
    EventuallyPersistentEngine *engine;
    EPStats *epStats;
};

#endif /* MC_ENGINE_HH */
