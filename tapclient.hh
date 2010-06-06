/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef TAPCLIENT_HH
#define TAPCLIENT_HH 1

extern "C" {
    void* tapClientConnectionMain(void *arg);
}

#define CONNECTION_TIMEOUT 15
#define MAX_RECONNECT_INTERVAL 30
#define MIN_RECONNECT_INTERVAL 5
#define MAX_RECONNECT_ATTEMPTS 300

#ifdef linux
/* /usr/include/netinet/in.h defines macros from ntohs() to _bswap_nn to
 * optimize the conversion functions, but the prototypes generate warnings
 * from gcc. The conversion methods isn't the bottleneck for my app, so
 * just remove the warnings by undef'ing the optimization ..
 */
#undef ntohs
#undef ntohl
#undef htons
#undef htonl
#endif

// Forward decl
class BinaryMessage;
class EventuallyPersistentEngine;

/**
 * Tap client connection
 */
class TapClientConnection {
friend class EventuallyPersistentEngine;
friend void* tapClientConnectionMain(void *arg);

private:
    TapClientConnection(const std::string &n, uint32_t f,
                        EventuallyPersistentEngine *e) :
        running(false), peer(n), flags(f), connected(false), reconnects(0),
        failed(false), retry_interval(0), last_retry(0),
        connect_timeout(CONNECTION_TIMEOUT),
        sock(-1), ai(NULL), message(NULL), engine(e), terminate(false),
        backfillage(0), zombie(false)
    {
        char *backfill;
        if ((backfill = getenv("MEMCACHED_TAP_BACKFILL_AGE")) != NULL) {
            backfillage = strtoull(backfill, NULL, 10);
            flags |= TAP_CONNECT_FLAG_BACKFILL;
        }
    }

    ~TapClientConnection() {
        stop();
        if (ai != NULL) {
            freeaddrinfo(ai);
        }
    }

    void setFailed() {
        (void)close(sock);
        sock = -1;
        failed = true;
        connected = false;
        connect_timeout = CONNECTION_TIMEOUT;
        retry_interval = last_retry += MIN_RECONNECT_INTERVAL;
        if (retry_interval > MAX_RECONNECT_INTERVAL) {
            retry_interval = last_retry = MIN_RECONNECT_INTERVAL;
        }
    }

    bool shouldRetry() {
        if (reconnects++ > MAX_RECONNECT_ATTEMPTS) {
            return false;
        }

        return failed;
    }

    void resolve(void) throw (std::runtime_error);
    void createSocket(void) throw (std::runtime_error);
    bool connect(void) throw (std::runtime_error);
    void apply(void);
    void consume(void);

    void run();

    bool wait(short mask) throw (std::runtime_error);


    void start() {
        bool error = false;
        LockHolder lh(mutex);
        error = running;
        running = true;
        terminate = false;
        reapZombie();
        lh.unlock();

        if (error) {
            throw std::runtime_error("Already running");
        }

        if (pthread_create(&tid, NULL, tapClientConnectionMain, this) != 0) {
            throw std::runtime_error("Error creating tap client connection thread");
        }
    }

    void stop() {
        LockHolder lh(mutex);
        if (running) {
            terminate = true;
            lh.unlock();
            // @todo this will delay the client up to a sec...
            // I need to figure out a better way to do this, but we can't
            // leave the threads detached, because that could cause a race
            // condition during shutdown..
            pthread_join(tid, NULL);
            zombie = false;
        } else {
            reapZombie();
        }
    }

    void reapZombie() {
        if (zombie) {
            pthread_join(tid, NULL);
            zombie = false;
        }
    }

    bool running;

    /**
     * String used to identify the peer.
     */
    std::string peer;

    /**
     * Flags passed to the peer
     */
    uint32_t flags;

    /**
     * Is connected?
     */
    bool connected;

    /**
     * Number of reconnect attempts
     */
    int reconnects;

    /**
     * connection attempt failed
     */
    bool failed;

    /**
     * Next retry in seconds
     */
    int retry_interval;

    /**
     * retry interval in seconds
     */
    int last_retry;

    /** Numbers of second left before a connect timeout */
    int connect_timeout;

    /** The socket used for communication to our tap provider */
    int sock;

    /** Pointer to the address info structure used for this connection */
    struct addrinfo *ai;

    /** The next incomming message */
    BinaryMessage *message;

    /** The header of the message we're currently reading */
    protocol_binary_request_header header;

    /** Offset into the current message */
    size_t offset;

    /** Pointer to the engine we should notify with the tap events */
    EventuallyPersistentEngine *engine;

    /** Should we terminate ASAP or continue running */
    bool terminate;

    /** The backfill age we're interested in */
    uint64_t backfillage;

    /** Do we have a zombie thread laying around??  */
    bool zombie;

    /** A mutex used to synchronize between the core and this tap client */
    Mutex mutex;

    /** The thread id this tap client connection is running as */
    pthread_t tid;

    DISALLOW_COPY_AND_ASSIGN(TapClientConnection);
};

#endif
