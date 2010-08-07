/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef EP_ENGINE_H
#define EP_ENGINE_H 1

#include "locks.hh"
#include "ep.hh"
#include "flusher.hh"
#include "sqlite-kvstore.hh"
#include "ep_extension.h"
#include "dispatcher.hh"
#include "item_pager.hh"
#include <memcached/util.h>
#ifdef ENABLE_INTERNAL_TAP
#include "tapclient.hh"
#endif

#include <cstdio>
#include <map>
#include <list>
#include <sstream>
#include <algorithm>
#include <errno.h>
#include <limits>

#include "command_ids.h"

#define NUMBER_OF_SHARDS 4
#define DEFAULT_TAP_IDLE_TIMEOUT 600

#ifndef DEFAULT_MIN_DATA_AGE
#define DEFAULT_MIN_DATA_AGE 120
#endif

#ifndef DEFAULT_QUEUE_AGE_CAP
#define DEFAULT_QUEUE_AGE_CAP 900
#endif

extern "C" {
    EXPORT_FUNCTION
    ENGINE_ERROR_CODE create_instance(uint64_t interface,
                                      GET_SERVER_API get_server_api,
                                      ENGINE_HANDLE **handle);
    void EvpHandleDisconnect(const void *cookie,
                             ENGINE_EVENT_TYPE type,
                             const void *event_data,
                             const void *cb_data);
    void *EvpNotifyTapIo(void*arg);
    void EvpHandleTapCallback(const void *cookie,
                              ENGINE_EVENT_TYPE type,
                              const void *event_data,
                              const void *cb_data);
    void EvpClockHandler(const void *cookie,
                         ENGINE_EVENT_TYPE type,
                         const void *event_data,
                         const void *cb_data);
}

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

class BoolCallback : public Callback<bool> {
public:
    void callback(bool &theValue) {
        value = theValue;
    }

    bool getValue() {
        return value;
    }

private:
    bool value;
};

class BoolPairCallback : public Callback<std::pair<bool, int64_t> > {
public:
    void callback(std::pair<bool, int64_t> &theValue) {
        value = theValue.first;
    }

    bool getValue() {
        return value;
    }

private:
    bool value;
};

// Forward decl
class BinaryMessage;
class EventuallyPersistentEngine;

class LookupCallback : public Callback<GetValue> {
public:
    LookupCallback(EventuallyPersistentEngine *e, const void* c) :
        engine(e), cookie(c) {}

    virtual void callback(GetValue &value);
private:
    EventuallyPersistentEngine *engine;
    const void *cookie;
};

static void add_casted_stat(const char *k, const char *v,
                            ADD_STAT add_stat, const void *cookie) {
    add_stat(k, static_cast<uint16_t>(strlen(k)),
             v, static_cast<uint32_t>(strlen(v)), cookie);
}

static void add_casted_stat(const char *k, uint64_t v,
                            ADD_STAT add_stat, const void *cookie) {
    std::stringstream vals;
    vals << v;
    add_casted_stat(k, vals.str().c_str(), add_stat, cookie);
}

template <typename T>
static void add_casted_stat(const char *k, const Atomic<T> &v,
                            ADD_STAT add_stat, const void *cookie) {
    add_casted_stat(k, v.get(), add_stat, cookie);
}

class StatVBucketVisitor : public VBucketVisitor {
public:
    StatVBucketVisitor(const void *c, ADD_STAT a) : cookie(c), add_stat(a) {}

    bool visitBucket(uint16_t vbid, vbucket_state_t state) {
        char buf[16];
        snprintf(buf, sizeof(buf), "vb_%d", vbid);
        add_casted_stat(buf, VBucket::toString(state), add_stat, cookie);
        return false;
    }

    void visit(StoredValue* v) {
        (void)v;
        assert(false); // this does not happen
    }

private:
    const void *cookie;
    ADD_STAT add_stat;
};

/**
 * The tap stream may include other events than data mutation events,
 * but the data structures in the TapConnection does only store a key
 * for the item to store. We don't want to add more data to those elements,
 * because that could potentially consume a lot of memory (the tap queue
 * may have a lot of elements).
 */
class TapVBucketEvent {
public:
    /**
     * Create a new instance of the TapVBucketEvent and initialize
     * its members.
     * @param ev Type of event
     * @param b The bucket this event belongs to
     * @param s The state change for this event
     */
    TapVBucketEvent(tap_event_t ev, uint16_t b, vbucket_state_t s) :
        event(ev), vbucket(b), state(s) {}
    tap_event_t event;
    uint16_t vbucket;
    vbucket_state_t state;
};

/**
 * Class used by the EventuallyPersistentEngine to keep track of all
 * information needed per Tap connection.
 */
class TapConnection {
    friend class EventuallyPersistentEngine;
    friend class BackFillVisitor;
private:
    /**
     * Add a new item to the tap queue.
     * The item may be ignored if the TapConnection got a vbucket filter
     * associated and the item's vbucket isn't part of the filter.
     *
     * @return true if the the queue was empty
     */
    bool addEvent(const QueuedItem &it) {
        if (vbucketFilter(it.getVBucketId())) {
            bool wasEmpty = queue->empty();
            std::pair<std::set<QueuedItem>::iterator, bool> ret;
            ret = queue_set->insert(it);
            if (ret.second) {
                queue->push_back(it);
            }
            return wasEmpty;
        } else {
            return queue->empty();
        }
    }

    /**
     * Add a key to the tap queue.
     * @return true if the the queue was empty
     */
    bool addEvent(const std::string &key, uint16_t vbid, enum queue_operation op) {
        return addEvent(QueuedItem(key, vbid, op));
    }

    QueuedItem next() {
        assert(!empty());
        QueuedItem qi = queue->front();
        queue->pop_front();
        queue_set->erase(qi);
        ++recordsFetched;
        return qi;
    }

    /**
     * Add a new high priority TapVBucketEvent to this TapConnection. A high
     * priority TapVBucketEvent will bypass the the normal queue of events to
     * be sent to the client, and be sent the next time it is possible to
     * send data over the tap connection.
     */
    void addVBucketHighPriority(TapVBucketEvent &ev) {
        vBucketHighPriority.push(ev);
    }

    /**
     * Get the next high priority TapVBucketEvent for this TapConnection.
     */
    TapVBucketEvent nextVBucketHighPriority() {
        TapVBucketEvent ret(TAP_PAUSE, 0, active);
        if (!vBucketHighPriority.empty()) {
            ret = vBucketHighPriority.front();
            vBucketHighPriority.pop();
            ++recordsFetched;
        }
        return ret;
    }

    /**
     * Add a new low priority TapVBucketEvent to this TapConnection. A low
     * priority TapVBucketEvent will only be sent when the tap connection
     * doesn't have any other events to send.
     */
    void addVBucketLowPriority(TapVBucketEvent &ev) {
        vBucketLowPriority.push(ev);
    }

    /**
     * Get the next low priority TapVBucketEvent for this TapConnection.
     */
    TapVBucketEvent nextVBucketLowPriority() {
        TapVBucketEvent ret(TAP_PAUSE, 0, active);
        if (!vBucketLowPriority.empty()) {
            ret = vBucketLowPriority.front();
            vBucketLowPriority.pop();
            ++recordsFetched;
        }
        return ret;
    }

    bool idle() {
        return queue->empty() && vBucketLowPriority.empty() && vBucketHighPriority.empty();
    }

    bool empty() {
        return queue->empty();
    }

    void flush() {
        pendingFlush = true;
        /* No point of keeping the rep queue when someone wants to flush it */
        queue->clear();
        queue_set->clear();
    }

    bool shouldFlush() {
        bool ret = pendingFlush;
        pendingFlush = false;
        return ret;
    }

    // This method is called while holding the tapNotifySync lock.
    void appendQueue(std::list<QueuedItem> *q) {
        queue->splice(queue->end(), *q);
    }

    void completeBackfill() {
        pendingBackfill = false;

        if (complete() && idle()) {
            // There is no data for this connection..
            // Just go ahead and disconnect it.
            doDisconnect = true;
        }
    }

    bool complete(void) {
        return dumpQueue && empty() && !pendingBackfill;
    }

    TapConnection(const std::string &n, uint32_t f):
        client(n), queue(NULL), queue_set(NULL), flags(f),
        recordsFetched(0), pendingFlush(false), expiry_time((rel_time_t)-1),
        reconnects(0), connected(true), paused(false), backfillAge(0),
        doRunBackfill(false), pendingBackfill(true), vbucketFilter(),
        vBucketHighPriority(), vBucketLowPriority(), doDisconnect(false)
    {
        queue = new std::list<QueuedItem>;
        queue_set = new std::set<QueuedItem>;
    }

    ~TapConnection() {
        delete queue;
        delete queue_set;
    }

    void encodeVBucketStateTransition(const TapVBucketEvent &ev, void **es,
                                      uint16_t *nes, uint16_t *vbucket) const;

    static uint64_t nextTapId() {
        return tapCounter++;
    }

    static std::string getAnonTapName() {
        std::stringstream s;
        s << "eq_tapq:anon_";
        s << TapConnection::nextTapId();
        return s.str();
    }


    /**
     * String used to identify the client.
     * @todo design the connect packet and fill inn som info here
     */
    std::string client;
    /**
     * The queue of keys that needs to be sent (this is the "live stream")
     */
    std::list<QueuedItem> *queue;
    /**
     * Set to prevent duplicate queue entries.
     *
     * Note that stl::set is O(log n) for ops we care about, so we'll
     * want to look out for this.
     */
    std::set<QueuedItem> *queue_set;
    /**
     * Flags passed by the client
     */
    uint32_t flags;
    /**
     * Counter of the number of records fetched from this stream since the
     * beginning
     */
    size_t recordsFetched;
    /**
     * Do we have a pending flush command?
     */
    bool pendingFlush;

    /**
     * when this tap conneciton expires.
     */
    rel_time_t expiry_time;

    /**
     * Number of times this client reconnected
     */
    uint32_t reconnects;

    /**
     * Is connected?
     */
    bool connected;

    /**
     * is his paused
     */
    bool paused;

    /**
     * Backfill age for the connection
     */
    uint64_t backfillAge;


    /**
     * Dump and disconnect?
     */
    bool dumpQueue;

    /**
     * We don't want to do the backfill in the thread used by the client,
     * because that would block all clients bound to the same thread.
     * Instead we run the backfill the first time we try to walk the
     * stream (that would be in the TAP thread). This would cause the other
     * tap streams to block, but allows all clients to use the cache.
     */
    bool doRunBackfill;

    // True until a backfill has dumped all the content.
    bool pendingBackfill;

    /**
     * Filter for the buckets we want.
     */
    VBucketFilter vbucketFilter;

    /**
     * VBucket status messages immediately (before userdata)
     */
    std::queue<TapVBucketEvent> vBucketHighPriority;
    /**
     * VBucket status messages sent when there is nothing else to send
     */
    std::queue<TapVBucketEvent> vBucketLowPriority;

    static Atomic<uint64_t> tapCounter;

    // True if this should be disconnected as soon as possible
    bool doDisconnect;

    DISALLOW_COPY_AND_ASSIGN(TapConnection);
};

static size_t percentOf(size_t val, double percent) {
    return static_cast<size_t>(static_cast<double>(val) * percent);
}

/**
 *
 */
class EventuallyPersistentEngine : public ENGINE_HANDLE_V1 {
    friend class LookupCallback;
public:
    ENGINE_ERROR_CODE initialize(const char* config)
    {
        ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
#ifdef ENABLE_INTERNAL_TAP
        char *master = NULL;
        char *tap_id = NULL;
#endif

        resetStats();

        if (config != NULL) {
            char *dbn = NULL, *initf = NULL, *svaltype = NULL;
            size_t htBuckets = 0;
            size_t htLocks = 0;
            size_t maxSize = 0;

            const int max_items = 20;
            struct config_item items[max_items];
            int ii = 0;
            memset(items, 0, sizeof(items));

            items[ii].key = "dbname";
            items[ii].datatype = DT_STRING;
            items[ii].value.dt_string = &dbn;

            ++ii;
            items[ii].key = "initfile";
            items[ii].datatype = DT_STRING;
            items[ii].value.dt_string = &initf;

            ++ii;
            items[ii].key = "warmup";
            items[ii].datatype = DT_BOOL;
            items[ii].value.dt_bool = &warmup;

            ++ii;
            items[ii].key = "waitforwarmup";
            items[ii].datatype = DT_BOOL;
            items[ii].value.dt_bool = &wait_for_warmup;

            ++ii;
            items[ii].key = "vb0";
            items[ii].datatype = DT_BOOL;
            items[ii].value.dt_bool = &startVb0;

            ++ii;
            items[ii].key = "tap_keepalive";
            items[ii].datatype = DT_SIZE;
            items[ii].value.dt_size = &tapKeepAlive;

            ++ii;
            items[ii].key = "ht_size";
            items[ii].datatype = DT_SIZE;
            items[ii].value.dt_size = &htBuckets;

            ++ii;
            items[ii].key = "stored_val_type";
            items[ii].datatype = DT_STRING;
            items[ii].value.dt_string = &svaltype;

            ++ii;
            items[ii].key = "ht_locks";
            items[ii].datatype = DT_SIZE;
            items[ii].value.dt_size = &htLocks;

            ++ii;
            items[ii].key = "max_size";
            items[ii].datatype = DT_SIZE;
            items[ii].value.dt_size = &maxSize;

#ifdef ENABLE_INTERNAL_TAP
            ++ii;
            items[ii].key = "tap_peer";
            items[ii].datatype = DT_STRING;
            items[ii].value.dt_string = &master;

            ++ii;
            items[ii].key = "tap_id";
            items[ii].datatype = DT_STRING;
            items[ii].value.dt_string = &tap_id;
#endif

            ++ii;
            items[ii].key = "tap_idle_timeout";
            items[ii].datatype = DT_SIZE;
            items[ii].value.dt_size = &tapIdleTimeout;

            ++ii;
            items[ii].key = "config_file";
            items[ii].datatype = DT_CONFIGFILE;

            ++ii;
            items[ii].key = "max_item_size";
            items[ii].datatype = DT_SIZE;
            items[ii].value.dt_size = &maxItemSize;

            ++ii;
            items[ii].key = "min_data_age";
            items[ii].datatype = DT_SIZE;
            items[ii].value.dt_size = &minDataAge;

            ++ii;
            items[ii].key = "mem_low_wat";
            items[ii].datatype = DT_SIZE;
            items[ii].value.dt_size = &memLowWat;

            ++ii;
            items[ii].key = "mem_high_wat";
            items[ii].datatype = DT_SIZE;
            items[ii].value.dt_size = &memHighWat;

            ++ii;
            items[ii].key = "queue_age_cap";
            items[ii].datatype = DT_SIZE;
            items[ii].value.dt_size = &queueAgeCap;

            ++ii;
            items[ii].key = NULL;

            assert(ii < max_items);

            if (serverApi->core->parse_config(config, items, stderr) != 0) {
                ret = ENGINE_FAILED;
            } else {
                if (dbn != NULL) {
                    dbname = dbn;
                }
                if (initf != NULL) {
                    initFile = initf;
                }
                HashTable::setDefaultNumBuckets(htBuckets);
                HashTable::setDefaultNumLocks(htLocks);
                StoredValue::setMaxDataSize(stats, maxSize);

                if (svaltype && !HashTable::setDefaultStorageValueType(svaltype)) {
                    getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                     "Unhandled storage value type: %s",
                                     svaltype);
                }
            }
        }

        if (tapIdleTimeout == 0) {
            tapIdleTimeout = (size_t)-1;
        }

        if (ret == ENGINE_SUCCESS) {
            time_t start = time(NULL);
            try {
                MultiDBSqliteStrategy *strategy =
                    new MultiDBSqliteStrategy(*this, dbname,
                                              initFile,
                                              NUMBER_OF_SHARDS);
                sqliteDb = new StrategicSqlite3(*this, strategy);
            } catch (std::exception& e) {
                std::stringstream ss;
                ss << "Failed to create database: " << e.what() << std::endl;
                if (!dbAccess()) {
                    ss << "No access to \"" << dbname << "\"."
                       << std::endl;
                }

                getLogger()->log(EXTENSION_LOG_WARNING, NULL, "%s",
                                 ss.str().c_str());
                return ENGINE_FAILED;
            }

            databaseInitTime = time(NULL) - start;
            epstore = new EventuallyPersistentStore(*this, sqliteDb, startVb0);
            setMinDataAge(minDataAge);
            setQueueAgeCap(queueAgeCap);

            if (epstore == NULL) {
                ret = ENGINE_ENOMEM;
            } else {
                if (!warmup) {
                    epstore->reset();
                }

                SERVER_CALLBACK_API *sapi;
                sapi = getServerApi()->callback;
                sapi->register_callback(reinterpret_cast<ENGINE_HANDLE*>(this),
                                        ON_DISCONNECT, EvpHandleDisconnect, this);
            }
            startEngineThreads();

            // If requested, don't complete the initialization until the
            // flusher transitions out of the initializing state (i.e
            // warmup is finished).
            const Flusher *flusher = epstore->getFlusher();
            if (wait_for_warmup && flusher) {
                while (flusher->state() == initializing) {
                    sleep(1);
                }
            }

#ifdef ENABLE_INTERNAL_TAP
            if (tap_id != NULL) {
                tapId.assign(tap_id);
                free(tap_id);
            }

            if (master != NULL) {
                setTapPeer(master);
                free(master);
            }
#endif

            if (memLowWat == std::numeric_limits<size_t>::max()) {
                memLowWat = percentOf(StoredValue::getMaxDataSize(stats), 0.6);
            }
            if (memHighWat == std::numeric_limits<size_t>::max()) {
                memHighWat = percentOf(StoredValue::getMaxDataSize(stats), 0.75);
            }

            stats.mem_low_wat = memLowWat;
            stats.mem_high_wat = memHighWat;

            shared_ptr<DispatcherCallback> cb(new ItemPager(epstore, stats,
                                                            memLowWat, memHighWat));
            epstore->getDispatcher()->schedule(cb, NULL, 5, 10);
        }

        if (ret == ENGINE_SUCCESS) {
            getlExtension = new GetlExtension(epstore, getServerApi);
            getlExtension->initialize();
        }

        getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "Engine init complete.\n");

        return ret;
    }

    void destroy()
    {
#ifdef ENABLE_INTERNAL_TAP
        stopReplication();
#endif
        stopEngineThreads();
    }

    ENGINE_ERROR_CODE itemAllocate(const void* cookie,
                                   item** item,
                                   const void* key,
                                   const size_t nkey,
                                   const size_t nbytes,
                                   const int flags,
                                   const rel_time_t exptime)
    {
        (void)cookie;
        if (nbytes > maxItemSize) {
            return ENGINE_E2BIG;
        }

        *item = new Item(key, nkey, nbytes, flags, exptime);
        if (*item == NULL) {
            return ENGINE_ENOMEM;
        } else {
            return ENGINE_SUCCESS;
        }
    }

    ENGINE_ERROR_CODE itemDelete(const void* cookie,
                                 const void* key,
                                 const size_t nkey,
                                 uint64_t cas,
                                 uint16_t vbucket)
    {
        (void)cas;
        (void)vbucket;
        std::string k(static_cast<const char*>(key), nkey);
        return itemDelete(cookie, k, vbucket);
    }

    ENGINE_ERROR_CODE itemDelete(const void* cookie,
                                 const std::string &key,
                                 uint16_t vbucket)
    {
        ENGINE_ERROR_CODE ret = epstore->del(key, vbucket, cookie);

        if (ret == ENGINE_SUCCESS) {
            addDeleteEvent(key, vbucket);
        }
        return ret;
    }


    void itemRelease(const void* cookie, item *item)
    {
        (void)cookie;
        delete (Item*)item;
    }

    ENGINE_ERROR_CODE get(const void* cookie,
                          item** item,
                          const void* key,
                          const int nkey,
                          uint16_t vbucket)
    {
        std::string k(static_cast<const char*>(key), nkey);

        GetValue gv(epstore->get(k, vbucket, cookie, serverApi->core));

        if (gv.getStatus() == ENGINE_SUCCESS) {
            *item = gv.getValue();
        }

        return gv.getStatus();

    }

    ENGINE_ERROR_CODE getStats(const void* cookie,
                               const char* stat_key,
                               int nkey,
                               ADD_STAT add_stat)
    {
        ENGINE_ERROR_CODE rv = ENGINE_KEY_ENOENT;
        if (stat_key == NULL) {
            rv = doEngineStats(cookie, add_stat);
        } else if (nkey == 3 && strncmp(stat_key, "tap", 3) == 0) {
            rv = doTapStats(cookie, add_stat);
        } else if (nkey == 4 && strncmp(stat_key, "hash", 3) == 0) {
            rv = doHashStats(cookie, add_stat);
        } else if (nkey == 7 && strncmp(stat_key, "vbucket", 7) == 0) {
            rv = doVBucketStats(cookie, add_stat);
        } else if (nkey > 4 && strncmp(stat_key, "key ", 4) == 0) {
            // Non-validating, non-blocking version
            rv = doKeyStats(cookie, add_stat, &stat_key[4], nkey-4, false);
        } else if (nkey > 5 && strncmp(stat_key, "vkey ", 5) == 0) {
            // Validating version; blocks
            rv = doKeyStats(cookie, add_stat, &stat_key[5], nkey-5, true);
        }

        return rv;
    }

    ENGINE_ERROR_CODE store(const void *cookie,
                            item* itm,
                            uint64_t *cas,
                            ENGINE_STORE_OPERATION operation,
                            uint16_t vbucket)
    {
        ENGINE_ERROR_CODE ret;
        Item *it = static_cast<Item*>(itm);
        item *i = NULL;

        it->setVBucketId(vbucket);

        switch (operation) {
        case OPERATION_CAS:
            if (it->getCas() == 0) {
                // Using a cas command with a cas wildcard doesn't make sense
                ret = ENGINE_NOT_STORED;
                break;
            }
            // FALLTHROUGH
        case OPERATION_SET:
            ret = epstore->set(*it, cookie);
            if (ret == ENGINE_SUCCESS) {
                *cas = it->getCas();
                addMutationEvent(it, vbucket);
            }

            break;

        case OPERATION_ADD:
            // @todo this isn't atomic!
            if (get(cookie, &i, it->getKey().c_str(), it->getNKey(),
                    vbucket) == ENGINE_SUCCESS) {
                itemRelease(cookie, i);
                ret = ENGINE_NOT_STORED;
            } else {
                ret = epstore->set(*it, cookie);
                *cas = it->getCas();
                if (ret == ENGINE_SUCCESS) {
                    addMutationEvent(it, vbucket);
                }
            }
            break;

        case OPERATION_REPLACE:
            // @todo this isn't atomic!
            ret = get(cookie, &i, it->getKey().c_str(),
                      it->getNKey(), vbucket);
            switch (ret) {
            case ENGINE_SUCCESS:
                itemRelease(cookie, i);
                ret = epstore->set(*it, cookie);
                if (ret == ENGINE_SUCCESS) {
                    *cas = it->getCas();
                    addMutationEvent(it, vbucket);
                }
                break;
            case ENGINE_KEY_ENOENT:
                ret = ENGINE_NOT_STORED;
                break;
            default:
                // Just return the error we got.
                break;
            }
            break;
        case OPERATION_APPEND:
        case OPERATION_PREPEND:
            do {
                if ((ret = get(cookie, &i, it->getKey().c_str(),
                               it->getNKey(), vbucket)) == ENGINE_SUCCESS) {
                    Item *old = reinterpret_cast<Item*>(i);

                    if (operation == OPERATION_APPEND) {
                        if (!old->append(*it)) {
                            itemRelease(cookie, i);
                            return ENGINE_ENOMEM;
                        }
                    } else {
                        if (!old->prepend(*it)) {
                            itemRelease(cookie, i);
                            return ENGINE_ENOMEM;
                        }
                    }

                    ret = store(cookie, old, cas, OPERATION_CAS, vbucket);
                    if (ret == ENGINE_SUCCESS) {
                        addMutationEvent(static_cast<Item*>(i), vbucket);
                    }
                    itemRelease(cookie, i);
                }
            } while (ret == ENGINE_KEY_EEXISTS);

            // Map the error code back to what memcacpable expects
            if (ret == ENGINE_KEY_ENOENT) {
                ret = ENGINE_NOT_STORED;
            }
            break;

        default:
            ret = ENGINE_ENOTSUP;
        }

        return ret;
    }

    ENGINE_ERROR_CODE arithmetic(const void* cookie,
                                 const void* key,
                                 const int nkey,
                                 const bool increment,
                                 const bool create,
                                 const uint64_t delta,
                                 const uint64_t initial,
                                 const rel_time_t exptime,
                                 uint64_t *cas,
                                 uint64_t *result,
                                 uint16_t vbucket)
    {
        item *it = NULL;

        ENGINE_ERROR_CODE ret = get(cookie, &it, key, nkey, vbucket);
        if (ret == ENGINE_SUCCESS) {
            Item *item = static_cast<Item*>(it);
            char *endptr = NULL;
            char data[24];
            size_t len = std::min(static_cast<uint32_t>(sizeof(data) - 1),
                                  item->getNBytes());
            data[len] = 0;
            memcpy(data, item->getData(), len);
            uint64_t val = strtoull(data, &endptr, 10);
            if ((errno != ERANGE) && (isspace(*endptr)
                                      || (*endptr == '\0' && endptr != data))) {
                if (increment) {
                    val += delta;
                } else {
                    if (delta > val) {
                        val = 0;
                    } else {
                        val -= delta;
                    }
                }

                std::stringstream vals;
                vals << val << "\r\n";
                size_t nb = vals.str().length();
                *result = val;
                Item *nit = new Item(key, (uint16_t)nkey, item->getFlags(),
                                     exptime, vals.str().c_str(), nb);
                nit->setCas(item->getCas());
                ret = store(cookie, nit, cas, OPERATION_CAS, vbucket);
                delete nit;
            } else {
                ret = ENGINE_EINVAL;
            }

            delete item;
        } else if (ret == ENGINE_NOT_MY_VBUCKET) {
            return ret;
        } else if (ret == ENGINE_KEY_ENOENT && create) {
            std::stringstream vals;

            vals << initial << "\r\n";
            size_t nb = vals.str().length();

            *result = initial;
            Item *item = new Item(key, (uint16_t)nkey, 0, exptime,
                                  vals.str().c_str(), nb);
            ret = store(cookie, item, cas, OPERATION_ADD, vbucket);
            delete item;
        }

        /* We had a race condition.. just call ourself recursively to retry */
        if (ret == ENGINE_KEY_EEXISTS) {
            return arithmetic(cookie, key, nkey, increment, create, delta,
                              initial, exptime, cas, result, vbucket);
        }

        return ret;
    }



    ENGINE_ERROR_CODE flush(const void *cookie, time_t when)
    {
        (void)cookie;
        ENGINE_ERROR_CODE ret= ENGINE_ENOTSUP;

        if (when == 0) {
            epstore->reset();
            addFlushEvent();
            ret = ENGINE_SUCCESS;
        }

        return ret;
    }

    tap_event_t walkTapQueue(const void *cookie, item **itm, void **es,
                             uint16_t *nes, uint8_t *ttl, uint16_t *flags,
                             uint32_t *seqno, uint16_t *vbucket) {
        LockHolder lh(tapNotifySync);
        std::map<const void*, TapConnection*>::iterator iter;
        TapConnection *connection = NULL;
        iter = tapConnectionMap.find(cookie);
        if (iter == tapConnectionMap.end()) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Walking a non-existent tap queue, disconnecting\n");
            return TAP_DISCONNECT;
        } else {
            connection = iter->second;
        }

        if (connection->doDisconnect) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Disconnecting pending connection\n");
            return TAP_DISCONNECT;
        }

        if (connection->doRunBackfill) {
            queueBackfill(connection, cookie);
        }

        tap_event_t ret = TAP_PAUSE;

        *es = NULL;
        *nes = 0;
        *ttl = (uint8_t)-1;
        *seqno = 0;
        *flags = 0;

        TapVBucketEvent ev = connection->nextVBucketHighPriority();
        if (ev.event != TAP_PAUSE) {
            assert(ev.event == TAP_VBUCKET_SET || ev.event == TAP_NOOP);
            connection->encodeVBucketStateTransition(ev, es, nes, vbucket);
            return ev.event;
        }

        QueuedItem *qip;
        qip = reinterpret_cast<QueuedItem*>(serverApi->core->get_engine_specific(cookie));
        if (qip != NULL || !connection->empty()) {
            QueuedItem qi("", 0, queue_op_set);
            if (qip != NULL) {
                qi = *qip;
                delete qip;
                serverApi->core->store_engine_specific(cookie, NULL);
            } else {
                qi = connection->next();
            }
            lh.unlock();

            ENGINE_ERROR_CODE r;
            *vbucket = qi.getVBucketId();
            std::string key = qi.getKey();
            r = get(cookie, itm, key.c_str(), (int)key.length(), qi.getVBucketId());
            if (r == ENGINE_SUCCESS) {
                ret = TAP_MUTATION;
            } else if (r == ENGINE_KEY_ENOENT) {
                ret = TAP_DELETION;
                r = itemAllocate(cookie, itm,
                                 key.c_str(), key.length(), 0, 0, 0);
                if (r != ENGINE_SUCCESS) {
                    EXTENSION_LOGGER_DESCRIPTOR *logger;
                    logger = (EXTENSION_LOGGER_DESCRIPTOR*)serverApi->extension->get_extension(EXTENSION_LOGGER);

                    getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                     "Failed to allocate memory for deletion of: %s\n", key.c_str());
                    ret = TAP_PAUSE;
                }
            } else if (r == ENGINE_EWOULDBLOCK) {
                qip = new QueuedItem(qi);
                serverApi->core->store_engine_specific(cookie, qip);
                connection->paused = true;
                return TAP_PAUSE;
            }
        } else if (connection->shouldFlush()) {
            ret = TAP_FLUSH;
        }

        if (ret == TAP_PAUSE && connection->complete()) {
            ev = connection->nextVBucketLowPriority();
            if (ev.event != TAP_PAUSE) {
                assert(ev.event == TAP_VBUCKET_SET);
                connection->paused = false;
                connection->encodeVBucketStateTransition(ev, es, nes, vbucket);
                if (ev.state == active) {
                    epstore->setVBucketState(ev.vbucket, dead, serverApi->core);
                }
                ret = ev.event;
            } else {
                ret = TAP_DISCONNECT;
            }
        }

        connection->paused = ret == TAP_PAUSE;
        return ret;
    }

    void createTapQueue(const void *cookie, std::string &client, uint32_t flags,
                        const void *userdata,
                        size_t nuserdata) {
        // map is set-assocative, so this will create an instance here..
        LockHolder lh(tapNotifySync);
        purgeExpiredTapConnections_UNLOCKED();
        char cookiehex[32];
        snprintf(cookiehex, sizeof(cookiehex), "%p", cookie);

        std::string name = "eq_tapq:";
        if (client.length() == 0) {
            name.assign(TapConnection::getAnonTapName());
        } else {
            name.append(client);
        }

        TapConnection *tap = NULL;

        std::list<TapConnection*>::iterator iter;
        for (iter = allTaps.begin(); iter != allTaps.end(); ++iter) {
            tap = *iter;
            if (tap->client == name) {
                tap->expiry_time = (rel_time_t)-1;
                ++tap->reconnects;
                break;
            } else {
                tap = NULL;
            }
        }

        // Disconnects aren't quite immediate yet, so if we see a
        // connection request for a client *and* expiry_time is 0, we
        // should kill this guy off.
        if (tap != NULL) {
            std::map<const void*, TapConnection*>::iterator miter;
            for (miter = tapConnectionMap.begin();
                 miter != tapConnectionMap.end();
                 ++miter) {
                if (miter->second == tap) {
                    break;
                }
            }

            if (tapKeepAlive == 0) {
                getLogger()->log(EXTENSION_LOG_INFO, NULL,
                                 "The TAP channel (\"%s\") exists, but should be nuked\n",
                                 name.c_str());
                tap->client.assign(TapConnection::getAnonTapName());
                tap->doDisconnect = true;
                tap->paused = true;
                tap = NULL;
            } else {
                getLogger()->log(EXTENSION_LOG_INFO, NULL,
                                 "The TAP channel (\"%s\") exists... grabbing the channel\n",
                                 name.c_str());
                TapConnection *n = new TapConnection(TapConnection::getAnonTapName(), 0);
                n->doDisconnect = true;
                n->paused = true;
                allTaps.push_back(n);
                tapConnectionMap[miter->first] = n;
            }
        }

        // Start decoding the userdata section of the packet...
        const char *ptr = static_cast<const char*>(userdata);
        uint64_t backfillAge = 0;
        std::vector<uint16_t> vbuckets;

        if (flags & TAP_CONNECT_FLAG_BACKFILL) { /* */
            assert(nuserdata >= sizeof(backfillAge));
            // use memcpy to avoid alignemt issues
            memcpy(&backfillAge, ptr, sizeof(backfillAge));
            backfillAge = ntohll(backfillAge);
            nuserdata -= sizeof(backfillAge);
            ptr += sizeof(backfillAge);
        }

        if (flags & TAP_CONNECT_FLAG_LIST_VBUCKETS) {
            uint16_t nvbuckets;
            assert(nuserdata >= sizeof(nvbuckets));
            memcpy(&nvbuckets, ptr, sizeof(nvbuckets));
            nuserdata -= sizeof(nvbuckets);
            ptr += sizeof(nvbuckets);
            nvbuckets = ntohs(nvbuckets);
            if (nvbuckets > 0) {
                assert(nuserdata >= (sizeof(uint16_t) * nvbuckets));
                for (uint16_t ii = 0; ii < nvbuckets; ++ii) {
                    uint16_t val;
                    memcpy(&val, ptr, sizeof(nvbuckets));
                    ptr += sizeof(uint16_t);
                    vbuckets.push_back(ntohs(val));
                }
            }
        }

        // @todo ensure that we don't have this client alredy
        // if so this should be a reconnect...
        if (tap == NULL) {
            TapConnection *tc = new TapConnection(name, flags);
            allTaps.push_back(tc);
            tapConnectionMap[cookie] = tc;

            if (flags & TAP_CONNECT_FLAG_BACKFILL) {
                tc->backfillAge = backfillAge;
            }

            if (tc->backfillAge < (uint64_t)time(NULL)) {
                setTapValidity(tc->client, cookie);
                tc->doRunBackfill = true;
                tc->pendingBackfill = true;
            }

            if (flags & TAP_CONNECT_FLAG_LIST_VBUCKETS) {
                tc->vbucketFilter = VBucketFilter(vbuckets);
            }

            tc->dumpQueue = flags & TAP_CONNECT_FLAG_DUMP;
            if (flags & TAP_CONNECT_FLAG_TAKEOVER_VBUCKETS) {
                for (std::vector<uint16_t>::iterator it = vbuckets.begin();
                     it != vbuckets.end(); ++it) {
                    TapVBucketEvent hi(TAP_VBUCKET_SET, *it, pending);
                    TapVBucketEvent lo(TAP_VBUCKET_SET, *it, active);
                    tc->addVBucketHighPriority(hi);
                    tc->addVBucketLowPriority(lo);
                }
                tc->dumpQueue = true;
            }
        } else {
            tapConnectionMap[cookie] = tap;
            tap->connected = true;
        }
        tapNotifySync.notify();
    }

    ENGINE_ERROR_CODE tapNotify(const void *cookie,
                                void *engine_specific,
                                uint16_t nengine,
                                uint8_t ttl,
                                uint16_t tap_flags,
                                tap_event_t tap_event,
                                uint32_t tap_seqno,
                                const void *key,
                                size_t nkey,
                                uint32_t flags,
                                uint32_t exptime,
                                uint64_t cas,
                                const void *data,
                                size_t ndata,
                                uint16_t vbucket)
    {
        (void)engine_specific;
        (void)nengine;
        (void)ttl;
        (void)tap_flags;
        (void)tap_seqno;

        // If cookie is null, this is the internal tap client, so we
        // should disconnect it if tap isn't enabled.
        if (!cookie && !tapEnabled) {
            return ENGINE_DISCONNECT;
        }

        switch (tap_event) {
        case TAP_FLUSH:
            return flush(cookie, 0);
        case TAP_DELETION:
            {
                std::string k(static_cast<const char*>(key), nkey);
                return itemDelete(cookie, k, vbucket);
            }

        case TAP_MUTATION:
            {
                // We don't get the trailing CRLF in tap mutation but should store it
                // to satisfy memcached expectations.
                //
                // We do this by manually constructing the item using its
                // value_t constructor to reduce memory copies as much as
                // possible.

                std::string k(static_cast<const char *>(key), nkey);
                std::string v;
                v.reserve(ndata+2);
                v.append(static_cast<const char*>(data), ndata);
                v.append("\r\n");
                shared_ptr<const Blob> vblob(Blob::New(v));

                Item *item = new Item(k, flags, exptime, vblob);
                item->setVBucketId(vbucket);

                /* @TODO we don't have CAS now.. we might in the future.. */
                (void)cas;
                ENGINE_ERROR_CODE ret = epstore->set(*item, cookie, true);
                if (ret == ENGINE_SUCCESS) {
                    addMutationEvent(item, vbucket);
                }

                delete item;
                return ret;
            }

        case TAP_OPAQUE:
            break;

        case TAP_VBUCKET_SET:
            {
                if (nengine != sizeof(vbucket_state_t)) {
                    // illegal datasize
                    return ENGINE_DISCONNECT;
                }

                vbucket_state_t state;
                memcpy(&state, engine_specific, nengine);
                state = (vbucket_state_t)ntohl(state);

                if (!is_valid_vbucket_state_t(state)) {
                    return ENGINE_DISCONNECT;
                }

                epstore->setVBucketState(vbucket, state, serverApi->core);
            }
            break;

        default:
            abort();
        }

        return ENGINE_SUCCESS;
    }


    /**
     * Visit the objects and add them to the tap connecitons queue.
     * @todo this code should honor the backfill time!
     */
    void queueBackfill(TapConnection *tc, const void *tok);

    void handleDisconnect(const void *cookie) {
        LockHolder lh(tapNotifySync);
        std::map<const void*, TapConnection*>::iterator iter;
        iter = tapConnectionMap.find(cookie);
        if (iter != tapConnectionMap.end()) {
            if (iter->second) {
                iter->second->expiry_time = serverApi->core->get_current_time();
                if (iter->second->doDisconnect) {
                    iter->second->expiry_time--;
                } else {
                    iter->second->expiry_time += (int)tapKeepAlive;
                }
                iter->second->connected = false;
            } else {
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "Found half-linked tap connection at: %p\n",
                                 cookie);
            }
            tapConnectionMap.erase(iter);
        }
        purgeExpiredTapConnections_UNLOCKED();
    }

    protocol_binary_response_status stopFlusher(const char **msg) {
        protocol_binary_response_status rv = PROTOCOL_BINARY_RESPONSE_SUCCESS;
        *msg = NULL;
        if (!epstore->pauseFlusher()) {
            getLogger()->log(EXTENSION_LOG_INFO, NULL,
                             "Attempted to stop flusher in state [%s]\n",
                             epstore->getFlusher()->stateName());
            *msg = "Flusher not running.";
            rv = PROTOCOL_BINARY_RESPONSE_EINVAL;
        }
        return rv;
    }

    protocol_binary_response_status startFlusher(const char **msg) {
        protocol_binary_response_status rv = PROTOCOL_BINARY_RESPONSE_SUCCESS;
        *msg = NULL;
        if (!epstore->resumeFlusher()) {
            getLogger()->log(EXTENSION_LOG_INFO, NULL,
                             "Attempted to start flusher in state [%s]\n",
                             epstore->getFlusher()->stateName());
            *msg = "Flusher not shut down.";
            rv = PROTOCOL_BINARY_RESPONSE_EINVAL;
        }
        return rv;
    }

    bool deleteVBucket(uint16_t vbid) {
        return epstore->deleteVBucket(vbid);
    }

    void resetStats()
    {
        stats.tooYoung.set(0);
        stats.tooOld.set(0);
        stats.dirtyAge.set(0);
        stats.dirtyAgeHighWat.set(0);
        stats.flushDuration.set(0);
        stats.flushDurationHighWat.set(0);
        stats.commit_time.set(0);
        stats.numValueEjects.set(0);
        stats.io_num_read.set(0);
        stats.io_num_write.set(0);
        stats.io_read_bytes.set(0);
        stats.io_write_bytes.set(0);
        stats.bgNumOperations.set(0);
        stats.bgWait.set(0);
        stats.bgLoad.set(0);
        stats.bgMinWait.set(999999999);
        stats.bgMaxWait.set(0);
        stats.bgMinLoad.set(999999999);
        stats.bgMaxLoad.set(0);
    }

    void setMinDataAge(int to) {
        epstore->setMinDataAge(to);
    }

    void setQueueAgeCap(int to) {
        epstore->setQueueAgeCap(to);
    }

    void setTxnSize(int to) {
        epstore->setTxnSize(to);
    }

    void setBGFetchDelay(uint32_t to) {
        epstore->setBGFetchDelay(to);
    }

#ifdef ENABLE_INTERNAL_TAP
    void setTapPeer(std::string peer) {
        tapConnect(peer);
    }

    bool startReplication() {
        LockHolder lh(tapMutex);
        tapEnabled = true;
        if (clientTap != NULL) {
            try {
                clientTap->start();
                return true;
            } catch (std::runtime_error &e) {
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "Failed to start replication: %s\n",
                                 e.what());
            }
        }

        return false;
    }

    bool stopReplication() {
        LockHolder ltm(tapMutex);
        tapEnabled = false;

        if (clientTap != NULL) {
            try {
                clientTap->stop();
                return true;
            } catch (std::runtime_error &e) {
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "Failed to start replication: %s\n",
                                 e.what());
            }
        }

        return false;
    }
#endif

    protocol_binary_response_status evictKey(const std::string &key,
                                             uint16_t vbucket,
                                             const char **msg) {
        return epstore->evictKey(key, vbucket, msg);
    }

    RCPtr<VBucket> getVBucket(uint16_t vbucket) {
        return epstore->getVBucket(vbucket);
    }

    void setVBucketState(uint16_t vbid, vbucket_state_t to) {
        epstore->setVBucketState(vbid, to, serverApi->core);
    }

    ~EventuallyPersistentEngine() {
        delete epstore;
        delete sqliteDb;
        delete getlExtension;
    }

    engine_info *getInfo() {
        return &info.info;
    }

    size_t getTapIdleTimeout() const {
        return tapIdleTimeout;
    }

    EPStats &getEpStats() {
        return stats;
    }

private:
    EventuallyPersistentEngine(GET_SERVER_API get_server_api);
    friend ENGINE_ERROR_CODE create_instance(uint64_t interface,
                                             GET_SERVER_API get_server_api,
                                             ENGINE_HANDLE **handle);

    void updateTapStats(void) {
        LockHolder lh(tapNotifySync);
        size_t depth = 0, totalSent = 0;
        std::map<const void*, TapConnection*>::iterator iter;
        for (iter = tapConnectionMap.begin(); iter != tapConnectionMap.end(); iter++) {
            depth += iter->second->queue->size();
            totalSent += iter->second->recordsFetched;
        }
        lh.unlock();

        // Use the depth and number of records fetched we calculated above.
        epstore->setTapStats(depth, totalSent);
    }

    void notifyTapIoThreadMain(void) {
        bool addNoop = false;

        rel_time_t now = ep_current_time();
        if (now > nextTapNoop && tapIdleTimeout != (size_t)-1) {
            addNoop = true;
            nextTapNoop = now + (tapIdleTimeout / 3);
        }
        LockHolder lh(tapNotifySync);
        // We should pause unless we purged some connections or
        // all queues have items.
        bool shouldPause = purgeExpiredTapConnections_UNLOCKED() == 0;
        // see if I have some channels that I have to signal..
        std::map<const void*, TapConnection*>::iterator iter;
        for (iter = tapConnectionMap.begin(); iter != tapConnectionMap.end(); iter++) {
            if (iter->second->doDisconnect || !iter->second->idle()) {
                shouldPause = false;
            } else if (addNoop) {
                TapVBucketEvent hi(TAP_NOOP, 0, pending);
                iter->second->addVBucketHighPriority(hi);
                shouldPause = false;
            }
        }

        if (shouldPause) {
            double diff = nextTapNoop - now;
            if (diff > 0) {
                tapNotifySync.wait(diff);
            }

            if (shutdown) {
                return;
            }
            purgeExpiredTapConnections_UNLOCKED();
        }

        // Collect the list of connections that need to be signaled.
        std::list<const void *> toNotify;
        for (iter = tapConnectionMap.begin(); iter != tapConnectionMap.end(); iter++) {
            if (iter->second->paused || iter->second->doDisconnect) {
                toNotify.push_back(iter->first);
            }
        }

        lh.unlock();

        // Signal all outstanding, paused connections.
        std::for_each(toNotify.begin(), toNotify.end(),
                      std::bind2nd(std::ptr_fun(serverApi->core->notify_io_complete),
                                   ENGINE_SUCCESS));
    }

    friend void *EvpNotifyTapIo(void*arg);
    void notifyTapIoThread(void) {
        // Fix clean shutdown!!!
        while (!shutdown) {

            updateTapStats();
            notifyTapIoThreadMain();

            if (shutdown) {
                return;
            }

            // Prevent the notify thread from busy-looping while
            // holding locks when there's work to do.
            LockHolder lh(tapNotifySync);
            tapNotifySync.wait(1.0);
        }
    }

    void purgeSingleExpiredTapConnection(TapConnection *tc) {
        allTaps.remove(tc);
        /* Assert that the connection doesn't live in the map.. */
        /* TROND: Remove this when we're sure we don't have a bug here */
        assert(!mapped(tc));
        delete tc;
    }

    bool mapped(TapConnection *tc) {
        bool rv = false;
        std::map<const void*, TapConnection*>::iterator it;
        for (it = tapConnectionMap.begin();
             it != tapConnectionMap.end();
             ++it) {
            if (it->second == tc) {
                rv = true;
            }
        }
        return rv;
    }

    int purgeExpiredTapConnections_UNLOCKED() {
        rel_time_t now = serverApi->core->get_current_time();
        std::list<TapConnection*> deadClients;

        std::list<TapConnection*>::iterator iter;
        for (iter = allTaps.begin(); iter != allTaps.end(); iter++) {
            TapConnection *tc = *iter;
            if (tc->expiry_time <= now && !mapped(tc) && !tc->connected) {
                deadClients.push_back(tc);
            }
        }

        for (iter = deadClients.begin(); iter != deadClients.end(); iter++) {
            TapConnection *tc = *iter;
            purgeSingleExpiredTapConnection(tc);
        }

        return static_cast<int>(deadClients.size());
    }

    TapConnection* findTapConnByName_UNLOCKED(const std::string&name) {
        TapConnection *rv(NULL);
        std::list<TapConnection*>::iterator iter;
        for (iter = allTaps.begin(); iter != allTaps.end(); iter++) {
            TapConnection *tc = *iter;
            if (tc->client == name) {
                rv = tc;
            }
        }
        return rv;
    }

    friend class BackFillVisitor;
    bool setEvents(const std::string &name,
                   std::list<QueuedItem> *q)
    {
        bool notify = true;
        bool found = false;
        LockHolder lh(tapNotifySync);

        TapConnection *tc = findTapConnByName_UNLOCKED(name);
        if (tc) {
            found = true;
            tc->appendQueue(q);
            notify = tc->paused; // notify if paused
        }

        if (notify) {
            tapNotifySync.notify();
        }

        return found;
    }

    void completeBackfill(const std::string &name) {
        bool notify(true);
        LockHolder lh(tapNotifySync);
        clearTapValidity(name);

        TapConnection *tc = findTapConnByName_UNLOCKED(name);
        if (tc) {
            tc->completeBackfill();
            notify = tc->paused; // notify if paused
        }

        if (notify) {
            tapNotifySync.notify();
        }
    }

    ssize_t queueDepth(const std::string &name) {
        ssize_t rv = -1;
        LockHolder lh(tapNotifySync);

        TapConnection *tc = findTapConnByName_UNLOCKED(name);
        if (tc) {
            rv = tc->queue->size();
        }

        return rv;
    }

    void setTapValidity(const std::string &name, const void* token);
    void clearTapValidity(const std::string &name);
    bool checkTapValidity(const std::string &name, const void* token);

    void addEvent(const std::string &str, uint16_t vbid, enum queue_operation op)
    {
        bool notify = false;
        LockHolder lh(tapNotifySync);

        std::list<TapConnection*>::iterator iter;
        for (iter = allTaps.begin(); iter != allTaps.end(); iter++) {
            TapConnection *tc = *iter;
            if (!tc->dumpQueue && tc->addEvent(str, vbid, op) && tc->paused) {
                notify = true;
            }
        }

        if (notify) {
            tapNotifySync.notify();
        }
    }

    void addMutationEvent(Item *it, uint16_t vbid) {
        // Currently we use the same queue for all kinds of events..
        addEvent(it->getKey(), vbid, queue_op_set);
    }

    void addDeleteEvent(const std::string &key, uint16_t vbid) {
        // Currently we use the same queue for all kinds of events..
        addEvent(key, vbid, queue_op_del);
    }

    void addFlushEvent() {
        LockHolder lh(tapNotifySync);
        bool notify = false;
        std::list<TapConnection*>::iterator iter;
        for (iter = allTaps.begin(); iter != allTaps.end(); iter++) {
            TapConnection *tc = *iter;
            if (!tc->dumpQueue) {
                tc->flush();
                notify = true;
            }
        }
        if (notify) {
            tapNotifySync.notify();
        }
    }

    void startEngineThreads(void);
    void stopEngineThreads(void) {
        if (startedEngineThreads) {
            LockHolder lh(tapNotifySync);
            shutdown = true;
            tapNotifySync.notify();
            lh.unlock();
            pthread_join(notifyThreadId, NULL);
        }
    }


    bool dbAccess(void) {
        bool ret = true;
        if (access(dbname, F_OK) == -1) {
            // file does not exist.. let's try to create it..
            FILE *fp = fopen(dbname, "w");
            if (fp == NULL) {
                ret= false;
            } else {
                fclose(fp);
                std::remove(dbname);
            }
        } else if (access(dbname, R_OK) == -1 || access(dbname, W_OK) == -1) {
            ret = false;
        }

        return ret;
    }

    ENGINE_ERROR_CODE doEngineStats(const void *cookie, ADD_STAT add_stat)
    {
        EPStats &epstats = getEpStats();
        add_casted_stat("ep_version", VERSION, add_stat, cookie);
        add_casted_stat("ep_storage_age",
                        epstats.dirtyAge, add_stat, cookie);
        add_casted_stat("ep_storage_age_highwat",
                        epstats.dirtyAgeHighWat, add_stat, cookie);
        add_casted_stat("ep_min_data_age",
                        epstats.min_data_age, add_stat, cookie);
        add_casted_stat("ep_queue_age_cap",
                        epstats.queue_age_cap, add_stat, cookie);
        add_casted_stat("ep_max_txn_size",
                        epstore->getTxnSize(), add_stat, cookie);
        add_casted_stat("ep_data_age",
                        epstats.dataAge, add_stat, cookie);
        add_casted_stat("ep_data_age_highwat",
                        epstats.dataAgeHighWat, add_stat, cookie);
        add_casted_stat("ep_too_young",
                        epstats.tooYoung, add_stat, cookie);
        add_casted_stat("ep_too_old",
                        epstats.tooOld, add_stat, cookie);
        add_casted_stat("ep_total_enqueued",
                        epstats.totalEnqueued, add_stat, cookie);
        add_casted_stat("ep_total_persisted",
                        epstats.totalPersisted, add_stat, cookie);
        add_casted_stat("ep_item_flush_failed",
                        epstats.flushFailed, add_stat, cookie);
        add_casted_stat("ep_item_commit_failed",
                        epstats.commitFailed, add_stat, cookie);
        add_casted_stat("ep_queue_size",
                        epstats.queue_size, add_stat, cookie);
        add_casted_stat("ep_flusher_todo",
                        epstats.flusher_todo, add_stat, cookie);
        add_casted_stat("ep_flusher_state",
                        epstore->getFlusher()->stateName(),
                        add_stat, cookie);
        add_casted_stat("ep_commit_time",
                        epstats.commit_time, add_stat, cookie);
        add_casted_stat("ep_flush_duration",
                        epstats.flushDuration, add_stat, cookie);
        add_casted_stat("ep_flush_duration_highwat",
                        epstats.flushDurationHighWat, add_stat, cookie);
        add_casted_stat("curr_items", epstats.curr_items, add_stat,
                        cookie);
        add_casted_stat("mem_used", stats.currentSize + stats.memOverhead, add_stat,
                        cookie);
        add_casted_stat("ep_kv_size", stats.currentSize, add_stat, cookie);
        add_casted_stat("ep_overhead", stats.memOverhead, add_stat, cookie);
        add_casted_stat("ep_max_data_size", epstats.maxDataSize, add_stat, cookie);
        add_casted_stat("ep_mem_low_wat", epstats.mem_low_wat, add_stat, cookie);
        add_casted_stat("ep_mem_high_wat", epstats.mem_high_wat, add_stat, cookie);
        add_casted_stat("ep_total_cache_size", StoredValue::getTotalCacheSize(stats),
                        add_stat, cookie);
        add_casted_stat("ep_storage_type",
                        HashTable::getDefaultStorageValueTypeStr(),
                        add_stat, cookie);
        add_casted_stat("ep_bg_fetched", epstats.bg_fetched, add_stat,
                        cookie);
        add_casted_stat("ep_num_value_ejects", epstats.numValueEjects, add_stat,
                        cookie);

        if (warmup) {
            add_casted_stat("ep_warmup_thread",
                            epstats.warmupComplete.get() ? "complete" : "running",
                            add_stat, cookie);
            add_casted_stat("ep_warmed_up", epstats.warmedUp, add_stat, cookie);
            if (epstats.warmupComplete.get()) {
                add_casted_stat("ep_warmup_time", epstats.warmupTime,
                                add_stat, cookie);
            }
        }

        add_casted_stat("ep_tap_total_queue", epstats.tap_queue,
                        add_stat, cookie);
        add_casted_stat("ep_tap_total_fetched", epstats.tap_fetched,
                        add_stat, cookie);
        add_casted_stat("ep_tap_keepalive", tapKeepAlive,
                        add_stat, cookie);

        add_casted_stat("ep_dbname", dbname, add_stat, cookie);
        add_casted_stat("ep_dbinit", databaseInitTime, add_stat, cookie);
        add_casted_stat("ep_warmup", warmup ? "true" : "false",
                        add_stat, cookie);

        add_casted_stat("ep_io_num_read", epstats.io_num_read, add_stat, cookie);
        add_casted_stat("ep_io_num_write", epstats.io_num_write, add_stat, cookie);
        add_casted_stat("ep_io_read_bytes", epstats.io_read_bytes, add_stat, cookie);
        add_casted_stat("ep_io_write_bytes", epstats.io_write_bytes, add_stat, cookie);

        if (epstats.bgNumOperations > 0) {
            add_casted_stat("ep_bg_num_samples", epstats.bgNumOperations, add_stat, cookie);
            add_casted_stat("ep_bg_min_wait",
                            hrtime2text(epstats.bgMinWait).c_str(),
                            add_stat, cookie);
            add_casted_stat("ep_bg_max_wait",
                            hrtime2text(epstats.bgMaxWait).c_str(),
                            add_stat, cookie);
            add_casted_stat("ep_bg_wait_avg",
                            hrtime2text(epstats.bgWait / epstats.bgNumOperations).c_str(),
                            add_stat, cookie);
            add_casted_stat("ep_bg_min_load",
                            hrtime2text(epstats.bgMinLoad).c_str(),
                            add_stat, cookie);
            add_casted_stat("ep_bg_max_load",
                            hrtime2text(epstats.bgMaxLoad).c_str(),
                            add_stat, cookie);
            add_casted_stat("ep_bg_load_avg",
                            hrtime2text(epstats.bgLoad / epstats.bgNumOperations).c_str(),
                            add_stat, cookie);
        }

        add_casted_stat("ep_num_non_resident", stats.numNonResident, add_stat, cookie);

        return ENGINE_SUCCESS;
    }

    ENGINE_ERROR_CODE doVBucketStats(const void *cookie, ADD_STAT add_stat) {
        StatVBucketVisitor svbv(cookie, add_stat);
        epstore->visit(svbv);
        return ENGINE_SUCCESS;
    }

    ENGINE_ERROR_CODE doHashStats(const void *cookie, ADD_STAT add_stat) {
        HashTableDepthStatVisitor depthVisitor;
        epstore->visitDepth(depthVisitor);
        add_casted_stat("ep_hash_bucket_size", epstore->getHashSize(), add_stat, cookie);
        add_casted_stat("ep_hash_num_locks", epstore->getHashLocks(), add_stat, cookie);
        add_casted_stat("ep_hash_min_depth", depthVisitor.min, add_stat, cookie);
        add_casted_stat("ep_hash_max_depth", depthVisitor.max, add_stat, cookie);
        return ENGINE_SUCCESS;
    }

    template <typename T>
    static void addTapStat(const char *name, TapConnection *tc, T val,
                           ADD_STAT add_stat, const void *cookie) {
        char tap[80];
        assert(strlen(name) + tc->client.size() + 2 < sizeof(tap));
        sprintf(tap, "%s:%s", tc->client.c_str(), name);
        add_casted_stat(tap, val, add_stat, cookie);
    }

    static void addTapStat(const char *name, TapConnection *tc, bool val,
                           ADD_STAT add_stat, const void *cookie) {
        addTapStat(name, tc, val ? "true" : "false", add_stat, cookie);
    }

    ENGINE_ERROR_CODE doTapStats(const void *cookie, ADD_STAT add_stat) {
        std::list<TapConnection*>::iterator iter;
        EPStats &epstats = getEpStats();
        add_casted_stat("ep_tap_total_queue", epstats.tap_queue, add_stat, cookie);
        add_casted_stat("ep_tap_total_fetched", epstats.tap_fetched, add_stat, cookie);
        add_casted_stat("ep_tap_keepalive", tapKeepAlive, add_stat, cookie);

        int totalTaps = 0;
        LockHolder lh(tapNotifySync);
        for (iter = allTaps.begin(); iter != allTaps.end(); iter++) {
            totalTaps++;
            TapConnection *tc = *iter;
            addTapStat("qlen", tc, tc->queue->size(), add_stat, cookie);
            addTapStat("qlen_high_pri", tc, tc->vBucketHighPriority.size(), add_stat, cookie);
            addTapStat("qlen_low_pri", tc, tc->vBucketLowPriority.size(), add_stat, cookie);
            addTapStat("vb_filters", tc, tc->vbucketFilter.size(), add_stat, cookie);
            addTapStat("rec_fetched", tc, tc->recordsFetched, add_stat, cookie);
            addTapStat("idle", tc, tc->idle(), add_stat, cookie);
            addTapStat("empty", tc, tc->empty(), add_stat, cookie);
            addTapStat("complete", tc, tc->complete(), add_stat, cookie);
            addTapStat("flags", tc, tc->flags, add_stat, cookie);
            addTapStat("connected", tc, tc->connected, add_stat, cookie);
            addTapStat("pending_disconnect", tc, tc->doDisconnect, add_stat, cookie);
            addTapStat("paused", tc, tc->paused, add_stat, cookie);
            addTapStat("pending_backfill", tc, tc->pendingBackfill, add_stat, cookie);
            if (tc->reconnects > 0) {
                addTapStat("reconnects", tc, tc->reconnects, add_stat, cookie);
            }
            if (tc->backfillAge != 0) {
                addTapStat("backfill_age", tc, (size_t)tc->backfillAge, add_stat, cookie);
            }
        }
        add_casted_stat("ep_tap_count", totalTaps, add_stat, cookie);

        add_casted_stat("ep_replication_state",
                        tapEnabled? "enabled": "disabled", add_stat, cookie);

        const char *repStatus = "stopped";
        std::string tapPeer;

#ifdef ENABLE_INTERNAL_TAP
        {
            LockHolder ltm(tapMutex);
            if (clientTap != NULL) {
                tapPeer.assign(clientTap->peer);
                if (clientTap->connected) {
                    if (!tapEnabled) {
                        repStatus = "stopping";
                    } else {
                        repStatus = "running";
                    }
                } else if (clientTap->running && tapEnabled) {
                    repStatus = "connecting";
                }
            }
        }
#endif

        add_casted_stat("ep_replication_peer",
                        tapPeer.empty()? "none":
                        tapPeer.c_str(), add_stat, cookie);
        add_casted_stat("ep_replication_status", repStatus, add_stat, cookie);

        return ENGINE_SUCCESS;
    }

    void addLookupResult(const void *cookie, Item *result) {
        LockHolder lh(lookupMutex);
        std::map<const void*, Item*>::iterator it = lookups.find(cookie);
        if (it != lookups.end()) {
            if (it->second != NULL) {
                getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                                 "Cleaning up old lookup result for '%s'\n",
                                 it->second->getKey().c_str());
                delete it->second;
            } else {
                getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                                 "Cleaning up old null lookup result\n");
            }
            lookups.erase(it);
        }
        lookups[cookie] = result;
    }

    bool fetchLookupResult(const void *cookie, Item **item) {
        // This will return *and erase* the lookup result for a connection.
        // You look it up, you own it.
        LockHolder lh(lookupMutex);
        std::map<const void*, Item*>::iterator it = lookups.find(cookie);
        if (it != lookups.end()) {
            *item = it->second;
            lookups.erase(it);
            return true;
        } else {
            return false;
        }
    }

    ENGINE_ERROR_CODE doKeyStats(const void *cookie, ADD_STAT add_stat,
                                 const char *k, int nkey, bool validate=false)
    {
        std::string key(k, nkey);
        ENGINE_ERROR_CODE rv = ENGINE_FAILED;

        Item *it = NULL;
        shared_ptr<Item> diskItem;
        struct key_stats kstats;
        rel_time_t now = ep_current_time();
        if (fetchLookupResult(cookie, &it)) {
            diskItem.reset(it); // Will be null if the key was not found
            if (!validate) {
                getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                                 "Found lookup results for non-validating key stat call. Would have leaked\n");
                diskItem.reset();
            }
        } else if (validate) {
            shared_ptr<LookupCallback> cb(new LookupCallback(this, cookie));
            // TODO:  Need a proper vbucket ID here.
            epstore->getFromUnderlying(key, 0, cb);
            return ENGINE_EWOULDBLOCK;
        }

        // TODO:  Need a proper vbucket ID here.
        if (epstore->getKeyStats(k, 0, kstats)) {
            std::string valid("this_is_a_bug");
            if (validate) {
                if (kstats.dirty) {
                    valid.assign("dirty");
                } else {
                    // TODO:  Need a proper vbucket ID here -- and server API
                    GetValue gv(epstore->get(key, 0, cookie, NULL));
                    if (gv.getStatus() == ENGINE_SUCCESS) {
                        shared_ptr<Item> item(gv.getValue());
                        if (diskItem.get()) {
                            // Both items exist
                            if (diskItem->getNBytes() != item->getNBytes()) {
                                valid.assign("length_mismatch");
                            } else if (memcmp(diskItem->getData(), item->getData(),
                                              diskItem->getNBytes()) != 0) {
                                valid.assign("data_mismatch");
                            } else if (diskItem->getFlags() != item->getFlags()) {
                                valid.assign("flags_mismatch");
                            } else {
                                valid.assign("valid");
                            }
                        } else {
                            // Since we do the disk lookup first, this could
                            // be transient
                            valid.assign("ram_but_not_disk");
                        }
                    } else {
                        valid.assign("item_deleted");
                    }
                }
                getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "Key '%s' is %s\n",
                                 key.c_str(), valid.c_str());
            }
            add_casted_stat("key_is_dirty", kstats.dirty, add_stat, cookie);
            add_casted_stat("key_exptime", kstats.exptime, add_stat, cookie);
            add_casted_stat("key_flags", kstats.flags, add_stat, cookie);
            add_casted_stat("key_cas", kstats.cas, add_stat, cookie);
            add_casted_stat("key_dirtied", kstats.dirty ? now -
                            kstats.dirtied : 0, add_stat, cookie);
            add_casted_stat("key_data_age", kstats.dirty ? now -
                            kstats.data_age : 0, add_stat, cookie);
            if (validate) {
                add_casted_stat("key_valid", valid.c_str(), add_stat, cookie);
            }
            rv = ENGINE_SUCCESS;
        } else {
            rv = ENGINE_KEY_ENOENT;
        }

        return rv;
    }

#ifdef ENABLE_INTERNAL_TAP
    void tapConnect(std::string &peer) {
        uint32_t flags = 0;

        LockHolder lh(tapMutex);
        bool found = false;
        if (clientTap != NULL) {
            if (clientTap->peer != peer) {
                delete clientTap;
            } else {
                found = true;
            }
        }

        if (!found) {
            clientTap = new TapClientConnection(peer, tapId, flags, this);
            tapConnect();
        }
    }

    void tapConnect() {
        try {
            tapEnabled = true;
            std::stringstream ss;
            ss << "Setting up TAP connection to: " << clientTap->peer
               << std::endl;
            getLogger()->log(EXTENSION_LOG_DEBUG, NULL, ss.str().c_str());
            clientTap->start();
        } catch (std::runtime_error &e) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Failed to create TAP connection: %s\n",
                             e.what());
        }
    }
#endif

    const char *dbname;
    const char *initFile;
    bool warmup;
    bool wait_for_warmup;
    bool startVb0;
    SERVER_HANDLE_V1 *serverApi;
    StrategicSqlite3 *sqliteDb;
    EventuallyPersistentStore *epstore;
    std::map<const void*, TapConnection*> tapConnectionMap;
    std::map<const std::string, const void*> backfillValidityMap;
    std::list<TapConnection*> allTaps;
    std::map<const void*, Item*> lookups;
    Mutex lookupMutex;
    time_t databaseInitTime;
    size_t tapKeepAlive;
    size_t tapIdleTimeout;
    size_t nextTapNoop;
    pthread_t notifyThreadId;
    bool startedEngineThreads;
    SyncObject tapNotifySync;
    volatile bool shutdown;
    GET_SERVER_API getServerApi;
    union {
        engine_info info;
        char buffer[sizeof(engine_info) + 10 * sizeof(feature_info) ];
    } info;
    GetlExtension *getlExtension;

    Mutex tapMutex;
#ifdef ENABLE_INTERNAL_TAP
    TapClientConnection* clientTap;
    std::string tapId;
#endif
    bool tapEnabled;
    size_t maxItemSize;
    size_t memLowWat;
    size_t memHighWat;
    size_t minDataAge;
    size_t queueAgeCap;
    EPStats stats;
};

/**
 * VBucketVisitor to backfill a TapConnection.
 */
class BackFillVisitor : public VBucketVisitor {
public:
    BackFillVisitor(EventuallyPersistentEngine *e, TapConnection *tc,
                    const void *token):
        VBucketVisitor(), engine(e), name(tc->client),
        queue(new std::list<QueuedItem>),
        filter(tc->vbucketFilter), validityToken(token),
        maxBackfillSize(250000), valid(true) { }

    ~BackFillVisitor() {
        delete queue;
    }

    bool visitBucket(uint16_t vbid, vbucket_state_t state) {
        if (filter(vbid)) {
            VBucketVisitor::visitBucket(vbid, state);
            return true;
        }
        return false;
    }

    void visit(StoredValue *v) {
        std::string k = v->getKey();
        QueuedItem qi(k, currentBucket, queue_op_set);
        queue->push_back(qi);
    }

    bool shouldContinue() {
        setEvents();
        return valid;
    }

    void apply(void) {
        setEvents();
        if (valid) {
            engine->completeBackfill(name);
        }
    }

private:

    void setEvents() {
        if (checkValidity()) {
            if (!queue->empty()) {
                // Don't notify unless we've got some data..
                engine->setEvents(name, queue);
            }
            waitForQueue();
        }
    }

    void waitForQueue() {
        bool reported(false);
        bool tooBig(true);

        while (checkValidity() && tooBig) {
            ssize_t theSize(engine->queueDepth(name));
            if (theSize < 0) {
                getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                                 "TapConnection %s went away.  Stopping backfill.\n",
                                 name.c_str());
                valid = false;
                return;
            }

            tooBig = theSize > maxBackfillSize;

            if (tooBig) {
                if (!reported) {
                    getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                                     "Tap queue depth too big for %s, sleeping\n",
                                     name.c_str());
                    reported = true;
                }
                sleep(1);
            }
        }
        if (reported) {
            getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                             "Resuming backfill of %s.\n",
                             name.c_str());
        }
    }

    bool checkValidity() {
        valid = valid && engine->checkTapValidity(name, validityToken);
        if (!valid) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Backfilling token for %s went invalid.  Stopping backfill.\n",
                             name.c_str());
        }
        return valid;
    }

    EventuallyPersistentEngine *engine;
    const std::string name;
    std::list<QueuedItem> *queue;
    VBucketFilter filter;
    const void *validityToken;
    ssize_t maxBackfillSize;
    bool valid;
};

#endif
