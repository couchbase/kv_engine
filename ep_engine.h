/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "locks.hh"
#include "ep.hh"
#include "sqlite-kvstore.hh"
#include <memcached/util.h>

#include <map>
#include <list>
#include <errno.h>

#define NUMBER_OF_SHARDS 4

extern "C" {
    EXPORT_FUNCTION
    ENGINE_ERROR_CODE create_instance(uint64_t interface,
                                      GET_SERVER_API get_server_api,
                                      ENGINE_HANDLE **handle);

    static const char *EvpItemGetKey(const item *it);
    static char *EvpItemGetData(const item *it);
    void EvpHandleDisconnect(const void *cookie,
                             ENGINE_EVENT_TYPE type,
                             const void *event_data,
                             const void *cb_data);
    static void *EvpNotifyTapIo(void*arg);
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

#define CMD_STOP_PERSISTENCE  0x80
#define CMD_START_PERSISTENCE 0x81
#define CMD_SET_FLUSH_PARAM 0x82

/**
 * I don't care about the set callbacks right now, but since this is a
 * demo, let's dump core if one of them fails so that we can debug it later
 */
class IgnoreCallback : public Callback<bool>
{
    virtual void callback(bool &value) {
        assert(value);
    }
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
     * @return true if the the queue was empty
     */
    bool addEvent(Item *it) {
        return addEvent(it->getKey());
    }

    /**
     * Add a key to the tap queue.
     * @return true if the the queue was empty
     */
    bool addEvent(const std::string &key) {
        bool wasEmpty = queue.empty();
        std::pair<std::set<std::string>::iterator, bool> ret;
        ret = queue_set.insert(key);
        if (ret.second) {
            queue.push_back(key);
        }
        return wasEmpty;
    }

    std::string next() {
        assert(!empty());
        std::string key = queue.front();
        queue.pop_front();
        queue_set.erase(key);
        ++recordsFetched;
        return key;
    }

    bool empty() {
        return queue.empty();
    }

    void flush() {
        pendingFlush = true;
        /* No point of keeping the rep queue when someone wants to flush it */
        queue.clear();
        queue_set.clear();
    }

    bool shouldFlush() {
        bool ret = pendingFlush;
        pendingFlush = false;
        return ret;
    }

    TapConnection(const std::string &n, uint32_t f) : client(n), flags(f),
        recordsFetched(0), pendingFlush(false), expiry_time((rel_time_t)-1),
        reconnects(0), connected(true), paused(false), backfillAge(0)
    { }

    /**
     * String used to identify the client.
     * @todo design the connect packet and fill inn som info here
     */
    std::string client;
    /**
     * The queue of keys that needs to be sent (this is the "live stream")
     */
    std::list<std::string> queue;
    /**
     * Set to prevent duplicate queue entries.
     *
     * Note that stl::set is O(log n) for ops we care about, so we'll
     * want to look out for this.
     */
    std::set<std::string> queue_set;
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

    DISALLOW_COPY_AND_ASSIGN(TapConnection);
};

class BackFillVisitor : public HashTableVisitor {
public:
    BackFillVisitor(TapConnection *tc) {
        tapConn = tc;
    }

    void visit(StoredValue *v) {
        tapConn->addEvent(v->getKey());
    }

private:
    TapConnection *tapConn;
};

/**
 *
 */
class EventuallyPersistentEngine : public ENGINE_HANDLE_V1 {
public:
    ENGINE_ERROR_CODE initialize(const char* config)
    {
        ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

        if (config != NULL) {
            char *dbn = NULL;
            const int max_items = 5;
            struct config_item items[max_items];
            int ii = 0;
            memset(items, 0, sizeof(items));

            items[ii].key = "dbname";
            items[ii].datatype = DT_STRING;
            items[ii].value.dt_string = &dbn;

            ++ii;
            items[ii].key = "warmup";
            items[ii].datatype = DT_BOOL;
            items[ii].value.dt_bool = &warmup;

            ++ii;
            items[ii].key = "tap_keepalive";
            items[ii].datatype = DT_SIZE;
            items[ii].value.dt_size = &tapKeepAlive;

            ++ii;
            items[ii].key = "config_file";
            items[ii].datatype = DT_CONFIGFILE;

            ++ii;
            items[ii].key = NULL;

            if (serverApi.parse_config(config, items, stderr) != 0) {
                ret = ENGINE_FAILED;
            } else {
                if (dbn != NULL) {
                    dbname = dbn;
                }
            }
        }

        if (ret == ENGINE_SUCCESS) {
            time_t start = time(NULL);
            try {
                sqliteDb = new MultiDBSqlite3(dbname, NUMBER_OF_SHARDS);
            } catch (std::exception& e) {
                std::cerr << "Failed to create database: " << e.what()
                          << std::endl;

                if (!dbAccess()) {
                    std::cerr << "No access to \"" << dbname << "\"."
                              << std::endl;
                }

                return ENGINE_FAILED;
            }

            databaseInitTime = time(NULL) - start;
            backend = epstore = new EventuallyPersistentStore(sqliteDb);

            if (backend == NULL) {
                ret = ENGINE_ENOMEM;
            } else {
                if (!warmup) {
                    backend->reset();
                }

                serverApi.register_callback(ON_DISCONNECT, EvpHandleDisconnect, this);
            }
            startEngineThreads();
        }

        return ret;
    }

    void destroy()
    {
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
                                 uint64_t cas)
    {
        (void)cas;
        std::string k(static_cast<const char*>(key), nkey);
        return itemDelete(cookie, k);
    }

    ENGINE_ERROR_CODE itemDelete(const void* cookie, const std::string &key)
    {
        (void)cookie;
        RememberingCallback<bool> delCb;

        backend->del(key, delCb);;
        delCb.waitForValue();
        if (delCb.val) {
            addDeleteEvent(key);
            return ENGINE_SUCCESS;
        } else {
            return ENGINE_KEY_ENOENT;
        }
    }


    void itemRelease(const void* cookie, item *item)
    {
        (void)cookie;
        delete (Item*)item;
    }

    ENGINE_ERROR_CODE get(const void* cookie,
                          item** item,
                          const void* key,
                          const int nkey)
    {
        (void)cookie;
        std::string k(static_cast<const char*>(key), nkey);
        RememberingCallback<GetValue> getCb;
        backend->get(k, getCb);
        getCb.waitForValue();
        if (getCb.val.success) {
            *item = getCb.val.value;
            return ENGINE_SUCCESS;
        } else {
            return ENGINE_KEY_ENOENT;
        }
    }

    ENGINE_ERROR_CODE getStats(const void* cookie,
                               const char* stat_key,
                               int nkey,
                               ADD_STAT add_stat)
    {
        (void)cookie;
        (void)nkey;
        (void)add_stat;

        if (stat_key == NULL) {
            // @todo add interesting stats
            struct ep_stats epstats;

            if (epstore) {
                epstore->getStats(&epstats);

                add_casted_stat("ep_storage_age",
                                epstats.dirtyAge, add_stat, cookie);
                add_casted_stat("ep_storage_age_highwat",
                                epstats.dirtyAgeHighWat, add_stat, cookie);
                add_casted_stat("ep_min_data_age",
                                epstats.min_data_age, add_stat, cookie);
                add_casted_stat("ep_data_age",
                                epstats.dataAge, add_stat, cookie);
                add_casted_stat("ep_data_age_highwat",
                                epstats.dataAgeHighWat, add_stat, cookie);
                add_casted_stat("ep_too_young",
                                epstats.tooYoung, add_stat, cookie);
                add_casted_stat("ep_queue_size",
                                epstats.queue_size, add_stat, cookie);
                add_casted_stat("ep_flusher_todo",
                                epstats.flusher_todo, add_stat, cookie);
                add_casted_stat("ep_commit_time",
                                epstats.commit_time, add_stat, cookie);
                add_casted_stat("ep_flush_duration",
                                epstats.flushDuration, add_stat, cookie);
                add_casted_stat("ep_flush_duration_highwat",
                                epstats.flushDurationHighWat, add_stat, cookie);
            }

            add_casted_stat("ep_dbname", dbname, add_stat, cookie);
            add_casted_stat("ep_dbinit", databaseInitTime, add_stat, cookie);
            add_casted_stat("ep_warmup", warmup ? "true" : "false",
                            add_stat, cookie);
            if (warmup) {
                add_casted_stat("ep_warmup_thread",
                                epstats.warmupComplete ? "complete" : "running",
                                add_stat, cookie);
                add_casted_stat("ep_warmed_up", epstats.warmedUp, add_stat, cookie);
                if (epstats.warmupComplete) {
                    add_casted_stat("ep_warmup_time", epstats.warmupTime,
                                    add_stat, cookie);
                }
            }

            std::list<TapConnection*>::iterator iter;
            size_t totalQueue = 0;
            size_t totalFetched = 0;
            {
                LockHolder lh(tapNotifySync);
                for (iter = allTaps.begin(); iter != allTaps.end(); iter++) {
                    char tap[80];
                    TapConnection *tc = *iter;
                    sprintf(tap, "%s:qlen", tc->client.c_str());
                    add_casted_stat(tap, tc->queue.size(), add_stat, cookie);
                    totalQueue += tc->queue.size();
                    sprintf(tap, "%s:rec_fetched", tc->client.c_str());
                    add_casted_stat(tap, tc->recordsFetched, add_stat, cookie);
                    totalFetched += tc->recordsFetched;
                    if (tc->reconnects > 0) {
                        sprintf(tap, "%s:reconnects", tc->client.c_str());
                        add_casted_stat(tap, tc->reconnects, add_stat, cookie);
                    }
                    if (tc->backfillAge != 0) {
                        sprintf(tap, "%s:backfill_age", tc->client.c_str());
                        add_casted_stat(tap, (size_t)tc->backfillAge, add_stat, cookie);
                    }
                }
            }

            add_casted_stat("ep_tap_total_queue", totalQueue, add_stat, cookie);
            add_casted_stat("ep_tap_total_fetched", totalFetched, add_stat, cookie);
            add_casted_stat("ep_tap_keepalive", tapKeepAlive, add_stat, cookie);
        }
        return ENGINE_SUCCESS;
    }

    ENGINE_ERROR_CODE store(const void *cookie,
                            item* itm,
                            uint64_t *cas,
                            ENGINE_STORE_OPERATION operation)
    {
        Item *it = static_cast<Item*>(itm);
        if (it->getCas() == 0) {
            it->setCas();
        }
        if (operation == OPERATION_SET) {
            backend->set(*it, ignoreCallback);
            *cas = it->getCas();
            addMutationEvent(it);
            return ENGINE_SUCCESS;
        } else if (operation == OPERATION_ADD) {
            item *i;
            if (get(cookie, &i, it->getKey().c_str(), it->nkey) == ENGINE_SUCCESS) {
                itemRelease(cookie, i);
                return ENGINE_NOT_STORED;
            } else {
                backend->set(*it, ignoreCallback);
                *cas = it->getCas();
                addMutationEvent(it);
                return ENGINE_SUCCESS;
            }
        } else if (operation == OPERATION_REPLACE) {
            item *i;
            if (get(cookie, &i, it->getKey().c_str(), it->nkey) == ENGINE_SUCCESS) {
                itemRelease(cookie, i);
                backend->set(*it, ignoreCallback);
                *cas = it->getCas();
                addMutationEvent(it);
                return ENGINE_SUCCESS;
            } else {
                return ENGINE_NOT_STORED;
            }
        } else {
            return ENGINE_ENOTSUP;
        }
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
                                 uint64_t *result)
    {
        // Arithmetic is supposed to be atomic, so we cannot let two threads
        // do this currently (for that we have to wait until we implement cas)
        LockHolder lh(arithmeticMutex);
        item *it = NULL;

        ENGINE_ERROR_CODE ret;
        ret = get(cookie, &it, key, nkey);
        if (ret == ENGINE_SUCCESS) {
            Item *item = static_cast<Item*>(it);
            char *endptr;
            uint64_t val = strtoull(item->getData(), &endptr, 10);
            if ((errno != ERANGE) && (isspace(*endptr) || (*endptr == '\0' && endptr != item->getData()))) {
                if (increment) {
                    val += delta;
                } else {
                    if (delta > val) {
                        val = 0;
                    } else {
                        val -= delta;
                    }
                }

                char value[80];
                size_t nb = snprintf(value, sizeof(value), "%llu\r\n",
                                     (unsigned long long)val);
                *result = val;
                Item *nit = new Item(key, (uint16_t)nkey, item->flags, exptime, value, nb);
                ret = store(cookie, nit, cas, OPERATION_SET);
                delete nit;
            } else {
                ret = ENGINE_EINVAL;
            }

            delete item;
        } else if (ret == ENGINE_KEY_ENOENT && create) {
            char value[80];
            size_t nb = snprintf(value, sizeof(value), "%llu\r\n",
                                 (unsigned long long)initial);
            *result = initial;
            Item *item = new Item(key, (uint16_t)nkey, 0, exptime, value, nb);
            ret = store(cookie, item, cas, OPERATION_ADD);
            delete item;
            if (ret == ENGINE_KEY_EEXISTS) {
                return arithmetic(cookie, key, nkey, increment, create, delta,
                                  initial, exptime, cas, result);
            }
        }

        return ret;
    }



    ENGINE_ERROR_CODE flush(const void *cookie, time_t when)
    {
        (void)cookie;
        ENGINE_ERROR_CODE ret= ENGINE_ENOTSUP;

        if (when == 0) {
            ;
#if 0
            epstore->reset();
            addFlushEvent();
#endif
        }

        return ret;
    }

    tap_event_t walkTapQueue(const void *cookie, item **itm, void **es,
                             uint16_t *nes, uint8_t *ttl, uint16_t *flags,
                             uint32_t *seqno) {
        LockHolder lh(tapNotifySync);
        TapConnection *connection = tapConnectionMap[cookie];
        assert(connection);
        tap_event_t ret = TAP_PAUSE;
        connection->paused = false;

        *es = NULL;
        *nes = 0;
        *ttl = (uint8_t)-1;
        *seqno = 0;
        *flags = 0;

        if (!connection->empty()) {
            std::string key = connection->next();
            lh.unlock();

            ENGINE_ERROR_CODE r;
            r = get(cookie, itm, key.c_str(), (int)key.length());
            if (r == ENGINE_SUCCESS) {
                ret = TAP_MUTATION;
            } else if (r == ENGINE_KEY_ENOENT) {
                ret = TAP_DELETION;
                r = itemAllocate(cookie, itm,
                                 key.c_str(), key.length(), 0, 0, 0);
                if (r != ENGINE_SUCCESS) {
                    std::cerr << "Failed to allocate memory for deletion of: "
                              << key.c_str() << std::endl;
                    ret = TAP_PAUSE;
                }
            }
        } else if (connection->shouldFlush()) {
            ret = TAP_FLUSH;
        } else {
            connection->paused = true;
        }

        return ret;
    }

    void createTapQueue(const void *cookie, std::string &client, uint32_t flags,
                        const void *userdata,
                        size_t nuserdata) {
        // map is set-assocative, so this will create an instance here..
        LockHolder lh(tapNotifySync);
        purgeExpiredTapConnections_UNLOCKED();

        std::string name = "eq_tapq:";
        if (client.length() == 0) {
            char buffer[32];
            snprintf(buffer, sizeof(buffer), "%p", cookie);
            name.append(buffer);
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
        if (tap != NULL && tapKeepAlive == 0) {
            std::cerr << "Forcing close of tap client " << name << std::endl;
            std::map<const void*, TapConnection*>::iterator miter;
            for (miter = tapConnectionMap.begin();
                 miter != tapConnectionMap.end();
                 ++miter) {
                if (miter->second == tap) {
                    break;
                }
            }
            assert(miter != tapConnectionMap.end());
            tapConnectionMap.erase(miter);
            purgeSingleExpiredTapConnection(tap);
            tap = NULL;
        }

        // @todo ensure that we don't have this client alredy
        // if so this should be a reconnect...
        if (tap == NULL) {
            TapConnection *tc = new TapConnection(name, flags);
            allTaps.push_back(tc);
            tapConnectionMap[cookie] = tc;

            if (flags & TAP_CONNECT_FLAG_BACKFILL) { /* */
                uint64_t age;
                assert(nuserdata >= sizeof(age));
                memcpy(&age, userdata, sizeof(age));
                tc->backfillAge = ntohll(age);
            }

            if (tc->backfillAge < (uint64_t)time(NULL)) {
                queueBackfill(tc);
            }
        } else {
            tapConnectionMap[cookie] = tap;
            tap->connected = true;
        }
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
                                size_t ndata)
    {
        (void)engine_specific;
        (void)nengine;
        (void)ttl;
        (void)tap_flags;
        (void)tap_seqno;

        switch (tap_event) {
        case TAP_FLUSH:
            return flush(cookie, 0);
        case TAP_DELETION:
        {
            std::string k(static_cast<const char*>(key), nkey);
            return itemDelete(cookie, k);
        }

        case TAP_MUTATION:
        {
            Item *item = new Item(key, (uint16_t)nkey, flags, exptime, data, ndata);
            /* @TODO we don't have CAS now.. we might in the future.. */
            (void)cas;
            uint64_t ncas;
            ENGINE_ERROR_CODE ret = store(cookie, item, &ncas, OPERATION_SET);
            delete item;
            return ret;
        }


        default:
            abort();
        }

        return ENGINE_SUCCESS;
    }


    /**
     * Visit the objects and add them to the tap connecitons queue.
     * @todo this code should honor the backfill time!
     */
    void queueBackfill(TapConnection *tc) {
        BackFillVisitor bfv(tc);
        epstore->visit(bfv);
    }

    void handleDisconnect(const void *cookie) {
        LockHolder lh(tapNotifySync);
        std::map<const void*, TapConnection*>::iterator iter;
        iter = tapConnectionMap.find(cookie);
        if (iter != tapConnectionMap.end()) {
            iter->second->expiry_time = serverApi.get_current_time()
                + (int)tapKeepAlive;
            iter->second->connected = false;
            tapConnectionMap.erase(iter);
        }
        purgeExpiredTapConnections_UNLOCKED();
    }

    protocol_binary_response_status stopFlusher(const char **msg) {
        protocol_binary_response_status rv = PROTOCOL_BINARY_RESPONSE_SUCCESS;
        *msg = NULL;
        if (epstore->getFlusherState() == RUNNING) {
            epstore->stopFlusher();
        } else {
            std::cerr << "Attempted to stop flusher in state "
                      << epstore->getFlusherState() << std::endl;
            *msg = "Flusher not running.";
            rv = PROTOCOL_BINARY_RESPONSE_EINVAL;
        }
        return rv;
    }

    protocol_binary_response_status startFlusher(const char **msg) {
        protocol_binary_response_status rv = PROTOCOL_BINARY_RESPONSE_SUCCESS;
        *msg = NULL;
        if (epstore->getFlusherState() == STOPPED) {
            epstore->startFlusher();
        } else {
            std::cerr << "Attempted to stop flusher in state "
                      << epstore->getFlusherState() << std::endl;
            *msg = "Flusher not shut down.";
            rv = PROTOCOL_BINARY_RESPONSE_EINVAL;
        }
        return rv;
    }

    void resetStats()
    {
        if (epstore) {
            epstore->resetStats();
        }

        // @todo reset statistics
    }

    void setMinDataAge(int to) {
        epstore->setMinDataAge(to);
    }

    ~EventuallyPersistentEngine() {
        delete epstore;
        delete sqliteDb;
    }

private:
    EventuallyPersistentEngine(SERVER_HANDLE_V1 *sApi);
    friend ENGINE_ERROR_CODE create_instance(uint64_t interface,
                                             GET_SERVER_API get_server_api,
                                             ENGINE_HANDLE **handle);

    friend void *EvpNotifyTapIo(void*arg);
    void notifyTapIoThread(void) {
        LockHolder lh(tapNotifySync);
        // Fix clean shutdown!!!
        while (!shutdown) {
            purgeExpiredTapConnections_UNLOCKED();
            // see if I have some channels that I have to signal..
            bool shouldPause = true;
            std::map<const void*, TapConnection*>::iterator iter;
            for (iter = tapConnectionMap.begin(); iter != tapConnectionMap.end(); iter++) {
                if (!iter->second->queue.empty()) {
                    shouldPause = false;
                }
            }

            if (shouldPause) {
                tapNotifySync.wait();
                purgeExpiredTapConnections_UNLOCKED();
            }

            // Create a copy of the list so that we can release the lock
            std::map<const void*, TapConnection*> tcm = tapConnectionMap;

            tapNotifySync.release();
            for (iter = tcm.begin(); iter != tcm.end(); iter++) {
                if (iter->second->paused) {
                    serverApi.notify_io_complete(iter->first, ENGINE_SUCCESS);
                }
            }
            tapNotifySync.aquire();
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

    void purgeExpiredTapConnections_UNLOCKED() {
        rel_time_t now = serverApi.get_current_time();
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
    }

    void addEvent(const std::string &str)
    {
        bool notify = false;
        LockHolder lh(tapNotifySync);

        std::list<TapConnection*>::iterator iter;
        for (iter = allTaps.begin(); iter != allTaps.end(); iter++) {
            TapConnection *tc = *iter;
            if (tc->addEvent(str) && tc->paused) {
                notify = true;
            }
        }

        if (notify) {
            tapNotifySync.notify();
        }
    }

    void addMutationEvent(Item *it) {
        // Currently we use the same queue for all kinds of events..
        addEvent(it->getKey());
    }

    void addDeleteEvent(const std::string &key) {
        // Currently we use the same queue for all kinds of events..
        addEvent(key);
    }

    void addFlushEvent() {
        LockHolder lh(tapNotifySync);
        bool notify = false;
        std::list<TapConnection*>::iterator iter;
        for (iter = allTaps.begin(); iter != allTaps.end(); iter++) {
            TapConnection *tc = *iter;
            tc->flush();
            notify = true;
        }
        if (notify) {
            tapNotifySync.notify();
        }
    }

    void add_casted_stat(const char *k, const char *v,
                         ADD_STAT add_stat, const void *cookie) {
        add_stat(k, static_cast<uint16_t>(strlen(k)),
                 v, static_cast<uint32_t>(strlen(v)), cookie);
    }

    void add_casted_stat(const char *k, size_t v,
                         ADD_STAT add_stat, const void *cookie) {
        char valS[32];
        snprintf(valS, sizeof(valS), "%lu", static_cast<unsigned long>(v));
        add_casted_stat(k, valS, add_stat, cookie);
    }

    void startEngineThreads(void);
    void stopEngineThreads(void) {
        LockHolder lh(tapNotifySync);
        shutdown = true;
        tapNotifySync.notify();
        tapNotifySync.release();
        pthread_join(notifyThreadId, NULL);
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

    const char *dbname;
    bool warmup;
    SERVER_HANDLE_V1 serverApi;
    IgnoreCallback ignoreCallback;
    KVStore *backend;
    MultiDBSqlite3 *sqliteDb;
    EventuallyPersistentStore *epstore;
    std::map<const void*, TapConnection*> tapConnectionMap;
    std::list<TapConnection*> allTaps;
    time_t databaseInitTime;
    size_t tapKeepAlive;
    pthread_t notifyThreadId;
    Mutex arithmeticMutex;
    SyncObject tapNotifySync;
    volatile bool shutdown;
};
