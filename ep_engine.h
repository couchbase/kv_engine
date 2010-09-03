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

#include "tapconnection.hh"


#define NUMBER_OF_SHARDS 4
#define DEFAULT_TAP_IDLE_TIMEOUT 600

#ifndef DEFAULT_MIN_DATA_AGE
#define DEFAULT_MIN_DATA_AGE 0
#endif

#ifndef DEFAULT_QUEUE_AGE_CAP
#define DEFAULT_QUEUE_AGE_CAP 900
#endif

extern "C" {
    EXPORT_FUNCTION
    ENGINE_ERROR_CODE create_instance(uint64_t interface,
                                      GET_SERVER_API get_server_api,
                                      ENGINE_HANDLE **handle);
    void *EvpNotifyTapIo(void*arg);
}


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

template <typename V>
class TapOperation {
public:
    virtual ~TapOperation() {}
    virtual void perform(TapConnection *tc, V arg) = 0;
};

class CompleteBackfillTapOperation : public TapOperation<void*> {
public:
    void perform(TapConnection *tc, void* arg);
};

class ReceivedItemTapOperation : public TapOperation<Item*> {
public:
    void perform(TapConnection *tc, Item* arg);
};

class CompletedBGFetchTapOperation : public TapOperation<EventuallyPersistentEngine*> {
public:
    void perform(TapConnection *tc, EventuallyPersistentEngine* arg);
};

/**
 *
 */
class EventuallyPersistentEngine : public ENGINE_HANDLE_V1 {
    friend class LookupCallback;
public:
    ENGINE_ERROR_CODE initialize(const char* config);
    void destroy();

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
                               ADD_STAT add_stat);
    void resetStats();


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
                             uint32_t *seqno, uint16_t *vbucket);

    void createTapQueue(const void *cookie,
                        std::string &client,
                        uint32_t flags,
                        const void *userdata,
                        size_t nuserdata);

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
                                uint16_t vbucket);

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
        lh.unlock();
        serverApi->core->notify_io_complete(cookie,
                                            ENGINE_DISCONNECT);
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

    template <typename V>
    void performTapOp(const std::string &name, TapOperation<V> &tapop, V arg) {
        bool notify(true);
        bool clear(true);
        LockHolder lh(tapNotifySync);

        TapConnection *tc = findTapConnByName_UNLOCKED(name);
        if (tc) {
            tapop.perform(tc, arg);
            notify = tc->paused; // notify if paused
            clear = tc->doDisconnect;
        }

        if (clear) {
            clearTapValidity(name);
        }

        if (notify) {
            tapNotifySync.notify();
        }
    }

    EventuallyPersistentStore* getEpStore() { return epstore; }

private:
    EventuallyPersistentEngine(GET_SERVER_API get_server_api);
    friend ENGINE_ERROR_CODE create_instance(uint64_t interface,
                                             GET_SERVER_API get_server_api,
                                             ENGINE_HANDLE **handle);
    tap_event_t doWalkTapQueue(const void *cookie, item **itm, void **es,
                               uint16_t *nes, uint8_t *ttl, uint16_t *flags,
                               uint32_t *seqno, uint16_t *vbucket,
                               TapConnection **c);


    ENGINE_ERROR_CODE processTapAck(const void *cookie,
                                    uint32_t seqno,
                                    uint16_t status,
                                    const std::string &msg);

    void notifyTapIoThreadMain(void);
    friend void *EvpNotifyTapIo(void*arg);
    void notifyTapIoThread(void);

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

    ENGINE_ERROR_CODE doEngineStats(const void *cookie, ADD_STAT add_stat);
    ENGINE_ERROR_CODE doVBucketStats(const void *cookie, ADD_STAT add_stat);
    ENGINE_ERROR_CODE doHashStats(const void *cookie, ADD_STAT add_stat);
    ENGINE_ERROR_CODE doTapStats(const void *cookie, ADD_STAT add_stat);
    ENGINE_ERROR_CODE doKeyStats(const void *cookie, ADD_STAT add_stat,
                                 const char *k, int nkey, bool validate=false);

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

#endif
