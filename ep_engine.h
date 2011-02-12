/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef EP_ENGINE_H
#define EP_ENGINE_H 1

#include "locks.hh"
#include "ep.hh"
#include "flusher.hh"
#include "kvstore.hh"
#include "ep_extension.h"
#include "dispatcher.hh"
#include "item_pager.hh"
#include "sync_registry.hh"

#include <cstdio>
#include <map>
#include <list>
#include <sstream>
#include <algorithm>
#include <errno.h>
#include <limits>

#include "command_ids.h"

#include "tapconnmap.hh"
#include "tapconnection.hh"


#define DEFAULT_TAP_NOOP_INTERVAL 200

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

    EXPORT_FUNCTION
    ENGINE_ERROR_CODE getLocked(EventuallyPersistentEngine *e,
            protocol_binary_request_header *request,
            const void *cookie,
            Item **item,
            const char **msg,
            size_t *msg_size,
            protocol_binary_response_status *res);
}

/* We're using notify_io_complete from ptr_fun, but that func
 * got a "C" linkage that ptr_fun doesn't like... just
 * cast it away with this typedef ;)
 */
typedef void (*NOTIFY_IO_COMPLETE_T)(const void *cookie,
                                     ENGINE_ERROR_CODE status);


// Forward decl
class EventuallyPersistentEngine;
class TapConnMap;
class TapThrottle;

/**
 * Base storage callback for things that look up data.
 */
class LookupCallback : public Callback<GetValue> {
public:
    LookupCallback(EventuallyPersistentEngine *e, const void* c) :
        engine(e), cookie(c) {}

    virtual void callback(GetValue &value);
private:
    EventuallyPersistentEngine *engine;
    const void *cookie;
};

/**
 * Vbucket visitor that counts active vbuckets.
 */
class VBucketCountVisitor : public VBucketVisitor {
public:
    VBucketCountVisitor(vbucket_state_t state) : desired_state(state),
                            requestedState(0), total(0), nonResident(0), totalNonResident(0),
                            numVbucket(0), htMemory(0), htItemMemory(0), numEjects(0),
                            opsCreate(0), opsUpdate(0), opsDelete(0), opsReject(0),
                            queueSize(0), queueMemory(0), queueAge(0),
                            queueFill(0), queueDrain(0), pendingWrites(0)
    { }

    bool visitBucket(RCPtr<VBucket> vb);

    void visit(StoredValue* v) {
        (void)v;
        assert(false); // this does not happen
    }

    vbucket_state_t getVBucketState() { return desired_state; }

    size_t getTotal() { return total; }

    size_t getRequested() { return requestedState; }

    size_t getNonResident() { return nonResident; }

    size_t getTotalNonResident() { return totalNonResident; }

    size_t getVBucketNumber() { return numVbucket; }

    size_t getMemResidentPer() { // Do we really need to provide?
        size_t tol = requestedState + nonResident;
        return (tol != 0) ? (size_t) (requestedState *100.0) / (tol) : 0;
        //return 0;
    }

    size_t getEjects() { return numEjects; }

    size_t getHashtableMemory() { return htMemory; }

    size_t getItemMemory() { return htItemMemory; }

    size_t getOpsCreate() { return opsCreate; }
    size_t getOpsUpdate() { return opsUpdate; }
    size_t getOpsDelete() { return opsDelete; }
    size_t getOpsReject() { return opsReject; }

    size_t getQueueSize() { return queueSize; }
    size_t getQueueMemory() { return queueMemory; }
    size_t getQueueFill() { return queueFill; }
    size_t getQueueDrain() { return queueDrain; }
    uint64_t   getAge() { return queueAge; }
    size_t getPendingWrites() { return pendingWrites; }
private:
    vbucket_state_t desired_state;

    size_t requestedState;
    size_t total;
    size_t nonResident;
    size_t totalNonResident;
    size_t numVbucket;
    size_t htMemory;
    size_t htItemMemory;
    size_t numEjects;

    size_t opsCreate;
    size_t opsUpdate;
    size_t opsDelete;
    size_t opsReject;

    size_t queueSize;
    size_t queueMemory;
    uint64_t queueAge;
    size_t queueFill;
    size_t queueDrain;
    size_t pendingWrites;
};

/**
 * memcached engine interface to the EventuallyPersistentStore.
 */
class EventuallyPersistentEngine : public ENGINE_HANDLE_V1 {
    friend class LookupCallback;
public:
    ENGINE_ERROR_CODE initialize(const char* config);
    void destroy(bool force);

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

        time_t expiretime = (exptime == 0) ? 0 : ep_abs_time(ep_reltime(exptime));

        *item = new Item(key, nkey, nbytes, flags, expiretime);
        if (*item == NULL) {
            return memoryCondition();
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
        std::string k(static_cast<const char*>(key), nkey);
        return itemDelete(cookie, k, cas, vbucket);
    }

    ENGINE_ERROR_CODE itemDelete(const void* cookie,
                                 const std::string &key,
                                 uint64_t cas,
                                 uint16_t vbucket)
    {
        ENGINE_ERROR_CODE ret = epstore->del(key, cas, vbucket, cookie);

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
        BlockTimer timer(&stats.getCmdHisto);
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

    void resetStats() { stats.reset(); }

    ENGINE_ERROR_CODE store(const void *cookie,
                            item* itm,
                            uint64_t *cas,
                            ENGINE_STORE_OPERATION operation,
                            uint16_t vbucket);

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
        BlockTimer timer(&stats.arithCmdHisto);
        item *it = NULL;

        rel_time_t expiretime = (exptime == 0 ||
                                 exptime == 0xffffffff) ?
            0 : ep_abs_time(ep_reltime(exptime));

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
            if (item->getCas() == (uint64_t) -1) {
                // item is locked, can't perform arithmetic operation
                delete item;
                return ENGINE_TMPFAIL;
            }
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
                vals << val;
                size_t nb = vals.str().length();
                *result = val;
                Item *nit = new Item(key, (uint16_t)nkey, item->getFlags(),
                                     item->getExptime(), vals.str().c_str(), nb);
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

            vals << initial;
            size_t nb = vals.str().length();

            *result = initial;
            Item *item = new Item(key, (uint16_t)nkey, 0, expiretime,
                                  vals.str().c_str(), nb);
            ret = store(cookie, item, cas, OPERATION_ADD, vbucket);
            delete item;
        }

        /* We had a race condition.. just call ourself recursively to retry */
        if (ret == ENGINE_KEY_EEXISTS) {
            return arithmetic(cookie, key, nkey, increment, create, delta,
                              initial, expiretime, cas, result, vbucket);
        }

        return ret;
    }



    ENGINE_ERROR_CODE flush(const void *cookie, time_t when);

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

    ENGINE_ERROR_CODE touch(const void* cookie,
                            protocol_binary_request_header *request,
                            ADD_RESPONSE response);

    /**
     * Visit the objects and add them to the tap connecitons queue.
     * @todo this code should honor the backfill time!
     */
    void queueBackfill(TapProducer *tc, const void *tok);

    void notifyIOComplete(const void *cookie, ENGINE_ERROR_CODE status) {
        BlockTimer bt(&stats.notifyIOHisto);
        serverApi->cookie->notify_io_complete(cookie, status);
    }

    template <typename T>
    void notifyIOComplete(T cookies, ENGINE_ERROR_CODE status) {
        std::for_each(cookies.begin(), cookies.end(),
                      std::bind2nd(std::ptr_fun((NOTIFY_IO_COMPLETE_T)serverApi->cookie->notify_io_complete),
                                   status));
    }

    void handleDisconnect(const void *cookie) {
        tapConnMap.disconnect(cookie, static_cast<int>(tapKeepAlive));
        serverApi->cookie->store_engine_specific(cookie, NULL);
        notifyIOComplete(cookie, ENGINE_DISCONNECT);
    }

    protocol_binary_response_status stopFlusher(const char **msg, size_t *msg_size) {
        (void) msg_size;
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

    protocol_binary_response_status startFlusher(const char **msg, size_t *msg_size) {
        (void) msg_size;
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

    protocol_binary_response_status evictKey(const std::string &key,
                                             uint16_t vbucket,
                                             const char **msg,
                                             size_t *msg_size) {
        return epstore->evictKey(key, vbucket, msg, msg_size);
    }

    bool getLocked(const std::string &key,
                   uint16_t vbucket,
                   Callback<GetValue> &cb,
                   rel_time_t currentTime,
                   uint32_t lockTimeout,
                   const void *cookie) {
        return epstore->getLocked(key, vbucket, cb, currentTime, lockTimeout, cookie);
    }

    size_t sync(std::set<KeySpec> keySpecs, const void *cookie);

    ENGINE_ERROR_CODE unlockKey(const std::string &key,
                                uint16_t vbucket,
                                uint64_t cas,
                                rel_time_t currentTime) {
        return epstore->unlockKey(key, vbucket, cas, currentTime);
    }

    RCPtr<VBucket> getVBucket(uint16_t vbucket) {
        return epstore->getVBucket(vbucket);
    }

    void setVBucketState(uint16_t vbid, vbucket_state_t to) {
        epstore->setVBucketState(vbid, to);
    }

    ~EventuallyPersistentEngine() {
        delete epstore;
        delete kvstore;
        delete getlExtension;
    }

    engine_info *getInfo() {
        return &info.info;
    }

    EPStats &getEpStats() {
        return stats;
    }

    EventuallyPersistentStore* getEpStore() { return epstore; }

    size_t getItemExpiryWindow() const {
        return itemExpiryWindow;
    }

    size_t getVbDelChunkSize() const {
        return vb_del_chunk_size;
    }

    size_t getVbChunkDelThresholdTime() const {
        return vb_chunk_del_threshold_time;
    }

    bool isForceShutdown(void) const {
        return forceShutdown;
    }

    SERVER_HANDLE_V1* getServerApi() { return serverApi; }

private:
    EventuallyPersistentEngine(GET_SERVER_API get_server_api);
    friend ENGINE_ERROR_CODE create_instance(uint64_t interface,
                                             GET_SERVER_API get_server_api,
                                             ENGINE_HANDLE **handle);
    tap_event_t doWalkTapQueue(const void *cookie, item **itm, void **es,
                               uint16_t *nes, uint8_t *ttl, uint16_t *flags,
                               uint32_t *seqno, uint16_t *vbucket,
                               TapProducer *c, bool &retry);


    ENGINE_ERROR_CODE processTapAck(const void *cookie,
                                    uint32_t seqno,
                                    uint16_t status,
                                    const std::string &msg);

    /**
     * Report the state of a memory condition when out of memory.
     *
     * @return ETMPFAIL if we think we can recover without interaction,
     *         else ENOMEM
     */
    ENGINE_ERROR_CODE memoryCondition() {
        // Do we think it's possible we could free something?
        bool haveEvidenceWeCanFreeMemory(stats.maxDataSize > stats.memOverhead);
        if (haveEvidenceWeCanFreeMemory) {
            // Look for more evidence by seeing if we have resident items.
            VBucketCountVisitor countVisitor(vbucket_state_active);
            epstore->visit(countVisitor);

            haveEvidenceWeCanFreeMemory = countVisitor.getTotalNonResident() <
                                          countVisitor.getTotal();
        }
        if (haveEvidenceWeCanFreeMemory) {
            ++stats.tmp_oom_errors;
            return ENGINE_TMPFAIL;
        } else {
            ++stats.oom_errors;
            return ENGINE_ENOMEM;
        }
    }

    void notifyTapIoThreadMain(void);
    friend void *EvpNotifyTapIo(void*arg);
    void notifyTapIoThread(void);

    bool populateEvents();

    friend class BackFillVisitor;
    friend class TapBGFetchCallback;
    friend class TapConnMap;
    friend class EventuallyPersistentStore;

    void addEvent(const std::string &str, uint16_t vbid,
                  enum queue_operation op) {
        pendingTapNotifications.push(QueuedItem(str, vbid, op));
    }

    void addMutationEvent(Item *it, uint16_t vbid) {
        // Currently we use the same queue for all kinds of events..
        addEvent(it->getKey(), vbid, queue_op_set);
    }

    void addDeleteEvent(const std::string &key, uint16_t vbid) {
        // Currently we use the same queue for all kinds of events..
        addEvent(key, vbid, queue_op_del);
    }

    void startEngineThreads(void);
    void stopEngineThreads(void) {
        if (startedEngineThreads) {
            shutdown = true;
            tapConnMap.notify();
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
    ENGINE_ERROR_CODE doTimingStats(const void *cookie, ADD_STAT add_stat);
    ENGINE_ERROR_CODE doDispatcherStats(const void *cookie, ADD_STAT add_stat);
    ENGINE_ERROR_CODE doKeyStats(const void *cookie, ADD_STAT add_stat,
                                 uint16_t vbid, std::string &key, bool validate=false);

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

    KVStore *newKVStore();

    // Get the current tap connection for this cookie.
    // If this method returns NULL, you should return TAP_DISCONNECT
    TapProducer* getTapProducer(const void *cookie);

    const char *dbname;
    const char *shardPattern;
    const char *initFile;
    const char *postInitFile;
    enum db_type dbStrategy;
    bool warmup;
    bool wait_for_warmup;
    bool fail_on_partial_warmup;
    bool startVb0;
    bool concurrentDB;
    bool forceShutdown;
    SERVER_HANDLE_V1 *serverApi;
    KVStore *kvstore;
    EventuallyPersistentStore *epstore;
    TapThrottle *tapThrottle;
    std::map<const void*, Item*> lookups;
    Mutex lookupMutex;
    time_t databaseInitTime;
    size_t tapKeepAlive;
    size_t tapNoopInterval;
    size_t nextTapNoop;
    pthread_t notifyThreadId;
    bool startedEngineThreads;
    AtomicQueue<QueuedItem> pendingTapNotifications;
    volatile bool shutdown;
    GET_SERVER_API getServerApiFunc;
    union {
        engine_info info;
        char buffer[sizeof(engine_info) + 10 * sizeof(feature_info) ];
    } info;
    GetlExtension *getlExtension;

    TapConnMap tapConnMap;
    Mutex tapMutex;
    size_t maxItemSize;
    size_t tapBacklogLimit;
    size_t memLowWat;
    size_t memHighWat;
    size_t minDataAge;
    size_t queueAgeCap;
    size_t itemExpiryWindow;
    size_t expiryPagerSleeptime;
    size_t nVBuckets;
    size_t dbShards;
    size_t vb_del_chunk_size;
    size_t vb_chunk_del_threshold_time;
    EPStats stats;
    SyncRegistry syncRegistry;
};

#endif
