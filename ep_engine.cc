/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 NorthScale, Inc.
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

#include <limits>

#include "config.h"
#include <assert.h>
#include <fcntl.h>

#include <memcached/engine.h>
#include <memcached/protocol_binary.h>

#include "ep_engine.h"

Atomic<uint64_t> TapConnection::tapCounter(1);

/**
 * Helper function to avoid typing in the long cast all over the place
 * @param handle pointer to the engine
 * @return the engine as a class
 */
static inline EventuallyPersistentEngine* getHandle(ENGINE_HANDLE* handle)
{
    return reinterpret_cast<EventuallyPersistentEngine*>(handle);
}

void LookupCallback::callback(GetValue &value) {
    if (value.getStatus() == ENGINE_SUCCESS) {
        engine->addLookupResult(cookie, value.getValue());
    } else {
        engine->addLookupResult(cookie, NULL);
    }
    engine->getServerApi()->core->notify_io_complete(cookie, ENGINE_SUCCESS);
}

template <typename T>
static void validate(T v, T l, T h) {
    if (v < l || v > h) {
        throw std::runtime_error("value out of range.");
    }
}

// The Engine API specifies C linkage for the functions..
extern "C" {

    static const engine_info* EvpGetInfo(ENGINE_HANDLE* handle)
    {
        return getHandle(handle)->getInfo();
    }

    static ENGINE_ERROR_CODE EvpInitialize(ENGINE_HANDLE* handle,
                                           const char* config_str)
    {
        return getHandle(handle)->initialize(config_str);
    }

    static void EvpDestroy(ENGINE_HANDLE* handle)
    {
        getHandle(handle)->destroy();
        delete getHandle(handle);
    }

    static ENGINE_ERROR_CODE EvpItemAllocate(ENGINE_HANDLE* handle,
                                             const void* cookie,
                                             item **item,
                                             const void* key,
                                             const size_t nkey,
                                             const size_t nbytes,
                                             const int flags,
                                             const rel_time_t exptime)
    {
        return getHandle(handle)->itemAllocate(cookie, item, key,
                                               nkey, nbytes, flags, exptime);
    }

    static ENGINE_ERROR_CODE EvpItemDelete(ENGINE_HANDLE* handle,
                                           const void* cookie,
                                           const void* key,
                                           const size_t nkey,
                                           uint64_t cas,
                                           uint16_t vbucket)
    {
        return getHandle(handle)->itemDelete(cookie, key, nkey, cas, vbucket);
    }

    static void EvpItemRelease(ENGINE_HANDLE* handle,
                               const void *cookie,
                               item* item)
    {
        getHandle(handle)->itemRelease(cookie, item);
    }

    static ENGINE_ERROR_CODE EvpGet(ENGINE_HANDLE* handle,
                                    const void* cookie,
                                    item** item,
                                    const void* key,
                                    const int nkey,
                                    uint16_t vbucket)
    {
        return getHandle(handle)->get(cookie, item, key, nkey, vbucket);
    }

    static ENGINE_ERROR_CODE EvpGetStats(ENGINE_HANDLE* handle,
                                         const void* cookie,
                                         const char* stat_key,
                                         int nkey,
                                         ADD_STAT add_stat)
    {
        return getHandle(handle)->getStats(cookie, stat_key, nkey, add_stat);
    }

    static ENGINE_ERROR_CODE EvpStore(ENGINE_HANDLE* handle,
                                      const void *cookie,
                                      item* item,
                                      uint64_t *cas,
                                      ENGINE_STORE_OPERATION operation,
                                      uint16_t vbucket)
    {
        return getHandle(handle)->store(cookie, item, cas, operation, vbucket);
    }

    static ENGINE_ERROR_CODE EvpArithmetic(ENGINE_HANDLE* handle,
                                           const void* cookie,
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
        return getHandle(handle)->arithmetic(cookie, key, nkey, increment,
                                             create, delta, initial, exptime,
                                             cas, result, vbucket);
    }

    static ENGINE_ERROR_CODE EvpFlush(ENGINE_HANDLE* handle,
                                      const void* cookie, time_t when)
    {
        return getHandle(handle)->flush(cookie, when);
    }

    static void EvpResetStats(ENGINE_HANDLE* handle, const void *cookie)
    {
        (void)cookie;
        return getHandle(handle)->resetStats();
    }

    static protocol_binary_response_status stopFlusher(EventuallyPersistentEngine *e,
                                                       const char **msg) {
        return e->stopFlusher(msg);
    }

    static protocol_binary_response_status startFlusher(EventuallyPersistentEngine *e,
                                                        const char **msg) {
        return e->startFlusher(msg);
    }

    static protocol_binary_response_status setTapParam(EventuallyPersistentEngine *e,
                                                       const char *keyz, const char *valz,
                                                       const char **msg) {
        protocol_binary_response_status rv = PROTOCOL_BINARY_RESPONSE_SUCCESS;

#ifdef ENABLE_INTERNAL_TAP
        if (strcmp(keyz, "tap_peer") == 0) {
            e->setTapPeer(valz);
            *msg = "Updated";
        } else {
#endif
            *msg = "Unknown config param";
            rv = PROTOCOL_BINARY_RESPONSE_KEY_ENOENT;
#ifdef ENABLE_INTERNAL_TAP
        }
#else
        (void)e; (void)keyz; (void)valz;
#endif
        return rv;
    }

    static protocol_binary_response_status setFlushParam(EventuallyPersistentEngine *e,
                                                         const char *keyz, const char *valz,
                                                         const char **msg) {
        *msg = "Updated";
        protocol_binary_response_status rv = PROTOCOL_BINARY_RESPONSE_SUCCESS;

        // Handle the actual mutation.
        try {
            int v = atoi(valz);
            if (strcmp(keyz, "min_data_age") == 0) {
                validate(v, 0, MAX_DATA_AGE_PARAM);
                e->setMinDataAge(v);
            } else if (strcmp(keyz, "queue_age_cap") == 0) {
                validate(v, 0, MAX_DATA_AGE_PARAM);
                e->setQueueAgeCap(v);
            } else if (strcmp(keyz, "max_txn_size") == 0) {
                validate(v, 1, MAX_TXN_SIZE);
                e->setTxnSize(v);
            } else if (strcmp(keyz, "bg_fetch_delay") == 0) {
                validate(v, 0, MAX_BG_FETCH_DELAY);
                e->setBGFetchDelay(static_cast<uint32_t>(v));
            } else if (strcmp(keyz, "max_size") == 0) {
                // Want more bits than int.
                char *ptr = NULL;
                // TODO:  This parser isn't perfect.
                uint64_t vsize = strtoull(valz, &ptr, 10);
                validate(vsize, static_cast<uint64_t>(0),
                         std::numeric_limits<uint64_t>::max());
                EPStats &stats = e->getEpStats();
                stats.maxDataSize = vsize;

                stats.mem_low_wat = percentOf(StoredValue::getMaxDataSize(stats), 0.6);
                stats.mem_high_wat = percentOf(StoredValue::getMaxDataSize(stats), 0.75);
            } else {
                *msg = "Unknown config param";
                rv = PROTOCOL_BINARY_RESPONSE_KEY_ENOENT;
            }
        } catch(std::runtime_error ignored_exception) {
            *msg = "Value out of range.";
            rv = PROTOCOL_BINARY_RESPONSE_EINVAL;
        }

        return rv;
    }

    static protocol_binary_response_status evictKey(EventuallyPersistentEngine *e,
                                                    protocol_binary_request_header *request,
                                                    const char **msg) {
        protocol_binary_request_no_extras *req =
            (protocol_binary_request_no_extras*)request;

        char keyz[256];

        // Read the key.
        int keylen = ntohs(req->message.header.request.keylen);
        if (keylen >= (int)sizeof(keyz)) {
            *msg = "Key is too large.";
            return PROTOCOL_BINARY_RESPONSE_EINVAL;
        }
        memcpy(keyz, ((char*)request) + sizeof(req->message.header), keylen);
        keyz[keylen] = 0x00;

        uint16_t vbucket = ntohs(request->request.vbucket);

        std::string key(keyz, keylen);

        getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                         "Manually evicting object with key %s\n",
                         keyz);

        return e->evictKey(key, vbucket, msg);
    }

    static protocol_binary_response_status setParam(EventuallyPersistentEngine *e,
                                                    protocol_binary_request_header *request,
                                                    const char **msg) {
        protocol_binary_request_no_extras *req =
            (protocol_binary_request_no_extras*)request;

        char keyz[32];
        char valz[512];

        // Read the key.
        int keylen = ntohs(req->message.header.request.keylen);
        if (keylen >= (int)sizeof(keyz)) {
            *msg = "Key is too large.";
            return PROTOCOL_BINARY_RESPONSE_EINVAL;
        }
        memcpy(keyz, ((char*)request) + sizeof(req->message.header), keylen);
        keyz[keylen] = 0x00;

        // Read the value.
        size_t bodylen = ntohl(req->message.header.request.bodylen)
            - ntohs(req->message.header.request.keylen);
        if (bodylen >= sizeof(valz)) {
            *msg = "Value is too large.";
            return PROTOCOL_BINARY_RESPONSE_EINVAL;
        }
        memcpy(valz, (char*)request + sizeof(req->message.header)
               + keylen, bodylen);
        valz[bodylen] = 0x00;

        protocol_binary_response_status rv;

        switch (request->request.opcode) {
        case CMD_SET_FLUSH_PARAM:
            rv = setFlushParam(e, keyz, valz, msg);
            break;
        case CMD_SET_TAP_PARAM:
            rv = setTapParam(e, keyz, valz, msg);
            break;
        default:
            rv = PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND;
        }

        return rv;
    }

    static protocol_binary_response_status getVbucket(EventuallyPersistentEngine *e,
                                                      protocol_binary_request_header *request,
                                                      const char **msg) {
        protocol_binary_request_no_extras *req =
            reinterpret_cast<protocol_binary_request_no_extras*>(request);
        assert(req);

        char keyz[8]; // stringy 2^16 int

        // Read the key.
        int keylen = ntohs(req->message.header.request.keylen);
        if (keylen >= (int)sizeof(keyz)) {
            *msg = "Key is too large.";
            return PROTOCOL_BINARY_RESPONSE_EINVAL;
        }
        memcpy(keyz, ((char*)request) + sizeof(req->message.header), keylen);
        keyz[keylen] = 0x00;

        protocol_binary_response_status rv(PROTOCOL_BINARY_RESPONSE_SUCCESS);

        uint16_t vbucket = 0;
        if (!parseUint16(keyz, &vbucket)) {
            *msg = "Value out of range.";
            rv = PROTOCOL_BINARY_RESPONSE_EINVAL;
        } else {
            RCPtr<VBucket> vb = e->getVBucket(vbucket);
            if (!vb) {
                *msg = "That's not my bucket.";
                rv = PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET;
            } else {
                *msg = vb->getStateString();
                assert(msg);
                rv = PROTOCOL_BINARY_RESPONSE_SUCCESS;
            }
        }

        return rv;
    }

    static protocol_binary_response_status setVbucket(EventuallyPersistentEngine *e,
                                                      protocol_binary_request_header *request,
                                                      const char **msg) {
        protocol_binary_request_no_extras *req =
            reinterpret_cast<protocol_binary_request_no_extras*>(request);
        assert(req);

        char keyz[32];
        char valz[32];

        // Read the key.
        int keylen = ntohs(req->message.header.request.keylen);
        if (keylen >= (int)sizeof(keyz)) {
            *msg = "Key is too large.";
            return PROTOCOL_BINARY_RESPONSE_EINVAL;
        }
        memcpy(keyz, ((char*)request) + sizeof(req->message.header), keylen);
        keyz[keylen] = 0x00;

        // Read the value.
        size_t bodylen = ntohl(req->message.header.request.bodylen)
            - ntohs(req->message.header.request.keylen);
        if (bodylen >= sizeof(valz)) {
            *msg = "Value is too large.";
            return PROTOCOL_BINARY_RESPONSE_EINVAL;
        }
        memcpy(valz, (char*)request + sizeof(req->message.header)
               + keylen, bodylen);
        valz[bodylen] = 0x00;

        protocol_binary_response_status rv(PROTOCOL_BINARY_RESPONSE_SUCCESS);
        *msg = "Configured";

        vbucket_state_t state;
        if (strcmp(valz, "active") == 0) {
            state = active;
        } else if(strcmp(valz, "replica") == 0) {
            state = replica;
        } else if(strcmp(valz, "pending") == 0) {
            state = pending;
        } else if(strcmp(valz, "dead") == 0) {
            state = dead;
        } else {
            *msg = "Invalid state.";
            return PROTOCOL_BINARY_RESPONSE_EINVAL;
        }

        uint16_t vbucket = 0;
        if (!parseUint16(keyz, &vbucket)) {
            *msg = "Value out of range.";
            rv = PROTOCOL_BINARY_RESPONSE_EINVAL;
        } else {
            e->setVBucketState(vbucket, state);
        }

        return rv;
    }

    static protocol_binary_response_status deleteVBucket(EventuallyPersistentEngine *e,
                                                         protocol_binary_request_header *request,
                                                         const char **msg) {
        protocol_binary_request_no_extras *req =
            reinterpret_cast<protocol_binary_request_no_extras*>(request);
        assert(req);

        char keyz[8]; // stringy 2^16 int

        // Read the key.
        int keylen = ntohs(req->message.header.request.keylen);
        if (keylen >= (int)sizeof(keyz)) {
            *msg = "Key is too large.";
            return PROTOCOL_BINARY_RESPONSE_EINVAL;
        }
        memcpy(keyz, ((char*)request) + sizeof(req->message.header), keylen);
        keyz[keylen] = 0x00;

        protocol_binary_response_status rv(PROTOCOL_BINARY_RESPONSE_SUCCESS);

        uint16_t vbucket = 0;
        if (!parseUint16(keyz, &vbucket)) {
            *msg = "Value out of range.";
            rv = PROTOCOL_BINARY_RESPONSE_EINVAL;
        } else {
            if (e->deleteVBucket(vbucket)) {
                *msg = "Deleted.";
            } else {
                // If we fail to delete, try to figure out why.
                RCPtr<VBucket> vb = e->getVBucket(vbucket);
                if (!vb) {
                    *msg = "Failed to delete vbucket.  Bucket not found.";
                    rv = PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET;
                } else if(vb->getState() != dead) {
                    *msg = "Failed to delete vbucket.  Must be in the dead state.";
                    rv = PROTOCOL_BINARY_RESPONSE_EINVAL;
                } else {
                    *msg = "Failed to delete vbucket.  Unknown reason.";
                    rv = PROTOCOL_BINARY_RESPONSE_EINTERNAL;
                }
            }
        }

        assert(msg);
        return rv;
    }

    static ENGINE_ERROR_CODE EvpUnknownCommand(ENGINE_HANDLE* handle,
                                               const void* cookie,
                                               protocol_binary_request_header *request,
                                               ADD_RESPONSE response)
    {
        (void)handle;
        (void)cookie;
        (void)request;

        bool handled = true;
        protocol_binary_response_status res =
            PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND;
        const char *msg = NULL;

        EventuallyPersistentEngine *h = getHandle(handle);

        switch (request->request.opcode) {
        case CMD_STOP_PERSISTENCE:
            res = stopFlusher(h, &msg);
            break;
        case CMD_START_PERSISTENCE:
            res = startFlusher(h, &msg);
            break;
        case CMD_DEL_VBUCKET:
            res = deleteVBucket(h, request, &msg);
            break;
        case CMD_SET_FLUSH_PARAM:
        case CMD_SET_TAP_PARAM:
            res = setParam(h, request, &msg);
            break;
#ifdef ENABLE_INTERNAL_TAP
        case CMD_START_REPLICATION:
            if (h->startReplication()) {
                msg = "Started";
                res = PROTOCOL_BINARY_RESPONSE_SUCCESS;
            } else {
                msg = "Error - is the peer set?";
                res = PROTOCOL_BINARY_RESPONSE_EINVAL;
            }
            break;
        case CMD_STOP_REPLICATION:
            h->stopReplication();
            res = PROTOCOL_BINARY_RESPONSE_SUCCESS;
            msg = "Stopped";
            break;
#endif
        case CMD_GET_VBUCKET:
            res = getVbucket(h, request, &msg);
            break;
        case CMD_SET_VBUCKET:
            res = setVbucket(h, request, &msg);
            break;
        case CMD_EVICT_KEY:
            res = evictKey(h, request, &msg);
            break;
        default:
            /* unknown command */
            handled = false;
            break;
        }

        if (handled) {
            size_t msg_size = msg ? strlen(msg) : 0;
            response(msg, static_cast<uint16_t>(msg_size),
                     NULL, 0, NULL, 0,
                     PROTOCOL_BINARY_RAW_BYTES,
                     static_cast<uint16_t>(res), 0, cookie);
        } else {
            response(NULL, 0, NULL, 0, NULL, 0,
                     PROTOCOL_BINARY_RAW_BYTES,
                     PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND, 0, cookie);
        }

        return ENGINE_SUCCESS;
    }

    static void EvpItemSetCas(ENGINE_HANDLE* handle, const void *cookie,
                              item *item, uint64_t cas) {
        (void)handle;
        (void)cookie;
        static_cast<Item*>(item)->setCas(cas);
    }

    static ENGINE_ERROR_CODE EvpTapNotify(ENGINE_HANDLE* handle,
                                          const void *cookie,
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
        return getHandle(handle)->tapNotify(cookie, engine_specific, nengine,
                                            ttl, tap_flags, tap_event,
                                            tap_seqno, key, nkey, flags,
                                            exptime, cas, data, ndata,
                                            vbucket);
    }


    static tap_event_t EvpTapIterator(ENGINE_HANDLE* handle,
                                      const void *cookie, item **itm,
                                      void **es, uint16_t *nes, uint8_t *ttl,
                                      uint16_t *flags, uint32_t *seqno,
                                      uint16_t *vbucket) {
        return getHandle(handle)->walkTapQueue(cookie, itm, es, nes, ttl,
                                               flags, seqno, vbucket);
    }

    static TAP_ITERATOR EvpGetTapIterator(ENGINE_HANDLE* handle,
                                          const void* cookie,
                                          const void* client,
                                          size_t nclient,
                                          uint32_t flags,
                                          const void* userdata,
                                          size_t nuserdata) {
        std::string c(static_cast<const char*>(client), nclient);
        // Figure out what we want from the userdata before adding it to the API
        // to the handle
        getHandle(handle)->createTapQueue(cookie, c, flags,
                                          userdata, nuserdata);
        return EvpTapIterator;
    }

    void EvpHandleDisconnect(const void *cookie,
                             ENGINE_EVENT_TYPE type,
                             const void *event_data,
                             const void *cb_data)
    {
        assert(type == ON_DISCONNECT);
        assert(event_data == NULL);
        void *c = const_cast<void*>(cb_data);
        return getHandle(static_cast<ENGINE_HANDLE*>(c))->handleDisconnect(cookie);
    }


    /**
     * The only public interface to the eventually persistance engine.
     * Allocate a new instance and initialize it
     * @param interface the highest interface the server supports (we only support
     *                  interface 1)
     * @param get_server_api callback function to get the server exported API
     *                  functions
     * @param handle Where to return the new instance
     * @return ENGINE_SUCCESS on success
     */
    ENGINE_ERROR_CODE create_instance(uint64_t interface,
                                      GET_SERVER_API get_server_api,
                                      ENGINE_HANDLE **handle)
    {
        SERVER_HANDLE_V1 *api = get_server_api();
        if (interface != 1 || api == NULL) {
            return ENGINE_ENOTSUP;
        }

        EventuallyPersistentEngine *engine;
        engine = new struct EventuallyPersistentEngine(get_server_api);
        if (engine == NULL) {
            return ENGINE_ENOMEM;
        }

        ep_current_time = api->core->get_current_time;

        *handle = reinterpret_cast<ENGINE_HANDLE*> (engine);
        return ENGINE_SUCCESS;
    }

    void *EvpNotifyTapIo(void*arg) {
        static_cast<EventuallyPersistentEngine*>(arg)->notifyTapIoThread();
        return NULL;
    }

    static bool EvpGetItemInfo(ENGINE_HANDLE *handle, const void *cookie,
                               const item* item, item_info *item_info)
    {
        (void)handle;
        (void)cookie;
        const Item *it = reinterpret_cast<const Item*>(item);
        if (item_info->nvalue < 1) {
            return false;
        }
        item_info->cas = it->getCas();
        item_info->exptime = it->getExptime();
        item_info->nbytes = it->getNBytes();
        item_info->flags = it->getFlags();
        item_info->clsid = 0;
        item_info->nkey = static_cast<uint16_t>(it->getNKey());
        item_info->nvalue = 1;
        item_info->key = it->getKey().c_str();
        item_info->value[0].iov_base = const_cast<char*>(it->getData());
        item_info->value[0].iov_len = it->getNBytes();
        return true;
    }
} // C linkage

static SERVER_EXTENSION_API *extensionApi;

EXTENSION_LOGGER_DESCRIPTOR *getLogger(void) {
    if (extensionApi != NULL) {
        return (EXTENSION_LOGGER_DESCRIPTOR*)extensionApi->get_extension(EXTENSION_LOGGER);
    }

    return NULL;
}

EventuallyPersistentEngine::EventuallyPersistentEngine(GET_SERVER_API get_server_api) :
    dbname("/tmp/test.db"), initFile(NULL), warmup(true), wait_for_warmup(true),
    startVb0(true), sqliteDb(NULL), epstore(NULL), databaseInitTime(0),
    tapIdleTimeout(DEFAULT_TAP_IDLE_TIMEOUT), nextTapNoop(0),
    startedEngineThreads(false), shutdown(false),
    getServerApi(get_server_api), getlExtension(NULL),
#ifdef ENABLE_INTERNAL_TAP
    clientTap(NULL),
#endif
    tapEnabled(false), maxItemSize(20*1024*1024),
    memLowWat(std::numeric_limits<size_t>::max()),
    memHighWat(std::numeric_limits<size_t>::max()),
    minDataAge(DEFAULT_MIN_DATA_AGE),
    queueAgeCap(DEFAULT_QUEUE_AGE_CAP)
{
    interface.interface = 1;
    ENGINE_HANDLE_V1::get_info = EvpGetInfo;
    ENGINE_HANDLE_V1::initialize = EvpInitialize;
    ENGINE_HANDLE_V1::destroy = EvpDestroy;
    ENGINE_HANDLE_V1::allocate = EvpItemAllocate;
    ENGINE_HANDLE_V1::remove = EvpItemDelete;
    ENGINE_HANDLE_V1::release = EvpItemRelease;
    ENGINE_HANDLE_V1::get = EvpGet;
    ENGINE_HANDLE_V1::get_stats = EvpGetStats;
    ENGINE_HANDLE_V1::reset_stats = EvpResetStats;
    ENGINE_HANDLE_V1::store = EvpStore;
    ENGINE_HANDLE_V1::arithmetic = EvpArithmetic;
    ENGINE_HANDLE_V1::flush = EvpFlush;
    ENGINE_HANDLE_V1::unknown_command = EvpUnknownCommand;
    ENGINE_HANDLE_V1::get_tap_iterator = EvpGetTapIterator;
    ENGINE_HANDLE_V1::tap_notify = EvpTapNotify;
    ENGINE_HANDLE_V1::item_set_cas = EvpItemSetCas;
    ENGINE_HANDLE_V1::get_item_info = EvpGetItemInfo;
    ENGINE_HANDLE_V1::get_stats_struct = NULL;
    ENGINE_HANDLE_V1::errinfo = NULL;
    ENGINE_HANDLE_V1::aggregate_stats = NULL;

    serverApi = getServerApi();
    extensionApi = serverApi->extension;
    memset(&info, 0, sizeof(info));
    info.info.description = "EP engine v" VERSION;
    info.info.features[info.info.num_features++].feature = ENGINE_FEATURE_CAS;
    info.info.features[info.info.num_features++].feature = ENGINE_FEATURE_PERSISTENT_STORAGE;
    info.info.features[info.info.num_features++].feature = ENGINE_FEATURE_LRU;
}

void EventuallyPersistentEngine::startEngineThreads(void)
{
    assert(!startedEngineThreads);
    if (pthread_create(&notifyThreadId, NULL, EvpNotifyTapIo, this) != 0) {
        throw std::runtime_error("Error creating thread to notify Tap connections");
    }
    startedEngineThreads = true;
}

// These two methods are always called with a lock.
void EventuallyPersistentEngine::setTapValidity(const std::string &name,
                                                const void* token) {
    backfillValidityMap[name] = token;
}
void EventuallyPersistentEngine::clearTapValidity(const std::string &name) {
    backfillValidityMap.erase(name);
}

// This is always called without a lock.
bool EventuallyPersistentEngine::checkTapValidity(const std::string &name,
                                                  const void* token) {
    LockHolder lh(tapNotifySync);
    std::map<const std::string, const void*>::iterator viter =
        backfillValidityMap.find(name);
    return viter != backfillValidityMap.end() && viter->second == token;
}

class BackFillThreadData {
public:

    BackFillThreadData(EventuallyPersistentEngine *e, TapConnection *tc,
                       EventuallyPersistentStore *s, const void *tok):
        bfv(e, tc, tok), epstore(s) {
    }

    BackFillVisitor bfv;
    EventuallyPersistentStore *epstore;
};


extern "C" {
    static void* launch_backfill_thread(void *arg) {
        BackFillThreadData *bftd = static_cast<BackFillThreadData *>(arg);

        bftd->epstore->visit(bftd->bfv);
        bftd->bfv.apply();

        delete bftd;
        return NULL;
    }
}

void EventuallyPersistentEngine::queueBackfill(TapConnection *tc, const void *tok) {
    tc->doRunBackfill = false;
    BackFillThreadData *bftd = new BackFillThreadData(this, tc, epstore, tok);
    pthread_attr_t attr;

    if (pthread_attr_init(&attr) != 0 ||
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED) != 0) {
        throw std::runtime_error("Error setting up thread attributes");
    }

    pthread_t tid;
    if (pthread_create(&tid, &attr, launch_backfill_thread, bftd) != 0) {
        throw std::runtime_error("Error creating tap queue backfill thread");
    }

    pthread_attr_destroy(&attr);
}

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

template <typename T>
static void addTapStat(const char *name, const TapConnection *tc, T val,
                       ADD_STAT add_stat, const void *cookie) {
    char tap[80];
    assert(strlen(name) + tc->getName().size() + 2 < sizeof(tap));
    sprintf(tap, "%s:%s", tc->getName().c_str(), name);
    add_casted_stat(tap, val, add_stat, cookie);
}

static void addTapStat(const char *name, const TapConnection *tc, bool val,
                       ADD_STAT add_stat, const void *cookie) {
    addTapStat(name, tc, val ? "true" : "false", add_stat, cookie);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doEngineStats(const void *cookie,
                                                            ADD_STAT add_stat) {
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
    add_casted_stat("ep_commit_num", epstats.flusherCommits,
                    add_stat, cookie);
    add_casted_stat("ep_commit_time",
                    epstats.commit_time, add_stat, cookie);
    add_casted_stat("ep_commit_time_total",
                    epstats.cumulativeFlushTime, add_stat, cookie);
    add_casted_stat("ep_vbucket_del",
                    epstats.vbucketDeletions, add_stat, cookie);
    add_casted_stat("ep_vbucket_del_fail",
                    epstats.vbucketDeletionFail, add_stat, cookie);
    add_casted_stat("ep_flush_preempts",
                    epstats.flusherPreempts, add_stat, cookie);
    add_casted_stat("ep_flush_duration",
                    epstats.flushDuration, add_stat, cookie);
    add_casted_stat("ep_flush_duration_total",
                    epstats.cumulativeFlushTime, add_stat, cookie);
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
    add_casted_stat("ep_oom_errors", stats.oom_errors, add_stat, cookie);
    add_casted_stat("ep_storage_type",
                    HashTable::getDefaultStorageValueTypeStr(),
                    add_stat, cookie);
    add_casted_stat("ep_bg_fetched", epstats.bg_fetched, add_stat,
                    cookie);
    add_casted_stat("ep_num_pager_runs", epstats.pagerRuns, add_stat,
                    cookie);
    add_casted_stat("ep_num_value_ejects", epstats.numValueEjects, add_stat,
                    cookie);
    add_casted_stat("ep_num_eject_failures", epstats.numFailedEjects, add_stat,
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

    add_casted_stat("ep_pending_ops", epstats.pendingOps, add_stat, cookie);
    add_casted_stat("ep_pending_ops_total", epstats.pendingOpsTotal,
                    add_stat, cookie);
    add_casted_stat("ep_pending_ops_max", epstats.pendingOpsMax, add_stat, cookie);
    add_casted_stat("ep_pending_ops_max_duration",
                    hrtime2text(epstats.pendingOpsMaxDuration).c_str(),
                    add_stat, cookie);

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

ENGINE_ERROR_CODE EventuallyPersistentEngine::doVBucketStats(const void *cookie,
                                                             ADD_STAT add_stat) {
    StatVBucketVisitor svbv(cookie, add_stat);
    epstore->visit(svbv);
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doHashStats(const void *cookie,
                                                          ADD_STAT add_stat) {
    HashTableDepthStatVisitor depthVisitor;
    epstore->visitDepth(depthVisitor);
    add_casted_stat("ep_hash_bucket_size", epstore->getHashSize(),
                    add_stat, cookie);
    add_casted_stat("ep_hash_num_locks", epstore->getHashLocks(),
                    add_stat, cookie);
    add_casted_stat("ep_hash_min_depth", depthVisitor.min, add_stat, cookie);
    add_casted_stat("ep_hash_max_depth", depthVisitor.max, add_stat, cookie);
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doTapStats(const void *cookie,
                                                         ADD_STAT add_stat) {
    std::list<TapConnection*>::iterator iter;
    int totalTaps = 0;
    size_t tap_queue = 0;
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
        addTapStat("has_item", tc, tc->hasItem(), add_stat, cookie);
        addTapStat("has_queued_item", tc, tc->hasQueuedItem(), add_stat, cookie);
        addTapStat("bg_queue_size", tc, tc->bgQueueSize, add_stat, cookie);
        addTapStat("bg_queued", tc, tc->bgQueued, add_stat, cookie);
        addTapStat("bg_result_size", tc, tc->bgResultSize, add_stat, cookie);
        addTapStat("bg_results", tc, tc->bgResults, add_stat, cookie);
        addTapStat("bg_jobs_issued", tc, tc->bgJobIssued, add_stat, cookie);
        addTapStat("bg_jobs_completed", tc, tc->bgJobCompleted, add_stat, cookie);
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

        tap_queue += tc->queue->size();

        if (tc->ackSupported) {
            addTapStat("ack_seqno", tc, tc->seqno, add_stat, cookie);
            addTapStat("recv_ack_seqno", tc, tc->seqnoReceived,
                       add_stat, cookie);
            addTapStat("ack_log_size", tc, tc->tapLog.size(), add_stat,
                       cookie);
            addTapStat("ack_window_full", tc, tc->windowIsFull(), add_stat,
                       cookie);
            if (tc->windowIsFull()) {
                addTapStat("expires", tc,
                           tc->expiry_time - ep_current_time(),
                           add_stat, cookie);
            }
        }
    }

    add_casted_stat("ep_tap_total_queue", tap_queue, add_stat, cookie);
    add_casted_stat("ep_tap_total_fetched", stats.numTapFetched, add_stat, cookie);
    add_casted_stat("ep_tap_keepalive", tapKeepAlive, add_stat, cookie);

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

    add_casted_stat("ep_tap_ack_window_size", TapConnection::ackWindowSize,
                    add_stat, cookie);
    add_casted_stat("ep_tap_ack_high_chunk_threshold",
                    TapConnection::ackHighChunkThreshold,
                    add_stat, cookie);
    add_casted_stat("ep_tap_ack_medium_chunk_threshold",
                    TapConnection::ackMediumChunkThreshold,
                    add_stat, cookie);
    add_casted_stat("ep_tap_ack_low_chunk_threshold",
                    TapConnection::ackLowChunkThreshold,
                    add_stat, cookie);
    add_casted_stat("ep_tap_ack_grace_period",
                    TapConnection::ackGracePeriod,
                    add_stat, cookie);

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doKeyStats(const void *cookie,
                                                         ADD_STAT add_stat,
                                                         const char *k,
                                                         int nkey,
                                                         bool validate) {
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

ENGINE_ERROR_CODE EventuallyPersistentEngine::getStats(const void* cookie,
                                                       const char* stat_key,
                                                       int nkey,
                                                       ADD_STAT add_stat) {
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

void EventuallyPersistentEngine::resetStats()
{
    stats.tooYoung.set(0);
    stats.tooOld.set(0);
    stats.dirtyAge.set(0);
    stats.dirtyAgeHighWat.set(0);
    stats.flushDuration.set(0);
    stats.flushDurationHighWat.set(0);
    stats.commit_time.set(0);
    stats.pagerRuns.set(0);
    stats.numValueEjects.set(0);
    stats.numFailedEjects.set(0);
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
    stats.pendingOps.set(0);
    stats.pendingOpsTotal.set(0);
    stats.pendingOpsMax.set(0);
    stats.pendingOpsMaxDuration.set(0);
    stats.numTapFetched.set(0);
}

void CompleteBackfillTapOperation::perform(TapConnection *tc, void *arg) {
    (void)arg;
    tc->completeBackfill();
}

void ReceivedItemTapOperation::perform(TapConnection *tc, Item *arg) {
    tc->gotBGItem(arg);
}

void CompletedBGFetchTapOperation::perform(TapConnection *tc,
                                           EventuallyPersistentEngine *epe) {
    tc->completedBGFetchJob(epe);
}
