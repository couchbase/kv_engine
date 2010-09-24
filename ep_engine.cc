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

static size_t percentOf(size_t val, double percent) {
    return static_cast<size_t>(static_cast<double>(val) * percent);
}

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
    engine->getServerApi()->cookie->notify_io_complete(cookie, value.getStatus());
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
        (void)e; (void)keyz; (void)valz;
        protocol_binary_response_status rv = PROTOCOL_BINARY_RESPONSE_SUCCESS;

        *msg = "Unknown config param";
        rv = PROTOCOL_BINARY_RESPONSE_KEY_ENOENT;
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
            } else if (strcmp(keyz, "mem_low_wat") == 0) {
                // Want more bits than int.
                char *ptr = NULL;
                // TODO:  This parser isn't perfect.
                uint64_t vsize = strtoull(valz, &ptr, 10);
                validate(vsize, static_cast<uint64_t>(0),
                         std::numeric_limits<uint64_t>::max());
                EPStats &stats = e->getEpStats();
                stats.mem_low_wat = vsize;
            } else if (strcmp(keyz, "mem_high_wat") == 0) {
                // Want more bits than int.
                char *ptr = NULL;
                // TODO:  This parser isn't perfect.
                uint64_t vsize = strtoull(valz, &ptr, 10);
                validate(vsize, static_cast<uint64_t>(0),
                         std::numeric_limits<uint64_t>::max());
                EPStats &stats = e->getEpStats();
                stats.mem_high_wat = vsize;
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

    static void EvpHandleDisconnect(const void *cookie,
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
    fail_on_partial_warmup(true),
    startVb0(true), sqliteStrategy(NULL), sqliteDb(NULL), epstore(NULL),
    databaseInitTime(0), tapIdleTimeout(DEFAULT_TAP_IDLE_TIMEOUT), nextTapNoop(0),
    startedEngineThreads(false), shutdown(false),
    getServerApiFunc(get_server_api), getlExtension(NULL),
    tapEnabled(false), maxItemSize(20*1024*1024), tapBacklogLimit(250000),
    memLowWat(std::numeric_limits<size_t>::max()),
    memHighWat(std::numeric_limits<size_t>::max()),
    minDataAge(DEFAULT_MIN_DATA_AGE),
    queueAgeCap(DEFAULT_QUEUE_AGE_CAP),
    itemExpiryWindow(3)
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

    serverApi = getServerApiFunc();
    extensionApi = serverApi->extension;
    memset(&info, 0, sizeof(info));
    info.info.description = "EP engine v" VERSION;
    info.info.features[info.info.num_features++].feature = ENGINE_FEATURE_CAS;
    info.info.features[info.info.num_features++].feature = ENGINE_FEATURE_PERSISTENT_STORAGE;
    info.info.features[info.info.num_features++].feature = ENGINE_FEATURE_LRU;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::initialize(const char* config) {
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    resetStats();
    if (config != NULL) {
        char *dbn = NULL, *initf = NULL, *svaltype = NULL;
        size_t htBuckets = 0;
        size_t htLocks = 0;
        size_t maxSize = 0;

        const int max_items = 22;
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
        items[ii].key = "failpartialwarmup";
        items[ii].datatype = DT_BOOL;
        items[ii].value.dt_bool = &fail_on_partial_warmup;

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

        ++ii;
        items[ii].key = "cache_size";
        items[ii].datatype = DT_SIZE;
        items[ii].value.dt_size = &maxSize;

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
        items[ii].key = "tap_backlog_limit";
        items[ii].datatype = DT_SIZE;
        items[ii].value.dt_size = &tapBacklogLimit;

        ++ii;
        items[ii].key = "expiry_window";
        items[ii].datatype = DT_SIZE;
        items[ii].value.dt_size = &itemExpiryWindow;

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
            sqliteStrategy = new MultiDBSqliteStrategy(*this, dbname,
                                                       initFile,
                                                       NUMBER_OF_SHARDS);
            sqliteDb = new StrategicSqlite3(*this, sqliteStrategy);
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

        if (memLowWat == std::numeric_limits<size_t>::max()) {
            memLowWat = percentOf(StoredValue::getMaxDataSize(stats), 0.6);
        }
        if (memHighWat == std::numeric_limits<size_t>::max()) {
            memHighWat = percentOf(StoredValue::getMaxDataSize(stats), 0.75);
        }

        stats.mem_low_wat = memLowWat;
        stats.mem_high_wat = memHighWat;

        startEngineThreads();

        // If requested, don't complete the initialization until the
        // flusher transitions out of the initializing state (i.e
        // warmup is finished).
        const Flusher *flusher = epstore->getFlusher();
        if (wait_for_warmup && flusher) {
            while (flusher->state() == initializing) {
                sleep(1);
            }
            if (fail_on_partial_warmup && stats.warmOOM > 0) {
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "Warmup failed to load %d records due to OOM, exiting.\n",
                                 static_cast<unsigned int>(stats.warmOOM));
                exit(1);
            }
        }

        if (HashTable::getDefaultStorageValueType() != small) {
            shared_ptr<DispatcherCallback> cb(new ItemPager(epstore, stats));
            epstore->getDispatcher()->schedule(cb, NULL, Priority::ItemPagerPriority, 10);
        }
    }

    if (ret == ENGINE_SUCCESS) {
        getlExtension = new GetlExtension(epstore, getServerApiFunc);
        getlExtension->initialize();
    }

    getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "Engine init complete.\n");

    return ret;
}

void EventuallyPersistentEngine::destroy() {
    stopEngineThreads();
}


ENGINE_ERROR_CODE  EventuallyPersistentEngine::store(const void *cookie,
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
        ret = epstore->add(*it, cookie);
        if (ret == ENGINE_SUCCESS) {
            *cas = it->getCas();
            addMutationEvent(it, vbucket);
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

    if (ret == ENGINE_ENOMEM) {
        ret = memoryCondition();
    }

    return ret;
}

inline tap_event_t EventuallyPersistentEngine::doWalkTapQueue(const void *cookie,
                                                              item **itm,
                                                              void **es,
                                                              uint16_t *nes,
                                                              uint8_t *ttl,
                                                              uint16_t *flags,
                                                              uint32_t *seqno,
                                                              uint16_t *vbucket,
                                                              TapConnection **c) {
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

    *c = connection;

    if (connection->doRunBackfill) {
        queueBackfill(connection, cookie);
    }

    tap_event_t ret = TAP_PAUSE;

    *es = NULL;
    *nes = 0;
    *ttl = (uint8_t)-1;
    *seqno = 0;
    *flags = 0;

    if (connection->windowIsFull()) {
        return TAP_PAUSE;
    }

    TapVBucketEvent ev = connection->nextVBucketHighPriority();
    if (ev.event != TAP_PAUSE) {
        assert(ev.event == TAP_VBUCKET_SET || ev.event == TAP_NOOP);
        connection->encodeVBucketStateTransition(ev, es, nes, vbucket);
        return ev.event;
    }

    if (connection->hasItem()) {
        ret = TAP_MUTATION;
        Item *item = connection->nextFetchedItem();
        lh.unlock();

        ++stats.numTapBGFetched;

        // If there's a better version in memory, grab it, else go
        // with what we pulled from disk.
        GetValue gv(epstore->get(item->getKey(), item->getVBucketId(),
                                 cookie, false));
        if (gv.getStatus() == ENGINE_SUCCESS) {
            *itm = gv.getValue();
            delete item;
        } else {
            *itm = item;
        }
        *vbucket = static_cast<Item*>(*itm)->getVBucketId();

        if (!connection->vbucketFilter(*vbucket)) {
            // We were going to use the item that we received from
            // disk, but the filter says not to, so we need to get rid
            // of it now.
            if (gv.getStatus() != ENGINE_SUCCESS) {
                delete item;
            }
            return TAP_NOOP;
        }
    } else if (connection->hasQueuedItem()) {
        QueuedItem qi = connection->next();
        lh.unlock();

        *vbucket = qi.getVBucketId();
        std::string key = qi.getKey();
        if (key.length() == 0) {
            return TAP_NOOP;
        }
        GetValue gv(epstore->get(key, qi.getVBucketId(), cookie,
                                 false, false));
        ENGINE_ERROR_CODE r = gv.getStatus();
        if (r == ENGINE_SUCCESS) {
            *itm = gv.getValue();
            ret = TAP_MUTATION;

            ++stats.numTapFGFetched;
        } else if (r == ENGINE_KEY_ENOENT) {
            ret = TAP_DELETION;
            r = itemAllocate(cookie, itm,
                             key.c_str(), key.length(), 0, 0, 0);
            if (r != ENGINE_SUCCESS) {
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "Failed to allocate memory for deletion of: %s\n", key.c_str());
                ret = TAP_PAUSE;
            }
            ++stats.numTapDeletes;
        } else if (r == ENGINE_EWOULDBLOCK) {
            connection->queueBGFetch(key, gv.getId());
            // This can optionally collect a few and batch them.
            connection->runBGFetch(epstore->getDispatcher(), cookie);
            // If there's an item ready, return NOOP so we'll come
            // back immediately, otherwise pause the connection
            // while we wait.
            return (connection->hasQueuedItem() || connection->hasItem())
                ? TAP_NOOP : TAP_PAUSE;
        } else {
            if (r == ENGINE_NOT_MY_VBUCKET) {
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "Trying to fetch an item for a bucket that "
                                 "doesn't exist on this server <%s>\n",
                                 connection->client.c_str());

            } else {
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "Tap internal error Internal error! <%s>:%d.  "
                                 "Disconnecting\n", connection->client.c_str(), r);
                return TAP_DISCONNECT;
            }
            ret = TAP_NOOP;
        }
    } else if (connection->shouldFlush()) {
        ret = TAP_FLUSH;
    }

    if (ret == TAP_PAUSE && connection->complete()) {
        ev = connection->nextVBucketLowPriority();
        if (ev.event != TAP_PAUSE) {
            assert(ev.event == TAP_VBUCKET_SET);
            connection->encodeVBucketStateTransition(ev, es, nes, vbucket);
            if (ev.state == active) {
                epstore->setVBucketState(ev.vbucket, dead);
            }
            ret = ev.event;
        } else {
            ret = TAP_DISCONNECT;
        }
    }

    return ret;
}

tap_event_t EventuallyPersistentEngine::walkTapQueue(const void *cookie,
                                                     item **itm,
                                                     void **es,
                                                     uint16_t *nes,
                                                     uint8_t *ttl,
                                                     uint16_t *flags,
                                                     uint32_t *seqno,
                                                     uint16_t *vbucket) {
    TapConnection *connection;
    tap_event_t ret = doWalkTapQueue(cookie, itm, es, nes, ttl, flags,
                                     seqno, vbucket, &connection);

    if (ret == TAP_PAUSE) {
        connection->paused = true;
    } else if (ret != TAP_DISCONNECT) {
        if (ret != TAP_NOOP) {
            ++stats.numTapFetched;
        }
        connection->paused = false;
        *seqno = connection->getSeqno();
        if (connection->requestAck()) {
            *flags = TAP_FLAG_ACK;
        }
    }

    return ret;
}

void EventuallyPersistentEngine::createTapQueue(const void *cookie,
                                                std::string &client,
                                                uint32_t flags,
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
            if (miter->first != NULL) {
                TapConnection *n = new TapConnection(*this, TapConnection::getAnonTapName(), 0);
                n->doDisconnect = true;
                n->paused = true;
                allTaps.push_back(n);
                tapConnectionMap[miter->first] = n;
            }
        }
    }

    bool reconnect = false;
    if (tap == NULL) {
        tap = new TapConnection(*this, name, flags);
        serverApi->cookie->set_tap_nack_mode(cookie, tap->ackSupported);
        allTaps.push_back(tap);
    } else {
        tap->rollback();
        tap->connected = true;
        tap->evaluateFlags();
        reconnect = true;
    }

    // Decoding the userdata section of the packet and update the filters
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

    tapConnectionMap[cookie] = tap;
    tap->setBackfillAge(backfillAge, reconnect);
    if (tap->doRunBackfill && tap->pendingBackfill) {
        setTapValidity(tap->client, cookie);
    }
    tap->setVBucketFilter(vbuckets);
    tapNotifySync.notify();
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::tapNotify(const void *cookie,
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
                                                        uint16_t vbucket) {
    (void)ttl;

    // If cookie is null, this is the internal tap client, so we
    // should disconnect it if tap isn't enabled.
    if (!cookie && !tapEnabled) {
        return ENGINE_DISCONNECT;
    }

    std::string k(static_cast<const char*>(key), nkey);

    switch (tap_event) {
    case TAP_ACK:
        return processTapAck(cookie, tap_seqno, tap_flags, k);
    case TAP_FLUSH:
        return flush(cookie, 0);
    case TAP_DELETION:
        return itemDelete(cookie, k, vbucket);

    case TAP_MUTATION:
        {
            // We don't get the trailing CRLF in tap mutation but should store it
            // to satisfy memcached expectations.
            //
            // We do this by manually constructing the item using its
            // value_t constructor to reduce memory copies as much as
            // possible.
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

            epstore->setVBucketState(vbucket, state);
        }
        break;

    default:
        abort();
    }

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::processTapAck(const void *cookie,
                                                            uint32_t seqno,
                                                            uint16_t status,
                                                            const std::string &msg)
{
    LockHolder lh(tapNotifySync);
    std::map<const void*, TapConnection*>::iterator iter;
    iter = tapConnectionMap.find(cookie);
    if (iter == tapConnectionMap.end()) {
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                         "Can't find client connection\n");
        return ENGINE_DISCONNECT;
    }
    TapConnection *connection = iter->second;
    if (connection->doDisconnect) {
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                         "Disconnecting pending connection\n");
        return ENGINE_DISCONNECT;
    }

    return connection->processAck(seqno, status, msg);
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


/**
 * VBucketVisitor to backfill a TapConnection.
 */
class BackFillVisitor : public VBucketVisitor {
public:
    BackFillVisitor(EventuallyPersistentEngine *e, TapConnection *tc,
                    const void *token):
        VBucketVisitor(), engine(e), name(tc->client),
        queue(new std::list<QueuedItem>),
        filter(tc->backFillVBucketFilter), validityToken(token),
        maxBackfillSize(e->tapBacklogLimit), valid(true) { }

    ~BackFillVisitor() {
        delete queue;
    }

    bool visitBucket(RCPtr<VBucket> vb) {
        if (filter(vb->getId())) {
            VBucketVisitor::visitBucket(vb);
            return true;
        }
        return false;
    }

    void visit(StoredValue *v) {
        std::string k = v->getKey();
        QueuedItem qi(k, currentBucket->getId(), queue_op_set);
        queue->push_back(qi);
    }

    bool shouldContinue() {
        setEvents();
        return valid;
    }

    void apply(void) {
        setEvents();
        if (valid) {
            CompleteBackfillTapOperation tapop;
            engine->performTapOp(name, tapop, static_cast<void*>(NULL));
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

/// @cond DETAILS
class BackFillThreadData {
public:

    BackFillThreadData(EventuallyPersistentEngine *e, TapConnection *tc,
                       EventuallyPersistentStore *s, const void *tok):
        bfv(e, tc, tok), epstore(s) {
    }

    BackFillVisitor bfv;
    EventuallyPersistentStore *epstore;
};
/// @endcond

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

bool VBucketCountVisitor::visitBucket(RCPtr<VBucket> vb) {
    size_t n = vb->ht.getNumItems();
    total += n;
    if (vb->getState() == desired_state) {
        requestedState += n;
    }
    return false;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doEngineStats(const void *cookie,
                                                            ADD_STAT add_stat) {
    VBucketCountVisitor countVisitor;
    epstore->visit(countVisitor);

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
    add_casted_stat("ep_expired", epstats.expired, add_stat, cookie);
    add_casted_stat("ep_item_flush_expired",
                    epstats.flushExpired, add_stat, cookie);
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
                    epstats.cumulativeCommitTime, add_stat, cookie);
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
    add_casted_stat("curr_items", countVisitor.getRequested(), add_stat, cookie);
    add_casted_stat("curr_items_tot", countVisitor.getTotal(), add_stat, cookie);
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
        add_casted_stat("ep_warmup_dups", epstats.warmDups, add_stat, cookie);
        add_casted_stat("ep_warmup_oom", epstats.warmOOM, add_stat, cookie);
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

    bool visitBucket(RCPtr<VBucket> vb) {
        char buf[16];
        snprintf(buf, sizeof(buf), "vb_%d", vb->getId());
        add_casted_stat(buf, VBucket::toString(vb->getState()), add_stat, cookie);
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
        addTapStat("bg_backlog_size", tc, tc->getBacklogSize(), add_stat, cookie);
        addTapStat("flags", tc, tc->flags, add_stat, cookie);
        addTapStat("connected", tc, tc->connected, add_stat, cookie);
        addTapStat("pending_disconnect", tc, tc->doDisconnect, add_stat, cookie);
        addTapStat("paused", tc, tc->paused, add_stat, cookie);
        addTapStat("pending_backfill", tc, tc->pendingBackfill, add_stat, cookie);
        if (tc->reconnects > 0) {
            addTapStat("reconnects", tc, tc->reconnects, add_stat, cookie);
        }
        if (tc->disconnects > 0) {
            addTapStat("disconnects", tc, tc->disconnects, add_stat, cookie);
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
    add_casted_stat("ep_tap_bg_fetched", stats.numTapBGFetched, add_stat, cookie);
    add_casted_stat("ep_tap_fg_fetched", stats.numTapFGFetched, add_stat, cookie);
    add_casted_stat("ep_tap_deletes", stats.numTapDeletes, add_stat, cookie);
    add_casted_stat("ep_tap_keepalive", tapKeepAlive, add_stat, cookie);

    add_casted_stat("ep_tap_count", totalTaps, add_stat, cookie);

    add_casted_stat("ep_replication_state",
                    tapEnabled? "enabled": "disabled", add_stat, cookie);

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

    add_casted_stat("ep_tap_bg_num_samples", stats.tapBgNumOperations, add_stat, cookie);
    add_casted_stat("ep_tap_bg_min_wait",
                    hrtime2text(stats.tapBgMinWait).c_str(),
                    add_stat, cookie);
    add_casted_stat("ep_tap_bg_max_wait",
                    hrtime2text(stats.tapBgMaxWait).c_str(),
                    add_stat, cookie);
    add_casted_stat("ep_tap_bg_wait_avg",
                    stats.tapBgNumOperations > 0 ?
                    hrtime2text(stats.tapBgWait / stats.tapBgNumOperations).c_str() : "n/a",
                    add_stat, cookie);
    add_casted_stat("ep_tap_bg_min_load",
                    hrtime2text(stats.tapBgMinLoad).c_str(),
                    add_stat, cookie);
    add_casted_stat("ep_tap_bg_max_load",
                    hrtime2text(stats.tapBgMaxLoad).c_str(),
                    add_stat, cookie);
    add_casted_stat("ep_tap_bg_load_avg",
                    stats.tapBgNumOperations > 0 ?
                    hrtime2text(stats.tapBgLoad / stats.tapBgNumOperations).c_str() : "n/a",
                    add_stat, cookie);

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doKeyStats(const void *cookie,
                                                         ADD_STAT add_stat,
                                                         uint16_t vbid,
                                                         std::string &key,
                                                         bool validate) {
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
        return epstore->getFromUnderlying(key, vbid, cookie, cb);
    }

    if (epstore->getKeyStats(key, vbid, kstats)) {
        std::string valid("this_is_a_bug");
        if (validate) {
            if (kstats.dirty) {
                valid.assign("dirty");
            } else {
                GetValue gv(epstore->get(key, vbid, cookie, serverApi->core));
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
        std::string key;
        std::string vbid;
        std::string s_key(&stat_key[4], nkey - 4);
        std::stringstream ss(s_key);

        ss >> key;
        ss >> vbid;
        if (key.length() == 0) {
            return rv;
        }
        uint16_t vbucket_id = vbid.length() != 0 ? static_cast<uint16_t>(atoi(vbid.c_str())) : 0;
        // Non-validating, non-blocking version
        rv = doKeyStats(cookie, add_stat, vbucket_id, key, false);
    } else if (nkey > 5 && strncmp(stat_key, "vkey ", 5) == 0) {
        std::string key;
        std::string vbid;
        std::string s_key(&stat_key[5], nkey - 5);
        std::stringstream ss(s_key);

        ss >> key;
        ss >> vbid;
        if (key.length() == 0) {
            return rv;
        }
        uint16_t vbucket_id = vbid.length() != 0 ? static_cast<uint16_t>(atoi(vbid.c_str())) : 0;
        // Validating version; blocks
        rv = doKeyStats(cookie, add_stat, vbucket_id, key, true);
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
    stats.tapBgNumOperations.set(0);
    stats.tapBgWait.set(0);
    stats.tapBgLoad.set(0);
    stats.tapBgMinWait.set(999999999);
    stats.tapBgMaxWait.set(0);
    stats.tapBgMinLoad.set(999999999);
    stats.tapBgMaxLoad.set(0);
    stats.pendingOps.set(0);
    stats.pendingOpsTotal.set(0);
    stats.pendingOpsMax.set(0);
    stats.pendingOpsMaxDuration.set(0);
    stats.numTapFetched.set(0);
}

void EventuallyPersistentEngine::notifyTapIoThreadMain(void) {
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
        if (iter->second->ackSupported &&
            (iter->second->expiry_time < now) &&
            iter->second->windowIsFull())
            {
                shouldPause = false;
                iter->second->doDisconnect = true;
            } else if (iter->second->doDisconnect || !iter->second->idle()) {
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
        if (iter->second->paused && !iter->second->doDisconnect) {
            toNotify.push_back(iter->first);
        }
    }

    lh.unlock();

    // Signal all outstanding, paused connections.
    std::for_each(toNotify.begin(), toNotify.end(),
                  std::bind2nd(std::ptr_fun(serverApi->cookie->notify_io_complete),
                               ENGINE_SUCCESS));
}

void EventuallyPersistentEngine::notifyTapIoThread(void) {
    // Fix clean shutdown!!!
    while (!shutdown) {

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



void CompleteBackfillTapOperation::perform(TapConnection *tc, void *arg) {
    (void)arg;
    tc->completeBackfill();
}

void ReceivedItemTapOperation::perform(TapConnection *tc, Item *arg) {
    tc->gotBGItem(arg);
}

void CompletedBGFetchTapOperation::perform(TapConnection *tc,
                                           EventuallyPersistentEngine *epe) {
    (void)epe;
    tc->completedBGFetchJob();
}
