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

#include "config.h"

#include <limits>
#include <assert.h>
#include <fcntl.h>

#include <memcached/engine.h>
#include <memcached/protocol_binary.h>

#include "ep_engine.h"
#include "statsnap.hh"
#include "tapthrottle.hh"
#include "htresizer.hh"
#include "checkpoint_remover.hh"

static void assembleSyncResponse(std::stringstream &resp,
                                 SyncListener *syncListener,
                                 EventuallyPersistentStore &epstore);
static void addSyncKeySpecs(std::stringstream &resp,
                            std::set<key_spec_t> &keyspecs,
                            uint8_t eventid,
                            EventuallyPersistentStore &epstore,
                            bool addCurrentCas = true);
static bool parseSyncOptions(uint32_t flags, sync_type_t *syncType, uint8_t *replicas);
static void notifyListener(std::vector< std::pair<StoredValue*, uint16_t> > &svList,
                           SyncListener *listener);

static size_t percentOf(size_t val, double percent) {
    return static_cast<size_t>(static_cast<double>(val) * percent);
}

static const char* DEFAULT_SHARD_PATTERN("%d/%b-%i.sqlite");

/**
 * Helper function to avoid typing in the long cast all over the place
 * @param handle pointer to the engine
 * @return the engine as a class
 */
static inline EventuallyPersistentEngine* getHandle(ENGINE_HANDLE* handle)
{
    EventuallyPersistentEngine* ret;
    ret = reinterpret_cast<EventuallyPersistentEngine*>(handle);
    ObjectRegistry::onSwitchThread(ret);
    return ret;
}

void LookupCallback::callback(GetValue &value) {
    if (value.getStatus() == ENGINE_SUCCESS) {
        engine->addLookupResult(cookie, value.getValue());
    } else {
        engine->addLookupResult(cookie, NULL);
    }
    engine->notifyIOComplete(cookie, value.getStatus());
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

    static void EvpDestroy(ENGINE_HANDLE* handle, const bool force)
    {
        getHandle(handle)->destroy(force);
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

    static void EvpResetStats(ENGINE_HANDLE* handle, const void *)
    {
        return getHandle(handle)->resetStats();
    }

    static protocol_binary_response_status stopFlusher(EventuallyPersistentEngine *e,
                                                       const char **msg,
                                                       size_t *msg_size) {
        return e->stopFlusher(msg, msg_size);
    }

    static protocol_binary_response_status startFlusher(EventuallyPersistentEngine *e,
                                                        const char **msg,
                                                        size_t *msg_size) {
        return e->startFlusher(msg, msg_size);
    }

    static protocol_binary_response_status setTapParam(EventuallyPersistentEngine *e,
                                                       const char *keyz, const char *valz,
                                                       const char **msg, size_t *) {
        protocol_binary_response_status rv = PROTOCOL_BINARY_RESPONSE_SUCCESS;

        try {
            if (strcmp(keyz, "tap_keepalive") == 0) {
                int v = atoi(valz);
                validate(v, 0, MAX_TAP_KEEP_ALIVE);
                e->setTapKeepAlive(static_cast<uint32_t>(v));
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

    static protocol_binary_response_status setFlushParam(EventuallyPersistentEngine *e,
                                                         const char *keyz, const char *valz,
                                                         const char **msg,
                                                         size_t *) {
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
            } else if (strcmp(keyz, "chk_max_items") == 0) {
                validate(v, MIN_CHECKPOINT_ITEMS, MAX_CHECKPOINT_ITEMS);
                CheckpointManager::setCheckpointMaxItems(v);
            } else if (strcmp(keyz, "chk_period") == 0) {
                validate(v, MIN_CHECKPOINT_PERIOD, MAX_CHECKPOINT_PERIOD);
                CheckpointManager::setCheckpointPeriod(v);
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
                                                    const char **msg,
                                                    size_t *msg_size) {
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

        return e->evictKey(key, vbucket, msg, msg_size);
    }

    ENGINE_ERROR_CODE getLocked(EventuallyPersistentEngine *e,
            protocol_binary_request_getl *grequest,
            const void *cookie,
            Item **item,
            const char **msg,
            size_t *,
            protocol_binary_response_status *res) {

        protocol_binary_request_header *request = &(grequest->message.header);
        protocol_binary_request_no_extras *req =
            (protocol_binary_request_no_extras*)request;
        *res = PROTOCOL_BINARY_RESPONSE_SUCCESS;

        char keyz[256];

        // Read the key.
        int keylen = ntohs(req->message.header.request.keylen);
        if (keylen >= (int)sizeof(keyz)) {
            *msg = "Key is too large.";
            *res = PROTOCOL_BINARY_RESPONSE_EINVAL;
            return ENGINE_EINVAL;
        }
        int extlen = req->message.header.request.extlen;
        memcpy(keyz, ((char*)request) + sizeof(req->message.header) + extlen, keylen);
        keyz[keylen] = 0x00;

        uint16_t vbucket = ntohs(request->request.vbucket);

        std::string key(keyz, keylen);

        RememberingCallback<GetValue> getCb;
        uint32_t lockTimeout;
        uint32_t max_timeout = (unsigned int)e->getGetlMaxTimeout();
        uint32_t default_timeout = (unsigned int)e->getGetlDefaultTimeout();
        if (extlen >= 8) {
            grequest->message.body.flags = grequest->message.body.flags;
            lockTimeout = ntohl(grequest->message.body.expiration);
        } else {
            lockTimeout = default_timeout;
        }

        getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                         "Executing getl for key %s timeout %d max: %d, default: %d\n",
                         keyz, lockTimeout, max_timeout, default_timeout);

        if (lockTimeout >  max_timeout || lockTimeout < 1) {
            lockTimeout = default_timeout;
        }

        bool gotLock = e->getLocked(key, vbucket, getCb,
                                    ep_current_time(),
                                    lockTimeout, cookie);

        getCb.waitForValue();
        ENGINE_ERROR_CODE rv = getCb.val.getStatus();

        if (rv == ENGINE_SUCCESS) {
            *item = getCb.val.getValue();

        } else if (rv == ENGINE_EWOULDBLOCK) {

            // need to wait for value
            return rv;
        } else if (!gotLock){

            *msg =  "LOCK_ERROR";
            *res = PROTOCOL_BINARY_RESPONSE_ETMPFAIL;
            return ENGINE_TMPFAIL;
        } else {
            RCPtr<VBucket> vb = e->getVBucket(vbucket);
            if (!vb) {
                *msg = "That's not my bucket.";
                *res = PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET;
                return ENGINE_NOT_MY_VBUCKET;
            }
            *msg = "NOT_FOUND";
            *res = PROTOCOL_BINARY_RESPONSE_KEY_ENOENT;
            return ENGINE_KEY_ENOENT;
        }

        return rv;
    }

    static protocol_binary_response_status unlockKey(EventuallyPersistentEngine *e,
                                                     protocol_binary_request_header *request,
                                                     const char **msg,
                                                     size_t *)
    {
        protocol_binary_request_no_extras *req =
            (protocol_binary_request_no_extras*)request;

        protocol_binary_response_status res = PROTOCOL_BINARY_RESPONSE_SUCCESS;
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
                         "Executing unl for key %s\n",
                         keyz);

        RememberingCallback<GetValue> getCb;
        uint64_t cas = request->request.cas;

        ENGINE_ERROR_CODE rv = e->unlockKey(key, vbucket, cas, ep_current_time());

        if (rv == ENGINE_SUCCESS) {
            *msg = "UNLOCKED";
        } else if (rv == ENGINE_TMPFAIL){
            *msg =  "UNLOCK_ERROR";
            res = PROTOCOL_BINARY_RESPONSE_ETMPFAIL;
        } else {
            RCPtr<VBucket> vb = e->getVBucket(vbucket);
            if (!vb) {
                *msg = "That's not my bucket.";
                res =  PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET;
            }
            *msg = "NOT_FOUND";
            res =  PROTOCOL_BINARY_RESPONSE_KEY_ENOENT;
        }

        return res;
    }

    static protocol_binary_response_status setParam(EventuallyPersistentEngine *e,
                                                    protocol_binary_request_header *request,
                                                    const char **msg,
                                                    size_t *msg_size) {
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
            rv = setFlushParam(e, keyz, valz, msg, msg_size);
            break;
        case CMD_SET_TAP_PARAM:
            rv = setTapParam(e, keyz, valz, msg, msg_size);
            break;
        default:
            rv = PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND;
        }

        return rv;
    }

    static ENGINE_ERROR_CODE syncCmd(EventuallyPersistentEngine *e,
                                     protocol_binary_request_header *request,
                                     const void *cookie,
                                     ADD_RESPONSE response) {
        void *data = e->getServerApi()->cookie->get_engine_specific(cookie);

        if (data != NULL) {
            SyncListener *syncListener = static_cast<SyncListener *>(data);
            std::stringstream resp;

            assembleSyncResponse(resp, syncListener, *e->getEpStore());
            e->getServerApi()->cookie->store_engine_specific(cookie, NULL);
            delete syncListener;

            std::string body = resp.str();
            response(NULL, 0, NULL, 0,
                     body.c_str(), static_cast<uint16_t>(body.length()),
                     PROTOCOL_BINARY_RAW_BYTES,
                     PROTOCOL_BINARY_RESPONSE_SUCCESS, 0, cookie);

            return ENGINE_SUCCESS;
        }

        char *body = (char *) (request + 1);
        off_t offset = 0;
        // flags, 32 bits
        uint32_t flags;

        memcpy(&flags, body + offset, sizeof(uint32_t));
        flags = ntohl(flags);
        offset += sizeof(uint32_t);

        uint8_t replicas;
        sync_type_t syncType;
        bool validFlags = parseSyncOptions(flags, &syncType, &replicas);

        if (!validFlags) {
            response(NULL, 0, NULL, 0, "", 0, PROTOCOL_BINARY_RAW_BYTES,
                     PROTOCOL_BINARY_RESPONSE_EINVAL, 0, cookie);
            return ENGINE_EINVAL;
        }

        if (replicas > 1) {
            // replica count > 1 not supported for chain mode replication, which is
            // the default in Membase deployments (ticket MB-3817)
            const std::string msg("A replica count > 1 is not supported.");
            response(NULL, 0, NULL, 0, msg.c_str(), msg.length(),
                     PROTOCOL_BINARY_RAW_BYTES,
                     PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, 0, cookie);
            return ENGINE_ENOTSUP;
        }

        // number of keys in the request, 16 bits
        uint16_t nkeys;

        memcpy(&nkeys, body + offset, sizeof(uint16_t));
        nkeys = ntohs(nkeys);
        offset += sizeof(uint16_t);

        // key specifications
        std::set<key_spec_t> *keyset = new std::set<key_spec_t>();

        for (int i = 0; i < nkeys; i++) {
            // CAS, 64 bits
            uint64_t cas;
            memcpy(&cas, body + offset, sizeof(uint64_t));
            cas = ntohll(cas);
            offset += sizeof(uint64_t);

            // vbucket id, 16 bits
            uint16_t vbucketid;
            memcpy(&vbucketid, body + offset, sizeof(uint16_t));
            vbucketid = ntohs(vbucketid);
            offset += sizeof(uint16_t);

            // key length, 16 bits
            uint16_t keylen;
            memcpy(&keylen, body + offset, sizeof(uint16_t));
            keylen = ntohs(keylen);
            offset += sizeof(uint16_t);

            // key string
            std::string key(body + offset, keylen);
            offset += keylen;

            key_spec_t keyspec(cas, vbucketid, key);
            keyset->insert(keyspec);
        }

        return e->sync(keyset, cookie, syncType, replicas, response);
    }

    static ENGINE_ERROR_CODE getVBucket(EventuallyPersistentEngine *e,
                                        const void *cookie,
                                        protocol_binary_request_header *request,
                                        ADD_RESPONSE response) {
        protocol_binary_request_get_vbucket *req =
            reinterpret_cast<protocol_binary_request_get_vbucket*>(request);
        assert(req);

        uint16_t vbucket = ntohs(req->message.header.request.vbucket);
        RCPtr<VBucket> vb = e->getVBucket(vbucket);
        if (!vb) {
            const std::string msg("That's not my bucket.");
            response(NULL, 0, NULL, 0, msg.c_str(), msg.length(),
                     PROTOCOL_BINARY_RAW_BYTES,
                     PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET, 0, cookie);
        } else {
            vbucket_state_t state = (vbucket_state_t)ntohl(vb->getState());
            response(NULL, 0, NULL, 0, &state, sizeof(state),
                     PROTOCOL_BINARY_RAW_BYTES,
                     PROTOCOL_BINARY_RESPONSE_SUCCESS, 0, cookie);
        }
        return ENGINE_SUCCESS;
    }

    static ENGINE_ERROR_CODE setVBucket(EventuallyPersistentEngine *e,
                                        const void *cookie,
                                        protocol_binary_request_header *request,
                                        ADD_RESPONSE response)
    {
        protocol_binary_request_set_vbucket *req =
            reinterpret_cast<protocol_binary_request_set_vbucket*>(request);

        size_t bodylen = ntohl(req->message.header.request.bodylen)
            - ntohs(req->message.header.request.keylen);
        if (bodylen != sizeof(vbucket_state_t)) {
            const std::string msg("Incorrect packet format");
            response(NULL, 0, NULL, 0, msg.c_str(), msg.length(),
                     PROTOCOL_BINARY_RAW_BYTES,
                     PROTOCOL_BINARY_RESPONSE_EINVAL, 0, cookie);
        }

        vbucket_state_t state;
        memcpy(&state, &req->message.body.state, sizeof(state));
        state = static_cast<vbucket_state_t>(ntohl(state));

        if (!is_valid_vbucket_state_t(state)) {
            const std::string msg("Invalid vbucket state");
            response(NULL, 0, NULL, 0, msg.c_str(), msg.length(),
                     PROTOCOL_BINARY_RAW_BYTES,
                     PROTOCOL_BINARY_RESPONSE_EINVAL, 0, cookie);
        }

        e->setVBucketState(ntohs(req->message.header.request.vbucket), state);
        response(NULL, 0, NULL, 0, NULL, 0, PROTOCOL_BINARY_RAW_BYTES,
                 PROTOCOL_BINARY_RESPONSE_SUCCESS, 0, cookie);

        return ENGINE_SUCCESS;
    }

    static ENGINE_ERROR_CODE delVBucket(EventuallyPersistentEngine *e,
                                        const void *cookie,
                                        protocol_binary_request_header *req,
                                        ADD_RESPONSE response) {
        uint16_t vbucket = ntohs(req->request.vbucket);
        if (e->deleteVBucket(vbucket)) {
            response(NULL, 0, NULL, 0, NULL, 0, PROTOCOL_BINARY_RAW_BYTES,
                     PROTOCOL_BINARY_RESPONSE_SUCCESS, 0, cookie);
        } else {
            // If we fail to delete, try to figure out why.
            RCPtr<VBucket> vb = e->getVBucket(vbucket);
            if (!vb) {
                const std::string msg("Failed to delete vbucket.  Bucket not found.");
                response(NULL, 0, NULL, 0, msg.c_str(), msg.length(),
                         PROTOCOL_BINARY_RAW_BYTES,
                         PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET, 0, cookie);
            } else if(vb->getState() != vbucket_state_dead) {
                const std::string msg("Failed to delete vbucket.  Must be in the dead state.");
                response(NULL, 0, NULL, 0, msg.c_str(), msg.length(),
                         PROTOCOL_BINARY_RAW_BYTES,
                         PROTOCOL_BINARY_RESPONSE_EINVAL, 0, cookie);
            } else {
                const std::string msg("Failed to delete vbucket.  Unknown reason.");
                response(NULL, 0, NULL, 0, msg.c_str(), msg.length(),
                         PROTOCOL_BINARY_RAW_BYTES,
                         PROTOCOL_BINARY_RESPONSE_EINTERNAL, 0, cookie);
            }
        }

        return ENGINE_SUCCESS;
    }

    static ENGINE_ERROR_CODE EvpUnknownCommand(ENGINE_HANDLE* handle,
                                               const void* cookie,
                                               protocol_binary_request_header *request,
                                               ADD_RESPONSE response)
    {
        protocol_binary_response_status res =
            PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND;
        const char *msg = NULL;
        size_t msg_size = 0;
        Item *item = NULL;

        EventuallyPersistentEngine *h = getHandle(handle);
        EPStats &stats = h->getEpStats();
        ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;

        switch (request->request.opcode) {
        case PROTOCOL_BINARY_CMD_GET_VBUCKET:
            {
                BlockTimer timer(&stats.getVbucketCmdHisto);
                return getVBucket(h, cookie, request, response);
            }

        case PROTOCOL_BINARY_CMD_DEL_VBUCKET:
            {
                BlockTimer timer(&stats.delVbucketCmdHisto);
                return delVBucket(h, cookie, request, response);
            }
            break;

        case PROTOCOL_BINARY_CMD_SET_VBUCKET:
            {
                BlockTimer timer(&stats.setVbucketCmdHisto);
                return setVBucket(h, cookie, request, response);
            }
            break;
        case CMD_ONLINE_UPDATE_START:
        case CMD_ONLINE_UPDATE_COMPLETE:
        case CMD_ONLINE_UPDATE_REVERT:
            return h->onlineUpdate(cookie, request, response);
        case PROTOCOL_BINARY_CMD_TOUCH:
        case PROTOCOL_BINARY_CMD_GAT:
        case PROTOCOL_BINARY_CMD_GATQ:
            return h->touch(cookie, request, response);

        case CMD_RESTORE_FILE:
        case CMD_RESTORE_ABORT:
        case CMD_RESTORE_COMPLETE:
            return h->handleRestoreCmd(cookie, request, response);

        case CMD_STOP_PERSISTENCE:
            res = stopFlusher(h, &msg, &msg_size);
            break;
        case CMD_START_PERSISTENCE:
            res = startFlusher(h, &msg, &msg_size);
            break;
        case CMD_SET_FLUSH_PARAM:
        case CMD_SET_TAP_PARAM:
            res = setParam(h, request, &msg, &msg_size);
            break;
        case CMD_EVICT_KEY:
            res = evictKey(h, request, &msg, &msg_size);
            break;
        case CMD_GET_LOCKED:
            rv = getLocked(h, (protocol_binary_request_getl*)request, cookie, &item, &msg, &msg_size, &res);
            if (rv == ENGINE_EWOULDBLOCK) {
                // we dont have the value for the item yet
                return rv;
            }
            break;
        case CMD_UNLOCK_KEY:
            res = unlockKey(h, request, &msg, &msg_size);
            break;
        case CMD_SYNC:
            return syncCmd(h, request, cookie, response);
            break;
        case CMD_DEREGISTER_TAP_CLIENT:
            return h->deregisterTapClient(cookie, request, response);
            break;
        case CMD_LAST_CLOSED_CHECKPOINT:
            return h->handleGetLastClosedCheckpointId(cookie, request, response);
        }

        if (item) {
            std::string key  = item->getKey();
            uint32_t flags = item->getFlags();

            response(static_cast<const void *>(key.data()),
                    item->getNKey(),
                    (const void *)&flags, sizeof(uint32_t),
                    static_cast<const void *>(item->getData()),
                    item->getNBytes(),
                    PROTOCOL_BINARY_RAW_BYTES,
                    static_cast<uint16_t>(res), item->getCas(),
                    cookie);
            delete item;
        } else {

            msg_size = (msg_size > 0 || msg == NULL) ? msg_size : strlen(msg);
            response(NULL, 0, NULL, 0,
                    msg, static_cast<uint16_t>(msg_size),
                    PROTOCOL_BINARY_RAW_BYTES,
                    static_cast<uint16_t>(res), 0, cookie);

        }
        return ENGINE_SUCCESS;
    }

    static void EvpItemSetCas(ENGINE_HANDLE* , const void *,
                              item *item, uint64_t cas) {
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
        if (getHandle(handle)->createTapQueue(cookie, c, flags,
                                              userdata, nuserdata)) {
            return EvpTapIterator;
        } else {
            return NULL;
        }
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
        ep_abs_time = api->core->abstime;
        ep_reltime = api->core->realtime;

        *handle = reinterpret_cast<ENGINE_HANDLE*> (engine);
        return ENGINE_SUCCESS;
    }

    void *EvpNotifyTapIo(void*arg) {
        ObjectRegistry::onSwitchThread(static_cast<EventuallyPersistentEngine*>(arg));
        static_cast<EventuallyPersistentEngine*>(arg)->notifyTapIoThread();
        return NULL;
    }

    static bool EvpGetItemInfo(ENGINE_HANDLE *, const void *,
                               const item* item, item_info *item_info)
    {
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

/**
 * Dispatcher callback responsible for bulk backfilling tap queues
 * from a KVStore.
 *
 * Note that this is only used if the KVStore reports that it has
 * efficient vbucket ops.
 */
class BackfillDiskLoad : public DispatcherCallback, public Callback<GetValue> {
public:

    BackfillDiskLoad(const std::string &n, EventuallyPersistentEngine* e,
                     TapConnMap &tcm, KVStore *s, uint16_t vbid, const void *token)
        : name(n), engine(e), connMap(tcm), store(s), vbucket(vbid), validityToken(token) {

        vbucket_version = engine->getEpStore()->getVBucketVersion(vbucket);
    }

    void callback(GetValue &gv) {
        // If a vbucket version of a bg fetched item is different from the current version,
        // skip this item.
        if (vbucket_version != gv.getVBucketVersion()) {
            delete gv.getValue();
            return;
        }
        ReceivedItemTapOperation tapop(true);
        // if the tap connection is closed, then free an Item instance
        if (!connMap.performTapOp(name, tapop, gv.getValue())) {
            delete gv.getValue();
        }

        NotifyPausedTapOperation notifyOp;
        connMap.performTapOp(name, notifyOp, engine);
    }

    bool callback(Dispatcher &, TaskId) {
        if (connMap.checkValidity(name, validityToken)) {
            if (!engine->getEpStore()->isFlushAllScheduled()) {
                store->dump(vbucket, *this);
            }
            CompleteDiskBackfillTapOperation op;
            connMap.performTapOp(name, op, static_cast<void*>(NULL));
        }

        return false;
    }

    std::string description() {
        std::stringstream rv;
        rv << "Loading tap backfill for vb " << vbucket;
        return rv.str();
    }

private:
    const std::string           name;
    EventuallyPersistentEngine *engine;
    TapConnMap                 &connMap;
    KVStore                    *store;
    uint16_t                    vbucket;
    uint16_t                    vbucket_version;
    const void                 *validityToken;
};

/**
 * VBucketVisitor to backfill a TapProducer.
 */
class BackFillVisitor : public VBucketVisitor {
public:
    BackFillVisitor(EventuallyPersistentEngine *e, TapProducer *tc,
                    const void *token):
        VBucketVisitor(), engine(e), name(tc->getName()),
        queue(new std::list<queued_item>),
        found(), filter(tc->backFillVBucketFilter), validityToken(token),
        maxBackfillSize(e->tapBacklogLimit), valid(true),
        efficientVBDump(e->epstore->getStorageProperties().hasEfficientVBDump()),
        residentRatioBelowThreshold(false) {
        found.reserve(e->tapBacklogLimit);
    }

    ~BackFillVisitor() {
        delete queue;
    }

    bool visitBucket(RCPtr<VBucket> vb) {
        if (filter(vb->getId())) {
            VBucketVisitor::visitBucket(vb);
            // If the current resident ratio for a given vbucket is below the resident threshold
            // for memory backfill only, schedule the disk backfill for more efficient bg fetches.
            double numItems = static_cast<double>(vb->ht.getNumItems());
            double numNonResident = static_cast<double>(vb->ht.getNumNonResidentItems());
            if (numItems == 0) {
                return true;
            }
            residentRatioBelowThreshold =
                ((numItems - numNonResident) / numItems) < backfillResidentThreshold ? true : false;
            if (efficientVBDump && residentRatioBelowThreshold) {
                vbuckets.push_back(vb->getId());
                ScheduleDiskBackfillTapOperation tapop;
                engine->tapConnMap.performTapOp(name, tapop, static_cast<void*>(NULL));
            }
            // When the backfill is scheduled for a given vbucket, record the vbucket's current
            // open checkpoint Id into the corresponding tap connection.
            engine->tapConnMap.recordCurrentOpenCheckpointId(name, vb->getId());
            return true;
        }
        return false;
    }

    void visit(StoredValue *v) {
        // If efficient VBdump is supported and an item is not resident,
        // skip the item as it will be fetched by the disk backfill.
        if (efficientVBDump && residentRatioBelowThreshold && !v->isResident()) {
            return;
        }
        std::string k = v->getKey();
        queued_item qi(new QueuedItem(k, currentBucket->getId(), queue_op_set, -1, v->getId()));
        uint16_t shardId = engine->kvstore->getShardId(*qi);
        found.push_back(std::make_pair(shardId, qi));
    }

    bool shouldContinue() {
        setEvents();
        return valid;
    }

    void apply(void) {
        // If efficient VBdump is supported, schedule all the disk backfill tasks.
        if (efficientVBDump && checkValidity()) {
            std::vector<uint16_t>::iterator it = vbuckets.begin();
            for (; it != vbuckets.end(); it++) {
                Dispatcher *d(engine->epstore->getRODispatcher());
                KVStore *underlying(engine->epstore->getROUnderlying());
                assert(d);
                shared_ptr<DispatcherCallback> cb(new BackfillDiskLoad(name,
                                                                       engine,
                                                                       engine->tapConnMap,
                                                                       underlying,
                                                                       *it,
                                                                       validityToken));
                d->schedule(cb, NULL, Priority::TapBgFetcherPriority);
            }
            vbuckets.clear();
        }

        setEvents();
        if (valid) {
            CompleteBackfillTapOperation tapop;
            engine->tapConnMap.performTapOp(name, tapop, static_cast<void*>(NULL));
        }
    }

    static void setResidentItemThreshold(double residentThreshold) {
        if (residentThreshold < MINIMUM_BACKFILL_RESIDENT_THRESHOLD) {
            std::stringstream ss;
            ss << "Resident item threshold " << residentThreshold
               << " for memory backfill only is too low. Ignore this new threshold...";
            getLogger()->log(EXTENSION_LOG_WARNING, NULL, ss.str().c_str());
            return;
        }
        backfillResidentThreshold = residentThreshold;
    }

private:

    void setEvents() {
        if (checkValidity()) {
            if (!found.empty()) {
                // Don't notify unless we've got some data..
                TaggedQueuedItemComparator<uint16_t> comparator;
                std::sort(found.begin(), found.end(), comparator);

                std::vector<std::pair<uint16_t, queued_item> >::iterator it(found.begin());
                for (; it != found.end(); ++it) {
                    queue->push_back(it->second);
                }
                found.clear();
                engine->tapConnMap.setEvents(name, queue);
            }
            waitForQueue();
        }
    }

    void waitForQueue() {
        bool reported(false);
        bool tooBig(true);

        while (checkValidity() && tooBig) {
            ssize_t theSize(engine->tapConnMap.queueDepth(name));
            if (theSize < 0) {
                getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                                 "TapProducer %s went away.  Stopping backfill.\n",
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
        if (valid) {
            valid = engine->tapConnMap.checkValidity(name, validityToken);
            if (!valid) {
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "Backfilling token for %s went invalid.  Stopping backfill.\n",
                                 name.c_str());
            }
        }
        return valid;
    }

    EventuallyPersistentEngine *engine;
    const std::string name;
    std::list<queued_item> *queue;
    std::vector<std::pair<uint16_t, queued_item> > found;
    std::vector<uint16_t> vbuckets;
    VBucketFilter filter;
    const void *validityToken;
    ssize_t maxBackfillSize;
    bool valid;
    bool efficientVBDump;
    bool residentRatioBelowThreshold;

    static double backfillResidentThreshold;
};

double BackFillVisitor::backfillResidentThreshold = DEFAULT_BACKFILL_RESIDENT_THRESHOLD;

/// @cond DETAILS
class BackFillThreadData {
public:

    BackFillThreadData(EventuallyPersistentEngine *e, TapProducer *tc,
                       EventuallyPersistentStore *s, const void *tok):
        bfv(e, tc, tok), engine(e), epstore(s) {
    }

    ~BackFillThreadData() {
        engine->backfillThreadTerminating();
    }

    BackFillVisitor bfv;
    EventuallyPersistentEngine *engine;
    EventuallyPersistentStore *epstore;
};
/// @endcond

extern "C" {
    static void* launch_backfill_thread(void *arg) {
        BackFillThreadData *bftd = static_cast<BackFillThreadData *>(arg);
        ObjectRegistry::onSwitchThread(bftd->engine);

        bftd->epstore->visit(bftd->bfv);
        bftd->bfv.apply();

        delete bftd;
        return NULL;
    }
}

EventuallyPersistentEngine::EventuallyPersistentEngine(GET_SERVER_API get_server_api) :
    dbname("/tmp/test.db"), shardPattern(DEFAULT_SHARD_PATTERN),
    initFile(NULL), postInitFile(NULL), dbStrategy(multi_db),
    warmup(true), wait_for_warmup(true), fail_on_partial_warmup(true),
    startVb0(true), concurrentDB(true), forceShutdown(false), kvstore(NULL),
    epstore(NULL), tapThrottle(new TapThrottle(stats)), databaseInitTime(0), tapKeepAlive(0),
    tapNoopInterval(DEFAULT_TAP_NOOP_INTERVAL), nextTapNoop(0),
    startedEngineThreads(false), shutdown(false),
    getServerApiFunc(get_server_api), getlExtension(NULL), tapConnMap(*this),
    maxItemSize(20*1024*1024), tapBacklogLimit(5000),
    memLowWat(std::numeric_limits<size_t>::max()),
    memHighWat(std::numeric_limits<size_t>::max()),
    minDataAge(DEFAULT_MIN_DATA_AGE),
    queueAgeCap(DEFAULT_QUEUE_AGE_CAP),
    itemExpiryWindow(3), expiryPagerSleeptime(3600), checkpointRemoverInterval(5),
    nVBuckets(1024), dbShards(4), vb_del_chunk_size(100), vb_chunk_del_threshold_time(500),
    mutation_count(0), getlDefaultTimeout(15), getlMaxTimeout(30)
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
    restore.manager = NULL;
    backfillThreads.num = 0;
    backfillThreads.shutdown = false;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::initialize(const char* config) {
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    size_t txnSize = 0;
    size_t tapIdleTimeout = (size_t)-1;

    resetStats();
    if (config != NULL) {
        char *dbn = NULL, *shardPat = NULL, *initf = NULL, *pinitf = NULL,
            *svaltype = NULL, *dbs=NULL;
        size_t htBuckets = 0;
        size_t htLocks = 0;
        size_t maxSize = 0;

        const int max_items = 48;
        struct config_item items[max_items];
        int ii = 0;
        memset(items, 0, sizeof(items));

        items[ii].key = "dbname";
        items[ii].datatype = DT_STRING;
        items[ii].value.dt_string = &dbn;

        ++ii;
        items[ii].key = "shardpattern";
        items[ii].datatype = DT_STRING;
        items[ii].value.dt_string = &shardPat;

        ++ii;
        items[ii].key = "initfile";
        items[ii].datatype = DT_STRING;
        items[ii].value.dt_string = &initf;

        ++ii;
        items[ii].key = "postInitfile";
        items[ii].datatype = DT_STRING;
        items[ii].value.dt_string = &pinitf;

        ++ii;
        items[ii].key = "db_strategy";
        items[ii].datatype = DT_STRING;
        items[ii].value.dt_string = &dbs;

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
        items[ii].key = "concurrentDB";
        items[ii].datatype = DT_BOOL;
        items[ii].value.dt_bool = &concurrentDB;

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
        items[ii].key = "max_txn_size";
        items[ii].datatype = DT_SIZE;
        items[ii].value.dt_size = &txnSize;

        ++ii;
        items[ii].key = "cache_size";
        items[ii].datatype = DT_SIZE;
        items[ii].value.dt_size = &maxSize;

        ++ii;
        items[ii].key = "tap_idle_timeout";
        items[ii].datatype = DT_SIZE;
        items[ii].value.dt_size = &tapIdleTimeout;

        ++ii;
        items[ii].key = "tap_noop_interval";
        items[ii].datatype = DT_SIZE;
        items[ii].value.dt_size = &tapNoopInterval;

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
        items[ii].key = "exp_pager_stime";
        items[ii].datatype = DT_SIZE;
        items[ii].value.dt_size = &expiryPagerSleeptime;

        ++ii;
        items[ii].key = "db_shards";
        items[ii].datatype = DT_SIZE;
        items[ii].value.dt_size = &dbShards;

        ++ii;
        items[ii].key = "max_vbuckets";
        items[ii].datatype = DT_SIZE;
        items[ii].value.dt_size = &nVBuckets;

        ++ii;
        items[ii].key = "vb_del_chunk_size";
        items[ii].datatype = DT_SIZE;
        items[ii].value.dt_size = &vb_del_chunk_size;

        ++ii;
        items[ii].key = "tap_bg_max_pending";
        items[ii].datatype = DT_SIZE;
        items[ii].value.dt_size = &TapProducer::bgMaxPending;

        ++ii;
        items[ii].key = "vb_chunk_del_time";
        items[ii].datatype = DT_SIZE;
        items[ii].value.dt_size = &vb_chunk_del_threshold_time;

        ++ii;
        float tap_backoff_period;
        int tap_backoff_period_idx = ii;
        items[ii].key = "tap_backoff_period";
        items[ii].datatype = DT_FLOAT;
        items[ii].value.dt_float = &tap_backoff_period;

        ++ii;
        size_t tap_ack_window_size;
        int tap_ack_window_size_idx = ii;
        items[ii].key = "tap_ack_window_size";
        items[ii].datatype = DT_SIZE;
        items[ii].value.dt_size = &tap_ack_window_size;

        ++ii;
        size_t tap_ack_interval;
        int tap_ack_interval_idx = ii;
        items[ii].key = "tap_ack_interval";
        items[ii].datatype = DT_SIZE;
        items[ii].value.dt_size = &tap_ack_interval;

        ++ii;
        size_t tap_ack_grace_period;
        int tap_ack_grace_period_idx = ii;
        items[ii].key = "tap_ack_grace_period";
        items[ii].datatype = DT_SIZE;
        items[ii].value.dt_size = &tap_ack_grace_period;

        ++ii;
        size_t tap_ack_initial_sequence_number;
        int tap_ack_initial_sequence_number_idx = ii;
        items[ii].key = "tap_ack_initial_sequence_number";
        items[ii].datatype = DT_SIZE;
        items[ii].value.dt_size = &tap_ack_initial_sequence_number;

        ++ii;
        items[ii].key = "chk_remover_stime";
        items[ii].datatype = DT_SIZE;
        items[ii].value.dt_size = &checkpointRemoverInterval;

        ++ii;
        size_t checkpoint_max_items;
        int checkpoint_max_items_idx = ii;
        items[ii].key = "chk_max_items";
        items[ii].datatype = DT_SIZE;
        items[ii].value.dt_size = &checkpoint_max_items;

        ++ii;
        size_t checkpoint_period;
        int checkpoint_period_idx = ii;
        items[ii].key = "chk_period";
        items[ii].datatype = DT_SIZE;
        items[ii].value.dt_size = &checkpoint_period;

        ++ii;
        bool inconsistentSlaveCheckpoint;
        int inconsistent_chk_idx = ii;
        items[ii].key = "inconsistent_slave_chk";
        items[ii].datatype = DT_BOOL;
        items[ii].value.dt_bool = &inconsistentSlaveCheckpoint;

        ++ii;
        bool restore_mode;
        int restore_idx = ii;
        items[ii].key = "restore_mode";
        items[ii].datatype = DT_BOOL;
        items[ii].value.dt_bool = &restore_mode;

        ++ii;
        float backfill_resident_threshold;
        int backfill_resident_threshold_idx = ii;
        items[ii].key = "bf_resident_threshold";
        items[ii].datatype = DT_FLOAT;
        items[ii].value.dt_float = &backfill_resident_threshold;

        ++ii;
        items[ii].key = "getl_default_timeout";
        items[ii].datatype = DT_SIZE;
        items[ii].value.dt_size = &getlDefaultTimeout;

        ++ii;
        items[ii].key = "getl_max_timeout";
        items[ii].datatype = DT_SIZE;
        items[ii].value.dt_size = &getlMaxTimeout;

        ++ii;
        items[ii].key = NULL;

        assert(ii < max_items);

        if (serverApi->core->parse_config(config, items, stderr) != 0) {
            ret = ENGINE_FAILED;
        } else {
            if (dbn != NULL) {
                dbname = dbn;
            }
            if (shardPat != NULL) {
                shardPattern = shardPat;
            }
            if (initf != NULL) {
                initFile = initf;
            }
            if (pinitf != NULL) {
                postInitFile = pinitf;
            }

            if (items[tap_backoff_period_idx].found) {
                TapProducer::backoffSleepTime = (double)tap_backoff_period;
            }

            if (items[tap_ack_window_size_idx].found) {
                TapProducer::ackWindowSize = (uint32_t)tap_ack_window_size;
            }

            if (items[tap_ack_interval_idx].found) {
                TapProducer::ackInterval = (uint32_t)tap_ack_interval;
            }

            if (items[tap_ack_grace_period_idx].found) {
                TapProducer::ackGracePeriod = (rel_time_t)tap_ack_grace_period;
            }

            if (items[tap_ack_initial_sequence_number_idx].found) {
                uint32_t init_seq_num = (uint32_t)tap_ack_initial_sequence_number;
                TapProducer::initialAckSequenceNumber = init_seq_num == 0 ? 1 : init_seq_num;
            }

            if (items[checkpoint_max_items_idx].found) {
                CheckpointManager::setCheckpointMaxItems(checkpoint_max_items);
            }
            if (items[checkpoint_period_idx].found) {
                CheckpointManager::setCheckpointPeriod(checkpoint_period);
            }
            if (items[inconsistent_chk_idx].found) {
                CheckpointManager::allowInconsistentSlaveCheckpoint(inconsistentSlaveCheckpoint);
            }

            if (items[restore_idx].found && restore_mode) {
                if ((restore.manager = create_restore_manager(*this)) == NULL) {
                    getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                     "Failed to create restore manager");
                    return ENGINE_FAILED;
                }
                restore.enabled.set(true);
            }

            if (items[backfill_resident_threshold_idx].found) {
                BackFillVisitor::setResidentItemThreshold(backfill_resident_threshold);
            }

            if (dbs != NULL) {
                if (!KVStore::stringToType(dbs, dbStrategy)) {
                    getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                     "Unhandled db type: %s", dbs);
                    return ENGINE_FAILED;
                }
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

    if (tapNoopInterval == 0 || tapIdleTimeout == 0) {
        tapNoopInterval = (size_t)-1;
    } else if (tapIdleTimeout != (size_t)-1) {
        tapNoopInterval = tapIdleTimeout / 3;
    }

    if (ret == ENGINE_SUCCESS) {
        time_t start = ep_real_time();
        try {
            kvstore = newKVStore();
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

        if (memLowWat == std::numeric_limits<size_t>::max()) {
            memLowWat = percentOf(StoredValue::getMaxDataSize(stats), 0.6);
        }
        if (memHighWat == std::numeric_limits<size_t>::max()) {
            memHighWat = percentOf(StoredValue::getMaxDataSize(stats), 0.75);
        }

        stats.mem_low_wat = memLowWat;
        stats.mem_high_wat = memHighWat;

        databaseInitTime = ep_real_time() - start;
        epstore = new EventuallyPersistentStore(*this, kvstore, startVb0,
                                                concurrentDB);
        if (epstore == NULL) {
            ret = ENGINE_ENOMEM;
            return ret;
        }

        setMinDataAge(minDataAge);
        setQueueAgeCap(queueAgeCap);
        if (txnSize > 0) {
            setTxnSize(txnSize);
        }

        if (!warmup) {
            epstore->reset();
        }

        SERVER_CALLBACK_API *sapi;
        sapi = getServerApi()->callback;
        sapi->register_callback(reinterpret_cast<ENGINE_HANDLE*>(this),
                ON_DISCONNECT, EvpHandleDisconnect, this);

        startEngineThreads();

        // If requested, don't complete the initialization until the
        // flusher transitions out of the initializing state (i.e
        // warmup is finished).
        const Flusher *flusher = epstore->getFlusher();
        useconds_t sleepTime = 1;
        useconds_t maxSleepTime = 500000;
        if (wait_for_warmup && flusher) {
            while (flusher->state() == initializing) {
                usleep(sleepTime);
                sleepTime = std::min(sleepTime << 1, maxSleepTime);
            }
            if (fail_on_partial_warmup && stats.warmOOM > 0) {
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "Warmup failed to load %d records due to OOM, exiting.\n",
                                 static_cast<unsigned int>(stats.warmOOM));
                exit(1);
            }
        } else {
            // Although we don't wait for the full data load, wait until the states of all vbuckets
            // are loaded from vbucket_states table. This won't take much time.
            while (flusher && !flusher->isVBStateLoaded()) {
                usleep(sleepTime);
                sleepTime = std::min(sleepTime << 1, maxSleepTime);
            }
        }

        // Run the vbucket state snapshot job once after the warmup
        epstore->scheduleVBSnapshot(Priority::VBucketPersistHighPriority);

        if (HashTable::getDefaultStorageValueType() != small) {
            shared_ptr<DispatcherCallback> cb(new ItemPager(epstore, stats));
            epstore->getNonIODispatcher()->schedule(cb, NULL, Priority::ItemPagerPriority, 10);

            shared_ptr<DispatcherCallback> exp_cb(new ExpiredItemPager(epstore, stats,
                                                                       expiryPagerSleeptime));
            epstore->getNonIODispatcher()->schedule(exp_cb, NULL, Priority::ItemPagerPriority,
                                                    expiryPagerSleeptime);
        }

        shared_ptr<DispatcherCallback> htr(new HashtableResizer(epstore));
        epstore->getNonIODispatcher()->schedule(htr, NULL, Priority::HTResizePriority,
                                                10);

        shared_ptr<DispatcherCallback> item_db_cb(epstore->getInvalidItemDbPager());
        epstore->getDispatcher()->schedule(item_db_cb, NULL,
                                           Priority::InvalidItemDbPagerPriority, 0);

        shared_ptr<DispatcherCallback> chk_cb(new ClosedUnrefCheckpointRemover(epstore, stats,
                                                                       checkpointRemoverInterval));
        epstore->getNonIODispatcher()->schedule(chk_cb, NULL,
                                                Priority::CheckpointRemoverPriority,
                                                checkpointRemoverInterval);

        shared_ptr<StatSnap> sscb(new StatSnap(this));
        epstore->getDispatcher()->schedule(sscb, NULL, Priority::StatSnapPriority,
                                           STATSNAP_FREQ);
    }

    if (ret == ENGINE_SUCCESS) {
        getlExtension = new GetlExtension(epstore, getServerApiFunc);
        getlExtension->initialize();
    }

    getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "Engine init complete.\n");

    return ret;
}

KVStore* EventuallyPersistentEngine::newKVStore() {
    KVStoreConfig conf(dbname, shardPattern, initFile,
                       postInitFile, nVBuckets, dbShards);
    return KVStore::create(dbStrategy, stats, conf);
}

void EventuallyPersistentEngine::destroy(bool force) {
    LockHolder lh(backfillThreads.sync);
    backfillThreads.shutdown = true;
    lh.unlock();

    forceShutdown = force;
    stopEngineThreads();
    tapConnMap.shutdownAllTapConnections();

    // wait for all of the backfill threads to complete
    lh.lock();
    while (backfillThreads.num > 0) {
        backfillThreads.sync.wait();
    }
}

/// @cond DETAILS
class AllFlusher : public DispatcherCallback {
public:
    AllFlusher(EventuallyPersistentStore *st, TapConnMap &tcm)
        : epstore(st), tapConnMap(tcm) { }
    bool callback(Dispatcher &, TaskId) {
        doFlush();
        return false;
    }

    void doFlush() {
        epstore->reset();
        tapConnMap.addFlushEvent();
    }

    std::string description() {
        return std::string("Performing flush.");
    }

private:
    EventuallyPersistentStore *epstore;
    TapConnMap                &tapConnMap;
};
/// @endcond

ENGINE_ERROR_CODE EventuallyPersistentEngine::flush(const void *, time_t when) {
    shared_ptr<AllFlusher> cb(new AllFlusher(epstore, tapConnMap));
    if (when == 0) {
        cb->doFlush();
    } else {
        epstore->getNonIODispatcher()->schedule(cb, NULL, Priority::FlushAllPriority,
                                                static_cast<double>(when),
                                                false);
    }

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE  EventuallyPersistentEngine::store(const void *cookie,
                                                     item* itm,
                                                     uint64_t *cas,
                                                     ENGINE_STORE_OPERATION operation,
                                                     uint16_t vbucket)
{
    BlockTimer timer(&stats.storeCmdHisto);
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
            addMutationEvent(it);
        }

        break;

    case OPERATION_ADD:
        // you can't call add while the server is running in restore mode..
        if (restore.enabled.get()) {
            return ENGINE_TMPFAIL;
        }

        ret = epstore->add(*it, cookie);
        if (ret == ENGINE_SUCCESS) {
            *cas = it->getCas();
            addMutationEvent(it);
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
                addMutationEvent(it);
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

                if (old->getCas() == (uint64_t) -1) {
                    // item is locked against updates
                    return ENGINE_TMPFAIL;
                }

                if (it->getCas() != 0 && old->getCas() != it->getCas()) {
                    return ENGINE_KEY_EEXISTS;
                }

                if (operation == OPERATION_APPEND) {
                    if (!old->append(*it)) {
                        itemRelease(cookie, i);
                        return memoryCondition();
                    }
                } else {
                    if (!old->prepend(*it)) {
                        itemRelease(cookie, i);
                        return memoryCondition();
                    }
                }

                ret = store(cookie, old, cas, OPERATION_CAS, vbucket);
                if (ret == ENGINE_SUCCESS) {
                    addMutationEvent(static_cast<Item*>(i));
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
                                                              TapProducer *connection,
                                                              bool &retry) {
    *es = NULL;
    *nes = 0;
    *ttl = (uint8_t)-1;
    *seqno = 0;
    *flags = 0;
    *vbucket = 0;

    retry = false;

    if (connection->shouldFlush()) {
        return TAP_FLUSH;
    }

    // Do not schedule the backfill for the registered TAP client (e.g., incremental backup client)
    if (connection->doRunBackfill && !(connection->registeredTAPClient)) {
        LockHolder holder(backfillThreads.sync);
        if (backfillThreads.shutdown) {
            return TAP_PAUSE;
        }
        ++backfillThreads.num;
        holder.unlock();
        queueBackfill(connection, cookie);
    }

    if (connection->isTimeForNoop()) {
        return TAP_NOOP;
    }

    if (connection->isSuspended() || connection->windowIsFull()) {
        return TAP_PAUSE;
    }

    tap_event_t ret = TAP_PAUSE;
    TapVBucketEvent ev = connection->nextVBucketHighPriority();
    if (ev.event != TAP_PAUSE) {
        switch (ev.event) {
        case TAP_VBUCKET_SET:
            connection->encodeVBucketStateTransition(ev, es, nes, vbucket);
            break;
        case TAP_OPAQUE:
            connection->opaqueCommandCode = ev.state;
            *vbucket = ev.vbucket;
            *es = &connection->opaqueCommandCode;
            *nes = sizeof(connection->opaqueCommandCode);
            break;
        default:
            abort();
        }
        return ev.event;
    }

    // Check if there are any checkpoint start / end messages to be sent to the TAP client.
    queued_item checkpoint_msg = connection->nextCheckpointMessage();
    if (checkpoint_msg->getOperation() != queue_op_empty) {
        switch (checkpoint_msg->getOperation()) {
        case queue_op_checkpoint_start:
            ret = TAP_CHECKPOINT_START;
            break;
        case queue_op_checkpoint_end:
            ret = TAP_CHECKPOINT_END;
            break;
        default:
            abort();
        }
        *vbucket = checkpoint_msg->getVBucketId();
        Item *item = new Item(checkpoint_msg->getKey(), 0, 0, checkpoint_msg->getValue(),
                              0, -1, checkpoint_msg->getVBucketId());
        *itm = item;
        return ret;
    }

    // Check if there are any items fetched from disk for backfill operations.
    if (connection->hasItem()) {
        ret = TAP_MUTATION;
        Item *item = connection->nextFetchedItem();

        ++stats.numTapBGFetched;
        ++connection->queueDrain;

        // If there's a better version in memory, grab it, else go
        // with what we pulled from disk.
        GetValue gv(epstore->get(item->getKey(), item->getVBucketId(),
                                 cookie, false));
        if (gv.getStatus() == ENGINE_SUCCESS) {
            delete item;
            *itm = item = gv.getValue();
        } else {
            *itm = item;
        }
        *vbucket = static_cast<Item*>(*itm)->getVBucketId();

        if (!connection->supportsAck()) {
            if (gv.getStoredValue() != NULL) {
                gv.getStoredValue()->incrementNumReplicas();
                syncRegistry.itemReplicated(*item);
            } else {
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "NULL StoredValue* for key %s, vbucket %d",
                                 item->getKey().c_str(), item->getVBucketId());
            }
        }

        if (!connection->vbucketFilter(*vbucket)) {
            // We were going to use the item that we received from
            // disk, but the filter says not to, so we need to get rid
            // of it now.
            if (gv.getStatus() != ENGINE_SUCCESS) {
                delete item;
            }
            retry = true;
            return TAP_NOOP;
        }

        queued_item qi(new QueuedItem(item->getKey(), item->getValue(), item->getVBucketId(),
                                      queue_op_set, -1, item->getId(), item->getFlags(),
                                      item->getExptime(), item->getCas()));
        connection->addTapLogElement(qi);
    } else if (connection->hasQueuedItem()) {
        if (connection->waitForBackfill() || connection->waitForCheckpointMsgAck()) {
            return TAP_PAUSE;
        }

        bool shouldPause = false;
        queued_item qi = connection->next(shouldPause);
        if (qi->getOperation() == queue_op_empty) {
            if (shouldPause) {
                return TAP_PAUSE;
            }
            retry = true;
            return TAP_NOOP;
        }

        *vbucket = qi->getVBucketId();
        // The item is from the backfill operation and needs to be fetched from the hashtable.
        if (qi->getOperation() == queue_op_set && qi->getValue()->length() == 0) {
            GetValue gv(epstore->get(qi->getKey(), qi->getVBucketId(), cookie,
                                     false, false));
            ENGINE_ERROR_CODE r = gv.getStatus();
            if (r == ENGINE_SUCCESS) {
                assert(gv.getStoredValue() != NULL);
                *itm = gv.getValue();
                ret = TAP_MUTATION;

                if (!connection->supportsAck()) {
                    gv.getStoredValue()->incrementNumReplicas();
                    syncRegistry.itemReplicated(*gv.getValue());
                }

                ++stats.numTapFGFetched;
                ++connection->queueDrain;
            } else if (r == ENGINE_KEY_ENOENT) {
                // Any deletions after the backfill gets the list of keys will be transmitted
                // through checkpoints below. Therefore, don't need to transmit it here.
                retry = true;
                return TAP_NOOP;
            } else if (r == ENGINE_EWOULDBLOCK) {
                connection->queueBGFetch(qi->getKey(), gv.getId(), *vbucket,
                                         epstore->getVBucketVersion(*vbucket));
                // This can optionally collect a few and batch them.
                connection->runBGFetch(epstore->getRODispatcher(), cookie);
                // If there's an item ready, return NOOP so we'll come
                // back immediately, otherwise pause the connection
                // while we wait.
                if (connection->hasQueuedItem() || connection->hasItem()) {
                    retry = true;
                    return TAP_NOOP;
                }
                return TAP_PAUSE;
            } else {
                if (r == ENGINE_NOT_MY_VBUCKET) {
                    getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                     "Trying to fetch an item for a bucket that "
                                     "doesn't exist on this server <%s>\n",
                                     connection->getName().c_str());
                } else {
                    getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                     "Tap internal error Internal error! <%s>:%d.  "
                                     "Disconnecting\n", connection->getName().c_str(), r);
                    return TAP_DISCONNECT;
                }
                retry = true;
                return TAP_NOOP;
            }
        } else { // The item is from the checkpoint in the unified queue.
            if (qi->getOperation() == queue_op_set) {
                Item *item = new Item(qi->getKey(), qi->getFlags(), qi->getExpiryTime(),
                                  qi->getValue(), qi->getCas(), qi->getRowId(), qi->getVBucketId());
                *itm = item;
                ret = TAP_MUTATION;

                StoredValue *sv = epstore->getStoredValue(qi->getKey(), qi->getVBucketId(), false);

                if (!connection->supportsAck()) {
                    if (sv && sv->getCas() == item->getCas()) {
                        sv->incrementNumReplicas();
                        syncRegistry.itemReplicated(*item);
                    }
                }

                ++stats.numTapFGFetched;
                ++connection->queueDrain;
            } else if (qi->getOperation() == queue_op_del) {
                ret = TAP_DELETION;
                ENGINE_ERROR_CODE r = itemAllocate(cookie, itm, qi->getKey().c_str(),
                                                   qi->getKey().length(), 0, 0, 0);
                if (r != ENGINE_SUCCESS) {
                    getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                     "Failed to allocate memory for deletion of: %s\n",
                                     qi->getKey().c_str());
                    ret = TAP_PAUSE;
                }
                ++stats.numTapDeletes;
                ++connection->queueDrain;
            }
        }

        if (ret == TAP_MUTATION || ret == TAP_DELETION) {
            connection->addTapLogElement(qi);
        }
    }

    if (ret == TAP_PAUSE && connection->complete()) {
        ev = connection->nextVBucketLowPriority();
        if (ev.event != TAP_PAUSE) {
            RCPtr<VBucket> vb = getVBucket(ev.vbucket);
            vbucket_state_t myState(vb ? vb->getState() : vbucket_state_dead);
            assert(ev.event == TAP_VBUCKET_SET);
            if (ev.state == vbucket_state_active && myState == vbucket_state_active &&
                connection->getTapAckLogSize() < MAX_TAKEOVER_TAP_LOG_SIZE) {
                // Set vbucket state to dead if the number of items waiting for acks is
                // less than the threshold.
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "Vbucket <%d> is going dead.\n",
                                  ev.vbucket);
                epstore->setVBucketState(ev.vbucket, vbucket_state_dead);
                connection->setTakeOverCompletionPhase(true);
            }
            if (connection->getTapAckLogSize() > 1) {
                // We're still waiting for acks for regular items.
                // Pop the tap log for this vbucket_state_active message and requeue it.
                connection->popTapLog();
                TapVBucketEvent lo(TAP_VBUCKET_SET, ev.vbucket, vbucket_state_active);
                connection->addVBucketLowPriority(lo);
                ret = TAP_PAUSE;
            } else {
                connection->encodeVBucketStateTransition(ev, es, nes, vbucket);
                ret = ev.event;
            }
        } else if (connection->hasPendingAcks()) {
            ret = TAP_PAUSE;
        } else {
            getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                             "Disconnecting tap stream <%s>",
                             connection->getName().c_str());
            connection->setDisconnect(true);
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
    TapProducer *connection = getTapProducer(cookie);
    if (!connection) {
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                         "Failed to lookup TAP connection.. Disconnecting\n");
        return TAP_DISCONNECT;
    }

    // Clear the notifySent flag and the paused flag to cause
    // the backend to schedule notification while we're figuring if
    // we've got data to send or not (to avoid race conditions)
    connection->paused.set(true);
    connection->notifySent.set(false);

    bool retry = false;
    tap_event_t ret;

    connection->lastWalkTime = ep_current_time();
    do {
        ret = doWalkTapQueue(cookie, itm, es, nes, ttl, flags,
                             seqno, vbucket, connection, retry);
    } while (retry);

    if (ret != TAP_PAUSE && ret != TAP_DISCONNECT) {
        // we're no longer paused (the front-end will call us again)
        // so we don't need the engine to notify us about new changes..
        connection->paused.set(false);
        if (ret == TAP_NOOP) {
            *seqno = 0;
        } else {
            ++stats.numTapFetched;
            *seqno = connection->getSeqno();
            if (connection->requestAck(ret, *vbucket)) {
                *flags = TAP_FLAG_ACK;
            }
        }
    }

    return ret;
}

bool EventuallyPersistentEngine::createTapQueue(const void *cookie,
                                                std::string &client,
                                                uint32_t flags,
                                                const void *userdata,
                                                size_t nuserdata)
{
    if (serverApi->cookie->reserve(cookie) != ENGINE_SUCCESS) {
        return false;
    }

    std::string name = "eq_tapq:";
    if (client.length() == 0) {
        name.assign(TapConnection::getAnonName());
    } else {
        name.append(client);
    }

    // Decoding the userdata section of the packet and update the filters
    const char *ptr = static_cast<const char*>(userdata);
    uint64_t backfillAge = 0;
    std::vector<uint16_t> vbuckets;
    std::map<uint16_t, uint64_t> lastCheckpointIds;

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
            nuserdata -= (sizeof(uint16_t) * nvbuckets);
        }
    }

    if (flags & TAP_CONNECT_CHECKPOINT) {
        uint16_t nCheckpoints = 0;
        if (nuserdata >= sizeof(nCheckpoints)) {
            memcpy(&nCheckpoints, ptr, sizeof(nCheckpoints));
            nuserdata -= sizeof(nCheckpoints);
            ptr += sizeof(nCheckpoints);
            nCheckpoints = ntohs(nCheckpoints);
        }
        if (nCheckpoints > 0) {
            assert(nuserdata >= ((sizeof(uint16_t) + sizeof(uint64_t)) * nCheckpoints));
            for (uint16_t j = 0; j < nCheckpoints; ++j) {
                uint16_t vbid;
                uint64_t checkpointId;
                memcpy(&vbid, ptr, sizeof(vbid));
                ptr += sizeof(uint16_t);
                memcpy(&checkpointId, ptr, sizeof(checkpointId));
                ptr += sizeof(uint64_t);
                lastCheckpointIds[ntohs(vbid)] = ntohll(checkpointId);
            }
            nuserdata -= ((sizeof(uint16_t) + sizeof(uint64_t)) * nCheckpoints);
        }
    }

    bool isRegisteredClient = false;
    bool isClosedCheckpointOnly = false;
    if (flags & TAP_CONNECT_REGISTERED_CLIENT) {
        isRegisteredClient = true;
        uint8_t closedCheckpointOnly = 0;
        if (nuserdata >= sizeof(closedCheckpointOnly)) {
            memcpy(&closedCheckpointOnly, ptr, sizeof(closedCheckpointOnly));
            nuserdata -= sizeof(closedCheckpointOnly);
            ptr += sizeof(closedCheckpointOnly);
        }
        isClosedCheckpointOnly = closedCheckpointOnly > 0 ? true : false;
    }

    TapProducer *tap = tapConnMap.newProducer(cookie, name, flags,
                                              backfillAge,
                                              static_cast<int>(tapKeepAlive));

    tap->setRegisteredClient(isRegisteredClient);
    tap->setClosedCheckpointOnlyFlag(isClosedCheckpointOnly);
    tap->setVBucketFilter(vbuckets);
    tap->registerTAPCursor(lastCheckpointIds);
    serverApi->cookie->store_engine_specific(cookie, tap);
    serverApi->cookie->set_tap_nack_mode(cookie, tap->supportsAck());
    tapConnMap.notify();
    return true;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::tapNotify(const void *cookie,
                                                        void *engine_specific,
                                                        uint16_t nengine,
                                                        uint8_t, // ttl
                                                        uint16_t tap_flags,
                                                        tap_event_t tap_event,
                                                        uint32_t tap_seqno,
                                                        const void *key,
                                                        size_t nkey,
                                                        uint32_t flags,
                                                        uint32_t exptime,
                                                        uint64_t, // cas
                                                        const void *data,
                                                        size_t ndata,
                                                        uint16_t vbucket)
{
    void *specific = serverApi->cookie->get_engine_specific(cookie);
    TapConnection *connection = NULL;
    if (specific == NULL) {
        if (tap_event == TAP_ACK) {
            // tap producer is no longer connected..
            return ENGINE_DISCONNECT;
        } else {
            // Create a new tap consumer...
            connection = tapConnMap.newConsumer(cookie);
            if (connection == NULL) {
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "Failed to create new tap consumer.. disconnecting\n");
                return ENGINE_DISCONNECT;
            }
            serverApi->cookie->store_engine_specific(cookie, connection);
        }
    } else {
        connection = reinterpret_cast<TapConnection *>(specific);
    }

    std::string k(static_cast<const char*>(key), nkey);
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    switch (tap_event) {
    case TAP_ACK:
        ret = processTapAck(cookie, tap_seqno, tap_flags, k);
        break;
    case TAP_FLUSH:
        ret = flush(cookie, 0);
        break;
    case TAP_DELETION:
        {
            ret = epstore->del(k, 0, vbucket, cookie, true);
            if (ret == ENGINE_KEY_ENOENT) {
                ret = ENGINE_SUCCESS;
            }
            if (ret == ENGINE_SUCCESS) {
                addDeleteEvent(k, vbucket, 0);
            }
            TapConsumer *tc = dynamic_cast<TapConsumer*>(connection);
            if (tc && !tc->supportsCheckpointSync()) {
                // If the checkpoint synchronization is not supported,
                // check if a new checkpoint should be created or not.
                tc->checkVBOpenCheckpoint(vbucket);
            }
        }
        break;
    case TAP_CHECKPOINT_START:
    case TAP_CHECKPOINT_END:
        {
            TapConsumer *tc = dynamic_cast<TapConsumer*>(connection);
            if (tc) {
                if (data != NULL) {
                    uint64_t checkpointId;
                    memcpy(&checkpointId, data, sizeof(checkpointId));
                    checkpointId = ntohll(checkpointId);
                    ret = tc->processCheckpointCommand(tap_event, vbucket, checkpointId) ?
                          ENGINE_SUCCESS : ENGINE_DISCONNECT;
                } else {
                    ret = ENGINE_DISCONNECT;
                    getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                     "Checkpoint Id is missing in CHECKPOINT messages.\n");
                }
            } else {
                ret = ENGINE_DISCONNECT;
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "TAP consumer doesn't exists. Force disconnect\n");
            }
        }
        break;
    case TAP_MUTATION:
        {
            if (!tapThrottle->shouldProcess()) {
                ++stats.tapThrottled;
                if (connection->supportsAck()) {
                    ret = ENGINE_TMPFAIL;
                } else {
                    ret = ENGINE_DISCONNECT;
                    getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                     "Don't know how to trottle streams without ack support. Disconnecting\n");
                }
                break;
            }

            BlockTimer timer(&stats.tapMutationHisto);
            shared_ptr<const Blob> vblob(Blob::New(static_cast<const char*>(data), ndata));

            Item *item = new Item(k, flags, exptime, vblob);
            item->setVBucketId(vbucket);

            ret = epstore->set(*item, cookie, true);
            if (ret == ENGINE_SUCCESS) {
                addMutationEvent(item);
            } else if (ret == ENGINE_ENOMEM) {
                if (connection->supportsAck()) {
                    ret = ENGINE_TMPFAIL;
                } else {
                    getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                     "Connection does not support tap ack'ing.. disconnect it\n");
                    ret = ENGINE_DISCONNECT;
                }
            }

            delete item;
            TapConsumer *tc = dynamic_cast<TapConsumer*>(connection);
            if (tc && !tc->supportsCheckpointSync()) {
                tc->checkVBOpenCheckpoint(vbucket);
            }

            if (ret == ENGINE_DISCONNECT) {
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "Failed to apply tap mutation. Force disconnect\n");
            }
        }
        break;

    case TAP_OPAQUE:
        if (nengine == sizeof(uint32_t)) {
            uint32_t cc;
            memcpy(&cc, engine_specific, sizeof(cc));
            cc = ntohl(cc);

            switch (cc) {
            case TAP_OPAQUE_ENABLE_AUTO_NACK:
                connection->setSupportAck(true);

                getLogger()->log(EXTENSION_LOG_INFO, NULL,
                                 "Enable auto nack mode\n");
                serverApi->cookie->set_tap_nack_mode(cookie, true);
                break;
            case TAP_OPAQUE_ENABLE_CHECKPOINT_SYNC:
                connection->setSupportCheckpointSync(true);
                getLogger()->log(EXTENSION_LOG_INFO, NULL,
                                 "Enable checkpoint synchronization\n");
                break;
            case TAP_OPAQUE_OPEN_CHECKPOINT:
                /**
                 * This event is only received by the TAP client that wants to get mutations
                 * from closed checkpoints only. At this time, only incremental backup client
                 * receives this event so that it can close the connection and reconnect later.
                 */
                break;
            case TAP_OPAQUE_INITIAL_VBUCKET_STREAM:
                {
                    BlockTimer timer(&stats.tapVbucketResetHisto);
                    ret = resetVBucket(vbucket) ? ENGINE_SUCCESS : ENGINE_DISCONNECT;
                    if (ret == ENGINE_DISCONNECT) {
                        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                     "Failed to reset a vbucket %d. Force disconnect\n", vbucket);
                    }
                }
                break;
            case TAP_OPAQUE_START_ONLINEUPDATE:
            case TAP_OPAQUE_STOP_ONLINEUPDATE:
            case TAP_OPAQUE_REVERT_ONLINEUPDATE:
                {
                    TapConsumer *tc = dynamic_cast<TapConsumer*>(connection);
                    if (tc) {
                        if (tc->processOnlineUpdateCommand(cc, vbucket)) {
                            ret = ENGINE_SUCCESS;
                        } else {
                            ret = ENGINE_DISCONNECT;
                        }
                    } else {
                        ret = ENGINE_DISCONNECT;
                        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                         "TAP consumer doesn't exists. Force disconnect\n");
                    }
                }
                break;
            case TAP_OPAQUE_CLOSE_TAP_STREAM:
                /**
                 * This event is sent by the eVBucketMigrator to notify that the source node
                 * closes the tap replication stream and switches to TAKEOVER_VBUCKETS phase.
                 * This is just an informative message and doesn't require any action.
                 */
                break;
            default:
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "Received an unknown opaque command\n");
            }
        } else {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Received tap opaque with unknown size %d\n",
                             nengine);
        }
        break;

    case TAP_VBUCKET_SET:
        {
            BlockTimer timer(&stats.tapVbucketSetHisto);

            if (nengine != sizeof(vbucket_state_t)) {
                // illegal datasize
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "Received TAP_VBUCKET_SET with illegal size. force disconnect\n");
                ret = ENGINE_DISCONNECT;
                break;
            }

            vbucket_state_t state;
            memcpy(&state, engine_specific, nengine);
            state = (vbucket_state_t)ntohl(state);

            if (!is_valid_vbucket_state_t(state)) {
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "Received an invalid vbucket state, diconnecting\n");
                ret = ENGINE_DISCONNECT;
                break;
            }

            epstore->setVBucketState(vbucket, state);
        }
        break;

    default:
        // Unknown command
        ;
    }

    connection->processedEvent(tap_event, ret);
    return ret;
}

TapProducer* EventuallyPersistentEngine::getTapProducer(const void *cookie) {
    TapProducer *rv =
        reinterpret_cast<TapProducer*>(serverApi->cookie->get_engine_specific(cookie));
    if (!(rv && rv->connected)) {
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                         "Walking a non-existent tap queue, disconnecting\n");
        return NULL;
    }

    if (rv->doDisconnect()) {
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                         "Disconnecting pending connection\n");
        return NULL;
    }
    return rv;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::processTapAck(const void *cookie,
                                                            uint32_t seqno,
                                                            uint16_t status,
                                                            const std::string &msg)
{
    TapProducer *connection = getTapProducer(cookie);
    if (!connection) {
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

void EventuallyPersistentEngine::queueBackfill(TapProducer *tc, const void *tok) {
    tc->doRunBackfill = false;
    BackFillThreadData *bftd = new BackFillThreadData(this, tc, epstore, tok);
    pthread_attr_t attr;

    if (pthread_attr_init(&attr) != 0 ||
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED) != 0) {
        delete bftd;
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

template <typename T>
static void add_casted_stat(const char *k, T v,
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

/// @cond DETAILS
/**
 * Convert a histogram into a bunch of calls to add stats.
 */
template <typename T>
struct histo_stat_adder {
    histo_stat_adder(const char *k, ADD_STAT a, const void *c)
        : prefix(k), add_stat(a), cookie(c) {}
    void operator() (const HistogramBin<T>* b) {
        if (b->count()) {
            std::stringstream ss;
            ss << prefix << "_" << b->start() << "," << b->end();
            add_casted_stat(ss.str().c_str(), b->count(), add_stat, cookie);
        }
    }
    const char *prefix;
    ADD_STAT add_stat;
    const void *cookie;
};
/// @endcond

template <typename T>
static void add_casted_stat(const char *k, const Histogram<T> &v,
                            ADD_STAT add_stat, const void *cookie) {
    histo_stat_adder<T> a(k, add_stat, cookie);
    std::for_each(v.begin(), v.end(), a);
}

bool VBucketCountVisitor::visitBucket(RCPtr<VBucket> vb) {
    ++numVbucket;
    numItems += vb->ht.getNumItems();
    nonResident += vb->ht.getNumNonResidentItems();

    if (desired_state != vbucket_state_dead) {
        htMemory += vb->ht.memorySize();
        htItemMemory += vb->ht.getItemMemory();
        htCacheSize += vb->ht.cacheSize;
        numEjects += vb->ht.getNumEjects();
        opsCreate += vb->opsCreate;
        opsUpdate += vb->opsUpdate;
        opsDelete += vb->opsDelete;
        opsReject += vb->opsReject;

        queueSize += vb->dirtyQueueSize;
        queueMemory += vb->dirtyQueueMem;
        queueFill += vb->dirtyQueueFill;
        queueDrain += vb->dirtyQueueDrain;
        queueAge += vb->getQueueAge();
        pendingWrites += vb->dirtyQueuePendingWrites;
        onlineUpdate |= vb->checkpointManager.isOnlineUpdate();
    }

    return false;
}

/**
 * A container class holding VBucketCountVisitors to aggregate stats for different
 * vbucket states.
 */
class VBucketCountAggregator : public VBucketVisitor  {
public:
    bool visitBucket(RCPtr<VBucket> vb)  {
        std::map<vbucket_state_t, VBucketCountVisitor*>::iterator it;
        it = visitorMap.find(vb->getState());
        if ( it != visitorMap.end() ) {
            it->second->visitBucket(vb);
        }

        return false;
    }

    void addVisitor(VBucketCountVisitor* visitor)  {
        visitorMap[visitor->getVBucketState()] = visitor;
    }
private:
    std::map<vbucket_state_t, VBucketCountVisitor*> visitorMap;
};

ENGINE_ERROR_CODE EventuallyPersistentEngine::doEngineStats(const void *cookie,
                                                            ADD_STAT add_stat) {
    VBucketCountAggregator aggregator;

    VBucketCountVisitor activeCountVisitor(vbucket_state_active);
    aggregator.addVisitor(&activeCountVisitor);

    VBucketCountVisitor replicaCountVisitor(vbucket_state_replica);
    aggregator.addVisitor(&replicaCountVisitor);

    VBucketCountVisitor pendingCountVisitor(vbucket_state_pending);
    aggregator.addVisitor(&pendingCountVisitor);

    VBucketCountVisitor deadCountVisitor(vbucket_state_dead);
    aggregator.addVisitor(&deadCountVisitor);

    epstore->visit(aggregator);

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
    add_casted_stat("ep_total_new_items", stats.newItems, add_stat, cookie);
    add_casted_stat("ep_total_del_items", stats.delItems, add_stat, cookie);
    add_casted_stat("ep_total_persisted",
                    epstats.totalPersisted, add_stat, cookie);
    add_casted_stat("ep_item_flush_failed",
                    epstats.flushFailed, add_stat, cookie);
    add_casted_stat("ep_item_commit_failed",
                    epstats.commitFailed, add_stat, cookie);
    add_casted_stat("ep_item_begin_failed",
                    epstats.beginFailed, add_stat, cookie);
    add_casted_stat("ep_expired", epstats.expired, add_stat, cookie);
    add_casted_stat("ep_item_flush_expired",
                    epstats.flushExpired, add_stat, cookie);
    add_casted_stat("ep_queue_size",
                    epstats.queue_size, add_stat, cookie);
    add_casted_stat("ep_flusher_todo",
                    epstats.flusher_todo, add_stat, cookie);
    add_casted_stat("ep_uncommitted_items",
                    epstore->getNumUncommittedItems(), add_stat, cookie);
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
    add_casted_stat("ep_flush_all",
                    epstore->isFlushAllScheduled() ? "true" : "false", add_stat, cookie);
    add_casted_stat("curr_items", activeCountVisitor.getNumItems(), add_stat, cookie);
    add_casted_stat("curr_items_tot",
                   activeCountVisitor.getNumItems() +
                   replicaCountVisitor.getNumItems() +
                   pendingCountVisitor.getNumItems(),
                   add_stat, cookie);
    add_casted_stat("vb_active_num", activeCountVisitor.getVBucketNumber(), add_stat, cookie);
    add_casted_stat("vb_active_curr_items", activeCountVisitor.getNumItems(),
                   add_stat, cookie);
    add_casted_stat("vb_active_num_non_resident", activeCountVisitor.getNonResident(),
                    add_stat, cookie);
    add_casted_stat("vb_active_perc_mem_resident", activeCountVisitor.getMemResidentPer(),
                    add_stat, cookie);
    add_casted_stat("vb_active_eject", activeCountVisitor.getEjects(), add_stat, cookie);
    add_casted_stat("vb_active_ht_memory", activeCountVisitor.getHashtableMemory(),
                   add_stat, cookie);
    add_casted_stat("vb_active_itm_memory", activeCountVisitor.getItemMemory(),
                   add_stat, cookie);
    add_casted_stat("vb_active_ops_create", activeCountVisitor.getOpsCreate(), add_stat, cookie);
    add_casted_stat("vb_active_ops_update", activeCountVisitor.getOpsUpdate(), add_stat, cookie);
    add_casted_stat("vb_active_ops_delete", activeCountVisitor.getOpsDelete(), add_stat, cookie);
    add_casted_stat("vb_active_ops_reject", activeCountVisitor.getOpsReject(), add_stat, cookie);
    add_casted_stat("vb_active_queue_size", activeCountVisitor.getQueueSize(), add_stat, cookie);
    add_casted_stat("vb_active_queue_memory", activeCountVisitor.getQueueMemory(),
                   add_stat, cookie);
    add_casted_stat("vb_active_queue_age", activeCountVisitor.getAge(), add_stat, cookie);
    add_casted_stat("vb_active_queue_pending", activeCountVisitor.getPendingWrites(),
                   add_stat, cookie);
    add_casted_stat("vb_active_queue_fill", activeCountVisitor.getQueueFill(), add_stat, cookie);
    add_casted_stat("vb_active_queue_drain", activeCountVisitor.getQueueDrain(),
                   add_stat, cookie);

    add_casted_stat("vb_replica_num", replicaCountVisitor.getVBucketNumber(), add_stat, cookie);
    add_casted_stat("vb_replica_curr_items", replicaCountVisitor.getNumItems(), add_stat, cookie);
    add_casted_stat("vb_replica_num_non_resident", replicaCountVisitor.getNonResident(),
                   add_stat, cookie);
    add_casted_stat("vb_replica_perc_mem_resident", replicaCountVisitor.getMemResidentPer(),
                   add_stat, cookie);
    add_casted_stat("vb_replica_eject", replicaCountVisitor.getEjects(), add_stat, cookie);
    add_casted_stat("vb_replica_ht_memory", replicaCountVisitor.getHashtableMemory(),
                   add_stat, cookie);
    add_casted_stat("vb_replica_itm_memory", replicaCountVisitor.getItemMemory(), add_stat, cookie);
    add_casted_stat("vb_replica_ops_create", replicaCountVisitor.getOpsCreate(), add_stat, cookie);
    add_casted_stat("vb_replica_ops_update", replicaCountVisitor.getOpsUpdate(), add_stat, cookie);
    add_casted_stat("vb_replica_ops_delete", replicaCountVisitor.getOpsDelete(), add_stat, cookie);
    add_casted_stat("vb_replica_ops_reject", replicaCountVisitor.getOpsReject(), add_stat, cookie);
    add_casted_stat("vb_replica_queue_size", replicaCountVisitor.getQueueSize(), add_stat, cookie);
    add_casted_stat("vb_replica_queue_memory", replicaCountVisitor.getQueueMemory(),
                   add_stat, cookie);
    add_casted_stat("vb_replica_queue_age", replicaCountVisitor.getAge(), add_stat, cookie);
    add_casted_stat("vb_replica_queue_pending", replicaCountVisitor.getPendingWrites(),
                   add_stat, cookie);
    add_casted_stat("vb_replica_queue_fill", replicaCountVisitor.getQueueFill(), add_stat, cookie);
    add_casted_stat("vb_replica_queue_drain", replicaCountVisitor.getQueueDrain(), add_stat, cookie);

    add_casted_stat("vb_pending_num", pendingCountVisitor.getVBucketNumber(), add_stat, cookie);
    add_casted_stat("vb_pending_curr_items", pendingCountVisitor.getNumItems(), add_stat, cookie);
    add_casted_stat("vb_pending_num_non_resident", pendingCountVisitor.getNonResident(),
                   add_stat, cookie);
    add_casted_stat("vb_pending_perc_mem_resident", pendingCountVisitor.getMemResidentPer(),
                   add_stat, cookie);
    add_casted_stat("vb_pending_eject", pendingCountVisitor.getEjects(), add_stat, cookie);
    add_casted_stat("vb_pending_ht_memory", pendingCountVisitor.getHashtableMemory(),
                   add_stat, cookie);
    add_casted_stat("vb_pending_itm_memory", pendingCountVisitor.getItemMemory(), add_stat, cookie);
    add_casted_stat("vb_pending_ops_create", pendingCountVisitor.getOpsCreate(), add_stat, cookie);
    add_casted_stat("vb_pending_ops_update", pendingCountVisitor.getOpsUpdate(), add_stat, cookie);
    add_casted_stat("vb_pending_ops_delete", pendingCountVisitor.getOpsDelete(), add_stat, cookie);
    add_casted_stat("vb_pending_ops_reject", pendingCountVisitor.getOpsReject(), add_stat, cookie);
    add_casted_stat("vb_pending_queue_size", pendingCountVisitor.getQueueSize(), add_stat, cookie);
    add_casted_stat("vb_pending_queue_memory", pendingCountVisitor.getQueueMemory(),
                   add_stat, cookie);
    add_casted_stat("vb_pending_queue_age", pendingCountVisitor.getAge(), add_stat, cookie);
    add_casted_stat("vb_pending_queue_pending", pendingCountVisitor.getPendingWrites(),
                   add_stat, cookie);
    add_casted_stat("vb_pending_queue_fill", pendingCountVisitor.getQueueFill(), add_stat, cookie);
    add_casted_stat("vb_pending_queue_drain", pendingCountVisitor.getQueueDrain(), add_stat, cookie);

    add_casted_stat("vb_dead_num", deadCountVisitor.getVBucketNumber(), add_stat, cookie);

    add_casted_stat("ep_vb_total",
                   activeCountVisitor.getVBucketNumber() +
                   replicaCountVisitor.getVBucketNumber() +
                   pendingCountVisitor.getVBucketNumber() +
                   deadCountVisitor.getVBucketNumber(),
                   add_stat, cookie);

    add_casted_stat("ep_diskqueue_items",
                    activeCountVisitor.getQueueSize() +
                    replicaCountVisitor.getQueueSize() +
                    pendingCountVisitor.getQueueSize(),
                    add_stat, cookie);
    add_casted_stat("ep_diskqueue_memory",
                    activeCountVisitor.getQueueMemory() +
                    replicaCountVisitor.getQueueMemory() +
                    pendingCountVisitor.getQueueMemory(),
                    add_stat, cookie);
    add_casted_stat("ep_diskqueue_fill",
                    activeCountVisitor.getQueueFill() +
                    replicaCountVisitor.getQueueFill() +
                    pendingCountVisitor.getQueueFill(),
                    add_stat, cookie);
    add_casted_stat("ep_diskqueue_drain",
                    activeCountVisitor.getQueueDrain() +
                    replicaCountVisitor.getQueueDrain() +
                    pendingCountVisitor.getQueueDrain(),
                    add_stat, cookie);
    add_casted_stat("ep_diskqueue_pending",
                    activeCountVisitor.getPendingWrites() +
                    replicaCountVisitor.getPendingWrites() +
                    pendingCountVisitor.getPendingWrites(),
                    add_stat, cookie);

    add_casted_stat("mem_used", stats.currentSize + stats.memOverhead, add_stat,
                    cookie);
    add_casted_stat("ep_kv_size", stats.currentSize, add_stat, cookie);
    add_casted_stat("ep_overhead", stats.memOverhead, add_stat, cookie);
    add_casted_stat("ep_max_data_size", epstats.maxDataSize, add_stat, cookie);
    add_casted_stat("ep_mem_low_wat", epstats.mem_low_wat, add_stat, cookie);
    add_casted_stat("ep_mem_high_wat", epstats.mem_high_wat, add_stat, cookie);
    add_casted_stat("ep_total_cache_size",
                    activeCountVisitor.getCacheSize() +
                    replicaCountVisitor.getCacheSize() +
                    pendingCountVisitor.getCacheSize(),
                    add_stat, cookie);
    add_casted_stat("ep_oom_errors", stats.oom_errors, add_stat, cookie);
    add_casted_stat("ep_tmp_oom_errors", stats.tmp_oom_errors, add_stat, cookie);
    add_casted_stat("ep_storage_type",
                    HashTable::getDefaultStorageValueTypeStr(),
                    add_stat, cookie);
    add_casted_stat("ep_bg_fetched", epstats.bg_fetched, add_stat,
                    cookie);
    add_casted_stat("ep_tap_bg_fetched", stats.numTapBGFetched, add_stat, cookie);
    add_casted_stat("ep_tap_bg_fetch_requeued", stats.numTapBGFetchRequeued,
                    add_stat, cookie);
    add_casted_stat("ep_num_pager_runs", epstats.pagerRuns, add_stat,
                    cookie);
    add_casted_stat("ep_num_expiry_pager_runs", epstats.expiryPagerRuns, add_stat,
                    cookie);
    add_casted_stat("ep_num_checkpoint_remover_runs", epstats.checkpointRemoverRuns,
                    add_stat, cookie);
    add_casted_stat("ep_items_rm_from_checkpoints", epstats.itemsRemovedFromCheckpoints,
                    add_stat, cookie);
    add_casted_stat("ep_num_value_ejects", epstats.numValueEjects, add_stat,
                    cookie);
    add_casted_stat("ep_num_eject_replicas", epstats.numReplicaEjects, add_stat,
                    cookie);
    add_casted_stat("ep_num_eject_failures", epstats.numFailedEjects, add_stat,
                    cookie);
    add_casted_stat("ep_num_not_my_vbuckets", epstats.numNotMyVBuckets, add_stat,
                    cookie);
    add_casted_stat("ep_db_cleaner_status",
                    epstats.dbCleanerComplete.get() ? "complete" : "running",
                    add_stat, cookie);
    add_casted_stat("ep_onlineupdate",
                    (activeCountVisitor.isOnlineUpdate() ||
                     pendingCountVisitor.isOnlineUpdate() ||
                     replicaCountVisitor.isOnlineUpdate() )
                    ? "true" : "false",
                    add_stat, cookie);
    add_casted_stat("ep_onlineupdate_revert_delete", epstats.numRevertDeletes, add_stat,
                    cookie);
    add_casted_stat("ep_onlineupdate_revert_add", epstats.numRevertAdds, add_stat,
                    cookie);
    add_casted_stat("ep_onlineupdate_revert_update", epstats.numRevertUpdates, add_stat,
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
    add_casted_stat("ep_dbshards", dbShards, add_stat, cookie);
    add_casted_stat("ep_db_strategy", KVStore::typeToString(dbStrategy),
                    add_stat, cookie);
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
                    epstats.pendingOpsMaxDuration,
                    add_stat, cookie);

    if (epstats.vbucketDeletions > 0) {
        add_casted_stat("ep_vbucket_del_max_walltime",
                        epstats.vbucketDelMaxWalltime,
                        add_stat, cookie);
        add_casted_stat("ep_vbucket_del_total_walltime",
                        epstats.vbucketDelTotWalltime,
                        add_stat, cookie);
        add_casted_stat("ep_vbucket_del_avg_walltime",
                        epstats.vbucketDelTotWalltime / epstats.vbucketDeletions,
                        add_stat, cookie);
    }

    if (epstats.bgNumOperations > 0) {
        add_casted_stat("ep_bg_num_samples", epstats.bgNumOperations, add_stat, cookie);
        add_casted_stat("ep_bg_min_wait",
                        epstats.bgMinWait,
                        add_stat, cookie);
        add_casted_stat("ep_bg_max_wait",
                        epstats.bgMaxWait,
                        add_stat, cookie);
        add_casted_stat("ep_bg_wait_avg",
                        epstats.bgWait / epstats.bgNumOperations,
                        add_stat, cookie);
        add_casted_stat("ep_bg_min_load",
                        epstats.bgMinLoad,
                        add_stat, cookie);
        add_casted_stat("ep_bg_max_load",
                        epstats.bgMaxLoad,
                        add_stat, cookie);
        add_casted_stat("ep_bg_load_avg",
                        epstats.bgLoad / epstats.bgNumOperations,
                        add_stat, cookie);
        add_casted_stat("ep_bg_wait",
                        epstats.bgWait,
                        add_stat, cookie);
        add_casted_stat("ep_bg_load",
                        epstats.bgLoad,
                        add_stat, cookie);
    }

    StorageProperties sprop(epstore->getStorageProperties());
    add_casted_stat("ep_store_max_concurrency", sprop.maxConcurrency(),
                    add_stat, cookie);
    add_casted_stat("ep_store_max_readers", sprop.maxReaders(),
                    add_stat, cookie);
    add_casted_stat("ep_store_max_readwrite", sprop.maxWriters(),
                    add_stat, cookie);
    add_casted_stat("ep_num_non_resident",
                    activeCountVisitor.getNonResident() +
                    pendingCountVisitor.getNonResident() +
                    replicaCountVisitor.getNonResident(),
                    add_stat, cookie);
    add_casted_stat("ep_num_active_non_resident", activeCountVisitor.getNonResident(),
                    add_stat, cookie);

    add_casted_stat("ep_latency_get_cmd", epstats.getCmdHisto.total(),
                    add_stat, cookie);
    add_casted_stat("ep_latency_store_cmd", epstats.storeCmdHisto.total(),
                    add_stat, cookie);
    add_casted_stat("ep_latency_arith_cmd", epstats.arithCmdHisto.total(),
                    add_stat, cookie);

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doVBucketStats(const void *cookie,
                                                             ADD_STAT add_stat,
                                                             bool prevStateRequested) {
    class StatVBucketVisitor : public VBucketVisitor {
    public:
        StatVBucketVisitor(const void *c, ADD_STAT a, bool isPrevStateRequested) :
            cookie(c), add_stat(a), isPrevState(isPrevStateRequested) {}

        bool visitBucket(RCPtr<VBucket> vb) {
            char buf[16];
            snprintf(buf, sizeof(buf), "vb_%d", vb->getId());
            if (isPrevState) {
                add_casted_stat(buf, VBucket::toString(vb->getInitialState()),
                                add_stat, cookie);
            } else {
                add_casted_stat(buf, VBucket::toString(vb->getState()), add_stat, cookie);
            }
            return false;
        }

    private:
        const void *cookie;
        ADD_STAT add_stat;
        bool isPrevState;
    };

    StatVBucketVisitor svbv(cookie, add_stat, prevStateRequested);
    epstore->visit(svbv);
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doHashStats(const void *cookie,
                                                          ADD_STAT add_stat) {

    class StatVBucketVisitor : public VBucketVisitor {
    public:
        StatVBucketVisitor(const void *c, ADD_STAT a) : cookie(c), add_stat(a) {}

        bool visitBucket(RCPtr<VBucket> vb) {
            uint16_t vbid = vb->getId();
            char buf[32];
            snprintf(buf, sizeof(buf), "vb_%d:state", vbid);
            add_casted_stat(buf, VBucket::toString(vb->getState()), add_stat, cookie);

            HashTableDepthStatVisitor depthVisitor;
            vb->ht.visitDepth(depthVisitor);

            snprintf(buf, sizeof(buf), "vb_%d:size", vbid);
            add_casted_stat(buf, vb->ht.getSize(), add_stat, cookie);
            snprintf(buf, sizeof(buf), "vb_%d:locks", vbid);
            add_casted_stat(buf, vb->ht.getNumLocks(), add_stat, cookie);
            snprintf(buf, sizeof(buf), "vb_%d:min_depth", vbid);
            add_casted_stat(buf, depthVisitor.min == -1 ? 0 : depthVisitor.min,
                            add_stat, cookie);
            snprintf(buf, sizeof(buf), "vb_%d:max_depth", vbid);
            add_casted_stat(buf, depthVisitor.max, add_stat, cookie);
            snprintf(buf, sizeof(buf), "vb_%d:histo", vbid);
            add_casted_stat(buf, depthVisitor.depthHisto, add_stat, cookie);
            snprintf(buf, sizeof(buf), "vb_%d:reported", vbid);
            add_casted_stat(buf, vb->ht.getNumItems(), add_stat, cookie);
            snprintf(buf, sizeof(buf), "vb_%d:counted", vbid);
            add_casted_stat(buf, depthVisitor.size, add_stat, cookie);
            snprintf(buf, sizeof(buf), "vb_%d:resized", vbid);
            add_casted_stat(buf, vb->ht.getNumResizes(), add_stat, cookie);
            snprintf(buf, sizeof(buf), "vb_%d:mem_size", vbid);
            add_casted_stat(buf, vb->ht.memSize, add_stat, cookie);
            snprintf(buf, sizeof(buf), "vb_%d:mem_size_counted", vbid);
            add_casted_stat(buf, depthVisitor.memUsed, add_stat, cookie);

            return false;
        }

        const void *cookie;
        ADD_STAT add_stat;
    };

    StatVBucketVisitor svbv(cookie, add_stat);
    epstore->visit(svbv);

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doCheckpointStats(const void *cookie,
                                                                ADD_STAT add_stat) {

    class StatCheckpointVisitor : public VBucketVisitor {
    public:
        StatCheckpointVisitor(EventuallyPersistentStore * eps, const void *c,
                              ADD_STAT a) : epstore(eps), cookie(c), add_stat(a) {}

        bool visitBucket(RCPtr<VBucket> vb) {
            uint16_t vbid = vb->getId();
            char buf[32];
            snprintf(buf, sizeof(buf), "vb_%d:state", vbid);
            add_casted_stat(buf, VBucket::toString(vb->getState()), add_stat, cookie);

            snprintf(buf, sizeof(buf), "vb_%d:open_checkpoint_id", vbid);
            add_casted_stat(buf, vb->checkpointManager.getOpenCheckpointId(), add_stat, cookie);
            snprintf(buf, sizeof(buf), "vb_%d:last_closed_checkpoint_id", vbid);
            add_casted_stat(buf, vb->checkpointManager.getLastClosedCheckpointId(),
                            add_stat, cookie);
            snprintf(buf, sizeof(buf), "vb_%d:persisted_checkpoint_id", vbid);
            add_casted_stat(buf, epstore->getLastPersistedCheckpointId(vbid), add_stat, cookie);
            snprintf(buf, sizeof(buf), "vb_%d:num_tap_cursors", vbid);
            add_casted_stat(buf, vb->checkpointManager.getNumOfTAPCursors(), add_stat, cookie);
            snprintf(buf, sizeof(buf), "vb_%d:num_checkpoint_items", vbid);
            add_casted_stat(buf, vb->checkpointManager.getNumItems(), add_stat, cookie);
            snprintf(buf, sizeof(buf), "vb_%d:num_checkpoints", vbid);
            add_casted_stat(buf, vb->checkpointManager.getNumCheckpoints(), add_stat, cookie);
            return false;
        }

        EventuallyPersistentStore *epstore;
        const void *cookie;
        ADD_STAT add_stat;
    };

    StatCheckpointVisitor cv(epstore, cookie, add_stat);
    epstore->visit(cv);

    return ENGINE_SUCCESS;
}

/// @cond DETAILS

/**
 * Aggregator object to count all tap stats.
 */
struct TapCounter {
    TapCounter()
        : tap_queue(0), totalTaps(0),
          tap_queueFill(0), tap_queueDrain(0), tap_queueBackoff(0),
          tap_queueBackfillRemaining(0), tap_queueItemOnDisk(0)
    {}

    size_t      tap_queue;
    size_t      totalTaps;

    size_t      tap_queueFill;
    size_t      tap_queueDrain;
    size_t      tap_queueBackoff;
    size_t      tap_queueBackfillRemaining;
    size_t      tap_queueItemOnDisk;
};

/**
 * Function object to send stats for a single tap connection.
 */
struct TapStatBuilder {
    TapStatBuilder(const void *c, ADD_STAT as, TapCounter* tc)
        : cookie(c), add_stat(as), aggregator(tc) {}

    void operator() (TapConnection *tc) {
        ++aggregator->totalTaps;
        tc->addStats(add_stat, cookie);

        TapProducer *tp = dynamic_cast<TapProducer*>(tc);
        if (tp) {
            aggregator->tap_queue += tp->getQueueSize();
            aggregator->tap_queueFill += tp->getQueueFillTotal();
            aggregator->tap_queueDrain += tp->getQueueDrainTotal();
            aggregator->tap_queueBackoff += tp->getQueueBackoff();
            aggregator->tap_queueBackfillRemaining += tp->getBacklogSize();
            aggregator->tap_queueItemOnDisk += tp->getRemaingOnDisk();
        }
    }

    const void *cookie;
    ADD_STAT    add_stat;
    TapCounter* aggregator;
};

struct TapAggStatBuilder {
    TapAggStatBuilder(std::map<std::string, TapCounter*> *m,
                      const char *s, size_t sl)
        : counters(m), sep(s), sep_len(sl) {}

    TapCounter *getTarget(TapProducer *tc) {
        TapCounter *rv = NULL;

        if (tc) {
            const std::string name(tc->getName());
            size_t pos1 = name.find(':');
            assert(pos1 != name.npos);
            size_t pos2 = name.find(sep, pos1+1, sep_len);
            if (pos2 != name.npos) {
                std::string prefix(name.substr(pos1+1, pos2 - pos1 - 1));
                rv = (*counters)[prefix];
                if (rv == NULL) {
                    rv = new TapCounter;
                    (*counters)[prefix] = rv;
                }
            }
        }
        return rv;
    }

    void aggregate(TapProducer *tp, TapCounter *tc){
            ++tc->totalTaps;
            tc->tap_queue += tp->getQueueSize();
            tc->tap_queueFill += tp->getQueueFillTotal();
            tc->tap_queueDrain += tp->getQueueDrainTotal();
            tc->tap_queueBackoff += tp->getQueueBackoff();
            tc->tap_queueBackfillRemaining += tp->getBacklogSize();
            tc->tap_queueItemOnDisk += tp->getRemaingOnDisk();
    }

    TapCounter *getTotalCounter() {
        TapCounter *rv = NULL;
        std::string sepr(sep);
        std::string total(sepr + "total");
        rv = (*counters)[total];
        if(rv == NULL) {
            rv = new TapCounter;
            (*counters)[total] = rv;
        }
        return rv;
    }

    void operator() (TapConnection *tc) {

        TapProducer *tp = dynamic_cast<TapProducer*>(tc);
        TapCounter *aggregator = getTarget(tp);
        if (aggregator && tp) {
            aggregate(tp, aggregator);
        }
        if (tp) {
            aggregate(tp, getTotalCounter());
        }
    }

    std::map<std::string, TapCounter*> *counters;
    const char *sep;
    size_t sep_len;
};

/// @endcond

static void showTapAggStat(const std::string prefix,
                           TapCounter *counter,
                           const void *cookie,
                           ADD_STAT add_stat) {

    char statname[80] = {0};
    const size_t sl(sizeof(statname));
    snprintf(statname, sl, "%s:count", prefix.c_str());
    add_casted_stat(statname, counter->totalTaps, add_stat, cookie);

    snprintf(statname, sl, "%s:qlen", prefix.c_str());
    add_casted_stat(statname, counter->tap_queue, add_stat, cookie);

    snprintf(statname, sl, "%s:fill", prefix.c_str());
    add_casted_stat(statname, counter->tap_queueFill,
                    add_stat, cookie);

    snprintf(statname, sl, "%s:drain", prefix.c_str());
    add_casted_stat(statname, counter->tap_queueDrain,
                    add_stat, cookie);

    snprintf(statname, sl, "%s:backoff", prefix.c_str());
    add_casted_stat(statname, counter->tap_queueBackoff,
                    add_stat, cookie);

    snprintf(statname, sl, "%s:backfill_remaining", prefix.c_str());
    add_casted_stat(statname, counter->tap_queueBackfillRemaining,
                    add_stat, cookie);

    snprintf(statname, sl, "%s:itemondisk", prefix.c_str());
    add_casted_stat(statname, counter->tap_queueItemOnDisk,
                    add_stat, cookie);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doTapAggStats(const void *cookie,
                                                            ADD_STAT add_stat,
                                                            const char *sepPtr,
                                                            size_t sep_len) {
    // In practice, this will be 1, but C++ doesn't let me use dynamic
    // array sizes.
    const size_t max_sep_len(8);
    sep_len = std::min(sep_len, max_sep_len);

    char sep[max_sep_len + 1];
    memcpy(sep, sepPtr, sep_len);
    sep[sep_len] = 0x00;

    std::map<std::string, TapCounter*> counters;
    TapAggStatBuilder tapVisitor(&counters, sep, sep_len);
    tapConnMap.each(tapVisitor);

    std::map<std::string, TapCounter*>::iterator it;
    for (it = counters.begin(); it != counters.end(); ++it) {
        showTapAggStat(it->first, it->second, cookie, add_stat);
        delete it->second;
    }

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doTapStats(const void *cookie,
                                                         ADD_STAT add_stat) {
    TapCounter aggregator;
    TapStatBuilder tapVisitor(cookie, add_stat, &aggregator);
    tapConnMap.each(tapVisitor);

    add_casted_stat("ep_tap_total_fetched", stats.numTapFetched, add_stat, cookie);
    add_casted_stat("ep_tap_bg_max_pending", TapProducer::bgMaxPending,
                    add_stat, cookie);
    add_casted_stat("ep_tap_bg_fetched", stats.numTapBGFetched, add_stat, cookie);
    add_casted_stat("ep_tap_bg_fetch_requeued", stats.numTapBGFetchRequeued,
                    add_stat, cookie);
    add_casted_stat("ep_tap_fg_fetched", stats.numTapFGFetched, add_stat, cookie);
    add_casted_stat("ep_tap_deletes", stats.numTapDeletes, add_stat, cookie);
    add_casted_stat("ep_tap_throttled", stats.tapThrottled, add_stat, cookie);
    add_casted_stat("ep_tap_keepalive", tapKeepAlive, add_stat, cookie);
    add_casted_stat("ep_tap_noop_interval", tapNoopInterval, add_stat, cookie);

    add_casted_stat("ep_tap_count", aggregator.totalTaps, add_stat, cookie);
    add_casted_stat("ep_tap_total_queue", aggregator.tap_queue, add_stat, cookie);
    add_casted_stat("ep_tap_queue_fill", aggregator.tap_queueFill, add_stat, cookie);
    add_casted_stat("ep_tap_queue_drain", aggregator.tap_queueDrain, add_stat, cookie);
    add_casted_stat("ep_tap_queue_backoff", aggregator.tap_queueBackoff,
                    add_stat, cookie);
    add_casted_stat("ep_tap_queue_backfillremaining",
                    aggregator.tap_queueBackfillRemaining, add_stat, cookie);
    add_casted_stat("ep_tap_queue_itemondisk", aggregator.tap_queueItemOnDisk,
                    add_stat, cookie);

    add_casted_stat("ep_tap_ack_window_size", TapProducer::ackWindowSize,
                    add_stat, cookie);
    add_casted_stat("ep_tap_ack_interval", TapProducer::ackInterval,
                    add_stat, cookie);
    add_casted_stat("ep_tap_ack_grace_period",
                    TapProducer::ackGracePeriod,
                    add_stat, cookie);
    add_casted_stat("ep_tap_backoff_period",
                    TapProducer::backoffSleepTime,
                    add_stat, cookie);


    if (stats.tapBgNumOperations > 0) {
        add_casted_stat("ep_tap_bg_num_samples", stats.tapBgNumOperations,
                        add_stat, cookie);
        add_casted_stat("ep_tap_bg_min_wait",
                        stats.tapBgMinWait,
                        add_stat, cookie);
        add_casted_stat("ep_tap_bg_max_wait",
                        stats.tapBgMaxWait,
                        add_stat, cookie);
        add_casted_stat("ep_tap_bg_wait_avg",
                        stats.tapBgWait / stats.tapBgNumOperations,
                        add_stat, cookie);
        add_casted_stat("ep_tap_bg_min_load",
                        stats.tapBgMinLoad,
                        add_stat, cookie);
        add_casted_stat("ep_tap_bg_max_load",
                        stats.tapBgMaxLoad,
                        add_stat, cookie);
        add_casted_stat("ep_tap_bg_load_avg",
                        stats.tapBgLoad / stats.tapBgNumOperations,
                        add_stat, cookie);
    }

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
                             "Found lookup results for non-validating key "
                             "stat call. Would have leaked\n");
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
        add_casted_stat("key_last_modification_time", kstats.last_modification_time,
                        add_stat, cookie);
        if (validate) {
            add_casted_stat("key_valid", valid.c_str(), add_stat, cookie);
        }
        rv = ENGINE_SUCCESS;
    } else {
        rv = ENGINE_KEY_ENOENT;
    }

    return rv;
}


ENGINE_ERROR_CODE EventuallyPersistentEngine::doTimingStats(const void *cookie,
                                                            ADD_STAT add_stat) {
    add_casted_stat("bg_wait", stats.bgWaitHisto, add_stat, cookie);
    add_casted_stat("bg_load", stats.bgLoadHisto, add_stat, cookie);
    add_casted_stat("bg_tap_wait", stats.tapBgWaitHisto, add_stat, cookie);
    add_casted_stat("bg_tap_load", stats.tapBgLoadHisto, add_stat, cookie);
    add_casted_stat("pending_ops", stats.pendingOpsHisto, add_stat, cookie);

    add_casted_stat("storage_age", stats.dirtyAgeHisto, add_stat, cookie);
    add_casted_stat("data_age", stats.dataAgeHisto, add_stat, cookie);

    // Regular commands
    add_casted_stat("get_cmd", stats.getCmdHisto, add_stat, cookie);
    add_casted_stat("store_cmd", stats.storeCmdHisto, add_stat, cookie);
    add_casted_stat("arith_cmd", stats.arithCmdHisto, add_stat, cookie);
    // Admin commands
    add_casted_stat("get_vb_cmd", stats.getVbucketCmdHisto, add_stat, cookie);
    add_casted_stat("set_vb_cmd", stats.setVbucketCmdHisto, add_stat, cookie);
    add_casted_stat("del_vb_cmd", stats.delVbucketCmdHisto, add_stat, cookie);
    // Tap commands
    add_casted_stat("tap_vb_set", stats.tapVbucketSetHisto, add_stat, cookie);
    add_casted_stat("tap_vb_reset", stats.tapVbucketResetHisto, add_stat, cookie);
    add_casted_stat("tap_mutation", stats.tapMutationHisto, add_stat, cookie);
    // Misc
    add_casted_stat("notify_io", stats.notifyIOHisto, add_stat, cookie);

    // Disk stats
    add_casted_stat("disk_insert", stats.diskInsertHisto, add_stat, cookie);
    add_casted_stat("disk_update", stats.diskUpdateHisto, add_stat, cookie);
    add_casted_stat("disk_del", stats.diskDelHisto, add_stat, cookie);
    add_casted_stat("disk_vb_chunk_del", stats.diskVBChunkDelHisto, add_stat, cookie);
    add_casted_stat("disk_vb_del", stats.diskVBDelHisto, add_stat, cookie);
    add_casted_stat("disk_commit", stats.diskCommitHisto, add_stat, cookie);
    add_casted_stat("disk_invalid_item_del", stats.diskInvaidItemDelHisto,
                    add_stat, cookie);

    add_casted_stat("online_update_revert", stats.checkpointRevertHisto, add_stat, cookie);

    return ENGINE_SUCCESS;
}

static void showJobLog(const char *prefix, const char *logname,
                       const std::vector<JobLogEntry> log,
                       const void *cookie, ADD_STAT add_stat) {
    char statname[80] = {0};
    for (size_t i = 0; i < log.size(); ++i) {
        snprintf(statname, sizeof(statname), "%s:%s:%d:task",
                 prefix, logname, static_cast<int>(i));
        add_casted_stat(statname, log[i].getName().c_str(),
                        add_stat, cookie);
        snprintf(statname, sizeof(statname), "%s:%s:%d:starttime",
                 prefix, logname, static_cast<int>(i));
        add_casted_stat(statname, log[i].getTimestamp(),
                        add_stat, cookie);
        snprintf(statname, sizeof(statname), "%s:%s:%d:runtime",
                 prefix, logname, static_cast<int>(i));
        add_casted_stat(statname, log[i].getDuration(),
                        add_stat, cookie);
    }
}

static void doDispatcherStat(const char *prefix, const DispatcherState &ds,
                             const void *cookie, ADD_STAT add_stat) {
    char statname[80] = {0};
    snprintf(statname, sizeof(statname), "%s:state", prefix);
    add_casted_stat(statname, ds.getStateName(), add_stat, cookie);

    snprintf(statname, sizeof(statname), "%s:status", prefix);
    add_casted_stat(statname, ds.isRunningTask() ? "running" : "idle",
                    add_stat, cookie);

    if (ds.isRunningTask()) {
        snprintf(statname, sizeof(statname), "%s:task", prefix);
        add_casted_stat(statname, ds.getTaskName().c_str(),
                        add_stat, cookie);

        snprintf(statname, sizeof(statname), "%s:runtime", prefix);
        add_casted_stat(statname, (gethrtime() - ds.getTaskStart()) / 1000,
                        add_stat, cookie);
    }

    showJobLog(prefix, "log", ds.getLog(), cookie, add_stat);
    showJobLog(prefix, "slow", ds.getSlowLog(), cookie, add_stat);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doDispatcherStats(const void *cookie,
                                                                ADD_STAT add_stat) {
    DispatcherState ds(epstore->getDispatcher()->getDispatcherState());
    doDispatcherStat("dispatcher", ds, cookie, add_stat);

    if (epstore->hasSeparateRODispatcher()) {
        DispatcherState rods(epstore->getRODispatcher()->getDispatcherState());
        doDispatcherStat("ro_dispatcher", rods, cookie, add_stat);
    }

    DispatcherState nds(epstore->getNonIODispatcher()->getDispatcherState());
    doDispatcherStat("nio_dispatcher", nds, cookie, add_stat);

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::getStats(const void* cookie,
                                                       const char* stat_key,
                                                       int nkey,
                                                       ADD_STAT add_stat) {
    ENGINE_ERROR_CODE rv = ENGINE_KEY_ENOENT;
    if (stat_key == NULL) {
        rv = doEngineStats(cookie, add_stat);
    } else if (nkey > 7 && strncmp(stat_key, "tapagg ", 7) == 0) {
        rv = doTapAggStats(cookie, add_stat, stat_key + 7, nkey - 7);
    } else if (nkey == 3 && strncmp(stat_key, "tap", 3) == 0) {
        rv = doTapStats(cookie, add_stat);
    } else if (nkey == 4 && strncmp(stat_key, "hash", 3) == 0) {
        rv = doHashStats(cookie, add_stat);
    } else if (nkey == 7 && strncmp(stat_key, "vbucket", 7) == 0) {
        rv = doVBucketStats(cookie, add_stat);
    } else if (nkey == 12 && strncmp(stat_key, "prev-vbucket", 12) == 0) {
        rv = doVBucketStats(cookie, add_stat, true);
    } else if (nkey == 10 && strncmp(stat_key, "checkpoint", 10) == 0) {
        rv = doCheckpointStats(cookie, add_stat);
    } else if (nkey == 7 && strncmp(stat_key, "timings", 7) == 0) {
        rv = doTimingStats(cookie, add_stat);
    } else if (nkey == 10 && strncmp(stat_key, "dispatcher", 10) == 0) {
        rv = doDispatcherStats(cookie, add_stat);
    } else if (nkey == 7 && strncmp(stat_key, "restore", 7) == 0) {
        rv = ENGINE_SUCCESS;
        LockHolder lh(restore.mutex);
        if (restore.manager) {
            restore.manager->stats(cookie, add_stat);
        }
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
        uint16_t vbucket_id(0);
        parseUint16(vbid.c_str(), &vbucket_id);
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
        uint16_t vbucket_id(0);
        parseUint16(vbid.c_str(), &vbucket_id);
        // Validating version; blocks
        rv = doKeyStats(cookie, add_stat, vbucket_id, key, true);
    }

    return rv;
}

void EventuallyPersistentEngine::notifyTapIoThread(void) {
    // Fix clean shutdown!!!
    while (!shutdown) {
        tapConnMap.notifyIOThreadMain();

        if (shutdown) {
            return;
        }

        tapConnMap.wait(1.0);
    }
}

void EventuallyPersistentEngine::notifyTapNotificationThread(void) {
    tapConnMap.notify();
}

void EventuallyPersistentEngine::setTapValidity(const std::string &name, const void* token) {
    tapConnMap.setValidity(name, token);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::touch(const void *cookie,
                                                    protocol_binary_request_header *request,
                                                    ADD_RESPONSE response)
{
    if (request->request.extlen != 4 || request->request.keylen == 0) {
        if (response(NULL, 0, NULL, 0, NULL, 0, PROTOCOL_BINARY_RAW_BYTES,
                     PROTOCOL_BINARY_RESPONSE_EINVAL, 0, cookie)) {
            return ENGINE_SUCCESS;
        } else {
            return ENGINE_FAILED;
        }
    }

    protocol_binary_request_touch *t = reinterpret_cast<protocol_binary_request_touch*>(request);
    void *key = t->bytes + sizeof(t->bytes);
    uint32_t exptime = ntohl(t->message.body.expiration);
    uint16_t nkey = ntohs(request->request.keylen);
    uint16_t vbucket = ntohs(request->request.vbucket);

    // try to get the object
    std::string k(static_cast<const char*>(key), nkey);

    exptime = serverApi->core->abstime(serverApi->core->realtime(exptime));
    GetValue gv(epstore->getAndUpdateTtl(k, vbucket, cookie,
                                         request->request.opcode != PROTOCOL_BINARY_CMD_TOUCH,
                                         exptime));
    ENGINE_ERROR_CODE rv = gv.getStatus();
    if (rv == ENGINE_SUCCESS) {
        bool ret;
        Item *it = gv.getValue();
        if (request->request.opcode == PROTOCOL_BINARY_CMD_TOUCH) {
            ret = response(NULL, 0, NULL, 0, NULL, 0,
                           PROTOCOL_BINARY_RAW_BYTES,
                           PROTOCOL_BINARY_RESPONSE_SUCCESS, 0, cookie);
        } else {
            uint32_t flags = it->getFlags();
            ret = response(NULL, 0, &flags, sizeof(flags),
                           it->getData(), it->getNBytes(),
                           PROTOCOL_BINARY_RAW_BYTES,
                           PROTOCOL_BINARY_RESPONSE_SUCCESS, it->getCas(),
                           cookie);
        }
        delete it;
        if (ret) {
            rv = ENGINE_SUCCESS;
        } else {
            rv = ENGINE_FAILED;
        }
    } else if (rv == ENGINE_KEY_ENOENT) {
        if (request->request.opcode == PROTOCOL_BINARY_CMD_GATQ) {
            // GATQ should not return response upon cache miss
            rv = ENGINE_SUCCESS;
        } else {
            if (response(NULL, 0, NULL, 0, NULL, 0, PROTOCOL_BINARY_RAW_BYTES,
                         PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0, cookie)) {
                rv = ENGINE_SUCCESS;
            } else {
                rv = ENGINE_FAILED;
            }
        }
    } else if (rv == ENGINE_NOT_MY_VBUCKET) {
        if (response(NULL, 0, NULL, 0, NULL, 0, PROTOCOL_BINARY_RAW_BYTES,
                     PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET, 0, cookie)) {
            rv = ENGINE_SUCCESS;
        } else {
            rv = ENGINE_FAILED;
        }
    }

    return rv;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::onlineUpdate(const void *cookie,
                                                           protocol_binary_request_header *request,
                                                           ADD_RESPONSE response) {

    protocol_binary_response_status rv(PROTOCOL_BINARY_RESPONSE_SUCCESS);

    switch(request->request.opcode) {
    case CMD_ONLINE_UPDATE_START:
        {
            class StartOnlineUpdateVBucketVisitor : public VBucketVisitor {
                public:
                    StartOnlineUpdateVBucketVisitor(protocol_binary_response_status& r)
                        : rv(r) {}

                    bool visitBucket(RCPtr<VBucket> vb) {
                        if (!vb || vb->getState() == vbucket_state_dead) {
                            return false;
                        }
                        protocol_binary_response_status r =
                            vb->checkpointManager.startOnlineUpdate();
                        if (r != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
                            rv = r;
                        }
                        return false;
                    }
                private:
                    protocol_binary_response_status& rv;
             };

            StartOnlineUpdateVBucketVisitor visitor(rv);
            epstore->visit(visitor);
        }
        break;
    case CMD_ONLINE_UPDATE_COMPLETE:
        {
            class CompleteOnlineUpdateVBucketVisitor : public VBucketVisitor {
                public:
                    CompleteOnlineUpdateVBucketVisitor(protocol_binary_response_status& r)
                        : rv(r) {}

                    bool visitBucket(RCPtr<VBucket> vb) {
                        if (!vb || vb->getState() == vbucket_state_dead) {
                            return false;
                        }
                        protocol_binary_response_status r =
                            vb->checkpointManager.stopOnlineUpdate();
                        if (r != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
                            rv = r;
                        }
                        return false;
                    }
                private:
                    protocol_binary_response_status& rv;
             };

             CompleteOnlineUpdateVBucketVisitor visitor(rv);
             epstore->visit(visitor);
        }
        break;
    case CMD_ONLINE_UPDATE_REVERT:
        {
            class RevertOnlineUpdateVBucketVisitor : public VBucketVisitor {
                public:
                    RevertOnlineUpdateVBucketVisitor(EventuallyPersistentStore* e,
                                                     protocol_binary_response_status& r)
                        : epstore(e), rv(r) {}

                    bool visitBucket(RCPtr<VBucket> vb) {
                        protocol_binary_response_status r = epstore->revertOnlineUpdate(vb);
                        if (r != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
                            rv = r;
                        }
                        return false;
                    }
                private:
                    EventuallyPersistentStore* epstore;
                    protocol_binary_response_status& rv;
             };

             RevertOnlineUpdateVBucketVisitor visitor(epstore, rv);
             epstore->visit(visitor);
        }
        break;
    default:
        break;
    }

    if (response(NULL, 0, NULL, 0, NULL, 0, PROTOCOL_BINARY_RAW_BYTES,
                 rv, 0, cookie)) {
        return ENGINE_SUCCESS;
    } else {
        return ENGINE_FAILED;
    }
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::sync(std::set<key_spec_t> *keys,
                                                   const void *cookie,
                                                   sync_type_t syncType,
                                                   uint8_t replicas,
                                                   ADD_RESPONSE response) {

    SyncListener *syncListener = new SyncListener(*this, cookie,
                                                  keys, syncType, replicas);
    std::vector< std::pair<StoredValue*, uint16_t> > storedValues;
    std::set<key_spec_t>::iterator it = keys->begin();

    while (it != keys->end()) {
        StoredValue *sv = epstore->getStoredValue(it->key, it->vbucketid, false);

        if (sv == NULL) {
            syncListener->getNonExistentKeys().insert(*it);
            keys->erase(it++);
        } else {
            if ((it->cas != 0) && (sv->getCas() != it->cas)) {
                syncListener->getInvalidCasKeys().insert(*it);
                keys->erase(it++);
                continue;
            }

            std::pair<StoredValue*, uint16_t> pair(sv, it->vbucketid);
            storedValues.push_back(pair);
            ++it;
        }
    }

    if (keys->size() == 0) {
        std::stringstream resp;

        assembleSyncResponse(resp, syncListener, *epstore);
        delete syncListener;

        std::string body = resp.str();
        response(NULL, 0, NULL, 0,
                 body.c_str(), static_cast<uint16_t>(body.length()),
                 PROTOCOL_BINARY_RAW_BYTES,
                 PROTOCOL_BINARY_RESPONSE_SUCCESS, 0, cookie);

        return ENGINE_SUCCESS;
    }

    switch (syncType) {
    case PERSIST:
        syncRegistry.addPersistenceListener(syncListener);
        notifyListener(storedValues, syncListener);
        break;
    case MUTATION:
        syncRegistry.addMutationListener(syncListener);
        break;
    case REP:
        syncRegistry.addReplicationListener(syncListener);
        notifyListener(storedValues, syncListener);
        break;
    case REP_OR_PERSIST:
    case REP_AND_PERSIST:
        syncRegistry.addReplicationListener(syncListener);
        syncRegistry.addPersistenceListener(syncListener);
        notifyListener(storedValues, syncListener);
    }

    if (syncListener->maybeEnableNotifyIOComplete()) {
        // Not all keys are SYNCed. Allow the SyncRegistry to notifyIOComplete
        // this request and block for now.
        return ENGINE_EWOULDBLOCK;
    }

    std::stringstream resp;

    assembleSyncResponse(resp, syncListener, *epstore);
    delete syncListener;

    std::string body = resp.str();
    response(NULL, 0, NULL, 0,
             body.c_str(), static_cast<uint16_t>(body.length()),
             PROTOCOL_BINARY_RAW_BYTES,
             PROTOCOL_BINARY_RESPONSE_SUCCESS, 0, cookie);

    return ENGINE_SUCCESS;
}

static void notifyListener(std::vector< std::pair<StoredValue*, uint16_t> > &svList,
                           SyncListener *listener) {

    sync_type_t syncType = listener->getSyncType();
    std::vector< std::pair<StoredValue*, uint16_t> >::iterator it;

    for (it = svList.begin(); it != svList.end(); it++) {
        StoredValue *sv = it->first;
        key_spec_t keyspec(sv->getCas(), it->second, sv->getKey());
        uint8_t replicas = sv->getNumReplicas();

        if ((replicas > 0) &&
            (syncType == REP || syncType == REP_OR_PERSIST ||
             syncType == REP_AND_PERSIST)) {

            listener->keySynced(keyspec, replicas);
        }

        if (sv->isClean() &&
            (syncType == PERSIST || syncType == REP_OR_PERSIST ||
             syncType == REP_AND_PERSIST)) {

            listener->keySynced(keyspec);
        }
    }
}

static bool parseSyncOptions(uint32_t flags, sync_type_t *syncType, uint8_t *replicas) {
    *replicas = (uint8_t) ((flags & 0xf0) >> 4);
    bool syncRep = (*replicas > 0);
    bool syncPersist = ((flags & 0x8) == 0x8);
    bool syncMutation = ((flags & 0x4) == 0x4);

    if ((syncPersist && syncMutation) ||
        (syncRep && syncMutation)) {
        return false;
    }

    if (syncPersist && !syncRep) {
        *syncType = PERSIST;
    } else if (!syncPersist && syncRep) {
        *syncType = REP;
    } else if (syncPersist && syncRep) {
        if (flags & 0x2) {
            *syncType = REP_AND_PERSIST;
        } else {
            *syncType = REP_OR_PERSIST;
        }
    } else if (syncMutation) {
        *syncType = MUTATION;
    } else {
        // No flags set at all or only the reserved bits
        // are used (ignore them for now).
        return false;
    }

    return true;
}

static void assembleSyncResponse(std::stringstream &resp,
                                 SyncListener *syncListener,
                                 EventuallyPersistentStore &epstore) {
    uint16_t nkeys = syncListener->getInvalidCasKeys().size() +
                     syncListener->getNonExistentKeys().size();

    switch (syncListener->getSyncType()) {
    case PERSIST:
        nkeys += syncListener->getPersistedKeys().size();
        break;
    case MUTATION:
        nkeys += syncListener->getModifiedKeys().size();
        nkeys += syncListener->getDeletedKeys().size();
        break;
    case REP:
        nkeys += syncListener->getReplicatedKeys().size();
        break;
    case REP_AND_PERSIST:
    case REP_OR_PERSIST:
        nkeys += syncListener->getReplicatedKeys().size();
        nkeys += syncListener->getPersistedKeys().size();
    }

    nkeys = htons(nkeys);
    resp.write((char *) &nkeys, sizeof(uint16_t));

    addSyncKeySpecs(resp, syncListener->getNonExistentKeys(),
                    SYNC_INVALID_KEY, epstore, false);
    addSyncKeySpecs(resp, syncListener->getInvalidCasKeys(),
                    SYNC_INVALID_CAS, epstore, false);

    switch (syncListener->getSyncType()) {
    case PERSIST:
        addSyncKeySpecs(resp, syncListener->getPersistedKeys(),
                        SYNC_PERSISTED_EVENT, epstore);
        break;
    case MUTATION:
        addSyncKeySpecs(resp, syncListener->getModifiedKeys(),
                        SYNC_MODIFIED_EVENT, epstore);
        addSyncKeySpecs(resp, syncListener->getDeletedKeys(),
                        SYNC_DELETED_EVENT, epstore);
        break;
    case REP:
        addSyncKeySpecs(resp, syncListener->getReplicatedKeys(),
                        SYNC_REPLICATED_EVENT, epstore);
        break;
    case REP_OR_PERSIST:
    case REP_AND_PERSIST:
        addSyncKeySpecs(resp, syncListener->getReplicatedKeys(),
                        SYNC_REPLICATED_EVENT, epstore);
        addSyncKeySpecs(resp, syncListener->getPersistedKeys(),
                        SYNC_PERSISTED_EVENT, epstore);
    }
}


static void addSyncKeySpecs(std::stringstream &resp,
                            std::set<key_spec_t> &keyspecs,
                            uint8_t eventid,
                            EventuallyPersistentStore &epstore,
                            bool addCurrentCas) {
    std::set<key_spec_t>::iterator it = keyspecs.begin();

    for ( ; it != keyspecs.end(); it++) {
        uint64_t cas = it->cas;
        uint16_t vbid = htons(it->vbucketid);
        uint16_t keylen = htons(it->key.length());

        if (addCurrentCas) {
            StoredValue *sv = epstore.getStoredValue(it->key, it->vbucketid);

            if (sv != NULL) {
                cas = sv->getCas();
            }
        }

        cas = htonll(cas);
        resp.write((char *) &cas, sizeof(uint64_t));
        resp.write((char *) &vbid, sizeof(uint16_t));
        resp.write((char *) &keylen, sizeof(uint16_t));
        resp.write((char *) &eventid, sizeof(uint8_t));
        resp.write(it->key.c_str(), it->key.length());
    }
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::handleRestoreCmd(const void *cookie,
                                                               protocol_binary_request_header *request,
                                                               ADD_RESPONSE response)
{
    LockHolder lh(restore.mutex);
    if (restore.manager == NULL) { // we need another "mode" variable
        if (response(NULL, 0, NULL, 0, NULL, 0, PROTOCOL_BINARY_RAW_BYTES,
                     PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED, 0, cookie)) {
            return ENGINE_SUCCESS;
        }
        return ENGINE_FAILED;
    }

    if (request->request.opcode == CMD_RESTORE_FILE) {
        std::string filename((const char*)request->bytes + sizeof(request->bytes) + request->request.extlen, ntohs(request->request.keylen));
        try {
            restore.manager->initialize(filename);
        } catch (std::string e) {
            if (response(NULL, 0, NULL, 0, e.c_str(), e.length(),
                         PROTOCOL_BINARY_RAW_BYTES,
                         PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0, cookie)) {
                return ENGINE_SUCCESS;
            }
            return ENGINE_FAILED;
        }

        try {
            restore.manager->start();
        } catch (std::string e) {
            if (response(NULL, 0, NULL, 0, e.c_str(), e.length(),
                         PROTOCOL_BINARY_RAW_BYTES,
                         PROTOCOL_BINARY_RESPONSE_EINTERNAL, 0, cookie)) {
                return ENGINE_SUCCESS;
            }
            return ENGINE_FAILED;
        }
    } else if (request->request.opcode == CMD_RESTORE_ABORT) {
        try {
            restore.manager->abort();
        } catch (std::string e) {
            if (response(NULL, 0, NULL, 0, e.c_str(), e.length(),
                         PROTOCOL_BINARY_RAW_BYTES,
                         PROTOCOL_BINARY_RESPONSE_EINTERNAL, 0, cookie)) {
                return ENGINE_SUCCESS;
            }
            return ENGINE_FAILED;
        }
    } else {
        if (restore.manager->isRunning()) {
            if (response(NULL, 0, NULL, 0, NULL, 0,
                         PROTOCOL_BINARY_RAW_BYTES,
                         PROTOCOL_BINARY_RESPONSE_EBUSY, 0, cookie)) {
                return ENGINE_SUCCESS;
            }
            return ENGINE_FAILED;
        }

        destroy_restore_manager(restore.manager);
        restore.enabled.set(false);
        restore.manager = NULL;
    }

    if (response(NULL, 0, NULL, 0, NULL, 0,
                 PROTOCOL_BINARY_RAW_BYTES,
                 PROTOCOL_BINARY_RESPONSE_SUCCESS, 0, cookie)) {
        return ENGINE_SUCCESS;
    }
    return ENGINE_FAILED;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::deregisterTapClient(const void *cookie,
                                                        protocol_binary_request_header *request,
                                                        ADD_RESPONSE response)
{
    std::string tap_name = "eq_tapq:";
    std::string name((const char*)request->bytes + sizeof(request->bytes) +
                      request->request.extlen, ntohs(request->request.keylen));
    tap_name.append(name);

    // Close the tap connection for the registered TAP client and remove its checkpoint cursors.
    bool rv = tapConnMap.closeTapConnectionByName(tap_name);
    if (!rv) {
        // If the tap connection is not found, we still need to remove its checkpoint cursors.
        const VBucketMap &vbuckets = getEpStore()->getVBuckets();
        size_t numOfVBuckets = vbuckets.getSize();
        for (size_t i = 0; i < numOfVBuckets; ++i) {
            assert(i <= std::numeric_limits<uint16_t>::max());
            uint16_t vbid = static_cast<uint16_t>(i);
            RCPtr<VBucket> vb = vbuckets.getBucket(vbid);
            if (!vb) {
                continue;
            }
            vb->checkpointManager.removeTAPCursor(tap_name);
        }
    }

    if (response(NULL, 0, NULL, 0, NULL, 0,
                 PROTOCOL_BINARY_RAW_BYTES,
                 PROTOCOL_BINARY_RESPONSE_SUCCESS, 0, cookie)) {
        return ENGINE_SUCCESS;
    }
    return ENGINE_FAILED;
}

ENGINE_ERROR_CODE
EventuallyPersistentEngine::handleGetLastClosedCheckpointId(const void *cookie,
                                                            protocol_binary_request_header *req,
                                                            ADD_RESPONSE response) {
    uint16_t vbucket = ntohs(req->request.vbucket);
    RCPtr<VBucket> vb = getVBucket(vbucket);
    if (!vb) {
        if (response(NULL, 0, NULL, 0, NULL, 0,
                     PROTOCOL_BINARY_RAW_BYTES,
                     PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET, 0, cookie)) {
            return ENGINE_SUCCESS;
        }
        return ENGINE_FAILED;
    }

    uint64_t checkpointId = vb->checkpointManager.getLastClosedCheckpointId();
    checkpointId = htonll(checkpointId);
    if (response(NULL, 0, NULL, 0, &checkpointId, sizeof(checkpointId),
                 PROTOCOL_BINARY_RAW_BYTES, PROTOCOL_BINARY_RESPONSE_SUCCESS, 0, cookie)) {
        return ENGINE_SUCCESS;
    }
    return ENGINE_FAILED;
}
