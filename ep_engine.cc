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
#include <iostream>
#include <fstream>
#include <cstdio>
#include <cstring>
#include <assert.h>
#include <fcntl.h>

#include <memcached/engine.h>
#include <memcached/protocol_binary.h>
#include "ep_engine.h"
#include "tapthrottle.hh"
#include "htresizer.hh"
#include "backfill.hh"

static size_t percentOf(size_t val, double percent) {
    return static_cast<size_t>(static_cast<double>(val) * percent);
}

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
                                             item **itm,
                                             const void* key,
                                             const size_t nkey,
                                             const size_t nbytes,
                                             const int flags,
                                             const rel_time_t exptime)
    {
        return getHandle(handle)->itemAllocate(cookie, itm, key,
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
                               item* itm)
    {
        getHandle(handle)->itemRelease(cookie, itm);
    }

    static ENGINE_ERROR_CODE EvpGet(ENGINE_HANDLE* handle,
                                    const void* cookie,
                                    item** itm,
                                    const void* key,
                                    const int nkey,
                                    uint16_t vbucket)
    {
        return getHandle(handle)->get(cookie, itm, key, nkey, vbucket);
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
                                      item* itm,
                                      uint64_t *cas,
                                      ENGINE_STORE_OPERATION operation,
                                      uint16_t vbucket)
    {
        return getHandle(handle)->store(cookie, itm, cas, operation, vbucket);
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
        getHandle(handle)->resetStats();
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
                e->getConfiguration().setMinDataAge(v);
            } else if (strcmp(keyz, "queue_age_cap") == 0) {
                e->getConfiguration().setQueueAgeCap(v);
            } else if (strcmp(keyz, "max_txn_size") == 0) {
                e->getConfiguration().setMaxTxnSize(v);
            } else if (strcmp(keyz, "couch_vbucket_batch_count") == 0) {
                e->getConfiguration().setCouchVbucketBatchCount(v);
            } else if (strcmp(keyz, "bg_fetch_delay") == 0) {
                e->getConfiguration().setBgFetchDelay(v);
            } else if (strcmp(keyz, "chk_max_items") == 0) {
                validate(v, MIN_CHECKPOINT_ITEMS, MAX_CHECKPOINT_ITEMS);
                e->getConfiguration().setChkMaxItems(v);
            } else if (strcmp(keyz, "chk_period") == 0) {
                validate(v, MIN_CHECKPOINT_PERIOD, MAX_CHECKPOINT_PERIOD);
                e->getConfiguration().setChkPeriod(v);
            } else if (strcmp(keyz, "max_checkpoints") == 0) {
                validate(v, DEFAULT_MAX_CHECKPOINTS, MAX_CHECKPOINTS_UPPER_BOUND);
                e->getConfiguration().setMaxCheckpoints(v);
            } else if (strcmp(keyz, "item_num_based_new_chk") == 0) {
                if (strcmp(valz, "true") == 0) {
                    e->getConfiguration().setItemNumBasedNewChk(true);
                } else {
                    e->getConfiguration().setItemNumBasedNewChk(false);
                }
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
            } else if (strcmp(keyz, "timing_log") == 0) {
                EPStats &stats = e->getEpStats();
                std::ostream *old = stats.timingLog;
                stats.timingLog = NULL;
                delete old;
                if (strcmp(valz, "off") == 0) {
                    getLogger()->log(EXTENSION_LOG_INFO, NULL,
                                     "Disabled timing log.");
                } else {
                    std::ofstream *tmp(new std::ofstream(valz));
                    if (tmp->good()) {
                        getLogger()->log(EXTENSION_LOG_INFO, NULL,
                                         "Logging detailed timings to ``%s''.", valz);
                        stats.timingLog = tmp;
                    } else {
                        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                         "Error setting detailed timing log to ``%s'':  %s",
                                         valz, strerror(errno));
                        delete tmp;
                    }
                }
            } else if (strcmp(keyz, "exp_pager_stime") == 0) {
                char *ptr = NULL;
                // TODO:  This parser isn't perfect.
                uint64_t vsize = strtoull(valz, &ptr, 10);
                validate(vsize, static_cast<uint64_t>(0),
                         std::numeric_limits<uint64_t>::max());
                e->getConfiguration().setExpPagerStime((size_t)vsize);
            } else if (strcmp(keyz, "tap_throttle_threshold") == 0) {
                e->getConfiguration().setTapThrottleThreshold(v);
            } else if (strcmp(keyz, "tap_throttle_queue_cap") == 0) {
                e->getConfiguration().setTapThrottleQueueCap(v);
            } else if (strcmp(keyz, "inconsistent_slave_chk") == 0) {
                if (strcmp(valz, "true") == 0) {
                    e->getConfiguration().setInconsistentSlaveChk(true);
                } else {
                    e->getConfiguration().setInconsistentSlaveChk(false);
                }
            } else if (strcmp(keyz, "keep_closed_chks") == 0) {
                if (strcmp(valz, "true") == 0) {
                    e->getConfiguration().setKeepClosedChks(true);
                } else {
                    e->getConfiguration().setKeepClosedChks(false);
                }
            } else if (strcmp(keyz, "chk_meta_items_only") == 0) {
                bool chk_meta_items_only = true;
                if (strcmp(valz, "false") == 0) {
                    chk_meta_items_only = false;
                }
                e->getConfiguration().setChkMetaItemsOnly(chk_meta_items_only);
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
                                protocol_binary_request_header *req,
                                const void *cookie,
                                Item **itm,
                                const char **msg,
                                size_t *,
                                protocol_binary_response_status *res) {

        uint8_t extlen = req->request.extlen;
        if (extlen != 0 && extlen != 4) {
            *msg = "Invalid packet format (extlen may be 0 or 4)";
            *res = PROTOCOL_BINARY_RESPONSE_EINVAL;
            return ENGINE_EINVAL;
        }

        protocol_binary_request_getl *grequest =
            (protocol_binary_request_getl*)req;
        *res = PROTOCOL_BINARY_RESPONSE_SUCCESS;

        const char *keyp = reinterpret_cast<const char*>(req->bytes);
        keyp += sizeof(req->bytes) + extlen;
        std::string key(keyp, ntohs(req->request.keylen));
        uint16_t vbucket = ntohs(req->request.vbucket);

        RememberingCallback<GetValue> getCb;
        uint32_t max_timeout = (unsigned int)e->getGetlMaxTimeout();
        uint32_t default_timeout = (unsigned int)e->getGetlDefaultTimeout();
        uint32_t lockTimeout = default_timeout;
        if (extlen == 4) {
            lockTimeout = ntohl(grequest->message.body.expiration);
        }

        if (lockTimeout >  max_timeout || lockTimeout < 1) {
            getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                             "Illegal value for lock timeout specified %u."
                             " Using default value: %u\n",
                             lockTimeout, default_timeout);
            lockTimeout = default_timeout;
        }

        bool gotLock = e->getLocked(key, vbucket, getCb,
                                    ep_current_time(),
                                    lockTimeout, cookie);

        getCb.waitForValue();
        ENGINE_ERROR_CODE rv = getCb.val.getStatus();

        if (rv == ENGINE_SUCCESS) {
            *itm = getCb.val.getValue();

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

    static ENGINE_ERROR_CODE observeCmd(EventuallyPersistentEngine *e,
                                        protocol_binary_request_header *request,
                                        const void *cookie,
                                        ADD_RESPONSE response) {
        protocol_binary_request_observe *req =
            reinterpret_cast<protocol_binary_request_observe*>(request);
        assert(req);

        uint16_t keylen = ntohs(req->message.header.request.keylen);
        uint8_t extlen = req->message.header.request.extlen;
        uint16_t vbucket = ntohs(req->message.header.request.vbucket);
        uint32_t bodylen = ntohl(req->message.header.request.bodylen);
        uint64_t cas = ntohll(req->message.header.request.cas);
        uint32_t exp = ntohl(req->message.body.expiration);

        // Return invalid if no key or observe set is specified
        if (keylen == 0 || (bodylen - keylen - extlen) == 0) {
            response(NULL, 0, NULL, 0, "", 0, PROTOCOL_BINARY_RAW_BYTES,
                     PROTOCOL_BINARY_RESPONSE_EINVAL, 0, cookie);
            return ENGINE_FAILED;
        }

        const char *keyp = reinterpret_cast<const char*>(req->bytes);
        keyp += sizeof(request->bytes) + extlen;
        std::string key(keyp, keylen);

        const char *obs_set_pos = reinterpret_cast<const char*>(req->bytes);
        obs_set_pos += sizeof(request->bytes) + extlen + keylen;
        std::string obs_set(obs_set_pos, (bodylen - extlen - keylen));

        return e->observe(cookie, key, cas, vbucket, obs_set, exp, response);
    }

    static ENGINE_ERROR_CODE unobserveCmd(EventuallyPersistentEngine *e,
                                          protocol_binary_request_header *request,
                                          const void *cookie,
                                          ADD_RESPONSE response) {
        uint16_t keylen = ntohs(request->request.keylen);
        uint16_t vbucket = ntohs(request->request.vbucket);
        uint32_t bodylen = ntohl(request->request.bodylen);
        uint64_t cas = ntohll(request->request.cas);

        // Return invalid if no key or observe set is specified
        if (keylen == 0 || (bodylen - keylen) == 0) {
            response(NULL, 0, NULL, 0, "", 0, PROTOCOL_BINARY_RAW_BYTES,
                     PROTOCOL_BINARY_RESPONSE_EINVAL, 0, cookie);
            return ENGINE_FAILED;
        }

        const char *keyp = reinterpret_cast<const char*>(request->bytes);
        keyp += sizeof(request->bytes);
        std::string key(keyp, keylen);

        const char *obs_set_pos = reinterpret_cast<const char*>(request->bytes);
        obs_set_pos += sizeof(request->bytes) + keylen;
        std::string obs_set(obs_set_pos, (bodylen - keylen));

        return e->unobserve(cookie, key, cas, vbucket, obs_set, response);
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
        Item *itm = NULL;

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
            rv = getLocked(h, request, cookie, &itm, &msg, &msg_size, &res);
            if (rv == ENGINE_EWOULDBLOCK) {
                // we dont have the value for the item yet
                return rv;
            }
            break;
        case CMD_UNLOCK_KEY:
            res = unlockKey(h, request, &msg, &msg_size);
            break;
        case CMD_OBSERVE:
            return observeCmd(h, request, cookie, response);
            break;
        case CMD_UNOBSERVE:
            return unobserveCmd(h, request, cookie, response);
            break;
        case CMD_DEREGISTER_TAP_CLIENT:
            return h->deregisterTapClient(cookie, request, response);
            break;
        case CMD_LAST_CLOSED_CHECKPOINT:
            return h->handleGetLastClosedCheckpointId(cookie, request, response);
        case CMD_RESET_REPLICATION_CHAIN:
            return h->resetReplicationChain(cookie, request, response);

        case CMD_GET_META:
        case CMD_GETQ_META:
            return h->getMeta(cookie,
                              reinterpret_cast<protocol_binary_request_get_meta*>(request),
                              response);
        case CMD_SET_WITH_META:
        case CMD_SETQ_WITH_META:
        case CMD_ADD_WITH_META:
        case CMD_ADDQ_WITH_META:
            return h->setWithMeta(cookie,
                                  reinterpret_cast<protocol_binary_request_set_with_meta*>(request),
                                  response);
        case CMD_DEL_WITH_META:
        case CMD_DELQ_WITH_META:
            return h->deleteWithMeta(cookie,
                                     reinterpret_cast<protocol_binary_request_delete_with_meta*>(request),
                                     response);
        }

        // Send a special response for getl since we don't want to send the key
        if (itm && request->request.opcode == CMD_GET_LOCKED) {
            uint32_t flags = itm->getFlags();

            response(NULL, 0, (const void *)&flags, sizeof(uint32_t),
                    static_cast<const void *>(itm->getData()),
                    itm->getNBytes(),
                    PROTOCOL_BINARY_RAW_BYTES,
                    static_cast<uint16_t>(res), itm->getCas(),
                    cookie);
        } else if (itm) {
            std::string key  = itm->getKey();
            uint32_t flags = itm->getFlags();

            response(static_cast<const void *>(key.data()),
                    itm->getNKey(),
                    (const void *)&flags, sizeof(uint32_t),
                    static_cast<const void *>(itm->getData()),
                    itm->getNBytes(),
                    PROTOCOL_BINARY_RAW_BYTES,
                    static_cast<uint16_t>(res), itm->getCas(),
                    cookie);
            delete itm;
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
                              item *itm, uint64_t cas) {
        static_cast<Item*>(itm)->setCas(cas);
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
        getHandle(static_cast<ENGINE_HANDLE*>(c))->handleDisconnect(cookie);
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

    void *EvpNotifyPendingConns(void*arg) {
        ObjectRegistry::onSwitchThread(static_cast<EventuallyPersistentEngine*>(arg));
        static_cast<EventuallyPersistentEngine*>(arg)->notifyPendingConnections();
        return NULL;
    }

    static bool EvpGetItemInfo(ENGINE_HANDLE *, const void *,
                               const item* itm, item_info *itm_info)
    {
        const Item *it = reinterpret_cast<const Item*>(itm);
        if (itm_info->nvalue < 1) {
            return false;
        }
        itm_info->cas = it->getCas();
        itm_info->exptime = it->getExptime();
        itm_info->nbytes = it->getNBytes();
        itm_info->flags = it->getFlags();
        itm_info->clsid = 0;
        itm_info->nkey = static_cast<uint16_t>(it->getNKey());
        itm_info->nvalue = 1;
        itm_info->key = it->getKey().c_str();
        itm_info->value[0].iov_base = const_cast<char*>(it->getData());
        itm_info->value[0].iov_len = it->getNBytes();
        return true;
    }
} // C linkage

/**
 * Call the response callback and return the appropriate value so that
 * the core knows what to do..
 */
static ENGINE_ERROR_CODE sendResponse(ADD_RESPONSE response, const void *key,
                                      uint16_t keylen,
                                      const void *ext, uint8_t extlen,
                                      const void *body, uint32_t bodylen,
                                      uint8_t datatype, uint16_t status,
                                      uint64_t cas, const void *cookie)
{
    if (response(key, keylen, ext, extlen, body, bodylen, datatype,
                 status, cas, cookie)) {
        return ENGINE_SUCCESS;
    } else {
        return ENGINE_FAILED;
    }
}

static SERVER_EXTENSION_API *extensionApi;

EXTENSION_LOGGER_DESCRIPTOR *getLogger(void) {
    if (extensionApi != NULL) {
        return (EXTENSION_LOGGER_DESCRIPTOR*)extensionApi->get_extension(EXTENSION_LOGGER);
    }

    return NULL;
}

EventuallyPersistentEngine::EventuallyPersistentEngine(GET_SERVER_API get_server_api) :
    forceShutdown(false), kvstore(NULL),
    epstore(NULL), tapThrottle(new TapThrottle(stats)), databaseInitTime(0),
    startedEngineThreads(false), shutdown(false),
    getServerApiFunc(get_server_api), getlExtension(NULL),
    tapConnMap(NULL), tapConfig(NULL), checkpointConfig(NULL),
    memLowWat(std::numeric_limits<size_t>::max()),
    memHighWat(std::numeric_limits<size_t>::max()),
    mutation_count(0), observeRegistry(&epstore, &stats), warmingUp(true)
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
}

class EpEngineValueChangeListener : public ValueChangedListener {
public:
    EpEngineValueChangeListener(EventuallyPersistentEngine &e) : engine(e) {
        // EMPTY
    }

    virtual void sizeValueChanged(const std::string &key, size_t value) {
        if (key.compare("getl_max_timeout") == 0) {
            engine.setGetlMaxTimeout(value);
        } else if (key.compare("getl_default_timeout") == 0) {
            engine.setGetlDefaultTimeout(value);
        } else if (key.compare("max_item_size") == 0) {
            engine.setMaxItemSize(value);
        }
    }

private:
    EventuallyPersistentEngine &engine;
};



ENGINE_ERROR_CODE EventuallyPersistentEngine::initialize(const char* config) {
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    resetStats();
    if (config != NULL) {
        if (!configuration.parseConfiguration(config, serverApi)) {
            return ENGINE_FAILED;
        }
    }

    // Start updating the variables from the config!
    HashTable::setDefaultNumBuckets(configuration.getHtSize());
    HashTable::setDefaultNumLocks(configuration.getHtLocks());
    StoredValue::setMaxDataSize(stats, configuration.getMaxSize());
    StoredValue::setMutationMemoryThreshold(configuration.getMutationMemThreshold());
    std::string storedValType = configuration.getStoredValType();
    if (storedValType.length() > 0) {
        if (!HashTable::setDefaultStorageValueType(storedValType.c_str())) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                         "Unhandled storage value type: %s",
                         configuration.getStoredValType().c_str());
        }
    }

    maxItemSize = configuration.getMaxItemSize();
    configuration.addValueChangedListener("max_item_size",
                                          new EpEngineValueChangeListener(*this));

    memLowWat = configuration.getMemLowWat();
    memHighWat = configuration.getMemHighWat();

    if (configuration.isRestoreMode()) {
        if ((restore.manager = create_restore_manager(*this)) == NULL) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                    "Failed to create restore manager");
            return ENGINE_FAILED;
        }
        restore.enabled.set(true);
    }

    getlDefaultTimeout = configuration.getGetlDefaultTimeout();
    configuration.addValueChangedListener("getl_default_timeout",
                                          new EpEngineValueChangeListener(*this));
    getlMaxTimeout = configuration.getGetlMaxTimeout();
    configuration.addValueChangedListener("getl_max_timeout",
                                          new EpEngineValueChangeListener(*this));

    tapConnMap = new TapConnMap(*this);
    tapConfig = new TapConfig(*this);
    TapConfig::addConfigChangeListener(*this);

    checkpointConfig = new CheckpointConfig(*this);
    CheckpointConfig::addConfigChangeListener(*this);

    if (ret == ENGINE_SUCCESS) {
        time_t start = ep_real_time();
        try {
            if ((kvstore = KVStoreFactory::create(*this)) == NULL) {
                return ENGINE_FAILED;
            }
        } catch (std::exception& e) {
            std::stringstream ss;
            ss << "Failed to create database: " << e.what() << std::endl;
            if (!dbAccess()) {
                ss << "No access to \"" << configuration.getDbname() << "\"."
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
        epstore = new EventuallyPersistentStore(*this, kvstore, configuration.isVb0(),
                                                configuration.isConcurrentDB());
        if (epstore == NULL) {
            ret = ENGINE_ENOMEM;
            return ret;
        }

        // Register the callback
        SERVER_CALLBACK_API *sapi;
        sapi = getServerApi()->callback;
        sapi->register_callback(reinterpret_cast<ENGINE_HANDLE*>(this),
                                ON_DISCONNECT, EvpHandleDisconnect, this);
        startEngineThreads();

        // Complete the initialization of the ep-store
        epstore->initialize();
    }

    if (ret == ENGINE_SUCCESS) {
        getlExtension = new GetlExtension(epstore, getServerApiFunc);
        getlExtension->initialize();
    }

    getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "Engine init complete.\n");

    return ret;
}

KVStore* EventuallyPersistentEngine::newKVStore() {
    // @todo nuke me!
    return KVStoreFactory::create(*this);
}

void EventuallyPersistentEngine::destroy(bool force) {
    forceShutdown = force;
    stopEngineThreads();
    tapConnMap->shutdownAllTapConnections();
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
    shared_ptr<AllFlusher> cb(new AllFlusher(epstore, *tapConnMap));
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
    ENGINE_ERROR_CODE ret;
    Item *it = static_cast<Item*>(itm);
    item *i = NULL;

    it->setVBucketId(vbucket);
    it->fixupJSON();

    switch (operation) {
    case OPERATION_CAS:
        if (it->getCas() == 0) {
            // Using a cas command with a cas wildcard doesn't make sense
            ret = ENGINE_NOT_STORED;
            break;
        }
        // FALLTHROUGH
    case OPERATION_SET:
        if (isDegradedMode()) {
            // We're allowed to run set in restore mode..
            if (!restore.enabled.get()) {
                return ENGINE_TMPFAIL;
            }
        }
        ret = epstore->set(*it, cookie);
        if (ret == ENGINE_SUCCESS) {
            *cas = it->getCas();
            addMutationEvent(it);
        }

        break;

    case OPERATION_ADD:
        // you can't call add while the server is running in restore mode..
        if (isDegradedMode()) {
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
                    itemRelease(cookie, i);
                    return ENGINE_TMPFAIL;
                }

                if (it->getCas() != 0 && old->getCas() != it->getCas()) {
                    itemRelease(cookie, i);
                    return ENGINE_KEY_EEXISTS;
                }

                if ((old->getValue()->length() + it->getValue()->length()) > maxItemSize) {
                    itemRelease(cookie, i);
                    return ENGINE_E2BIG;
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

    VBucketFilter backFillVBFilter;
   if (connection->runBackfill(backFillVBFilter)) {
        queueBackfill(backFillVBFilter, connection, cookie);
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
            connection->opaqueCommandCode = (uint32_t) ev.state;
            *vbucket = ev.vbucket;
            *es = &connection->opaqueCommandCode;
            *nes = sizeof(connection->opaqueCommandCode);
            break;
        default:
            abort();
        }
        return ev.event;
    }

    Item *it = connection->getNextItem(cookie, vbucket, ret);
    switch (ret) {
    case TAP_CHECKPOINT_START:
    case TAP_CHECKPOINT_END:
    case TAP_MUTATION:
    case TAP_DELETION:
        *itm = it;
        if (ret == TAP_MUTATION || ret == TAP_DELETION) {
            connection->itemRevSeqno = htonl(it->getSeqno());
            *es = &connection->itemRevSeqno;
            *nes = sizeof(connection->itemRevSeqno);
        }
        break;
    case TAP_NOOP:
        retry = true;
        break;
    default:
        break;
    }

    if (ret == TAP_PAUSE && (connection->dumpQueue || connection->doTakeOver)) {
        TapVBucketEvent vbev = connection->checkDumpOrTakeOverCompletion();
        if (vbev.event == TAP_VBUCKET_SET) {
            connection->encodeVBucketStateTransition(vbev, es, nes, vbucket);
        }
        ret = vbev.event;
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
        connection->lastMsgTime = ep_current_time();
        if (ret == TAP_NOOP) {
            *seqno = 0;
        } else {
            ++stats.numTapFetched;
            *seqno = connection->getSeqno();
            if (connection->requestAck(ret, *vbucket)) {
                *flags = TAP_FLAG_ACK;
                connection->seqnoAckRequested = *seqno;
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
        if (nuserdata < sizeof(backfillAge)) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Backfill age is missing. Reject connection request from %s\n",
                             name.c_str());
            return false;
        }
        // use memcpy to avoid alignemt issues
        memcpy(&backfillAge, ptr, sizeof(backfillAge));
        backfillAge = ntohll(backfillAge);
        nuserdata -= sizeof(backfillAge);
        ptr += sizeof(backfillAge);
    }

    if (flags & TAP_CONNECT_FLAG_LIST_VBUCKETS) {
        uint16_t nvbuckets;
        if (nuserdata < sizeof(nvbuckets)) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Number of vbuckets is missing. Reject connection request from %s\n",
                             name.c_str());
            return false;
        }
        memcpy(&nvbuckets, ptr, sizeof(nvbuckets));
        nuserdata -= sizeof(nvbuckets);
        ptr += sizeof(nvbuckets);
        nvbuckets = ntohs(nvbuckets);
        if (nvbuckets > 0) {
            if (nuserdata < (sizeof(uint16_t) * nvbuckets)) {
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "# of vbuckets not matched. Reject connection request from %s\n",
                                 name.c_str());
                return false;
            }
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
            if (nuserdata < ((sizeof(uint16_t) + sizeof(uint64_t)) * nCheckpoints)) {
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "# of checkpoint Ids not matched. Reject connection request from %s\n",
                             name.c_str());
                return false;
            }
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

    TapProducer *tp = dynamic_cast<TapProducer*>(tapConnMap->findByName(name));
    if (tp && tp->isConnected() && !tp->doDisconnect() && isRegisteredClient) {
        return false;
    }

    TapProducer *tap = tapConnMap->newProducer(cookie, name, flags,
                                              backfillAge,
                                              static_cast<int>(configuration.getTapKeepalive()));

    tap->setRegisteredClient(isRegisteredClient);
    tap->setClosedCheckpointOnlyFlag(isClosedCheckpointOnly);
    tap->setVBucketFilter(vbuckets);
    tap->registerTAPCursor(lastCheckpointIds);
    serverApi->cookie->store_engine_specific(cookie, tap);
    tapConnMap->notify();
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
                                                        uint64_t cas,
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
            connection = tapConnMap->newConsumer(cookie);
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
            if (ret == ENGINE_SUCCESS) {
                addDeleteEvent(k, vbucket, 0);
            }
            if (ret == ENGINE_KEY_ENOENT) {
                ret = ENGINE_SUCCESS;
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
            TapConsumer *tc = dynamic_cast<TapConsumer*>(connection);
            RCPtr<Blob> vblob(Blob::New(static_cast<const char*>(data), ndata));
            Item *itm = new Item(k, flags, exptime, vblob);
            itm->setVBucketId(vbucket);

            if (tc) {
                bool meta = false;
                if (nengine == sizeof(uint32_t)) {
                    uint32_t seqnum;
                    memcpy(&seqnum, engine_specific, sizeof(seqnum));
                    seqnum = ntohl(seqnum);
                    itm->setCas(cas);
                    itm->setSeqno(seqnum);
                    meta = true;
                }

                if (tc->isBackfillPhase(vbucket)) {
                    ret = epstore->addTAPBackfillItem(*itm, meta);
                } else {
                    if (meta) {
                        ret = epstore->setWithMeta(*itm, 0, cookie, true, true);
                    } else {
                        ret = epstore->set(*itm, cookie, true);
                    }
                }
            } else {
                ret = ENGINE_DISCONNECT;
            }

            if (ret == ENGINE_SUCCESS) {
                addMutationEvent(itm);
            } else if (ret == ENGINE_ENOMEM) {
                if (connection->supportsAck()) {
                    ret = ENGINE_TMPFAIL;
                } else {
                    getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                     "Connection does not support tap ack'ing.. disconnect it\n");
                    ret = ENGINE_DISCONNECT;
                }
            }

            delete itm;
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
                // @todo: the memcached core will _ALWAYS_ send nack
                //        if it encounter an error. This should be
                // set as the default when we move to .next after 2.0
                // (currently we need to allow the message for
                // backwards compatibility)
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
                    TapConsumer *tc = dynamic_cast<TapConsumer*>(connection);
                    if (tc) {
                        tc->setBackfillPhase(true, vbucket);
                    } else {
                        ret = ENGINE_DISCONNECT;
                        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                         "TAP consumer doesn't exists. Force disconnect\n");
                    }
                }
                break;
            case TAP_OPAQUE_CLOSE_BACKFILL:
                {
                    TapConsumer *tc = dynamic_cast<TapConsumer*>(connection);
                    if (tc) {
                        tc->setBackfillPhase(false, vbucket);
                    } else {
                        ret = ENGINE_DISCONNECT;
                        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                         "TAP consumer doesn't exists. Force disconnect\n");
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
    if (pthread_create(&notifyThreadId, NULL, EvpNotifyPendingConns, this) != 0) {
        throw std::runtime_error("Error creating thread to notify pending connections");
    }
    startedEngineThreads = true;
}

void EventuallyPersistentEngine::queueBackfill(const VBucketFilter &backfillVBFilter,
                                               TapProducer *tc, const void *tok) {
    shared_ptr<DispatcherCallback> backfill_cb(new BackfillTask(this, tc, epstore,
                                                                tok, backfillVBFilter));
    epstore->getNonIODispatcher()->schedule(backfill_cb, NULL,
                                            Priority::BackfillTaskPriority,
                                            0, false, false);
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

bool VBucketCountVisitor::visitBucket(RCPtr<VBucket> &vb) {
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
    bool visitBucket(RCPtr<VBucket> &vb)  {
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

    configuration.addStats(add_stat, cookie);

    EPStats &epstats = getEpStats();
    add_casted_stat("ep_version", VERSION, add_stat, cookie);
    add_casted_stat("ep_storage_age",
                    epstats.dirtyAge, add_stat, cookie);
    add_casted_stat("ep_storage_age_highwat",
                    epstats.dirtyAgeHighWat, add_stat, cookie);
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
    add_casted_stat("ep_flusher_deduplication",
                    epstats.flusherDedup, add_stat, cookie);
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
    add_casted_stat("ep_value_size", stats.totalValueSize, add_stat, cookie);
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
    if (configuration.isWarmup()) {
        add_casted_stat("ep_warmup_thread",
                        epstats.warmupComplete.get() ? "complete" : "running",
                        add_stat, cookie);
        add_casted_stat("ep_warmed_up", epstats.warmedUp, add_stat, cookie);
        add_casted_stat("ep_warmup_dups", epstats.warmDups, add_stat, cookie);
        add_casted_stat("ep_warmup_oom", epstats.warmOOM, add_stat, cookie);

        if (epstats.warmupKeysTime > 0) {
            add_casted_stat("ep_warmup_keys_time", epstats.warmupKeysTime,
                            add_stat, cookie);
        }

        if (epstats.warmupComplete.get()) {
            add_casted_stat("ep_warmup_time", epstats.warmupTime,
                            add_stat, cookie);
        }
    }

    add_casted_stat("ep_dbinit", databaseInitTime, add_stat, cookie);
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
    add_casted_stat("ep_latency_arith_cmd", epstats.arithCmdHisto.total(),
                    add_stat, cookie);

    add_casted_stat("ep_degraded_mode", isDegradedMode(), add_stat, cookie);
    add_casted_stat("ep_exp_pager_stime", epstore->getExpiryPagerSleeptime(),
                    add_stat, cookie);
    add_casted_stat("ep_total_observe_sets", epstats.totalObserveSets,
                    add_stat, cookie);
    add_casted_stat("ep_stats_observe_polls", epstats.statsObservePolls,
                    add_stat, cookie);
    add_casted_stat("ep_observe_calls", epstats.observeCalls, add_stat,
                    cookie);
    add_casted_stat("ep_unobserve_calls", epstats.unobserveCalls, add_stat,
                    cookie);
    add_casted_stat("ep_observe_registry_size", epstats.obsRegSize, add_stat,
                    cookie);
    add_casted_stat("ep_observe_errors", epstats.obsErrors, add_stat, cookie);
    add_casted_stat("ep_obs_reg_clean_job", epstats.obsCleanerRuns, add_stat,
                    cookie);

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doMemoryStats(const void *cookie,
                                                           ADD_STAT add_stat) {

    add_casted_stat("mem_used", stats.currentSize + stats.memOverhead, add_stat,
                    cookie);
    add_casted_stat("ep_kv_size", stats.currentSize, add_stat, cookie);
    add_casted_stat("ep_value_size", stats.totalValueSize, add_stat, cookie);
    add_casted_stat("ep_overhead", stats.memOverhead, add_stat, cookie);
    add_casted_stat("ep_max_data_size", stats.maxDataSize, add_stat, cookie);
    add_casted_stat("ep_mem_low_wat", stats.mem_low_wat, add_stat, cookie);
    add_casted_stat("ep_mem_high_wat", stats.mem_high_wat, add_stat, cookie);
    add_casted_stat("ep_oom_errors", stats.oom_errors, add_stat, cookie);
    add_casted_stat("ep_tmp_oom_errors", stats.tmp_oom_errors, add_stat, cookie);

    std::map<std::string, size_t> allocator_stats;
    MemoryAllocatorStats::getAllocatorStats(allocator_stats);
    std::map<std::string, size_t>::iterator it = allocator_stats.begin();
    for (; it != allocator_stats.end(); ++it) {
        add_casted_stat(it->first.c_str(), it->second, add_stat, cookie);
    }

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doVBucketStats(const void *cookie,
                                                             ADD_STAT add_stat,
                                                             bool prevStateRequested,
                                                             bool details) {
    class StatVBucketVisitor : public VBucketVisitor {
    public:
        StatVBucketVisitor(const void *c, ADD_STAT a,
                           bool isPrevStateRequested, bool detailsRequested) :
            cookie(c), add_stat(a), isPrevState(isPrevStateRequested),
            isDetailsRequested(detailsRequested) {}

        bool visitBucket(RCPtr<VBucket> &vb) {
            if (isPrevState) {
                char buf[16];
                snprintf(buf, sizeof(buf), "vb_%d", vb->getId());
                add_casted_stat(buf, VBucket::toString(vb->getInitialState()),
                                add_stat, cookie);
            } else {
                vb->addStats(isDetailsRequested, add_stat, cookie);
            }
            return false;
        }

    private:
        const void *cookie;
        ADD_STAT add_stat;
        bool isPrevState;
        bool isDetailsRequested;
    };

    StatVBucketVisitor svbv(cookie, add_stat, prevStateRequested, details);
    epstore->visit(svbv);
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doHashStats(const void *cookie,
                                                          ADD_STAT add_stat) {

    class StatVBucketVisitor : public VBucketVisitor {
    public:
        StatVBucketVisitor(const void *c, ADD_STAT a) : cookie(c), add_stat(a) {}

        bool visitBucket(RCPtr<VBucket> &vb) {
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

        bool visitBucket(RCPtr<VBucket> &vb) {
            uint16_t vbid = vb->getId();
            char buf[64];
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
            snprintf(buf, sizeof(buf), "vb_%d:num_items_for_persistence", vbid);
            add_casted_stat(buf, vb->checkpointManager.getNumItemsForPersistence(),
                            add_stat, cookie);
            std::list<std::string> tapcursor_names = vb->checkpointManager.getTAPCursorNames();
            std::list<std::string>::iterator tap_it = tapcursor_names.begin();
            for (;tap_it != tapcursor_names.end(); ++tap_it) {
                snprintf(buf, sizeof(buf),
                         "vb_%d:cursor_checkpoint_id:%s", vbid, (*tap_it).c_str());
                add_casted_stat(buf, vb->checkpointManager.getCheckpointIdForTAPCursor(*tap_it),
                            add_stat, cookie);
            }
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
          tap_queueBackfillRemaining(0), tap_queueItemOnDisk(0), tap_totalBacklogSize(0)
    {}

    size_t      tap_queue;
    size_t      totalTaps;

    size_t      tap_queueFill;
    size_t      tap_queueDrain;
    size_t      tap_queueBackoff;
    size_t      tap_queueBackfillRemaining;
    size_t      tap_queueItemOnDisk;
    size_t      tap_totalBacklogSize;
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
            aggregator->tap_queueBackfillRemaining += tp->getBackfillRemaining();
            aggregator->tap_queueItemOnDisk += tp->getRemaingOnDisk();
            aggregator->tap_totalBacklogSize += tp->getBackfillRemaining() +
                                                tp->getRemainingOnCheckpoints();
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
            tc->tap_queueBackfillRemaining += tp->getBackfillRemaining();
            tc->tap_queueItemOnDisk += tp->getRemaingOnDisk();
            tc->tap_totalBacklogSize += tp->getBackfillRemaining() +
                                        tp->getRemainingOnCheckpoints();
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

    snprintf(statname, sl, "%s:total_backlog_size", prefix.c_str());
    add_casted_stat(statname, counter->tap_totalBacklogSize,
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
    tapConnMap->each(tapVisitor);

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
    tapConnMap->each(tapVisitor);

    add_casted_stat("ep_tap_total_fetched", stats.numTapFetched, add_stat, cookie);
    add_casted_stat("ep_tap_bg_max_pending", tapConfig->getBgMaxPending(),
                    add_stat, cookie);
    add_casted_stat("ep_tap_bg_fetched", stats.numTapBGFetched, add_stat, cookie);
    add_casted_stat("ep_tap_bg_fetch_requeued", stats.numTapBGFetchRequeued,
                    add_stat, cookie);
    add_casted_stat("ep_tap_fg_fetched", stats.numTapFGFetched, add_stat, cookie);
    add_casted_stat("ep_tap_deletes", stats.numTapDeletes, add_stat, cookie);
    add_casted_stat("ep_tap_throttled", stats.tapThrottled, add_stat, cookie);
    add_casted_stat("ep_tap_noop_interval", tapConnMap->getTapNoopInterval(), add_stat, cookie);
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
    add_casted_stat("ep_tap_total_backlog_size", aggregator.tap_totalBacklogSize,
                    add_stat, cookie);
    add_casted_stat("ep_tap_ack_window_size", tapConfig->getAckWindowSize(),
                    add_stat, cookie);
    add_casted_stat("ep_tap_ack_interval", tapConfig->getAckInterval(),
                    add_stat, cookie);
    add_casted_stat("ep_tap_ack_grace_period", tapConfig->getAckGracePeriod(),
                    add_stat, cookie);
    add_casted_stat("ep_tap_backoff_period",
                    tapConfig->getBackoffSleepTime(),
                    add_stat, cookie);
    add_casted_stat("ep_tap_throttle_threshold",
                    stats.tapThrottleThreshold * 100.0,
                    add_stat, cookie);
    add_casted_stat("ep_tap_throttle_queue_cap",
                    stats.tapThrottleWriteQueueCap, add_stat, cookie);

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
                    shared_ptr<Item> itm(gv.getValue());
                    if (diskItem.get()) {
                        // Both items exist
                        if (diskItem->getNBytes() != itm->getNBytes()) {
                            valid.assign("length_mismatch");
                        } else if (memcmp(diskItem->getData(), itm->getData(),
                                          diskItem->getNBytes()) != 0) {
                            valid.assign("data_mismatch");
                        } else if (diskItem->getFlags() != itm->getFlags()) {
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
    add_casted_stat("disk_invalid_vbtable_del", stats.diskInvalidVBTableDelHisto,
                    add_stat, cookie);
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

ENGINE_ERROR_CODE EventuallyPersistentEngine::doObserveStats(const void* cookie,
                                                             ADD_STAT add_stat,
                                                             const char* stat_key,
                                                             int nkey) {
    stats.statsObservePolls++;
    std::string obs_set(stat_key, nkey);
    state_map* smap = getObserveRegistry().getObserveSetState(obs_set);
    std::map<std::string, std::string>::iterator itr;
    for (itr = smap->begin(); itr != smap->end(); itr++) {
        add_casted_stat(itr->first.c_str(), itr->second.c_str(), add_stat, cookie);
    }
    delete smap;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::getStats(const void* cookie,
                                                       const char* stat_key,
                                                       int nkey,
                                                       ADD_STAT add_stat) {
    if (stat_key != NULL) {
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "stats %s %d", stat_key, nkey);
    } else {
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "stats engine");
    }

    ENGINE_ERROR_CODE rv = ENGINE_KEY_ENOENT;
    if (stat_key == NULL) {
        rv = doEngineStats(cookie, add_stat);
    } else if (nkey > 8 && strncmp(stat_key, "observe ", 8) == 0) {
        rv = doObserveStats(cookie, add_stat, stat_key + 8, nkey - 8);
    } else if (nkey > 7 && strncmp(stat_key, "tapagg ", 7) == 0) {
        rv = doTapAggStats(cookie, add_stat, stat_key + 7, nkey - 7);
    } else if (nkey == 3 && strncmp(stat_key, "tap", 3) == 0) {
        rv = doTapStats(cookie, add_stat);
    } else if (nkey == 4 && strncmp(stat_key, "hash", 3) == 0) {
        rv = doHashStats(cookie, add_stat);
    } else if (nkey == 7 && strncmp(stat_key, "vbucket", 7) == 0) {
        rv = doVBucketStats(cookie, add_stat, false, false);
    } else if (nkey == 15 && strncmp(stat_key, "vbucket-details", 15) == 0) {
        rv = doVBucketStats(cookie, add_stat, false, true);
    } else if (nkey == 12 && strncmp(stat_key, "prev-vbucket", 12) == 0) {
        rv = doVBucketStats(cookie, add_stat, true, false);
    } else if (nkey == 10 && strncmp(stat_key, "checkpoint", 10) == 0) {
        rv = doCheckpointStats(cookie, add_stat);
    } else if (nkey == 7 && strncmp(stat_key, "timings", 7) == 0) {
        rv = doTimingStats(cookie, add_stat);
    } else if (nkey == 10 && strncmp(stat_key, "dispatcher", 10) == 0) {
        rv = doDispatcherStats(cookie, add_stat);
    } else if (nkey == 6 && strncmp(stat_key, "memory", 6) == 0) {
        rv = doMemoryStats(cookie, add_stat);
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
    } else if (nkey == 7 && strncmp(stat_key, "couchdb", 7) == 0) {
        add_casted_stat("ep_set_vbucket", stats.setVbucketStateHisto,
                        add_stat, cookie);
        add_casted_stat("ep_snapshot_vbucket", stats.snapshotVbucketHisto,
                        add_stat, cookie);
        add_casted_stat("ep_delq", stats.couchDelqHisto,
                        add_stat, cookie);
        add_casted_stat("ep_get_hit", stats.couchGetHisto,
                        add_stat, cookie);
        add_casted_stat("ep_get_fail", stats.couchGetFailHisto,
                        add_stat, cookie);
        add_casted_stat("ep_set_hit", stats.couchSetHisto,
                        add_stat, cookie);
        add_casted_stat("ep_set_fail", stats.couchSetFailHisto,
                        add_stat, cookie);

        rv = ENGINE_SUCCESS;
    } else if (nkey == 7 && strncmp(stat_key, "kvstore", 7) == 0) {
        getEpStore()->getROUnderlying()->addStats("ro", add_stat, cookie);
        getEpStore()->getRWUnderlying()->addStats("rw", add_stat, cookie);
        rv = ENGINE_SUCCESS;
    }

    return rv;
}

void EventuallyPersistentEngine::notifyPendingConnections(void) {
    // Fix clean shutdown!!!
    while (!shutdown) {
        tapConnMap->notifyIOThreadMain();
        epstore->firePendingVBucketOps();

        if (shutdown) {
            return;
        }

        tapConnMap->wait(1.0);
    }
}

void EventuallyPersistentEngine::notifyNotificationThread(void) {
    tapConnMap->notify();
}

void EventuallyPersistentEngine::setTapValidity(const std::string &name, const void* token) {
    tapConnMap->setValidity(name, token);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::touch(const void *cookie,
                                                    protocol_binary_request_header *request,
                                                    ADD_RESPONSE response)
{
    if (request->request.extlen != 4 || request->request.keylen == 0) {
        return sendResponse(response, NULL, 0, NULL, 0, NULL, 0,
                            PROTOCOL_BINARY_RAW_BYTES,
                            PROTOCOL_BINARY_RESPONSE_EINVAL, 0, cookie);
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
            rv = sendResponse(response, NULL, 0, NULL, 0, NULL, 0,
                              PROTOCOL_BINARY_RAW_BYTES,
                              PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0, cookie);
        }
    } else if (rv == ENGINE_NOT_MY_VBUCKET) {
        rv = sendResponse(response, NULL, 0, NULL, 0, NULL, 0,
                          PROTOCOL_BINARY_RAW_BYTES,
                          PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET, 0, cookie);
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

                    bool visitBucket(RCPtr<VBucket> &vb) {
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

                    bool visitBucket(RCPtr<VBucket> &vb) {
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

                    bool visitBucket(RCPtr<VBucket> &vb) {
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

    return sendResponse(response, NULL, 0, NULL, 0, NULL, 0,
                        PROTOCOL_BINARY_RAW_BYTES, rv, 0, cookie);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::observe(const void *cookie,
                                                      std::string key,
                                                      uint64_t cas,
                                                      uint16_t vbucket,
                                                      std::string obs_set,
                                                      uint32_t expiration,
                                                      ADD_RESPONSE response) {
    protocol_binary_response_status rv;

    stats.observeCalls++;
    getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "observe %s %ld %s %d",
                     key.c_str(), cas, obs_set.c_str(), expiration);
    rv = getObserveRegistry().observeKey(key, cas, vbucket, expiration, obs_set);
    if (rv == PROTOCOL_BINARY_RESPONSE_ETMPFAIL) {
        return sendResponse(response, NULL, 0, NULL, 0, NULL, 0, 0, memoryCondition(),
                            0, cookie);
    } else if (rv == PROTOCOL_BINARY_RESPONSE_EBUSY) {
        return sendResponse(response, "Observe set full", 16, NULL, 0, NULL, 0, 0,
                            rv, 0, cookie);
    }
    return sendResponse(response, NULL, 0, NULL, 0, NULL, 0, 0, rv, 0, cookie);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::unobserve(const void *cookie,
                                                        std::string key,
                                                        uint64_t cas,
                                                        uint16_t vbucket,
                                                        std::string obs_set,
                                                        ADD_RESPONSE response) {
    stats.unobserveCalls++;
    getLogger()->log(EXTENSION_LOG_DEBUG, NULL, "unobserve %s %ld %s",
                     key.c_str(), cas, obs_set.c_str());
    getObserveRegistry().unobserveKey(key, cas, vbucket, obs_set);
    return sendResponse(response, NULL, 0, NULL, 0, NULL, 0, 0, 0, 0, cookie);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::handleRestoreCmd(const void *cookie,
                                                               protocol_binary_request_header *request,
                                                               ADD_RESPONSE response)
{
    LockHolder lh(restore.mutex);
    if (restore.manager == NULL) { // we need another "mode" variable
        return sendResponse(response,NULL, 0, NULL, 0, NULL, 0,
                            PROTOCOL_BINARY_RAW_BYTES,
                            PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED,
                            0, cookie);
    }

    if (request->request.opcode == CMD_RESTORE_FILE) {
        std::string filename((const char*)request->bytes + sizeof(request->bytes) +
                             request->request.extlen, ntohs(request->request.keylen));
        try {
            restore.manager->initialize(filename);
        } catch (std::string e) {
            return sendResponse(response, NULL, 0, NULL, 0, e.c_str(),
                                e.length(),
                                PROTOCOL_BINARY_RAW_BYTES,
                                PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0, cookie);
        }

        try {
            restore.manager->start();
        } catch (std::string e) {
            return sendResponse(response, NULL, 0, NULL, 0, e.c_str(),
                                e.length(), PROTOCOL_BINARY_RAW_BYTES,
                                PROTOCOL_BINARY_RESPONSE_EINTERNAL, 0, cookie);
        }
    } else if (request->request.opcode == CMD_RESTORE_ABORT) {
        try {
            restore.manager->abort();
        } catch (std::string e) {
            return sendResponse(response, NULL, 0, NULL, 0, e.c_str(),
                                e.length(), PROTOCOL_BINARY_RAW_BYTES,
                                PROTOCOL_BINARY_RESPONSE_EINTERNAL, 0, cookie);
        }
    } else {
        if (restore.manager->isRunning()) {
            return sendResponse(response, NULL, 0, NULL, 0, NULL, 0,
                                PROTOCOL_BINARY_RAW_BYTES,
                                PROTOCOL_BINARY_RESPONSE_EBUSY, 0, cookie);
        }

        destroy_restore_manager(restore.manager);
        restore.enabled.set(false);
        restore.manager = NULL;
        if (!isDegradedMode()) {
            epstore->completeDegradedMode();
        }
    }

    return sendResponse(response, NULL, 0, NULL, 0, NULL, 0,
                        PROTOCOL_BINARY_RAW_BYTES,
                        PROTOCOL_BINARY_RESPONSE_SUCCESS, 0, cookie);
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
    bool rv = tapConnMap->closeTapConnectionByName(tap_name);
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

    return sendResponse(response, NULL, 0, NULL, 0, NULL, 0,
                        PROTOCOL_BINARY_RAW_BYTES,
                        PROTOCOL_BINARY_RESPONSE_SUCCESS, 0, cookie);
}

ENGINE_ERROR_CODE
EventuallyPersistentEngine::handleGetLastClosedCheckpointId(const void *cookie,
                                                            protocol_binary_request_header *req,
                                                            ADD_RESPONSE response) {
    uint16_t vbucket = ntohs(req->request.vbucket);
    RCPtr<VBucket> vb = getVBucket(vbucket);
    if (!vb) {
        return sendResponse(response, NULL, 0, NULL, 0, NULL, 0,
                            PROTOCOL_BINARY_RAW_BYTES,
                            PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET, 0, cookie);
    }

    uint64_t checkpointId = vb->checkpointManager.getLastClosedCheckpointId();
    checkpointId = htonll(checkpointId);
    return sendResponse(response, NULL, 0, NULL, 0, &checkpointId,
                        sizeof(checkpointId), PROTOCOL_BINARY_RAW_BYTES,
                        PROTOCOL_BINARY_RESPONSE_SUCCESS, 0, cookie);
}

ENGINE_ERROR_CODE
EventuallyPersistentEngine::resetReplicationChain(const void *cookie,
                                                  protocol_binary_request_header *req,
                                                  ADD_RESPONSE response) {
    (void) req;
    tapConnMap->resetReplicaChain();
    return sendResponse(response, NULL, 0, NULL, 0, NULL, 0,
                        PROTOCOL_BINARY_RAW_BYTES,
                        PROTOCOL_BINARY_RESPONSE_SUCCESS, 0, cookie);
}

static protocol_binary_response_status engine_error_2_protocol_error(ENGINE_ERROR_CODE e) {
    protocol_binary_response_status ret;

    switch (e) {
    case ENGINE_SUCCESS:
        return PROTOCOL_BINARY_RESPONSE_SUCCESS;
    case ENGINE_KEY_ENOENT:
        return PROTOCOL_BINARY_RESPONSE_KEY_ENOENT;
    case ENGINE_KEY_EEXISTS:
        return PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS;
    case ENGINE_ENOMEM:
        return PROTOCOL_BINARY_RESPONSE_ENOMEM;
    case ENGINE_TMPFAIL:
        return PROTOCOL_BINARY_RESPONSE_ETMPFAIL;
    case ENGINE_NOT_STORED:
        return PROTOCOL_BINARY_RESPONSE_NOT_STORED;
    case ENGINE_EINVAL:
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    case ENGINE_ENOTSUP:
        return PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED;
    case ENGINE_E2BIG:
        return PROTOCOL_BINARY_RESPONSE_E2BIG;
    case ENGINE_NOT_MY_VBUCKET:
        return PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET;
    case ENGINE_ERANGE:
        return PROTOCOL_BINARY_RESPONSE_ERANGE;
    default:
        ret = PROTOCOL_BINARY_RESPONSE_EINTERNAL;
    }

    return ret;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::getMeta(const void* cookie,
                                                      protocol_binary_request_get_meta *request,
                                                      ADD_RESPONSE response)
{
    if (request->message.header.request.extlen != 0 || request->message.header.request.keylen == 0) {
        return sendResponse(response, NULL, 0, NULL, 0, NULL, 0,
                            PROTOCOL_BINARY_RAW_BYTES,
                            PROTOCOL_BINARY_RESPONSE_EINVAL, 0, cookie);
    }

    std::string key((char *)(request->bytes + sizeof(request->bytes)),
                    (size_t)ntohs(request->message.header.request.keylen));
    uint16_t vbucket = ntohs(request->message.header.request.vbucket);

    std::string meta;
    uint64_t cas;
    uint32_t flags;

    ENGINE_ERROR_CODE rv = epstore->getMetaData(key, vbucket, cookie,
                                                meta, cas, flags);

    if (rv == ENGINE_SUCCESS) {
        rv = sendResponse(response, NULL, 0, (const void *)&flags, 4,
                          meta.data(), meta.length(),
                          PROTOCOL_BINARY_RAW_BYTES,
                          PROTOCOL_BINARY_RESPONSE_SUCCESS,
                          cas, cookie);
    } else if (rv != ENGINE_EWOULDBLOCK) {
        if (rv == ENGINE_KEY_ENOENT &&
            request->message.header.request.opcode == CMD_GETQ_META) {
            rv = ENGINE_SUCCESS;
        } else {
            rv = sendResponse(response, NULL, 0, NULL, 0, NULL, 0,
                              PROTOCOL_BINARY_RAW_BYTES,
                              engine_error_2_protocol_error(rv), 0, cookie);
        }
    }

    return rv;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::setWithMeta(const void* cookie,
                                                    protocol_binary_request_set_with_meta *request,
                                                    ADD_RESPONSE response)
{
    // revid_nbytes, flags and exptime is mandatory fields.. and we need a key
    if (request->message.header.request.extlen != 12 || request->message.header.request.keylen == 0) {
        return sendResponse(response, NULL, 0, NULL, 0, NULL, 0,
                            PROTOCOL_BINARY_RAW_BYTES,
                            PROTOCOL_BINARY_RESPONSE_EINVAL, 0, cookie);
    }

    if (isDegradedMode()) {
        // We're allowed to run set in restore mode..
        if (!restore.enabled.get()) {
            return sendResponse(response, NULL, 0, NULL, 0, NULL, 0,
                                PROTOCOL_BINARY_RAW_BYTES,
                                PROTOCOL_BINARY_RESPONSE_ETMPFAIL,
                                0, cookie);
        }
    }

    uint8_t *key = request->bytes + sizeof(request->bytes);
    uint16_t nkey = ntohs(request->message.header.request.keylen);
    uint16_t vbucket = ntohs(request->message.header.request.vbucket);
    uint32_t flags = request->message.body.flags;

    uint8_t *dta = key + nkey;
    size_t nbytes = ntohl(request->message.header.request.bodylen);
    nbytes -= nkey + request->message.header.request.extlen;
    uint32_t metabytes = ntohl(request->message.body.nmeta_bytes);
    nbytes -= metabytes;
    uint32_t exptime = ntohl(request->message.body.expiration);
    if (exptime != 0) {
        exptime = serverApi->core->abstime(serverApi->core->realtime(exptime));
    }
    uint32_t seqno;
    uint64_t cas;
    uint32_t length;
    uint32_t fl;
    uint8_t opcode = request->message.header.request.opcode;

    if (!Item::decodeMeta(dta + nbytes, seqno, cas, length, fl) ||
        length != nbytes || fl != flags) {
        return sendResponse(response, NULL, 0, NULL, 0, NULL, 0,
                            PROTOCOL_BINARY_RAW_BYTES,
                            PROTOCOL_BINARY_RESPONSE_EINVAL, 0, cookie);
    }

    Item *itm = new Item(key, nkey, nbytes, flags,
                         exptime, cas, -1, vbucket);
    if (itm == NULL) {
        return sendResponse(response, NULL, 0, NULL, 0, NULL, 0,
                            PROTOCOL_BINARY_RAW_BYTES,
                            PROTOCOL_BINARY_RESPONSE_ENOMEM, 0, cookie);
    }
    memcpy((char*)itm->getData(), dta, nbytes);
    itm->setSeqno(seqno);

    bool allowExisting = (opcode == CMD_SET_WITH_META ||
                          opcode == CMD_SETQ_WITH_META);

    ENGINE_ERROR_CODE ret = epstore->setWithMeta(*itm,
                                                 ntohll(request->message.header.request.cas),
                                                 cookie, false, allowExisting);
    protocol_binary_response_status rc;
    rc = engine_error_2_protocol_error(ret);

    if (ret == ENGINE_SUCCESS) {
        addMutationEvent(itm);
        cas = itm->getCas();
    } else {
        cas = 0;
    }

    delete itm;
    if ((opcode == CMD_SETQ_WITH_META || opcode == CMD_ADDQ_WITH_META) &&
        rc == PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        return ENGINE_SUCCESS;
    }

    return sendResponse(response, NULL, 0, NULL, 0, NULL, 0,
                        PROTOCOL_BINARY_RAW_BYTES,
                        rc, cas, cookie);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::deleteWithMeta(const void* cookie,
                                                             protocol_binary_request_delete_with_meta *request,
                                                             ADD_RESPONSE response) {
    // revid_nbytes, flags and exptime is mandatory fields.. and we need a key
    if (request->message.header.request.extlen != 12 || request->message.header.request.keylen == 0) {
        return sendResponse(response, NULL, 0, NULL, 0, NULL, 0,
                            PROTOCOL_BINARY_RAW_BYTES,
                            PROTOCOL_BINARY_RESPONSE_EINVAL, 0, cookie);
    }

    if (isDegradedMode()) {
        // We're allowed to run set in restore mode..
        if (!restore.enabled.get()) {
            return sendResponse(response, NULL, 0, NULL, 0, NULL, 0,
                                PROTOCOL_BINARY_RAW_BYTES,
                                PROTOCOL_BINARY_RESPONSE_ETMPFAIL,
                                0, cookie);
        }
    }

    const char *key_ptr = reinterpret_cast<const char*>(request->bytes);
    key_ptr += sizeof(request->bytes);
    uint16_t nkey = ntohs(request->message.header.request.keylen);
    std::string key(key_ptr, nkey);
    uint16_t vbucket = ntohs(request->message.header.request.vbucket);

    uint8_t *dta = request->bytes + sizeof(request->bytes) + nkey;
    size_t nbytes = ntohl(request->message.header.request.bodylen);
    nbytes -= nkey + request->message.header.request.extlen;
    uint32_t metabytes = ntohl(request->message.body.nmeta_bytes);
    nbytes -= metabytes;

    uint32_t seqno;
    uint64_t cas;
    uint32_t length;
    uint32_t fl;
    uint8_t opcode = request->message.header.request.opcode;

    if (!Item::decodeMeta(dta + nbytes, seqno, cas, length, fl) || length != nbytes) {
        return sendResponse(response, NULL, 0, NULL, 0, NULL, 0,
                            PROTOCOL_BINARY_RAW_BYTES,
                            PROTOCOL_BINARY_RESPONSE_EINVAL, 0, cookie);
    }

    ENGINE_ERROR_CODE ret = epstore->deleteWithMeta(key, seqno,
                                                    ntohll(request->message.header.request.cas),
                                                    vbucket, cookie, false);
    protocol_binary_response_status rc;
    rc = engine_error_2_protocol_error(ret);

    if (ret == ENGINE_SUCCESS) {
        addDeleteEvent(key, vbucket, ntohll(request->message.header.request.cas));
    }

    if (opcode == CMD_DELQ_WITH_META && rc == PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        return ENGINE_SUCCESS;
    }

    return sendResponse(response, NULL, 0, NULL, 0, NULL, 0,
                        PROTOCOL_BINARY_RAW_BYTES, rc, 0, cookie);
}
