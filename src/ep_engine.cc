/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 Couchbase, Inc
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

#include <assert.h>
#include <fcntl.h>
#include <memcached/engine.h>
#include <memcached/protocol_binary.h>
#include <memcached/util.h>
#include <platform/platform.h>
#include <stdarg.h>

#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <limits>
#include <string>
#include <vector>

#include "backfill.h"
#include "ep_engine.h"
#include "htresizer.h"
#include "iomanager/iomanager.h"
#include "memory_tracker.h"
#include "stats-info.h"
#define STATWRITER_NAMESPACE core_engine
#include "statwriter.h"
#undef STATWRITER_NAMESPACE
#include "tapthrottle.h"
#include "warmup.h"

static ALLOCATOR_HOOKS_API *hooksApi;
static SERVER_LOG_API *loggerApi;

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

static inline void releaseHandle(ENGINE_HANDLE* handle) {
    (void) handle;
    ObjectRegistry::onSwitchThread(NULL);
}

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
    ENGINE_ERROR_CODE rv = ENGINE_FAILED;
    EventuallyPersistentEngine *e = ObjectRegistry::onSwitchThread(NULL, true);
    if (response(key, keylen, ext, extlen, body, bodylen, datatype,
                 status, cas, cookie)) {
        rv = ENGINE_SUCCESS;
    }
    ObjectRegistry::onSwitchThread(e);
    return rv;
}

template <typename T>
static void validate(T v, T l, T h) {
    if (v < l || v > h) {
        throw std::runtime_error("Value out of range.");
    }
}


static void checkNumeric(const char* str) {
    int i = 0;
    if (str[0] == '-') {
        i++;
    }
    for (; str[i]; i++) {
        if (!std::isdigit(str[i])) {
            throw std::runtime_error("Value is not numeric");
        }
    }
}

// The Engine API specifies C linkage for the functions..
extern "C" {

    static const engine_info* EvpGetInfo(ENGINE_HANDLE* handle)
    {
        engine_info* info = getHandle(handle)->getInfo();
        releaseHandle(handle);
        return info;
    }

    static ENGINE_ERROR_CODE EvpInitialize(ENGINE_HANDLE* handle,
                                           const char* config_str)
    {
        ENGINE_ERROR_CODE err_code = getHandle(handle)->initialize(config_str);
        releaseHandle(handle);
        return err_code;
    }

    static void EvpDestroy(ENGINE_HANDLE* handle, const bool force)
    {
        getHandle(handle)->destroy(force);
        delete getHandle(handle);
        releaseHandle(NULL);
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
        ENGINE_ERROR_CODE err_code = getHandle(handle)->itemAllocate(cookie, itm, key,
                                                                     nkey, nbytes, flags,
                                                                     exptime);
        releaseHandle(handle);
        return err_code;
    }

    static ENGINE_ERROR_CODE EvpItemDelete(ENGINE_HANDLE* handle,
                                           const void* cookie,
                                           const void* key,
                                           const size_t nkey,
                                           uint64_t* cas,
                                           uint16_t vbucket)
    {
        ENGINE_ERROR_CODE err_code = getHandle(handle)->itemDelete(cookie, key, nkey,
                                                                   cas, vbucket);
        releaseHandle(handle);
        return err_code;
    }

    static void EvpItemRelease(ENGINE_HANDLE* handle,
                               const void *cookie,
                               item* itm)
    {
        getHandle(handle)->itemRelease(cookie, itm);
        releaseHandle(handle);
    }

    static ENGINE_ERROR_CODE EvpGet(ENGINE_HANDLE* handle,
                                    const void* cookie,
                                    item** itm,
                                    const void* key,
                                    const int nkey,
                                    uint16_t vbucket)
    {
        ENGINE_ERROR_CODE err_code = getHandle(handle)->get(cookie, itm, key, nkey, vbucket);
        releaseHandle(handle);
        return err_code;
    }

    static ENGINE_ERROR_CODE EvpGetStats(ENGINE_HANDLE* handle,
                                         const void* cookie,
                                         const char* stat_key,
                                         int nkey,
                                         ADD_STAT add_stat)
    {
        ENGINE_ERROR_CODE err_code = getHandle(handle)->getStats(cookie, stat_key, nkey,
                                                                 add_stat);
        releaseHandle(handle);
        return err_code;
    }

    static ENGINE_ERROR_CODE EvpStore(ENGINE_HANDLE* handle,
                                      const void *cookie,
                                      item* itm,
                                      uint64_t *cas,
                                      ENGINE_STORE_OPERATION operation,
                                      uint16_t vbucket)
    {
        ENGINE_ERROR_CODE err_code = getHandle(handle)->store(cookie, itm, cas, operation,
                                                              vbucket);
        releaseHandle(handle);
        return err_code;
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
        ENGINE_ERROR_CODE ecode = getHandle(handle)->arithmetic(cookie, key, nkey, increment,
                                                                create, delta, initial,
                                                                exptime, cas, result, vbucket);
        releaseHandle(handle);
        return ecode;
    }

    static ENGINE_ERROR_CODE EvpFlush(ENGINE_HANDLE* handle,
                                      const void* cookie, time_t when)
    {
        ENGINE_ERROR_CODE err_code = getHandle(handle)->flush(cookie, when);
        releaseHandle(handle);
        return err_code;
    }

    static void EvpResetStats(ENGINE_HANDLE* handle, const void *)
    {
        getHandle(handle)->resetStats();
        releaseHandle(handle);
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
            int v = atoi(valz);
            if (strcmp(keyz, "tap_keepalive") == 0) {
                checkNumeric(valz);
                validate(v, 0, MAX_TAP_KEEP_ALIVE);
                e->setTapKeepAlive(static_cast<uint32_t>(v));
            } else if (strcmp(keyz, "tap_throttle_threshold") == 0) {
                checkNumeric(valz);
                e->getConfiguration().setTapThrottleThreshold(v);
            } else if (strcmp(keyz, "tap_throttle_queue_cap") == 0) {
                checkNumeric(valz);
                e->getConfiguration().setTapThrottleQueueCap(v);
            } else if (strcmp(keyz, "tap_throttle_cap_pcnt") == 0) {
                checkNumeric(valz);
                e->getConfiguration().setTapThrottleCapPcnt(v);
            } else {
                *msg = "Unknown config param";
                rv = PROTOCOL_BINARY_RESPONSE_KEY_ENOENT;
            }
        } catch(std::runtime_error &ignored_exception) {
            *msg = "Value out of range.";
            rv = PROTOCOL_BINARY_RESPONSE_EINVAL;
        }

        return rv;
    }

    static protocol_binary_response_status setCheckpointParam(EventuallyPersistentEngine *e,
                                                              const char *keyz,
                                                              const char *valz,
                                                              const char **msg,
                                                              size_t *) {
        protocol_binary_response_status rv = PROTOCOL_BINARY_RESPONSE_SUCCESS;

        try {
            int v = atoi(valz);
            if (strcmp(keyz, "chk_max_items") == 0) {
                checkNumeric(valz);
                validate(v, MIN_CHECKPOINT_ITEMS, MAX_CHECKPOINT_ITEMS);
                e->getConfiguration().setChkMaxItems(v);
            } else if (strcmp(keyz, "chk_period") == 0) {
                checkNumeric(valz);
                validate(v, MIN_CHECKPOINT_PERIOD, MAX_CHECKPOINT_PERIOD);
                e->getConfiguration().setChkPeriod(v);
            } else if (strcmp(keyz, "max_checkpoints") == 0) {
                checkNumeric(valz);
                validate(v, DEFAULT_MAX_CHECKPOINTS, MAX_CHECKPOINTS_UPPER_BOUND);
                e->getConfiguration().setMaxCheckpoints(v);
            } else if (strcmp(keyz, "item_num_based_new_chk") == 0) {
                if (strcmp(valz, "true") == 0) {
                    e->getConfiguration().setItemNumBasedNewChk(true);
                } else {
                    e->getConfiguration().setItemNumBasedNewChk(false);
                }
            } else if (strcmp(keyz, "keep_closed_chks") == 0) {
                if (strcmp(valz, "true") == 0) {
                    e->getConfiguration().setKeepClosedChks(true);
                } else {
                    e->getConfiguration().setKeepClosedChks(false);
                }
            } else {
                *msg = "Unknown config param";
                rv = PROTOCOL_BINARY_RESPONSE_KEY_ENOENT;
            }
        } catch(std::runtime_error &ignored_exception) {
            *msg = "Value out of range.";
            rv = PROTOCOL_BINARY_RESPONSE_EINVAL;
        }

        return rv;
    }

    static protocol_binary_response_status setFlushParam(EventuallyPersistentEngine *e,
                                                         const char *keyz, const char *valz,
                                                         const char **msg,
                                                         size_t *) {
        protocol_binary_response_status rv = PROTOCOL_BINARY_RESPONSE_SUCCESS;

        // Handle the actual mutation.
        try {
            int v = atoi(valz);
            if (strcmp(keyz, "max_txn_size") == 0) {
                checkNumeric(valz);
                e->getConfiguration().setMaxTxnSize(v);
            } else if (strcmp(keyz, "bg_fetch_delay") == 0) {
                checkNumeric(valz);
                e->getConfiguration().setBgFetchDelay(v);
            } else if (strcmp(keyz, "flushall_enabled") == 0) {
                if (strcmp(valz, "true") == 0) {
                    e->getConfiguration().setFlushallEnabled(true);
                } else if(strcmp(valz, "false") == 0) {
                    e->getConfiguration().setFlushallEnabled(false);
                } else {
                    throw std::runtime_error("value out of range.");
                }
            } else if (strcmp(keyz, "max_size") == 0) {
                char *ptr = NULL;
                checkNumeric(valz);
                uint64_t vsize = strtoull(valz, &ptr, 10);
                validate(vsize, static_cast<uint64_t>(0),
                         std::numeric_limits<uint64_t>::max());
                e->getConfiguration().setMaxSize(vsize);
                e->getConfiguration().setMemLowWat(percentOf(vsize, 0.75));
                e->getConfiguration().setMemHighWat(percentOf(vsize, 0.85));
            } else if (strcmp(keyz, "mem_low_wat") == 0) {
                char *ptr = NULL;
                checkNumeric(valz);
                uint64_t vsize = strtoull(valz, &ptr, 10);
                validate(vsize, static_cast<uint64_t>(0),
                         std::numeric_limits<uint64_t>::max());
                e->getConfiguration().setMemLowWat(vsize);
            } else if (strcmp(keyz, "mem_high_wat") == 0) {
                char *ptr = NULL;
                checkNumeric(valz);
                uint64_t vsize = strtoull(valz, &ptr, 10);
                validate(vsize, static_cast<uint64_t>(0),
                         std::numeric_limits<uint64_t>::max());
                e->getConfiguration().setMemHighWat(vsize);
            } else if (strcmp(keyz, "mutation_mem_threshold") == 0) {
                checkNumeric(valz);
                validate(v, 0, 100);
                e->getConfiguration().setMutationMemThreshold(v);
            } else if (strcmp(keyz, "timing_log") == 0) {
                EPStats &stats = e->getEpStats();
                std::ostream *old = stats.timingLog;
                stats.timingLog = NULL;
                delete old;
                if (strcmp(valz, "off") == 0) {
                    LOG(EXTENSION_LOG_INFO, "Disabled timing log.");
                } else {
                    std::ofstream *tmp(new std::ofstream(valz));
                    if (tmp->good()) {
                        LOG(EXTENSION_LOG_INFO,
                            "Logging detailed timings to ``%s''.", valz);
                        stats.timingLog = tmp;
                    } else {
                        LOG(EXTENSION_LOG_WARNING,
                            "Error setting detailed timing log to ``%s'':  %s",
                            valz, strerror(errno));
                        delete tmp;
                    }
                }
            } else if (strcmp(keyz, "exp_pager_stime") == 0) {
                char *ptr = NULL;
                checkNumeric(valz);
                uint64_t vsize = strtoull(valz, &ptr, 10);
                validate(vsize, static_cast<uint64_t>(0),
                         std::numeric_limits<uint64_t>::max());
                e->getConfiguration().setExpPagerStime((size_t)vsize);
            } else if (strcmp(keyz, "couch_response_timeout") == 0) {
                checkNumeric(valz);
                e->getConfiguration().setCouchResponseTimeout(v);
            } else if (strcmp(keyz, "alog_sleep_time") == 0) {
                checkNumeric(valz);
                e->getConfiguration().setAlogSleepTime(v);
            } else if (strcmp(keyz, "alog_task_time") == 0) {
                checkNumeric(valz);
                e->getConfiguration().setAlogTaskTime(v);
            } else if (strcmp(keyz, "pager_active_vb_pcnt") == 0) {
                checkNumeric(valz);
                e->getConfiguration().setPagerActiveVbPcnt(v);
            } else if (strcmp(keyz, "warmup_min_memory_threshold") == 0) {
                checkNumeric(valz);
                validate(v, 0, std::numeric_limits<int>::max());
                e->getConfiguration().setWarmupMinMemoryThreshold(v);
            } else if (strcmp(keyz, "warmup_min_items_threshold") == 0) {
                checkNumeric(valz);
                validate(v, 0, std::numeric_limits<int>::max());
                e->getConfiguration().setWarmupMinItemsThreshold(v);
            } else {
                *msg = "Unknown config param";
                rv = PROTOCOL_BINARY_RESPONSE_KEY_ENOENT;
            }
        } catch(std::runtime_error& ex) {
            *msg = strdup(ex.what());
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

        LOG(EXTENSION_LOG_DEBUG, "Manually evicting object with key %s\n", keyz);

        protocol_binary_response_status rv = e->evictKey(key, vbucket, msg, msg_size);
        if (rv == PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET ||
            rv == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT) {
            if (e->isDegradedMode()) {
                return PROTOCOL_BINARY_RESPONSE_ETMPFAIL;
            }
        }
        return rv;
    }

    static ENGINE_ERROR_CODE getLocked(EventuallyPersistentEngine *e,
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
            LOG(EXTENSION_LOG_DEBUG, "Illegal value for lock timeout specified "
                "%u. Using default value: %u\n", lockTimeout, default_timeout);
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
            if (e->isDegradedMode()) {
                *msg = "LOCK_TMP_ERROR";
                *res = PROTOCOL_BINARY_RESPONSE_ETMPFAIL;
                return ENGINE_TMPFAIL;
            }

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

        LOG(EXTENSION_LOG_DEBUG, "Executing unl for key %s\n", keyz);

        RememberingCallback<GetValue> getCb;
        uint64_t cas = ntohll(request->request.cas);

        ENGINE_ERROR_CODE rv = e->unlockKey(key, vbucket, cas, ep_current_time());

        if (rv == ENGINE_SUCCESS) {
            *msg = "UNLOCKED";
        } else if (rv == ENGINE_TMPFAIL){
            *msg =  "UNLOCK_ERROR";
            res = PROTOCOL_BINARY_RESPONSE_ETMPFAIL;
        } else {
            if (e->isDegradedMode()) {
                *msg = "LOCK_TMP_ERROR";
                return PROTOCOL_BINARY_RESPONSE_ETMPFAIL;
            }

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
                                                    protocol_binary_request_set_param *req,
                                                    const char **msg,
                                                    size_t *msg_size) {
        size_t keylen = ntohs(req->message.header.request.keylen);
        uint8_t extlen = req->message.header.request.extlen;
        size_t vallen = ntohl(req->message.header.request.bodylen);
        engine_param_t paramtype =
            static_cast<engine_param_t>(ntohl(req->message.body.param_type));

        if (keylen == 0 || (vallen - keylen - extlen) == 0) {
            return PROTOCOL_BINARY_RESPONSE_EINVAL;
        }

        const char *keyp = reinterpret_cast<const char*>(req->bytes) + sizeof(req->bytes);
        const char *valuep = keyp + keylen;
        vallen -= (keylen + extlen);

        char keyz[32];
        char valz[512];

        // Read the key.
        if (keylen >= sizeof(keyz)) {
            *msg = "Key is too large.";
            return PROTOCOL_BINARY_RESPONSE_EINVAL;
        }
        memcpy(keyz, keyp, keylen);
        keyz[keylen] = 0x00;

        // Read the value.
        if (vallen >= sizeof(valz)) {
            *msg = "Value is too large.";
            return PROTOCOL_BINARY_RESPONSE_EINVAL;
        }
        memcpy(valz, valuep, vallen);
        valz[vallen] = 0x00;

        protocol_binary_response_status rv;

        switch (paramtype) {
        case engine_param_flush:
            rv = setFlushParam(e, keyz, valz, msg, msg_size);
            break;
        case engine_param_tap:
            rv = setTapParam(e, keyz, valz, msg, msg_size);
            break;
        case engine_param_checkpoint:
            rv = setCheckpointParam(e, keyz, valz, msg, msg_size);
            break;
        default:
            rv = PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND;
        }

        return rv;
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
            return sendResponse(response, NULL, 0, NULL, 0, msg.c_str(), msg.length(),
                                PROTOCOL_BINARY_RAW_BYTES,
                                PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET, 0, cookie);
        } else {
            vbucket_state_t state = (vbucket_state_t)ntohl(vb->getState());
            return sendResponse(response, NULL, 0, NULL, 0, &state, sizeof(state),
                                PROTOCOL_BINARY_RAW_BYTES,
                                PROTOCOL_BINARY_RESPONSE_SUCCESS, 0, cookie);
        }
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
            sendResponse(response, NULL, 0, NULL, 0, msg.c_str(), msg.length(),
                         PROTOCOL_BINARY_RAW_BYTES,
                         PROTOCOL_BINARY_RESPONSE_EINVAL, 0, cookie);
        }

        vbucket_state_t state;
        memcpy(&state, &req->message.body.state, sizeof(state));
        state = static_cast<vbucket_state_t>(ntohl(state));

        if (!is_valid_vbucket_state_t(state)) {
            const std::string msg("Invalid vbucket state");
            sendResponse(response, NULL, 0, NULL, 0, msg.c_str(), msg.length(),
                         PROTOCOL_BINARY_RAW_BYTES,
                         PROTOCOL_BINARY_RESPONSE_EINVAL, 0, cookie);
        }

        uint16_t vb = ntohs(req->message.header.request.vbucket);
        if(e->setVBucketState(vb, state) == ENGINE_ERANGE) {
            const std::string msg("VBucket number too big");
            return sendResponse(response, NULL, 0, NULL, 0, msg.c_str(),
                                msg.length(), PROTOCOL_BINARY_RAW_BYTES,
                                PROTOCOL_BINARY_RESPONSE_ERANGE, 0, cookie);
        }
        return sendResponse(response, NULL, 0, NULL, 0, NULL, 0,
                            PROTOCOL_BINARY_RAW_BYTES,
                            PROTOCOL_BINARY_RESPONSE_SUCCESS, 0, cookie);
    }

    static ENGINE_ERROR_CODE delVBucket(EventuallyPersistentEngine *e,
                                        const void *cookie,
                                        protocol_binary_request_header *req,
                                        ADD_RESPONSE response) {
        std::string msg = "";
        protocol_binary_response_status res = PROTOCOL_BINARY_RESPONSE_SUCCESS;
        uint16_t vbucket = ntohs(req->request.vbucket);

        if (ntohs(req->request.keylen) > 0 || req->request.extlen > 0) {
            msg = "Incorrect packet format.";
            return sendResponse(response, NULL, 0, NULL, 0, msg.c_str(), msg.length(),
                                PROTOCOL_BINARY_RAW_BYTES,
                                PROTOCOL_BINARY_RESPONSE_EINVAL, 0, cookie);
        }

        bool sync = false;
        uint32_t bodylen = ntohl(req->request.bodylen);
        if (bodylen > 0) {
            const char* ptr = reinterpret_cast<const char*>(req->bytes) +
                sizeof(req->bytes);
            if (bodylen == 7 && strncmp(ptr, "async=0", bodylen) == 0) {
                sync = true;
            }
        }

        ENGINE_ERROR_CODE err;
        void* es = e->getEngineSpecific(cookie);
        if (sync) {
            if (es == NULL) {
                err = e->deleteVBucket(vbucket, cookie);
                e->storeEngineSpecific(cookie, e);
            } else {
                e->storeEngineSpecific(cookie, NULL);
                LOG(EXTENSION_LOG_INFO, "Completed sync deletion of vbucket %u",
                    (unsigned)vbucket);
                err = ENGINE_SUCCESS;
            }
        } else {
            err = e->deleteVBucket(vbucket);
        }
        switch (err) {
        case ENGINE_SUCCESS:
            LOG(EXTENSION_LOG_WARNING,
                "Deletion of vbucket %d was completed.", vbucket);
            break;
        case ENGINE_NOT_MY_VBUCKET:
            LOG(EXTENSION_LOG_WARNING, "Deletion of vbucket %d failed "
                "because the vbucket doesn't exist!!!", vbucket);
            msg = "Failed to delete vbucket.  Bucket not found.";
            res = PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET;
            break;
        case ENGINE_EINVAL:
            LOG(EXTENSION_LOG_WARNING, "Deletion of vbucket %d failed "
                "because the vbucket is not in a dead state\n", vbucket);
            msg = "Failed to delete vbucket.  Must be in the dead state.";
            res = PROTOCOL_BINARY_RESPONSE_EINVAL;
            break;
        case ENGINE_EWOULDBLOCK:
            LOG(EXTENSION_LOG_WARNING, "Requst to vbucket %d deletion is in"
                " EWOULDBLOCK until the database file is removed from disk",
                vbucket);
            return ENGINE_EWOULDBLOCK;
        default:
            LOG(EXTENSION_LOG_WARNING, "Deletion of vbucket %d failed "
                "because of unknown reasons\n", vbucket);
            msg = "Failed to delete vbucket.  Unknown reason.";
            res = PROTOCOL_BINARY_RESPONSE_EINTERNAL;
        }

        return sendResponse(response, NULL, 0, NULL, 0, msg.c_str(), msg.length(),
                            PROTOCOL_BINARY_RAW_BYTES, res, 0, cookie);
    }

    static ENGINE_ERROR_CODE getReplicaCmd(EventuallyPersistentEngine *e,
                                           protocol_binary_request_header *request,
                                           const void *cookie,
                                           Item **it,
                                           const char **msg,
                                           protocol_binary_response_status *res) {
        EventuallyPersistentStore *eps = e->getEpStore();
        protocol_binary_request_no_extras *req =
            (protocol_binary_request_no_extras*)request;
        int keylen = ntohs(req->message.header.request.keylen);
        uint16_t vbucket = ntohs(req->message.header.request.vbucket);
        ENGINE_ERROR_CODE error_code;
        std::string keystr(((char *)request) + sizeof(req->message.header), keylen);

        GetValue rv(eps->getReplica(keystr, vbucket, cookie, true));

        if ((error_code = rv.getStatus()) != ENGINE_SUCCESS) {
            if (error_code == ENGINE_NOT_MY_VBUCKET) {
                *msg = "That's not my bucket.";
                *res = PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET;
            } else if (error_code == ENGINE_TMPFAIL) {
                *msg = "NOT_FOUND";
                *res = PROTOCOL_BINARY_RESPONSE_KEY_ENOENT;
            } else {
                return error_code;
            }
        } else {
            *it = rv.getValue();
            *res = PROTOCOL_BINARY_RESPONSE_SUCCESS;
        }
        return ENGINE_SUCCESS;
    }

    static ENGINE_ERROR_CODE processUnknownCommand(EventuallyPersistentEngine *h,
                                                   const void* cookie,
                                                   protocol_binary_request_header *request,
                                                   ADD_RESPONSE response)
    {
        protocol_binary_response_status res = PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND;
        const char *msg = NULL;
        size_t msg_size = 0;
        Item *itm = NULL;

        EPStats &stats = h->getEpStats();
        ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;

        switch (request->request.opcode) {
        case PROTOCOL_BINARY_CMD_GET_VBUCKET:
            {
                BlockTimer timer(&stats.getVbucketCmdHisto);
                rv = getVBucket(h, cookie, request, response);
                return rv;
            }
        case PROTOCOL_BINARY_CMD_DEL_VBUCKET:
            {
                BlockTimer timer(&stats.delVbucketCmdHisto);
                rv = delVBucket(h, cookie, request, response);
                return rv;
            }
        case PROTOCOL_BINARY_CMD_SET_VBUCKET:
            {
                BlockTimer timer(&stats.setVbucketCmdHisto);
                rv = setVBucket(h, cookie, request, response);
                return rv;
            }
        case PROTOCOL_BINARY_CMD_TOUCH:
        case PROTOCOL_BINARY_CMD_GAT:
        case PROTOCOL_BINARY_CMD_GATQ:
            {
                rv = h->touch(cookie, request, response);
                return rv;
            }
        case CMD_STOP_PERSISTENCE:
            res = stopFlusher(h, &msg, &msg_size);
            break;
        case CMD_START_PERSISTENCE:
            res = startFlusher(h, &msg, &msg_size);
            break;
        case CMD_SET_PARAM:
            res = setParam(h, reinterpret_cast<protocol_binary_request_set_param*>(request),
                           &msg, &msg_size);
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
            return h->observe(cookie, request, response);
        case CMD_DEREGISTER_TAP_CLIENT:
            {
                rv = h->deregisterTapClient(cookie, request, response);
                return rv;
            }
        case CMD_RESET_REPLICATION_CHAIN:
            {
                rv = h->resetReplicationChain(cookie, request, response);
                return rv;
            }
        case CMD_CHANGE_VB_FILTER:
            {
                rv = h->changeTapVBFilter(cookie, request, response);
                return rv;
            }
        case CMD_LAST_CLOSED_CHECKPOINT:
        case CMD_CREATE_CHECKPOINT:
        case CMD_CHECKPOINT_PERSISTENCE:
            {
                rv = h->handleCheckpointCmds(cookie, request, response);
                return rv;
            }
        case CMD_GET_META:
        case CMD_GETQ_META:
            {
                rv = h->getMeta(cookie,
                                reinterpret_cast<protocol_binary_request_get_meta*>(request),
                                response);
                return rv;
            }
        case CMD_SET_WITH_META:
        case CMD_SETQ_WITH_META:
        case CMD_ADD_WITH_META:
        case CMD_ADDQ_WITH_META:
            {
                rv = h->setWithMeta(cookie,
                                    reinterpret_cast<protocol_binary_request_set_with_meta*>(request),
                                    response);
                return rv;
            }
        case CMD_DEL_WITH_META:
        case CMD_DELQ_WITH_META:
            {
                rv = h->deleteWithMeta(cookie,
                                       reinterpret_cast<protocol_binary_request_delete_with_meta*>(request),
                                       response);
                return rv;
            }
        case CMD_RETURN_META:
            {
                return h->returnMeta(cookie,
                                     reinterpret_cast<protocol_binary_request_return_meta*>(request),
                                     response);
            }
        case CMD_GET_REPLICA:
            rv = getReplicaCmd(h, request, cookie, &itm, &msg, &res);
            if (rv != ENGINE_SUCCESS) {
                return rv;
            }
            break;
        case CMD_ENABLE_TRAFFIC:
        case CMD_DISABLE_TRAFFIC:
            {
                rv = h->handleTrafficControlCmd(cookie, request, response);
                return rv;
            }
        }

        // Send a special response for getl since we don't want to send the key
        if (itm && request->request.opcode == CMD_GET_LOCKED) {
            uint32_t flags = itm->getFlags();
            rv = sendResponse(response, NULL, 0, (const void *)&flags, sizeof(uint32_t),
                              static_cast<const void *>(itm->getData()),
                              itm->getNBytes(),
                              PROTOCOL_BINARY_RAW_BYTES,
                              static_cast<uint16_t>(res), itm->getCas(),
                              cookie);
            delete itm;
        } else if (itm) {
            const std::string &key  = itm->getKey();
            uint32_t flags = itm->getFlags();
            rv = sendResponse(response, static_cast<const void *>(key.data()),
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
            rv = sendResponse(response, NULL, 0, NULL, 0,
                              msg, static_cast<uint16_t>(msg_size),
                              PROTOCOL_BINARY_RAW_BYTES,
                              static_cast<uint16_t>(res), 0, cookie);

        }
        return rv;
    }

    static ENGINE_ERROR_CODE EvpUnknownCommand(ENGINE_HANDLE* handle,
                                               const void* cookie,
                                               protocol_binary_request_header *request,
                                               ADD_RESPONSE response)
    {
        ENGINE_ERROR_CODE err_code = processUnknownCommand(getHandle(handle), cookie,
                                                           request, response);
        releaseHandle(handle);
        return err_code;
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
        ENGINE_ERROR_CODE err_code = getHandle(handle)->tapNotify(cookie, engine_specific,
                                                                  nengine, ttl, tap_flags, (uint16_t)tap_event, tap_seqno,
                                                                  key, nkey, flags, exptime, cas, data, ndata,
                                                                  vbucket);
        releaseHandle(handle);
        return err_code;
    }

    static tap_event_t EvpTapIterator(ENGINE_HANDLE* handle,
                                      const void *cookie, item **itm,
                                      void **es, uint16_t *nes, uint8_t *ttl,
                                      uint16_t *flags, uint32_t *seqno,
                                      uint16_t *vbucket) {
        uint16_t tap_event = getHandle(handle)->walkTapQueue(cookie, itm, es, nes, ttl,
                                                             flags, seqno, vbucket);
        releaseHandle(handle);
        return static_cast<tap_event_t>(tap_event);
    }

    static TAP_ITERATOR EvpGetTapIterator(ENGINE_HANDLE* handle,
                                          const void* cookie,
                                          const void* client,
                                          size_t nclient,
                                          uint32_t flags,
                                          const void* userdata,
                                          size_t nuserdata)
    {
        EventuallyPersistentEngine *h = getHandle(handle);
        TAP_ITERATOR iterator = NULL;
        {
            std::string c(static_cast<const char*>(client), nclient);
            // Figure out what we want from the userdata before adding it to the API
            // to the handle
            if (h->createTapQueue(cookie, c, flags, userdata, nuserdata)) {
                iterator = EvpTapIterator;
            }
        }
        releaseHandle(handle);
        return iterator;
    }


    static ENGINE_ERROR_CODE EvpUprStep(ENGINE_HANDLE* handle,
                                        const void* cookie,
                                        struct upr_message_producers *producers)
    {
        ENGINE_ERROR_CODE errCode;
        errCode = getHandle(handle)->uprStep(cookie, producers);
        releaseHandle(handle);
        return errCode;
    }


    static ENGINE_ERROR_CODE EvpUprOpen(ENGINE_HANDLE* handle,
                                        const void* cookie,
                                        uint32_t opaque,
                                        uint32_t seqno,
                                        uint32_t flags,
                                        void *name,
                                        uint16_t nname)
    {
        ENGINE_ERROR_CODE errCode;
        errCode = getHandle(handle)->uprOpen(cookie, opaque, seqno, flags,
                                             name, nname);
        releaseHandle(handle);
        return errCode;
    }

    static ENGINE_ERROR_CODE EvpUprAddStream(ENGINE_HANDLE* handle,
                                             const void* cookie,
                                             uint32_t opaque,
                                             uint16_t vbucket,
                                             uint32_t flags,
                                             send_stream_req stream_req)
    {
        ENGINE_ERROR_CODE errCode;
        errCode = getHandle(handle)->uprAddStream(cookie, opaque, vbucket,
                                                  flags, stream_req);
        releaseHandle(handle);
        return errCode;
    }

    static ENGINE_ERROR_CODE EvpUprCloseStream(ENGINE_HANDLE* handle,
                                               const void* cookie,
                                               uint16_t vbucket)
    {
        ENGINE_ERROR_CODE errCode;
        errCode = getHandle(handle)->uprCloseStream(cookie, vbucket);
        releaseHandle(handle);
        return errCode;
    }


    static ENGINE_ERROR_CODE EvpUprStreamReq(ENGINE_HANDLE* handle,
                                             const void* cookie,
                                             uint32_t flags,
                                             uint32_t opaque,
                                             uint16_t vbucket,
                                             uint64_t startSeqno,
                                             uint64_t endSeqno,
                                             uint64_t vbucketUuid,
                                             uint64_t highSeqno,
                                             uint64_t *rollbackSeqno)
    {
        ENGINE_ERROR_CODE errCode;
        errCode = getHandle(handle)->uprStreamReq(cookie, flags, opaque,
                                                  vbucket, startSeqno,
                                                  endSeqno, vbucketUuid,
                                                  highSeqno, rollbackSeqno);
        releaseHandle(handle);
        return errCode;
    }

    static ENGINE_ERROR_CODE EvpUprGetFailoverLog(ENGINE_HANDLE* handle,
                                                  const void* cookie,
                                                  uint32_t opaque,
                                                  uint16_t vbucket,
                                                  upr_add_failover_log callback)
    {
        ENGINE_ERROR_CODE errCode;
        errCode = getHandle(handle)->uprGetFailoverLog(cookie, opaque, vbucket,
                                                       callback);
        releaseHandle(handle);
        return errCode;
    }


    static ENGINE_ERROR_CODE EvpUprStreamEnd(ENGINE_HANDLE* handle,
                                             const void* cookie,
                                             uint32_t opaque,
                                             uint16_t vbucket,
                                             uint32_t flags)
    {
        ENGINE_ERROR_CODE errCode;
        errCode = getHandle(handle)->uprStreamEnd(cookie, opaque,
                                                  vbucket, flags);
        releaseHandle(handle);
        return errCode;
    }


    static ENGINE_ERROR_CODE EvpUprSnapshotMarker(ENGINE_HANDLE* handle,
                                                  const void* cookie,
                                                  uint32_t opaque,
                                                  uint16_t vbucket)
    {
        ENGINE_ERROR_CODE errCode;
        errCode = getHandle(handle)->uprSnapshotMarker(cookie, opaque, vbucket);
        releaseHandle(handle);
        return errCode;
    }

    static ENGINE_ERROR_CODE EvpUprMutation(ENGINE_HANDLE* handle,
                                            const void* cookie,
                                            uint32_t opaque,
                                            const void *key,
                                            uint16_t nkey,
                                            const void *value,
                                            uint32_t nvalue,
                                            uint64_t cas,
                                            uint16_t vbucket,
                                            uint32_t flags,
                                            uint8_t datatype,
                                            uint64_t bySeqno,
                                            uint64_t revSeqno,
                                            uint32_t expiration,
                                            uint32_t lockTime)
    {
        ENGINE_ERROR_CODE errCode;
        errCode = getHandle(handle)->uprMutation(cookie, opaque, key, nkey,
                                                 value, nvalue, cas, vbucket,
                                                 flags, datatype,
                                                 bySeqno, revSeqno, expiration,
                                                 lockTime);
        releaseHandle(handle);
        return errCode;
    }

    static ENGINE_ERROR_CODE EvpUprDeletion(ENGINE_HANDLE* handle,
                                            const void* cookie,
                                            uint32_t opaque,
                                            const void *key,
                                            uint16_t nkey,
                                            uint64_t cas,
                                            uint16_t vbucket,
                                            uint64_t bySeqno,
                                            uint64_t revSeqno)
    {
        ENGINE_ERROR_CODE errCode;
        errCode = getHandle(handle)->uprDeletion(cookie, opaque, key, nkey, cas,
                                                 vbucket, bySeqno, revSeqno);
        releaseHandle(handle);
        return errCode;
    }

    static ENGINE_ERROR_CODE EvpUprExpiration(ENGINE_HANDLE* handle,
                                              const void* cookie,
                                              uint32_t opaque,
                                              const void *key,
                                              uint16_t nkey,
                                              uint64_t cas,
                                              uint16_t vbucket,
                                              uint64_t bySeqno,
                                              uint64_t revSeqno)
    {
        ENGINE_ERROR_CODE errCode;
        errCode = getHandle(handle)->uprExpiration(cookie, opaque, key, nkey,
                                                   cas, vbucket, bySeqno,
                                                   revSeqno);
        releaseHandle(handle);
        return errCode;
    }

    static ENGINE_ERROR_CODE EvpUprFlush(ENGINE_HANDLE* handle,
                                         const void* cookie,
                                         uint32_t opaque,
                                         uint16_t vbucket)
    {
        ENGINE_ERROR_CODE errCode;
        errCode = getHandle(handle)->uprFlush(cookie, opaque, vbucket);
        releaseHandle(handle);
        return errCode;
    }

    static ENGINE_ERROR_CODE EvpUprSetVbucketState(ENGINE_HANDLE* handle,
                                                   const void* cookie,
                                                   uint32_t opaque,
                                                   uint16_t vbucket,
                                                   vbucket_state_t state)
    {
        ENGINE_ERROR_CODE errCode;
        errCode = getHandle(handle)->uprSetVbucketState(cookie, opaque,
                                                        vbucket, state);
        releaseHandle(handle);
        return errCode;
    }

    static ENGINE_ERROR_CODE EvpUprResponseHandler(ENGINE_HANDLE* handle,
                                                   const void* cookie,
                                                   protocol_binary_response_header *response)
    {
        ENGINE_ERROR_CODE errCode;
        errCode = getHandle(handle)->uprResponseHandler(cookie, response);
        releaseHandle(handle);
        return errCode;
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
        releaseHandle(static_cast<ENGINE_HANDLE*>(c));
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

        hooksApi = api->alloc_hooks;
        loggerApi = api->log;
        MemoryTracker::getInstance();

        Atomic<size_t>* inital_tracking = new Atomic<size_t>();

        ObjectRegistry::setStats(inital_tracking);
        EventuallyPersistentEngine *engine;
        engine = new struct EventuallyPersistentEngine(get_server_api);
        ObjectRegistry::setStats(NULL);

        if (engine == NULL) {
            return ENGINE_ENOMEM;
        }

        if (MemoryTracker::trackingMemoryAllocations()) {
            engine->getEpStats().memoryTrackerEnabled.set(true);
            engine->getEpStats().totalMemory.set(inital_tracking->get());
        }
        delete inital_tracking;

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

void LOG(EXTENSION_LOG_LEVEL severity, const char *fmt, ...) {
    char buffer[2048];

    if (loggerApi != NULL) {
        EXTENSION_LOGGER_DESCRIPTOR* logger =
            (EXTENSION_LOGGER_DESCRIPTOR*)loggerApi->get_logger();
        EventuallyPersistentEngine* engine = ObjectRegistry::getCurrentEngine();

        if (loggerApi->get_level() <= severity) {
            va_list va;
            va_start(va, fmt);
            vsnprintf(buffer, sizeof(buffer) - 1, fmt, va);
            if (engine) {
                logger->log(severity, NULL, "(%s) %s", engine->getName(), buffer);
            } else {
                logger->log(severity, NULL, "(No Engine) %s", buffer);
            }
            va_end(va);
        }
    }
}

ALLOCATOR_HOOKS_API *getHooksApi(void) {
    return hooksApi;
}

EventuallyPersistentEngine::EventuallyPersistentEngine(GET_SERVER_API get_server_api) :
    epstore(NULL), workload(NULL), workloadPriority(NO_BUCKET_PRIORITY),
    tapThrottle(NULL), startedEngineThreads(false),
    getServerApiFunc(get_server_api), tapConnMap(NULL), tapConfig(NULL),
    checkpointConfig(NULL),
    flushAllEnabled(false), startupTime(0)
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


    ENGINE_HANDLE_V1::upr.step = EvpUprStep;
    ENGINE_HANDLE_V1::upr.open = EvpUprOpen;
    ENGINE_HANDLE_V1::upr.add_stream = EvpUprAddStream;
    ENGINE_HANDLE_V1::upr.close_stream = EvpUprCloseStream;
    ENGINE_HANDLE_V1::upr.get_failover_log = EvpUprGetFailoverLog;
    ENGINE_HANDLE_V1::upr.stream_req = EvpUprStreamReq;
    ENGINE_HANDLE_V1::upr.stream_end = EvpUprStreamEnd;
    ENGINE_HANDLE_V1::upr.snapshot_marker = EvpUprSnapshotMarker;
    ENGINE_HANDLE_V1::upr.mutation = EvpUprMutation;
    ENGINE_HANDLE_V1::upr.deletion = EvpUprDeletion;
    ENGINE_HANDLE_V1::upr.expiration = EvpUprExpiration;
    ENGINE_HANDLE_V1::upr.flush = EvpUprFlush;
    ENGINE_HANDLE_V1::upr.set_vbucket_state = EvpUprSetVbucketState;
    ENGINE_HANDLE_V1::upr.response_handler = EvpUprResponseHandler;

    serverApi = getServerApiFunc();
    memset(&info, 0, sizeof(info));
    info.info.description = "EP engine v" VERSION;
    info.info.features[info.info.num_features++].feature = ENGINE_FEATURE_CAS;
    info.info.features[info.info.num_features++].feature = ENGINE_FEATURE_PERSISTENT_STORAGE;
    info.info.features[info.info.num_features++].feature = ENGINE_FEATURE_LRU;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::reserveCookie(const void *cookie) {
    EventuallyPersistentEngine *epe = ObjectRegistry::onSwitchThread(NULL, true);
    ENGINE_ERROR_CODE rv = serverApi->cookie->reserve(cookie);
    ObjectRegistry::onSwitchThread(epe);
    return rv;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::releaseCookie(const void *cookie) {
    EventuallyPersistentEngine *epe = ObjectRegistry::onSwitchThread(NULL, true);
    ENGINE_ERROR_CODE rv = serverApi->cookie->release(cookie);
    ObjectRegistry::onSwitchThread(epe);
    return rv;
}

void EventuallyPersistentEngine::storeEngineSpecific(const void *cookie,
                                                     void *engine_data) {
    EventuallyPersistentEngine *epe = ObjectRegistry::onSwitchThread(NULL, true);
    serverApi->cookie->store_engine_specific(cookie, engine_data);
    ObjectRegistry::onSwitchThread(epe);
}

void *EventuallyPersistentEngine::getEngineSpecific(const void *cookie) {
    EventuallyPersistentEngine *epe = ObjectRegistry::onSwitchThread(NULL, true);
    void *engine_data = serverApi->cookie->get_engine_specific(cookie);
    ObjectRegistry::onSwitchThread(epe);
    return engine_data;
}

void EventuallyPersistentEngine::registerEngineCallback(ENGINE_EVENT_TYPE type,
                                                        EVENT_CALLBACK cb,
                                                        const void *cb_data) {
    EventuallyPersistentEngine *epe = ObjectRegistry::onSwitchThread(NULL, true);
    SERVER_CALLBACK_API *sapi = getServerApi()->callback;
    sapi->register_callback(reinterpret_cast<ENGINE_HANDLE*>(this),
                            type, cb, cb_data);
    ObjectRegistry::onSwitchThread(epe);
}

/**
 * A configuration value changed listener that responds to ep-engine
 * parameter changes by invoking engine-specific methods on
 * configuration change events.
 */
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

    virtual void booleanValueChanged(const std::string &key, bool value) {
        if (key.compare("flushall_enabled") == 0) {
            engine.setFlushAll(value);
        }
    }
private:
    EventuallyPersistentEngine &engine;
};



ENGINE_ERROR_CODE EventuallyPersistentEngine::initialize(const char* config) {
    resetStats();
    if (config != NULL) {
        if (!configuration.parseConfiguration(config, serverApi)) {
            return ENGINE_FAILED;
        }
    }

    name = configuration.getCouchBucket();

    // Start updating the variables from the config!
    HashTable::setDefaultNumBuckets(configuration.getHtSize());
    HashTable::setDefaultNumLocks(configuration.getHtLocks());
    StoredValue::setMutationMemoryThreshold(configuration.getMutationMemThreshold());

    if (configuration.getMaxSize() == 0) {
        configuration.setMaxSize(std::numeric_limits<size_t>::max());
    }

    if (configuration.getMemLowWat() == std::numeric_limits<size_t>::max()) {
        configuration.setMemLowWat(percentOf(configuration.getMaxSize(), 0.75));
    }

    if (configuration.getMemHighWat() == std::numeric_limits<size_t>::max()) {
        configuration.setMemHighWat(percentOf(configuration.getMaxSize(), 0.85));
    }

    maxItemSize = configuration.getMaxItemSize();
    configuration.addValueChangedListener("max_item_size",
                                          new EpEngineValueChangeListener(*this));

    getlDefaultTimeout = configuration.getGetlDefaultTimeout();
    configuration.addValueChangedListener("getl_default_timeout",
                                          new EpEngineValueChangeListener(*this));
    getlMaxTimeout = configuration.getGetlMaxTimeout();
    configuration.addValueChangedListener("getl_max_timeout",
                                          new EpEngineValueChangeListener(*this));

    flushAllEnabled = configuration.isFlushallEnabled();
    configuration.addValueChangedListener("flushall_enabled",
                                          new EpEngineValueChangeListener(*this));

    workload = new WorkLoadPolicy(configuration.getMaxNumWorkers(),
                                  configuration.getMaxNumShards());
    if ((unsigned int)workload->getNumShards() > configuration.getMaxVbuckets()) {
        LOG(EXTENSION_LOG_WARNING, "Invalid configuration: Shards must be "
            "equal or less than max number of vbuckets");
        return ENGINE_FAILED;
    }

    uprConnMap_ = new UprConnMap(*this);
    tapConnMap = new TapConnMap(*this);
    tapConfig = new TapConfig(*this);
    tapThrottle = new TapThrottle(configuration, stats);
    TapConfig::addConfigChangeListener(*this);

    checkpointConfig = new CheckpointConfig(*this);
    CheckpointConfig::addConfigChangeListener(*this);

    epstore = new EventuallyPersistentStore(*this);
    if (epstore == NULL) {
        return ENGINE_ENOMEM;
    }

    // Register the callback
    registerEngineCallback(ON_DISCONNECT, EvpHandleDisconnect, this);
    startEngineThreads();

    // Complete the initialization of the ep-store
    if (!epstore->initialize()) {
        return ENGINE_FAILED;
    }

    if(configuration.isDataTrafficEnabled()) {
        enableTraffic(true);
    }

    tapConnMap->initialize();
    uprConnMap_->initialize();

    // record engine initialization time
    startupTime = ep_real_time();

    LOG(EXTENSION_LOG_DEBUG, "Engine init complete.\n");

    return ENGINE_SUCCESS;
}

void EventuallyPersistentEngine::destroy(bool force) {
    stats.forceShutdown = force;
    stopEngineThreads();
    if (epstore) {
        epstore->snapshotStats();
    }
    if (tapConnMap) {
        tapConnMap->shutdownAllTapConnections();
    }
}

/// @cond DETAILS
class AllFlusher : public DispatcherCallback {
public:
    AllFlusher(EventuallyPersistentStore *st, TapConnMap &tcm)
        : epstore(st), tapConnMap(tcm) { }
    bool callback(Dispatcher &, TaskId &) {
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

    if (!flushAllEnabled) {
        return ENGINE_ENOTSUP;
    }

    if (isDegradedMode()) {
        return ENGINE_TMPFAIL;
    }

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
        if (isDegradedMode()) {
            return ENGINE_TMPFAIL;
        }
        ret = epstore->set(*it, cookie);
        if (ret == ENGINE_SUCCESS) {
            *cas = it->getCas();
        }

        break;

    case OPERATION_ADD:
        if (isDegradedMode()) {
            return ENGINE_TMPFAIL;
        }

        if (it->getCas() != 0) {
            // Adding an item with a cas value doesn't really make sense...
            return ENGINE_KEY_EEXISTS;
        }

        ret = epstore->add(*it, cookie);
        if (ret == ENGINE_SUCCESS) {
            *cas = it->getCas();
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
    } else if (ret == ENGINE_NOT_STORED || ret == ENGINE_NOT_MY_VBUCKET) {
        if (isDegradedMode()) {
            return ENGINE_TMPFAIL;
        }
    }

    return ret;
}

inline uint16_t EventuallyPersistentEngine::doWalkTapQueue(const void *cookie,
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

    if (connection->isTimeForNoop()) {
        LOG(EXTENSION_LOG_INFO, "%s Sending a NOOP message.\n",
            connection->logHeader());
        return TAP_NOOP;
    }

    if (connection->isSuspended() || connection->windowIsFull()) {
        LOG(EXTENSION_LOG_INFO, "%s Connection in pause state because it is in"
            " suspended state or its ack windows is full.\n",
            connection->logHeader());
        return TAP_PAUSE;
    }

    uint16_t ret = TAP_PAUSE;
    VBucketEvent ev = connection->nextVBucketHighPriority();
    if (ev.event != TAP_PAUSE) {
        switch (ev.event) {
        case TAP_VBUCKET_SET:
            LOG(EXTENSION_LOG_WARNING,
                "%s Sending TAP_VBUCKET_SET with vbucket %d and state \"%s\"\n",
                connection->logHeader(), ev.vbucket,
                VBucket::toString(ev.state));
            connection->encodeVBucketStateTransition(ev, es, nes, vbucket);
            break;
        case TAP_OPAQUE:
            LOG(EXTENSION_LOG_WARNING,
                "%s Sending TAP_OPAQUE with command \"%s\" and vbucket %d\n",
                connection->logHeader(),
                ConnHandler::opaqueCmdToString(ntohl((uint32_t) ev.state)),
                ev.vbucket);
            connection->opaqueCommandCode = (uint32_t) ev.state;
            *vbucket = ev.vbucket;
            *es = &connection->opaqueCommandCode;
            *nes = sizeof(connection->opaqueCommandCode);
            break;
        default:
            LOG(EXTENSION_LOG_WARNING,
                "%s Unknown VBucketEvent message type %d\n",
                connection->logHeader(), ev.event);
            abort();
        }
        return ev.event;
    }

    if (connection->waitForOpaqueMsgAck()) {
        return TAP_PAUSE;
    }

    VBucketFilter backFillVBFilter;
    if (connection->runBackfill(backFillVBFilter)) {
        queueBackfill(backFillVBFilter, connection);
    }

    uint8_t nru = INITIAL_NRU_VALUE;
    Item *it = connection->getNextItem(cookie, vbucket, ret, nru);
    switch (ret) {
    case TAP_CHECKPOINT_START:
    case TAP_CHECKPOINT_END:
    case TAP_MUTATION:
    case TAP_DELETION:
        *itm = it;
        if (ret == TAP_MUTATION) {
            *nes = TapEngineSpecific::packSpecificData(ret, connection,
                                                       it->getRevSeqno(), nru);
            *es = connection->specificData;
        } else if (ret == TAP_DELETION) {
            *nes = TapEngineSpecific::packSpecificData(ret, connection,
                                                       it->getRevSeqno());
            *es = connection->specificData;
        } else if (ret == TAP_CHECKPOINT_START) {
            // Send the current value of the max deleted seqno
            RCPtr<VBucket> vb = getVBucket(*vbucket);
            if (!vb) {
                retry = true;
                return TAP_NOOP;
            }
            *nes = TapEngineSpecific::packSpecificData(ret, connection,
                                                       vb->ht.getMaxDeletedRevSeqno());
            *es = connection->specificData;
        }
        break;
    case TAP_NOOP:
        retry = true;
        break;
    default:
        break;
    }

    if (ret == TAP_PAUSE && (connection->dumpQueue || connection->doTakeOver)) {
        VBucketEvent vbev = connection->checkDumpOrTakeOverCompletion();
        if (vbev.event == TAP_VBUCKET_SET) {
            LOG(EXTENSION_LOG_WARNING,
                "%s Sending TAP_VBUCKET_SET with vbucket %d and state \"%s\"\n",
                connection->logHeader(), vbev.vbucket,
                VBucket::toString(vbev.state));
            connection->encodeVBucketStateTransition(vbev, es, nes, vbucket);
        }
        ret = vbev.event;
    }

    return ret;
}

uint16_t EventuallyPersistentEngine::walkTapQueue(const void *cookie,
                                                  item **itm,
                                                  void **es,
                                                  uint16_t *nes,
                                                  uint8_t *ttl,
                                                  uint16_t *flags,
                                                  uint32_t *seqno,
                                                  uint16_t *vbucket) {
    TapProducer *connection = getTapProducer(cookie);
    if (!connection) {
        LOG(EXTENSION_LOG_WARNING,
            "Failed to lookup TAP connection.. Disconnecting\n");
        return TAP_DISCONNECT;
    }

    connection->paused.set(false);

    bool retry = false;
    uint16_t ret;

    connection->lastWalkTime = ep_current_time();
    do {
        ret = doWalkTapQueue(cookie, itm, es, nes, ttl, flags,
                             seqno, vbucket, connection, retry);
    } while (retry);

    if (ret != TAP_PAUSE && ret != TAP_DISCONNECT) {
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

            if (ret == TAP_MUTATION) {
                if (connection->haveFlagByteorderSupport()) {
                    *flags |= TAP_FLAG_NETWORK_BYTE_ORDER;
                }
            }
        }
    } else {
        connection->paused.set(true);
        connection->notifySent.set(false);
    }

    return ret;
}

bool EventuallyPersistentEngine::createTapQueue(const void *cookie,
                                                std::string &client,
                                                uint32_t flags,
                                                const void *userdata,
                                                size_t nuserdata)
{
    if (reserveCookie(cookie) != ENGINE_SUCCESS) {
        return false;
    }

    std::string tapName = "eq_tapq:";
    if (client.length() == 0) {
        tapName.assign(ConnHandler::getAnonName());
    } else {
        tapName.append(client);
    }

    // Decoding the userdata section of the packet and update the filters
    const char *ptr = static_cast<const char*>(userdata);
    uint64_t backfillAge = 0;
    std::vector<uint16_t> vbuckets;
    std::map<uint16_t, uint64_t> lastCheckpointIds;

    if (flags & TAP_CONNECT_FLAG_BACKFILL) { /* */
        if (nuserdata < sizeof(backfillAge)) {
            LOG(EXTENSION_LOG_WARNING,
                "Backfill age is missing. Reject connection request from %s\n",
                tapName.c_str());
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
            LOG(EXTENSION_LOG_WARNING,
                "Number of vbuckets is missing. Reject connection request from %s\n",
                tapName.c_str());
            return false;
        }
        memcpy(&nvbuckets, ptr, sizeof(nvbuckets));
        nuserdata -= sizeof(nvbuckets);
        ptr += sizeof(nvbuckets);
        nvbuckets = ntohs(nvbuckets);
        if (nvbuckets > 0) {
            if (nuserdata < (sizeof(uint16_t) * nvbuckets)) {
                LOG(EXTENSION_LOG_WARNING,
                    "# of vbuckets not matched. Reject connection request from %s\n",
                    tapName.c_str());
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
                LOG(EXTENSION_LOG_WARNING, "# of checkpoint Ids not matched. "
                    "Reject connection request from %s\n", tapName.c_str());
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

    tapConnMap->newProducer(cookie, tapName, flags,
                            backfillAge,
                            static_cast<int>(configuration.getTapKeepalive()),
                            vbuckets,
                            lastCheckpointIds);

    tapConnMap->notify();
    return true;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::tapNotify(const void *cookie,
                                                        void *engine_specific,
                                                        uint16_t nengine,
                                                        uint8_t ttl,
                                                        uint16_t tap_flags,
                                                        uint16_t tap_event,
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
    (void) ttl;
    void *specific = getEngineSpecific(cookie);
    ConnHandler *connection = NULL;
    if (specific == NULL) {
        if (tap_event == TAP_ACK) {
            LOG(EXTENSION_LOG_WARNING, "Tap producer with cookie %s does not "
                "exist. Force disconnect...\n", (char *) cookie);
            // tap producer is no longer connected..
            return ENGINE_DISCONNECT;
        } else {
            // Create a new tap consumer...
            connection = tapConnMap->newConsumer(cookie);
            if (connection == NULL) {
                LOG(EXTENSION_LOG_WARNING, "Failed to create new tap consumer. "
                    "Force disconnect\n");
                return ENGINE_DISCONNECT;
            }
            storeEngineSpecific(cookie, connection);
        }
    } else {
        connection = reinterpret_cast<ConnHandler *>(specific);
    }

    std::string k(static_cast<const char*>(key), nkey);
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    if (tap_event == TAP_MUTATION || tap_event == TAP_DELETION) {
        if (!tapThrottle->shouldProcess()) {
            ++stats.tapThrottled;
            if (connection->supportsAck()) {
                ret = ENGINE_TMPFAIL;
            } else {
                ret = ENGINE_DISCONNECT;
                LOG(EXTENSION_LOG_WARNING, "%s Can't throttle streams without "
                    "ack support. Force disconnect...\n",
                    connection->logHeader());
            }
            return ret;
        }
    }

    switch (tap_event) {
    case TAP_ACK:
        ret = processTapAck(cookie, tap_seqno, tap_flags, k);
        break;
    case TAP_FLUSH:
        ret = flush(cookie, 0);
        LOG(EXTENSION_LOG_WARNING, "%s Received flush.\n",
            connection->logHeader());
        break;
    case TAP_DELETION:
        {
            TapConsumer *tc = dynamic_cast<TapConsumer*>(connection);
            if (!tc) {
                LOG(EXTENSION_LOG_WARNING, "%s not a consumer! Force disconnect",
                    connection->logHeader());
                return ENGINE_DISCONNECT;
            }

            bool meta = false;
            ItemMetaData itemMeta(cas, DEFAULT_REV_SEQ_NUM, flags, exptime);

            if (nengine == TapEngineSpecific::sizeRevSeqno) {
                TapEngineSpecific::readSpecificData(tap_event, engine_specific,
                                                    nengine,
                                                    &itemMeta.revSeqno);
                meta = true;
                if (itemMeta.cas == 0) {
                    itemMeta.cas = Item::nextCas();
                }
                if (itemMeta.revSeqno == 0) {
                    itemMeta.revSeqno = DEFAULT_REV_SEQ_NUM;
                }
            }

            ret = ConnHandlerDelete(tc, k, cookie, vbucket, meta, itemMeta);
        }
        break;

    case TAP_CHECKPOINT_START:
    case TAP_CHECKPOINT_END:
        {
            TapConsumer *tc = dynamic_cast<TapConsumer*>(connection);
            if (tc) {
                if (tap_event == TAP_CHECKPOINT_START &&
                    nengine == TapEngineSpecific::sizeRevSeqno) {
                    // Set the current value for the max deleted seqno
                    RCPtr<VBucket> vb = getVBucket(vbucket);
                    if (!vb) {
                        return ENGINE_TMPFAIL;
                    }
                    uint64_t seqnum;
                    TapEngineSpecific::readSpecificData(tap_event, engine_specific, nengine,
                                                        &seqnum);
                    vb->ht.setMaxDeletedRevSeqno(seqnum);
                }

                if (data) {
                    uint64_t checkpointId;
                    memcpy(&checkpointId, data, sizeof(checkpointId));
                    checkpointId = ntohll(checkpointId);
                    ConnHandlerCheckPoint(tc, tap_event, vbucket, checkpointId);
                }
                else {
                    ret = ENGINE_DISCONNECT;
                    LOG(EXTENSION_LOG_WARNING, "%s Checkpoint Id is missing in "
                        "CHECKPOINT messages. Force disconnect...\n",
                        connection->logHeader());
                }
            }
            else {
                ret = ENGINE_DISCONNECT;
                LOG(EXTENSION_LOG_WARNING,
                    "%s not a consumer! Force disconnect\n",
                    connection->logHeader());
            }
        }

        break;

    case TAP_MUTATION:
        {
            TapConsumer *tc = dynamic_cast<TapConsumer*>(connection);

            if (!tc) {
                LOG(EXTENSION_LOG_WARNING,
                    "%s not a consumer! Force disconnect\n",
                    connection->logHeader());
                ret = ENGINE_DISCONNECT;
            }
            else {
                bool meta = false;
                uint8_t nru = INITIAL_NRU_VALUE;
                uint64_t seqnum;
                if (nengine >= TapEngineSpecific::sizeRevSeqno) {
                    TapEngineSpecific::readSpecificData(tap_event, engine_specific, 
                                                        nengine, &seqnum, &nru);
                    meta = true;
                }
                
                ret = ConnHandlerMutate(tc, k, cookie, flags, exptime, cas,
                                        seqnum, vbucket, meta, data, ndata);
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
                LOG(EXTENSION_LOG_INFO, "%s Enable auto nack mode\n",
                    connection->logHeader());
                break;
            case TAP_OPAQUE_ENABLE_CHECKPOINT_SYNC:
                connection->setSupportCheckpointSync(true);
                LOG(EXTENSION_LOG_INFO, "%s Enable checkpoint synchronization\n",
                    connection->logHeader());
                break;
            case TAP_OPAQUE_OPEN_CHECKPOINT:
                /**
                 * This event is only received by the TAP client that wants to get mutations
                 * from closed checkpoints only. At this time, only incremental backup client
                 * receives this event so that it can close the connection and reconnect later.
                 */
                LOG(EXTENSION_LOG_INFO, "%s Beginning of checkpoint.\n",
                    connection->logHeader());
                break;
            case TAP_OPAQUE_INITIAL_VBUCKET_STREAM:
                {
                    LOG(EXTENSION_LOG_INFO,
                        "%s Backfill started for vbucket %d.\n",
                        connection->logHeader(), vbucket);
                    BlockTimer timer(&stats.tapVbucketResetHisto);
                    ret = resetVBucket(vbucket) ? ENGINE_SUCCESS : ENGINE_DISCONNECT;
                    if (ret == ENGINE_DISCONNECT) {
                        LOG(EXTENSION_LOG_WARNING,
                            "%s Failed to reset a vbucket %d. Force disconnect\n",
                            connection->logHeader(), vbucket);
                    } else {
                        LOG(EXTENSION_LOG_WARNING,
                            "%s Reset vbucket %d was completed succecssfully.\n",
                            connection->logHeader(), vbucket);
                    }

                    TapConsumer *tc = dynamic_cast<TapConsumer*>(connection);
                    if (tc) {
                        tc->setBackfillPhase(true, vbucket);
                    } else {
                        ret = ENGINE_DISCONNECT;
                        LOG(EXTENSION_LOG_WARNING,
                            "TAP consumer doesn't exists. Force disconnect\n");
                    }
                }
                break;
            case TAP_OPAQUE_CLOSE_BACKFILL:
                {
                    LOG(EXTENSION_LOG_INFO, "%s Backfill finished.\n",
                        connection->logHeader());
                    TapConsumer *tc = dynamic_cast<TapConsumer*>(connection);
                    if (tc) {
                        tc->setBackfillPhase(false, vbucket);
                    } else {
                        ret = ENGINE_DISCONNECT;
                        LOG(EXTENSION_LOG_WARNING,
                            "%s not a consumer! Force disconnect\n",
                            connection->logHeader());
                    }
                }
                break;
            case TAP_OPAQUE_CLOSE_TAP_STREAM:
                /**
                 * This event is sent by the eVBucketMigrator to notify that the source node
                 * closes the tap replication stream and switches to TAKEOVER_VBUCKETS phase.
                 * This is just an informative message and doesn't require any action.
                 */
                LOG(EXTENSION_LOG_INFO,
                    "%s Received close tap stream. Switching to takeover phase.\n",
                    connection->logHeader());
                break;
            case TAP_OPAQUE_COMPLETE_VB_FILTER_CHANGE:
                /**
                 * This opaque message is just for notifying that the source node receives
                 * change_vbucket_filter request and processes it successfully.
                 */
                LOG(EXTENSION_LOG_INFO,
                    "%s Notified that the source node changed a vbucket filter.\n",
                    connection->logHeader());
                break;
            default:
                LOG(EXTENSION_LOG_WARNING,
                    "%s Received an unknown opaque command\n",
                    connection->logHeader());
            }
        } else {
            LOG(EXTENSION_LOG_WARNING,
                "%s Received tap opaque with unknown size %d\n",
                connection->logHeader(), nengine);
        }
        break;

    case TAP_VBUCKET_SET:
        {
            BlockTimer timer(&stats.tapVbucketSetHisto);

            if (nengine != sizeof(vbucket_state_t)) {
                // illegal datasize
                LOG(EXTENSION_LOG_WARNING,
                    "%s Received TAP_VBUCKET_SET with illegal size."
                    " Force disconnect\n", connection->logHeader());
                ret = ENGINE_DISCONNECT;
                break;
            }

            vbucket_state_t state;
            memcpy(&state, engine_specific, nengine);
            state = (vbucket_state_t)ntohl(state);

            if (!is_valid_vbucket_state_t(state)) {
                LOG(EXTENSION_LOG_WARNING,
                    "%s Received an invalid vbucket state. Force disconnect\n",
                    connection->logHeader());
                ret = ENGINE_DISCONNECT;
                break;
            }

            LOG(EXTENSION_LOG_INFO,
                "%s Received TAP_VBUCKET_SET with vbucket %d and state \"%s\"\n",
                connection->logHeader(), vbucket, VBucket::toString(state));

            epstore->setVBucketState(vbucket, state);
        }
        break;

    default:
        // Unknown command
        LOG(EXTENSION_LOG_WARNING,
            "%s Recieved bad opcode, ignoring message\n",
            connection->logHeader());
    }

    connection->processedEvent(tap_event, ret);
    return ret;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::ConnHandlerDelete(Consumer *consumer,
                                                                const std::string &key,
                                                                const void *cookie,
                                                                uint16_t vbucket,
                                                                bool meta,
                                                                ItemMetaData& itemMeta)
{
    uint64_t delCas = 0;
    ENGINE_ERROR_CODE ret = epstore->deleteItem(key, &delCas, vbucket, cookie,
                                                true, meta, false, &itemMeta,
                                                consumer->isBackfillPhase(vbucket));
    if (ret == ENGINE_KEY_ENOENT) {
        ret = ENGINE_SUCCESS;
    }

    if (!consumer->supportsCheckpointSync()) {
        // If the checkpoint synchronization is not supported,
        // check if a new checkpoint should be created or not.
        consumer->checkVBOpenCheckpoint(vbucket);
    }

    return ret;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::ConnHandlerMutate(Consumer *consumer,
                                                                const std::string key,
                                                                const void *cookie,
                                                                uint32_t flags,
                                                                uint32_t exptime,
                                                                uint64_t cas,
                                                                uint32_t seqno,
                                                                uint16_t vbucket,
                                                                bool meta,
                                                                const void *data,
                                                                size_t ndata)
{
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    value_t vblob(Blob::New(static_cast<const char*>(data), ndata));
    Item *item = new Item(key, flags, exptime, vblob);
    item->setVBucketId(vbucket);
    if (meta) {
        item->setCas(cas);
        item->setRevSeqno(seqno);
    }

    uint8_t nru = INITIAL_NRU_VALUE;

    if (consumer->isBackfillPhase(vbucket)) {
        ret = epstore->addTAPBackfillItem(*item, meta, nru);
    }
    else {
        if (meta) {
            ret = epstore->setWithMeta(*item, 0, cookie, true, true, nru);
        }
        else {
            ret = epstore->set(*item, cookie, true, nru);
        }
    }

    delete item;

    if (ret == ENGINE_ENOMEM) {
        if (consumer->supportsAck()) {
            ret = ENGINE_TMPFAIL;
        }
        else {
            LOG(EXTENSION_LOG_WARNING, "%s Connection does not support "
                "tap ack'ing.. Force disconnect\n",
                consumer->logHeader());
            ret = ENGINE_DISCONNECT;
        }
    }

    if (!consumer->supportsCheckpointSync()) {
        consumer->checkVBOpenCheckpoint(vbucket);
    }

    if (ret == ENGINE_DISCONNECT) {
        LOG(EXTENSION_LOG_WARNING, "%s Failed to apply tap mutation. "
            "Force disconnect\n", consumer->logHeader());
    }

    return ret;
}


ENGINE_ERROR_CODE EventuallyPersistentEngine::ConnHandlerCheckPoint(Consumer *consumer,
                                                                    uint8_t event,
                                                                    uint16_t vbucket,
                                                                    uint64_t checkpointId)
{
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    if (consumer->processCheckpointCommand(event, vbucket, checkpointId)) {
        getEpStore()->wakeUpFlusher();
        ret = ENGINE_SUCCESS;
    }
    else {
        ret = ENGINE_DISCONNECT;
        LOG(EXTENSION_LOG_WARNING, "%s Error processing "
            "checkpoint %llu. Force disconnect\n",
            consumer->logHeader(), checkpointId);
    }

    return ret;
}

TapProducer* EventuallyPersistentEngine::getTapProducer(const void *cookie) {
    TapProducer *rv =
        reinterpret_cast<TapProducer*>(getEngineSpecific(cookie));
    if (!(rv && rv->isConnected())) {
        LOG(EXTENSION_LOG_WARNING,
            "Walking a non-existent tap queue, disconnecting\n");
        return NULL;
    }

    if (rv->doDisconnect()) {
        LOG(EXTENSION_LOG_WARNING,
            "%s Disconnecting pending connection\n", rv->logHeader());
        return NULL;
    }
    return rv;
}

UprProducer* EventuallyPersistentEngine::getUprProducer(const void *cookie) {
    UprProducer *rv =
        reinterpret_cast<UprProducer*>(getEngineSpecific(cookie));
    if (!(rv && rv->isConnected())) {
        LOG(EXTENSION_LOG_WARNING,
            "Walking a non-existent upr queue, disconnecting\n");
        return NULL;
    }

    if (rv->doDisconnect()) {
        LOG(EXTENSION_LOG_WARNING,
            "%s Disconnecting pending connection\n", rv->logHeader());
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
        LOG(EXTENSION_LOG_WARNING,
            "Unable to process tap ack. No producer found\n");
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
                                               Producer *tc) 
{
    shared_ptr<DispatcherCallback> backfill_cb(new BackfillTask(this, tc,
                                                                backfillVBFilter));
    epstore->getNonIODispatcher()->schedule(backfill_cb, NULL,
                                            Priority::BackfillTaskPriority,
                                            0, false, false);
}

bool VBucketCountVisitor::visitBucket(RCPtr<VBucket> &vb) {
    ++numVbucket;
    numItems += vb->ht.getNumItems();
    numTempItems += vb->ht.getNumTempItems();
    nonResident += vb->ht.getNumNonResidentItems();

    if (vb->getHighPriorityChkSize() > 0) {
        chkPersistRemaining++;
    }

    if (desired_state != vbucket_state_dead) {
        htMemory += vb->ht.memorySize();
        htItemMemory += vb->ht.getItemMemory();
        htCacheSize += vb->ht.cacheSize;
        numEjects += vb->ht.getNumEjects();
        numExpiredItems += vb->numExpiredItems;
        metaDataMemory += vb->ht.metaDataMemory;
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

    epstore->updateCachedResidentRatio(activeCountVisitor.getMemResidentPer(),
                                       replicaCountVisitor.getMemResidentPer());
    tapThrottle->adjustWriteQueueCap(activeCountVisitor.getNumItems() +
                                     replicaCountVisitor.getNumItems() +
                                     pendingCountVisitor.getNumItems());

    configuration.addStats(add_stat, cookie);

    EPStats &epstats = getEpStats();
    add_casted_stat("ep_version", VERSION, add_stat, cookie);
    add_casted_stat("ep_storage_age",
                    epstats.dirtyAge, add_stat, cookie);
    add_casted_stat("ep_storage_age_highwat",
                    epstats.dirtyAgeHighWat, add_stat, cookie);
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
    add_casted_stat("ep_expired_access", epstats.expired_access,
                    add_stat, cookie);
    add_casted_stat("ep_expired_pager", epstats.expired_pager,
                    add_stat, cookie);
    add_casted_stat("ep_item_flush_expired",
                    epstats.flushExpired, add_stat, cookie);
    add_casted_stat("ep_queue_size",
                    epstats.diskQueueSize, add_stat, cookie);
    add_casted_stat("ep_flusher_todo",
                    epstats.flusher_todo, add_stat, cookie);
    add_casted_stat("ep_uncommitted_items",
                    epstats.flusher_todo, add_stat, cookie);
    add_casted_stat("ep_diskqueue_items",
                    epstats.diskQueueSize, add_stat, cookie);
    add_casted_stat("ep_flusher_state",
                    epstore->getFlusher(0)->stateName(),
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
    add_casted_stat("ep_flush_duration_total",
                    epstats.cumulativeFlushTime, add_stat, cookie);
    add_casted_stat("ep_flush_all",
                    epstore->isFlushAllScheduled() ? "true" : "false", add_stat, cookie);
    add_casted_stat("curr_items", activeCountVisitor.getNumItems(), add_stat, cookie);
    add_casted_stat("curr_temp_items", activeCountVisitor.getNumTempItems(), add_stat, cookie);
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
    add_casted_stat("vb_active_expired", activeCountVisitor.getExpired(), add_stat, cookie);
    add_casted_stat("vb_active_meta_data_memory", activeCountVisitor.getMetaDataMemory(),
                    add_stat, cookie);
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
    add_casted_stat("vb_replica_expired", replicaCountVisitor.getExpired(), add_stat, cookie);
    add_casted_stat("vb_replica_meta_data_memory", replicaCountVisitor.getMetaDataMemory(),
                    add_stat, cookie);
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
    add_casted_stat("vb_pending_expired", pendingCountVisitor.getExpired(), add_stat, cookie);
    add_casted_stat("vb_pending_meta_data_memory", pendingCountVisitor.getMetaDataMemory(),
                    add_stat, cookie);
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

    add_casted_stat("ep_vb_snapshot_total", epstats.snapshotVbucketHisto.total(),
                    add_stat, cookie);

    add_casted_stat("ep_vb_total",
                    activeCountVisitor.getVBucketNumber() +
                    replicaCountVisitor.getVBucketNumber() +
                    pendingCountVisitor.getVBucketNumber() +
                    deadCountVisitor.getVBucketNumber(),
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
    add_casted_stat("ep_meta_data_memory",
                    activeCountVisitor.getMetaDataMemory() +
                    replicaCountVisitor.getMetaDataMemory() +
                    pendingCountVisitor.getMetaDataMemory(),
                    add_stat, cookie);

    size_t memUsed =  stats.getTotalMemoryUsed();
    add_casted_stat("mem_used", memUsed, add_stat, cookie);
    add_casted_stat("bytes", memUsed, add_stat, cookie);
    add_casted_stat("ep_kv_size", stats.currentSize, add_stat, cookie);
    add_casted_stat("ep_value_size", stats.totalValueSize, add_stat, cookie);
    add_casted_stat("ep_overhead", stats.memOverhead, add_stat, cookie);
    add_casted_stat("ep_total_cache_size",
                    activeCountVisitor.getCacheSize() +
                    replicaCountVisitor.getCacheSize() +
                    pendingCountVisitor.getCacheSize(),
                    add_stat, cookie);
    add_casted_stat("ep_oom_errors", stats.oom_errors, add_stat, cookie);
    add_casted_stat("ep_tmp_oom_errors", stats.tmp_oom_errors, add_stat, cookie);
    add_casted_stat("ep_mem_tracker_enabled",
                    stats.memoryTrackerEnabled ? "true" : "false",
                    add_stat, cookie);
    add_casted_stat("ep_bg_fetched", epstats.bg_fetched, add_stat,
                    cookie);
    add_casted_stat("ep_bg_meta_fetched", epstats.bg_meta_fetched, add_stat,
                    cookie);
    add_casted_stat("ep_bg_remaining_jobs", epstats.numRemainingBgJobs,
                    add_stat, cookie);
    add_casted_stat("ep_max_bg_remaining_jobs", epstats.maxRemainingBgJobs,
                    add_stat, cookie);
    add_casted_stat("ep_tap_bg_fetched", stats.numTapBGFetched, add_stat, cookie);
    add_casted_stat("ep_tap_bg_fetch_requeued", stats.numTapBGFetchRequeued,
                    add_stat, cookie);
    add_casted_stat("ep_num_pager_runs", epstats.pagerRuns, add_stat,
                    cookie);
    add_casted_stat("ep_num_expiry_pager_runs", epstats.expiryPagerRuns, add_stat,
                    cookie);
    add_casted_stat("ep_items_rm_from_checkpoints", epstats.itemsRemovedFromCheckpoints,
                    add_stat, cookie);
    add_casted_stat("ep_num_value_ejects", epstats.numValueEjects, add_stat,
                    cookie);
    add_casted_stat("ep_num_eject_failures", epstats.numFailedEjects, add_stat,
                    cookie);
    add_casted_stat("ep_num_not_my_vbuckets", epstats.numNotMyVBuckets, add_stat,
                    cookie);

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

    size_t vbDeletions = epstats.vbucketDeletions.get();
    if (vbDeletions > 0) {
        add_casted_stat("ep_vbucket_del_max_walltime",
                        epstats.vbucketDelMaxWalltime,
                        add_stat, cookie);
        add_casted_stat("ep_vbucket_del_avg_walltime",
                        epstats.vbucketDelTotWalltime / vbDeletions,
                        add_stat, cookie);
    }

    size_t numBgOps = epstats.bgNumOperations.get();
    if (numBgOps > 0) {
        add_casted_stat("ep_bg_num_samples", epstats.bgNumOperations, add_stat, cookie);
        add_casted_stat("ep_bg_min_wait",
                        epstats.bgMinWait,
                        add_stat, cookie);
        add_casted_stat("ep_bg_max_wait",
                        epstats.bgMaxWait,
                        add_stat, cookie);
        add_casted_stat("ep_bg_wait_avg",
                        epstats.bgWait / numBgOps,
                        add_stat, cookie);
        add_casted_stat("ep_bg_min_load",
                        epstats.bgMinLoad,
                        add_stat, cookie);
        add_casted_stat("ep_bg_max_load",
                        epstats.bgMaxLoad,
                        add_stat, cookie);
        add_casted_stat("ep_bg_load_avg",
                        epstats.bgLoad / numBgOps,
                        add_stat, cookie);
        add_casted_stat("ep_bg_wait",
                        epstats.bgWait,
                        add_stat, cookie);
        add_casted_stat("ep_bg_load",
                        epstats.bgLoad,
                        add_stat, cookie);
    }

    add_casted_stat("ep_num_non_resident",
                    activeCountVisitor.getNonResident() +
                    pendingCountVisitor.getNonResident() +
                    replicaCountVisitor.getNonResident(),
                    add_stat, cookie);

    add_casted_stat("ep_degraded_mode", isDegradedMode(), add_stat, cookie);
    add_casted_stat("ep_exp_pager_stime", epstore->getExpiryPagerSleeptime(),
                    add_stat, cookie);

    add_casted_stat("ep_mlog_compactor_runs", epstats.mlogCompactorRuns,
                    add_stat, cookie);
    add_casted_stat("ep_num_access_scanner_runs", epstats.alogRuns,
                    add_stat, cookie);
    add_casted_stat("ep_access_scanner_last_runtime", epstats.alogRuntime,
                    add_stat, cookie);
    add_casted_stat("ep_access_scanner_num_items", epstats.alogNumItems,
                    add_stat, cookie);

    char timestr[20];
    struct tm alogTim = *gmtime((time_t *)&epstats.alogTime);
    strftime(timestr, 20, "%Y-%m-%d %H:%M:%S", &alogTim);
    add_casted_stat("ep_access_scanner_task_time", timestr, add_stat, cookie);

    add_casted_stat("ep_startup_time", startupTime, add_stat, cookie);

    if (getConfiguration().isWarmup()) {
        Warmup *wp = epstore->getWarmup();
        assert(wp);
        if (epstats.warmupComplete) {
            add_casted_stat("ep_warmup_thread", "complete", add_stat, cookie);
        } else {
            add_casted_stat("ep_warmup_thread", "running", add_stat, cookie);
        }
        if (wp->getTime() > 0) {
            add_casted_stat("ep_warmup_time", wp->getTime() / 1000, add_stat, cookie);
        }
        add_casted_stat("ep_warmup_oom", epstats.warmOOM, add_stat, cookie);
        add_casted_stat("ep_warmup_dups", epstats.warmDups, add_stat, cookie);
    }

    add_casted_stat("ep_num_ops_get_meta", epstats.numOpsGetMeta,
                    add_stat, cookie);
    add_casted_stat("ep_num_ops_set_meta", epstats.numOpsSetMeta,
                    add_stat, cookie);
    add_casted_stat("ep_num_ops_del_meta", epstats.numOpsDelMeta,
                    add_stat, cookie);
    add_casted_stat("ep_num_ops_set_meta_res_fail",
                    epstats.numOpsSetMetaResolutionFailed, add_stat, cookie);
    add_casted_stat("ep_num_ops_del_meta_res_fail",
                    epstats.numOpsDelMetaResolutionFailed, add_stat, cookie);
    add_casted_stat("ep_num_ops_set_ret_meta", epstats.numOpsSetRetMeta,
                    add_stat, cookie);
    add_casted_stat("ep_num_ops_del_ret_meta", epstats.numOpsDelRetMeta,
                    add_stat, cookie);
    add_casted_stat("ep_num_ops_get_meta_on_set_meta",
                    epstats.numOpsGetMetaOnSetWithMeta, add_stat, cookie);
    add_casted_stat("ep_chk_persistence_timeout",
                    VBucket::getCheckpointFlushTimeout(),
                    add_stat, cookie);
    add_casted_stat("ep_chk_persistence_remains",
                    activeCountVisitor.getChkPersistRemaining() +
                    pendingCountVisitor.getChkPersistRemaining() +
                    replicaCountVisitor.getChkPersistRemaining(),
                    add_stat, cookie);
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doMemoryStats(const void *cookie,
                                                            ADD_STAT add_stat) {
    add_casted_stat("bytes", stats.getTotalMemoryUsed(), add_stat, cookie);
    add_casted_stat("mem_used", stats.getTotalMemoryUsed(), add_stat, cookie);
    add_casted_stat("ep_kv_size", stats.currentSize, add_stat, cookie);
    add_casted_stat("ep_value_size", stats.totalValueSize, add_stat, cookie);
    add_casted_stat("ep_overhead", stats.memOverhead, add_stat, cookie);
    add_casted_stat("ep_max_size", stats.getMaxDataSize(), add_stat, cookie);
    add_casted_stat("ep_mem_low_wat", stats.mem_low_wat, add_stat, cookie);
    add_casted_stat("ep_mem_high_wat", stats.mem_high_wat, add_stat, cookie);
    add_casted_stat("ep_oom_errors", stats.oom_errors, add_stat, cookie);
    add_casted_stat("ep_tmp_oom_errors", stats.tmp_oom_errors, add_stat, cookie);
    add_casted_stat("ep_mem_tracker_enabled",
                    stats.memoryTrackerEnabled ? "true" : "false",
                    add_stat, cookie);

    std::map<std::string, size_t> alloc_stats;
    MemoryTracker::getInstance()->getAllocatorStats(alloc_stats);
    std::map<std::string, size_t>::iterator it = alloc_stats.begin();
    for (; it != alloc_stats.end(); ++it) {
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

class StatCheckpointVisitor : public VBucketVisitor {
public:
    StatCheckpointVisitor(EventuallyPersistentStore * eps, const void *c,
                          ADD_STAT a) : epstore(eps), cookie(c), add_stat(a) {}

    bool visitBucket(RCPtr<VBucket> &vb) {
        addCheckpointStat(cookie, add_stat, epstore, vb);
        return false;
    }

    static void addCheckpointStat(const void *cookie, ADD_STAT add_stat,
                                  EventuallyPersistentStore *eps, RCPtr<VBucket> &vb) {
        if (!vb) {
            return;
        }

        uint16_t vbid = vb->getId();
        char buf[256];
        snprintf(buf, sizeof(buf), "vb_%d:state", vbid);
        add_casted_stat(buf, VBucket::toString(vb->getState()), add_stat, cookie);
        vb->checkpointManager.addStats(add_stat, cookie);
        snprintf(buf, sizeof(buf), "vb_%d:persisted_checkpoint_id", vbid);
        add_casted_stat(buf, eps->getLastPersistedCheckpointId(vbid), add_stat, cookie);
    }

    EventuallyPersistentStore *epstore;
    const void *cookie;
    ADD_STAT add_stat;
};

class StatCheckpointCallback : public DispatcherCallback {
public:
    StatCheckpointCallback(EventuallyPersistentEngine *e, const void *c,
                           ADD_STAT a) : ep(e), cookie(c), add_stat(a) { }

    bool callback(Dispatcher &, TaskId &) {
        StatCheckpointVisitor scv(ep->getEpStore(), cookie, add_stat);
        ep->getEpStore()->visit(scv);
        ep->notifyIOComplete(cookie, ENGINE_SUCCESS);
        return false;
    }

    std::string description() {
        return "checkpoint stats for all vbuckets";
    }

private:
    EventuallyPersistentEngine *ep;
    const void *cookie;
    ADD_STAT add_stat;
};

ENGINE_ERROR_CODE EventuallyPersistentEngine::doCheckpointStats(const void *cookie,
                                                                ADD_STAT add_stat,
                                                                const char* stat_key,
                                                                int nkey) {

    if (nkey == 10) {
        void* es = getEngineSpecific(cookie);
        if (es == NULL) {
            shared_ptr<DispatcherCallback> cb(new StatCheckpointCallback(this, cookie,
                                                                         add_stat));
            epstore->getNonIODispatcher()->schedule(cb, NULL,
                                                    Priority::CheckpointStatsPriority,
                                                    0, false, false);
            storeEngineSpecific(cookie, this);
            return ENGINE_EWOULDBLOCK;
        } else {
            storeEngineSpecific(cookie, NULL);
        }
    } else if (nkey > 11) {
        std::string vbid(&stat_key[11], nkey - 11);
        uint16_t vbucket_id(0);
        if (!parseUint16(vbid.c_str(), &vbucket_id)) {
            return ENGINE_EINVAL;
        }
        RCPtr<VBucket> vb = getVBucket(vbucket_id);
        StatCheckpointVisitor::addCheckpointStat(cookie, add_stat, epstore, vb);
    }

    return ENGINE_SUCCESS;
}

/// @cond DETAILS

/**
 * Function object to send stats for a single tap connection.
 */
struct TapStatBuilder {
    TapStatBuilder(const void *c, ADD_STAT as, TapCounter* tc)
        : cookie(c), add_stat(as), aggregator(tc) {}

    void operator() (connection_t &tc) {
        ++aggregator->totalTaps;
        tc->addStats(add_stat, cookie);

        TapProducer *tp = dynamic_cast<TapProducer*>(tc.get());
        if (tp) {
            tp->aggregateQueueStats(aggregator);
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
        tp->aggregateQueueStats(tc);
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

    void operator() (connection_t &tc) {

        TapProducer *tp = dynamic_cast<TapProducer*>(tc.get());

        if (tp && tp->isConnected()) {
            TapCounter *aggregator = getTarget(tp);
            if (aggregator && tp) {
                aggregate(tp, aggregator);
            }
            if (tp) {
                aggregate(tp, getTotalCounter());
            }
        }
    }

    std::map<std::string, TapCounter*> *counters;
    const char *sep;
    size_t sep_len;
};

/// @endcond

static void showTapAggStat(const std::string &prefix,
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
    uprConnMap_->each(tapVisitor);

    add_casted_stat("ep_tap_total_fetched", stats.numTapFetched, add_stat, cookie);
    add_casted_stat("ep_tap_bg_max_pending", tapConfig->getBgMaxPending(),
                    add_stat, cookie);
    add_casted_stat("ep_tap_bg_fetched", stats.numTapBGFetched, add_stat, cookie);
    add_casted_stat("ep_tap_bg_fetch_requeued", stats.numTapBGFetchRequeued,
                    add_stat, cookie);
    add_casted_stat("ep_tap_fg_fetched", stats.numTapFGFetched, add_stat, cookie);
    add_casted_stat("ep_tap_deletes", stats.numTapDeletes, add_stat, cookie);
    add_casted_stat("ep_tap_throttled", stats.tapThrottled, add_stat, cookie);
    add_casted_stat("ep_tap_noop_interval", tapConnMap->getNoopInterval(), add_stat, cookie);
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

    if (fetchLookupResult(cookie, &it)) {
        diskItem.reset(it); // Will be null if the key was not found
        if (!validate) {
            LOG(EXTENSION_LOG_DEBUG,
                "Found lookup results for non-validating key "
                "stat call. Would have leaked\n");
            diskItem.reset();
        }
    } else if (validate) {
        rv = epstore->statsVKey(key, vbid, cookie);
        if (rv == ENGINE_NOT_MY_VBUCKET || rv == ENGINE_KEY_ENOENT) {
            if (isDegradedMode()) {
                return ENGINE_TMPFAIL;
            }
        }
        return rv;
    }

    rv = epstore->getKeyStats(key, vbid, kstats);
    if (rv == ENGINE_SUCCESS) {
        std::string valid("this_is_a_bug");
        if (validate) {
            if (kstats.dirty) {
                valid.assign("dirty");
            } else if (diskItem.get()) {
                valid.assign(epstore->validateKey(key, vbid, *diskItem));
            } else {
                valid.assign("ram_but_not_disk");
            }
            LOG(EXTENSION_LOG_DEBUG, "Key '%s' is %s\n", key.c_str(),
                valid.c_str());
        }
        add_casted_stat("key_is_dirty", kstats.dirty, add_stat, cookie);
        add_casted_stat("key_exptime", kstats.exptime, add_stat, cookie);
        add_casted_stat("key_flags", kstats.flags, add_stat, cookie);
        add_casted_stat("key_cas", kstats.cas, add_stat, cookie);
        add_casted_stat("key_vb_state", VBucket::toString(kstats.vb_state), add_stat,
                        cookie);
        if (validate) {
            add_casted_stat("key_valid", valid.c_str(), add_stat, cookie);
        }
    }
    return rv;
}


ENGINE_ERROR_CODE EventuallyPersistentEngine::doTimingStats(const void *cookie,
                                                            ADD_STAT add_stat) {
    add_casted_stat("bg_wait", stats.bgWaitHisto, add_stat, cookie);
    add_casted_stat("bg_load", stats.bgLoadHisto, add_stat, cookie);
    add_casted_stat("set_with_meta", stats.setWithMetaHisto, add_stat, cookie);
    add_casted_stat("bg_tap_wait", stats.tapBgWaitHisto, add_stat, cookie);
    add_casted_stat("bg_tap_load", stats.tapBgLoadHisto, add_stat, cookie);
    add_casted_stat("pending_ops", stats.pendingOpsHisto, add_stat, cookie);

    add_casted_stat("storage_age", stats.dirtyAgeHisto, add_stat, cookie);

    // Regular commands
    add_casted_stat("get_cmd", stats.getCmdHisto, add_stat, cookie);
    add_casted_stat("store_cmd", stats.storeCmdHisto, add_stat, cookie);
    add_casted_stat("arith_cmd", stats.arithCmdHisto, add_stat, cookie);
    add_casted_stat("get_stats_cmd", stats.getStatsCmdHisto, add_stat, cookie);
    // Admin commands
    add_casted_stat("get_vb_cmd", stats.getVbucketCmdHisto, add_stat, cookie);
    add_casted_stat("set_vb_cmd", stats.setVbucketCmdHisto, add_stat, cookie);
    add_casted_stat("del_vb_cmd", stats.delVbucketCmdHisto, add_stat, cookie);
    add_casted_stat("chk_persistence_cmd", stats.chkPersistenceHisto,
                    add_stat, cookie);
    // Tap commands
    add_casted_stat("tap_vb_set", stats.tapVbucketSetHisto, add_stat, cookie);
    add_casted_stat("tap_vb_reset", stats.tapVbucketResetHisto, add_stat, cookie);
    add_casted_stat("tap_mutation", stats.tapMutationHisto, add_stat, cookie);
    // Misc
    add_casted_stat("notify_io", stats.notifyIOHisto, add_stat, cookie);
    add_casted_stat("batch_read", stats.getMultiHisto, add_stat, cookie);

    // Disk stats
    add_casted_stat("disk_insert", stats.diskInsertHisto, add_stat, cookie);
    add_casted_stat("disk_update", stats.diskUpdateHisto, add_stat, cookie);
    add_casted_stat("disk_del", stats.diskDelHisto, add_stat, cookie);
    add_casted_stat("disk_vb_del", stats.diskVBDelHisto, add_stat, cookie);
    add_casted_stat("disk_commit", stats.diskCommitHisto, add_stat, cookie);
    add_casted_stat("disk_vbstate_snapshot", stats.snapshotVbucketHisto,
                    add_stat, cookie);

    add_casted_stat("item_alloc_sizes", stats.itemAllocSizeHisto,
                    add_stat, cookie);

    return ENGINE_SUCCESS;
}

static void showJobLog(const char *prefix, const char *logname,
                       const std::vector<JobLogEntry> &log,
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
    DispatcherState nds(epstore->getNonIODispatcher()->getDispatcherState());
    doDispatcherStat("nio_dispatcher", nds, cookie, add_stat);

    IOManager::get()->doWorkerStat(ObjectRegistry::getCurrentEngine(), cookie,
                                   add_stat);
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::doWorkloadStats(const void *cookie,
                                                              ADD_STAT add_stat) {
    char statname[80] = {0};

    int readers = workload->getNumReaders();
    snprintf(statname, sizeof(statname), "ep_workload:num_readers");
    add_casted_stat(statname, readers, add_stat, cookie);

    int writers = workload->getNumWriters();
    snprintf(statname, sizeof(statname), "ep_workload:num_writers");
    add_casted_stat(statname, writers, add_stat, cookie);

    int shards = workload->getNumShards();
    snprintf(statname, sizeof(statname), "ep_workload:num_shards");
    add_casted_stat(statname, shards, add_stat, cookie);

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::getStats(const void* cookie,
                                                       const char* stat_key,
                                                       int nkey,
                                                       ADD_STAT add_stat) {
    BlockTimer timer(&stats.getStatsCmdHisto);
    if (stat_key != NULL) {
        LOG(EXTENSION_LOG_DEBUG, "stats %s %d", stat_key, nkey);
    } else {
        LOG(EXTENSION_LOG_DEBUG, "stats engine");
    }

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
        rv = doVBucketStats(cookie, add_stat, false, false);
    } else if (nkey == 15 && strncmp(stat_key, "vbucket-details", 15) == 0) {
        rv = doVBucketStats(cookie, add_stat, false, true);
    } else if (nkey == 12 && strncmp(stat_key, "prev-vbucket", 12) == 0) {
        rv = doVBucketStats(cookie, add_stat, true, false);
    } else if (nkey >= 10 && strncmp(stat_key, "checkpoint", 10) == 0) {
        rv = doCheckpointStats(cookie, add_stat, stat_key, nkey);
    } else if (nkey == 7 && strncmp(stat_key, "timings", 7) == 0) {
        rv = doTimingStats(cookie, add_stat);
    } else if (nkey == 10 && strncmp(stat_key, "dispatcher", 10) == 0) {
        rv = doDispatcherStats(cookie, add_stat);
    } else if (nkey == 6 && strncmp(stat_key, "memory", 6) == 0) {
        rv = doMemoryStats(cookie, add_stat);
    } else if (nkey == 4 && strncmp(stat_key, "uuid", 4) == 0) {
        add_casted_stat("uuid", configuration.getUuid(), add_stat, cookie);
        rv = ENGINE_SUCCESS;
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
    } else if (nkey == 9 && strncmp(stat_key, "kvtimings", 9) == 0) {
        getEpStore()->addKVStoreTimingStats(add_stat, cookie);
        rv = ENGINE_SUCCESS;
    } else if (nkey == 7 && strncmp(stat_key, "kvstore", 7) == 0) {
        getEpStore()->addKVStoreStats(add_stat, cookie);
        rv = ENGINE_SUCCESS;
    } else if (nkey == 6 && strncmp(stat_key, "warmup", 6) == 0) {
        epstore->getWarmup()->addStats(add_stat, cookie);
        rv = ENGINE_SUCCESS;
    } else if (nkey == 4 && strncmp(stat_key, "info", 4) == 0) {
        add_casted_stat("info", get_stats_info(), add_stat, cookie);
        rv = ENGINE_SUCCESS;
    } else if (nkey == 9 && strncmp(stat_key, "allocator", 9) ==0) {
        char* buffer = (char*)malloc(sizeof(char) * 20000);
        MemoryTracker::getInstance()->getDetailedStats(buffer, 20000);
        add_casted_stat("detailed", buffer, add_stat, cookie);
        free(buffer);
        rv = ENGINE_SUCCESS;
    } else if (nkey == 6 && strncmp(stat_key, "config", 6) == 0) {
        configuration.addStats(add_stat, cookie);
        rv = ENGINE_SUCCESS;
    } else if (nkey > 15 && strncmp(stat_key, "tap-vbtakeover", 14) == 0) {
        std::string tStream;
        std::string vbid;
        std::string buffer(&stat_key[15], nkey - 15);
        std::stringstream ss(buffer);
        ss >> vbid;
        ss >> tStream;

        uint16_t vbucket_id(0);
        parseUint16(vbid.c_str(), &vbucket_id);
        rv = doTapVbTakeoverStats(cookie, add_stat, tStream, vbucket_id);
    } else if (nkey == 8 && strncmp(stat_key, "workload", 8) == 0) {
        return doWorkloadStats(cookie, add_stat);
    }

    return rv;
}

void EventuallyPersistentEngine::notifyPendingConnections(void) {
    uint32_t blurb = tapConnMap->prepareWait();
    // No need to aquire shutdown lock
    while (!stats.shutdown.isShutdown) {
        tapConnMap->notifyIOThreadMain();
        epstore->firePendingVBucketOps();

        if (stats.shutdown.isShutdown) {
            return;
        }

        blurb = tapConnMap->wait(1.0, blurb);
    }
}

void EventuallyPersistentEngine::notifyNotificationThread(void) {
    LockHolder lh(stats.shutdown.mutex);
    if (!stats.shutdown.isShutdown) {
        tapConnMap->notify();
    }
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::observe(const void* cookie,
                                                      protocol_binary_request_header *request,
                                                      ADD_RESPONSE response) {
    protocol_binary_request_no_extras *req =
        (protocol_binary_request_no_extras*)request;

    size_t offset = 0;
    const char* data = reinterpret_cast<const char*>(req->bytes) + sizeof(req->bytes);
    uint32_t data_len = ntohl(req->message.header.request.bodylen);
    std::stringstream result;

    while (offset < data_len) {
        uint16_t vb_id;
        uint16_t keylen;

        // Parse a key
        if (data_len - offset < 4) {
            std::string msg("Invalid packet structure");
            return sendResponse(response, NULL, 0, 0, 0, msg.c_str(), msg.length(),
                                PROTOCOL_BINARY_RAW_BYTES,
                                PROTOCOL_BINARY_RESPONSE_EINVAL, 0,
                                cookie);
        }

        memcpy(&vb_id, data + offset, sizeof(uint16_t));
        vb_id = ntohs(vb_id);
        offset += sizeof(uint16_t);

        memcpy(&keylen, data + offset, sizeof(uint16_t));
        keylen = ntohs(keylen);
        offset += sizeof(uint16_t);

        if (data_len - offset < keylen) {
            std::string msg("Invalid packet structure");
            return sendResponse(response, NULL, 0, 0, 0, msg.c_str(), msg.length(),
                                PROTOCOL_BINARY_RAW_BYTES,
                                PROTOCOL_BINARY_RESPONSE_EINVAL, 0,
                                cookie);
        }

        const std::string key(data + offset, keylen);
        offset += keylen;

        LOG(EXTENSION_LOG_DEBUG, "Observing key: %s, in vbucket %d.",
            key.c_str(), vb_id);

        // Get key stats
        uint16_t keystatus = 0;
        struct key_stats kstats;
        memset(&kstats, 0, sizeof(key_stats));
        ENGINE_ERROR_CODE rv = epstore->getKeyStats(key, vb_id, kstats, true);
        if (rv == ENGINE_SUCCESS) {
            if (kstats.logically_deleted) {
                keystatus = OBS_STATE_LOGICAL_DEL;
            } else if (!kstats.dirty) {
                keystatus = OBS_STATE_PERSISTED;
            } else {
                keystatus = OBS_STATE_NOT_PERSISTED;
            }
        } else if (rv == ENGINE_KEY_ENOENT) {
            keystatus = OBS_STATE_NOT_FOUND;
        } else if (rv == ENGINE_NOT_MY_VBUCKET) {
            std::string msg("Not my vbucket");
            return sendResponse(response, NULL, 0, 0, 0, msg.c_str(), msg.length(),
                                PROTOCOL_BINARY_RAW_BYTES,
                                PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET, 0,
                                cookie);
        } else {
            std::string msg("Internal error");
            return sendResponse(response, NULL, 0, 0, 0, msg.c_str(), msg.length(),
                                PROTOCOL_BINARY_RAW_BYTES,
                                PROTOCOL_BINARY_RESPONSE_EINTERNAL, 0,
                                cookie);
        }

        // Put the result into a response buffer
        vb_id = htons(vb_id);
        keylen = htons(keylen);
        uint64_t cas = htonll(kstats.cas);
        result.write((char*) &vb_id, sizeof(uint16_t));
        result.write((char*) &keylen, sizeof(uint16_t));
        result.write(key.c_str(), ntohs(keylen));
        result.write((char*) &keystatus, sizeof(uint8_t));
        result.write((char*) &cas, sizeof(uint64_t));
    }

    uint64_t persist_time = 0;
    double queue_size = static_cast<double>(stats.diskQueueSize);
    double item_trans_time = epstore->getTransactionTimePerItem();

    if (item_trans_time > 0 && queue_size > 0) {
        persist_time = static_cast<uint32_t>(queue_size * item_trans_time);
    }
    persist_time = persist_time << 32;

    return sendResponse(response, NULL, 0, 0, 0, result.str().data(),
                        result.str().length(),
                        PROTOCOL_BINARY_RAW_BYTES,
                        PROTOCOL_BINARY_RESPONSE_SUCCESS, persist_time,
                        cookie);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::touch(const void *cookie,
                                                    protocol_binary_request_header *request,
                                                    ADD_RESPONSE response)
{
    if (request->request.extlen != 4 || request->request.keylen == 0) {
        return sendResponse(response, NULL, 0, NULL, 0, NULL, 0, PROTOCOL_BINARY_RAW_BYTES,
                            PROTOCOL_BINARY_RESPONSE_EINVAL, 0, cookie);
    }

    protocol_binary_request_touch *t = reinterpret_cast<protocol_binary_request_touch*>(request);
    void *key = t->bytes + sizeof(t->bytes);
    uint32_t exptime = ntohl(t->message.body.expiration);
    uint16_t nkey = ntohs(request->request.keylen);
    uint16_t vbucket = ntohs(request->request.vbucket);

    // try to get the object
    std::string k(static_cast<const char*>(key), nkey);

    if (exptime != 0) {
        exptime = serverApi->core->abstime(serverApi->core->realtime(exptime));
    }
    GetValue gv(epstore->getAndUpdateTtl(k, vbucket, cookie,
                                         request->request.opcode != PROTOCOL_BINARY_CMD_TOUCH,
                                         (time_t)exptime));
    ENGINE_ERROR_CODE rv = gv.getStatus();
    if (rv == ENGINE_SUCCESS) {
        Item *it = gv.getValue();
        if (request->request.opcode == PROTOCOL_BINARY_CMD_TOUCH) {
            rv = sendResponse(response, NULL, 0, NULL, 0, NULL, 0,
                              PROTOCOL_BINARY_RAW_BYTES,
                              PROTOCOL_BINARY_RESPONSE_SUCCESS, it->getCas(),
                              cookie);
        } else {
            uint32_t flags = it->getFlags();
            rv = sendResponse(response, NULL, 0, &flags, sizeof(flags),
                              it->getData(), it->getNBytes(),
                              PROTOCOL_BINARY_RAW_BYTES,
                              PROTOCOL_BINARY_RESPONSE_SUCCESS, it->getCas(),
                              cookie);
        }
        delete it;
    } else if (rv == ENGINE_KEY_EEXISTS) {
        if (isDegradedMode()) {
            std::string msg("Temporary Failure");
            rv = sendResponse(response, NULL, 0, NULL, 0, msg.c_str(),
                              msg.length(), PROTOCOL_BINARY_RAW_BYTES,
                              PROTOCOL_BINARY_RESPONSE_ETMPFAIL, 0, cookie);
        } else {
            std::string msg("Lock Error");
            rv = sendResponse(response, NULL, 0, NULL, 0, msg.c_str(),
                              msg.length(), PROTOCOL_BINARY_RAW_BYTES,
                              PROTOCOL_BINARY_RESPONSE_ETMPFAIL, 0, cookie);
        }
    } else if (rv == ENGINE_KEY_ENOENT) {
        if (isDegradedMode()) {
            std::string msg("Temporary Failure");
            rv = sendResponse(response, NULL, 0, NULL, 0, msg.c_str(),
                              msg.length(), PROTOCOL_BINARY_RAW_BYTES,
                              PROTOCOL_BINARY_RESPONSE_ETMPFAIL, 0, cookie);
        } else if (request->request.opcode == PROTOCOL_BINARY_CMD_GATQ) {
            // GATQ should not return response upon cache miss
            rv = ENGINE_SUCCESS;
        } else {
            std::string msg("Not Found");
            rv = sendResponse(response, NULL, 0, NULL, 0, msg.c_str(),
                              msg.length(), PROTOCOL_BINARY_RAW_BYTES,
                              PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0, cookie);
        }
    } else if (rv == ENGINE_NOT_MY_VBUCKET) {
        if (isDegradedMode()) {
            std::string msg("Temporary Failure");
            rv = sendResponse(response, NULL, 0, NULL, 0, msg.c_str(),
                              msg.length(), PROTOCOL_BINARY_RAW_BYTES,
                              PROTOCOL_BINARY_RESPONSE_ETMPFAIL, 0, cookie);
        } else {
            std::string msg("Not My VBucket");
            rv = sendResponse(response, NULL, 0, NULL, 0, msg.c_str(),
                              msg.length(), PROTOCOL_BINARY_RAW_BYTES,
                              PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET, 0, cookie);
        }
    }

    return rv;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::deregisterTapClient(const void *cookie,
                                                                  protocol_binary_request_header *request,
                                                                  ADD_RESPONSE response)
{
    std::string tap_name = "eq_tapq:";
    std::string cName((const char*)request->bytes + sizeof(request->bytes) +
                      request->request.extlen, ntohs(request->request.keylen));
    tap_name.append(cName);

    // Close the tap connection for the registered TAP client and remove its checkpoint cursors.
    bool rv = tapConnMap->closeConnectionByName(tap_name);
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
EventuallyPersistentEngine::handleCheckpointCmds(const void *cookie,
                                                 protocol_binary_request_header *req,
                                                 ADD_RESPONSE response)
{
    std::stringstream msg;
    uint16_t vbucket = ntohs(req->request.vbucket);
    RCPtr<VBucket> vb = getVBucket(vbucket);

    if (!vb) {
        msg << "VBucket " << vbucket << " not found!!!";
        return sendResponse(response, NULL, 0, NULL, 0,
                            msg.str().c_str(), msg.str().length(),
                            PROTOCOL_BINARY_RAW_BYTES,
                            PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET, 0, cookie);
    }

    int16_t status = PROTOCOL_BINARY_RESPONSE_SUCCESS;

    switch (req->request.opcode) {
    case CMD_LAST_CLOSED_CHECKPOINT:
        {
            uint64_t checkpointId = vb->checkpointManager.getLastClosedCheckpointId();
            checkpointId = htonll(checkpointId);
            return sendResponse(response, NULL, 0, NULL, 0,
                                &checkpointId, sizeof(checkpointId),
                                PROTOCOL_BINARY_RAW_BYTES,
                                status, 0, cookie);
        }
        break;
    case CMD_CREATE_CHECKPOINT:
        if (vb->getState() != vbucket_state_active) {
            msg << "VBucket " << vbucket << " not in active state!!!";
            status = PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET;
        } else {
            uint64_t checkpointId = htonll(vb->checkpointManager.createNewCheckpoint());
            getEpStore()->wakeUpFlusher();

            uint64_t persistedChkId = htonll(epstore->getLastPersistedCheckpointId(vb->getId()));
            char val[128];
            memcpy(val, &checkpointId, sizeof(uint64_t));
            memcpy(val + sizeof(uint64_t), &persistedChkId, sizeof(uint64_t));
            return sendResponse(response, NULL, 0, NULL, 0,
                                val, sizeof(uint64_t) * 2,
                                PROTOCOL_BINARY_RAW_BYTES,
                                status, 0, cookie);
        }
        break;
    case CMD_CHECKPOINT_PERSISTENCE:
        {
            uint16_t keylen = ntohs(req->request.keylen);
            uint32_t bodylen = ntohl(req->request.bodylen);
            if ((bodylen - keylen) == 0) {
                msg << "No checkpoint id is given for CMD_CHECKPOINT_PERSISTENCE!!!";
                status = PROTOCOL_BINARY_RESPONSE_EINVAL;
            } else {
                uint64_t chk_id;
                memcpy(&chk_id, req->bytes + sizeof(req->bytes) + keylen,
                       bodylen - keylen);
                chk_id = ntohll(chk_id);
                void *es = getEngineSpecific(cookie);
                if (!es) {
                    uint16_t persisted_chk_id =
                        epstore->getVBuckets().getPersistenceCheckpointId(vbucket);
                    if (chk_id > persisted_chk_id) {
                        vb->addHighPriorityVBEntry(chk_id, cookie);
                        storeEngineSpecific(cookie, this);
                        return ENGINE_EWOULDBLOCK;
                    }
                } else {
                    storeEngineSpecific(cookie, NULL);
                    LOG(EXTENSION_LOG_INFO,
                        "Checkpoint %llu persisted for vbucket %d.",
                        chk_id, vbucket);
                }
            }
        }
        break;
    default:
        {
            msg << "Unknown checkpoint command opcode: " << req->request.opcode;
            status = PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND;
        }
    }

    return sendResponse(response, NULL, 0, NULL, 0,
                        msg.str().c_str(), msg.str().length(),
                        PROTOCOL_BINARY_RAW_BYTES,
                        status, 0, cookie);
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
    ItemMetaData metadata;
    uint32_t deleted;

    ENGINE_ERROR_CODE rv = epstore->getMetaData(key, vbucket, cookie,
                                                metadata, deleted);
    uint8_t meta[20];
    deleted = htonl(deleted);
    uint32_t flags = metadata.flags;
    uint32_t exp = htonl(metadata.exptime);
    uint64_t seqno = htonll(metadata.revSeqno);

    memcpy(meta, &deleted, 4);
    memcpy(meta + 4, &flags, 4);
    memcpy(meta + 8, &exp, 4);
    memcpy(meta + 12, &seqno, 8);

    if (rv == ENGINE_SUCCESS) {
        rv = sendResponse(response, NULL, 0, (const void *)meta,
                          20, NULL, 0,
                          PROTOCOL_BINARY_RAW_BYTES,
                          PROTOCOL_BINARY_RESPONSE_SUCCESS,
                          metadata.cas, cookie);
    } else if (rv != ENGINE_EWOULDBLOCK) {
        if (rv == ENGINE_KEY_ENOENT &&
            request->message.header.request.opcode == CMD_GETQ_META) {
            rv = ENGINE_SUCCESS;
        } else {
            rv = sendResponse(response, NULL, 0, NULL, 0, NULL, 0,
                              PROTOCOL_BINARY_RAW_BYTES,
                              engine_error_2_protocol_error(rv),
                              metadata.cas, cookie);
        }
    }

    return rv;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::setWithMeta(const void* cookie,
                                                          protocol_binary_request_set_with_meta *request,
                                                          ADD_RESPONSE response)
{
    // revid_nbytes, flags and exptime is mandatory fields.. and we need a key
    uint8_t extlen = request->message.header.request.extlen;
    uint16_t keylen = ntohs(request->message.header.request.keylen);
    if ((extlen != 24 && extlen != 28) || request->message.header.request.keylen == 0) {
        return sendResponse(response, NULL, 0, NULL, 0, NULL, 0,
                            PROTOCOL_BINARY_RAW_BYTES,
                            PROTOCOL_BINARY_RESPONSE_EINVAL, 0, cookie);
    }

    if (isDegradedMode()) {
        return sendResponse(response, NULL, 0, NULL, 0, NULL, 0,
                            PROTOCOL_BINARY_RAW_BYTES,
                            PROTOCOL_BINARY_RESPONSE_ETMPFAIL,
                            0, cookie);
    }

    uint8_t opcode = request->message.header.request.opcode;
    uint8_t *key = request->bytes + sizeof(request->bytes);
    uint16_t vbucket = ntohs(request->message.header.request.vbucket);
    uint32_t bodylen = ntohl(request->message.header.request.bodylen);
    size_t vallen = bodylen - keylen - extlen;

    if (vallen > maxItemSize) {
        LOG(EXTENSION_LOG_WARNING,
            "Item value size %ld for setWithMeta is bigger "
            "than the max size %ld allowed!!!\n", vallen, maxItemSize);
        return sendResponse(response, NULL, 0, NULL, 0, NULL, 0,
                            PROTOCOL_BINARY_RAW_BYTES,
                            PROTOCOL_BINARY_RESPONSE_E2BIG, 0, cookie);
    }

    uint32_t flags = request->message.body.flags;
    uint32_t expiration = ntohl(request->message.body.expiration);
    uint64_t seqno = ntohll(request->message.body.seqno);
    uint64_t cas = ntohll(request->message.body.cas);
    expiration = expiration == 0 ? 0 : ep_abs_time(ep_reltime(expiration));

    bool force = false;
    if (extlen == 28) {
        uint32_t options;
        memcpy(&options, request->bytes + sizeof(request->bytes),
               sizeof(options));
        key += 4;
        if (ntohl(options) & SKIP_CONFLICT_RESOLUTION_FLAG) {
            force = true;
        }
    }

    uint8_t *dta = key + keylen;
    Item *itm = new Item(key, keylen, vallen, flags, expiration, cas, -1,
                         vbucket);

    if (itm == NULL) {
        return sendResponse(response, NULL, 0, NULL, 0, NULL, 0,
                            PROTOCOL_BINARY_RAW_BYTES,
                            PROTOCOL_BINARY_RESPONSE_ENOMEM, 0, cookie);
    }

    void *startTimeC = getEngineSpecific(cookie);
    hrtime_t startTime;
    if (startTimeC) {
        startTime = *(static_cast<hrtime_t *> (startTimeC));
    } else {
        startTime = gethrtime();
    }

    itm->setRevSeqno(seqno);
    memcpy((char*)itm->getData(), dta, vallen);

    bool allowExisting = (opcode == CMD_SET_WITH_META ||
                          opcode == CMD_SETQ_WITH_META);

    ENGINE_ERROR_CODE ret = epstore->setWithMeta(*itm,
                                                 ntohll(request->message.header.request.cas),
                                                 cookie, force, allowExisting);

    if (ret == ENGINE_SUCCESS) {
        ++stats.numOpsSetMeta;
        hrtime_t endTime(gethrtime());
        hrtime_t elapsed = (endTime - startTime) / 1000;
        stats.setWithMetaHisto.add(elapsed);
    } else if (ret == ENGINE_ENOMEM) {
        ret = memoryCondition();
    } else if (ret == ENGINE_EWOULDBLOCK) {
        delete itm;
        ++stats.numOpsGetMetaOnSetWithMeta;
        if (!startTimeC) {
            startTimeC = malloc(sizeof(hrtime_t));
            memcpy(startTimeC, &startTime, sizeof(hrtime_t));
            storeEngineSpecific(cookie, startTimeC);
        }
        return ret;
    }

    protocol_binary_response_status rc;
    rc = engine_error_2_protocol_error(ret);

    if (ret == ENGINE_SUCCESS) {
        cas = itm->getCas();
    } else {
        cas = 0;
    }

    if (startTimeC) {
        free(startTimeC);
        startTimeC = NULL;
        storeEngineSpecific(cookie, startTimeC);
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
    uint16_t nkey = ntohs(request->message.header.request.keylen);
    uint8_t extlen = request->message.header.request.extlen;
    if ((extlen != 24 && extlen != 28) || nkey == 0) {
        return sendResponse(response, NULL, 0, NULL, 0, NULL, 0,
                            PROTOCOL_BINARY_RAW_BYTES,
                            PROTOCOL_BINARY_RESPONSE_EINVAL, 0, cookie);
    }

    if (isDegradedMode()) {
        return sendResponse(response, NULL, 0, NULL, 0, NULL, 0,
                            PROTOCOL_BINARY_RAW_BYTES,
                            PROTOCOL_BINARY_RESPONSE_ETMPFAIL,
                            0, cookie);
    }

    uint8_t opcode = request->message.header.request.opcode;
    const char *key_ptr = reinterpret_cast<const char*>(request->bytes);
    key_ptr += sizeof(request->bytes);
    uint16_t vbucket = ntohs(request->message.header.request.vbucket);
    uint64_t cas = ntohll(request->message.header.request.cas);

    uint32_t flags = request->message.body.flags;
    uint32_t expiration = ntohl(request->message.body.expiration);
    uint64_t seqno = ntohll(request->message.body.seqno);
    uint64_t metacas = ntohll(request->message.body.cas);
    expiration = expiration == 0 ? 0 : ep_abs_time(ep_reltime(expiration));

    bool force = false;
    if (extlen == 28) {
        uint32_t options;
        memcpy(&options, request->bytes + sizeof(request->bytes),
               sizeof(options));
        key_ptr += 4;
        if (ntohl(options) & SKIP_CONFLICT_RESOLUTION_FLAG) {
            force = true;
        }
    }
    std::string key(key_ptr, nkey);

    ItemMetaData itm_meta(metacas, seqno, flags, expiration);
    ENGINE_ERROR_CODE ret = epstore->deleteItem(key, &cas, vbucket, cookie,
                                                force, true, false, &itm_meta);
    if (ret == ENGINE_SUCCESS) {
        stats.numOpsDelMeta++;
    } else if (ret == ENGINE_ENOMEM) {
        ret = memoryCondition();
    } else if (ret == ENGINE_EWOULDBLOCK) {
        return ENGINE_EWOULDBLOCK;
    }

    protocol_binary_response_status rc;
    rc = engine_error_2_protocol_error(ret);

    if (opcode == CMD_DELQ_WITH_META && rc == PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        return ENGINE_SUCCESS;
    }

    return sendResponse(response, NULL, 0, NULL, 0, NULL, 0,
                        PROTOCOL_BINARY_RAW_BYTES, rc, 0, cookie);
}

ENGINE_ERROR_CODE
EventuallyPersistentEngine::changeTapVBFilter(const void *cookie,
                                              protocol_binary_request_header *request,
                                              ADD_RESPONSE response) {
    protocol_binary_request_no_extras *req = (protocol_binary_request_no_extras*)request;

    uint16_t keylen = ntohs(req->message.header.request.keylen);
    const char *ptr = ((char*)req) + sizeof(req->message.header);
    std::string tap_name = "eq_tapq:";
    tap_name.append(std::string(ptr, keylen));

    const char *msg = NULL;
    protocol_binary_response_status rv = PROTOCOL_BINARY_RESPONSE_SUCCESS;
    size_t nuserdata = ntohl(req->message.header.request.bodylen) - keylen;
    uint16_t nvbuckets = 0;

    if (nuserdata < sizeof(nvbuckets)) {
        msg = "Number of vbuckets is missing";
        rv = PROTOCOL_BINARY_RESPONSE_EINVAL;
    } else {
        ptr += keylen;
        memcpy(&nvbuckets, ptr, sizeof(nvbuckets));
        nuserdata -= sizeof(nvbuckets);
        ptr += sizeof(nvbuckets);
        nvbuckets = ntohs(nvbuckets);
        if (nvbuckets > 0) {
            if (nuserdata < ((sizeof(uint16_t) + sizeof(uint64_t)) * nvbuckets)) {
                msg = "Number of (vbucket id, checkpoint id) pair is not matched";
                rv = PROTOCOL_BINARY_RESPONSE_EINVAL;
            } else {
                std::vector<uint16_t> vbuckets;
                std::map<uint16_t, uint64_t> checkpointIds;
                for (uint16_t i = 0; i < nvbuckets; ++i) {
                    uint16_t vbid;
                    uint64_t chkid;
                    memcpy(&vbid, ptr, sizeof(vbid));
                    ptr += sizeof(uint16_t);
                    memcpy(&chkid, ptr, sizeof(chkid));
                    ptr += sizeof(uint64_t);
                    vbuckets.push_back(ntohs(vbid));
                    checkpointIds[ntohs(vbid)] = ntohll(chkid);
                }
                if (!tapConnMap->changeVBucketFilter(tap_name, vbuckets, checkpointIds)) {
                    msg = "TAP producer not exist!!!";
                    rv = PROTOCOL_BINARY_RESPONSE_EINVAL;
                }
            }
        } else {
            msg = "Number of vbuckets should be greater than 0";
            rv = PROTOCOL_BINARY_RESPONSE_EINVAL;
        }
    }

    return sendResponse(response, NULL, 0, NULL, 0,
                        msg, msg ? static_cast<uint16_t>(strlen(msg)) : 0,
                        PROTOCOL_BINARY_RAW_BYTES,
                        static_cast<uint16_t>(rv),
                        0, cookie);
}

ENGINE_ERROR_CODE
EventuallyPersistentEngine::handleTrafficControlCmd(const void *cookie,
                                                    protocol_binary_request_header *request,
                                                    ADD_RESPONSE response)
{
    std::stringstream msg;
    int16_t status = PROTOCOL_BINARY_RESPONSE_SUCCESS;

    switch (request->request.opcode) {
    case CMD_ENABLE_TRAFFIC:
        if (stillWarmingUp()) {
            // engine is still warming up, do not turn on data traffic yet
            msg << "Persistent engine is still warming up!";
            status = PROTOCOL_BINARY_RESPONSE_ETMPFAIL;
        } else {
            if (enableTraffic(true)) {
                msg << "Data traffic to persistent engine is enabled";
            } else {
                msg << "Data traffic to persistence engine was already enabled";
            }
        }
        break;
    case CMD_DISABLE_TRAFFIC:
        if (enableTraffic(false)) {
            msg << "Data traffic to persistence engine is disabled";
        } else {
            msg << "Data traffic to persistence engine was already disabled";
        }
        break;
    default:
        msg << "Unknown traffic control opcode: " << request->request.opcode;
        status = PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND;
    }

    return sendResponse(response, NULL, 0, NULL, 0,
                        msg.str().c_str(), msg.str().length(),
                        PROTOCOL_BINARY_RAW_BYTES,
                        status, 0, cookie);
}

ENGINE_ERROR_CODE
EventuallyPersistentEngine::doTapVbTakeoverStats(const void *cookie,
                                                 ADD_STAT add_stat,
                                                 std::string &key,
                                                 uint16_t vbid) {
    RCPtr<VBucket> vb = getVBucket(vbid);
    if (!vb) {
        return ENGINE_NOT_MY_VBUCKET;
    }
    std::string tapName("eq_tapq:");
    tapName.append(key);
    size_t vb_items = vb->ht.getNumItems();
    size_t del_items = epstore->getRWUnderlying(vbid)->getNumPersistedDeletes(vbid);

    add_casted_stat("name", tapName, add_stat, cookie);

    uint64_t total;
    uint64_t chk_items;
    if (key.length() == 0 || !tapConnMap->findByName(tapName)) {
        chk_items = vb->checkpointManager.getNumOpenChkItems();
        total = vb_items + del_items + chk_items;
        add_casted_stat("status", "does_not_exist", add_stat, cookie);
    } else {
        if (tapConnMap->isBackfillCompleted(tapName)) {
            chk_items = vb->checkpointManager.getNumItemsForTAPConnection(tapName);
            total = chk_items;
            add_casted_stat("status", "backfill completed", add_stat, cookie);
        } else {
            chk_items = vb->checkpointManager.getNumOpenChkItems();
            total = vb_items + del_items + chk_items;
            add_casted_stat("status", "backfilling", add_stat, cookie);
        }
    }

    add_casted_stat("estimate", total, add_stat, cookie);
    add_casted_stat("on_disk_deletes", del_items, add_stat, cookie);
    add_casted_stat("chk_items", chk_items, add_stat, cookie);
    add_casted_stat("vb_items", vb_items, add_stat, cookie);

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE
EventuallyPersistentEngine::returnMeta(const void* cookie,
                                       protocol_binary_request_return_meta *request,
                                       ADD_RESPONSE response) {
    uint8_t extlen = request->message.header.request.extlen;
    uint16_t keylen = ntohs(request->message.header.request.keylen);
    if (extlen != 12 || request->message.header.request.keylen == 0) {
        return sendResponse(response, NULL, 0, NULL, 0, NULL, 0,
                            PROTOCOL_BINARY_RAW_BYTES,
                            PROTOCOL_BINARY_RESPONSE_EINVAL, 0, cookie);
    }

    if (isDegradedMode()) {
        return sendResponse(response, NULL, 0, NULL, 0, NULL, 0,
                            PROTOCOL_BINARY_RAW_BYTES,
                            PROTOCOL_BINARY_RESPONSE_ETMPFAIL,
                            0, cookie);
    }

    uint8_t *key = request->bytes + sizeof(request->bytes);
    uint16_t vbucket = ntohs(request->message.header.request.vbucket);
    uint32_t bodylen = ntohl(request->message.header.request.bodylen);
    uint64_t cas = ntohll(request->message.header.request.cas);
    uint32_t mutate_type = ntohl(request->message.body.mutation_type);
    uint32_t flags = ntohl(request->message.body.flags);
    uint32_t exp = ntohl(request->message.body.expiration);
    exp = exp == 0 ? 0 : ep_abs_time(ep_reltime(exp));
    size_t vallen = bodylen - keylen - extlen;
    uint64_t seqno;



    ENGINE_ERROR_CODE ret = ENGINE_EINVAL;
    if (mutate_type == SET_RET_META || mutate_type == ADD_RET_META) {
        uint8_t *dta = key + keylen;
        Item *itm = new Item(key, keylen, vallen, flags, exp, cas, -1, vbucket);

        if (!itm) {
            return sendResponse(response, NULL, 0, NULL, 0, NULL, 0,
                                PROTOCOL_BINARY_RAW_BYTES,
                                PROTOCOL_BINARY_RESPONSE_ENOMEM, 0, cookie);
        }

        memcpy((char*)itm->getData(), dta, vallen);
        if (mutate_type == SET_RET_META) {
            ret = epstore->set(*itm, cookie);
        } else {
            ret = epstore->add(*itm, cookie);
        }
        if (ret == ENGINE_SUCCESS) {
            ++stats.numOpsSetRetMeta;
        }
        cas = itm->getCas();
        seqno = htonll(itm->getRevSeqno());
        delete itm;
    } else if (mutate_type == DEL_RET_META) {
        ItemMetaData itm_meta;
        std::string key_str(reinterpret_cast<char*>(key), keylen);
        ret = epstore->deleteItem(key_str, &cas, vbucket, cookie, false, false,
                                  true, &itm_meta);
        if (ret == ENGINE_SUCCESS) {
            ++stats.numOpsDelRetMeta;
        }
        flags = itm_meta.flags;
        exp = itm_meta.exptime;
        cas = itm_meta.cas;
        seqno = htonll(itm_meta.revSeqno);
    } else {
        return sendResponse(response, NULL, 0, NULL, 0, NULL, 0,
                            PROTOCOL_BINARY_RAW_BYTES,
                            PROTOCOL_BINARY_RESPONSE_EINVAL, 0, cookie);
    }

    if (ret != ENGINE_SUCCESS) {
        protocol_binary_response_status rc = engine_error_2_protocol_error(ret);
        return sendResponse(response, NULL, 0, NULL, 0, NULL, 0,
                            PROTOCOL_BINARY_RAW_BYTES, rc, 0, cookie);
    }

    uint8_t meta[16];
    exp = htonl(exp);
    memcpy(meta, &flags, 4);
    memcpy(meta + 4, &exp, 4);
    memcpy(meta + 8, &seqno, 8);

    return sendResponse(response, NULL, 0, (const void *)meta, 16, NULL, 0,
                        PROTOCOL_BINARY_RAW_BYTES,
                        PROTOCOL_BINARY_RESPONSE_SUCCESS, cas, cookie);
}
