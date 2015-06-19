/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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

/*
 *                "ewouldblock_engine"
 *
 * The "ewouldblock_engine" allows one to test how memcached responds when the
 * engine returns EWOULDBLOCK instead of the correct response.
 *
 * Motivation:
 *
 * The EWOULDBLOCK response code can be returned from a number of engine
 * functions, and is used to indicate that the request could not be immediately
 * fulfilled, and it "would block" if it tried to. The correct way for
 * memcached to handle this (in general) is to suspend that request until it
 * is later notified by the engine (via notify_io_complete()).
 *
 * However, engines typically return the correct response to requests
 * immediately, only rarely (and from memcached's POV non-deterministically)
 * returning EWOULDBLOCK. This makes testing of the code-paths handling
 * EWOULDBLOCK tricky.
 *
 *
 * Operation:
 * This engine, when loaded by memcached proxies requests to a "real" engine.
 * Depending on how it is configured, it can simply pass the request on to the
 * real engine, or artificially return EWOULDBLOCK back to memcached.
 *
 * See the 'Modes' enum below for the possible modes for a connection. The mode
 * can be selected by sending a `request_ewouldblock_ctl` command
 *  (opcode PROTOCOL_BINARY_CMD_EWOULDBLOCK_CTL).
 */

#include "ewouldblock_engine.h"

#include <atomic>
#include <condition_variable>
#include <cstring>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <random>
#include <string>
#include <thread>

#include <memcached/engine.h>
#include "utilities/engine_loader.h"

// Shared state between the main thread of execution and the background
// thread processing pending io ops.
static std::mutex mutex;
static std::condition_variable condvar;
static std::queue<const void*> pending_io_ops;
std::atomic<bool> stop_notification_thread;


/* Public API declaration ****************************************************/

extern "C" {
    MEMCACHED_PUBLIC_API
    ENGINE_ERROR_CODE create_instance(uint64_t interface, GET_SERVER_API gsa,
                                      ENGINE_HANDLE **handle);

    MEMCACHED_PUBLIC_API
    void destroy_engine(void);
}

/** ewouldblock_engine class */
class EWB_Engine : public ENGINE_HANDLE_V1 {

private:
    enum class Cmd { NONE, GET_INFO, ALLOCATE, REMOVE, GET, STORE, ARITHMETIC,
                     FLUSH, GET_STATS, UNKNOWN_COMMAND };

    std::string to_string(Cmd cmd);

public:
    EWB_Engine(GET_SERVER_API gsa_);

    ~EWB_Engine();

    // Convert from a handle back to the read object.
    static EWB_Engine* to_engine(ENGINE_HANDLE* handle) {
        return reinterpret_cast<EWB_Engine*> (handle);
    }

    /* Returns true if the next command should have a fake error code injected.
     * @param func Address of the command function (get, store, etc).
     * @param cookie The cookie for the user's request.
     * @param[out] Error code to return.
     */
    bool should_inject_error(Cmd cmd, const void* cookie,
                             ENGINE_ERROR_CODE& err) {
        std::lock_guard<std::mutex> guard(cookie_map_mutex);
        auto& state = cookie_map[cookie];
        if (state == nullptr) {
            return false;
        }

        const bool inject = state->should_inject_error(cmd, err);

        if (inject) {
            if (err == ENGINE_EWOULDBLOCK) {
                // The server expects that if EWOULDBLOCK is returned then the
                // server should be notified in the future when the operation is
                // ready - so add this op to the pending IO queue.
                {
                    std::lock_guard<std::mutex> guard(mutex);
                    pending_io_ops.push(cookie);
                }
                condvar.notify_one();
            }
        }

        return inject;
    }

    /* Implementation of all the engine functions. ***************************/

    static const engine_info* get_info(ENGINE_HANDLE* handle) {
        return &to_engine(handle)->info.eng_info;
    }

    static ENGINE_ERROR_CODE initialize(ENGINE_HANDLE* handle,
                                        const char* config_str) {
        EWB_Engine* ewb = to_engine(handle);
        auto logger = ewb->gsa()->log->get_logger();

        // Extract the name of the real engine we will be proxying; then
        // create and initialize it.
        std::string config(config_str);
        auto seperator = config.find(";");
        std::string real_engine_name(config.substr(0, seperator));
        std::string real_engine_config;
        if (seperator != std::string::npos) {
            real_engine_config = config.substr(seperator);
        }

        if ((ewb->real_engine_ref = load_engine(real_engine_name.c_str(),
                                                NULL, NULL, logger)) == NULL) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "ERROR: EWB_Engine::initialize(): Failed to load real "
                        "engine '%s'", real_engine_name.c_str());
            abort();
        }

        if (!create_engine_instance(ewb->real_engine_ref, ewb->gsa, NULL,
                                    &ewb->real_handle)) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "ERROR: EWB_Engine::initialize(): Failed create "
                        "engine instance '%s'", real_engine_name.c_str());
            abort();
        }

        if (ewb->real_handle->interface != 1) {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "ERROR: EWB_Engine::initialize(): Only support engine "
                        "with interface v1 - got v%" PRIu64 ".",
                        ewb->real_engine->interface.interface);
            abort();
        }
        ewb->real_engine =
                reinterpret_cast<ENGINE_HANDLE_V1*>(ewb->real_handle);
        ENGINE_ERROR_CODE res = ewb->real_engine->initialize(
                ewb->real_handle, real_engine_config.c_str());

        if (res == ENGINE_SUCCESS) {
            // For engine interface functions which cannot return EWOULDBLOCK, we can
            // simply use the real_engine's functions directly.
            ewb->ENGINE_HANDLE_V1::get_stats_struct = ewb->real_engine->get_stats_struct;
            ewb->ENGINE_HANDLE_V1::item_set_cas = ewb->real_engine->item_set_cas;
            ewb->ENGINE_HANDLE_V1::get_item_info = ewb->real_engine->get_item_info;
            ewb->ENGINE_HANDLE_V1::set_item_info = ewb->real_engine->set_item_info;

            // TODO: Add support for DCP interposing. For now just use the real
            // engines' interface.
            ewb->ENGINE_HANDLE_V1::dcp = ewb->real_engine->dcp;
        }
        return res;
    }

    static void destroy(ENGINE_HANDLE* handle, const bool force) {
        EWB_Engine* ewb = to_engine(handle);
        ewb->real_engine->destroy(ewb->real_handle, force);
        delete ewb;
    }

    static ENGINE_ERROR_CODE allocate(ENGINE_HANDLE* handle, const void* cookie,
                                      item **item, const void* key,
                                      const size_t nkey, const size_t nbytes,
                                      const int flags, const rel_time_t exptime,
                                      uint8_t datatype) {
        EWB_Engine* ewb = to_engine(handle);
        ENGINE_ERROR_CODE err = ENGINE_SUCCESS;
        if (ewb->should_inject_error(Cmd::ALLOCATE, cookie, err)) {
            return err;
        } else {
            return ewb->real_engine->allocate(ewb->real_handle, cookie, item,
                                              key, nkey, nbytes, flags, exptime,
                                              datatype);
        }
    }

    static ENGINE_ERROR_CODE remove(ENGINE_HANDLE* handle, const void* cookie,
                                    const void* key, const size_t nkey,
                                    uint64_t* cas, uint16_t vbucket,
                                    mutation_descr_t* mut_info) {
        EWB_Engine* ewb = to_engine(handle);
        ENGINE_ERROR_CODE err = ENGINE_SUCCESS;
        if (ewb->should_inject_error(Cmd::REMOVE, cookie, err)) {
            return err;
        } else {
            return ewb->real_engine->remove(ewb->real_handle, cookie, key, nkey,
                                            cas, vbucket, mut_info);
        }
    }

    static void release(ENGINE_HANDLE* handle, const void *cookie, item* item) {
        EWB_Engine* ewb = to_engine(handle);
        return ewb->real_engine->release(ewb->real_handle, cookie, item);
    }

    static ENGINE_ERROR_CODE get(ENGINE_HANDLE* handle, const void* cookie,
                                 item** item, const void* key, const int nkey,
                                 uint16_t vbucket) {
        EWB_Engine* ewb = to_engine(handle);
        ENGINE_ERROR_CODE err = ENGINE_SUCCESS;
        if (ewb->should_inject_error(Cmd::GET, cookie, err)) {
            return err;
        } else {
            return ewb->real_engine->get(ewb->real_handle, cookie, item, key,
                                         nkey, vbucket);
        }
    }

    static ENGINE_ERROR_CODE store(ENGINE_HANDLE* handle, const void *cookie,
                                   item* item, uint64_t *cas,
                                   ENGINE_STORE_OPERATION operation,
                                   uint16_t vbucket) {
        EWB_Engine* ewb = to_engine(handle);
        ENGINE_ERROR_CODE err = ENGINE_SUCCESS;
        if (ewb->should_inject_error(Cmd::STORE, cookie, err)) {
            return err;
        } else {
            return ewb->real_engine->store(ewb->real_handle, cookie, item, cas,
                                           operation, vbucket);
        }
    }

    static ENGINE_ERROR_CODE arithmetic(ENGINE_HANDLE* handle,
                                        const void* cookie, const void* key,
                                        const int nkey, const bool increment,
                                        const bool create, const uint64_t delta,
                                        const uint64_t initial,
                                        const rel_time_t exptime, item **item,
                                        uint8_t datatype, uint64_t *result,
                                        uint16_t vbucket) {
        EWB_Engine* ewb = to_engine(handle);
        ENGINE_ERROR_CODE err = ENGINE_SUCCESS;
        if (ewb->should_inject_error(Cmd::ARITHMETIC, cookie, err)) {
            return err;
        } else {
            return ewb->real_engine->arithmetic(ewb->real_handle, cookie, key,
                                                nkey, increment, create, delta,
                                                initial, exptime, item,
                                                datatype, result, vbucket);
        }
    }

    static ENGINE_ERROR_CODE flush(ENGINE_HANDLE* handle, const void* cookie,
                                   time_t when) {
        // Flush is a little different - it often returns EWOULDBLOCK, and
        // notify_io_complete() just tells the server it can issue it's *next*
        // command (i.e. no need to re-flush). Therefore just pass Flush
        // straight through for now.
        EWB_Engine* ewb = to_engine(handle);
        return ewb->real_engine->flush(ewb->real_handle, cookie, when);
    }

    static ENGINE_ERROR_CODE get_stats(ENGINE_HANDLE* handle,
                                       const void* cookie, const char* stat_key,
                                       int nkey, ADD_STAT add_stat) {
        EWB_Engine* ewb = to_engine(handle);
        ENGINE_ERROR_CODE err = ENGINE_SUCCESS;
        if (ewb->should_inject_error(Cmd::GET_STATS, cookie, err)) {
            return err;
        } else {
            return ewb->real_engine->get_stats(ewb->real_handle, cookie, stat_key,
                                               nkey, add_stat);
        }
    }

    static void reset_stats(ENGINE_HANDLE* handle, const void* cookie) {
        EWB_Engine* ewb = to_engine(handle);
        return ewb->real_engine->reset_stats(ewb->real_handle, cookie);
    }

    static void* get_stats_struct(ENGINE_HANDLE* handle, const void* cookie) {
        EWB_Engine* ewb = to_engine(handle);
        return ewb->real_engine->get_stats_struct(ewb->real_handle, cookie);
    }

    static ENGINE_ERROR_CODE unknown_command(ENGINE_HANDLE* handle,
                                             const void* cookie,
                                             protocol_binary_request_header *request,
                                             ADD_RESPONSE response) {
        EWB_Engine* ewb = to_engine(handle);
        if (request->request.opcode == PROTOCOL_BINARY_CMD_EWOULDBLOCK_CTL) {
            request_ewouldblock_ctl* req =
                    reinterpret_cast<request_ewouldblock_ctl*>(request);
            const EWBEngine_Mode mode = static_cast<EWBEngine_Mode>(ntohl(req->message.body.mode));
            const uint32_t value = ntohl(req->message.body.value);
            const ENGINE_ERROR_CODE injected_error =
                    static_cast<ENGINE_ERROR_CODE>(ntohl(req->message.body.inject_error));

            std::shared_ptr<FaultInjectMode> new_mode = nullptr;

            // Validate mode, and construct new fault injector.
            switch (mode) {
                case EWBEngineMode_NEXT_N:
                    new_mode = std::make_shared<ErrOnNextN>(injected_error, value);
                    break;

                case EWBEngineMode_RANDOM:
                    new_mode = std::make_shared<ErrRandom>(injected_error, value);
                    break;

                case EWBEngineMode_FIRST:
                    new_mode = std::make_shared<ErrOnFirst>(injected_error);
                    break;

                case EWBEngineMode_SEQUENCE:
                    new_mode = std::make_shared<ErrSequence>(injected_error, value);
                    break;

                default:
                    break;
            }

            if (new_mode == nullptr) {
                auto logger = ewb->gsa()->log->get_logger();
                logger->log(EXTENSION_LOG_WARNING, NULL,
                            "EWB_Engine::unknown_command(): "
                            "Got unexpected mode=%d for EWOULDBLOCK_CTL, ",
                            mode);
                response(nullptr, 0, nullptr, 0, nullptr, 0,
                         PROTOCOL_BINARY_RAW_BYTES,
                         PROTOCOL_BINARY_RESPONSE_EINVAL, /*cas*/0, cookie);
                return ENGINE_FAILED;
            } else {
                {
                    std::lock_guard<std::mutex> guard(ewb->cookie_map_mutex);
                    ewb->cookie_map.erase(cookie);
                    ewb->cookie_map[cookie] = new_mode;
                }
                response(nullptr, 0, nullptr, 0, nullptr, 0,
                         PROTOCOL_BINARY_RAW_BYTES,
                         PROTOCOL_BINARY_RESPONSE_SUCCESS, /*cas*/0, cookie);
                return ENGINE_SUCCESS;
            }
        } else {
            ENGINE_ERROR_CODE err = ENGINE_SUCCESS;
            if (ewb->should_inject_error(Cmd::UNKNOWN_COMMAND, cookie, err)) {
                return err;
            } else {
                return ewb->real_engine->unknown_command(ewb->real_handle, cookie,
                                                         request, response);
            }
        }
    }

    static void item_set_cas(ENGINE_HANDLE *handle, const void* cookie,
                             item* item, uint64_t cas) {
        // Should never be called as ENGINE_HANDLE_V1::item_set_cas is updated
        // to point to the real_engine once it is initialized. This function
        //only exists so there is a non-NULL value for
        // ENGINE_HANDLE_V1::item_set_cas initially to keep load_engine()
        // happy.
        abort();
    }

    static bool get_item_info(ENGINE_HANDLE *handle, const void *cookie,
                              const item* item, item_info *item_info) {
        // Should never be called - set item_set_cas().
        abort();
    }
    static bool set_item_info(ENGINE_HANDLE *handle, const void *cookie,
                              item* item, const item_info *item_info) {
        // Should never be called - set item_set_cas().
        abort();
    }

    GET_SERVER_API gsa;
    union {
        engine_info eng_info;
        char buffer[sizeof(engine_info) +
                    (sizeof(feature_info) * LAST_REGISTERED_ENGINE_FEATURE)];
    } info;

    // Actual engine we are proxying requests to.
    ENGINE_HANDLE* real_handle;
    ENGINE_HANDLE_V1* real_engine;
    engine_reference* real_engine_ref;

private:

    // Handle of the notification thread.
    std::thread notification_thread;

    // Base class for all fault injection modes.
    struct FaultInjectMode {
        FaultInjectMode(ENGINE_ERROR_CODE injected_error_)
          : injected_error(injected_error_) {}

        virtual bool should_inject_error(Cmd cmd, ENGINE_ERROR_CODE& err) = 0;

    protected:
        ENGINE_ERROR_CODE injected_error;
    };

    // Subclasses for each fault inject mode: /////////////////////////////////

    class ErrOnFirst : public FaultInjectMode {
    public:
        ErrOnFirst(ENGINE_ERROR_CODE injected_error_)
          : FaultInjectMode(injected_error_),
            prev_cmd(Cmd::NONE) {}

        bool should_inject_error(Cmd cmd, ENGINE_ERROR_CODE& err) {
            // Block unless the previous command from this cookie
            // was the same - i.e. all of a connections' commands
            // will EWOULDBLOCK the first time they are called.
            bool inject = (prev_cmd != cmd);
            prev_cmd = cmd;
            if (inject) {
                err = injected_error;
            }
            return inject;
        }

    private:
        // Last command issued by this cookie.
        Cmd prev_cmd;
    };

    class ErrOnNextN : public FaultInjectMode {
    public:
        ErrOnNextN(ENGINE_ERROR_CODE injected_error_, uint32_t count_)
          : FaultInjectMode(injected_error_),
            count(count_) {}

        bool should_inject_error(Cmd cmd, ENGINE_ERROR_CODE& err) {
            if (count > 0) {
                --count;
                err = injected_error;
                return true;
            } else {
                return false;
            }
        }

    private:
        // The count of commands issued that should return error.
        uint32_t count;
    };

    class ErrRandom : public FaultInjectMode {
    public:
        ErrRandom(ENGINE_ERROR_CODE injected_error_, uint32_t percentage_)
          : FaultInjectMode(injected_error_),
            percentage_to_err(percentage_) {}

        bool should_inject_error(Cmd cmd, ENGINE_ERROR_CODE& err) {
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<uint32_t> dis(1, 100);
            if (dis(gen) < percentage_to_err) {
                err = injected_error;
                return true;
            } else {
                return false;
            }
        }
    private:
        // Percentage chance that the specified error should be injected.
        uint32_t percentage_to_err;
    };

    class ErrSequence : public FaultInjectMode {
    public:
        ErrSequence(ENGINE_ERROR_CODE injected_error_, uint32_t sequence_)
            : FaultInjectMode(injected_error_),
              sequence(sequence_),
              pos(0) {}

        bool should_inject_error(Cmd cmd, ENGINE_ERROR_CODE& err) {
            bool inject = false;
            if (pos < 32) {
                inject = (sequence & (1 << pos)) != 0;
                pos++;
            }
            if (inject) {
                err = injected_error;
            }
            return inject;
        }

    private:
        uint32_t sequence;
        uint32_t pos;
    };

    // Map of connections (aka cookies) to their current mode.
    std::map<const void*, std::shared_ptr<FaultInjectMode> > cookie_map;
    // Mutex for above map.
    std::mutex cookie_map_mutex;
};

void process_pending_queue(SERVER_HANDLE_V1* server) {
    std::unique_lock<std::mutex> lk(mutex);
    while (!stop_notification_thread) {
        condvar.wait(lk);
        while (!pending_io_ops.empty()) {
            const void* cookie = pending_io_ops.front();
            pending_io_ops.pop();
            lk.unlock();
            server->cookie->notify_io_complete(cookie, ENGINE_SUCCESS);
            lk.lock();
        }
    }
}

EWB_Engine::EWB_Engine(GET_SERVER_API gsa_)
  : gsa(gsa_),
    real_engine(NULL),
    notification_thread(process_pending_queue, gsa())
{
    interface.interface = 1;
    ENGINE_HANDLE_V1::get_info = get_info;
    ENGINE_HANDLE_V1::initialize = initialize;
    ENGINE_HANDLE_V1::destroy = destroy;
    ENGINE_HANDLE_V1::allocate = allocate;
    ENGINE_HANDLE_V1::remove = remove;
    ENGINE_HANDLE_V1::release = release;
    ENGINE_HANDLE_V1::get = get;
    ENGINE_HANDLE_V1::store = store;
    ENGINE_HANDLE_V1::arithmetic = arithmetic;
    ENGINE_HANDLE_V1::flush = flush;
    ENGINE_HANDLE_V1::get_stats = get_stats;
    ENGINE_HANDLE_V1::reset_stats = reset_stats;
    ENGINE_HANDLE_V1::aggregate_stats = NULL;
    ENGINE_HANDLE_V1::unknown_command = unknown_command;
    ENGINE_HANDLE_V1::tap_notify = NULL;
    ENGINE_HANDLE_V1::get_tap_iterator = NULL;
    ENGINE_HANDLE_V1::item_set_cas = item_set_cas;
    ENGINE_HANDLE_V1::get_item_info = get_item_info;
    ENGINE_HANDLE_V1::set_item_info = set_item_info;
    ENGINE_HANDLE_V1::get_engine_vb_map = NULL;
    ENGINE_HANDLE_V1::get_stats_struct = NULL;

    std::memset(&info, 0, sizeof(info.buffer));
    info.eng_info.description = "EWOULDBLOCK Engine";
    info.eng_info.features[info.eng_info.num_features++].feature = ENGINE_FEATURE_LRU;
    info.eng_info.features[info.eng_info.num_features++].feature = ENGINE_FEATURE_DATATYPE;

    // Spin up a background thread to perform IO notifications.
}

EWB_Engine::~EWB_Engine() {
    free(real_engine_ref);
    stop_notification_thread = true;
    condvar.notify_all();
    notification_thread.join();

}

ENGINE_ERROR_CODE create_instance(uint64_t interface,
                                  GET_SERVER_API gsa,
                                  ENGINE_HANDLE **handle)
{
    if (interface != 1) {
        return ENGINE_ENOTSUP;
    }

    EWB_Engine* engine = new EWB_Engine(gsa);

    *handle = reinterpret_cast<ENGINE_HANDLE*> (engine);
    return ENGINE_SUCCESS;
}

void destroy_engine(void) {
    // nothing todo.
}

std::string EWB_Engine::to_string(const Cmd cmd) {
    const std::map<Cmd, std::string> names({
        {Cmd::NONE, "NONE"},
        {Cmd::GET_INFO, "GET_INFO"},
        {Cmd::ALLOCATE, "ALLOCATE"},
        {Cmd::REMOVE, "REMOVE"},
        {Cmd::GET, "GET"},
        {Cmd::STORE, "STORE"},
        {Cmd::ARITHMETIC, "ARITHMETIC"},
        {Cmd::FLUSH, "FLUSH"},
        {Cmd::GET_STATS, "GET_STATS"},
        {Cmd::UNKNOWN_COMMAND, "UNKNOWN_COMMAND"}});
    return names.at(cmd);
}
