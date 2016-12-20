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

/* The "crash" bucket is a bucket which simply crashes when it is initialized.
 * It is intended to be used to test crash catching using Google Breakpad.
 */

#include "config.h"

#include <stdlib.h>
#include <stdexcept>
#include <string>

#include <memcached/engine.h>
#include <memcached/visibility.h>
#include <memcached/util.h>
#include <memcached/config_parser.h>
#include <platform/cb_malloc.h>

extern "C" {
MEMCACHED_PUBLIC_API
ENGINE_ERROR_CODE create_instance(uint64_t interface,
                                  GET_SERVER_API gsa,
                                  ENGINE_HANDLE **handle);

MEMCACHED_PUBLIC_API
void destroy_engine(void);
} // extern "C"

struct CrashEngine {
    ENGINE_HANDLE_V1 engine;
    union {
        engine_info eng_info;
        char buffer[sizeof(engine_info) +
                    (sizeof(feature_info) * LAST_REGISTERED_ENGINE_FEATURE)];
    } info;
};

static CrashEngine* get_handle(ENGINE_HANDLE* handle)
{
    return reinterpret_cast<CrashEngine*>(handle);
}

static const engine_info* get_info(ENGINE_HANDLE* handle)
{
    return &get_handle(handle)->info.eng_info;
}

// How do I crash thee? Let me count the ways.
enum class CrashMode {
    SegFault,
    UncaughtException
};

static char dummy;

/* Recursive functions which will crash using the given method after
 * 'depth' calls.
 * Note: mutates a dummy global variable to prevent optimization
 * removing the recursion.
 */
EXPORT_SYMBOL
char recursive_crash_function(char depth, CrashMode mode) {
    if (depth == 0) {
        switch (mode) {
        case CrashMode::SegFault: {
            char* death = (char*)0xdeadcbdb;
            return *death + dummy;
        }
        case CrashMode::UncaughtException:
            throw std::runtime_error("crash_engine: This exception wasn't handled");
        }
    }
    recursive_crash_function(depth - 1, mode);
    return dummy++;
}

/* 'initializes' this engine - given this is the crash_engine that
 * means crashing it.
 */
static ENGINE_ERROR_CODE initialize(ENGINE_HANDLE* handle,
                                    const char* config_str)
{
    (void)handle;
    (void)config_str;
    std::string mode_string(getenv("MEMCACHED_CRASH_TEST"));
    CrashMode mode;
    if (mode_string == "segfault") {
        mode = CrashMode::SegFault;
    } else if (mode_string == "exception") {
        mode = CrashMode::UncaughtException;
    } else {
        fprintf(stderr, "crash_engine::initialize: could not find a valid "
                "CrashMode from MEMCACHED_CRASH_TEST env var ('%s')\n",
                mode_string.c_str());
        exit(1);
    }
    return ENGINE_ERROR_CODE(recursive_crash_function(25, mode));
}

static void destroy(ENGINE_HANDLE* handle, const bool force)
{
    (void)force;
    cb_free(handle);
}

static ENGINE_ERROR_CODE item_allocate(ENGINE_HANDLE* handle,
                                       const void* cookie,
                                       item **item,
                                       const DocKey& key,
                                       const size_t nbytes,
                                       const int flags,
                                       const rel_time_t exptime,
                                       uint8_t datatype,
                                       uint16_t vbucket)
{
    return ENGINE_FAILED;
}

static ENGINE_ERROR_CODE item_delete(ENGINE_HANDLE* handle,
                                     const void* cookie,
                                     const DocKey& key,
                                     uint64_t* cas,
                                     uint16_t vbucket,
                                     mutation_descr_t* mut_info)
{
    return ENGINE_FAILED;
}

static void item_release(ENGINE_HANDLE* handle, const void *cookie,
                         item* item)
{

}

static ENGINE_ERROR_CODE get(ENGINE_HANDLE* handle,
                             const void* cookie,
                             item** item,
                             const DocKey& key,
                             uint16_t vbucket,
                             DocumentState)
{
    return ENGINE_FAILED;
}

static ENGINE_ERROR_CODE get_stats(ENGINE_HANDLE* handle,
                                   const void* cookie,
                                   const char* stat_key,
                                   int nkey,
                                   ADD_STAT add_stat)
{
    return ENGINE_FAILED;
}

static ENGINE_ERROR_CODE store(ENGINE_HANDLE* handle,
                               const void *cookie,
                               item* item,
                               uint64_t *cas,
                               ENGINE_STORE_OPERATION operation,
                               DocumentState)
{
    return ENGINE_FAILED;
}

static ENGINE_ERROR_CODE flush(ENGINE_HANDLE* handle,
                               const void* cookie)
{
    return ENGINE_FAILED;
}

static void reset_stats(ENGINE_HANDLE* handle,
                         const void *cookie)
{
}

static void item_set_cas(ENGINE_HANDLE *handle, const void *cookie,
                         item* item, uint64_t val)
{
}

static bool get_item_info(ENGINE_HANDLE *handle, const void *cookie,
                          const item* item, item_info *item_info)
{
    return false;
}

static bool set_item_info(ENGINE_HANDLE *handle, const void *cookie,
                          item* item, const item_info *itm_info)
{
    return false;
}

ENGINE_ERROR_CODE create_instance(uint64_t interface,
                                  GET_SERVER_API gsa,
                                  ENGINE_HANDLE **handle)
{
    CrashEngine* engine;
    (void)gsa;

    if (interface != 1) {
        return ENGINE_ENOTSUP;
    }

    if ((engine = reinterpret_cast<CrashEngine*>(cb_calloc(1, sizeof(*engine)))) == NULL) {
        return ENGINE_ENOMEM;
    }

    engine->engine.interface.interface = 1;
    engine->engine.get_info = get_info;
    engine->engine.initialize = initialize;
    engine->engine.destroy = destroy;
    engine->engine.allocate = item_allocate;
    engine->engine.remove = item_delete;
    engine->engine.release = item_release;
    engine->engine.get = get;
    engine->engine.get_stats = get_stats;
    engine->engine.reset_stats = reset_stats;
    engine->engine.store = store;
    engine->engine.flush = flush;
    engine->engine.item_set_cas = item_set_cas;
    engine->engine.get_item_info = get_item_info;
    engine->engine.set_item_info = set_item_info;
    engine->info.eng_info.description = "Crash Engine";
    engine->info.eng_info.num_features = 0;
    *handle = reinterpret_cast<ENGINE_HANDLE*>(&engine->engine);
    return ENGINE_SUCCESS;
}

void destroy_engine(){

}
