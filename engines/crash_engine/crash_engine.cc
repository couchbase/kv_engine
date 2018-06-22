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
#include <gsl/gsl>
#include <stdexcept>
#include <string>

#include <memcached/engine.h>
#include <memcached/visibility.h>
#include <memcached/util.h>
#include <memcached/config_parser.h>
#include <platform/cb_malloc.h>

extern "C" {
MEMCACHED_PUBLIC_API
ENGINE_ERROR_CODE create_instance(GET_SERVER_API gsa, ENGINE_HANDLE** handle);

MEMCACHED_PUBLIC_API
void destroy_engine(void);
} // extern "C"

class CrashEngine : public EngineIface {
public:
    ENGINE_ERROR_CODE initialize(const char* config_str) override;
    void destroy(bool) override;

    cb::EngineErrorItemPair allocate(gsl::not_null<const void*> cookie,
                                     const DocKey& key,
                                     const size_t nbytes,
                                     const int flags,
                                     const rel_time_t exptime,
                                     uint8_t datatype,
                                     uint16_t vbucket) override;
    std::pair<cb::unique_item_ptr, item_info> allocate_ex(
            gsl::not_null<const void*> cookie,
            const DocKey& key,
            size_t nbytes,
            size_t priv_nbytes,
            int flags,
            rel_time_t exptime,
            uint8_t datatype,
            uint16_t vbucket) override;

    ENGINE_ERROR_CODE remove(gsl::not_null<const void*> cookie,
                             const DocKey& key,
                             uint64_t& cas,
                             uint16_t vbucket,
                             mutation_descr_t& mut_info) override;

    void release(gsl::not_null<item*> item) override;

    cb::EngineErrorItemPair get(gsl::not_null<const void*> cookie,
                                const DocKey& key,
                                uint16_t vbucket,
                                DocStateFilter documentStateFilter) override;
    cb::EngineErrorItemPair get_if(
            gsl::not_null<const void*> cookie,
            const DocKey& key,
            uint16_t vbucket,
            std::function<bool(const item_info&)> filter) override;

    cb::EngineErrorMetadataPair get_meta(gsl::not_null<const void*> cookie,
                                         const DocKey& key,
                                         uint16_t vbucket) override;

    cb::EngineErrorItemPair get_locked(gsl::not_null<const void*> cookie,
                                       const DocKey& key,
                                       uint16_t vbucket,
                                       uint32_t lock_timeout) override;

    ENGINE_ERROR_CODE unlock(gsl::not_null<const void*> cookie,
                             const DocKey& key,
                             uint16_t vbucket,
                             uint64_t cas) override;

    cb::EngineErrorItemPair get_and_touch(gsl::not_null<const void*> cookie,
                                          const DocKey& key,
                                          uint16_t vbucket,
                                          uint32_t expirytime) override;

    ENGINE_ERROR_CODE store(gsl::not_null<const void*> cookie,
                            gsl::not_null<item*> item,
                            uint64_t& cas,
                            ENGINE_STORE_OPERATION operation,
                            DocumentState document_state) override;
};

// How do I crash thee? Let me count the ways.
enum class CrashMode {
    SegFault,
    UncaughtStdException,
    UncaughtUnknownException
};

static char dummy;

/* Recursive functions which will crash using the given method after
 * 'depth' calls.
 * Note: mutates a dummy global variable to prevent optimization
 * removing the recursion.
 */
MEMCACHED_PUBLIC_API
char recursive_crash_function(char depth, CrashMode mode) {
    if (depth == 0) {
        switch (mode) {
        case CrashMode::SegFault: {
            char* death = (char*)(uintptr_t)0xdeadcbdb;
            return *death + dummy;
        }
        case CrashMode::UncaughtStdException:
            throw std::runtime_error(
                    "crash_engine: This exception wasn't handled");
        case CrashMode::UncaughtUnknownException:
            // Crash via exception not derived from std::exception
            class UnknownException {};
            throw UnknownException();
        }
    }
    recursive_crash_function(depth - char(1), mode);
    return dummy++;
}

/* 'initializes' this engine - given this is the crash_engine that
 * means crashing it.
 */
ENGINE_ERROR_CODE CrashEngine::initialize(const char* config_str) {
    std::string mode_string(getenv("MEMCACHED_CRASH_TEST"));
    CrashMode mode;
    if (mode_string == "segfault") {
        mode = CrashMode::SegFault;
    } else if (mode_string == "std_exception") {
        mode = CrashMode::UncaughtStdException;
    } else if (mode_string == "unknown_exception") {
        mode = CrashMode::UncaughtUnknownException;
    } else {
        fprintf(stderr, "crash_engine::initialize: could not find a valid "
                "CrashMode from MEMCACHED_CRASH_TEST env var ('%s')\n",
                mode_string.c_str());
        exit(1);
    }
    return ENGINE_ERROR_CODE(recursive_crash_function(25, mode));
}

void CrashEngine::destroy(const bool force) {
    delete this;
}

cb::EngineErrorItemPair CrashEngine::allocate(gsl::not_null<const void*> cookie,
                                              const DocKey& key,
                                              const size_t nbytes,
                                              const int flags,
                                              const rel_time_t exptime,
                                              uint8_t datatype,
                                              uint16_t vbucket) {
    return cb::makeEngineErrorItemPair(cb::engine_errc::failed);
}

std::pair<cb::unique_item_ptr, item_info> CrashEngine::allocate_ex(
        gsl::not_null<const void*> cookie,
        const DocKey& key,
        size_t nbytes,
        size_t priv_nbytes,
        int flags,
        rel_time_t exptime,
        uint8_t datatype,
        uint16_t vbucket) {
    throw cb::engine_error{cb::engine_errc::failed, "crash_engine"};
}

ENGINE_ERROR_CODE CrashEngine::remove(gsl::not_null<const void*> cookie,
                                      const DocKey& key,
                                      uint64_t& cas,
                                      uint16_t vbucket,
                                      mutation_descr_t& mut_info) {
    return ENGINE_FAILED;
}

void CrashEngine::release(gsl::not_null<item*> item) {
}

cb::EngineErrorItemPair CrashEngine::get(gsl::not_null<const void*> cookie,
                                         const DocKey& key,
                                         uint16_t vbucket,
                                         DocStateFilter) {
    return cb::makeEngineErrorItemPair(cb::engine_errc::failed);
}

cb::EngineErrorItemPair CrashEngine::get_if(
        gsl::not_null<const void*>,
        const DocKey&,
        uint16_t,
        std::function<bool(const item_info&)>) {
    return cb::makeEngineErrorItemPair(cb::engine_errc::failed);
}

cb::EngineErrorMetadataPair CrashEngine::get_meta(
        gsl::not_null<const void*> cookie,
        const DocKey& key,
        uint16_t vbucket) {
    return {cb::engine_errc::failed, {}};
}

cb::EngineErrorItemPair CrashEngine::get_and_touch(
        gsl::not_null<const void*> cookie, const DocKey&, uint16_t, uint32_t) {
    return cb::makeEngineErrorItemPair(cb::engine_errc::failed);
}

cb::EngineErrorItemPair CrashEngine::get_locked(
        gsl::not_null<const void*> cookie,
        const DocKey& key,
        uint16_t vbucket,
        uint32_t lock_timeout) {
    return cb::makeEngineErrorItemPair(cb::engine_errc::failed);
}

ENGINE_ERROR_CODE CrashEngine::unlock(gsl::not_null<const void*> cookie,
                                      const DocKey& key,
                                      uint16_t vbucket,
                                      uint64_t cas) {
    return ENGINE_FAILED;
}

static ENGINE_ERROR_CODE get_stats(gsl::not_null<ENGINE_HANDLE*> handle,
                                   gsl::not_null<const void*> cookie,
                                   cb::const_char_buffer key,
                                   ADD_STAT add_stat) {
    return ENGINE_FAILED;
}

ENGINE_ERROR_CODE CrashEngine::store(gsl::not_null<const void*> cookie,
                                     gsl::not_null<item*> item,
                                     uint64_t& cas,
                                     ENGINE_STORE_OPERATION operation,
                                     DocumentState) {
    return ENGINE_FAILED;
}

static ENGINE_ERROR_CODE flush(gsl::not_null<ENGINE_HANDLE*> handle,
                               gsl::not_null<const void*> cookie) {
    return ENGINE_FAILED;
}

static void reset_stats(gsl::not_null<ENGINE_HANDLE*> handle,
                        gsl::not_null<const void*> cookie) {
}

static void item_set_cas(gsl::not_null<ENGINE_HANDLE*> handle,
                         gsl::not_null<item*> item,
                         uint64_t val) {
}

static void item_set_datatype(gsl::not_null<ENGINE_HANDLE*> handle,
                              gsl::not_null<item*> item,
                              protocol_binary_datatype_t val) {
}

static bool get_item_info(gsl::not_null<ENGINE_HANDLE*> handle,
                          gsl::not_null<const item*> item,
                          gsl::not_null<item_info*> item_info) {
    return false;
}

static bool set_item_info(gsl::not_null<ENGINE_HANDLE*> handle,
                          gsl::not_null<item*> item,
                          gsl::not_null<const item_info*> itm_info) {
    return false;
}

static bool is_xattr_enabled(gsl::not_null<ENGINE_HANDLE*> handle) {
    return true;
}

static size_t get_max_item_size(gsl::not_null<ENGINE_HANDLE*> handle) {
    return default_max_item_size;
}

ENGINE_ERROR_CODE create_instance(GET_SERVER_API gsa, ENGINE_HANDLE** handle) {
    CrashEngine* engine;

    try {
        engine = new CrashEngine();
    } catch (std::bad_alloc&) {
        return ENGINE_ENOMEM;
    }

    engine->get_stats = get_stats;
    engine->reset_stats = reset_stats;
    engine->flush = flush;
    engine->item_set_cas = item_set_cas;
    engine->item_set_datatype = item_set_datatype;
    engine->get_item_info = get_item_info;
    engine->set_item_info = set_item_info;
    engine->isXattrEnabled = is_xattr_enabled;
    engine->getMaxItemSize = get_max_item_size;
    *handle = engine;
    return ENGINE_SUCCESS;
}

void destroy_engine(){

}
