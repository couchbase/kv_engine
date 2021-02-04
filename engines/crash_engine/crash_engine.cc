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

/**
 * The "crash" bucket is a bucket which simply crashes when it is initialized.
 * It is intended to be used to test crash catching using Google Breakpad.
 */
#include "crash_engine_public.h"

#include <cstdlib>
#include <stdexcept>
#include <string>

#include <memcached/config_parser.h>
#include <memcached/engine.h>
#include <platform/exceptions.h>

// How do I crash thee? Let me count the ways.
enum class CrashMode {
    SegFault,
    UncaughtStdException,
    UncaughtStdExceptionWithTrace,
    UncaughtUnknownException
};

static char dummy;

/**
 * Recursive functions which will crash using the given method after
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
        case CrashMode::UncaughtStdExceptionWithTrace:
            cb::throwWithTrace(std::runtime_error(
                    "crash_engine: This exception wasn't handled"));
        case CrashMode::UncaughtUnknownException:
            // Crash via exception not derived from std::exception
            class UnknownException {};
            throw UnknownException();
        }
    }
    recursive_crash_function(depth - char(1), mode);
    return dummy++;
}

class CrashEngine : public EngineIface {
public:
    /**
     * 'initializes' this engine - given this is the crash_engine that
     * means crashing it.
     */
    cb::engine_errc initialize(const char*) override {
        std::string mode_string(getenv("MEMCACHED_CRASH_TEST"));
        CrashMode mode;
        if (mode_string == "segfault") {
            mode = CrashMode::SegFault;
        } else if (mode_string == "std_exception") {
            mode = CrashMode::UncaughtStdException;
        } else if (mode_string == "std_exception_with_trace") {
            mode = CrashMode::UncaughtStdExceptionWithTrace;
        } else if (mode_string == "unknown_exception") {
            mode = CrashMode::UncaughtUnknownException;
        } else {
            fprintf(stderr,
                    "crash_engine::initialize: could not find a valid "
                    "CrashMode from MEMCACHED_CRASH_TEST env var ('%s')\n",
                    mode_string.c_str());
            exit(1);
        }
        return cb::engine_errc(recursive_crash_function(25, mode));
    }

    void destroy(bool) override {
        delete this;
    }

    std::pair<cb::unique_item_ptr, item_info> allocateItem(
            gsl::not_null<const void*>,
            const DocKey&,
            size_t,
            size_t,
            int,
            rel_time_t,
            uint8_t,
            Vbid) override {
        throw cb::engine_error{cb::engine_errc::failed, "crash_engine"};
    }

    cb::engine_errc remove(gsl::not_null<const void*>,
                           const DocKey&,
                           uint64_t&,
                           Vbid,
                           const std::optional<cb::durability::Requirements>&,
                           mutation_descr_t&) override {
        return cb::engine_errc::failed;
    }

    void release(gsl::not_null<ItemIface*> item) override {
    }

    cb::EngineErrorItemPair get(gsl::not_null<const void*>,
                                const DocKey&,
                                Vbid,
                                DocStateFilter) override {
        return cb::makeEngineErrorItemPair(cb::engine_errc::failed);
    }

    cb::EngineErrorItemPair get_if(
            gsl::not_null<const void*>,
            const DocKey&,
            Vbid,
            std::function<bool(const item_info&)>) override {
        return cb::makeEngineErrorItemPair(cb::engine_errc::failed);
    }

    cb::EngineErrorMetadataPair get_meta(gsl::not_null<const void*>,
                                         const DocKey&,
                                         Vbid) override {
        return {cb::engine_errc::failed, {}};
    }

    cb::EngineErrorItemPair get_locked(gsl::not_null<const void*>,
                                       const DocKey&,
                                       Vbid,
                                       uint32_t) override {
        return cb::makeEngineErrorItemPair(cb::engine_errc::failed);
    }

    cb::engine_errc unlock(gsl::not_null<const void*>,
                           const DocKey&,
                           Vbid,
                           uint64_t) override {
        return cb::engine_errc::failed;
    }

    cb::EngineErrorItemPair get_and_touch(
            gsl::not_null<const void*>,
            const DocKey&,
            Vbid,
            uint32_t,
            const std::optional<cb::durability::Requirements>&) override {
        return cb::makeEngineErrorItemPair(cb::engine_errc::failed);
    }

    cb::engine_errc store(gsl::not_null<const void*>,
                          gsl::not_null<ItemIface*>,
                          uint64_t&,
                          StoreSemantics,
                          const std::optional<cb::durability::Requirements>&,
                          DocumentState,
                          bool) override {
        return cb::engine_errc::failed;
    }

    cb::engine_errc get_stats(gsl::not_null<const void*>,
                              std::string_view,
                              std::string_view,
                              const AddStatFn&) override {
        return cb::engine_errc::failed;
    }

    void reset_stats(gsl::not_null<const void*>) override {
    }

    void item_set_cas(gsl::not_null<ItemIface*>, uint64_t) override {
    }

    void item_set_datatype(gsl::not_null<ItemIface*>,
                           protocol_binary_datatype_t) override {
    }

    bool get_item_info(gsl::not_null<const ItemIface*>,
                       gsl::not_null<item_info*>) override {
        return false;
    }

    cb::engine::FeatureSet getFeatures() override {
        return {};
    }

    cb::HlcTime getVBucketHlcNow(Vbid) override {
        return {};
    }
};

unique_engine_ptr create_crash_engine_instance() {
    return unique_engine_ptr{new CrashEngine()};
}
