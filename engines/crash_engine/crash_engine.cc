/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/**
 * The "crash" bucket is a bucket which simply crashes when it is initialized.
 * It is intended to be used to test crash catching using Google Breakpad.
 */
#include "crash_engine_public.h"

#include <cstdlib>
#include <stdexcept>
#include <string>
#include <thread>

#include <folly/lang/Assume.h>
#include <memcached/engine.h>
#include <platform/exceptions.h>

#if defined __GNUC__
#define MEMCACHED_PUBLIC_API __attribute__((visibility("default")))
#else
#define MEMCACHED_PUBLIC_API
#endif

// How do I crash thee? Let me count the ways.
enum class CrashMode {
    SegFault,
    UncaughtStdException,
    UncaughtStdExceptionViaStdThread,
    UncaughtStdExceptionWithTrace,
    UncaughtUnknownException
};

static volatile char* death = nullptr;

/**
 * Recursive functions which will crash using the given method after
 * 'depth' calls.
 */
MEMCACHED_PUBLIC_API FOLLY_NOINLINE char recursive_crash_function(
        char depth, CrashMode mode) {
    if (depth == 0) {
        switch (mode) {
        case CrashMode::SegFault: {
            return *death;
        }
        case CrashMode::UncaughtStdException:
        case CrashMode::UncaughtStdExceptionViaStdThread:
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
    return 0;
}

class CrashEngine : public EngineIface {
public:
    /**
     * 'initializes' this engine - given this is the crash_engine that
     * means crashing it.
     */
    cb::engine_errc initialize(std::string_view) override {
        std::string mode_string(getenv("MEMCACHED_CRASH_TEST"));
        CrashMode mode;
        if (mode_string == "segfault") {
            mode = CrashMode::SegFault;
        } else if (mode_string == "std_exception") {
            mode = CrashMode::UncaughtStdException;
        } else if (mode_string == "std_exception_via_std_thread") {
            mode = CrashMode::UncaughtStdExceptionViaStdThread;
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
        if (mode == CrashMode::UncaughtStdExceptionViaStdThread) {
            std::thread thread{[mode] { recursive_crash_function(25, mode); }};
            thread.join();
            // Thread should crash before it joins.
            folly::assume_unreachable();
        } else {
            return cb::engine_errc(recursive_crash_function(25, mode));
        }
    }

    void destroy(bool) override {
        delete this;
    }

    cb::unique_item_ptr allocateItem(CookieIface&,
                                     const DocKey&,
                                     size_t,
                                     size_t,
                                     int,
                                     rel_time_t,
                                     uint8_t,
                                     Vbid) override {
        throw cb::engine_error{cb::engine_errc::failed, "crash_engine"};
    }

    cb::engine_errc remove(CookieIface&,
                           const DocKey&,
                           uint64_t&,
                           Vbid,
                           const std::optional<cb::durability::Requirements>&,
                           mutation_descr_t&) override {
        return cb::engine_errc::failed;
    }

    void release(ItemIface& item) override {
    }

    cb::EngineErrorItemPair get(CookieIface&,
                                const DocKey&,
                                Vbid,
                                DocStateFilter) override {
        return cb::makeEngineErrorItemPair(cb::engine_errc::failed);
    }

    cb::EngineErrorItemPair get_if(
            CookieIface&,
            const DocKey&,
            Vbid,
            std::function<bool(const item_info&)>) override {
        return cb::makeEngineErrorItemPair(cb::engine_errc::failed);
    }

    cb::EngineErrorMetadataPair get_meta(CookieIface&,
                                         const DocKey&,
                                         Vbid) override {
        return {cb::engine_errc::failed, {}};
    }

    cb::EngineErrorItemPair get_locked(CookieIface&,
                                       const DocKey&,
                                       Vbid,
                                       uint32_t) override {
        return cb::makeEngineErrorItemPair(cb::engine_errc::failed);
    }

    cb::engine_errc unlock(CookieIface&,
                           const DocKey&,
                           Vbid,
                           uint64_t) override {
        return cb::engine_errc::failed;
    }

    cb::EngineErrorItemPair get_and_touch(
            CookieIface&,
            const DocKey&,
            Vbid,
            uint32_t,
            const std::optional<cb::durability::Requirements>&) override {
        return cb::makeEngineErrorItemPair(cb::engine_errc::failed);
    }

    cb::engine_errc store(CookieIface&,
                          ItemIface&,
                          uint64_t&,
                          StoreSemantics,
                          const std::optional<cb::durability::Requirements>&,
                          DocumentState,
                          bool) override {
        return cb::engine_errc::failed;
    }

    cb::engine_errc get_stats(CookieIface&,
                              std::string_view,
                              std::string_view,
                              const AddStatFn&) override {
        return cb::engine_errc::failed;
    }

    void reset_stats(CookieIface&) override {
    }

    bool get_item_info(const ItemIface&, item_info&) override {
        return false;
    }

    cb::engine::FeatureSet getFeatures() override {
        return {};
    }

    std::optional<cb::HlcTime> getVBucketHlcNow(Vbid) override {
        return {};
    }
};

unique_engine_ptr create_crash_engine_instance() {
    return unique_engine_ptr{new CrashEngine()};
}
