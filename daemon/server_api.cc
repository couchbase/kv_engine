/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "buckets.h"
#include "environment.h"
#include "mc_time.h"
#include "memcached.h"
#include "settings.h"
#include <executor/executorpool.h>
#include <memcached/document_expired.h>
#include <memcached/engine.h>
#include <memcached/server_bucket_iface.h>
#include <memcached/server_core_iface.h>

struct ServerBucketApi : public ServerBucketIface {
    std::optional<AssociatedBucketHandle> tryAssociateBucket(
            EngineIface* engine) const override {
        auto* bucket = BucketManager::instance().tryAssociateBucket(engine);
        if (!bucket) {
            return {};
        }

        return AssociatedBucketHandle(engine, [bucket](EngineIface*) {
            BucketManager::instance().disassociateBucket(bucket);
        });
    }
};

struct ServerCoreApi : public ServerCoreIface {
    std::chrono::steady_clock::time_point get_uptime_now() override {
        return mc_time_uptime_now();
    }

    rel_time_t get_current_time() override {
        return mc_time_get_current_time();
    }

    rel_time_t realtime(rel_time_t exptime) override {
        return mc_time_convert_to_real_time(exptime);
    }

    time_t abstime(rel_time_t exptime) override {
        return mc_time_convert_to_abs_time(exptime);
    }

    time_t limit_abstime(time_t t, std::chrono::seconds limit) override {
        return mc_time_limit_abstime(t, limit);
    }

    size_t getMaxEngineFileDescriptors() override {
        return environment.engine_file_descriptors;
    }

    size_t getQuotaSharingPagerConcurrency() override {
        auto& instance = Settings::instance();
        auto numNonIO = ExecutorPool::get()->getNumNonIO();
        // Calculate number of concurrent paging visitors to use as a percentage
        // of the number of NonIO threads.
        auto userValue = instance.getQuotaSharingPagerConcurrencyPercentage() *
                         numNonIO / 100;
        return std::clamp(userValue, size_t(1), numNonIO);
    }

    std::chrono::milliseconds getQuotaSharingPagerSleepTime() override {
        return Settings::instance().getQuotaSharingPagerSleepTime();
    }
};

void cb::server::document_expired(const EngineIface& engine, size_t nbytes) {
    BucketManager::instance().forEach([&engine, nbytes](Bucket& bucket) {
        if (bucket.type != BucketType::ClusterConfigOnly &&
            &engine == &bucket.getEngine()) {
            bucket.documentExpired(nbytes);
            return false;
        }
        return true;
    });
}

class ServerApiImpl : public ServerApi {
public:
    ServerApiImpl() : ServerApi() {
        core = &core_api;
        bucket = &bucket_api;
    }

protected:
    ServerCoreApi core_api;
    ServerBucketApi bucket_api;
};

/**
 * Callback the engines may call to get the public server interface
 * @return pointer to a structure containing the interface. The client should
 *         know the layout and perform the proper casts.
 */
ServerApi* get_server_api() {
    static ServerApiImpl rv;
    return &rv;
}
