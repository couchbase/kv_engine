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
#include "enginemap.h"
#include "environment.h"
#include "mc_time.h"
#include "settings.h"
#include <memcached/document_expired.h>
#include <memcached/engine.h>
#include <memcached/server_bucket_iface.h>
#include <memcached/server_core_iface.h>

struct ServerBucketApi : public ServerBucketIface {
    unique_engine_ptr createBucket(
            const std::string& module,
            ServerApi* (*get_server_api)()) const override {
        auto type = module_to_bucket_type(module);
        if (type == BucketType::Unknown) {
            return {};
        }

        try {
            return new_engine_instance(type, get_server_api);
        } catch (const std::exception&) {
            return {};
        }
    }

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

    ThreadPoolConfig getThreadPoolSizes() override {
        auto& instance = Settings::instance();
        return {instance.getNumReaderThreads(), instance.getNumWriterThreads()};
    }

    size_t getMaxEngineFileDescriptors() override {
        return environment.engine_file_descriptors;
    }

    size_t getQuotaSharingPagerConcurrency() override {
        auto& instance = Settings::instance();
        // Calculate number of concurrent paging visitors to use as a percentage
        // of the number of NonIO threads.
        int userValue = instance.getQuotaSharingPagerConcurrencyPercentage() *
                        instance.getNumNonIoThreads() / 100;
        return std::clamp(userValue, 1, instance.getNumNonIoThreads());
    }

    bool isCollectionsEnabled() const override {
        return Settings::instance().isCollectionsEnabled();
    }

    bool isServerlessDeployment() const override {
        // Call isServerlessDeployment() from settings.h to get hold of the
        // state from memcached side of KV.
        return ::isServerlessDeployment();
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
