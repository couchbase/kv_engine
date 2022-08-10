/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "environment.h"
#include "mc_time.h"
#include "settings.h"

#include <memcached/config_parser.h>
#include <memcached/server_core_iface.h>

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

    bool isCollectionsEnabled() const override {
        return Settings::instance().isCollectionsEnabled();
    }

    bool isServerlessDeployment() const override {
        // Call isServerlessDeployment() from settings.h to get hold of the
        // state from memcached side of KV.
        return ::isServerlessDeployment();
    }
};