/*
 *     Copyright 2020 Couchbase, Inc.
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

    int parse_config(const char* str,
                     config_item* items,
                     FILE* error) override {
        return ::parse_config(str, items, error);
    }

    ThreadPoolConfig getThreadPoolSizes() override {
        auto& instance = Settings::instance();
        return ThreadPoolConfig(instance.getNumReaderThreads(),
                                instance.getNumWriterThreads());
    }

    size_t getMaxEngineFileDescriptors() override {
        return environment.engine_file_descriptors;
    }

    bool isCollectionsEnabled() const override {
        return Settings::instance().isCollectionsEnabled();
    }

    void setStorageThreadCallback(std::function<void(size_t)> cb) override {
        storageThreadCallback = cb;
    }

    void updateStorageThreads(size_t num) {
        if (storageThreadCallback) {
            storageThreadCallback(num);
        }
    }

    std::function<void(size_t)> storageThreadCallback;
};