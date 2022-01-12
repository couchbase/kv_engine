/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "kvstore/kvstore_config.h"

#include "ep_types.h"

/**
 * Config for the NexusKVStore. We subclass for the sake of using the same API
 * for the non-nexus KVStores but we don't really care about the underlying
 * KVStoreConfig.
 */
class NexusKVStoreConfig : public KVStoreConfig {
public:
    NexusKVStoreConfig(Configuration& config,
                       std::string_view backend,
                       uint16_t numShards,
                       uint16_t shardId);

    KVStoreConfig& getPrimaryConfig() {
        return *primaryConfig;
    }

    KVStoreConfig& getSecondaryConfig() {
        return *secondaryConfig;
    }

    cb::ErrorHandlingMethod getErrorHandlingMethod() const {
        return errorHandlingMethod;
    }

    bool isImplicitCompactionEnabled() const {
        return implicitCompactionEnabled;
    }

    bool isConcurrentFlushCompactionEnabled() const {
        return concurrentFlushCompactionEnabled;
    }

protected:
    std::unique_ptr<KVStoreConfig> primaryConfig;
    std::unique_ptr<KVStoreConfig> secondaryConfig;
    cb::ErrorHandlingMethod errorHandlingMethod;
    bool implicitCompactionEnabled;
    bool concurrentFlushCompactionEnabled;
};
