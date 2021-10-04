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

#include "nexus-kvstore-config.h"

#include "configuration.h"

#include "kvstore/couch-kvstore/couch-kvstore-config.h"
#ifdef EP_USE_MAGMA
#include "kvstore/magma-kvstore/magma-kvstore_config.h"
#endif

#include <platform/dirutils.h>

NexusKVStoreConfig::NexusKVStoreConfig(Configuration& config,
                                       std::string_view backend,
                                       uint16_t numShards,
                                       uint16_t shardId)
    : KVStoreConfig(config, backend, numShards, shardId) {
    auto primaryBackend = config.getNexusPrimaryBackend();
    primaryConfig = KVStoreConfig::createKVStoreConfig(
            config, primaryBackend, numShards, shardId);

    primaryConfig->setDBName(primaryConfig->getDBName() +
                             cb::io::DirectorySeparator + "nexus-primary");

    auto secondaryBackend = config.getNexusSecondaryBackend();
    secondaryConfig = KVStoreConfig::createKVStoreConfig(
            config, secondaryBackend, numShards, shardId);

    secondaryConfig->setDBName(secondaryConfig->getDBName() +
                               cb::io::DirectorySeparator + "nexus-secondary");

    // Nexus needs compaction to expire items from the same time point to assert
    // that items expired by both KVStores are the same.
    config.setCompactionExpireFromStart(true);

    if (primaryBackend == "magma") {
#ifdef EP_USE_MAGMA
        auto& magmaKVStoreConfig =
                dynamic_cast<MagmaKVStoreConfig&>(*primaryConfig);
        magmaKVStoreConfig.setMagmaSyncEveryBatch(true);
        magmaKVStoreConfig.setMagmaCheckpointInterval(
                std::chrono::milliseconds(0));
#endif
    } else if (primaryBackend == "couchdb") {
        // Couchstore has an optimization that forces rollback to zero should
        // we try to roll back more than 50% of the seqnos. Magma doesn't have
        // this so we need to turn it off for couchstore to ensure comparability
        dynamic_cast<CouchKVStoreConfig&>(*primaryConfig)
                .setMidpointRollbackOptimisation(false);
    }

    if (secondaryBackend == "magma") {
#ifdef EP_USE_MAGMA
        auto& magmaKVStoreConfig =
                dynamic_cast<MagmaKVStoreConfig&>(*secondaryConfig);
        magmaKVStoreConfig.setMagmaSyncEveryBatch(true);
        magmaKVStoreConfig.setMagmaCheckpointInterval(
                std::chrono::milliseconds(0));
#endif
    } else if (primaryBackend == "couchdb") {
        // Couchstore has an optimization that forces rollback to zero should
        // we try to roll back more than 50% of the seqnos. Magma doesn't have
        // this so we need to turn it off for couchstore to ensure comparability
        dynamic_cast<CouchKVStoreConfig&>(*secondaryConfig)
                .setMidpointRollbackOptimisation(false);
    }

    errorHandlingMethod =
            cb::getErrorHandlingMethod(config.getNexusErrorHandling());
}