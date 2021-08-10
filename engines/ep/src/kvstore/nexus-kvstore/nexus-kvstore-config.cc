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

    auto errorHandling = config.getNexusErrorHandling();
    if (errorHandling == "abort") {
        errorHandlingMethod = NexusErrorHandlingMethod::Abort;
    } else if (errorHandling == "log") {
        errorHandlingMethod = NexusErrorHandlingMethod::Log;
    } else if (errorHandling == "throw") {
        errorHandlingMethod = NexusErrorHandlingMethod::Throw;
    } else {
        throw std::logic_error(
                "NexusKVStoreConfig::NexusKVStoreConfig invalid errorHandling "
                "parameter given");
    }
}