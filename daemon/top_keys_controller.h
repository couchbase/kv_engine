/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "top_keys.h"

#include <executor/globaltask.h>
#include <memcached/engine_error.h>
#include <platform/uuid.h>

#include <memory>
#include <vector>

namespace cb::trace::topkeys {

class Controller {
public:
    /**
     * Create a Collector instance used to track key access and install
     * it on the front end threads (so that it can start tracking key access).
     *
     * @param num_keys The maximum number of keys to track in a shard
     * @param shards The number of internal maps to shard the keyspace across
     * @param expiry_time The time after which the collected data should be
     *                    considered expired and can be discarded (and the
     *                    collector can be reset)
     * @param buckets If non-empty only track keys accessed in the specified
     *                bucket. For space efficiencty we use the bucket *id*
     *                and not the bucket name (as buckets typically don't
     *                have a long lifetime and we want to avoid storing the
     *                bucket name in each shard map)
     *
     * @return the uuid of the created collector which can be used to
     *         stop the collector and retrieve the collected data or an
     *         error code if the collector couldn't be created.
     *
     * @throws std::exception subclass if errors occurs
     */
    std::pair<cb::engine_errc, cb::uuid::uuid_t> create(
            std::size_t num_keys,
            std::size_t shards,
            std::chrono::seconds expiry_time,
            const std::vector<std::size_t>& buckets = {},
            const std::vector<CollectionIDType>& collections = {});

    std::pair<cb::engine_errc, nlohmann::json> stop(const cb::uuid::uuid_t& id,
                                                    std::size_t limit = 100);

    /// Deal with the expiration of a collector (called by the
    /// expired_tasks_remover task when a collector expires). The collector will
    /// be removed and the collected data will be discarded.
    void onExpiry(cb::uuid::uuid_t id);

    static Controller& instance();

protected:
    Controller() = default;

    std::pair<cb::engine_errc, cb::uuid::uuid_t> createLocked(
            std::shared_ptr<Collector>& coll,
            std::size_t num_keys,
            std::size_t shards,
            std::chrono::seconds expiry_time,
            const std::vector<std::size_t>& buckets,
            const std::vector<CollectionIDType>& collections);
    std::pair<cb::engine_errc, std::shared_ptr<Collector>> stopLocked(
            std::shared_ptr<Collector>& coll, const cb::uuid::uuid_t& id);

    void installCollector(const std::shared_ptr<Collector>& collector);
    void removeCollector();

    ExTask expired_tasks_remover;
    folly::Synchronized<std::shared_ptr<Collector>, std::mutex>
            active_collector;
};

} // namespace cb::trace::topkeys
