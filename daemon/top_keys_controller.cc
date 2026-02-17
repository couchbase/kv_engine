/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "top_keys_controller.h"

#include "front_end_thread.h"
#include <logger/logger.h>

namespace cb::trace::topkeys {

std::pair<cb::engine_errc, cb::uuid::uuid_t> Controller::create(
        std::size_t num_keys,
        std::size_t shards,
        std::chrono::seconds expiry_time,
        const std::vector<std::size_t>& buckets,
        const std::vector<CollectionIDType>& collections) {
    Expects(num_keys && "Collector must track at least 1 key");
    return active_collector.withLock([&](auto& coll) {
        return createLocked(
                coll, num_keys, shards, expiry_time, buckets, collections);
    });
}

std::pair<cb::engine_errc, nlohmann::json> Controller::stop(
        const cb::uuid::uuid_t& id, std::size_t limit) {
    auto [status, collector] = active_collector.withLock(
            [&](auto& coll) { return stopLocked(coll, id); });

    if (status == cb::engine_errc::no_such_key) {
        return {status, {}};
    }

    if (status == engine_errc::key_already_exists) {
        return {cb::engine_errc::key_already_exists,
                {{"uuid", ::to_string(collector->get_uuid())}}};
    }

    nlohmann::json result;
    try {
        result = collector->getResults(limit);
    } catch (const std::exception& e) {
        LOG_WARNING_CTX("Failed to get top keys results. Discard trace data",
                        {"error", e.what()});
        return {cb::engine_errc::failed, {}};
    }
    return {cb::engine_errc::success, std::move(result)};
}

void Controller::onExpiry(cb::uuid::uuid_t id) {
    auto data = active_collector.withLock(
            [&](auto& coll) -> std::shared_ptr<Collector> {
                if (!coll || coll->get_uuid() != id) {
                    // the collector has already been removed (either due to a
                    // previous expiry or due to a stop call) or a new collector
                    // has been created. In both cases we can just ignore the
                    // expiry.
                    return {};
                }
                if (coll.use_count() > 1) {
                    removeCollector();
                }
                std::shared_ptr<Collector> old;
                coll.swap(old);
                return old;
            });
    // Outside lock scope and we can free up memory
}

Controller& Controller::instance() {
    static Controller controller;
    return controller;
}

std::pair<cb::engine_errc, cb::uuid::uuid_t> Controller::createLocked(
        std::shared_ptr<Collector>& coll,
        std::size_t num_keys,
        std::size_t shards,
        std::chrono::seconds expiry_time,
        const std::vector<std::size_t>& buckets,
        const std::vector<CollectionIDType>& collections) {
    if (coll) {
        return {cb::engine_errc::too_busy, coll->get_uuid()};
    }

    coll = Collector::create(
            num_keys, shards, expiry_time, buckets, collections, true);
    installCollector(coll);
    return {cb::engine_errc::success, coll->get_uuid()};
}

std::pair<cb::engine_errc, std::shared_ptr<Collector>> Controller::stopLocked(
        std::shared_ptr<Collector>& coll, const cb::uuid::uuid_t& id) {
    if (!coll) {
        return {cb::engine_errc::no_such_key, {}};
    }
    if (!id.is_nil() && id != coll->get_uuid()) {
        return {cb::engine_errc::key_already_exists, coll};
    }
    // stop all the collectors
    if (coll.use_count() > 1) {
        removeCollector();
    }
    std::shared_ptr<Collector> old;
    coll.swap(old);
    return {cb::engine_errc::success, old};
}

void Controller::installCollector(const std::shared_ptr<Collector>& collector) {
    // Iterate over the front end collectors and set its collector to the
    // provided one (so that it can start tracking key access) but wait for them
    // to be installed before returning.
    FrontEndThread::forEach([&collector](auto& t) { t.keyTrace = collector; },
                            true);
}

void Controller::removeCollector() {
    FrontEndThread::forEach([](auto& t) { t.keyTrace.reset(); }, true);
}

} // namespace cb::trace::topkeys
