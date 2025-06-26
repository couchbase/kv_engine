/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "cache.h"

#include <bucket_logger.h>
#include <cbcrypto/digest.h>
#include <folly/ScopeGuard.h>
#include <folly/Synchronized.h>
#include <memcached/engine_error.h>
#include <nlohmann/json.hpp>
#include <platform/dirutils.h>
#include <platform/uuid.h>
#include <statistics/collector.h>

namespace cb::snapshot {

bool Cache::insert(Manifest manifest) {
    return snapshots
            .withLock([&manifest, time = time()](auto& map) {
                return map.try_emplace(manifest.uuid, Entry(manifest, time));
            })
            .second;
}

std::optional<Manifest> Cache::lookup(const std::string& uuid) const {
    return snapshots.withLock(
            [&uuid, time = time()](auto& map) -> std::optional<Manifest> {
                auto iter = map.find(uuid);
                if (iter == map.end()) {
                    return std::nullopt;
                }
                iter->second.timestamp = time;
                return iter->second.manifest;
            });
}

std::optional<Manifest> Cache::lookup(const Vbid vbid) const {
    return snapshots.withLock(
            [&vbid, time = time()](auto& map) -> std::optional<Manifest> {
                for (auto& [uuid, entry] : map) {
                    if (entry.manifest.vbid == vbid) {
                        entry.timestamp = time;
                        return entry.manifest;
                    }
                }
                return std::nullopt;
            });
}

cb::engine_errc Cache::remove(std::string_view uuid) const {
    std::error_code ec;
    if (!remove_all(path / uuid, ec)) {
        EP_LOG_WARN_CTX("Cache::remove failed to remove snapshot",
                        {{"uuid", uuid}, {"error", ec.message()}});
        return cb::engine_errc::failed;
    }
    return cb::engine_errc::success;
}

cb::engine_errc Cache::release(const std::string& uuid) {
    return snapshots.withLock([&uuid, this](auto& map) {
        auto iter = map.find(uuid);
        if (iter != map.end()) {
            map.erase(iter);
            return remove(uuid);
        }
        return cb::engine_errc::no_such_key;
    });
}

cb::engine_errc Cache::release(Vbid vbid) {
    return snapshots.withLock([vbid, this](auto& map) {
        for (const auto& [uuid, entry] : map) {
            if (entry.manifest.vbid == vbid) {
                auto victim = entry.manifest.uuid;
                map.erase(victim);
                return remove(victim);
            }
        }
        return cb::engine_errc::no_such_key;
    });
}

void Cache::purge(std::chrono::seconds age) {
    snapshots.withLock([&age, this, time = time()](auto& map) {
        const auto limit = time - age;
        std::vector<std::string> uuids;

        for (auto& [uuid, entry] : map) {
            // Remove all snapshots that have a timestamp below the limit, i.e.
            // they've not been used for age seconds.
            if (entry.timestamp < limit) {
                remove(entry.manifest.uuid);
                uuids.emplace_back(entry.manifest.uuid);
            }
        }

        for (const auto& uuid : uuids) {
            map.erase(uuid);
        }
    });
}

std::variant<cb::engine_errc, Manifest> Cache::prepare(
        Vbid vbid,
        const std::function<std::variant<cb::engine_errc, Manifest>(
                const std::filesystem::path&, Vbid)>& prepare) {
    auto existing = lookup(vbid);
    if (existing.has_value()) {
        return *existing;
    }

    auto prepared = prepare(path, vbid);
    if (std::holds_alternative<cb::engine_errc>(prepared)) {
        return prepared;
    }

    // Save the manfiest
    const auto& manifest = std::get<cb::snapshot::Manifest>(prepared);
    if (!snapshots.withLock([manifest, time = time()](auto& map) {
            return map.try_emplace(manifest.uuid, Entry(manifest, time)).second;
        })) {
        EP_LOG_WARN_CTX("Cache::prepare try_emplace failed",
                        {{"uuid", manifest.uuid}});
        (void)remove(manifest.uuid);
        return cb::engine_errc::key_already_exists;
    }
    return prepared;
}

std::variant<cb::engine_errc, Manifest> Cache::lookupOrFetch(
        Vbid vbid,
        const std::function<std::variant<cb::engine_errc, Manifest>()>&
                fetch_manifest) {
    auto existing = lookup(vbid);
    if (existing.has_value()) {
        return *existing;
    }
    auto rv = fetch_manifest();
    if (std::holds_alternative<cb::engine_errc>(rv)) {
        return rv;
    }
    const auto& manifest = std::get<Manifest>(rv);

    EP_LOG_INFO_CTX("Downloaded snapshot manifest",
                    {"vb", vbid},
                    {"uuid", manifest.uuid});

    std::error_code ec;
    remove_all(path / manifest.uuid, ec);
    create_directories(path / manifest.uuid);

    // If failing in this scope clean-up
    auto removeSnapshot =
            folly::makeGuard([this, &manifest]() { remove(manifest.uuid); });

    const auto manifestPath = path / manifest.uuid / "manifest.json";
    if (!cb::io::saveFile(manifestPath, nlohmann::json(manifest).dump(), ec)) {
        EP_LOG_WARN_CTX("Cache::lookupOrFetch Failed to store manifest",
                        {"vbid", vbid},
                        {"error", ec.message()},
                        {"path", manifestPath});
        return cb::engine_errc::failed;
    }

    // At this point we have a snapshot dir and manifest.json, we can
    // consider this a "valid" snapshot. Crash here and we would re-add
    // to the Cache but with the files marked Present/Absent/Truncated
    removeSnapshot.dismiss();
    return rv;
}

std::variant<cb::engine_errc, Manifest> Cache::download(
        Vbid vbid,
        const std::function<std::variant<cb::engine_errc, Manifest>()>&
                fetch_manifest,
        const std::function<cb::engine_errc(const std::filesystem::path&,
                                            const Manifest&)>& download_files) {
    auto fetched = lookupOrFetch(vbid, fetch_manifest);
    if (std::holds_alternative<cb::engine_errc>(fetched)) {
        return fetched;
    }
    const auto& manifest = std::get<Manifest>(fetched);

    auto rv = download_files(path / manifest.uuid, manifest);
    if (rv != engine_errc::success) {
        std::error_code ec;
        remove_all(path / manifest.uuid, ec);
        return rv;
    }

    if (!snapshots.withLock([&manifest, time = time()](auto& map) {
            return map.try_emplace(manifest.uuid, Entry(manifest, time)).second;
        })) {
        EP_LOG_WARN_CTX("Cache::download try_emplace failed",
                        {{"uuid", manifest.uuid}});
        return cb::engine_errc::key_already_exists;
    }

    return manifest;
}

std::filesystem::path Cache::make_absolute(
        const std::filesystem::path& relative, std::string_view uuid) const {
    return path / uuid / relative;
}

void Cache::addDebugStats(const StatCollector& collector) const {
    // Lock the map, in general would prefer keep locking scope minimal but
    // this stat collection is for debug usage (cbcollect) and not operational
    snapshots.withLock([&collector, time = time()](auto& map) {
        collector.addStat("snapshots_size", map.size());

        for (const auto& [uuid, entry] : map) {
            entry.addDebugStats(collector, time);
        }
    });
}

void Cache::dump(std::ostream& os) const {
    snapshots.withLock([this, &os](auto& map) {
        for (const auto& [uuid, entry] : map) {
            nlohmann::json json;
            to_json(json, entry.manifest);
            auto age = time() - entry.timestamp;
            os << "age:" << age.count() << " " << json.dump() << std::endl;
        }
    });
}

void Cache::Entry::addDebugStats(const StatCollector& collector,
                                 cb::time::steady_clock::time_point now) const {
    collector.addStat(
            std::string_view{fmt::format("vb_{}:age", manifest.vbid.get())},
            std::chrono::duration_cast<std::chrono::seconds>(now - timestamp)
                    .count());
    manifest.addDebugStats(collector);
}

std::ostream& operator<<(std::ostream& os, const Cache& cache) {
    cache.dump(os);
    return os;
}

} // namespace cb::snapshot