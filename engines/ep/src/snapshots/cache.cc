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
#include <folly/Synchronized.h>
#include <memcached/engine_error.h>
#include <nlohmann/json.hpp>
#include <platform/dirutils.h>
#include <platform/uuid.h>
#include <statistics/collector.h>

namespace cb::snapshot {

cb::engine_errc Cache::initialise() {
    std::error_code ec;
    for (const auto& entry : std::filesystem::directory_iterator(path, ec)) {
        if (is_directory(entry.path(), ec) &&
            exists(entry.path() / "manifest.json", ec)) {
            try {
                Manifest manifest = nlohmann::json::parse(
                        cb::io::loadFile(entry.path() / "manifest.json"));
                snapshots.withLock([&manifest](auto& map) {
                    map.emplace(manifest.uuid, Entry(manifest));
                });
            } catch (const std::exception&) {
                // We failed to parse the entry.. just remove it.
                remove_all(entry.path(), ec);
                return cb::engine_errc::failed;
            }
        }
    }
    return cb::engine_errc::success;
}

std::optional<Manifest> Cache::lookup(const std::string& uuid) const {
    return snapshots.withLock([&uuid](auto& map) -> std::optional<Manifest> {
        auto iter = map.find(uuid);
        if (iter == map.end()) {
            return std::nullopt;
        }
        iter->second.timestamp = std::chrono::steady_clock::now();
        return iter->second.manifest;
    });
}

std::optional<Manifest> Cache::lookup(const Vbid vbid) const {
    return snapshots.withLock([&vbid](auto& map) -> std::optional<Manifest> {
        for (auto& [uuid, entry] : map) {
            if (entry.manifest.vbid == vbid) {
                entry.timestamp = std::chrono::steady_clock::now();
                return entry.manifest;
            }
        }
        return std::nullopt;
    });
}

void Cache::remove(const Manifest& manifest) const {
    std::error_code ec;
    if (!remove_all(path / manifest.uuid, ec)) {
        EP_LOG_WARN_CTX("Failed to remove snapshot",
                        {{"uuid", manifest.uuid}, {"error", ec.message()}});
    }
}

void Cache::release(const std::string& uuid) {
    snapshots.withLock([&uuid, this](auto& map) {
        auto iter = map.find(uuid);
        if (iter != map.end()) {
            remove(iter->second.manifest);
            map.erase(iter);
        }
    });
}

void Cache::release(Vbid vbid) {
    snapshots.withLock([vbid, this](auto& map) {
        for (const auto& [uuid, entry] : map) {
            if (entry.manifest.vbid == vbid) {
                remove(entry.manifest);
                map.erase(uuid);
                return;
            }
        }
    });
}

void Cache::purge(std::chrono::seconds age) {
    snapshots.withLock([&age, this](auto& map) {
        const auto tp = std::chrono::steady_clock::now() - age;
        std::vector<std::string> uuids;

        for (auto& [uuid, entry] : map) {
            if (entry.timestamp > tp) {
                remove(entry.manifest);
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
    if (!snapshots.withLock([manifest](auto& map) {
            return map.try_emplace(manifest.uuid, Entry(manifest)).second;
        })) {
        EP_LOG_WARN_CTX("Cache::prepare try_emplace failed",
                        {{"uuid", manifest.uuid}});
        return cb::engine_errc::failed;
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
    FILE* fp = fopen((path / manifest.uuid / "manifest.json").string().c_str(),
                     "w");
    if (!fp) {
        return cb::engine_errc::failed;
        }

        fprintf(fp, "%s\n", nlohmann::json(manifest).dump().c_str());
        fclose(fp);
        return rv;
}

std::variant<cb::engine_errc, Manifest> Cache::download(
        Vbid vbid,
        const std::function<std::variant<cb::engine_errc, Manifest>()>&
                fetch_manifest,
        const std::function<cb::engine_errc(const std::filesystem::path&,
                                            const Manifest&)>& download_files,
        const std::function<void(std::string_view)>& release_snapshot) {
    auto fetched = lookupOrFetch(vbid, fetch_manifest);
    if (std::holds_alternative<cb::engine_errc>(fetched)) {
        return fetched;
    }
    const auto& manifest = std::get<Manifest>(fetched);

    auto rv = download_files(path / manifest.uuid, manifest);
    if (rv != engine_errc::success) {
        std::error_code ec;
        remove_all(path / manifest.uuid, ec);
        return cb::engine_errc::failed;
    }

    snapshots.withLock([&manifest](auto& map) {
        map.emplace(manifest.uuid, Entry(manifest));
    });

    release_snapshot(manifest.uuid);
    return manifest;
}

std::filesystem::path Cache::make_absolute(
        const std::filesystem::path& relative, std::string_view uuid) const {
    return path / uuid / relative;
}

void Cache::addDebugStats(const StatCollector& collector) const {
    // Lock the map, in general would prefer keep locking scope minimal but
    // this stat collection is for debug usage (cbcollect) and not operational
    snapshots.withLock([&collector](auto& map) {
        collector.addStat("snapshots_size", map.size());

        for (const auto& [uuid, entry] : map) {
            entry.addDebugStats(collector);
        }
    });
}

void Cache::Entry::addDebugStats(const StatCollector& collector) const {
    collector.addStat(
            std::string_view{fmt::format("vb_{}:age", manifest.vbid.get())},
            std::chrono::duration_cast<std::chrono::seconds>(
                    std::chrono::steady_clock::now() - timestamp)
                    .count());
    manifest.addDebugStats(collector);
}

} // namespace cb::snapshot