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

std::string Cache::calculateSha512sum(const std::filesystem::path& path,
                                      std::size_t size) const {
    try {
        return cb::crypto::sha512sum(path, size);
    } catch (const std::exception& e) {
        EP_LOG_WARN_CTX("Failed calculating sha512",
                        {"path", path.string()},
                        {"size", size},
                        {"error", e.what()});
    }
    return {};
}

void Cache::maybeUpdateSha512Sum(const std::filesystem::path& root,
                                 FileInfo& info) const {
    if (info.sha512.empty()) {
        auto sum = calculateSha512sum(root / info.path, info.size);
        if (!sum.empty()) {
            info.sha512 = sum;
        }
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
        const std::function<cb::engine_errc(
                const std::filesystem::path&, Vbid, Manifest&)>& executor) {
    auto existing = lookup(vbid);
    if (existing.has_value()) {
        return *existing;
    }

    Manifest manifest;

    manifest.uuid = ::to_string(cb::uuid::random());
    manifest.vbid = vbid;
    create_directories(path / manifest.uuid);
    cb::engine_errc rv;
    try {
        rv = executor(path / manifest.uuid, vbid, manifest);
    } catch (const std::exception&) {
        std::error_code ec;
        remove_all(path / manifest.uuid, ec);
        throw;
    }

    if (rv == engine_errc::success) {
        // Add checksums for all files in the snapshot
        for (auto& file : manifest.files) {
            maybeUpdateSha512Sum(path / manifest.uuid, file);
        }
        for (auto& file : manifest.deks) {
            maybeUpdateSha512Sum(path / manifest.uuid, file);
        }

        FILE* fp = fopen(
                (path / manifest.uuid / "manifest.json").string().c_str(), "w");
        if (fp) {
            fprintf(fp, "%s\n", nlohmann::json(manifest).dump().c_str());
            fclose(fp);
            snapshots.withLock([&manifest](auto& map) {
                map.emplace(manifest.uuid, Entry(manifest));
            });
            return manifest;
        }
        rv = cb::engine_errc::failed;
    }

    std::error_code ec;
    remove_all(path / manifest.uuid, ec);
    return rv;
}

std::variant<cb::engine_errc, Manifest> Cache::download(
        Vbid vbid,
        const std::function<cb::engine_errc(Manifest&)>& fetch_manifest,
        const std::function<cb::engine_errc(const std::filesystem::path&,
                                            Manifest&)>& download_files,
        const std::function<void(std::string_view)>& release_snapshot) {
    Manifest manifest;

    auto existing = lookup(vbid);
    if (existing.has_value()) {
        manifest = *existing;
    } else {
        auto rv = fetch_manifest(manifest);
        if (rv != engine_errc::success) {
            return rv;
        }

        std::error_code ec;
        remove_all(path / manifest.uuid, ec);
        create_directories(path / manifest.uuid);
        FILE* fp = fopen(
                (path / manifest.uuid / "manifest.json").string().c_str(), "w");
        if (!fp) {
            return cb::engine_errc::failed;
        }

        fprintf(fp, "%s\n", nlohmann::json(manifest).dump().c_str());
        fclose(fp);
    }

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
} // namespace cb::snapshot