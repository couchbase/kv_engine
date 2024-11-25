/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <folly/Synchronized.h>
#include <memcached/engine_error.h>
#include <snapshot/manifest.h>
#include <iostream>
#include <optional>
#include <string>
#include <variant>

namespace cb::snapshot {

/**
 * The snapshot Cache class is responsible for maintaining a
 * write-through cache of all snapshots available for a bucket. All
 * snapshots are described through a Manifest object and is identified
 * by a UUID. There may only be a single snapshot created at any point in
 * time for a given VB. This is controlled and enforced by the core which
 * will serialize all snapshot creation (@todo - there is a potential race
 * if one tries to do a download of a snapshot on a node where the vbucket
 * is active etc fix that..).
 *
 * A new snapshot is created by calling prepare() and where one provides
 * the vbucket and a callback function. If a snapshot exists for the vbucket
 * that snapshot gets returned. If no snapshot exists a new directory gets
 * created and the callback is called with the directory, vbucket and a
 * manifest object. The callback may populate the directory with all files
 * which should go into the snapshot and add them to the manifest object.
 * The snapshot gets inserted to the cache and subsequent lookups with the
 * UUID or vbucket will return that snapshot (until removed by a timer or
 * from an explicit release)
 *
 * When one wants to download a snapshot from a *remote* server one would
 * use the download method. It takes 3 different callabcks in addition to the
 * requested vbucket. If the snapshot doesn't exist the first callback gets
 * executed, and its job is to retrieve the manifest from the remote server
 * and store it on this server. If a snapshot exist then it is selected instead.
 * Then the second callback gets executed and this callback is supposed
 * to download (and check) the files within the manifest and if that callback
 * succeeds the last callback gets called to allow the caller to release
 * the manifest on the remote server. At this time the snapshould should be
 * available locally and may be fetched from one of the "lookup" methods.
 */
class Cache {
public:
    Cache(const std::filesystem::path& path) : path(path / "snapshots") {
    }

    /// @return true if the manifest was added to cache (fail means duplicate)
    bool insert(Manifest manifest);

    /// Look up a snapshot with the provided UUID
    std::optional<Manifest> lookup(const std::string& uuid) const;

    /// Look up the "current" snapshot for the provided VB
    std::optional<Manifest> lookup(Vbid vbid) const;

    /**
     * Prepare (or use an existing) snapshot for the provided vbucket
     *
     * @param vb the vbucket to get the snapshot for
     * @param executor An executor to use to generate a new snapshot if none
     *                 exists for the vbucket
     * @returns the failure reason if the operation failed
     *          the created manifest upon success
     */
    std::variant<cb::engine_errc, Manifest> prepare(
            Vbid vb,
            const std::function<std::variant<cb::engine_errc, Manifest>(
                    const std::filesystem::path&, Vbid)>& executor);

    /**
     * Prepare a remote snapshot. This involves preparing the snapshot
     * on the remote node (if we don't have a local copy), download all files
     * and finally release the snapshot on the remote host.
     *
     * @param vbid The vbucket to create prepare the snapshot for
     * @param fetch_manifest The callback to fetch the manifest if we don't
     *                       have a local manifest
     * @param download_files The callback to inspect the snapshot and
     *                       potentially continue downloading files
     * @param release_snapshot The callback to release the snapshot on the
     *                         remote node (only called if download_callback
     *                         returns success)
     * @returns the failure reason if the operation failed
     *          the created manifest upon success
     */
    std::variant<cb::engine_errc, Manifest> download(
            Vbid vbid,
            const std::function<std::variant<cb::engine_errc, Manifest>()>&
                    fetch_manifest,
            const std::function<cb::engine_errc(const std::filesystem::path&,
                                                const Manifest&)>&
                    download_files,
            const std::function<void(std::string_view)>& release_snapshot);

    /**
     * Convert a relative path from within a snapshot to an absolute path
     *
     * @param relative the path which is relative to the snapshot
     * @param uuid the snapshot uuid
     * @return an absolute path on disk for the requested file
     */
    std::filesystem::path make_absolute(const std::filesystem::path& relative,
                                        std::string_view uuid) const;

    /// Release (and delete) a snapshot identified with the provided UUID
    void release(const std::string& uuid);

    /// Release (and delete) a snapshot for the VB
    void release(Vbid vbid);

    /// Remove all snapshots older than the provided age
    void purge(std::chrono::seconds age);

    /// produce stats for the cache which may be useful for debugging/cbcollect
    void addDebugStats(const StatCollector& collector) const;

    void dump(std::ostream& os = std::cerr) const;

protected:
    /// Nuke a manifest from disk
    void remove(const Manifest& manifest) const;

    std::variant<cb::engine_errc, Manifest> lookupOrFetch(
            Vbid vbid,
            const std::function<std::variant<cb::engine_errc, Manifest>()>&
                    fetch_manifest);

    /// The location of the snapshots
    std::filesystem::path path;

    struct Entry {
        explicit Entry(Manifest m) : manifest(std::move(m)) {
        }
        mutable std::chrono::steady_clock::time_point timestamp =
                std::chrono::steady_clock::now();
        Manifest manifest;
        void addDebugStats(const StatCollector& collector) const;
    };
    folly::Synchronized<std::map<std::string, Entry>, std::mutex> snapshots;
};

std::ostream& operator<<(std::ostream& os, const Cache& version);

} // namespace cb::snapshot
