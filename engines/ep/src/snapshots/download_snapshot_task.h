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

#include "ep_task.h"
#include <memcached/vbucket.h>
#include <protocol/connection/client_connection.h>
#include <snapshot/download_properties.h>
#include <snapshot/manifest.h>
#include <variant>

namespace cb {
enum class engine_errc;
}
class CookieIface;

namespace cb::snapshot {
class Cache;

/**
 * DownloadSnapshotTask is used to download a full snapshot from another
 * server.
 */
class DownloadSnapshotTask : public EpTask {
public:
    DownloadSnapshotTask(CookieIface& cookie,
                         EventuallyPersistentEngine& ep,
                         Cache& manager,
                         Vbid vbid,
                         const nlohmann::json& manifest);

    std::string getDescription() const override {
        return description;
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        // @todo this could be deducted from the total size
        return std::chrono::seconds(30);
    }

    std::pair<cb::engine_errc, std::string> getResult() const {
        return result.copy();
    }

protected:
    std::variant<cb::engine_errc, Manifest> doDownloadManifest();
    cb::engine_errc doDownloadFiles(std::filesystem::path dir,
                                    const Manifest& manifest);
    void doReleaseSnapshot(std::string_view uuid);

    MemcachedConnection& getConnection();
    std::unique_ptr<MemcachedConnection> connection;

    bool run() override;
    /// The description of the task to return to the framework (as it contains
    /// per-task data we don't want to have to reformat that every time)
    const std::string description;
    /// The cookie requested the operation
    CookieIface& cookie;
    /// The snapshot cache to help on asist in the download (in order to
    /// continue a partially downloaded snapshot etc)
    Cache& manager;
    /// The vbucket to download the snapshot for
    const Vbid vbid;
    /// The properties to use for the download (host, credentials etc)
    const DownloadProperties properties;
    /// The result of the operation to pass on to the front end thread when
    /// the task is done. The pair consists of an error code and a string
    /// which may contain extra information to pass along back to the client.
    folly::Synchronized<std::pair<cb::engine_errc, std::string>, std::mutex>
            result;
};

} // namespace cb::snapshot
