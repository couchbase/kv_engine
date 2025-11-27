/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "download_snapshot_task.h"

#include <bucket_logger.h>
#include <ep_engine.h>
#include <memcached/cookie_iface.h>
#include <nlohmann/json.hpp>
#include <platform/dirutils.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <snapshot/download_properties.h>
#include <snapshot/manifest.h>
#include <snapshot/snapshot_downloader.h>
#include <snapshots/cache.h>

namespace cb::snapshot {

DownloadSnapshotTask::DownloadSnapshotTask(
        EventuallyPersistentEngine& ep,
        Cache& manager,
        std::shared_ptr<DownloadSnapshotTaskListener> listener,
        Vbid vbid,
        const nlohmann::json& manifest)
    : EpTask(ep, TaskId::DownloadSnapshotTask),
      description(fmt::format("Download vbucket snapshot for {}", vbid)),
      manager(manager),
      listener(std::move(listener)),
      vbid(vbid),
      properties(manifest) {
    // empty
}

void DownloadSnapshotTask::createConnection() {
    connection =
            std::make_unique<MemcachedConnection>(properties.hostname,
                                                  properties.port,
                                                  AF_UNSPEC,
                                                  properties.tls.has_value());
    if (properties.tls.has_value()) {
        connection->setTlsConfigFiles(properties.tls->cert,
                                      properties.tls->key,
                                      properties.tls->ca_store);
        if (!properties.tls->passphrase.empty()) {
            connection->setPemPassphrase(properties.tls->passphrase);
        }
    }
    connection->connect();
    if (properties.sasl.has_value()) {
        connection->authenticate(properties.sasl->username,
                                 properties.sasl->password,
                                 properties.sasl->mechanism);
    }

    connection->setAgentName("fbr/" PRODUCT_VERSION);
    connection->setFeatures(
            {cb::mcbp::Feature::XERROR, cb::mcbp::Feature::JSON});
    connection->selectBucket(properties.bucket);
}

std::variant<cb::engine_errc, Manifest>
DownloadSnapshotTask::doDownloadManifest() {
    listener->stateChanged(DownloadSnapshotTaskState::PrepareSnapshot);
    BinprotGenericCommand prepare(cb::mcbp::ClientOpcode::PrepareSnapshot);
    prepare.setVBucket(vbid);
    nlohmann::json json;
    try {
        auto rsp = connection->execute(prepare);
        if (!rsp.isSuccess()) {
            EP_LOG_WARN_CTX("Failed to prepare snapshot",
                            {"vb", vbid},
                            {"status", rsp.getStatus()});
            listener->failed(fmt::format("Failed to prepare snapshot: {}: {}",
                                         rsp.getStatus(),
                                         rsp.getDataView()));
            return cb::engine_errc::failed;
        }
        json = rsp.getDataJson();
    } catch (const std::exception& e) {
        listener->failed(fmt::format(
                "Error occurred during PrepareSnapshot: {}", e.what()));
        EP_LOG_WARN_CTX("Error occurred during PrepareSnapshot",
                        {"vb", vbid},
                        {"error", e.what()});
        return cb::engine_errc::failed;
    }

    try {
        return Manifest{json};
    } catch (const std::exception& e) {
        listener->failed(
                fmt::format("Failed to parse snapshot manifest: {}", e.what()));
        EP_LOG_WARN_CTX("Failed to parse snapshot manifest",
                        {"vb", vbid},
                        {"json", json},
                        {"error", e.what()});
    }
    return cb::engine_errc::failed;
}

size_t DownloadSnapshotTask::getChecksumLength() {
    if (!connection || connection->isSsl() ||
        !engine->isFileFragmentChecksumEnabled()) {
        // Don't checksum when using TLS, the encryption layer should detect
        // problems. A value of 0 disables checksumming.
        return 0;
    }

    return engine->getFileFragmentChecksumLength();
}

cb::engine_errc DownloadSnapshotTask::doDownloadFiles(
        std::filesystem::path dir, const Manifest& manifest) {
    listener->setManifest(manifest);
    listener->stateChanged(DownloadSnapshotTaskState::DownloadFiles);

    try {
        download(
                std::move(connection),
                dir,
                manifest,
                properties.fsync_interval,
                getChecksumLength(),
                [this](auto level, auto msg, auto json) {
                    auto& logger = getGlobalBucketLogger();
                    logger->logWithContext(level, msg, json);
                },
                [this](auto bytes) {
                    engine->getEpStats().snapshotBytesRead += bytes;
                });
    } catch (const engine_error& e) {
        listener->failed(fmt::format(
                "Received exception while downloading snapshot: {}", e.what()));
        EP_LOG_ERR_CTX("DownloadSnapshotTask::doDownloadFiles()",
                       {"vb", vbid},
                       {"status", static_cast<engine_errc>(e.code().value())},
                       {"error", e.what()});
        return static_cast<engine_errc>(e.code().value());
    } catch (const std::exception& e) {
        listener->failed(fmt::format(
                "Received exception while downloading snapshot: {}", e.what()));
        EP_LOG_ERR_CTX("DownloadSnapshotTask::doDownloadFiles()",
                       {"vb", vbid},
                       {"error", e.what()});
        return cb::engine_errc::failed;
    }

    return cb::engine_errc::success;
}

bool DownloadSnapshotTask::run() {
    try {
        createConnection();
        auto rv = manager.download(
                vbid,
                [this]() { return doDownloadManifest(); },
                [this](const auto& dir, auto& manifest) {
                    return doDownloadFiles(dir, manifest);
                });
        if (std::holds_alternative<Manifest>(rv)) {
            listener->stateChanged(DownloadSnapshotTaskState::Finished);
        } else {
            listener->failed(fmt::format("Failed to download snapshot: {}",
                                         std::get<cb::engine_errc>(rv)));
        }
    } catch (const std::exception& e) {
        listener->failed(fmt::format("Received exception: {}", e.what()));
        EP_LOG_ERR_CTX("DownloadSnapshotTask::run()",
                       {"vb", vbid},
                       {"error", e.what()});
    }
    return false;
}

} // namespace cb::snapshot
