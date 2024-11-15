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

class DownloadSnapshotTaskImpl : public DownloadSnapshotTask {
public:
    DownloadSnapshotTaskImpl(CookieIface& cookie,
                             EventuallyPersistentEngine& ep,
                             cb::snapshot::Cache& manager,
                             Vbid vbid,
                             const nlohmann::json& manifest)
        : DownloadSnapshotTask(ep),
          description(fmt::format("Download vbucket snapshot for {}", vbid)),
          cookie(cookie),
          manager(manager),
          vbid(vbid),
          properties(manifest) {
        // empty
    }

    std::string getDescription() const override {
        return description;
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        // @todo this could be deducted from the total size
        return std::chrono::seconds(30);
    }

    std::pair<cb::engine_errc, std::string> getResult() const override {
        return result.copy();
    }

protected:
    cb::engine_errc doDownloadManifest(cb::snapshot::Manifest& manifest);
    cb::engine_errc doDownloadFiles(std::filesystem::path dir,
                                    cb::snapshot::Manifest& manifest);
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
    cb::snapshot::Cache& manager;
    /// The vbucket to download the snapshot for
    const Vbid vbid;
    /// The properties to use for the download (host, credentials etc)
    const cb::snapshot::DownloadProperties properties;
    /// The result of the operation to pass on to the front end thread when
    /// the task is done. The pair consists of an error code and a string
    /// which may contain extra information to pass along back to the client.
    folly::Synchronized<std::pair<cb::engine_errc, std::string>, std::mutex>
            result;
};

MemcachedConnection& DownloadSnapshotTaskImpl::getConnection() {
    if (connection) {
        return *connection;
    }

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
    return *connection;
}

cb::engine_errc DownloadSnapshotTaskImpl::doDownloadManifest(
        cb::snapshot::Manifest& manifest) {
    auto& conn = getConnection();
    BinprotGenericCommand prepare(cb::mcbp::ClientOpcode::PrepareSnapshot);
    prepare.setVBucket(vbid);
    nlohmann::json json;
    try {
        auto rsp = conn.execute(prepare);
        if (!rsp.isSuccess()) {
            EP_LOG_WARN_CTX("Failed to prepare snapshot",
                            {"conn_id", cookie.getConnectionId()},
                            {"vb", vbid},
                            {"status", rsp.getStatus()});
            result = {cb::engine_errc::failed,
                      fmt::format("Failed to prepare snapshot: {}: {}",
                                  rsp.getStatus(),
                                  rsp.getDataView())};
            return cb::engine_errc::failed;
        }
        json = rsp.getDataJson();
    } catch (const std::exception& e) {
        EP_LOG_WARN_CTX("Error occurred during PrepareSnapshot",
                        {"conn_id", cookie.getConnectionId()},
                        {"vb", vbid},
                        {"error", e.what()});
    }
    try {
        manifest = json;
    } catch (const std::exception& e) {
        EP_LOG_WARN_CTX("Failed to parse snapshot manifest",
                        {"conn_id", cookie.getConnectionId()},
                        {"vb", vbid},
                        {"json", json},
                        {"error", e.what()});
        result = {
                cb::engine_errc::failed,
                fmt::format("Failed to parse snapshot manifest: {}", e.what())};
        return cb::engine_errc::failed;
    }
    EP_LOG_INFO_CTX("Downloaded snapshot manifest",
                    {"conn_id", cookie.getConnectionId()},
                    {"vb", vbid},
                    {"uuid", manifest.uuid});
    return cb::engine_errc::success;
}

cb::engine_errc DownloadSnapshotTaskImpl::doDownloadFiles(
        std::filesystem::path dir, cb::snapshot::Manifest& manifest) {
    auto dconn = connection->clone();

    if (properties.sasl.has_value()) {
        dconn->authenticate(properties.sasl->username,
                            properties.sasl->password,
                            properties.sasl->mechanism);
    }

    dconn->setAgentName("fbr/" PRODUCT_VERSION);
    dconn->setFeatures({cb::mcbp::Feature::XERROR, cb::mcbp::Feature::JSON});
    dconn->selectBucket(properties.bucket);

    try {
        cb::snapshot::download(std::move(dconn),
                               dir,
                               manifest,
                               [this](auto level, auto msg, auto json) {
                                   auto& logger = getGlobalBucketLogger();
                                   logger->logWithContext(level, msg, json);
                               });
    } catch (const std::exception& e) {
        result = {cb::engine_errc::failed,
                  fmt::format("Received exception: {}", e.what())};
        EP_LOG_ERR_CTX("DownloadSnapshotTaskImpl::doDownloadFiles()",
                       {"conn_id", cookie.getConnectionId()},
                       {"vb", vbid},
                       {"error", e.what()});
        return cb::engine_errc::failed;
    }

    return cb::engine_errc::success;
}

void DownloadSnapshotTaskImpl::doReleaseSnapshot(std::string_view uuid) {
    try {
        BinprotGenericCommand release(cb::mcbp::ClientOpcode::ReleaseSnapshot,
                                      std::string(uuid));
        auto rsp = getConnection().execute(release);
        if (!rsp.isSuccess()) {
            EP_LOG_WARN_CTX("Failed to release snapshot",
                            {"conn_id", cookie.getConnectionId()},
                            {"uuid", uuid},
                            {"vb", vbid},
                            {"status", rsp.getStatus()});
        }
    } catch (const std::exception& e) {
        EP_LOG_WARN_CTX("Failed to release snapshot",
                        {"conn_id", cookie.getConnectionId()},
                        {"uuid", uuid},
                        {"vb", vbid},
                        {"error", e.what()});
    }
}

bool DownloadSnapshotTaskImpl::run() {
    try {
        auto rv = manager.download(
                vbid,
                [this](auto& manifest) { return doDownloadManifest(manifest); },
                [this](const auto& dir, auto& manifest) {
                    doDownloadFiles(dir, manifest);
                    return cb::engine_errc::success;
                },
                [this](auto uuid) { return doReleaseSnapshot(uuid); });

        if (std::holds_alternative<cb::engine_errc>(rv)) {
            result = {std::get<cb::engine_errc>(rv), {}};
        } else {
            result = {cb::engine_errc::success,
                      nlohmann::json(std::get<cb::snapshot::Manifest>(rv))
                              .dump()};
        }
    } catch (const std::exception& e) {
        result = {cb::engine_errc::failed,
                  fmt::format("Received exception: {}", e.what())};
        EP_LOG_ERR_CTX("DownloadSnapshotTaskImpl::run()",
                       {"conn_id", cookie.getConnectionId()},
                       {"vb", vbid},
                       {"error", e.what()});
    }
    cookie.notifyIoComplete(cb::engine_errc::success);
    return false;
}

std::shared_ptr<DownloadSnapshotTask> DownloadSnapshotTask::create(
        CookieIface& cookie,
        EventuallyPersistentEngine& ep,
        cb::snapshot::Cache& manager,
        Vbid vbid,
        std::string_view manifest) {
    return std::make_shared<DownloadSnapshotTaskImpl>(
            cookie, ep, manager, vbid, nlohmann::json::parse(manifest));
}

DownloadSnapshotTask::DownloadSnapshotTask(EventuallyPersistentEngine& ep)
    : EpTask(ep, TaskId::DownloadSnapshotTask, 0, true) {
}
