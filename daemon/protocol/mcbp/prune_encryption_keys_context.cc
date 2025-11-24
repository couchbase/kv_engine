/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "prune_encryption_keys_context.h"

#include <cbcrypto/encrypted_file_header.h>
#include <cbcrypto/file_utilities.h>
#include <cbcrypto/key_store.h>
#include <daemon/buckets.h>
#include <daemon/concurrency_semaphores.h>
#include <daemon/mcaudit.h>
#include <daemon/one_shot_limited_concurrency_task.h>
#include <daemon/settings.h>
#include <dek/manager.h>
#include <executor/executorpool.h>
#include <platform/dirutils.h>

PruneEncryptionKeysContext::PruneEncryptionKeysContext(Cookie& cookie)
    : BackgroundThreadCommandContext(
              cookie,
              TaskId::Core_PruneEncryptionKeysTask,
              "PruneEncryptionKeys",
              ConcurrencySemaphores::instance()
                      .encryption_and_snapshot_management),
      keys(nlohmann::json::parse(cookie.getRequest().getValueString())
                   .get<std::vector<std::string>>()),
      entity(cb::dek::to_entity(cookie.getRequest().getKeyString())) {
}

cb::engine_errc PruneEncryptionKeysContext::execute() {
    LOG_INFO_CTX("Prune encryption keys",
                 {"conn_id", cookie.getConnectionId()},
                 {"entity", entity},
                 {"keys", keys});
    if (!validateKeys()) {
        return cb::engine_errc::encryption_key_not_available;
    }
    try {
        if (entity == cb::dek::Entity::Audit) {
            cb::audit::pruneDeks(keys);
        } else if (entity == cb::dek::Entity::Logs) {
            pruneLog();
        } else {
            throw std::runtime_error(fmt::format(
                    "Prune encryption keys not supported for entity: {}",
                    entity));
        }
    } catch (const std::exception& e) {
        LOG_ERROR_CTX("Exception occurred while pruning encryption keys",
                      {"conn_id", cookie.getConnectionId()},
                      {"entity", entity},
                      {"keys", keys},
                      {"error", e.what()});
        return cb::engine_errc::failed;
    }

    LOG_INFO_CTX("Data encryption keys pruned",
                 {"conn_id", cookie.getConnectionId()},
                 {"entity", entity},
                 {"config", keys});

    return cb::engine_errc::success;
}

bool PruneEncryptionKeysContext::validateKeys() const {
    auto& manager = cb::dek::Manager::instance();
    for (const auto& key : keys) {
        if (key != cb::crypto::KeyDerivationKey::UnencryptedKeyId &&
            !manager.lookup(entity, key)) {
            LOG_WARNING_CTX("Prune encryption keys: Key not found",
                            {"conn_id", cookie.getConnectionId()},
                            {"entity", entity},
                            {"key", key});
            return false;
        }
    }
    return true;
}

void PruneEncryptionKeysContext::pruneLog() {
    std::filesystem::path pattern =
            Settings::instance().getLoggerConfig().filename;
    auto directory = pattern.parent_path();
    auto filename = pattern.filename().string();
    auto active_key =
            cb::dek::Manager::instance().lookup(cb::dek::Entity::Logs);

    maybeRewriteFiles(
            directory,
            [&filename, this](const auto& path, auto id) {
                if (path.filename().string().starts_with(filename)) {
                    if (path.extension() == ".txt" && id.empty()) {
                        return std::ranges::find(keys, "unencrypted") !=
                               keys.end();
                    }
                    if (path.extension() == ".cef" && !id.empty()) {
                        return std::ranges::find(keys, id) != keys.end();
                    }
                }
                return false;
            },
            active_key,
            [](auto id) {
                return cb::dek::Manager::instance().lookup(
                        cb::dek::Entity::Logs, id);
            },
            [](std::string_view message, const nlohmann::json& ctx) {
                LOG_WARNING_CTX(message, ctx);
            });
}