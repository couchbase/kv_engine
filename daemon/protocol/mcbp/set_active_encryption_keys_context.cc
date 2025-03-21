/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "set_active_encryption_keys_context.h"

#include <cbcrypto/key_store.h>
#include <daemon/buckets.h>
#include <daemon/concurrency_semaphores.h>
#include <daemon/one_shot_limited_concurrency_task.h>
#include <dek/manager.h>
#include <executor/executorpool.h>

SetActiveEncryptionKeysContext::SetActiveEncryptionKeysContext(Cookie& cookie)
    : BackgroundThreadCommandContext(
              cookie,
              TaskId::Core_SetActiveEncryptionKeysTask,
              "SetActiveEncryptionKeys",
              ConcurrencySemaphores::instance()
                      .encryption_and_snapshot_management),
      json(nlohmann::json::parse(cookie.getRequest().getValueString())),
      loggable_json(cb::dek::toLoggableJson(json)),
      entity(cookie.getRequest().getKeyString()) {
}

cb::engine_errc SetActiveEncryptionKeysContext::execute() {
    LOG_INFO_CTX("Updating Data encryption",
                 {"conn_id", cookie.getConnectionId()},
                 {"entity", entity},
                 {"config", loggable_json});
    try {
        if (entity.front() == '@') {
            using namespace std::string_view_literals;
            try {
                cb::dek::Manager::instance().setActive(
                        cb::dek::to_entity(entity), json);
                status = cb::engine_errc::success;
            } catch (const std::invalid_argument&) {
                status = cb::engine_errc::no_such_key;
            }
        } else {
            status = cb::engine_errc::no_such_key;
            BucketManager::instance().forEach([this](auto& bucket) -> bool {
                if (bucket.name == entity) {
                    if (bucket.type == BucketType::ClusterConfigOnly) {
                        status = cb::engine_errc::not_supported;
                    } else {
                        status = bucket.getEngine().set_active_encryption_keys(
                                json);
                    }
                    return false;
                }
                return true;
            });
        }
    } catch (const std::exception& e) {
        LOG_ERROR_CTX("Exception occurred while setting active encryption key",
                      {"conn_id", cookie.getConnectionId()},
                      {"entity", entity},
                      {"config", loggable_json},
                      {"error", e.what()});
        status = cb::engine_errc::disconnect;
    }

    if (status == cb::engine_errc::success) {
        LOG_INFO_CTX("Data encryption updated",
                     {"conn_id", cookie.getConnectionId()},
                     {"entity", entity},
                     {"config", loggable_json});
    } else if (status != cb::engine_errc::disconnect) {
        LOG_WARNING_CTX("Failed to update encryption keys",
                        {"conn_id", cookie.getConnectionId()},
                        {"entity", entity},
                        {"status", to_string(status)},
                        {"entity", entity},
                        {"config", loggable_json});
    }

    return status;
}
