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
#include <filesystem>
#include <vector>

using namespace cb::dek;

static nlohmann::json getActiveEncryptionKeysJson(const std::string_view data) {
    // The data is expected to be a JSON object containing the keystore and
    // an (optional) array of unavailable keys.
    // The previous version of the protocol used to have the keystore
    // as the root object, so we need to handle that case too.
    // To simplify the implementation of the command, we always
    // return a JSON object with the keystore as a top-level key.
    // and an array of unavailable keys.
    auto ret = nlohmann::json::parse(data);
    if (!ret.contains("keystore")) {
        ret = {
                {"keystore", std::move(ret)},
        };
    }
    if (!ret.contains("unavailable")) {
        ret["unavailable"] = nlohmann::json::array();
    }
    return ret;
}

static nlohmann::json getLoggableJson(const nlohmann::json& payload) {
    auto ret = payload;
    ret["keystore"] = cb::crypto::toLoggableJson(
            payload["keystore"].get<cb::crypto::KeyStore>());
    if (ret["unavailable"].empty()) {
        ret.erase("unavailable");
    }
    return ret;
}

SetActiveEncryptionKeysContext::SetActiveEncryptionKeysContext(Cookie& cookie)
    : BackgroundThreadCommandContext(
              cookie,
              TaskId::Core_SetActiveEncryptionKeysTask,
              "SetActiveEncryptionKeys",
              ConcurrencySemaphores::instance()
                      .encryption_and_snapshot_management),
      json(getActiveEncryptionKeysJson(cookie.getRequest().getValueString())),
      loggable_json(getLoggableJson(json)),
      entity(cookie.getRequest().getKeyString()) {
}

cb::engine_errc
SetActiveEncryptionKeysContext::mergeUnavailableKeysFromCache() {
    cb::crypto::KeyStore keystore = json["keystore"];
    bool added = false;
    auto& manager = Manager::instance();
    auto ent = to_entity(entity);
    std::vector<std::string> missing_keys;
    for (const auto& k : json["unavailable"]) {
        const auto key = k.get<std::string>();
        if (!keystore.lookup(key)) {
            auto sk = manager.lookup(ent, key);
            if (sk) {
                added = true;
                keystore.add(sk);
            } else {
                missing_keys.emplace_back(key);
            }
        }
    }

    if (!missing_keys.empty()) {
        nlohmann::json missing_keys_json = missing_keys;
        LOG_WARNING_CTX(
                "Failed to update Data encryption",
                {"conn_id", cookie.getConnectionId()},
                {"status", cb::engine_errc::encryption_key_not_available},
                {"entity", entity},
                {"missing", missing_keys_json});
        cookie.setErrorContext(fmt::format("Unknown encryption key(s): {}",
                                           missing_keys_json.dump()));
        return cb::engine_errc::encryption_key_not_available;
    }

    if (added) {
        json["keystore"] = keystore;
        loggable_json["keystore"] = cb::crypto::toLoggableJson(keystore);
        LOG_WARNING_CTX("Updating Data encryption (added missing keys)",
                        {"conn_id", cookie.getConnectionId()},
                        {"entity", entity},
                        {"config", loggable_json});
    }

    return cb::engine_errc::success;
}

void SetActiveEncryptionKeysContext::setBucketKeys() {
    status = cb::engine_errc::no_such_key;
    BucketManager::instance().forEach([this](auto& bucket) -> bool {
        if (bucket.name == entity) {
            if (bucket.type == BucketType::ClusterConfigOnly) {
                status = cb::engine_errc::not_supported;
                return false;
            }
            status =
                    bucket.getEngine().set_active_encryption_keys(cookie, json);
            return false;
        }
        return true;
    });
}

void SetActiveEncryptionKeysContext::setCoreKeys() {
    status = mergeUnavailableKeysFromCache();
    if (status != cb::engine_errc::success) {
        return;
    }

    Manager::instance().setActive(to_entity(entity), json["keystore"]);
}

cb::engine_errc SetActiveEncryptionKeysContext::execute() {
    LOG_INFO_CTX("Updating Data encryption",
                 {"conn_id", cookie.getConnectionId()},
                 {"entity", entity},
                 {"config", loggable_json});
    try {
        if (entity.front() == '@') {
            setCoreKeys();
        } else {
            setBucketKeys();
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
        loggable_json.erase("unavailable");
        LOG_INFO_CTX("Data encryption updated",
                     {"conn_id", cookie.getConnectionId()},
                     {"entity", entity},
                     {"config", loggable_json});
    } else if (status != cb::engine_errc::disconnect &&
               status != cb::engine_errc::encryption_key_not_available) {
        LOG_WARNING_CTX("Failed to update encryption keys",
                        {"conn_id", cookie.getConnectionId()},
                        {"entity", entity},
                        {"status", to_string(status)},
                        {"entity", entity},
                        {"config", loggable_json});
    }

    return status;
}
