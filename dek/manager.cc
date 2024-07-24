/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "manager.h"
#include <cbcrypto/file_reader.h>
#include <cbcrypto/file_writer.h>
#include <cbcrypto/key_store.h>
#include <fmt/format.h>
#include <folly/Synchronized.h>
#include <nlohmann/json.hpp>
#include <platform/dirutils.h>

namespace cb::dek {

std::string format_as(Entity entity) {
    switch (entity) {
    case Entity::Config:
        return "@config";
    case Entity::Logs:
        return "@logs";
    case Entity::Audit:
        return "@audit";
    }
    throw std::invalid_argument(
            fmt::format("cb::dek::format_as(Entity): Unknown value {}",
                        static_cast<int>(entity)));
}

Entity to_entity(std::string_view entity) {
    if (entity == "@config") {
        return Entity::Config;
    }
    if (entity == "@logs") {
        return Entity::Logs;
    }
    if (entity == "@audit") {
        return Entity::Audit;
    }
    throw std::invalid_argument(
            fmt::format("cb::dek::to_entity(): Unknown value {}", entity));
}

class ManagerImpl : public Manager {
    [[nodiscard]] SharedEncryptionKey lookup(
            Entity entity, std::string_view id) const override;

    void reset(const nlohmann::json& json) override;
    void setActive(Entity entity, SharedEncryptionKey key) override;
    void setActive(Entity entity, crypto::KeyStore ks) override;
    [[nodiscard]] nlohmann::json to_json() const override;
    [[nodiscard]] nlohmann::json to_json(Entity entity) const override;

protected:
    folly::Synchronized<std::unordered_map<Entity, crypto::KeyStore>> keys;
};

SharedEncryptionKey ManagerImpl::lookup(Entity entity,
                                        std::string_view id) const {
    return keys.withRLock([&entity, id](auto& keys) -> SharedEncryptionKey {
        auto itr = keys.find(entity);
        if (itr == keys.end()) {
            return {};
        }

        if (id.empty()) {
            return itr->second.getActiveKey();
        }

        return itr->second.lookup(id);
    });
}

void ManagerImpl::reset(const nlohmann::json& json) {
    std::unordered_map<Entity, crypto::KeyStore> next;
    if (json.empty()) {
        keys.swap(next);
        return;
    }

    if (!json.is_object()) {
        throw std::runtime_error("Provided json should be an object");
    }
    for (auto it = json.begin(); it != json.end(); ++it) {
        next[to_entity(it.key())] = it.value();
    }

    keys.swap(next);
}

void ManagerImpl::setActive(const Entity entity, SharedEncryptionKey key) {
    keys.withWLock(
            [entity, &key](auto& map) { map[entity].setActiveKey(key); });
}

void ManagerImpl::setActive(Entity entity, crypto::KeyStore ks) {
    keys.withWLock([entity, &ks](auto& map) { map[entity] = std::move(ks); });
}

nlohmann::json ManagerImpl::to_json() const {
    return keys.withRLock([](auto& keys) {
        nlohmann::json ret = nlohmann::json::object();
        for (const auto& [n, type_keys] : keys) {
            auto name = format_as(n);
            ret[name] = type_keys;
        }
        return ret;
    });
}

nlohmann::json ManagerImpl::to_json(Entity entity) const {
    return keys.withRLock([&entity](auto& keys) -> nlohmann::json {
        auto iter = keys.find(entity);
        if (iter == keys.end()) {
            return nlohmann::json::object();
        }

        return iter->second;
    });
}

std::unique_ptr<Manager> Manager::create() {
    return std::make_unique<ManagerImpl>();
}

Manager& Manager::instance() {
    static ManagerImpl inst;
    return inst;
}

std::string Manager::load(Entity entity,
                          const std::filesystem::path& path,
                          std::chrono::microseconds wait) {
    auto lookup_function = [this, entity](auto key) {
        return lookup(entity, key);
    };

    auto reader = crypto::FileReader::create(path, lookup_function, wait);
    return reader->read();
}

void Manager::save(Entity entity,
                   const std::filesystem::path& path,
                   std::string_view data) const {
    auto writer = crypto::FileWriter::create(lookup(entity), path);
    writer->write(data);
    writer->flush();
}

} // namespace cb::dek