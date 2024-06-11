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
#include <fmt/format.h>
#include <folly/Synchronized.h>
#include <nlohmann/json.hpp>
#include <platform/dirutils.h>
#include <utilities/json_utilities.h>

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
    [[nodiscard]] std::shared_ptr<DataEncryptionKey> lookup(
            Entity entity, std::string_view id) const override;

    void reset(const nlohmann::json& json) override;
    void setActive(Entity entity,
                   std::shared_ptr<DataEncryptionKey> key) override;
    [[nodiscard]] nlohmann::json to_json() const override;

protected:
    struct Node {
        void add(std::shared_ptr<DataEncryptionKey> key) {
            keys.emplace_back(std::move(key));
            active = keys.back();
        }
        std::vector<std::shared_ptr<DataEncryptionKey>> keys;
        std::shared_ptr<DataEncryptionKey> active;
    };

    folly::Synchronized<std::unordered_map<Entity, Node>> keys;
};

std::shared_ptr<DataEncryptionKey> ManagerImpl::lookup(
        Entity entity, std::string_view id) const {
    return keys.withRLock(
            [&entity, id](auto& keys) -> std::shared_ptr<DataEncryptionKey> {
                auto itr = keys.find(entity);
                if (itr == keys.end()) {
                    return {};
                }

                if (id.empty()) {
                    return itr->second.active;
                }

                for (auto& dek : itr->second.keys) {
                    if (dek->id == id) {
                        return dek;
                    }
                }
                return {};
            });
}

void ManagerImpl::reset(const nlohmann::json& json) {
    std::unordered_map<Entity, Node> next;
    if (json.empty()) {
        keys.swap(next);
        return;
    }

    auto add_entry = [&next, this](const Entity entity,
                                   const nlohmann::json& entry) {
        auto object = std::make_shared<DataEncryptionKey>();
        *object = entry;
        next[entity].keys.emplace_back(std::move(object));
    };

    if (!json.is_object()) {
        throw std::runtime_error("Provided json should be an object");
    }
    for (auto it = json.begin(); it != json.end(); ++it) {
        const auto entity = to_entity(it.key());
        if (!it.value().is_object()) {
            throw std::runtime_error(fmt::format(
                    R"(Entry for "{}" should be an object)", entity));
        }
        auto& value = it.value();
        auto keys = getJsonObject(value,
                                  "keys",
                                  nlohmann::json::value_t::array,
                                  fmt::format("Entry for {} keys", entity));
        for (const auto& obj : keys) {
            add_entry(entity, obj);
        }

        if (value.contains("active")) {
            const auto active =
                    getJsonObject(value,
                                  "active",
                                  nlohmann::json::value_t::string,
                                  fmt::format("Entry for {} active", entity))
                            .get<std::string>();
            if (!active.empty()) {
                for (auto& entry : next[entity].keys) {
                    if (entry->id == active) {
                        next[entity].active = entry;
                        break;
                    }
                }
                if (!next[entity].active) {
                    throw std::runtime_error(fmt::format(
                            R"(The active key "{}" for "{}" does not exists)",
                            active,
                            entity));
                }
            }
        }
    }

    keys.swap(next);
}

void ManagerImpl::setActive(const Entity entity,
                            std::shared_ptr<DataEncryptionKey> key) {
    keys.withWLock([entity, &key](auto& map) {
        // Too lazy to define my own comparator and use std::find....
        for (const auto& k : map[entity].keys) {
            if (k->id == key->id) {
                return;
            }
        }
        map[entity].add(std::move(key));
    });
}

nlohmann::json ManagerImpl::to_json() const {
    return keys.withRLock([](auto& keys) {
        nlohmann::json ret = nlohmann::json::object();
        for (const auto& [n, type_keys] : keys) {
            auto name = format_as(n);

            nlohmann::json node_keys = nlohmann::json::array();
            for (const auto& k : type_keys.keys) {
                node_keys.emplace_back(*k);
            }

            ret[name]["keys"] = std::move(node_keys);
            if (type_keys.active) {
                ret[name]["active"] = type_keys.active->id;
            }
        }
        return ret;
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