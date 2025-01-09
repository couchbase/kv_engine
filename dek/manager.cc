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
    case Entity::Count:
        break;
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

nlohmann::json toLoggableJson(const cb::crypto::KeyStore& keystore) {
    nlohmann::json ids = nlohmann::json::array();
    keystore.iterateKeys([&ids](auto key) { ids.emplace_back(key->getId()); });
    nlohmann::json entry;
    entry["keys"] = std::move(ids);
    if (keystore.getActiveKey()) {
        entry["active"] = keystore.getActiveKey()->getId();
    }
    return entry;
}

class ManagerImpl : public Manager {
    [[nodiscard]] SharedEncryptionKey lookup(
            Entity entity, std::string_view id) const override;

    void reset(const nlohmann::json& json) override;
    void setActive(Entity entity, SharedEncryptionKey key) override;
    void setActive(Entity entity, crypto::KeyStore ks) override;
    [[nodiscard]] nlohmann::json to_json() const override;
    [[nodiscard]] nlohmann::json to_json(Entity entity) const override;
    void iterate(const std::function<void(Entity, const crypto::KeyStore&)>&
                         callback) override;
    std::atomic_uint64_t* getEntityGenerationCounter(Entity entity) override;

protected:
    struct ObservableKeyStore {
        crypto::KeyStore keystore;
        std::atomic_uint64_t generation{0};
    };

    using SynchronizedObservableKeyStore =
            folly::Synchronized<ObservableKeyStore, std::mutex>;

    std::array<SynchronizedObservableKeyStore, int(Entity::Count)> stores;

    SynchronizedObservableKeyStore& getKeyStoreForEntity(Entity entity) {
        return stores[static_cast<int>(entity)];
    }

    const SynchronizedObservableKeyStore& getKeyStoreForEntity(
            Entity entity) const {
        return stores[static_cast<int>(entity)];
    }
};

SharedEncryptionKey ManagerImpl::lookup(Entity entity,
                                        std::string_view id) const {
    const auto& store = getKeyStoreForEntity(entity);
    return store.withLock([id](const auto& oks) {
        if (id.empty()) {
            return oks.keystore.getActiveKey();
        }
        return oks.keystore.lookup(id);
    });
}

void ManagerImpl::reset(const nlohmann::json& json) {
    for (std::size_t ii = 0; ii < stores.size(); ++ii) {
        const auto entity = static_cast<Entity>(ii);
        auto& store = getKeyStoreForEntity(entity);
        store.withLock([](auto& oks) {
            oks.keystore = {};
            ++oks.generation;
        });
    }

    if (!json.is_object()) {
        throw std::runtime_error("Provided json should be an object");
    }

    for (auto it = json.begin(); it != json.end(); ++it) {
        auto& store = getKeyStoreForEntity(to_entity(it.key()));
        store.withLock([&it](auto& oks) {
            oks.keystore = it.value();
            ++oks.generation;
        });
    }
}

void ManagerImpl::setActive(const Entity entity, SharedEncryptionKey key) {
    auto& store = getKeyStoreForEntity(entity);
    store.withLock([&key](auto& oks) {
        oks.keystore.setActiveKey(std::move(key));
        ++oks.generation;
    });
}

void ManagerImpl::setActive(Entity entity, crypto::KeyStore ks) {
    auto& store = getKeyStoreForEntity(entity);
    store.withLock([&ks](auto& oks) {
        oks.keystore = std::move(ks);
        ++oks.generation;
    });
}

nlohmann::json ManagerImpl::to_json() const {
    nlohmann::json ret;
    for (size_t ii = 0; ii < stores.size(); ++ii) {
        const auto entity = static_cast<Entity>(ii);
        ret[format_as(entity)] = to_json(entity);
    }

    return ret;
}

nlohmann::json ManagerImpl::to_json(Entity entity) const {
    const auto& store = getKeyStoreForEntity(entity);
    return store.withLock(
            [](const auto& oks) -> nlohmann::json { return oks.keystore; });
}

void ManagerImpl::iterate(
        const std::function<void(Entity, const crypto::KeyStore&)>& callback) {
    for (size_t ii = 0; ii < stores.size(); ++ii) {
        const auto entity = static_cast<Entity>(ii);
        stores[ii].withLock([&callback, entity](const auto& oks) {
            callback(entity, oks.keystore);
        });
    }
}

std::atomic_uint64_t* ManagerImpl::getEntityGenerationCounter(Entity entity) {
    auto& store = getKeyStoreForEntity(entity);
    std::atomic_uint64_t* ptr;
    store.withLock([&ptr](auto& oks) { ptr = &oks.generation; });
    return ptr;
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