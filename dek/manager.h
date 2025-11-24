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

#include <cbcrypto/key_store.h>
#include <nlohmann/json_fwd.hpp>
#include <atomic>
#include <filesystem>
#include <string>

namespace cb::dek {

using cb::crypto::SharedKeyDerivationKey;

/// The various entities supporting encryption in the core
enum class Entity { Config = 0, Logs = 1, Audit = 2, Count = 3 };

std::string format_as(Entity entity);
inline auto to_string(Entity entity) {
    return format_as(entity);
}
Entity to_entity(std::string_view entity);

class Manager {
public:
    virtual ~Manager() = default;
    /// In the case where you would want multiple instances of the
    /// manager in the same process (memcached_testapp running in
    /// embedded mode is a good example) this method should be used.
    /// Inside memcached you should use the singleton instance
    [[nodiscard]] static std::unique_ptr<Manager> create();

    /// The instance when used as a singleton
    [[nodiscard]] static Manager& instance();

    /**
     *  Load the named file and try to decrypt it
     *
     * @param entity The entity used to search for the key if the file is
     *               encrypted
     * @param path The name of the file to load
     * @param wait The amount of time to wait for the file to appear if
     *             the file doesn't exist
     */
    [[nodiscard]] std::string load(Entity entity,
                                   const std::filesystem::path& path,
                                   std::chrono::microseconds wait = {});

    /// Encrypt the provided data with the active key for the entity (if no keys
    /// are generated the data will be saved in plain text)
    void save(Entity entity,
              const std::filesystem::path& path,
              std::string_view data) const;

    [[nodiscard]] virtual nlohmann::json to_json() const = 0;

    /// Get a JSON dump for the provided entity
    [[nodiscard]] virtual nlohmann::json to_json(Entity entity) const = 0;

    /// Get a named current encryption key for the named entity
    [[nodiscard]] virtual SharedKeyDerivationKey lookup(
            Entity entity, std::string_view id = {}) const = 0;

    /// Parse the provided JSON and replace the list of known keys with the
    /// provided list
    virtual void reset(const nlohmann::json& json) = 0;

    /// Set the active encryption key for a given entity
    virtual void setActive(Entity entity, SharedKeyDerivationKey key) = 0;
    /// Set the list of active encryption keys for a given entity
    virtual void setActive(Entity entity, crypto::KeyStore ks) = 0;

    /// Iterate over all key stores
    virtual void iterate(
            const std::function<void(Entity, const crypto::KeyStore&)>&
                    callback) = 0;

    /// Get the generation counter for the named entity. Note that unless you're
    /// using the static instance you must ensure that the lifetime for the
    /// Manager instance is longer than the lifetime of the object using the
    /// returned reference. The intended use case for this variable is to
    /// use a lock free chck to see if the generation has changed.
    virtual std::atomic_uint64_t* getEntityGenerationCounter(Entity entity) = 0;
};
} // namespace cb::dek
