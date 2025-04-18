/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

/**
 * This file contains the definitions of the privilege system used
 * by the memcached core. For more information see rbac.md in the
 * docs directory.
 */
#include <cbsasl/domain.h>
#include <memcached/dockey_view.h>
#include <memcached/rbac/privileges.h>
#include <nlohmann/json_fwd.hpp>
#include <bitset>
#include <chrono>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace cb::rbac {

using Domain = cb::sasl::Domain;

/**
 * A user in our system is defined with a name and a domain
 */
struct UserIdent {
    UserIdent() = default;
    explicit UserIdent(const nlohmann::json& json);
    UserIdent(std::string n, Domain d) : name(std::move(n)), domain(d) {
    }

    [[nodiscard]] static bool is_internal(std::string_view name) {
        return !name.empty() && name.front() == '@';
    }

    [[nodiscard]] bool is_internal() const {
        return is_internal(name);
    }

    /// Get the name of the user. Internal users won't have the log
    /// redaction tags around the name
    [[nodiscard]] std::string getSanitizedName() const;

    std::string name;
    Domain domain{cb::rbac::Domain::Local};
};

/// Get a JSON dump of the UserIdent
void to_json(nlohmann::json& json, const UserIdent& ui);

[[nodiscard]] inline bool operator==(const UserIdent& lhs,
                                     const UserIdent& rhs) {
    return lhs.domain == rhs.domain && lhs.name == rhs.name;
}

/**
 * An array containing all of the possible privileges we've got. It is
 * tightly coupled with the Privilege enum class, and when entries is
 * added to the Privilege enum class the size of the mask needs to
 * be updated.
 */
using PrivilegeMask = std::bitset<size_t(Privilege::RangeScan) + 1>;

/// The in-memory representation of a collection
class Collection {
public:
    /// Initialize the collection from the JSON representation
    explicit Collection(const nlohmann::json& json);
    /// Get a JSON dump of the Collection
    [[nodiscard]] nlohmann::json to_json() const;
    /**
     * Check if the privilege is set for the collection
     *
     * @param privilege The privilege to check for
     * @return PrivilegeAccess::Ok if the privilege is held
     *         PrivilegeAccess::Fail otherwise
     */
    [[nodiscard]] PrivilegeAccess check(Privilege privilege) const;

    /// Check if this object is identical to another object
    [[nodiscard]] bool operator==(const Collection& other) const;

protected:
    /// The privilege mask describing the access to this collection
    PrivilegeMask privilegeMask;
};

/// Get a JSON dump of the Collection
void to_json(nlohmann::json& json, const Collection& collection);

/// The in-memory representation of a scope
class Scope {
public:
    /// Initialize the scope from the JSON representation
    explicit Scope(const nlohmann::json& json);

    /// Get a JSON dump of the Scope
    ///
    /// Prefer using the nlohmann::json() constructor like
    /// <code>nlohmann::json json = scope;</code>
    [[nodiscard]] nlohmann::json to_json() const;

    /**
     * Check if the privilege is set for the scope by using the following
     * algorithm:
     *
     *  1  If the scope is configured with collections the check is delegated
     *     to the collection (and no access if the collection isn't found)
     *  2  If no collections are defined for the scope, use the scopes
     *     privilege mask.
     *
     * @param privilege The privilege to check for
     * @param collection The requested collection id
     * @param parentHasCollectionPrivileges True/false if the parent of this
     *        object (the Bucket) has >0 collection privileges.
     * @return PrivilegeAccess::Ok if the privilege is held
     *         PrivilegeAccess::Fail otherwise
     */
    [[nodiscard]] PrivilegeAccess check(
            Privilege privilege,
            std::optional<uint32_t> collection,
            bool parentHasCollectionPrivileges) const;

    /// Check if the provided privilege exists in the scope or one of the
    /// collections in the scope
    [[nodiscard]] PrivilegeAccess checkForPrivilegeAtLeastInOneCollection(
            Privilege privilege) const;

    /// Check if this object is identical to another object
    [[nodiscard]] bool operator==(const Scope& other) const;

protected:
    /// The privilege mask describing the access to this scope IFF no
    /// collections is configured
    PrivilegeMask privilegeMask;
    /// All the collections the scope contains
    std::unordered_map<uint32_t, Collection> collections;
};

/// Get a JSON dump of the Scope
void to_json(nlohmann::json& json, const Scope& scope);

class Bucket {
public:
    Bucket() = default;

    /// Initialize the bucket from the JSON representation
    explicit Bucket(const nlohmann::json& json);

    /// Get a JSON dump of the Bucket
    ///
    /// Prefer using the nlohmann::json() constructor like
    /// <code>nlohmann::json json = bucket;</code>
    [[nodiscard]] nlohmann::json to_json() const;

    /**
     * Check if the privilege is set for the bucket by using the following
     * algorithm:
     *
     *  1  If the bucket is configured with scopes the check is delegated
     *     to the scope (and no access if the scope isn't found)
     *  2  If no scopes are defined for the bucket, use the buckets
     *     privilege mask.
     *
     * @param privilege The privilege to check for
     * @param scope The requested scope id
     * @param collection The requested collection id
     * @return PrivilegeAccess::Ok if the privilege is held
     *         PrivilegeAccess::Fail otherwise
     */
    [[nodiscard]] PrivilegeAccess check(
            Privilege privilege,
            std::optional<uint32_t> scope,
            std::optional<uint32_t> collection) const;

    /// Check if the provided privilege exists in the bucket or one of the
    /// scopes in the bucket
    [[nodiscard]] PrivilegeAccess checkForPrivilegeAtLeastInOneCollection(
            Privilege privilege) const;

    /// Check if this object is identical to another object
    [[nodiscard]] bool operator==(const Bucket& other) const;

    /// Get the underlying privilege mask (used for some of the old unit
    /// tests.. should be removed when we drop the support for the old
    /// password database format.
    [[nodiscard]] const PrivilegeMask& getPrivileges() const {
        return privilegeMask;
    }

    /// @return true if there are privileges defined against scopes
    [[nodiscard]] bool hasScopePrivileges() const {
        return !scopes.empty();
    }

protected:
    /// The privilege mask describing the access to this scope IFF no
    /// scopes is configured
    PrivilegeMask privilegeMask;

    /// true if any collection privilege exists
    bool collectionPrivilegeExists{false};

    /// All of the scopes the bucket contains
    std::unordered_map<uint32_t, Scope> scopes;
};

/// Get a JSON dump of the Bucket
void to_json(nlohmann::json& json, const Bucket& bucket);

/**
 * The UserEntry object is in an in-memory representation of the per-user
 * privileges.
 */
class UserEntry {
public:
    [[nodiscard]] bool operator==(const UserEntry& other) const;

    /**
     * Create a new UserEntry from the provided JSON
     *
     * @param username The name of the user we're currently parse
     * @param json A JSON representation of the user.
     * @param domain The expected domain for this user
     * @throws std::invalid_argument if the provided JSON isn't according
     *         to the specification.
     * @throws std::bad_alloc if we run out of memory
     * @throws std::runtime_error if the domain found for the entry isn't
     *         the expected tomain
     */
    UserEntry(const std::string& username,
              const nlohmann::json& json,
              Domain domain);

    /**
     * Get a map containing all the buckets and the privileges in those
     * buckets that the user have access to.
     */
    [[nodiscard]] const std::unordered_map<std::string,
                                           std::shared_ptr<const Bucket>>&
    getBuckets() const {
        return buckets;
    }

    /**
     * Get all the "global" (not related to a bucket) privileges the user
     * have in its effective set.
     */
    [[nodiscard]] const PrivilegeMask& getPrivileges() const {
        return privilegeMask;
    }

    [[nodiscard]] nlohmann::json to_json(Domain domain) const;

    /**
     * Get the timestamp for the last time we updated the user entry
     */
    [[nodiscard]] std::chrono::steady_clock::time_point getTimestamp() const {
        return timestamp;
    }

    /**
     * Set the timestamp for the user. It looks a bit weird that this method
     * is const and that the timestamp is marked mutable, but it has a reason.
     * The user database is using a copy on write schema, so we don't want
     * to update any entries in here. As part of moving LDAP authentication and
     * authorization to ns_server it push the external users at a fixed rate.
     * We don't want to copy the entire user database just to update the
     * timestamp. The timestamp is needed as ns_server wants to not have
     * to return the RBAC data as part of each authentication request. We
     * need to know that the entry is fresh (and not 1 month old) when we try
     * to log in.
     */
    void setTimestamp(std::chrono::steady_clock::time_point ts) const {
        timestamp = ts;
    }

protected:
    mutable std::chrono::steady_clock::time_point timestamp;
    std::unordered_map<std::string, std::shared_ptr<const Bucket>> buckets;
    PrivilegeMask privilegeMask;
    bool internal;
};

class PrivilegeContext;
void to_json(nlohmann::json& json, const PrivilegeContext& ctx);

/**
 * The PrivilegeContext is the current context (selected bucket).
 * The reason for this class is to provide a fast lookup for all
 * the privileges. It is used (possibly multiple times) for every
 * command being executed.
 */
class PrivilegeContext {
public:
    PrivilegeContext() = delete;

    /**
     * Create a new (empty) instance of the privilege context.
     *
     * The generation is set to "max" which will cause the access
     * check to return stale if being used. This is the initial
     * context being used.
     */
    explicit PrivilegeContext(Domain domain)
        : generation(std::numeric_limits<uint32_t>::max()), domain(domain) {
    }

    /**
     * Create a new instance of the privilege context from the
     * given generation and assign it the given mask.
     *
     * @param gen the generation of the privilege database
     * @param domain The domain this privilege context belongs to
     * @param m the mask to set it to
     * @param bucket The bucket this privilege context is linked to
     * @param never_stale If set to true this privilege context will
     *                    never become stale.
     */
    PrivilegeContext(uint32_t gen,
                     Domain domain,
                     const PrivilegeMask& m,
                     std::shared_ptr<const Bucket> bucket,
                     bool never_stale = false)
        : generation(gen),
          domain(domain),
          mask(m),
          bucket(std::move(bucket)),
          never_stale(never_stale) {
        // empty
    }

    /**
     * Check if the given privilege is part of the context
     *
     * @param privilege the privilege to check
     * @param sid the scope id
     * @param cid the collection id (if set, sid must also be set)
     * @return if access is granted or not.
     */
    [[nodiscard]] PrivilegeAccess check(Privilege privilege,
                                        std::optional<ScopeID> sid,
                                        std::optional<CollectionID> cid) const;

    /**
     * Check if the given privilege is part of the context
     *
     * @param privilege the privilege to check
     * @return if access is granted or not.
     */
    [[nodiscard]] PrivilegeAccess check(Privilege privilege) const {
        return check(privilege, {}, {});
    }

    /**
     * Check if the given privilege exists in at least one of the bucket/
     * scope/collection
     *
     * @param privilege the privilege to check
     * @return if access is granted or not.
     */
    [[nodiscard]] PrivilegeAccess checkForPrivilegeAtLeastInOneCollection(
            Privilege privilege) const;

    /**
     * Get the generation of the Privilege Database this context maps
     * to. If there is a mismatch with this number and the current number
     * of the privilege database this context is no longer valid.
     */
    [[nodiscard]] uint32_t getGeneration() const {
        return generation;
    }

    /**
     * Get a textual representation of this object in the format:
     *
     *   [privilege,privilege,privilege]
     *
     * An empty set is written as [none], and a full set is written
     * as [all].
     */
    [[nodiscard]] std::string to_string() const;

    /**
     * Clear all the privileges in this context which contains bucket
     * privileges.
     */
    void clearBucketPrivileges();

    /**
     * Set all the privileges in this context which contains bucket privileges.
     */
    void setBucketPrivileges();

    /**
     * Drop the named privilege from the privilege mask
     *
     * @param privilege the privilege to drop
     */
    void dropPrivilege(Privilege privilege);

    /// Is this privilege context stale, or may it be used?
    [[nodiscard]] bool isStale() const;

    /// @return true if there are scope privileges defined
    [[nodiscard]] bool hasScopePrivileges() const {
        return bucket && bucket->hasScopePrivileges();
    }

protected:
    void setBucketPrivilegeBits(bool value);

    /// The Database version this mask belongs to
    uint32_t generation;
    /// The Domain the mask belongs to
    Domain domain;
    /// The mask of effective privileges
    PrivilegeMask mask;

    /// The Bucket rbac setting
    std::shared_ptr<const Bucket> bucket;

    /// The set of dropped privileges
    PrivilegeMask droppedPrivileges;

    /// If set to true this object will never become stale
    /// (It is used together with token based authentication where
    /// the token contains the authorizations which should
    /// have the same lifespan as the token)
    bool never_stale = false;

    friend void to_json(nlohmann::json& json, const PrivilegeContext& ctx);
};

/**
 * Base class for exceptions thrown by the cb::rbac module in
 * case you want to handle all of them with the same catch block.
 */
class Exception : public std::runtime_error {
protected:
    explicit Exception(const char* msg) : std::runtime_error(msg) {
    }
};

/**
 * An exception class representing that the user doesn't exist in the
 * PrivilegeDatabase.
 */
class NoSuchUserException : public Exception {
public:
    explicit NoSuchUserException(const char* msg) : Exception(msg) {
    }
};

/**
 * An exception class representing that the bucket doesn't exist in the
 * PrivilegeDatabase.
 */
class NoSuchBucketException : public Exception {
public:
    explicit NoSuchBucketException(const char* msg) : Exception(msg) {
    }
};

/**
 * The PrivilegeDatabase is a container for all of the RBAC configuration
 * of the system.
 */
class PrivilegeDatabase {
public:
    /**
     * Create a new instance of the PrivilegeDatabase and initialize
     * it to the provided JSON
     *
     * @param json A JSON representation of the privilege database as
     *             specified above (or null to create an empty database)
     * @throws std::invalid_argument for invalid syntax
     * @throws std::bad_alloc if we run out of memory
     */
    PrivilegeDatabase(const nlohmann::json& json, Domain domain);

    /**
     * Try to look up a user in the privilege database
     *
     * @param user The name of the user to look up
     * @return The user entry for that user
     * @throws cb::rbac::NoSuchUserException if the user doesn't exist
     */
    [[nodiscard]] const UserEntry& lookup(const std::string& user) const;

    /**
     * Create a new PrivilegeContext for the specified user in the specified
     * bucket.
     *
     * @param user The name of the user
     * @param bucket The name of the bucket (may be "" if you're not
     *               connecting to a bucket (aka the no bucket)).
     * @return The privilege context representing the user in that bucket
     * @throws cb::rbac::NoSuchUserException if the user doesn't exist
     * @throws cb::rbac::NoSuchBucketException if the user doesn't have access
     *                                         to that bucket.
     */
    [[nodiscard]] PrivilegeContext createContext(
            const std::string& user,
            Domain domain,
            const std::string& bucket) const;

    std::unique_ptr<PrivilegeDatabase> updateUser(const std::string& user,
                                                  Domain domain,
                                                  UserEntry& entry) const;

    [[nodiscard]] nlohmann::json to_json(Domain domain) const;

    /**
     * The generation for this PrivilegeDatabase (a privilege context must
     * match this generation in order to be valid)
     */
    const uint32_t generation;

protected:
    std::unordered_map<std::string, UserEntry> userdb;
};

/**
 * Create a new PrivilegeContext for the specified user in the specified
 * bucket.
 *
 * @todo this might starve the writers?
 *
 * @param user The user to look up
 * @param bucket The name of the bucket (may be "" if you're not
 *               connecting to a bucket (aka the no bucket)).
 * @return The privilege context representing the user in that bucket
 * @throws cb::rbac::NoSuchUserException if the user doesn't exist
 * @throws cb::rbac::NoSuchBucketException if the user doesn't have access
 *                                         to that bucket.
 */
PrivilegeContext createContext(const UserIdent& user,
                               const std::string& bucket);

/**
 * Load the named file and install it as the current privilege database
 *
 * @param filename the name of the new file
 * @throws std::runtime_error
 */
void loadPrivilegeDatabase(const std::string& filename);

/**
 * Create a (local) privilege database based on the provided content
 *
 * @param content The JSON representation for the privilege database
 * @throws std::runtime_error
 */
void createPrivilegeDatabase(std::string_view content);

/**
 * Check if the specified user have access to the specified bucket
 */
bool mayAccessBucket(const UserIdent& user, const std::string& bucket);

/**
 * Update the user entry with the supplied new configuration
 *
 * @param descr the new definition for the user
 */
void updateExternalUser(std::string_view descr);

/**
 * Initialize the RBAC module
 */
void initialize();

/**
 * Destroy the RBAC module
 */
void destroy();

/**
 * Get the modification timestamp for an external user (if found)
 *
 * @param user the name of the user to search for
 * @return The modification timestamp for the user if found
 */
std::optional<std::chrono::steady_clock::time_point> getExternalUserTimestamp(
        const std::string& user);

/**
 * Dump the user database to JSON
 *
 * This should only be used for testing as it holds a read lock for the
 * database while generating the dump.
 */
nlohmann::json to_json(Domain domain);
} // namespace cb::rbac

template <>
struct std::hash<cb::rbac::UserIdent> {
    std::size_t operator()(cb::rbac::UserIdent const& user) const noexcept;
};
