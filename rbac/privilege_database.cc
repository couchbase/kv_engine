/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
#include <memcached/rbac.h>

#include <folly/Synchronized.h>
#include <nlohmann/json.hpp>
#include <platform/dirutils.h>
#include <utilities/logtags.h>
#include <atomic>
#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>
#include <streambuf>
#include <string>

namespace cb::rbac {

nlohmann::json UserIdent::to_json() const {
    nlohmann::json ret;
    ret["user"] = name;
    ret["domain"] = ::to_string(domain);
    return ret;
}

struct DatabaseContext {
    // Every time we create a new PrivilegeDatabase we bump the generation.
    // The PrivilegeContext contains the generation number it was generated
    // from so that we can easily detect if the PrivilegeContext is stale.
    // The current_generation contains the version number of the
    // PrivilegeDatabase currently in use, and create_generation is the counter
    // being used to work around race conditions where multiple threads is
    // trying to create and update the RBAC database (last one wins)
    std::atomic<uint32_t> current_generation{0};
    std::atomic<uint32_t> create_generation{0};

    folly::Synchronized<std::unique_ptr<PrivilegeDatabase>> db;
};

/// We keep one context for the local scope, and one for the external
DatabaseContext contexts[2];

static std::vector<std::string> privilegeMask2Vector(
        const PrivilegeMask& mask) {
    std::vector<std::string> ret;

    if (mask.all()) {
        ret.emplace_back("all");
        return ret;
    }

    for (std::size_t ii = 0; ii < mask.size(); ++ii) {
        if (mask.test(ii)) {
            ret.emplace_back(cb::rbac::to_string(Privilege(ii)));
        }
    }

    return ret;
}

/**
 * Parse a JSON array containing a set of privileges.
 *
 * @param priv The JSON array to parse
 * @param buckets Set to true if this is for the bucket list (which
 *                will mask out some of the privileges you can't
 *                specify for a bucket)
 * @return A privilege mask containing the privileges found in the JSON
 */
static PrivilegeMask parsePrivileges(const nlohmann::json& privs,
                                     bool buckets) {
    PrivilegeMask ret;
    for (const auto& priv : privs) {
        const std::string str{priv.get<std::string>()};
        if (str == "all") {
            ret.set();
        } else {
            ret[int(to_privilege(str))] = true;
        }
    }

    for (std::size_t ii = 0; ii < ret.size(); ++ii) {
        if (buckets == !is_bucket_privilege(Privilege(ii))) {
            ret[ii] = false;
        }
    }

    return ret;
}

/// Convert the Domain into the correct index to use into the contexts
/// array.
static int to_index(Domain domain) {
    switch (domain) {
    case Domain::Local:
        return 0;
    case Domain::External:
        return 1;
    }

    throw std::invalid_argument("to_index(): Invalid domain provided");
}

bool Collection::operator==(const Collection& other) const {
    return privilegeMask == other.privilegeMask;
}

Collection::Collection(const nlohmann::json& json) {
    auto iter = json.find("privileges");
    if (iter != json.end()) {
        privilegeMask = parsePrivileges(*iter, true);
    } else {
        throw std::invalid_argument(
                "rbac::Collection(json) \"collections\" with no \"privileges\" "
                "key is invalid");
    }

    if (privilegeMask.none()) {
        throw std::invalid_argument(
                "rbac::Collection(json) \"collections\" with empty "
                "\"privileges\" is invalid");
    }
}

nlohmann::json Collection::to_json() const {
    return nlohmann::json{privilegeMask2Vector(privilegeMask)};
}

PrivilegeAccess Collection::check(Privilege privilege) const {
    return privilegeMask.test(uint8_t(privilege)) ? PrivilegeAccessOk
                                                  : PrivilegeAccessFail;
}

bool Scope::operator==(const Scope& other) const {
    return privilegeMask == other.privilegeMask &&
           collections == other.collections;
}

Scope::Scope(const nlohmann::json& json) {
    auto iter = json.find("privileges");
    if (iter != json.end()) {
        privilegeMask = parsePrivileges(*iter, true);
    }
    iter = json.find("collections");
    if (iter != json.end()) {
        for (auto it = iter->begin(); it != iter->end(); ++it) {
            size_t pos = 0;
            uint32_t cid = std::stoul(it.key(), &pos, 16);
            if (it.key().length() != pos) {
                throw std::invalid_argument(
                        "Scope::Scope(): Extra characters present for CID");
            }
            collections.emplace(cid, Collection(it.value()));
        }
    }

    // scopes can only have no/empty privileges if there are collections
    if (collections.empty() && privilegeMask.none()) {
        throw std::invalid_argument(
                "rbac::Scope(json) \"scopes\" with no \"privileges\" and no "
                "\"collections\" is invalid");
    }
}

nlohmann::json Scope::to_json() const {
    nlohmann::json ret;
    ret["privileges"] = privilegeMask2Vector(privilegeMask);
    for (auto& e : collections) {
        ret["collections"][std::to_string(e.first)] = e.second.to_json();
    }

    return ret;
}

PrivilegeAccess Scope::check(Privilege privilege,
                             std::optional<uint32_t> collection,
                             bool parentHasCollectionPrivileges) const {
    if (privilegeMask.test(uint8_t(privilege))) {
        return PrivilegeAccessOk;
    }

    // No collection-ID, cannot go deeper - so fail
    if (!collection) {
        return PrivilegeAccessFail;
    }

    const auto iter = collections.find(*collection);
    if (iter == collections.end()) {
        // Collection is not found, but to determine the failure, check if any
        // collection privileges exist in the search
        return privilegeMask.any() || parentHasCollectionPrivileges
                       ? PrivilegeAccessFail
                       : PrivilegeAccessFailNoPrivileges;
    }

    // delegate the check to the collections
    return iter->second.check(privilege);
}

bool Bucket::operator==(const Bucket& other) const {
    return privilegeMask == other.privilegeMask && scopes == other.scopes;
}

Bucket::Bucket(const nlohmann::json& json) {
    if (json.is_array()) {
        // This is the old file format and everything should be
        // a list of privileges
        privilegeMask = parsePrivileges(json, true);
    } else {
        auto iter = json.find("privileges");
        if (iter != json.end()) {
            privilegeMask = parsePrivileges(*iter, true);
        }
        iter = json.find("scopes");
        if (iter != json.end()) {
            for (auto it = iter->begin(); it != iter->end(); ++it) {
                size_t pos = 0;
                uint32_t sid = std::stoul(it.key(), &pos, 16);
                if (it.key().length() != pos) {
                    throw std::invalid_argument(
                            "Bucket::Bucket(): Extra characters present for "
                            "SID");
                }
                scopes.emplace(sid, Scope(it.value()));
            }
        }
    }

    // Count how many privileges at the bucket are applicable to collections
    for (std::size_t ii = 0;
         ii < privilegeMask.size() && !collectionPrivilegeExists;
         ++ii) {
        if (is_collection_privilege(Privilege(ii)) && privilegeMask.test(ii)) {
            collectionPrivilegeExists = true;
        }
    }
}

nlohmann::json Bucket::to_json() const {
    nlohmann::json ret;
    ret["privileges"] = privilegeMask2Vector(privilegeMask);
    for (auto& e : scopes) {
        ret["scopes"][std::to_string(e.first)] = e.second.to_json();
    }

    return ret;
}

PrivilegeAccess Bucket::check(Privilege privilege,
                              std::optional<uint32_t> scope,
                              std::optional<uint32_t> collection) const {
    if (privilegeMask.test(uint8_t(privilege))) {
        return PrivilegeAccessOk;
    }

    PrivilegeAccess status(PrivilegeAccess::Status::Fail);
    // We don't have any scope to search the next level or it's not a privilege
    // that would be permissible at a lower level
    if (scope && is_collection_privilege(privilege)) {
        const auto iter = scopes.find(*scope);
        if (iter != scopes.end()) {
            // Delegate the check to the scopes
            status = iter->second.check(
                    privilege, collection, collectionPrivilegeExists);
        } else {
            // They don't have that scope at all, but do they have any
            // collection privileges which will determine  the error code.
            status = collectionPrivilegeExists
                             ? PrivilegeAccessFail
                             : PrivilegeAccessFailNoPrivileges;
        }
    }

    return status;
}

bool UserEntry::operator==(const UserEntry& other) const {
    return (internal == other.internal &&
            privilegeMask == other.privilegeMask && buckets == other.buckets);
}

UserEntry::UserEntry(const std::string& username,
                     const nlohmann::json& json,
                     Domain expectedDomain)
    : timestamp(std::chrono::steady_clock::now()) {
    // All system internal users is prefixed with @
    internal = username.front() == '@';

    // Domain must be present so that we know where it comes from
    auto iter = json.find("domain");
    if (iter != json.end()) {
        const auto domain = cb::sasl::to_domain(iter->get<std::string>());
        if (domain != expectedDomain) {
            throw std::runtime_error(
                    R"(UserEntry::UserEntry: Invalid domain in this context)");
        }

        if (internal && domain != Domain::Local) {
            throw std::runtime_error(
                    R"(UserEntry::UserEntry: Internal users should be local)");
        }
    }

    iter = json.find("privileges");
    if (iter != json.end()) {
        // Parse the privileges
        privilegeMask = parsePrivileges(*iter, false);
    }

    iter = json.find("buckets");
    if (iter != json.end()) {
        if (!iter->is_object()) {
            throw std::invalid_argument(
                    R"(UserEntry::UserEntry: "buckets" should be an object)");
        }

        for (auto it = iter->begin(); it != iter->end(); ++it) {
            buckets.emplace(it.key(), std::make_shared<Bucket>(it.value()));
        }
    }
}

nlohmann::json UserEntry::to_json(Domain domain) const {
    nlohmann::json ret;
    ret["domain"] = ::to_string(domain);
    ret["privileges"] = privilegeMask2Vector(privilegeMask);
    for (const auto& b : buckets) {
        ret["buckets"][b.first] = b.second->to_json();
    }

    return ret;
}

PrivilegeDatabase::PrivilegeDatabase(const nlohmann::json& json, Domain domain)
    : generation(contexts[to_index(domain)].create_generation.operator++()) {
    for (auto it = json.begin(); it != json.end(); ++it) {
        const std::string username = it.key();
        userdb.emplace(username, UserEntry(username, it.value(), domain));
    }
}

std::unique_ptr<PrivilegeDatabase> PrivilegeDatabase::updateUser(
        const std::string& user, Domain domain, UserEntry& entry) const {
    // Check if they differ
    auto iter = userdb.find(user);
    if (iter != userdb.end() && entry == iter->second) {
        // This is the same entry I've got.. no need to do anything, just
        // make sure that we timestamp it
        iter->second.setTimestamp(std::chrono::steady_clock::now());
        return std::unique_ptr<PrivilegeDatabase>{};
    }

    // They differ, I need to change the entry!
    auto ret = std::make_unique<PrivilegeDatabase>(nullptr, domain);
    ret->userdb = userdb;
    iter = ret->userdb.find(user);
    if (iter != ret->userdb.end()) {
        ret->userdb.erase(iter);
    }
    ret->userdb.emplace(user, std::move(entry));
    return ret;
}

PrivilegeContext PrivilegeDatabase::createContext(
        const std::string& user,
        Domain domain,
        const std::string& bucket) const {
    const auto& ue = lookup(user);
    if (bucket.empty()) {
        return PrivilegeContext(generation, domain, ue.getPrivileges(), {});
    }

    // Add the bucket specific privileges
    auto iter = ue.getBuckets().find(bucket);
    if (iter == ue.getBuckets().cend()) {
        // No explicit match.. Is there a wildcard entry
        iter = ue.getBuckets().find("*");
        if (iter == ue.getBuckets().cend()) {
            throw NoSuchBucketException(bucket.c_str());
        }
    }

    return PrivilegeContext(
            generation, domain, ue.getPrivileges(), iter->second);
}

std::pair<PrivilegeContext, bool> PrivilegeDatabase::createInitialContext(
        const UserIdent& user) const {
    const auto& ue = lookup(user.name);
    return {PrivilegeContext(generation, user.domain, ue.getPrivileges(), {}),
            ue.isInternal()};
}

nlohmann::json PrivilegeDatabase::to_json(Domain domain) const {
    nlohmann::json ret;
    for (const auto& entry : userdb) {
        ret[entry.first] = entry.second.to_json(domain);
    }

    return ret;
}

const UserEntry& PrivilegeDatabase::lookup(const std::string& user) const {
    auto iter = userdb.find(user);
    if (iter == userdb.cend()) {
        throw NoSuchUserException(user.c_str());
    }

    return iter->second;
}

void PrivilegeContext::dropPrivilege(Privilege privilege) {
    // Given that we're using a shared_ptr to the buckets we can't modify
    // the privilege mask for the buckets/scopes/collections.
    // Keep them around in a vector and check it later on.
    droppedPrivileges.push_back(privilege);
}

bool PrivilegeContext::isStale() const {
    return generation != contexts[to_index(domain)].current_generation;
}

PrivilegeAccess PrivilegeContext::check(Privilege privilege,
                                        std::optional<ScopeID> sid,
                                        std::optional<CollectionID> cid) const {
    const auto idx = size_t(privilege);
#ifndef NDEBUG
    if (idx >= mask.size()) {
        throw std::invalid_argument("Invalid privilege passed for the check)");
    }
#endif

    if (cid && !sid) {
        throw std::invalid_argument(
                "PrivilegeContext::check: can't provide cid and no sid");
    }

    // Check if the user dropped the privilege over the connection.
    if (!droppedPrivileges.empty()) {
        if (std::find(droppedPrivileges.begin(),
                      droppedPrivileges.end(),
                      privilege) != droppedPrivileges.end()) {
            return PrivilegeAccessFail;
        }
    }

    if (mask.test(idx)) {
        return PrivilegeAccessOk;
    }

    if (bucket && is_bucket_privilege(privilege)) {
        return bucket->check(
                privilege,
                sid ? std::optional<uint32_t>(*sid) : std::nullopt,
                cid ? std::optional<uint32_t>(*cid) : std::nullopt);
    }

    return PrivilegeAccessFail;
}

std::string PrivilegeContext::to_string() const {
    if (mask.all()) {
        return "[all]";
    } else if (mask.none()) {
        return "[none]";
    }

    std::string ret;
    ret.reserve(80);
    ret.append("[");
    for (size_t ii = 0; ii < mask.size(); ++ii) {
        if (mask.test(ii)) {
            ret.append(cb::rbac::to_string(Privilege(ii)));
            ret.append(",");
        }
    }
    ret.back() = ']';

    return ret;
}

void PrivilegeContext::clearBucketPrivileges() {
    setBucketPrivilegeBits(false);
}

void PrivilegeContext::setBucketPrivileges() {
    setBucketPrivilegeBits(true);
}

void PrivilegeContext::setBucketPrivilegeBits(bool value) {
    for (std::size_t ii = 0; ii < mask.size(); ++ii) {
        if (is_bucket_privilege(Privilege(ii))) {
            mask[ii] = value;
        }
    }
}

PrivilegeContext createContext(const UserIdent& user,
                               const std::string& bucket) {
    auto& ctx = contexts[to_index(user.domain)];
    return (*ctx.db.rlock())->createContext(user.name, user.domain, bucket);
}

std::pair<PrivilegeContext, bool> createInitialContext(const UserIdent& user) {
    auto& ctx = contexts[to_index(user.domain)];
    return (*ctx.db.rlock())->createInitialContext(user);
}

void loadPrivilegeDatabase(const std::string& filename) {
    const auto content = cb::io::loadFile(filename, std::chrono::seconds{5});
    std::unique_ptr<PrivilegeDatabase> database;
    std::string error;

    // In MB-40238 we saw something we think might be an invalid configuration
    // being provided to us from ns_server, but we didn't log the content
    // of the database because it wasn't one of the nlohmann exceptions being
    // thrown. Extend the logging to also include logic_errors (std::stoi may
    // throw std::invalid_argument), and runtime_error to make sure we push
    // the content of the database to the caller.
    try {
        nlohmann::json json;
        json = nlohmann::json::parse(content);
        database = std::make_unique<PrivilegeDatabase>(json, Domain::Local);
    } catch (nlohmann::json::exception& e) {
        error = e.what();
    } catch (const std::logic_error& e) {
        error = e.what();
    } catch (const std::runtime_error& e) {
        error = e.what();
    }

    if (!error.empty()) {
        std::stringstream ss;
        ss << "Failed to parse RBAC database: " << error << std::endl
           << cb::userdataStartTag << "RBAC database content: " << std::endl
           << "===========================================" << std::endl
           << content << std::endl
           << "===========================================" << std::endl
           << cb::userdataEndTag;

        throw std::runtime_error(ss.str());
    }

    auto& ctx = contexts[to_index(Domain::Local)];

    auto locked = ctx.db.wlock();
    // Handle race conditions
    if ((*locked)->generation < database->generation) {
        ctx.current_generation = database->generation;
        locked->swap(database);
    }
}

void initialize() {
    // Create an empty database to avoid having to add checks
    // if it exists or not...
    contexts[to_index(Domain::Local)].db = std::make_unique<PrivilegeDatabase>(
            nlohmann::json{}, Domain::Local);
    contexts[to_index(Domain::External)].db =
            std::make_unique<PrivilegeDatabase>(nlohmann::json{},
                                                Domain::External);
}

void destroy() {
    contexts[to_index(Domain::Local)].db.wlock()->reset();
    contexts[to_index(Domain::External)].db.wlock()->reset();
}

bool mayAccessBucket(const UserIdent& user, const std::string& bucket) {
    try {
        createContext(user, bucket);
        return true;
    } catch (const Exception&) {
        // The user do not have access to the bucket
    }

    return false;
}

void updateExternalUser(const std::string& descr) {
    // Parse the JSON and create the UserEntry object before grabbing
    // the write lock!
    auto json = nlohmann::json::parse(descr);
    const std::string username = json.begin().key();
    UserEntry entry(username, json[username], Domain::External);

    auto& ctx = contexts[to_index(Domain::External)];

    auto locked = ctx.db.wlock();
    auto next = (*locked)->updateUser(username, Domain::External, entry);
    if (next) {
        // I changed the database. Update the context gen counter and
        // swap the databases
        ctx.current_generation = next->generation;
        locked->swap(next);
    }
}

nlohmann::json to_json(Domain domain) {
    auto& ctx = contexts[to_index(domain)];
    return (*ctx.db.rlock())->to_json(domain);
}

std::optional<std::chrono::steady_clock::time_point> getExternalUserTimestamp(
        const std::string& user) {
    auto& ctx = contexts[to_index(Domain::External)];
    try {
        auto ue = (*ctx.db.rlock())->lookup(user);
        return {ue.getTimestamp()};
    } catch (const NoSuchUserException&) {
        return {};
    }
}

} // namespace cb::rbac
