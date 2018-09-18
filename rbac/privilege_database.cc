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

#include <nlohmann/json.hpp>
#include <platform/dirutils.h>
#include <platform/rwlock.h>
#include <strings.h>
#include <atomic>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <streambuf>
#include <string>

namespace cb {
namespace rbac {

// Every time we create a new PrivilegeDatabase we bump the generation.
// The PrivilegeContext contains the generation number it was generated
// from so that we can easily detect if the PrivilegeContext is stale.
static std::atomic<uint32_t> generation{0};

// The read write lock needed when you want to build a context
cb::RWLock rwlock;
std::unique_ptr<PrivilegeDatabase> db;

bool UserEntry::operator==(const UserEntry& other) const {
    return (internal == other.internal && domain == other.domain &&
            privileges == other.privileges && buckets == other.buckets);
}

UserEntry::UserEntry(const std::string& username, const nlohmann::json& json) {
    // All system internal users is prefixed with @
    internal = username.front() == '@';

    // Domain must be present so that we know where it comes from
    auto iter = json.find("domain");
    if (iter != json.end()) {
        domain = cb::sasl::to_domain(iter->get<std::string>());
        if (internal && domain == cb::sasl::Domain::External) {
            throw std::runtime_error(
                    R"(UserEntry::UserEntry: internal users must be locally defined)");
        }
    }

    iter = json.find("privileges");
    if (iter != json.end()) {
        // Parse the privileges
        privileges = parsePrivileges(*iter, false);
    }

    iter = json.find("buckets");
    if (iter != json.end()) {
        if (!iter->is_object()) {
            throw std::invalid_argument(
                    R"(UserEntry::UserEntry: "buckets" should be an object)");
        }

        for (auto it = iter->begin(); it != iter->end(); ++it) {
            auto privileges = parsePrivileges(it.value(), true);
            if (privileges.any()) {
                buckets[it.key()] = privileges;
            }
        }
    }
}

nlohmann::json UserEntry::to_json() const {
    nlohmann::json ret;
    ret["domain"] = ::to_string(domain);
    ret["privileges"] = mask2string(privileges);
    for (const auto b : buckets) {
        ret["buckets"][b.first] = mask2string(b.second);
    }

    return ret;
}

std::vector<std::string> UserEntry::mask2string(
        const PrivilegeMask& mask) const {
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

PrivilegeMask UserEntry::parsePrivileges(const nlohmann::json& privs,
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

    if (buckets) {
        ret[int(Privilege::BucketManagement)] = false;
        ret[int(Privilege::NodeManagement)] = false;
        ret[int(Privilege::SessionManagement)] = false;
        ret[int(Privilege::Audit)] = false;
        ret[int(Privilege::AuditManagement)] = false;
        ret[int(Privilege::IdleConnection)] = false;
        ret[int(Privilege::CollectionManagement)] = false;
        ret[int(Privilege::Impersonate)] = false;
    } else {
        ret[int(Privilege::Read)] = false;
        ret[int(Privilege::Insert)] = false;
        ret[int(Privilege::Delete)] = false;
        ret[int(Privilege::Upsert)] = false;
        ret[int(Privilege::DcpConsumer)] = false;
        ret[int(Privilege::DcpProducer)] = false;
        ret[int(Privilege::Tap)] = false;
        ret[int(Privilege::MetaRead)] = false;
        ret[int(Privilege::MetaWrite)] = false;
        ret[int(Privilege::XattrRead)] = false;
        ret[int(Privilege::XattrWrite)] = false;
    }

    return ret;
}

PrivilegeDatabase::PrivilegeDatabase(const nlohmann::json& json)
    : generation(cb::rbac::generation.operator++()) {
    for (auto it = json.begin(); it != json.end(); ++it) {
        const std::string username = it.key();
        userdb.emplace(username, UserEntry(username, it.value()));
    }
}

std::unique_ptr<PrivilegeDatabase> PrivilegeDatabase::removeUser(
        const std::string& user) const {
    auto ret = std::make_unique<PrivilegeDatabase>(nullptr);
    ret->userdb = userdb;
    auto iter = ret->userdb.find(user);
    if (iter != ret->userdb.end()) {
        ret->userdb.erase(iter);
    }
    return ret;
}

std::unique_ptr<PrivilegeDatabase> PrivilegeDatabase::updateUser(
        const std::string& user, UserEntry& entry) const {
    // Check if they differ
    auto iter = userdb.find(user);
    if (iter != userdb.end() && entry == iter->second) {
        // This is the same entry I've got.. no need to do anything
        return std::unique_ptr<PrivilegeDatabase>{};
    }

    // They differ, I need to change the entry!
    auto ret = std::make_unique<PrivilegeDatabase>(nullptr);
    ret->userdb = userdb;
    iter = ret->userdb.find(user);
    if (iter != ret->userdb.end()) {
        ret->userdb.erase(iter);
    }
    ret->userdb.emplace(user, std::move(entry));
    return ret;
}

PrivilegeContext PrivilegeDatabase::createContext(
        const std::string& user, const std::string& bucket) const {
    PrivilegeMask mask;

    const auto& ue = lookup(user);

    if (!bucket.empty()) {
        // Add the bucket specific privileges
        auto iter = ue.getBuckets().find(bucket);
        if (iter == ue.getBuckets().cend()) {
            // No explicit match.. Is there a wildcard entry
            iter = ue.getBuckets().find("*");
            if (iter == ue.getBuckets().cend()) {
                throw NoSuchBucketException(bucket.c_str());
            }
        }

        mask |= iter->second;
    }

    // Add the rest of the privileges
    mask |= ue.getPrivileges();
    return PrivilegeContext(generation, mask);
}

std::pair<PrivilegeContext, bool> PrivilegeDatabase::createInitialContext(
        const std::string& user, cb::sasl::Domain domain) const {
    const auto& ue = lookup(user);
    if (ue.getDomain() != domain) {
        throw NoSuchUserException(user.c_str());
    }
    return {PrivilegeContext(generation, ue.getPrivileges()), ue.isInternal()};
}

nlohmann::json PrivilegeDatabase::to_json() const {
    nlohmann::json ret;
    for (const auto& entry : userdb) {
        ret[entry.first] = entry.second.to_json();
    }

    return ret;
}

PrivilegeAccess PrivilegeContext::check(Privilege privilege) const {
    if (generation != cb::rbac::generation) {
        return PrivilegeAccess::Stale;
    }

    const auto idx = size_t(privilege);
#ifndef NDEBUG
    if (idx >= mask.size()) {
        throw std::invalid_argument("Invalid privilege passed for the check)");
    }
#endif
    if (mask.test(idx)) {
        return PrivilegeAccess::Ok;
    }

    return PrivilegeAccess::Fail;
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

bool PrivilegeContext::dropPrivilege(Privilege privilege) {
    if (mask[int(privilege)]) {
        mask[int(privilege)] = false;
        return true;
    }

    return false;
}

void PrivilegeContext::clearBucketPrivileges() {
    setBucketPrivilegeBits(false);
}

void PrivilegeContext::setBucketPrivileges() {
    setBucketPrivilegeBits(true);
}

void PrivilegeContext::setBucketPrivilegeBits(bool value) {
    mask[int(Privilege::Read)] = value;
    mask[int(Privilege::Insert)] = value;
    mask[int(Privilege::Upsert)] = value;
    mask[int(Privilege::Delete)] = value;
    mask[int(Privilege::SimpleStats)] = value;
    mask[int(Privilege::DcpConsumer)] = value;
    mask[int(Privilege::DcpProducer)] = value;
    mask[int(Privilege::Tap)] = value;
    mask[int(Privilege::MetaRead)] = value;
    mask[int(Privilege::MetaWrite)] = value;
    mask[int(Privilege::XattrRead)] = value;
    mask[int(Privilege::SystemXattrRead)] = value;
    mask[int(Privilege::XattrWrite)] = value;
    mask[int(Privilege::SystemXattrWrite)] = value;
}

PrivilegeContext createContext(const std::string& user,
                               const std::string& bucket) {
    std::lock_guard<cb::ReaderLock> guard(rwlock.reader());
    return db->createContext(user, bucket);
}

std::pair<PrivilegeContext, bool> createInitialContext(
        const std::string& user, cb::sasl::Domain domain) {
    std::lock_guard<cb::ReaderLock> guard(rwlock.reader());
    return db->createInitialContext(user, domain);
}

void loadPrivilegeDatabase(const std::string& filename) {
    const auto content = cb::io::loadFile(filename);
    auto json = nlohmann::json::parse(content);
    auto database = std::make_unique<PrivilegeDatabase>(json);
    std::lock_guard<cb::WriterLock> guard(rwlock.writer());
    // Handle race conditions
    if (db->generation < database->generation) {
        db.swap(database);
    }
}

void initialize() {
    // Create an empty database to avoid having to add checks
    // if it exists or not...
    db = std::make_unique<PrivilegeDatabase>(nlohmann::json{});
}

void destroy() {
    db.reset();
}

bool mayAccessBucket(const std::string& user, const std::string& bucket) {
    try {
        cb::rbac::createContext(user, bucket);
        return true;
    } catch (const Exception&) {
        // The user do not have access to the bucket
    }

    return false;
}

void removeUser(const std::string& user) {
    std::lock_guard<cb::WriterLock> guard(rwlock.writer());
    auto next = db->removeUser(user);
    db.swap(next);
}

void updateUser(const std::string& user, const std::string& descr) {
    if (descr.empty()) {
        removeUser(user);
        return;
    }

    // Parse the JSON and create the UserEntry object before grabbing
    // the write lock!
    auto json = nlohmann::json::parse(descr);
    const std::string username = json.begin().key();
    UserEntry entry(username, json[username]);
    std::lock_guard<cb::WriterLock> guard(rwlock.writer());
    auto next = db->updateUser(user, entry);
    if (next) {
        // I changed the database.. swap
        db.swap(next);
    }
}

nlohmann::json to_json() {
    std::lock_guard<cb::ReaderLock> guard(rwlock.reader());
    return db->to_json();
}

} // namespace rbac
} // namespace cb
