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
#include <utilities/logtags.h>
#include <atomic>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <streambuf>
#include <string>

namespace cb {
namespace rbac {

struct DatabaseContext {
    // Every time we create a new PrivilegeDatabase we bump the generation.
    // The PrivilegeContext contains the generation number it was generated
    // from so that we can easily detect if the PrivilegeContext is stale.
    std::atomic<uint32_t> generation{0};

    // The read write lock needed when you want to build a context
    cb::RWLock rwlock;
    std::unique_ptr<PrivilegeDatabase> db;
};

/// We keep one context for the local scope, and one for the external
DatabaseContext contexts[2];

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

bool UserEntry::operator==(const UserEntry& other) const {
    return (internal == other.internal && privileges == other.privileges &&
            buckets == other.buckets);
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

nlohmann::json UserEntry::to_json(Domain domain) const {
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
        ret[int(Privilege::SystemSettings)] = false;
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
        ret[int(Privilege::Select)] = false;
        ret[int(Privilege::Settings)] = false;
    }

    return ret;
}

PrivilegeDatabase::PrivilegeDatabase(const nlohmann::json& json, Domain domain)
    : generation(contexts[to_index(domain)].generation.operator++()) {
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
    return PrivilegeContext(generation, domain, mask);
}

std::pair<PrivilegeContext, bool> PrivilegeDatabase::createInitialContext(
        const std::string& user, Domain domain) const {
    const auto& ue = lookup(user);
    return {PrivilegeContext(generation, domain, ue.getPrivileges()),
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

PrivilegeAccess PrivilegeDatabase::check(const PrivilegeContext& context,
                                         Privilege privilege) const {
    if (context.getGeneration() != generation) {
        return PrivilegeAccess::Stale;
    }

    return context.check(privilege);
}

PrivilegeAccess PrivilegeContext::check(Privilege privilege) const {
    if (generation != contexts[to_index(domain)].generation) {
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
                               Domain domain,
                               const std::string& bucket) {
    auto& ctx = contexts[to_index(domain)];
    std::lock_guard<cb::ReaderLock> guard(ctx.rwlock.reader());
    return ctx.db->createContext(user, domain, bucket);
}

std::pair<PrivilegeContext, bool> createInitialContext(const std::string& user,
                                                       Domain domain) {
    auto& ctx = contexts[to_index(domain)];
    std::lock_guard<cb::ReaderLock> guard(ctx.rwlock.reader());
    return ctx.db->createInitialContext(user, domain);
}

void loadPrivilegeDatabase(const std::string& filename) {
    const auto content = cb::io::loadFile(filename);
    nlohmann::json json;
    try {
        json = nlohmann::json::parse(content);
    } catch (nlohmann::json::exception& exception) {
        std::stringstream ss;
        ss << "Failed to parse RBAC database: " << exception.what() << std::endl
           << cb::userdataStartTag << "RBAC database content: " << std::endl
           << "===========================================" << std::endl
           << content << std::endl
           << "===========================================" << std::endl
           << cb::userdataEndTag;

        throw std::runtime_error(ss.str());
    }
    auto database = std::make_unique<PrivilegeDatabase>(json, Domain::Local);

    auto& ctx = contexts[to_index(Domain::Local)];
    std::lock_guard<cb::WriterLock> guard(ctx.rwlock.writer());
    // Handle race conditions
    if (ctx.db->generation < database->generation) {
        ctx.db.swap(database);
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
    contexts[to_index(Domain::Local)].db.reset();
    contexts[to_index(Domain::External)].db.reset();
}

bool mayAccessBucket(const std::string& user,
                     Domain domain,
                     const std::string& bucket) {
    try {
        createContext(user, domain, bucket);
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

    std::lock_guard<cb::WriterLock> guard(ctx.rwlock.writer());
    auto next = ctx.db->updateUser(username, Domain::External, entry);
    if (next) {
        // I changed the database.. swap
        ctx.db.swap(next);
    }
}

nlohmann::json to_json(Domain domain) {
    auto& ctx = contexts[to_index(domain)];
    std::lock_guard<cb::ReaderLock> guard(ctx.rwlock.reader());
    return ctx.db->to_json(domain);
}

boost::optional<std::chrono::steady_clock::time_point> getExternalUserTimestamp(
        const std::string& user) {
    auto& ctx = contexts[to_index(Domain::External)];
    std::lock_guard<cb::WriterLock> guard(ctx.rwlock.reader());
    try {
        auto ue = ctx.db->lookup(user);
        return {ue.getTimestamp()};
    } catch (const NoSuchUserException&) {
        return {};
    }
}

} // namespace rbac
} // namespace cb
