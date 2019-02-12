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

#include <cJSON_utils.h>
#include <platform/make_unique.h>
#include <platform/memorymap.h>
#include <platform/rwlock.h>
#include <strings.h>
#include <atomic>
#include <fstream>
#include <iostream>
#include <mutex>
#include <streambuf>
#include <string>

namespace cb {
namespace rbac {

// Every time we create a new PrivilegeDatabase we bump the generation.
// The PrivilegeContext contains the generation number it was generated
// from so that we can easily detect if the PrivilegeContext is stale.
// The current_generation contains the version number of the PrivilegeDatabase
// currently in use, and create_generation is the counter being used
// to work around race conditions where multiple threads is trying to
// create and update the RBAC database (last one wins)
static std::atomic<uint32_t> current_generation{0};
static std::atomic<uint32_t> create_generation{0};

// The read write lock needed when you want to build a context
cb::RWLock rwlock;
std::unique_ptr<PrivilegeDatabase> db;

UserEntry::UserEntry(const cJSON& root) {
    if (root.string == nullptr) {
        throw std::invalid_argument(
            "UserEntry::UserEntry: string can't be nullptr. It should contain username");
    }

    // All system internal users is prefixed with @
    internal = root.string[0] == '@';

    auto* json = const_cast<cJSON*>(&root);
    const auto* it = cJSON_GetObjectItem(json, "privileges");
    if (it != nullptr) {
        privileges = parsePrivileges(it, false);
    }

    it = cJSON_GetObjectItem(json, "buckets");
    if (it != nullptr) {
        if (it->type != cJSON_Object) {
            throw std::invalid_argument(
                    "UserEntry::UserEntry::"
                    " \"buckets\" should be an object");
        }

        for (it = it->child; it != nullptr; it = it->next) {
            auto privileges = parsePrivileges(it, true);
            if (privileges.any()) {
                buckets[it->string] = privileges;
            }
        }
    }

    it = cJSON_GetObjectItem(json, "domain");
    if (it == nullptr) {
        domain = cb::sasl::Domain::Local;
    } else if (it->type == cJSON_String) {
        domain = cb::sasl::to_domain(it->valuestring);
    } else {
        throw std::invalid_argument(
                "UserEntry::UserEntry::"
                " \"domain\" should be a string");
    }
}

PrivilegeMask UserEntry::parsePrivileges(const cJSON* priv, bool buckets) {
    PrivilegeMask ret;

    for (const auto* it = priv->child; it != nullptr; it = it->next) {
        if (it->type != cJSON_String) {
            throw std::runtime_error(
                    "UserEntry::parsePrivileges: privileges must be specified "
                    "as strings");
        }

        const std::string str(it->valuestring);
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

PrivilegeDatabase::PrivilegeDatabase(const cJSON* json)
    : generation(cb::rbac::create_generation.operator++()) {

    if (json != nullptr) {
        for (auto it = json->child; it != nullptr; it = it->next) {
            userdb.emplace(it->string, UserEntry(*it));
        }
    }
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
        const std::string& user, cb::sasl::Domain domain) {
    const auto& ue = lookup(user);
    if (ue.getDomain() != domain) {
        throw NoSuchUserException(user.c_str());
    }
    return {PrivilegeContext(generation, ue.getPrivileges()), ue.isInternal()};
}

PrivilegeAccess PrivilegeContext::check(Privilege privilege) const {
    if (generation != cb::rbac::current_generation) {
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
    cb::MemoryMappedFile map(filename.c_str(),
                             cb::MemoryMappedFile::Mode::RDONLY);
    map.open();
    std::string content(reinterpret_cast<char*>(map.getRoot()), map.getSize());
    map.close();

    unique_cJSON_ptr json(cJSON_Parse(content.c_str()));
    if (json.get() == nullptr) {
        throw std::runtime_error(
                "PrivilegeDatabaseManager::load: Failed to parse json");
    }

    auto database = std::make_unique<PrivilegeDatabase>(json.get());

    std::lock_guard<cb::WriterLock> guard(rwlock.writer());
    // Handle race conditions
    if (db->generation < database->generation) {
        db.swap(database);
        current_generation = db->generation;
    }
}

void initialize() {
    // Create an empty database to avoid having to add checks
    // if it exists or not...
    db = std::make_unique<PrivilegeDatabase>(nullptr);
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

} // namespace rbac
} // namespace cb
