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
static std::atomic<uint32_t> generation{0};

// The read write lock needed when you want to build a context
cb::RWLock rwlock;
std::unique_ptr<PrivilegeDatabase> db;

UserEntry::UserEntry(const cJSON& root) {
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

    it = cJSON_GetObjectItem(json, "type");
    if (it == nullptr) {
        domain = Domain::Builtin;
    } else if (it->type == cJSON_String) {
        if (strcasecmp("builtin", it->valuestring) == 0) {
            domain = Domain::Builtin;
        } else if (strcasecmp("saslauthd", it->valuestring) == 0) {
            domain = Domain::Saslauthd;
        } else {
            throw std::invalid_argument(
                    "UserEntry::UserEntry::"
                    " \"type\" should be \"builtin\" "
                    "or \"saslauthd\"");
        }
    } else {
        throw std::invalid_argument(
                "UserEntry::UserEntry::"
                " \"type\" should be a string");
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
        ret[int(Privilege::Write)] = false;
        ret[int(Privilege::DcpConsumer)] = false;
        ret[int(Privilege::DcpProducer)] = false;
        ret[int(Privilege::TapProducer)] = false;
        ret[int(Privilege::TapConsumer)] = false;
        ret[int(Privilege::MetaRead)] = false;
        ret[int(Privilege::MetaWrite)] = false;
        ret[int(Privilege::XattrRead)] = false;
        ret[int(Privilege::XattrWrite)] = false;
    }

    return ret;
}

PrivilegeDatabase::PrivilegeDatabase(const cJSON* json)
    : generation(cb::rbac::generation.operator++()) {

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
            throw NoSuchBucketException(bucket.c_str());
        }

        mask |= iter->second;
    }

    // Add the rest of the privileges
    mask |= ue.getPrivileges();
    return PrivilegeContext(generation, mask);
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

void PrivilegeContext::clearBucketPrivileges() {
    mask[int(Privilege::Read)] = false;
    mask[int(Privilege::Write)] = false;
    mask[int(Privilege::SimpleStats)] = false;
    mask[int(Privilege::DcpConsumer)] = false;
    mask[int(Privilege::DcpProducer)] = false;
    mask[int(Privilege::TapProducer)] = false;
    mask[int(Privilege::TapConsumer)] = false;
    mask[int(Privilege::MetaRead)] = false;
    mask[int(Privilege::MetaWrite)] = false;
    mask[int(Privilege::XattrRead)] = false;
    mask[int(Privilege::XattrWrite)] = false;
}

void PrivilegeContext::setBucketPrivileges() {
    mask[int(Privilege::Read)] = true;
    mask[int(Privilege::Write)] = true;
    mask[int(Privilege::SimpleStats)] = true;
    mask[int(Privilege::DcpConsumer)] = true;
    mask[int(Privilege::DcpProducer)] = true;
    mask[int(Privilege::TapProducer)] = true;
    mask[int(Privilege::TapConsumer)] = true;
    mask[int(Privilege::MetaRead)] = true;
    mask[int(Privilege::MetaWrite)] = true;
    mask[int(Privilege::XattrRead)] = true;
    mask[int(Privilege::XattrWrite)] = true;
}

PrivilegeContext createContext(const std::string& user,
                               const std::string& bucket) {
    std::lock_guard<cb::ReaderLock> guard(rwlock.reader());
    return db->createContext(user, bucket);
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

    std::unique_ptr<PrivilegeDatabase> database;
    // Guess what, MSVC wasn't happy with std::make_unique :P
    database.reset(new PrivilegeDatabase(json.get()));

    std::lock_guard<cb::WriterLock> guard(rwlock.writer());
    // Handle race conditions
    if (db->generation < database->generation) {
        db.swap(database);
    }
}

void initialize() {
    // Create an empty database to avoid having to add checks
    // if it exists or not... Guess what, MSVC wasn't happy with
    // std::make_unique :P
    db.reset(new PrivilegeDatabase(nullptr));
}

void destroy() {
    db.reset();
}

} // namespace rbac
} // namespace cb
