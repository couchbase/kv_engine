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
        privileges = parsePrivileges(it);
    }

    it = cJSON_GetObjectItem(json, "buckets");
    if (it != nullptr) {
        if (it->type != cJSON_Object) {
            throw std::invalid_argument(
                    "UserEntry::UserEntry::"
                    " \"buckets\" should be an object");
        }

        for (it = it->child; it != nullptr; it = it->next) {
            buckets[it->string] = parsePrivileges(it);
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

PrivilegeMask UserEntry::parsePrivileges(const cJSON* priv) {
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

    return ret;
}

PrivilegeDatabase::PrivilegeDatabase(const cJSON* json)
    : generation(cb::rbac::generation.operator++()) {

    if (json != nullptr) {
        for (auto it = json->child; it != nullptr; it = it->next) {
            userdb.emplace(it->string, UserEntry(*it));
        }

        // for backwards compatibilty, create an entry for the "default" user
        try {
            lookup("default");
        } catch (const cb::rbac::NoSuchUserException&) {
            unique_cJSON_ptr def(cJSON_Parse("{  \"default\": {\n"
                                                 "    \"buckets\": {\n"
                                                 "      \"default\": [\n"
                                                 "        \"DcpConsumer\",\n"
                                                 "        \"DcpProducer\",\n"
                                                 "        \"MetaRead\",\n"
                                                 "        \"MetaWrite\",\n"
                                                 "        \"Read\",\n"
                                                 "        \"SimpleStats\",\n"
                                                 "        \"TapConsumer\",\n"
                                                 "        \"TapProducer\",\n"
                                                 "        \"Write\",\n"
                                                 "        \"XattrRead\",\n"
                                                 "        \"XattrWrite\"\n"
                                                 "      ]\n"
                                                 "    },\n"
                                                 "    \"privileges\": [],\n"
                                                 "    \"type\": \"builtin\",\n"
                                                 "    \"internal\": false\n"
                                                 "  }\n"
                                                 "}"));
            if (!def) {
                throw std::runtime_error(
                    "PrivilegeDatabase::PrivilegeDatabase: Failed to parse JSON for the default user");
            }

            json = def.get()->child;
            userdb.emplace(json->string, UserEntry(*json));
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
    return mask[idx] ? PrivilegeAccess::Ok : PrivilegeAccess::Fail;
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
