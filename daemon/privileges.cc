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

#include <memcached/privileges.h>
#include <stdexcept>
#include <unordered_map>

namespace cb {
namespace rbac {

static const std::unordered_map<std::string, Privilege> privilegemap = {
        {"Read", Privilege::Read},
        {"Write", Privilege::Write},
        {"SimpleStats", Privilege::SimpleStats},
        {"Stats", Privilege::Stats},
        {"BucketManagement", Privilege::BucketManagement},
        {"NodeManagement", Privilege::NodeManagement},
        {"SessionManagement", Privilege::SessionManagement},
        {"Audit", Privilege::Audit},
        {"AuditManagement", Privilege::AuditManagement},
        {"DcpConsumer", Privilege::DcpConsumer},
        {"DcpProducer", Privilege::DcpProducer},
        {"TapProducer", Privilege::TapProducer},
        {"TapConsumer", Privilege::TapConsumer},
        {"MetaRead", Privilege::MetaRead},
        {"MetaWrite", Privilege::MetaWrite},
        {"IdleConnection", Privilege::IdleConnection},
        {"XattrRead", Privilege::XattrRead},
        {"XattrWrite", Privilege::XattrWrite},
        {"CollectionManagement", Privilege::CollectionManagement},
        {"Impersonate", Privilege::Impersonate}};

std::string to_string(const Privilege& privilege) {
    for (const auto& entry : privilegemap) {
        if (entry.second == privilege) {
            return entry.first;
        }
    }

    throw std::invalid_argument("to_string: Unknown privilege detected: " +
                                std::to_string(int(privilege)));
}

Privilege to_privilege(const std::string& str) {
    auto it = privilegemap.find(str);
    if (it == privilegemap.cend()) {
        throw std::invalid_argument("to_privilege: Unknown privilege: " + str);
    }
    return it->second;
}
}
}
