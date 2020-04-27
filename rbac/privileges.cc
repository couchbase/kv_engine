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

#include <memcached/rbac/privileges.h>
#include <platform/string_hex.h>
#include <stdexcept>
#include <unordered_map>

namespace cb::rbac {

static const std::unordered_map<std::string, Privilege> privilegemap = {
        {"Read", Privilege::Read},
        {"Insert", Privilege::Insert},
        {"Delete", Privilege::Delete},
        {"Upsert", Privilege::Upsert},
        {"SimpleStats", Privilege::SimpleStats},
        {"Stats", Privilege::Stats},
        {"BucketManagement", Privilege::BucketManagement},
        {"NodeManagement", Privilege::NodeManagement},
        {"SessionManagement", Privilege::SessionManagement},
        {"Audit", Privilege::Audit},
        {"AuditManagement", Privilege::AuditManagement},
        {"DcpConsumer", Privilege::DcpConsumer},
        {"DcpProducer", Privilege::DcpProducer},
        {"DcpStream", Privilege::DcpStream},
        {"Tap", Privilege::Tap},
        {"MetaRead", Privilege::MetaRead},
        {"MetaWrite", Privilege::MetaWrite},
        {"IdleConnection", Privilege::IdleConnection},
        {"XattrRead", Privilege::XattrRead},
        {"SystemXattrRead", Privilege::SystemXattrRead},
        {"XattrWrite", Privilege::XattrWrite},
        {"SystemXattrWrite", Privilege::SystemXattrWrite},
        {"CollectionManagement", Privilege::CollectionManagement},
        {"SecurityManagement", Privilege::SecurityManagement},
        {"Impersonate", Privilege::Impersonate},
        {"Select", Privilege::Select},
        {"Settings", Privilege::Settings},
        {"SystemSettings", Privilege::SystemSettings}};

std::string to_string(Privilege privilege) {
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

std::string PrivilegeAccess::to_string() const {
    switch (status) {
    case Status::Ok:
        return "Ok";
    case Status::Fail:
        return "Fail";
    case Status::FailNoPrivileges:
        return "FailNoPrivileges";
    }
    throw std::invalid_argument(
            "PrivilegeAccess::to_string(): Unknown status: " +
            std::to_string(int(status)));
}

/// is this a privilege related to a bucket or not
bool is_bucket_privilege(Privilege priv) {
    switch (priv) {
    case Privilege::Read:
    case Privilege::Insert:
    case Privilege::Delete:
    case Privilege::Upsert:
    case Privilege::DcpConsumer:
    case Privilege::DcpProducer:
    case Privilege::DcpStream:
    case Privilege::Tap:
    case Privilege::MetaRead:
    case Privilege::MetaWrite:
    case Privilege::XattrRead:
    case Privilege::SystemXattrRead:
    case Privilege::XattrWrite:
    case Privilege::SystemXattrWrite:
    case Privilege::Select:
    case Privilege::Settings:
    case Privilege::SimpleStats:
        return true;

    case Privilege::BucketManagement:
    case Privilege::NodeManagement:
    case Privilege::SessionManagement:
    case Privilege::Audit:
    case Privilege::AuditManagement:
    case Privilege::IdleConnection:
    case Privilege::CollectionManagement:
    case Privilege::SecurityManagement:
    case Privilege::Impersonate:
    case Privilege::SystemSettings:
    case Privilege::Stats:
        return false;
    }

    throw std::invalid_argument(
            "is_bucket_privilege() invalid privilege provided: " +
            cb::to_hex(uint8_t(priv)));
}

bool is_collection_privilege(Privilege priv) {
    switch (priv) {
    case Privilege::Read:
    case Privilege::Insert:
    case Privilege::Delete:
    case Privilege::Upsert:
    case Privilege::MetaRead:
    case Privilege::MetaWrite:
    case Privilege::XattrRead:
    case Privilege::SystemXattrRead:
    case Privilege::XattrWrite:
    case Privilege::SystemXattrWrite:
    case Privilege::DcpStream:
    case Privilege::SimpleStats:
        return true;

    case Privilege::DcpConsumer:
    case Privilege::DcpProducer:
    case Privilege::Tap:
    case Privilege::Settings:
    case Privilege::Select:
    case Privilege::BucketManagement:
    case Privilege::NodeManagement:
    case Privilege::SessionManagement:
    case Privilege::Audit:
    case Privilege::AuditManagement:
    case Privilege::IdleConnection:
    case Privilege::CollectionManagement:
    case Privilege::SecurityManagement:
    case Privilege::Impersonate:
    case Privilege::SystemSettings:
    case Privilege::Stats:
        return false;
    }

    throw std::invalid_argument(
            "is_collection_privilege() invalid privilege provided: " +
            cb::to_hex(uint8_t(priv)));
}

} // namespace cb::rbac
