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
        {"MetaWrite", Privilege::MetaWrite},
        {"IdleConnection", Privilege::IdleConnection},
        {"SystemXattrRead", Privilege::SystemXattrRead},
        {"SystemXattrWrite", Privilege::SystemXattrWrite},
        {"SecurityManagement", Privilege::SecurityManagement},
        {"Impersonate", Privilege::Impersonate},
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
    case Privilege::MetaWrite:
    case Privilege::SystemXattrRead:
    case Privilege::SystemXattrWrite:
    case Privilege::Settings:
    case Privilege::SimpleStats:
        return true;

    case Privilege::BucketManagement:
    case Privilege::NodeManagement:
    case Privilege::SessionManagement:
    case Privilege::Audit:
    case Privilege::AuditManagement:
    case Privilege::IdleConnection:
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
    case Privilege::MetaWrite:
    case Privilege::SystemXattrRead:
    case Privilege::SystemXattrWrite:
    case Privilege::DcpStream:
    case Privilege::SimpleStats:
        return true;

    case Privilege::DcpConsumer:
    case Privilege::DcpProducer:
    case Privilege::Settings:
    case Privilege::BucketManagement:
    case Privilege::NodeManagement:
    case Privilege::SessionManagement:
    case Privilege::Audit:
    case Privilege::AuditManagement:
    case Privilege::IdleConnection:
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
