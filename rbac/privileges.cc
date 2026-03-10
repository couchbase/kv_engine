/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <fmt/format.h>
#include <gsl/gsl-lite.hpp>
#include <memcached/rbac/privileges.h>
#include <nlohmann/json.hpp>
#include <array>
#include <bitset>
#include <stdexcept>

namespace cb::rbac {

/// Enum describing the various attributes associated with a privilege.
enum class Attribute {
    /// Is this privilege applicable at the bucket level
    Bucket,
    /// Is this privilege applicable at the collection level
    Collection,
    Count
};

/// Each privilege contains a name and a set of attributes.
struct PrivilegeMeta {
    PrivilegeMeta() = default;
    PrivilegeMeta(std::string_view name, const std::vector<Attribute>& attrs)
        : name(name) {
        for (const auto& a : attrs) {
            attributes.set(static_cast<std::size_t>(a));
        }
    }
    bool check(Attribute attr) const {
        return attributes.test(static_cast<std::size_t>(attr));
    }
    std::string_view name;
    std::bitset<static_cast<std::size_t>(Attribute::Count)> attributes;
};

/// A singleton class containing information for all privileges.
class PrivilegeInformationService {
public:
    bool isLegal(Privilege priv) const {
        const auto idx = static_cast<std::size_t>(priv);
        return idx < privileges.size() && !privileges[idx].name.empty();
    }

    std::string_view name(Privilege priv) const {
        ensureValid(priv, "name");
        return privileges[static_cast<std::size_t>(priv)].name;
    }

    bool isBucketPrivilege(Privilege priv) const {
        ensureValid(priv, "isBucketPrivilege");
        return privileges[static_cast<std::size_t>(priv)].check(
                Attribute::Bucket);
    }

    bool isCollectionPrivilege(Privilege priv) const {
        ensureValid(priv, "isCollectionPrivilege");
        return privileges[static_cast<std::size_t>(priv)].check(
                Attribute::Collection);
    }

    Privilege lookup(std::string_view str) const {
        for (std::size_t ii = 0; ii < privileges.size(); ++ii) {
            if (privileges[ii].name == str) {
                return static_cast<Privilege>(ii);
            }
        }
        throw std::invalid_argument(
                fmt::format("to_privilege: Unknown privilege: {}", str));
    }

    PrivilegeInformationService() {
        using namespace std::string_view_literals;
        setup(Privilege::Read,
              {"Read"sv, {Attribute::Bucket, Attribute::Collection}});
        setup(Privilege::Insert,
              {"Insert"sv, {Attribute::Bucket, Attribute::Collection}});
        setup(Privilege::Delete,
              {"Delete"sv, {Attribute::Bucket, Attribute::Collection}});
        setup(Privilege::Upsert,
              {"Upsert"sv, {Attribute::Bucket, Attribute::Collection}});
        setup(Privilege::SimpleStats,
              {"SimpleStats"sv, {Attribute::Bucket, Attribute::Collection}});
        setup(Privilege::Stats, {"Stats"sv, {}});
        setup(Privilege::NodeSupervisor, {"NodeSupervisor"sv, {}});
        setup(Privilege::Administrator, {"Administrator"sv, {}});
        setup(Privilege::Audit, {"Audit"sv, {}});
        setup(Privilege::DcpConsumer, {"DcpConsumer"sv, {Attribute::Bucket}});
        setup(Privilege::DcpProducer, {"DcpProducer"sv, {Attribute::Bucket}});
        setup(Privilege::DcpStream,
              {"DcpStream"sv, {Attribute::Bucket, Attribute::Collection}});
        setup(Privilege::MetaWrite,
              {"MetaWrite"sv, {Attribute::Bucket, Attribute::Collection}});
        setup(Privilege::IdleConnection, {"IdleConnection"sv, {}});
        setup(Privilege::SystemXattrRead,
              {"SystemXattrRead"sv,
               {Attribute::Bucket, Attribute::Collection}});
        setup(Privilege::SystemXattrWrite,
              {"SystemXattrWrite"sv,
               {Attribute::Bucket, Attribute::Collection}});
        setup(Privilege::BucketThrottleManagement,
              {"BucketThrottleManagement"sv, {}});
        setup(Privilege::Unthrottled, {"Unthrottled"sv, {}});
        setup(Privilege::Unmetered, {"Unmetered"sv, {}});
        setup(Privilege::Impersonate, {"Impersonate"sv, {}});
        setup(Privilege::Settings, {"Settings"sv, {Attribute::Bucket}});
        setup(Privilege::SystemSettings, {"SystemSettings"sv, {}});
        setup(Privilege::SystemCollectionLookup,
              {"SystemCollectionLookup"sv,
               {Attribute::Bucket, Attribute::Collection}});
        setup(Privilege::SystemCollectionMutation,
              {"SystemCollectionMutation"sv,
               {Attribute::Bucket, Attribute::Collection}});
        setup(Privilege::RangeScan,
              {"RangeScan"sv, {Attribute::Bucket, Attribute::Collection}});
    }

private:
    void ensureValid(Privilege priv, const char* method) const {
        if (!isLegal(priv)) {
            throw std::invalid_argument(fmt::format(
                    "PrivilegeInformationService::{}(): Unknown privilege: {}",
                    method,
                    static_cast<int>(priv)));
        }
    }

    void setup(Privilege priv, PrivilegeMeta meta) {
        privileges[static_cast<std::size_t>(priv)] = std::move(meta);
    }

    std::array<PrivilegeMeta,
               static_cast<std::size_t>(Privilege::RangeScan) + 1>
            privileges;
};

/// The one and only instance
static const PrivilegeInformationService privilegeInformationServiceInstance;

std::string format_as(Privilege privilege) {
    return std::string(privilegeInformationServiceInstance.name(privilege));
}

void to_json(nlohmann::json& json, const Privilege& privilege) {
    json = format_as(privilege);
}

Privilege to_privilege(const std::string& str) {
    return privilegeInformationServiceInstance.lookup(str);
}

cb::engine_errc PrivilegeAccess::getEngineErrorCode(
        std::optional<ScopeID> sid, std::optional<CollectionID> cid) const {
    switch (status) {
    case Status::Ok:
        return cb::engine_errc::success;
    case Status::Fail:
        return cb::engine_errc::no_access;
    case Status::FailNoPrivileges:
        if (cid) {
            return cb::engine_errc::unknown_collection;
        }
        if (sid) {
            return cb::engine_errc::unknown_scope;
        }
        return cb::engine_errc::no_access;
    }
    Expects(false && "Unknown PrivilegeAccess status");
}

bool is_bucket_privilege(Privilege priv) {
    return privilegeInformationServiceInstance.isBucketPrivilege(priv);
}

bool is_collection_privilege(Privilege priv) {
    return privilegeInformationServiceInstance.isCollectionPrivilege(priv);
}

bool is_legal_privilege(Privilege privilege) {
    return privilegeInformationServiceInstance.isLegal(privilege);
}

std::string_view format_as(const PrivilegeAccess::Status& status) {
    using namespace std::string_view_literals;
    switch (status) {
    case PrivilegeAccess::Status::Ok:
        return "Ok"sv;
    case PrivilegeAccess::Status::Fail:
        return "Fail"sv;
    case PrivilegeAccess::Status::FailNoPrivileges:
        return "FailNoPrivileges"sv;
    }
    Expects(false && "Unknown PrivilegeAccess::Status");
}

std::ostream& operator<<(std::ostream& os,
                         const PrivilegeAccess::Status& status) {
    os << format_as(status);
    return os;
}

} // namespace cb::rbac
