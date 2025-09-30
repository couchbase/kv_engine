/*
 *     Copyright 2022 Couchbase, Inc
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

#include "audit_event_filter.h"

#include "audit.h"
#include "audit_descriptor_manager.h"
#include <fmt/format.h>
#include <unordered_map>
#include <vector>

using namespace std::string_view_literals;

using AuditId = uint32_t;

class BucketFilter {
    using UserFilterMap =
            std::unordered_map<cb::rbac::UserIdent, std::vector<AuditId>>;
    /// Instead of populating the array with all legal entries we insert
    /// a single entry containing the special value -1 to indicate all
    /// events
    static constexpr uint32_t allEvents = uint32_t(-1);

public:
    BucketFilter() = default;

    explicit BucketFilter(const nlohmann::json& json)
        : enabled(getEnabled(json)),
          exclude_user_filter(getFilter(json, "filter_out")),
          include_user_filter(getFilter(json, "filter_in")) {
    }

    bool isMatch(AuditId id,
                 const cb::rbac::UserIdent& user,
                 std::optional<ScopeID>,
                 std::optional<CollectionID>) const {
        if (!include_user_filter.empty()) {
            auto iter = include_user_filter.find(user);
            if (iter == include_user_filter.end()) {
                return true;
            }
        }

        auto iter = exclude_user_filter.find(user);
        if (iter == exclude_user_filter.end()) {
            return false;
        }

        const auto& vec = iter->second;
        if (vec.size() == 1 && vec.front() == allEvents) {
            return true;
        }

        return std::ranges::binary_search(vec, id);
    }

    nlohmann::json to_json() const {
        static const char* wildcard = "*";
        nlohmann::json ret;
        if (enabled.size() == 1 && enabled.front() == allEvents) {
            ret["enabled"] = wildcard;
        } else {
            ret["enabled"] = enabled;
        }
        if (!exclude_user_filter.empty()) {
            auto& entry = ret["filter_out"];
            for (const auto& [name, id] : exclude_user_filter) {
                std::string nm = name.name;
                if (name.domain == cb::rbac::Domain::Local) {
                    nm.append("/couchbase");
                } else {
                    nm.append("/external");
                }
                if (id.size() == 1 && id.front() == allEvents) {
                    entry[nm] = wildcard;
                } else {
                    entry[nm] = id;
                }
            }
        }
        return ret;
    }

    bool isEnabled(AuditId id) const {
        return std::ranges::binary_search(enabled, id);
    }

protected:
    static cb::rbac::UserIdent userIdentFromString(std::string_view str) {
        auto pos = str.find("/");
        if (pos == std::string_view::npos) {
            return {std::string{str}, cb::rbac::Domain::Local};
        }
        std::string name{str.data(), pos};
        str.remove_prefix(pos + 1);
        if (str == "couchbase"sv) {
            return {std::move(name), cb::rbac::Domain::Local};
        }

        if (str == "external"sv) {
            return {std::move(name), cb::rbac::Domain::External};
        }

        if (str == "unknown"sv) {
            return {std::move(name), cb::rbac::Domain::Unknown};
        }

        throw std::runtime_error(fmt::format(
                "userIdentFromString: invalid domain privided: {}", str));
    }

    /**
     * Parse the provided JSON and return a sorted vector of identifiers.
     * The provided json should either be a string containing '*' which
     * indicated "all", or an array with the provided numbers.
     *
     * To avoid creating a full array of all of the identifiers when "all"
     * is selected, we insert the special value "-1".
     */
    static std::vector<AuditId> getIdentifierVector(
            const nlohmann::json& json) {
        std::vector<AuditId> ret;
        const char* error_message =
                R"(getIdentifierVector(): enabled should be an array of id, or a string containing "*" for all)";

        // check the type
        if (json.is_string()) {
            // The value should be "*"
            if (json.get<std::string>() != "*") {
                throw std::runtime_error(error_message);
            }
            return {allEvents};
        }

        if (!json.is_array()) {
            throw std::runtime_error(error_message);
        }

        for (const auto& e : json) {
            ret.push_back(e.get<uint32_t>());
        }

        std::ranges::sort(ret);
        return ret;
    }

    static std::vector<AuditId> getEnabled(const nlohmann::json& json) {
        std::vector<AuditId> ret;
        auto iter = json.find("enabled");
        if (iter == json.end()) {
            return {};
        }

        return getIdentifierVector(*iter);
    }

    UserFilterMap getFilter(const nlohmann::json& json,
                            const std::string_view key) {
        auto iter = json.find(key);
        if (iter == json.end()) {
            return {};
        }

        UserFilterMap ret;
        for (auto it = iter->begin(); it != iter->end(); ++it) {
            ret[userIdentFromString(it.key())] =
                    getIdentifierVector(it.value());
        }

        return ret;
    }

    /// A sorted vector containing all of the enabled identifiers (to allow
    /// for binary search). If all events are enabled the vector contains
    /// a single entry with '-1'.
    std::vector<AuditId> enabled;
    UserFilterMap exclude_user_filter;
    UserFilterMap include_user_filter;
};

class AuditEventFilterImpl : public AuditEventFilter {
public:
    AuditEventFilterImpl(uint64_t g, const nlohmann::json& json)
        : generation(g),
          defaultBucketFilter(getDefaultBucketFilter(json)),
          bucketFilter(getBucketFilters(json)) {
    }

    std::unique_ptr<AuditEventFilter> clone() const override {
        return std::make_unique<AuditEventFilterImpl>(*this);
    }

    bool isValid() const override {
        return generation == AuditImpl::generation;
    }

    bool isFilteredOut(uint32_t id,
                       const cb::rbac::UserIdent& uid,
                       const cb::rbac::UserIdent* euid,
                       std::optional<std::string_view> bucket,
                       std::optional<ScopeID> scope,
                       std::optional<CollectionID> collection) const override {
        if (!isIdSubjectToFilter(id)) {
            return false;
        }

        if (euid) {
            return isFilteredOut(id, uid, bucket, scope, collection) &&
                   isFilteredOut(id, *euid, bucket, scope, collection);
        }
        return isFilteredOut(id, uid, bucket, scope, collection);
    }

    nlohmann::json to_json() const override {
        nlohmann::json ret;
        ret["default"] = defaultBucketFilter.to_json();
        if (!bucketFilter.empty()) {
            auto& buckets = ret["buckets"];
            for (const auto& [name, filter] : bucketFilter) {
                buckets[name] = filter.to_json();
            }
        }
        return ret;
    }

    bool isEnabled() const override {
        return true;
    }

    bool isEnabled(uint32_t id,
                   std::optional<std::string_view> bucket) const override {
        if (bucket) {
            auto iter = bucketFilter.find(std::string{*bucket});
            if (iter != bucketFilter.end()) {
                return iter->second.isEnabled(id);
            }
        }

        return defaultBucketFilter.isEnabled(id);
    }

protected:
    bool isFilteredOut(uint32_t id,
                       const cb::rbac::UserIdent& uid,
                       std::optional<std::string_view> bucket,
                       std::optional<ScopeID> scope,
                       std::optional<CollectionID> collection) const {
        if (bucket) {
            auto iter = bucketFilter.find(std::string{*bucket});
            if (iter != bucketFilter.end()) {
                return !iter->second.isEnabled(id) ||
                       iter->second.isMatch(id, uid, scope, collection);
            }
        }

        return !defaultBucketFilter.isEnabled(id) ||
               defaultBucketFilter.isMatch(id, uid, scope, collection);
    }

    /// Is the provided ID subject to filtering by this filter
    static bool isIdSubjectToFilter(uint32_t id) {
        return AuditDescriptorManager::lookup(id).isFilteringPermitted();
    }

    BucketFilter getDefaultBucketFilter(const nlohmann::json& json) {
        auto iter = json.find("default");
        if (iter == json.end()) {
            return {};
        }
        return BucketFilter{*iter};
    }

    std::unordered_map<std::string, BucketFilter> getBucketFilters(
            const nlohmann::json& json) {
        auto iter = json.find("buckets");
        if (iter == json.end()) {
            return {};
        }

        std::unordered_map<std::string, BucketFilter> ret;
        for (auto it = iter->begin(); it != iter->end(); ++it) {
            ret[it.key()] = BucketFilter{it.value()};
        }

        return ret;
    }

    const uint64_t generation;
    const BucketFilter defaultBucketFilter;
    const std::unordered_map<std::string, BucketFilter> bucketFilter;
};

/// We don't need to try to parse things when filtering isn't enabled
class DisabledEventFilter : public AuditEventFilter {
public:
    explicit DisabledEventFilter(uint64_t g) : generation(g) {
    }

    std::unique_ptr<AuditEventFilter> clone() const override {
        return std::make_unique<DisabledEventFilter>(*this);
    }

    bool isValid() const override {
        return generation == AuditImpl::generation;
    }

    bool isEnabled() const override {
        return false;
    }

    bool isEnabled(uint32_t, std::optional<std::string_view>) const override {
        return false;
    }

    nlohmann::json to_json() const override {
        return {};
    }

    bool isFilteredOut(uint32_t id,
                       const cb::rbac::UserIdent& uid,
                       const cb::rbac::UserIdent* euid,
                       std::optional<std::string_view> bucket,
                       std::optional<ScopeID> scope,
                       std::optional<CollectionID> collection) const override {
        return true;
    }

protected:
    const uint64_t generation;
};

std::unique_ptr<AuditEventFilter> AuditEventFilter::create(
        uint64_t g, const nlohmann::json& json) {
    if (json.empty()) {
        return std::make_unique<DisabledEventFilter>(g);
    }

    return std::make_unique<AuditEventFilterImpl>(g, json);
}
