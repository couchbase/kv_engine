/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "tenant_manager.h"

#include "log_macros.h"
#include "nobucket_taskable.h"
#include "one_shot_task.h"
#include "settings.h"
#include <boost/filesystem.hpp>
#include <executor/executorpool.h>
#include <executor/globaltask.h>
#include <folly/Synchronized.h>
#include <memcached/rbac.h>
#include <memcached/tenant.h>
#include <nlohmann/json.hpp>
#include <platform/dirutils.h>
#include <unordered_map>

class TenantManagerImpl {
public:
    static TenantManagerImpl& instance() {
        static TenantManagerImpl _instance;
        return _instance;
    }

    std::shared_ptr<Tenant> get(const cb::rbac::UserIdent& ident, bool create);
    nlohmann::json to_json();

    void purgeIdleTenants();

    bool isEnabled() {
        return enabled.load();
    }

protected:
    TenantManagerImpl()
        : userDirectory(
                  boost::filesystem::path{Settings::instance().getRoot()} /
                  "etc" / "couchbase" / "kv" / "security" / "user.d") {
        if (getenv("COUCHBASE_DBAAS")) {
            const auto path = userDirectory / "default.json";
            if (exists(path)) {
                try {
                    initial = nlohmann::json::parse(
                            cb::io::loadFile(path.generic_string()));
                    LOG_INFO("Enable tenant tracking using the constraints: {}",
                             initial.dump());
                    enabled = true;
                } catch (const std::exception& exception) {
                    LOG_CRITICAL("Failed to load {}: {}",
                                 path.generic_string(),
                                 exception.what());
                }
            } else {
                FATAL_ERROR(EXIT_FAILURE,
                            "COUCHBASE_DBAAS set, but {} does not exist.",
                            userDirectory.generic_string());
            }
        }
    }

    const boost::filesystem::path userDirectory;
    std::atomic_bool enabled{false};
    nlohmann::json initial;
    folly::Synchronized<
            std::unordered_map<std::string, std::shared_ptr<Tenant>>,
            std::mutex>
            tenants;
};

std::shared_ptr<Tenant> TenantManagerImpl::get(const cb::rbac::UserIdent& ident,
                                               bool create) {
    auto name = ident.to_json().dump();
    bool created = false;
    auto ret = tenants.withLock([&name, id = ident, create, &created, this](
                                        auto& map) {
        auto iter = map.find(name);
        if (iter == map.end()) {
            if (create) {
                auto ret = std::make_shared<Tenant>(std::move(id), initial);
                map[name] = ret;
                created = true;
                return ret;
            }
            return std::shared_ptr<Tenant>{};
        }
        return iter->second;
    });

    if (created) {
        // start a task to read the restrictions
        ExecutorPool::get()->schedule(std::make_shared<OneShotTask>(
                TaskId::Core_TenantConfig,
                "Tenant: " + name,
                [tenant = ret,
                 nm = std::move(name),
                 id = ident,
                 root = userDirectory]() {
                    auto fname = root / (id.name + ".json");
                    if (exists(fname)) {
                        try {
                            const auto json = nlohmann::json::parse(
                                    cb::io::loadFile(fname.generic_string()));
                            tenant->resetConstraints(json);
                            LOG_INFO("Update tenant {} to use {}",
                                     id.to_json(),
                                     json.dump());
                        } catch (const std::exception& exception) {
                            LOG_ERROR("Failed to read {}: {}",
                                      fname.generic_string(),
                                      exception.what());
                        }
                    }
                }));
    }

    return ret;
}

nlohmann::json TenantManagerImpl::to_json() {
    return tenants.withLock([](auto& map) {
        nlohmann::json ret = nlohmann::json::array();
        for (const auto& [nm, ten] : map) {
            auto ident = nlohmann::json::parse(nm);
            auto entry = ten->to_json();
            entry["id"] = ident;
            ret.emplace_back(entry);
        }
        return ret;
    });
}

void TenantManagerImpl::purgeIdleTenants() {
    tenants.withLock([](auto& map) {
        // Don't bother purge idle users if the map is small
        constexpr size_t minimumUserPurgeSize = 100;
        if (map.size() < minimumUserPurgeSize) {
            return;
        }

        auto iter = map.begin();
        while (iter != map.end()) {
            if (iter->second->mayDeleteTenant()) {
                iter = map.erase(iter);
            } else {
                iter++;
            }
        }
    });
}

std::shared_ptr<Tenant> TenantManager::get(const cb::rbac::UserIdent& ident,
                                           bool create) {
    return TenantManagerImpl::instance().get(ident, create);
}
bool TenantManager::isTenantTrackingEnabled() {
    return TenantManagerImpl::instance().isEnabled();
}

nlohmann::json TenantManager::to_json() {
    return TenantManagerImpl::instance().to_json();
}

class TenantPurger : public GlobalTask {
public:
    TenantPurger()
        : GlobalTask(NoBucketTaskable::instance(), TaskId::Core_TenantPurger) {
    }

    std::string getDescription() const override {
        return "Purge unused tenants";
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        return std::chrono::milliseconds(25);
    }

protected:
    bool run() override {
        TenantManagerImpl::instance().purgeIdleTenants();
        // Purge interval is 30 minutes
        snooze(30.0 * 60);
        return true;
    }
};

static ExTask tenantPurgerTask;

void TenantManager::startup() {
    tenantPurgerTask = std::make_shared<TenantPurger>();
    ExecutorPool::get()->schedule(tenantPurgerTask);
}

void TenantManager::shutdown() {
    if (tenantPurgerTask) {
        ExecutorPool::get()->cancel(tenantPurgerTask->getId(), true);
        tenantPurgerTask.reset();
    }
}
