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
#include "settings.h"
#include <cbsasl/server.h>
#include <executor/executorpool.h>
#include <executor/globaltask.h>
#include <folly/Synchronized.h>
#include <memcached/rbac.h>
#include <memcached/tenant.h>
#include <nlohmann/json.hpp>
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

protected:
    TenantManagerImpl() {
        LOG_INFO("Tenant resource control: {}",
                 Settings::instance().isEnforceTenantLimitsEnabled());
    }

    folly::Synchronized<
            std::unordered_map<std::string, std::shared_ptr<Tenant>>,
            std::mutex>
            tenants;
};

std::shared_ptr<Tenant> TenantManagerImpl::get(const cb::rbac::UserIdent& ident,
                                               bool create) {
    // Do this in two passes.. first try to just get it, if it fails
    // do the slow path where we need to look up the user in the userdb
    // first
    auto name = ident.to_json().dump();
    auto ret = tenants.withLock([&name, id = ident](auto& map) {
        auto iter = map.find(name);
        if (iter == map.end()) {
            return std::shared_ptr<Tenant>{};
        }
        return iter->second;
    });

    if (ret || !create) {
        // We found the user or it wasn't found but we shouldn't create it
        return ret;
    }

    // We need the user data
    auto userentry = cb::sasl::server::getUser(ident);
    if (!userentry) {
        // no such user and we can't create
        return {};
    }

    const auto& user = userentry.value();
    ret = tenants.withLock([&name, id = ident, create, &user](auto& map) {
        auto iter = map.find(name);
        if (iter == map.end()) {
            if (create) {
                auto ret = std::make_shared<Tenant>(std::move(id), user);
                map[name] = ret;
                return ret;
            }
            return std::shared_ptr<Tenant>{};
        }
        return iter->second;
    });

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

void TenantManager::setLimits(const cb::rbac::UserIdent& ident,
                              const cb::sasl::pwdb::user::Limits& limits) {
    auto tenant = TenantManagerImpl::instance().get(ident, false);
    if (tenant) {
        tenant->setLimits(limits);
    }
}

std::shared_ptr<Tenant> TenantManager::get(const cb::rbac::UserIdent& ident,
                                           bool create) {
    return TenantManagerImpl::instance().get(ident, create);
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
