/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include <logger/logger.h>
#include <memcached/tenant.h>
#include <nlohmann/json.hpp>
#include <platform/timeutils.h>
#include <chrono>

constexpr std::size_t mib = 1024 * 1024;

std::string to_string(Tenant::RateLimit limit) {
    switch (limit) {
    case Tenant::RateLimit::Connections:
        return "Connections";
    case Tenant::RateLimit::Operations:
        return "Operations";
    case Tenant::RateLimit::Ingress:
        return "Ingress";
    case Tenant::RateLimit::Egress:
        return "Egress";
    case Tenant::RateLimit::None:
        return "None";
    }
    return "to_string(Tenant::RateLimit limit): Invalid value: " +
           std::to_string(int(limit));
}

Tenant::Tenant(cb::rbac::UserIdent ident, const cb::sasl::pwdb::User& user)
    : identity(std::move(ident)), uuid(user.getUuid()) {
    setLimits(user.getLimits());
}

void Tenant::setLimits(const cb::sasl::pwdb::user::Limits& limits) {
    bool changed = false;
    auto set = [&changed](std::atomic<size_t>& var, uint64_t val) {
        if (!val) {
            val = std::numeric_limits<uint64_t>::max();
        }
        if (var.load() != val) {
            var.store(val);
            changed = true;
        }
    };
    set(constraints.ingress, limits.ingress_mib_per_min * mib);
    set(constraints.egress, limits.egress_mib_per_min * mib);
    set(constraints.connections, limits.num_connections);
    set(constraints.operations, limits.num_ops_per_min);
    if (changed) {
        LOG_INFO(
                "Tenant {} rate limited to egress_mib_per_min:{} "
                "ingress_mib_per_min:{} num_connections:{} num_ops_per_min:{}",
                identity.to_json().dump(),
                limits.ingress_mib_per_min,
                limits.egress_mib_per_min,
                limits.num_connections,
                limits.num_ops_per_min);
    }
}

nlohmann::json Tenant::to_json() const {
    return nlohmann::json{
            {"egress_bytes", sent.load()},
            {"ingress_bytes", received.load()},
            {"num_operations", operations.load()},
            {"connections",
             {{"current", curr_conns.load()}, {"total", total_conns.load()}}},
            {"rate_limited",
             {{"ingress_mib_per_min", rate_limited.ingress.load()},
              {"egress_mib_per_min", rate_limited.egress.load()},
              {"num_connections", rate_limited.connections.load()},
              {"num_ops_per_min", rate_limited.operations.load()}}},
            {"uuid", to_string(uuid)},
            {"cpu",
             cb::time2text(
                     std::chrono::nanoseconds{total_cpu_time_ns.load()})}};
}

void Tenant::send(size_t nbytes) {
    sent += nbytes;
    rateLimits[EgressIndex].getValue() += nbytes;
}

void Tenant::recv(size_t nbytes) {
    received += nbytes;
    rateLimits[IngressIndex].getValue() += nbytes;
}

void Tenant::executed() {
    rateLimits[OpsIndex].getValue()++;
    ++operations;
}

Tenant::RateLimit Tenant::checkRateLimits() {
    if (rateLimits[IngressIndex].getValue() > constraints.ingress) {
        ++rate_limited.ingress;
        return RateLimit::Ingress;
    }

    if (rateLimits[EgressIndex].getValue() > constraints.egress) {
        ++rate_limited.egress;
        return RateLimit::Egress;
    }

    if (rateLimits[OpsIndex].getValue() > constraints.operations) {
        ++rate_limited.operations;
        return RateLimit::Operations;
    }

    if (curr_conns > constraints.connections) {
        ++rate_limited.connections;
        return RateLimit::Connections;
    }

    return RateLimit::None;
}

bool Tenant::mayDeleteTenant() {
    return false;
}

void Tenant::addCpuTime(std::chrono::nanoseconds ns) {
    total_cpu_time_ns += ns.count();
}

std::atomic<uint64_t>& Tenant::SloppyGauge::getValue() {
    static bool unit_test = getenv("MEMCACHED_UNIT_TESTS") != nullptr;
    const auto now = uint64_t(
            unit_test ? std::chrono::duration_cast<std::chrono::seconds>(
                                std::chrono::steady_clock::now()
                                        .time_since_epoch())
                                .count()
                      : std::chrono::duration_cast<std::chrono::minutes>(
                                std::chrono::steady_clock::now()
                                        .time_since_epoch())
                                .count());
    if (point_in_time != now) {
        value = 0;
        point_in_time = now;
    }

    return value;
}
