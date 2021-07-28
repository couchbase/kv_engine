/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
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

void to_json(nlohmann::json& json, const Tenant::Constraints& constraints) {
    json = {{"ingress_mib_per_min", constraints.ingress.load() / mib},
            {"egress_mib_per_min", constraints.egress.load() / mib},
            {"num_connections", constraints.connections.load()},
            {"num_ops_per_min", constraints.operations.load()}};
}

static inline void update(std::atomic<std::size_t>& atomic,
                          const nlohmann::json& json,
                          std::string_view key,
                          std::size_t multiplier) {
    auto iter = json.find(key);
    if (iter != json.end() && iter->is_number_integer()) {
        atomic.store(iter->get<size_t>() * multiplier);
    }
}

void from_json(const nlohmann::json& json, Tenant::Constraints& constraints) {
    update(constraints.ingress, json, "ingress_mib_per_min", mib);
    update(constraints.egress, json, "egress_mib_per_min", mib);
    update(constraints.connections, json, "num_connections", 1);
    update(constraints.operations, json, "num_ops_per_min", 1);
}

Tenant::Tenant(cb::rbac::UserIdent ident, const nlohmann::json& c)
    : identity(std::move(ident)) {
    from_json(c, constraints);
    uuid.withLock([](auto& u) { std::fill(u.begin(), u.end(), 0); });
}

nlohmann::json Tenant::to_json() const {
    auto ret = nlohmann::json{
            {"egress_bytes", sent.load()},
            {"ingress_bytes", received.load()},
            {"num_operations", operations.load()},
            {"connections",
             {{"current", curr_conns.load()}, {"total", total_conns.load()}}},
            {"rate_limited",
             // Ideally we could have just used the to_json method automatically
             // but egress and ingress should be reported as mib per minute.
             // internally we need to operate on bytes when we check on the
             // limit and we don't want to do conversion for every check so
             // the method converting to and from JSON automatically handle
             // the conversion.
             {{"ingress_mib_per_min", rate_limited.ingress.load()},
              {"egress_mib_per_min", rate_limited.egress.load()},
              {"num_connections", rate_limited.connections.load()},
              {"num_ops_per_min", rate_limited.operations.load()}}},
            {"cpu",
             cb::time2text(
                     std::chrono::nanoseconds{total_cpu_time_ns.load()})}};
    // maybe add uuid
    uuid.withLock([&ret](auto& u) {
        static cb::uuid::uuid_t none{{0}};
        if (u == none) {
            return;
        }
        ret["uuid"] = to_string(u);
    });

    return ret;
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

void Tenant::resetConstraints(const nlohmann::json& spec) {
    from_json(spec, constraints);
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
