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
    json = {{"ingress", constraints.ingress.load()},
            {"egress", constraints.egress.load()},
            {"connections", constraints.connections.load()},
            {"operations", constraints.operations.load()}};
}

static inline void update(std::atomic<std::size_t>& atomic,
                          const nlohmann::json& json,
                          std::string_view key) {
    auto iter = json.find(key);
    if (iter != json.end() && iter->is_number_integer()) {
        atomic.store(iter->get<size_t>());
    }
}

void from_json(const nlohmann::json& json, Tenant::Constraints& constraints) {
    update(constraints.ingress, json, "ingress");
    update(constraints.egress, json, "egress");
    update(constraints.connections, json, "connections");
    update(constraints.operations, json, "operations");
}

Tenant::Tenant(cb::rbac::UserIdent ident, const nlohmann::json& c)
    : identity(std::move(ident)) {
    from_json(c, constraints);
}

nlohmann::json Tenant::to_json() const {
    return nlohmann::json{
            {"egress", sent.load()},
            {"ingress", received.load()},
            {"operations", operations.load()},
            {"connections",
             {{"current", curr_conns.load()}, {"total", total_conns.load()}}},
            {"constraints", constraints},
            {"rate_limited", rate_limited},
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
