/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <cbsasl/user.h>
#include <memcached/rbac.h>
#include <nlohmann/json_fwd.hpp>
#include <platform/non_negative_counter.h>
#include <platform/uuid.h>
#include <array>
#include <atomic>
#include <chrono>

class Tenant {
public:
    struct Constraints {
        /// bytes read from the network per minute
        std::atomic<size_t> ingress{0};
        /// bytes written to the network per minute
        std::atomic<size_t> egress{0};
        /// Concurrent connections
        std::atomic<size_t> connections{0};
        /// number of operations per minute
        std::atomic<size_t> operations{0};
    };

    /// Initialize a new Tenant to use the provided constraints
    Tenant(cb::rbac::UserIdent ident, const cb::sasl::pwdb::User& user);

    nlohmann::json to_json() const;

    const cb::rbac::UserIdent& getIdentity() {
        return identity;
    }

    void logon() {
        curr_conns++;
        total_conns++;
    }

    void logoff() {
        curr_conns--;
    }

    /// Update the network egress counter with nbytes
    void send(size_t nbytes);

    /// Update the network ingress counter with nbytes
    void recv(size_t nbytes);

    /// update the operation counter as a command was executed
    void executed();

    /// Add CPU time being consumed
    void addCpuTime(std::chrono::nanoseconds ns);

    enum class RateLimit { None, Ingress, Egress, Operations, Connections };
    /// Check that we're below the limits (we give up searching when the first
    /// limit checked is exceeded)
    RateLimit checkRateLimits();

    /// Check if the tenant may be deleted or not (0 connections and haven't
    /// been used for a while)
    bool mayDeleteTenant();

    /// Set the constraints to match the users limits
    void setLimits(const cb::sasl::pwdb::user::Limits& limits);

protected:
    const cb::rbac::UserIdent identity;
    const cb::uuid::uuid_t uuid;

    /// A sloppy gauge where we count the number of entries within a given
    /// period. If the environment variable MEMCACHED_UNIT_TESTS is set
    /// the period is 1 second, otherwise the period is set to 1 minute
    class SloppyGauge {
    public:
        std::atomic<uint64_t>& getValue();

    protected:
        std::atomic<uint64_t> point_in_time{0};
        std::atomic<uint64_t> value{0};
    };

    static constexpr size_t IngressIndex = 0;
    static constexpr size_t EgressIndex = 1;
    static constexpr size_t OpsIndex = 2;
    std::array<SloppyGauge, 3> rateLimits;

    /// Total number of bytes sent
    std::atomic<std::size_t> sent{0};
    /// Total number of bytes received
    std::atomic<std::size_t> received{0};
    /// Total number of operations
    std::atomic<std::size_t> operations{0};
    /// Current number of connections
    cb::AtomicNonNegativeCounter<std::size_t> curr_conns{0};
    /// The total number of connections used by this tenant
    std::atomic<std::size_t> total_conns{0};
    /// Total amount of CPU spent for this tenant (as of now this is only
    /// the front end threads)
    std::atomic<uint64_t> total_cpu_time_ns{0};

    /// The current set of constraints
    Constraints constraints;
    /// The number of times we've been rate limited for the various
    /// constraints
    Constraints rate_limited;
};

std::string to_string(Tenant::RateLimit limit);
