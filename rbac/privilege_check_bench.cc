/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <benchmark/benchmark.h>
#include <memcached/rbac.h>
#include <nlohmann/json.hpp>

#include <array>
#include <cinttypes>
#include <iostream>

using namespace cb::rbac;

uint64_t success = 0;
uint64_t failed = 0;
uint64_t failed_no_privs = 0;

static void check_privilege(PrivilegeContext& context,
                            benchmark::State& state) {
    static const ScopeID sid{500};
    static const CollectionID cid{500};
    while (state.KeepRunning()) {
        switch (context.check(Privilege::Read, sid, cid).getStatus()) {
        case PrivilegeAccess::Status::Ok:
            ++success;
            break;
        case PrivilegeAccess::Status::Fail:
            ++failed;
            break;
        case PrivilegeAccess::Status::FailNoPrivileges:
            ++failed_no_privs;
            break;
        }
    }
}

static void BucketLevel(benchmark::State& state) {
    UserIdent user("bucket", Domain::Local);
    auto context = createContext(user, "bucket");
    check_privilege(context, state);
}

static void ScopeLevel(benchmark::State& state) {
    UserIdent user("scope", Domain::Local);
    auto context = createContext(user, "bucket");
    check_privilege(context, state);
}

static void CollectionLevel(benchmark::State& state) {
    UserIdent user("collection", Domain::Local);
    auto context = createContext(user, "bucket");
    check_privilege(context, state);
}

BENCHMARK(BucketLevel);
BENCHMARK(ScopeLevel);
BENCHMARK(CollectionLevel);

std::string to_hex(int val) {
    std::array<char, 32> buf{};
    snprintf(buf.data(), buf.size(), "0x%" PRIx32, val);
    return buf.data();
}

int main(int argc, char** argv) {
    ::benchmark::Initialize(&argc, argv);
    if (::benchmark::ReportUnrecognizedArguments(argc, argv)) {
        return EXIT_FAILURE;
    }

    auto skeleton = R"(
{
  "bucket":{"buckets":{"bucket":["Read"]},"domain":"local"},
  "collection":{"buckets":{"bucket":{}},"domain":"local"},
  "scope":{"buckets":{"bucket":{}},"domain":"local"}
})"_json;

    // Build a privilege database
    skeleton["scope"]["buckets"]["bucket"]["scopes"] = nlohmann::json::object();
    auto& scopes = skeleton["scope"]["buckets"]["bucket"]["scopes"];
    for (int ii = 0; ii < 1000; ++ii) {
        scopes[to_hex(ii)] = R"({"privileges":["Read"]})"_json;
    }

    skeleton["collection"]["buckets"]["bucket"]["scopes"] = scopes;
    auto& collections = skeleton["collection"]["buckets"]["bucket"]["scopes"];
    nlohmann::json json = nlohmann::json::object();
    for (int jj = 0; jj < 1000; ++jj) {
        json[to_hex(jj)] = R"({"privileges":["Read"]})"_json;
    }
    collections[to_hex(500)] = nlohmann::json::object();
    collections[to_hex(500)]["collections"] = json;

    initialize();
    createPrivilegeDatabase(skeleton.dump());

    ::benchmark::RunSpecifiedBenchmarks();

    std::cout << "Success: " << success << std::endl
              << "Failed : " << failed << std::endl
              << "No Priv: " << failed_no_privs << std::endl;

    ::benchmark::Shutdown();
}
