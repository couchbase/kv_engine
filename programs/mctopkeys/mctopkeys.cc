/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <programs/mc_program_getopt.h>
#include <protocol/connection/client_mcbp_commands.h>

#include <algorithm>
#include <iostream>

static void usage(const McProgramGetopt& getopt, const int exitcode) {
    std::cerr << R"(Usage mctopkeys [options]

Options:

)" << getopt << R"(

Example:
    mctopkeys --user Administrator --password secret --duration 10 --limit 1000 --collect-limit 10000 --bucket_filter=mybucket1,mybucket2 --shards=64

)" << std::endl;
    std::exit(exitcode);
}

static std::vector<std::unique_ptr<MemcachedConnection>> getConnections(
        McProgramGetopt& getopt) {
    auto connection = getopt.getConnection();
    connection->setAgentName("mctopkeys " MEMCACHED_VERSION);
    connection->setFeatures(
            {cb::mcbp::Feature::XERROR, cb::mcbp::Feature::JSON});

    const auto rsp = connection->execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::GetClusterConfig});
    if (!rsp.isSuccess()) {
        throw ConnectionError("Failed to fetch cluster map", rsp);
    }
    auto json = rsp.getDataJson();
    if (!json.contains("nodesExt") || !json["nodesExt"].is_array()) {
        throw std::logic_error(fmt::format(
                "Expected clustermap to contain \"nodesExt\" as an array: {}",
                json.dump()));
    }

    std::vector<std::unique_ptr<MemcachedConnection>> connections;
    connections.emplace_back(std::move(connection));

    for (auto& node : json["nodesExt"]) {
        if (node.contains("thisNode") && node["thisNode"].get<bool>()) {
            // skip the node we're already connected to
            continue;
        }
        if (node.contains("services") && !node["services"].is_object()) {
            throw std::logic_error(fmt::format(
                    "Expected \"services\" to be an object: {}", node.dump()));
        }
        const std::string hostname =
                json.value("hostname", connections.front()->getHostname());
        in_port_t port = 0;
        if (connections.front()->isSsl()) {
            port = node["services"].value("kvSSL", 11207);
        } else {
            port = node["services"].value("kv", 11210);
        }
        connections.emplace_back(getopt.createAuthenticatedConnection(
                hostname, port, AF_UNSPEC, connections.front()->isSsl()));
        connections.back()->setAgentName("mctopkeys " MEMCACHED_VERSION);
        connections.back()->setFeatures(
                {cb::mcbp::Feature::XERROR, cb::mcbp::Feature::JSON});
    }

    return connections;
}

static std::unordered_map<std::string,
                          std::unordered_map<std::string, std::string>>
getCollectionManifests(MemcachedConnection& conn) {
    std::unordered_map<std::string,
                       std::unordered_map<std::string, std::string>>
            collection_manifests;
    auto buckets = conn.listBuckets();
    for (auto bucket : buckets) {
        conn.selectBucket(bucket);
        const auto rsp = conn.execute(BinprotGenericCommand{
                cb::mcbp::ClientOpcode::CollectionsGetManifest});
        if (rsp.isSuccess()) {
            auto scopes = rsp.getDataJson()["scopes"];
            for (auto& scope : scopes) {
                for (auto& collection : scope["collections"]) {
                    auto cid = collection["uid"].get<std::string>();
                    collection_manifests[bucket][fmt::format("cid:0x{}", cid)] =
                            collection["name"].get<std::string>();
                }
            }
        }
    }
    return collection_manifests;
}

int main(const int argc, char** argv) {
    McProgramGetopt getopt;
    using cb::getopt::Argument;
    auto duration = std::chrono::seconds(1);
    std::size_t limit = 100;
    std::size_t collect_limit = 50000;
    std::optional<std::size_t> shards;
    std::string bucket_filter;
    bool cluster = false;
    bool decode_collections = false;
    std::unordered_map<std::string,
                       std::unordered_map<std::string, std::string>>
            collection_manifests;

    getopt.addOption(
            {[&duration](auto value) {
                 duration =
                         std::chrono::seconds(std::stoul(std::string{value}));
             },
             "duration",
             Argument::Required,
             "number",
             "The number of seconds to run the sampling for (default 1)"});

    getopt.addOption(
            {[&limit](auto value) { limit = std::stoul(std::string{value}); },
             "limit",
             Argument::Required,
             "number",
             "The number of keys to display (default 10)"});

    getopt.addOption({[&collect_limit](auto value) {
                          collect_limit = std::stoul(std::string{value});
                      },
                      "collect-limit",
                      Argument::Required,
                      "number",
                      "The number of unique keys to collect on each thread on "
                      "the server (default 50000)"});

    getopt.addOption(
            {[&shards](auto value) { shards = std::stoul(std::string{value}); },
             "shards",
             Argument::Required,
             "number",
             "The number shards on each server to use for tracing of keys "
             "(default: 4x server threads)"});

    getopt.addOption({[&bucket_filter](auto value) {
                          if (!bucket_filter.empty()) {
                              bucket_filter.push_back(',');
                          }
                          bucket_filter.append(value);
                      },
                      "bucket_filter",
                      Argument::Required,
                      "bucketname[,bucketname...]",
                      "Limit tracing to the named buckets"});

    getopt.addOption({[&cluster](auto) { cluster = true; },
                      "cluster",
                      "Request tracing on all nodes in the cluster"});

    getopt.addOption(
            {[&decode_collections](auto) { decode_collections = true; },
             "decode-collections",
             "Try to decode the collections to their names"});

    getopt.addOption({[&getopt](auto) { usage(getopt, EXIT_SUCCESS); },
                      "help",
                      "This help text"});

    auto extraArgs = getopt.parse(
            argc, argv, [&getopt] { usage(getopt, EXIT_FAILURE); });

    try {
        getopt.assemble();

        std::vector<std::unique_ptr<MemcachedConnection>> connections;
        if (cluster) {
            connections = getConnections(getopt);
        } else {
            connections.emplace_back(getopt.getConnection());
            connections.back()->setAgentName("mctopkeys " MEMCACHED_VERSION);
            connections.back()->setFeatures(
                    {cb::mcbp::Feature::XERROR, cb::mcbp::Feature::JSON});
        }

        const auto start_command = fmt::format(
                "topkeys.start?limit={}&expected_duration={}{}{}",
                collect_limit,
                duration.count(),
                bucket_filter.empty()
                        ? ""
                        : fmt::format("&bucket_filter={}", bucket_filter),
                shards.has_value() ? fmt::format("&shards={}", *shards) : "");
        for (const auto& connection : connections) {
            const auto rsp = connection->execute(BinprotGenericCommand{
                    cb::mcbp::ClientOpcode::IoctlSet, start_command});
            if (!rsp.isSuccess()) {
                std::cerr << "Failed to start topkeys collection on "
                          << connection->getHostname() << std::endl;
            }
        }

        std::this_thread::sleep_for(duration);

        if (decode_collections) {
            collection_manifests =
                    getCollectionManifests(*getopt.getConnection());
        }

        nlohmann::json result;

        for (const auto& connection : connections) {
            auto rsp = connection->execute(BinprotGenericCommand{
                    cb::mcbp::ClientOpcode::IoctlGet,
                    fmt::format("topkeys.stop?limit={}", limit)});
            if (!rsp.isSuccess()) {
                std::cerr << "Failed to stop topkeys collection on "
                          << connection->getHostname() << ": "
                          << rsp.getStatus() << std::endl;
                continue;
            }
            auto json = rsp.getDataJson();
            if (decode_collections) {
                auto copy = json;
                auto& keys = json["keys"];
                for (auto& [key, value] : keys.items()) {
                    if (collection_manifests.contains(key)) {
                        for (auto& [collection, data] : value.items()) {
                            if (collection_manifests[key].contains(
                                        collection)) {
                                copy["keys"][key].erase(collection);
                                copy["keys"][key]
                                    [collection_manifests[key][collection]] =
                                            data;
                            }
                        }
                    }
                }
                json = std::move(copy);
            }

            result[connection->getHostname()] = std::move(json);
        }
        std::cout << result.dump(2) << std::endl;
    } catch (const ConnectionError& ex) {
        fmt::print(stderr, "{}\n", ex.what());
        return EXIT_FAILURE;
    } catch (const std::runtime_error& ex) {
        fmt::print(stderr, "{}\n", ex.what());
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
