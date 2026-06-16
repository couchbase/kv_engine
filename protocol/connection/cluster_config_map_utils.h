/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "client_connection.h"
#include "client_mcbp_commands.h"

/**
 * Utility class to map key/vbuckets to nodes
 *
 * @tparam T the users node type
 */
template <typename T>
class NodeLocator {
public:
    NodeLocator(const NodeLocator&) = delete;
    NodeLocator() = delete;

    /**
     * Create a new instance of the node locator and initialize it from
     * the cluster topology map fetched from the server (current selected
     * bucket for the connection object)
     *
     * @param connection The connection used to fetch the the cluster topology
     *                   map from (must be an authenticated connection)
     * @param create_node_callback The callback to create a new node based
     *                             on the information found in the clustermap.
     *                             Called with hostname, port, tls
     * @param hash_function The hash function to use to map a key to a vbucket
     * @return
     */
    static std::unique_ptr<NodeLocator<T>> create(
            MemcachedConnection& connection,
            const std::function<std::unique_ptr<T>(
                    std::string_view, uint16_t, bool)>& create_node_callback,
            std::function<uint32_t(std::string_view)> hash_function) {
        // get the CCCP
        auto rsp = connection.execute(BinprotGenericCommand{
                cb::mcbp::ClientOpcode::GetClusterConfig});
        if (!rsp.isSuccess()) {
            throw ConnectionError("Failed to fetch cluster map", rsp);
        }
        auto json = rsp.getDataJson();

        uint16_t port;
        try {
            auto vbservermap = json["vBucketServerMap"];

            // Validate and extract nodesExt
            if (!json.contains("nodesExt")) {
                throw std::runtime_error(
                        "'nodesExt' not found in cluster config");
            }
            const auto& nodesExt = json["nodesExt"];
            if (!nodesExt.is_array() || nodesExt.empty()) {
                throw std::runtime_error("'nodesExt' is not a non-empty array");
            }

            // Validate and extract services
            if (!nodesExt[0].contains("services")) {
                throw std::runtime_error("'services' not found in nodesExt[0]");
            }
            const auto& services = nodesExt[0]["services"];

            // Validate and extract port
            const char* port_key = connection.isSsl() ? "kvSSL" : "kv";
            if (!services.contains(port_key)) {
                throw std::runtime_error(fmt::format(
                        "'{}' port not found in services", port_key));
            }
            port = services[port_key].get<uint16_t>();
        } catch (const std::exception& e) {
            throw std::runtime_error(fmt::format(
                    "Failed to parse cluster config: {}", e.what()));
        }

        std::vector<std::unique_ptr<T>> node_list;
        auto vbservermap = json["vBucketServerMap"];
        auto nodes = vbservermap["serverList"];
        for (const auto& n : nodes) {
            auto h = n.get<std::string>();
            auto idx = h.find(':');
            h.resize(idx);
            if (h.find("$HOST") != std::string::npos) {
                h = connection.getHostname();
            }
            node_list.emplace_back(
                    create_node_callback(h, port, connection.isSsl()));
        }

        std::vector<size_t> active_vbmap;
        auto map = vbservermap["vBucketMap"];
        for (const auto& e : map) {
            active_vbmap.push_back(e[0].get<int>());
        }
        Expects(!active_vbmap.empty());
        Expects(!node_list.empty());
        // can't use std::make_unique without making constructor public
        return std::unique_ptr<NodeLocator<T>>{
                new NodeLocator<T>(std::move(active_vbmap),
                                   std::move(node_list),
                                   std::move(hash_function))};
    }

    /**
     * Lookup which vbucket and node a given key belongs to
     *
     * @param key the key to look up
     * @return A pair with the node and vbucket the key belongs to
     */
    [[nodiscard]] std::pair<T&, Vbid> lookup(const std::string_view key) const {
        auto vb = hash_function(key) % active_vbmap.size();
        return {*node_list[active_vbmap[vb]],
                Vbid(gsl::narrow<Vbid::id_type>(vb))};
    }

    /**
     * Lookup the node responsible for the given vbucket
     */
    [[nodiscard]] T& lookup(const Vbid vb) const {
        Expects(vb.get() < active_vbmap.size());
        return *node_list[active_vbmap[vb.get()]];
    }

    /// Iterate over the nodes and call the provided callback
    void iterate(const std::function<void(T&)>& callback) const {
        for (const auto& n : node_list) {
            callback(*n);
        }
    }

protected:
    NodeLocator(std::vector<size_t> active_vbmap,
                std::vector<std::unique_ptr<T>> node_list,
                std::function<uint32_t(std::string_view)> hash_function)
        : active_vbmap(std::move(active_vbmap)),
          node_list(std::move(node_list)),
          hash_function(std::move(hash_function)) {
    }

    /// The index in the array is the vbucket number, and the value at that
    /// entry is the index into the connection vector.
    const std::vector<size_t> active_vbmap;
    /// A connection to all of the servers
    const std::vector<std::unique_ptr<T>> node_list;
    const std::function<uint32_t(std::string_view)> hash_function;
};
