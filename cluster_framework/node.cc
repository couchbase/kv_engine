/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "node.h"
#include <folly/portability/Unistd.h>
#include <nlohmann/json.hpp>
#include <platform/dirutils.h>
#include <platform/process_monitor.h>
#include <platform/strerror.h>
#include <protocol/connection/client_connection_map.h>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>

using namespace std::chrono_literals;

namespace cb::test {

Node::~Node() = default;
Node::Node(std::filesystem::path directory) : directory(std::move(directory)) {
}

class NodeImpl : public Node {
public:
    NodeImpl(std::filesystem::path directory, std::string id);
    ~NodeImpl() override;
    void startMemcachedServer();

    [[nodiscard]] std::unique_ptr<MemcachedConnection> getConnection()
            const override;
    [[nodiscard]] const ConnectionMap& getConnectionMap() const override {
        return connectionMap;
    }
    [[nodiscard]] std::string_view getId() const override {
        return id;
    }
    [[nodiscard]] nlohmann::json& getConfig() override {
        return config;
    }

    void writeConfig() const override {
        std::ofstream out(configfile.generic_string());
        out << config.dump(2);
        out.close();
    }

protected:
    void parsePortnumberFile();

    const std::filesystem::path configfile;
    nlohmann::json config;
    ConnectionMap connectionMap;
    const std::string id;
    std::unique_ptr<ProcessMonitor> child;
    std::atomic_bool allow_child_death{false};
};

NodeImpl::NodeImpl(std::filesystem::path directory, std::string id)
    : Node(std::move(directory)),
      configfile(Node::directory / "memcached.json"),
      id(std::move(id)) {
    const std::filesystem::path source_root(SOURCE_ROOT);
    const auto errmaps =
            source_root / "etc" / "couchbase" / "kv" / "error_maps";
    std::filesystem::path rbac;

    const auto* rbac_file = getenv("MEMCACHED_RBAC");
    if (rbac_file) {
        rbac = rbac_file;
    } else {
        rbac = source_root / "tests" / "testapp_cluster" / "rbac.json";
    }
    const auto log_filename = NodeImpl::directory / "log" / "memcached_log";
    const auto portnumber_file = NodeImpl::directory / "memcached.ports.json";
    const auto minidump_dir = NodeImpl::directory / "crash";
    create_directories(minidump_dir);
    create_directories(log_filename.parent_path());

    config = {{"max_connections", 1000},
              {"system_connections", 250},
              {"stdin_listener", false},
              {"datatype_json", true},
              {"datatype_snappy", true},
              {"xattr_enabled", true},
              {"dedupe_nmvb_maps", false},
              {"active_external_users_push_interval", "30 m"},
              {"error_maps_dir", errmaps.generic_string()},
              {"external_auth_service", true},
              {"external_auth_service_scram_support", true},
              {"root", NodeImpl::directory.generic_string()},
              {"rbac_file", rbac.generic_string()},
              {"sasl_mechanisms", "PLAIN OAUTHBEARER"},
              {"opcode_attributes_override",
               {{"version", 1},
                {"default", {{"slow", 500}}},
                {"COMPACT_DB", {{"slow", "5 m"}}}}},
              {"logger",
               {{"unit_test", false},
                {"console", true},
                {"filename", log_filename.generic_string()}}},
              {"breakpad",
               {{"enabled", true},
                {"minidump_dir", minidump_dir.generic_string()},
                {"content", "default"}}},
              {"portnumber_file", portnumber_file.generic_string()},
              {"parent_identifier", getpid()}};
    std::ofstream out(configfile.generic_string());
    out << config.dump(2);
    out.close();
}

void NodeImpl::startMemcachedServer() {
    std::filesystem::path exe{std::filesystem::current_path() / "memcached"};
    exe = exe.generic_string();
    if (!std::filesystem::exists(exe)) {
        exe = exe.generic_string() + ".exe";
        if (!std::filesystem::exists(exe)) {
            throw std::runtime_error(
                    "NodeImpl::startMemcachedServer(): Failed to locate "
                    "memcached");
        }
    }
    std::vector<std::string> argv = {
            {exe.generic_string(), "-C", configfile.generic_string()}};
    child = ProcessMonitor::create(argv, [this](const auto& ec) {
        if (!allow_child_death) {
            std::cerr << "memcached process on " << directory.generic_string()
                      << " terminated: " << ec.to_string() << std::endl;

            // We've set the cycle size to be 200M so we should expect
            // only a single log file (but for simplicity just iterate
            // over them all and print the last 8k of each file
            std::cerr << "Last 8k of the log files" << std::endl
                      << "========================" << std::endl;
            for (const auto& p :
                 std::filesystem::directory_iterator(directory / "log")) {
                if (is_regular_file(p)) {
                    auto content = cb::io::loadFile(p.path());
                    if (content.size() > 8192) {
                        content = content.substr(
                                content.find('\n', content.size() - 8192));
                    }
                    std::cerr << p.path().generic_string() << std::endl
                              << content << std::endl
                              << "-----------------------------" << std::endl;
                }
            }

            std::cerr << "Terminating process" << std::endl;
            std::_Exit(EXIT_FAILURE);
        }
    });

    // wait and read the portnumber file
    parsePortnumberFile();
}

NodeImpl::~NodeImpl() {
    if (child) {
        allow_child_death = true;
        child->terminate();
    }

    // make sure we reap the thread
    child.reset();
    if (!configfile.empty()) {
        try {
            remove(configfile);
        } catch (const std::exception& e) {
            std::cerr << "WARNING: Failed to remove \"" << configfile
                      << "\": " << e.what() << std::endl;
        }
    }
}

void NodeImpl::parsePortnumberFile() {
    connectionMap.initialize(nlohmann::json::parse(cb::io::loadFile(
            config["portnumber_file"], std::chrono::minutes{5})));
    std::filesystem::remove(config["portnumber_file"].get<std::string>());
}

std::unique_ptr<MemcachedConnection> NodeImpl::getConnection() const {
    auto ret = connectionMap.getConnection().clone();
    ret->setAutoRetryTmpfail(true);
    ret->setAgentName("cluster_testapp");
    ret->setFeatures({cb::mcbp::Feature::MUTATION_SEQNO,
                      cb::mcbp::Feature::XATTR,
                      cb::mcbp::Feature::XERROR,
                      cb::mcbp::Feature::JSON});
    return ret;
}

std::unique_ptr<Node> Node::create(
        std::filesystem::path directory,
        const std::string& id,
        std::function<void(std::string_view, nlohmann::json&)> configCallback) {
    auto ret = std::make_unique<NodeImpl>(std::move(directory), id);
    if (configCallback) {
        configCallback(id, ret->getConfig());
        ret->writeConfig();
    }

    ret->startMemcachedServer();
    return ret;
}

} // namespace cb::test
