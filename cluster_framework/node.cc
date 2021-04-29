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
#include <boost/filesystem.hpp>
#include <folly/portability/Unistd.h>
#include <nlohmann/json.hpp>
#include <platform/dirutils.h>
#include <platform/strerror.h>
#include <protocol/connection/client_connection_map.h>
#ifndef WIN32
#include <sys/wait.h>
#include <csignal>
#endif
#include <chrono>
#include <fstream>
#include <iostream>
#include <thread>

namespace cb::test {

Node::~Node() = default;
Node::Node(boost::filesystem::path directory)
    : directory(std::move(directory)) {
}

class NodeImpl : public Node {
public:
    NodeImpl(boost::filesystem::path directory, std::string id);
    ~NodeImpl() override;
    void startMemcachedServer();

    std::unique_ptr<MemcachedConnection> getConnection() const override;
    const ConnectionMap& getConnectionMap() const override {
        return connectionMap;
    }

protected:
    void parsePortnumberFile();

    const boost::filesystem::path configfile;
    nlohmann::json config;
    ConnectionMap connectionMap;
    const std::string id;
};

NodeImpl::NodeImpl(boost::filesystem::path directory, std::string id)
    : Node(std::move(directory)),
      configfile(Node::directory / "memcached.json"),
      id(std::move(id)) {
    const boost::filesystem::path source_root(SOURCE_ROOT);
    const auto errmaps =
            source_root / "etc" / "couchbase" / "kv" / "error_maps";
    const auto rbac = source_root / "tests" / "testapp_cluster" / "rbac.json";
    const auto log_filename = NodeImpl::directory / "memcached_log";
    const auto portnumber_file = NodeImpl::directory / "memcached.ports.json";
    const auto minidump_dir = NodeImpl::directory / "crash";
    create_directories(minidump_dir);

    config = {{"max_connections", 1000},
              {"system_connections", 250},
              {"stdin_listener", false},
              {"datatype_json", true},
              {"datatype_snappy", true},
              {"xattr_enabled", true},
              {"dedupe_nmvb_maps", false},
              {"active_external_users_push_interval", "30 m"},
              {"always_collect_trace_info", true},
              {"error_maps_dir", errmaps.generic_string()},
              {"external_auth_service", true},
              {"rbac_file", rbac.generic_string()},
              {"ssl_cipher_list", "HIGH"},
              {"ssl_minimum_protocol", "tlsv1"},
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
              {"parent_identifier", (int)getpid()},
              {"prometheus", {{"port", 0}, {"family", "inet"}}}};
    config["interfaces"][0] = {{"tag", "plain"},
                               {"system", true},
                               {"port", 0},
                               {"ipv4", "required"},
                               {"host", "*"}};
    std::ofstream out(configfile.generic_string());
    out << config.dump(2);
    out.close();
}

void NodeImpl::startMemcachedServer() {
#ifdef WIN32
    STARTUPINFO sinfo{};
    PROCESS_INFORMATION pinfo{};
    sinfo.cb = sizeof(sinfo);

    char commandline[1024];
    sprintf(commandline,
            "memcached.exe -C %s",
            configfile.generic_string().c_str());
    if (!CreateProcess("memcached.exe", // lpApplicationName
                       commandline, // lpCommandLine
                       nullptr, // lpProcessAttributes
                       nullptr, // lpThreadAttributes
                       false, // bInheritHandles
                       0, // dwCreationFlags
                       nullptr, // lpEnvironment
                       nullptr, // lpCurrentDirectory
                       &sinfo, // lpStartupInfo
                       &pinfo)) { // lpProcessInfoqrmation
        throw std::system_error(GetLastError(),
                                std::system_category(),
                                "Failed to execute memcached");
    }

    child = pinfo.hProcess;
#else
    child = fork();
    if (child == -1) {
        throw std::system_error(
                errno, std::system_category(), "Failed to start client");
    }

    if (child == 0) {
        std::string binary(OBJECT_ROOT);
        const auto memcached_json = configfile.generic_string();
        binary.append("/memcached");

        std::vector<const char*> argv;
        argv.emplace_back(binary.c_str());
        argv.emplace_back("-C");
        argv.emplace_back(memcached_json.c_str());
        argv.emplace_back(nullptr);
        execvp(argv[0], const_cast<char**>(argv.data()));
        throw std::system_error(
                errno, std::system_category(), "Failed to execute memcached");
    }
#endif

    // wait and read the portnumber file
    parsePortnumberFile();
}

NodeImpl::~NodeImpl() {
    if (isRunning()) {
#ifdef WIN32
        // @todo This should be made a bit more robust
        if (!TerminateProcess(child, 0)) {
            std::cerr << "TerminateProcess failed!" << std::endl;
            std::cerr.flush();
            _exit(EXIT_FAILURE);
        }
        DWORD status;
        if ((status = WaitForSingleObject(child, 60000)) != WAIT_OBJECT_0) {
            std::cerr << "Unexpected return value from WaitForSingleObject: "
                      << status << std::endl;
            std::cerr.flush();
            _exit(EXIT_FAILURE);
        }
        if (!GetExitCodeProcess(child, &status)) {
            std::cerr << "GetExitCodeProcess failed: " << GetLastError()
                      << std::endl;
            std::cerr.flush();
            _exit(EXIT_FAILURE);
        }
#else
        // Start by giving it a slow and easy start...
        const auto timeout =
                std::chrono::steady_clock::now() + std::chrono::seconds(15);
        kill(child, SIGTERM);

        do {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        } while (isRunning() && std::chrono::steady_clock::now() < timeout);

        if (isRunning()) {
            // no mercy!
            kill(child, SIGKILL);

            int status;
            pid_t ret;
            while (true) {
                ret = waitpid(child, &status, 0);
                if (ret == reinterpret_cast<pid_t>(-1) && errno == EINTR) {
                    // Just loop again
                    continue;
                }
                break;
            }
        }
#endif
    }

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
    cb::io::rmrf(config["portnumber_file"]);
}

#ifdef WIN32
bool Node::isRunning() const {
    if (child != INVALID_HANDLE_VALUE) {
        DWORD status;

        if (!GetExitCodeProcess(child, &status)) {
            throw std::system_error(
                    GetLastError(),
                    std::system_category(),
                    "NodeImpl::isRunning: GetExitCodeProcess failed");

            std::cerr << "GetExitCodeProcess: failed: " << cb_strerror()
                      << std::endl;
            exit(EXIT_FAILURE);
        }

        if (status == STILL_ACTIVE) {
            return true;
        }

        CloseHandle(child);
        child = INVALID_HANDLE_VALUE;
    }
    return false;
}
#else
bool Node::isRunning() const {
    if (child != 0) {
        int status;
        auto next = waitpid(child, &status, WNOHANG);
        if (next == static_cast<pid_t>(-1)) {
            throw std::system_error(errno,
                                    std::system_category(),
                                    "NodeImpl::isRunning: waitpid failed");
        }

        if (next == child) {
            child = 0;
            return false;
        }

        return true;
    }

    return false;
}
#endif

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

std::unique_ptr<Node> Node::create(boost::filesystem::path directory,
                                   const std::string& id) {
    auto ret = std::make_unique<NodeImpl>(std::move(directory), id);
    ret->startMemcachedServer();
    return ret;
}

} // namespace cb::test
