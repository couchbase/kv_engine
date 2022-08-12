/*
 * Portions Copyright (c) 2010-Present Couchbase
 * Portions Copyright (c) 2009 Sun Microsystems
 *
 * Use of this software is governed by the Apache License, Version 2.0 and
 * BSD 3 Clause included in the files licenses/APL2.txt and
 * licenses/BSD-3-Clause-Sun-Microsystems.txt
 */

#include "testapp.h"
#include "testapp_binprot.h"

#include <cbsasl/client.h>
#include <folly/portability/GTest.h>
#include <folly/portability/Stdlib.h>
#include <folly/portability/SysTypes.h>
#include <getopt.h>
#include <gsl/gsl-lite.hpp>
#include <json/syntax_validator.h>
#include <mcbp/protocol/framebuilder.h>
#include <memcached/protocol_binary.h>
#include <nlohmann/json.hpp>
#include <platform/backtrace.h>
#include <platform/cb_malloc.h>
#include <platform/cbassert.h>
#include <platform/dirutils.h>
#include <platform/process_monitor.h>
#include <platform/socket.h>
#include <platform/strerror.h>
#include <platform/string_hex.h>
#include <protocol/mcbp/ewb_encode.h>
#include <atomic>
#include <chrono>
#include <csignal>
#include <filesystem>
#include <fstream>

std::unique_ptr<McdEnvironment> mcd_env;

std::atomic_bool expectMemcachedTermination{false};
std::unique_ptr<ProcessMonitor> memcachedProcess;

in_port_t port = -1;
SOCKET sock = INVALID_SOCKET;
static time_t server_start_time = 0;

// used in embedded mode to shutdown memcached
extern void shutdown_server();

std::set<cb::mcbp::Feature> enabled_hello_features;

int memcached_verbose = 0;
// State variable if we're running the memcached server in a
// thread in the same process or not
static bool embedded_memcached_server;

time_t get_server_start_time() {
    return server_start_time;
}

std::ostream& operator<<(std::ostream& os, const TransportProtocols& t) {
    os << to_string(t);
    return os;
}

std::string to_string(const TransportProtocols& transport) {
    switch (transport) {
    case TransportProtocols::McbpPlain:
        return "Mcbp";
    case TransportProtocols::McbpSsl:
        return "McbpSsl";
    }
    throw std::logic_error("Unknown transport");
}

std::string to_string(ClientJSONSupport json) {
    switch (json) {
    case ClientJSONSupport::Yes:
        return "JsonYes";
    case ClientJSONSupport::No:
        return "JsonNo";
    }
    throw std::logic_error("Unknown JSON support");
}

std::string to_string(ClientSnappySupport snappy) {
    switch (snappy) {
    case ClientSnappySupport::Yes:
        return "SnappyYes";
    case ClientSnappySupport::No:
        return "SnappyNo";
    }
    throw std::logic_error("Unknown ClientSnappySupport value: " +
                           std::to_string(int(snappy)));
}

void TestappTest::rebuildAdminConnection() {
    adminConnection.reset();
    connectionMap.iterate([](const auto& c) {
        if (!adminConnection) {
            adminConnection = c.clone();
            adminConnection->authenticate(
                    "@admin", mcd_env->getPassword("@admin"), "PLAIN");
            adminConnection->unselectBucket();

            std::vector<cb::mcbp::Feature> features = {
                    {cb::mcbp::Feature::MUTATION_SEQNO,
                     cb::mcbp::Feature::XATTR,
                     cb::mcbp::Feature::XERROR,
                     cb::mcbp::Feature::SELECT_BUCKET,
                     cb::mcbp::Feature::SubdocReplaceBodyWithXattr,
                     cb::mcbp::Feature::SNAPPY,
                     cb::mcbp::Feature::JSON}};
            adminConnection->setFeatures(features);
        }
    });
    if (!adminConnection) {
        throw std::runtime_error("Failed to rebuild the admin connection");
    }
}

void TestappTest::rebuildUserConnection(bool tls) {
    userConnection.reset();
    connectionMap.iterate([&](const auto& c) {
        if (!userConnection && (c.isSsl() == tls)) {
            userConnection = c.clone();
            userConnection->authenticate("Luke", mcd_env->getPassword("Luke"));
            userConnection->selectBucket(bucketName);
        }
    });
    if (!userConnection) {
        throw std::runtime_error("Failed to rebuild the user connection");
    }
}

void TestappTest::CreateTestBucket(const std::string& bucketConf) {
    if (!adminConnection) {
        std::cerr << "TestappTest::CreateTestBucket(): Admin connection not "
                     "set up"
                  << std::endl;
        mcd_env->terminate(EXIT_FAILURE);
    }

    mcd_env->getTestBucket().setUpBucket(
            bucketName, bucketConf, *adminConnection);
}

void TestappTest::DeleteTestBucket() {
    if (!adminConnection) {
        std::cerr << "TestappTest::DeleteTestBucket(): Admin connection not "
                     "set up"
                  << std::endl;
        mcd_env->terminate(EXIT_FAILURE);
    }

    // Disconnect the user connection as we're going to delete the bucket
    // (this may speed up bucket deletion as the server won't have to run
    // the logic to disconnect the client first)
    userConnection.reset();

    try {
        adminConnection->deleteBucket(bucketName);
    } catch (const ConnectionError& error) {
        EXPECT_FALSE(error.isNotFound()) << "Delete bucket [" << bucketName
                                         << "] failed with: " << error.what();
    }
}

TestBucketImpl& TestappTest::GetTestBucket() {
    return mcd_env->getTestBucket();
}

// Per-test-case set-up.
// Called before the first test in this test case.
void TestappTest::SetUpTestCase() {
    createUserConnection = false;
    doSetUpTestCaseWithConfiguration(generate_config());
}

void TestappTest::doSetUpTestCaseWithConfiguration(
        nlohmann::json config, const std::string& bucketConf) {
    token = 0xdeadbeef;
    memcached_cfg = std::move(config);
    remove(mcd_env->getPortnumberFile().c_str());
    start_memcached_server();

    if (HasFailure()) {
        std::cerr << "Error in TestappTest::SetUpTestCase, terminating process"
                  << std::endl;

        mcd_env->terminate(EXIT_FAILURE);
    }

    rebuildAdminConnection();
    CreateTestBucket(bucketConf);

    // The connection map should only contain the bootstrap interfaces
    bool error = false;
    connectionMap.iterate([&error](const MemcachedConnection& c) {
        if (c.getTag() != "bootstrap") {
            std::cerr << "ERROR: Did not expect user interfaces to be created "
                         "at this time: "
                      << c.to_string() << std::endl;
            error = true;
        }
    });

    if (error) {
        std::exit(EXIT_FAILURE);
    }

    auto createInterface = [](const std::string& host,
                              const std::string& family) {
        // Create the SSL interface to use
        BinprotGenericCommand cmd(cb::mcbp::ClientOpcode::Ifconfig);
        cmd.setKey("define");
        nlohmann::json descr = {{"host", host},
                                {"port", 0},
                                {"family", family},
                                {"system", false},
                                {"type", "mcbp"},
                                {"tls", true},
                                {"tag", "ssl"}};
        cmd.setValue(descr.dump());
        auto rsp = adminConnection->execute(cmd);
        if (!rsp.isSuccess()) {
            std::cerr << "Failed to define interface: " << host << " " + family
                      << ": " << to_string(rsp.getStatus()) << std::endl
                      << rsp.getDataString() << std::endl;
            std::exit(EXIT_FAILURE);
        }

        TestappTest::connectionMap.add(rsp.getDataJson());
        remove(mcd_env->getPortnumberFile().c_str());
    };

    try {
        tls_properties = {
                {"private key", OBJECT_ROOT "/tests/cert/servers/node1.key"},
                {"certificate chain",
                 OBJECT_ROOT "/tests/cert/servers/chain.cert"},
                {"CA file", OBJECT_ROOT "/tests/cert/root/ca_root.cert"},
                {"minimum version", "TLS 1"},
                {"cipher list",
                 {{"TLS 1.2", "HIGH"},
                  {"TLS 1.3",
                   "TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256:TLS_"
                   "AES_"
                   "128_GCM_SHA256:TLS_AES_128_CCM_8_SHA256:TLS_AES_128_CCM_"
                   "SHA256"}}},
                {"cipher order", true},
                {"client cert auth", "disabled"}};

        BinprotGenericCommand cmd(cb::mcbp::ClientOpcode::Ifconfig);
        cmd.setKey("tls");
        cmd.setValue(tls_properties.dump());
        auto rsp = adminConnection->execute(cmd);
        if (!rsp.isSuccess()) {
            std::cerr << "Failed to set TLS properties: "
                      << to_string(rsp.getStatus()) << std::endl
                      << rsp.getDataString() << std::endl;
            std::exit(EXIT_FAILURE);
        }

        auto [ipv4, ipv6] = cb::net::getIpAddresses(false);
        if (!ipv4.empty()) {
            createInterface("127.0.0.1", "inet");
        }
        if (!ipv6.empty()) {
            createInterface("::1", "inet6");
        }
    } catch (const std::exception& exception) {
        std::cerr << "Failed to define SSL bootstrap interfaces: "
                  << exception.what() << std::endl;
        std::exit(EXIT_FAILURE);
    }
}

// Per-test-case tear-down.
// Called after the last test in this test case.
void TestappTest::TearDownTestCase() {
    if (sock != INVALID_SOCKET) {
        cb::net::closesocket(sock);
        sock = INVALID_SOCKET;
        enabled_hello_features.clear();
    }

    if (memcachedProcess && memcachedProcess->isRunning()) {
        DeleteTestBucket();
    }
    userConnection.reset();
    adminConnection.reset();
    stop_memcached_server();
}

void TestappTest::reconfigure_client_cert_auth(const std::string& state,
                                               const std::string& path,
                                               const std::string& prefix,
                                               const std::string& delimiter) {
    memcached_cfg["client_cert_auth"] = {};
    memcached_cfg["client_cert_auth"]["prefixes"] =
            nlohmann::json::array({nlohmann::json{{"path", path},
                                                  {"prefix", prefix},
                                                  {"delimiter", delimiter}}});
    tls_properties["client cert auth"] = state;
    // update the server to use this!
    reconfigure();
}

void TestappTest::setClientCertData(MemcachedConnection& connection,
                                    std::string_view user) {
    auto directory = std::filesystem::path(OBJECT_ROOT) / "tests" / "cert";
    auto cert = directory / "clients" /
                std::filesystem::path{std::string(user) + ".cert"};
    auto key = directory / "clients" /
               std::filesystem::path{std::string(user) + ".key"};
    auto ca = directory / "root" / "ca_root.cert";
    connection.setSslCertFile(cert.generic_string());
    connection.setSslKeyFile(key.generic_string());
    connection.setCaFile(ca.generic_string());
}

bool TestappTest::isJSON(std::string_view value) {
    auto validator = cb::json::SyntaxValidator::New();
    return validator->validate(value);
}

// per test setup function.
void TestappTest::SetUp() {
    verify_server_running();

    const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
    name.assign(info->test_case_name());
    name.append("_");
    name.append(info->name());
    std::replace(name.begin(), name.end(), '/', '_');

    if (!userConnection && createUserConnection) {
        rebuildUserConnection(isTlsEnabled());
    }

    if (userConnection) {
        // We may have created the connection in a test batch which mix
        // both SSL and plain connections
        if (userConnection->isSsl() != isTlsEnabled()) {
            rebuildUserConnection(isTlsEnabled());
        }
        // Update the features
        prepare(*userConnection);
        // Make sure that we have the correct bucket as the current
        // bucket
        userConnection->selectBucket(bucketName);
    }
}

// per test tear-down function.
void TestappTest::TearDown() {
    if (sock != INVALID_SOCKET) {
        cb::net::closesocket(sock);
        sock = INVALID_SOCKET;
        enabled_hello_features.clear();
    }
}

const std::string TestappTest::bucketName = "default";

// per test setup function.
void McdTestappTest::SetUp() {
    verify_server_running();
    if (getProtocolParam() == TransportProtocols::McbpPlain) {
        reconnect_to_server();
    } else {
        // Not reached (exception is thrown)
    }

    set_json_feature(hasJSONSupport() == ClientJSONSupport::Yes);

    // Set ewouldblock_engine test harness to default mode.
    ewouldblock_engine_configure(cb::engine_errc::would_block,
                                 EWBEngineMode::First,
                                 /*unused*/ 0);

    setCompressionMode("off");
}

// per test tear-down function.
void McdTestappTest::TearDown() {
    cb::net::closesocket(sock);
}

ClientJSONSupport McdTestappTest::hasJSONSupport() const {
    return getJSONParam();
}

void TestappTest::setCompressionMode(const std::string& compression_mode) {
    try {
        mcd_env->getTestBucket().setCompressionMode(
                *adminConnection, bucketName, compression_mode);
    } catch (const std::exception&) {
        rebuildAdminConnection();
        throw;
    }
}

void TestappTest::setMinCompressionRatio(float min_compression_ratio) {
    try {
        mcd_env->getTestBucket().setMinCompressionRatio(
                *adminConnection,
                bucketName,
                std::to_string(min_compression_ratio));
    } catch (const std::exception&) {
        rebuildAdminConnection();
        throw;
    }
}

void TestappTest::waitForAtLeastSeqno(MemcachedConnection& conn,
                                      Vbid vbid,
                                      uint64_t uuid,
                                      uint64_t seqno) {
    ASSERT_TRUE(mcd_env->getTestBucket().supportsPersistence())
            << "Error: your bucket does not support persistence";

    // Poll for that sequence number to be persisted.
    ObserveInfo observe;
    do {
        observe = conn.observeSeqno(vbid, uuid);
        EXPECT_EQ(0, observe.formatType);
        EXPECT_EQ(vbid, observe.vbId);
        EXPECT_EQ(uuid, observe.uuid);

        if (observe.lastPersistedSeqno < seqno) {
            // Don't busy wait, yield a little. A 2019 poll of sleeps in KV
            // reveals 100us to be our most popular wait time.
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        } else {
            break;
        }
    } while (true);
}

Document TestappTest::storeAndPersistItem(MemcachedConnection& conn,
                                          Vbid vbid,
                                          std::string key) {
    if (!conn.hasFeature(cb::mcbp::Feature::MUTATION_SEQNO)) {
        throw std::runtime_error(
                "TestappTest::storeAndPersistItem: connection must have "
                "Mutation Seqno enabled");
    }
    Document doc;
    doc.info.id = key;
    doc.value = "persist me";
    auto mutation = conn.mutate(doc, vbid, MutationType::Set);
    EXPECT_NE(0, mutation.seqno);
    EXPECT_NE(0, mutation.vbucketuuid);
    doc.info.cas = mutation.cas;

    waitForAtLeastSeqno(conn, vbid, mutation.vbucketuuid, mutation.seqno);

    return doc;
}

std::string McdTestappTest::PrintToStringCombinedName(
        const ::testing::TestParamInfo<
                ::testing::tuple<TransportProtocols, ClientJSONSupport>>&
                info) {
    return to_string(::testing::get<0>(info.param)) + "_" +
           to_string(::testing::get<1>(info.param));
}

static std::string get_errmaps_dir() {
    return cb::io::sanitizePath(SOURCE_ROOT "/etc/couchbase/kv/error_maps");
}

nlohmann::json TestappTest::generate_config() {
    nlohmann::json ret = {
            {"always_collect_trace_info", true},
            {"max_connections", Testapp::MAX_CONNECTIONS},
            {"system_connections", Testapp::MAX_CONNECTIONS / 4},
            {"stdin_listener", false},
            {"datatype_json", true},
            {"datatype_snappy", true},
            {"xattr_enabled", true},
            {"dedupe_nmvb_maps", false},
            {"active_external_users_push_interval", "30 m"},
            {"error_maps_dir", get_errmaps_dir()},
            {"audit_file", mcd_env->getAuditFilename()},
            {"rbac_file", mcd_env->getRbacFilename()},
            {"breakpad",
             {{"enabled", true},
              {"minidump_dir", mcd_env->getMinidumpDir()},
              {"content", "default"}}},
            {"threads", 2},
            {"num_auxio_threads", "default"},
            {"num_nonio_threads", "default"},
            {"num_reader_threads", "default"},
            {"num_writer_threads", "default"},
            {"opcode_attributes_override",
             {{"version", 1},
              {"EWB_CTL", {{"slow", 50}}},
              {"default", {{"slow", 500}}}}},
            {"logger",
             {{"unit_test", true},
              {"filename", mcd_env->getLogFilePattern()},
              {"cyclesize", 200 * 1024 * 1024}}},
            {"portnumber_file", mcd_env->getPortnumberFile()},
            {"prometheus", {{"port", 0}, {"family", "inet"}}},
    };

    if (!embedded_memcached_server) {
        ret["parent_identifier"] = (int)getpid();
    }

    if (memcached_verbose == 0) {
        ret["logger"]["console"] = false;
    } else {
        ret["verbosity"] = memcached_verbose - 1;
    }

    return ret;
}

void write_config_to_file(const std::string& config) {
    FILE* fp = fopen(mcd_env->getConfigurationFile().c_str(), "w");

    if (fp == nullptr) {
        throw std::system_error(errno,
                                std::system_category(),
                                "Failed to open file \"" +
                                        mcd_env->getConfigurationFile() + "\"");
    } else {
        fprintf(fp, "%s", config.c_str());
        fclose(fp);
    }
}

#ifdef WIN32
HANDLE TestappTest::pidTToHandle(pid_t pid) {
    return reinterpret_cast<HANDLE>(static_cast<size_t>(pid));
}

pid_t TestappTest::handleToPidT(HANDLE handle) {
    return static_cast<pid_t>(reinterpret_cast<size_t>(handle));
}
#endif

void TestappTest::verify_server_running() {
    if (embedded_memcached_server) {
        // we don't monitor this thread...
        return;
    }

    if (!(memcachedProcess && memcachedProcess->isRunning())) {
        std::cerr << "Server not running" << std::endl;
        mcd_env->terminate(EXIT_FAILURE);
    }
}

void TestappTest::parse_portnumber_file() {
    try {
        // I've seen that running under valgrind startup of the processes
        // might be slow, and getting even worse if the machine is under
        // load. Instead of having a "false timeout" just because the
        // server is slow, lets set the deadline to a high value so that
        // if we hit it we have a real problem and not just a loaded
        // server (rebuilding all of the source one more time is just
        // putting more load on the servers).
        connectionMap.initialize(nlohmann::json::parse(cb::io::loadFile(
                mcd_env->getPortnumberFile(), std::chrono::minutes{5})));

        // The tests which don't use the MemcachedConnection class needs the
        // global variables port and ssl_port to be set
        port = (in_port_t)-1;

        connectionMap.iterate([](const MemcachedConnection& connection) {
            if (connection.getFamily() == AF_INET) {
                if (!connection.isSsl()) {
                    port = connection.getPort();
                }
            }
        });

        if (port == in_port_t(-1)) {
            std::stringstream ss;
            connectionMap.iterate([&ss](const MemcachedConnection& connection) {
                ss << "[" << connection.to_string() << "]," << std::endl;
            });

            throw std::runtime_error(
                    "parse_portnumber_file: Failed to locate an plain IPv4 "
                    "connection from: " +
                    ss.str());
        }
        EXPECT_EQ(0, remove(mcd_env->getPortnumberFile().c_str()));
    } catch (const std::exception& e) {
        std::cerr << "FATAL ERROR in parse_portnumber_file!" << std::endl
                  << "An error occured while getting the connection ports: "
                  << std::endl
                  << e.what() << std::endl;
        std::abort();
    }
}

int memcached_main(int argc, char** argv);

void memcached_server_thread_main(std::string config) {
    char* argv[4];
    int argc = 0;
    argv[argc++] = const_cast<char*>("./memcached");
    argv[argc++] = const_cast<char*>("-C");
    argv[argc++] = const_cast<char*>(config.c_str());

    // Reset getopt()'s optind so memcached_main starts from the first
    // argument.
    optind = 1;
    memcached_main(argc, argv);
}

void TestappTest::spawn_embedded_server() {
    if (num_server_starts > 0) {
        std::cerr << "ERROR: Embedded server may only be started once as we "
                     "don't properly reset all global variables"
                  << std::endl;
        mcd_env->terminate(EXIT_FAILURE);
    }

    memcached_server_thread = std::thread(memcached_server_thread_main,
                                          mcd_env->getConfigurationFile());
}

void TestappTest::start_external_server() {
    std::filesystem::path exe{std::filesystem::current_path() / "memcached"};
    exe = exe.generic_string();
    if (!std::filesystem::exists(exe)) {
        exe = exe.generic_string() + ".exe";
        if (!std::filesystem::exists(exe)) {
            throw std::runtime_error(
                    "start_external_server(): Failed to locate memcached");
        }
    }

    std::vector<std::string> argv;
    if (getenv("RUN_UNDER_PERF") != nullptr) {
        argv.emplace_back("perf");
        argv.emplace_back("record");
        argv.emplace_back("--call-graph");
        argv.emplace_back("dwarf");
    }
    argv.emplace_back(exe.generic_string());
    argv.emplace_back("-C");
    argv.emplace_back(mcd_env->getConfigurationFile());
    expectMemcachedTermination.store(false);
    memcachedProcess = ProcessMonitor::create(argv, [](const auto& ec) {
        if (!expectMemcachedTermination) {
            std::cerr << "memcached process terminated: "
                      << ec.to_json().dump(2) << std::endl;
            std::_Exit(EXIT_FAILURE);
        }
    });
}

SOCKET connect_to_server_plain() {
    if (port == in_port_t(-1)) {
        throw std::runtime_error(
                "create_connect_plain_socket: Can't connect to port == -1");
    }
    MemcachedConnection connection("127.0.0.1", port, AF_INET, false);
    connection.connect();
    connection.authenticate("Luke", mcd_env->getPassword("Luke"));
    connection.selectBucket("default");
    return connection.releaseSocket();
}

/*
    re-connect to server.
    Uses global port and ssl_port values.
    New socket-fd written to global "sock" and "ssl_bio"
*/
void reconnect_to_server() {
    if (sock != INVALID_SOCKET) {
        cb::net::closesocket(sock);
    }
    enabled_hello_features.clear();
    sock = connect_to_server_plain();
    ASSERT_NE(INVALID_SOCKET, sock);
}

static void set_feature(cb::mcbp::Feature feature, bool enable) {
    // First update the currently enabled features.
    if (enable) {
        enabled_hello_features.insert(feature);
    } else {
        enabled_hello_features.erase(feature);
    }

    BinprotHelloCommand command("testapp");
    for (auto f : enabled_hello_features) {
        command.enableFeature(f, true);
    }

    std::vector<uint8_t> blob;
    command.encode(blob);

    safe_send(blob);

    blob.resize(0);
    safe_recv_packet(blob);
    const auto& response = *reinterpret_cast<cb::mcbp::Response*>(blob.data());
    ASSERT_EQ(cb::mcbp::Status::Success, response.getStatus());

    BinprotHelloResponse rsp;
    rsp.assign(std::move(blob));

    auto enabled = rsp.getFeatures();
    EXPECT_EQ(enabled_hello_features.size(), enabled.size());
    for (auto f : enabled_hello_features) {
        EXPECT_NE(enabled.end(), std::find(enabled.begin(), enabled.end(), f));
    }
}

std::pair<cb::mcbp::Status, std::string> TestappTest::fetch_value(
        const std::string& key) {
    if (!userConnection) {
        rebuildUserConnection(false);
    }
    const auto rsp = BinprotGetResponse{userConnection->execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::Get, key})};
    return {rsp.getStatus(), rsp.getDataString()};
}

void TestappTest::validate_datatype(const std::string& key,
                                    cb::mcbp::Datatype datatype) {
    if (!userConnection) {
        rebuildUserConnection(false);
    }
    bool addedJson = false;
    if (!userConnection->hasFeature(cb::mcbp::Feature::JSON)) {
        addedJson = true;
        userConnection->setFeature(cb::mcbp::Feature::JSON, true);
    }

    const auto info = userConnection->get(key, Vbid{0});
    if (addedJson) {
        userConnection->setFeature(cb::mcbp::Feature::JSON, false);
    }

    EXPECT_EQ(datatype, info.info.datatype);
}

void TestappTest::validate_json_document(const std::string& key,
                                         const std::string& expected_value) {
    if (!userConnection) {
        rebuildUserConnection(false);
    }
    const auto info = userConnection->get(key, Vbid{0});
    const auto exp = nlohmann::json::parse(expected_value).dump();
    const auto val = nlohmann::json::parse(info.value).dump();
    EXPECT_EQ(exp, val);
}

void TestappTest::validate_flags(const std::string& key,
                                 uint32_t expected_flags) {
    if (!userConnection) {
        rebuildUserConnection(false);
    }
    auto rsp = BinprotGetResponse{userConnection->execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::Get, key})};
    ASSERT_TRUE(rsp.isSuccess()) << to_string(rsp.getStatus()) << std::endl
                                 << rsp.getDataString();
    EXPECT_EQ(expected_flags, rsp.getDocumentFlags());
}

void TestappTest::delete_object(const std::string& key, bool ignore_missing) {
    if (!userConnection) {
        rebuildUserConnection(false);
    }
    auto rsp = userConnection->execute(
            BinprotGenericCommand{cb::mcbp::ClientOpcode::Delete, key});
    if (ignore_missing && rsp.getStatus() == cb::mcbp::Status::KeyEnoent) {
        return;
    }

    ASSERT_TRUE(rsp.isSuccess()) << to_string(rsp.getStatus()) << std::endl
                                 << rsp.getDataString();
}

void TestappTest::start_memcached_server() {
    write_config_to_file(memcached_cfg.dump(2));

    server_start_time = time(nullptr);

    if (embedded_memcached_server) {
        spawn_embedded_server();
    } else {
        start_external_server();
    }
    ++num_server_starts;
    parse_portnumber_file();
}

void TestappTest::store_document(std::string key,
                                 std::string value,
                                 uint32_t flags,
                                 uint32_t expiration,
                                 bool compress) {
    Document document;

    document.info.cas = cb::mcbp::cas::Wildcard;
    document.info.datatype = cb::mcbp::Datatype::Raw;
    document.info.expiration = expiration;
    document.info.flags = flags;
    document.info.id = std::move(key);
    document.value = std::move(value);
    if (compress) {
        document.compress();
    }

    if (!userConnection) {
        rebuildUserConnection(false);
    }

    bool addedSnappy = false;
    if (compress && !userConnection->hasFeature(cb::mcbp::Feature::SNAPPY)) {
        addedSnappy = true;
        userConnection->setFeature(cb::mcbp::Feature::SNAPPY, true);
    }

    userConnection->mutate(document, Vbid{0}, MutationType::Set);
    if (addedSnappy) {
        userConnection->setFeature(cb::mcbp::Feature::SNAPPY, false);
    }
}

void set_json_feature(bool enable) {
    set_feature(cb::mcbp::Feature::JSON, enable);
}

void set_mutation_seqno_feature(bool enable) {
    set_feature(cb::mcbp::Feature::MUTATION_SEQNO, enable);
}

void set_xerror_feature(bool enable) {
    set_feature(cb::mcbp::Feature::XERROR, enable);
}

void TestappTest::waitForShutdown(bool killed) {
    if (memcachedProcess) {
        while (memcachedProcess->isRunning()) {
            std::this_thread::sleep_for(std::chrono::microseconds{10});
        }
    }
}

void TestappTest::stop_memcached_server() {
    connectionMap.invalidate();
    if (sock != INVALID_SOCKET) {
        cb::net::closesocket(sock);
        sock = INVALID_SOCKET;
    }

    if (embedded_memcached_server) {
        shutdown_server();
        memcached_server_thread.join();
    }

    if (memcachedProcess) {
        // We don't need it to do a graceful stop
        expectMemcachedTermination.store(true);
        memcachedProcess->terminate(false);
        memcachedProcess.reset();
    }
}

static bool dump_socket_traffic = getenv("TESTAPP_PACKET_DUMP") != nullptr;

static const std::string phase_get_errno() {
    return cb_strerror();
}

void safe_send(const void* buf, size_t len) {
    if (sock == INVALID_SOCKET) {
        std::abort();
    }
    size_t offset = 0;
    const char* ptr = reinterpret_cast<const char*>(buf);
    do {
        size_t num_bytes = len - offset;
        ssize_t nw;
        nw = cb::net::send(sock, ptr + offset, num_bytes, 0);

        if (nw == -1) {
            if (errno != EINTR) {
                fprintf(stderr,
                        "Failed to write: %s\n",
                        phase_get_errno().c_str());
                print_backtrace_to_file(stderr);
                abort();
            }
        } else {
            if (dump_socket_traffic) {
                std::cerr << "PLAIN> ";
                for (ssize_t ii = 0; ii < nw; ++ii) {
                    std::cerr << "0x" << std::hex << std::setfill('0')
                              << std::setw(2)
                              << uint32_t(*(uint8_t*)(ptr + offset + ii))
                              << ", ";
                }
                std::cerr << std::dec << std::endl;
            }
            offset += nw;
        }
    } while (offset < len);
}

bool safe_recv(void* buf, size_t len) {
    if (sock == INVALID_SOCKET) {
        std::abort();
    }
    size_t offset = 0;
    if (len == 0) {
        return true;
    }
    do {
        ssize_t nr =
                cb::net::recv(sock, ((char*)buf) + offset, len - offset, 0);

        if (nr == -1) {
            EXPECT_EQ(EINTR, errno) << "Failed to read: " << phase_get_errno();
        } else {
            EXPECT_NE(0u, nr);
            offset += nr;
        }

        // Give up if we encountered an error.
        if (::testing::Test::HasFailure()) {
            return false;
        }
    } while (offset < len);

    return true;
}

/**
 * Internal function which receives a packet. The type parameter should have
 * these three functions:
 * - resize(n) ->: ensure the buffer has a total capacity for at least n bytes.
 *   This function will usually be called once for the header size (i.e. 24)
 *   and then another time for the total packet size (i.e. 24 + bodylen).
 * - data() -> char*: get the entire buffer
 * - size() -> size_t: get the current size of the buffer
 *
 * @param info an object conforming to the above.
 *
 * Once the function has completed, it will return true if no read errors
 * occurred. The actual size of the packet can be determined by parsing the
 * packet header.
 *
 * See StaticBufInfo which is an implementation that uses a fixed buffer.
 * std::vector naturally conforms to the interface.
 */
template <typename T>
bool safe_recv_packetT(T& info) {
    info.resize(sizeof(cb::mcbp::Response));
    auto* header = reinterpret_cast<cb::mcbp::Response*>(info.data());

    if (!safe_recv(header, sizeof(*header))) {
        return false;
    }

    auto bodylen = header->getBodylen();

    // Set response to NULL, because the underlying buffer may change.
    header = nullptr;

    info.resize(sizeof(*header) + bodylen);
    auto ret = safe_recv(info.data() + sizeof(*header), bodylen);

    if (dump_socket_traffic) {
        if (dump_socket_traffic) {
            std::cerr << "PLAIN< ";
            for (const auto& val : info) {
                std::cerr << cb::to_hex(uint8_t(val)) << ", ";
            }
        }
        std::cerr << std::endl;
    }
    return ret;
}

/**
 * Wrapper for a existing buffer which exposes an API suitable for use with
 * safe_recv_packetT()
 */
struct StaticBufInfo {
    StaticBufInfo(void* buf_, size_t len_)
        : buf(reinterpret_cast<char*>(buf_)), len(len_) {
    }
    size_t size(size_t) const {
        return len;
    }
    char* data() {
        return buf;
    }

    void resize(size_t n) {
        if (n > len) {
            throw std::runtime_error("Cannot enlarge buffer!");
        }
    }

    constexpr char* begin() const {
        return buf;
    }

    constexpr char* end() const {
        return buf + len;
    }

    char* buf;
    const size_t len;
};

bool safe_recv_packet(void* buf, size_t size) {
    StaticBufInfo info(buf, size);
    return safe_recv_packetT(info);
}

bool safe_recv_packet(std::vector<uint8_t>& buf) {
    return safe_recv_packetT(buf);
}

bool safe_recv_packet(std::vector<char>& buf) {
    return safe_recv_packetT(buf);
}

// Configues the ewouldblock_engine to use the given mode; value
// is a mode-specific parameter.
void TestappTest::ewouldblock_engine_configure(cb::engine_errc err_code,
                                               const EWBEngineMode& mode,
                                               uint32_t value,
                                               const std::string& key) {
    cb::mcbp::request::EWB_Payload payload;
    payload.setMode(static_cast<uint32_t>(mode));
    payload.setValue(value);
    payload.setInjectError(static_cast<uint32_t>(err_code));

    std::vector<uint8_t> buffer(sizeof(cb::mcbp::Request) +
                                sizeof(cb::mcbp::request::EWB_Payload) +
                                key.size());
    cb::mcbp::RequestBuilder builder({buffer.data(), buffer.size()});
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::EwouldblockCtl);
    builder.setOpaque(0xdeadbeef);
    builder.setExtras(
            {reinterpret_cast<const uint8_t*>(&payload), sizeof(payload)});
    builder.setKey({reinterpret_cast<const uint8_t*>(key.data()), key.size()});
    safe_send(buffer);

    buffer.resize(1024);
    safe_recv_packet(buffer.data(), buffer.size());
    mcbp_validate_response_header(
            *reinterpret_cast<cb::mcbp::Response*>(buffer.data()),
            cb::mcbp::ClientOpcode::EwouldblockCtl,
            cb::mcbp::Status::Success);
}

void TestappTest::ewouldblock_engine_configure(
        const std::vector<cb::engine_errc>& sequence) {
    ewouldblock_engine_configure(cb::engine_errc::success,
                                 EWBEngineMode::Sequence,
                                 0,
                                 ewb::encodeSequence(sequence));
}

void TestappTest::ewouldblock_engine_disable() {
    // Value for err_code doesn't matter...
    ewouldblock_engine_configure(
            cb::engine_errc::would_block, EWBEngineMode::Next_N, 0);
}

void TestappTest::reconfigure() {
    if (!adminConnection) {
        std::cerr << "TestappTest::reconfigure(): Admin connection not "
                     "set up"
                  << std::endl;
        mcd_env->terminate(EXIT_FAILURE);
    }

    write_config_to_file(memcached_cfg.dump(2));
    auto rsp = adminConnection->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::ConfigReload, {}, {}});
    ASSERT_TRUE(rsp.isSuccess())
            << "Failed to reconfigure server: " << to_string(rsp.getStatus())
            << std::endl
            << rsp.getDataString();

    rsp = adminConnection->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::Ifconfig, "tls", tls_properties.dump()});
    ASSERT_TRUE(rsp.isSuccess())
            << "Failed to set TLS properties: " << to_string(rsp.getStatus())
            << std::endl
            << rsp.getDataString() << std::endl;
}

int TestappTest::getResponseCount(cb::mcbp::Status statusCode) {
    nlohmann::json stats;

    if (!userConnection) {
        rebuildUserConnection(false);
    }

    stats = userConnection->stats("responses detailed");
    auto responses = stats["responses"];
    std::stringstream stream;
    stream << std::hex << uint16_t(statusCode);
    auto obj = responses.find(stream.str());
    if (obj == responses.end()) {
        return 0;
    }

    return gsl::narrow<int>(obj->get<size_t>());
}

cb::mcbp::Datatype TestappTest::expectedJSONDatatype() const {
    return hasJSONSupport() == ClientJSONSupport::Yes ? cb::mcbp::Datatype::JSON
                                                      : cb::mcbp::Datatype::Raw;
}

MemcachedConnection& TestappTest::getConnection() {
    // The basic tests should use a plain IPv4 unless something else is
    // required (this makes the return value of getConnection predictable
    // (rather than returning whatever happened to be stored in "front" of
    // the map.
    auto& connection = connectionMap.getConnection(
            isTlsEnabled(), mcd_env->haveIPv4() ? AF_INET : AF_INET6);
    connection.connect();
    return prepare(connection);
}

MemcachedConnection& TestappTest::getAdminConnection() {
    auto& conn = getConnection();
    conn.authenticate("@admin", "password", conn.getSaslMechanisms());
    return conn;
}

MemcachedConnection& TestappTest::prepare(MemcachedConnection& connection) {
    std::vector<cb::mcbp::Feature> features = {
            {cb::mcbp::Feature::MUTATION_SEQNO,
             cb::mcbp::Feature::XATTR,
             cb::mcbp::Feature::XERROR,
             cb::mcbp::Feature::SELECT_BUCKET,
             cb::mcbp::Feature::SubdocReplaceBodyWithXattr}};
    if (hasSnappySupport() == ClientSnappySupport::Yes) {
        features.push_back(cb::mcbp::Feature::SNAPPY);
    }

    if (hasJSONSupport() == ClientJSONSupport::Yes) {
        features.push_back(cb::mcbp::Feature::JSON);
    }

    connection.setFeatures(features);
    return connection;
}

/* Request stats
 * @return a map of stat key & values in the server response.
 */
stats_response_t request_stats() {
    BinprotGenericCommand cmd(cb::mcbp::ClientOpcode::Stat);
    std::vector<uint8_t> blob;
    cmd.encode(blob);
    safe_send(blob);

    stats_response_t result;
    while (true) {
        safe_recv_packet(blob);
        BinprotResponse rsp;
        rsp.assign(std::move(blob));
        mcbp_validate_response_header(
                const_cast<cb::mcbp::Response&>(rsp.getResponse()),
                cb::mcbp::ClientOpcode::Stat,
                cb::mcbp::Status::Success);
        // key length zero indicates end of the stats.
        if (rsp.getKey().empty()) {
            break;
        }

        result.insert(
                std::make_pair(std::string{rsp.getKey()}, rsp.getDataString()));
    }

    return result;
}

// Extracts a single statistic from the set of stats, returning as
// a uint64_t
uint64_t extract_single_stat(const stats_response_t& stats, const char* name) {
    auto iter = stats.find(name);
    EXPECT_NE(stats.end(), iter);
    uint64_t result = 0;
    result = std::stoul(iter->second);
    return result;
}

/*
    Using a memcached protocol extesnsion, shift the time
*/
void adjust_memcached_clock(
        int64_t clock_shift,
        cb::mcbp::request::AdjustTimePayload::TimeType timeType) {
    cb::mcbp::request::AdjustTimePayload payload;
    payload.setOffset(uint64_t(clock_shift));
    payload.setTimeType(timeType);

    std::vector<uint8_t> blob(sizeof(cb::mcbp::Request) + sizeof(payload));
    cb::mcbp::FrameBuilder<cb::mcbp::Request> builder(
            {blob.data(), blob.size()});
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::AdjustTimeofday);
    builder.setExtras(payload.getBuffer());
    builder.setOpaque(0xdeadbeef);
    safe_send(builder.getFrame()->getFrame());

    blob.resize(0);
    safe_recv_packet(blob);
    BinprotResponse rsp;
    rsp.assign(std::move(blob));
    mcbp_validate_response_header(
            const_cast<cb::mcbp::Response&>(rsp.getResponse()),
            cb::mcbp::ClientOpcode::AdjustTimeofday,
            cb::mcbp::Status::Success);
}

static void dumpTotalSocketsCreated() {
    std::cout << "Testapp created " << MemcachedConnection::totalSocketsCreated
              << " sockets" << std::endl;
}

nlohmann::json TestappTest::memcached_cfg;
nlohmann::json TestappTest::tls_properties;
ConnectionMap TestappTest::connectionMap;
uint64_t TestappTest::token;
std::thread TestappTest::memcached_server_thread;
std::size_t TestappTest::num_server_starts = 0;
std::unique_ptr<MemcachedConnection> TestappTest::adminConnection;
std::unique_ptr<MemcachedConnection> TestappTest::userConnection;
bool TestappTest::createUserConnection = false;

int main(int argc, char** argv) {
    if (getenv("COUNT_SOCKETS")) {
        atexit(dumpTotalSocketsCreated);
    }
    setupWindowsDebugCRTAssertHandling();

    // peek at the arguments to check if we're just going to dump the tests
    bool list_tests = false;
    for (int ii = 1; ii < argc; ++ii) {
        if (std::string_view{"--gtest_list_tests"} == argv[ii]) {
            list_tests = true;
        }
    }

    ::testing::InitGoogleTest(&argc, argv);

    if (list_tests) {
        return (RUN_ALL_TESTS());
    }

#ifndef WIN32
    /*
    ** When running the tests from within CLion it starts the test in
    ** another directory than pwd. This cause us to fail to locate the
    ** memcached binary to start. To work around that lets just do a
    ** chdir(dirname(argv[0])).
    */
    auto testdir = cb::io::dirname(argv[0]);
    if (chdir(testdir.c_str()) != 0) {
        std::cerr << "Failed to change directory to " << testdir << std::endl;
        exit(EXIT_FAILURE);
    }
#endif

    std::string engine_name("ep");
    std::string engine_config;

    int cmd;
    while ((cmd = getopt(argc, argv, "vc:eE:")) != EOF) {
        switch (cmd) {
        case 'v':
            memcached_verbose++;
            break;
        case 'c':
            engine_config = optarg;
            break;
        case 'e':
            embedded_memcached_server = true;
            break;
        case 'E':
            engine_name.assign(optarg);
            break;
        default:
            std::cerr << "Usage: " << argv[0] << " [-v] [-e]" << std::endl
                      << std::endl
                      << "  -v Verbose - Print verbose memcached output "
                      << "to stderr." << std::endl
                      << "               (use multiple times to increase the"
                      << " verbosity level." << std::endl
                      << "  -c CONFIG - Additional configuration to pass to "
                      << "bucket creation." << std::endl
                      << "  -e Embedded - Run the memcached daemon in the "
                      << "same process (for debugging only..)" << std::endl
                      << "  -E ENGINE engine type to use. <default|ep>"
                      << std::endl;
            return 1;
        }
    }

    // If not running in embedded mode we need the McdEnvironment to manageSSL
    // initialization and shutdown.
    try {
        mcd_env.reset(McdEnvironment::create(
                !embedded_memcached_server, engine_name, engine_config));
    } catch (const std::exception& e) {
        std::cerr << "Failed to set up test environment: " << e.what()
                  << std::endl;
        exit(EXIT_FAILURE);
    }

    cb::net::initialize();
    try {
        cb::backtrace::initialize();
    } catch (const std::exception& e) {
        std::cerr << "Failed to setup bactrace support: " << e.what()
                  << std::endl;
        mcd_env->terminate(EXIT_FAILURE);
    }

#if !defined(WIN32)
    /*
     * When shutting down SSL connections the SSL layer may attempt to
     * write to the underlying socket. If the socket has been closed
     * on the server side then this will raise a SIGPIPE (and
     * terminate the test program). This is Bad.
     */
    if (signal(SIGPIPE, SIG_IGN) == SIG_ERR) {
        std::cerr << "Fatal: failed to ignore SIGPIPE" << std::endl;
        return 1;
    }
#endif

    mcd_env->terminate(RUN_ALL_TESTS());
}
