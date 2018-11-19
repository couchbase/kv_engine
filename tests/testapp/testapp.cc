/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "testapp.h"

#include "ssl_impl.h"

#include <JSON_checker.h>
#include <cbsasl/client.h>
#include <gtest/gtest.h>
#include <mcbp/protocol/framebuilder.h>
#include <platform/backtrace.h>
#include <platform/cb_malloc.h>
#include <platform/dirutils.h>
#include <platform/socket.h>
#include <platform/strerror.h>
#include <snappy-c.h>
#include <gsl/gsl>

#include <getopt.h>
#include <platform/compress.h>
#include <fstream>

#include <include/memcached/protocol_binary.h>
#include <nlohmann/json.hpp>
#include <atomic>
#include <csignal>
#include <thread>

McdEnvironment* mcd_env = nullptr;

/* test phases (bitmasks) */
#define phase_plain 0x2
#define phase_ssl 0x4

#define phase_max 4
static int current_phase = 0;

pid_t server_pid = pid_t(-1);
in_port_t port = -1;
in_port_t ssl_port = -1;
SOCKET sock = INVALID_SOCKET;
SOCKET sock_ssl;
static std::atomic<bool> allow_closed_read;
static time_t server_start_time = 0;

// used in embedded mode to shutdown memcached
extern void shutdown_server();

std::set<cb::mcbp::Feature> enabled_hello_features;

int memcached_verbose = 0;
// State variable if we're running the memcached server in a
// thread in the same process or not
static bool embedded_memcached_server;

/* static storage for the different environment variables set by
 * putenv().
 *
 * (These must be static as putenv() essentially 'takes ownership' of
 * the provided array, so it is unsafe to use an automatic variable.
 * However, if we use the result of cb_malloc() (i.e. the heap) then
 * memory leak checkers (e.g. Valgrind) will report the memory as
 * leaked as it's impossible to free it).
 */
static char mcd_parent_monitor_env[80];
static char mcd_port_filename_env[80];

static SOCKET connect_to_server_ssl(in_port_t ssl_port);

void set_allow_closed_read(bool enabled) {
    allow_closed_read = enabled;
}

bool sock_is_ssl() {
    return current_phase == phase_ssl;
}

time_t get_server_start_time() {
    return server_start_time;
}

std::ostream& operator<<(std::ostream& os, const TransportProtocols& t) {
    os << to_string(t);
    return os;
}

std::string to_string(const TransportProtocols& transport) {
#ifdef JETBRAINS_CLION_IDE
    // CLion don't properly parse the output when the
    // output gets written as the string instead of the
    // number. This makes it harder to debug the tests
    // so let's just disable it while we're waiting
    // for them to supply a fix.
    // See https://youtrack.jetbrains.com/issue/CPP-6039
    return std::to_string(static_cast<int>(transport));
#else
    switch (transport) {
    case TransportProtocols::McbpPlain:
        return "Mcbp";
    case TransportProtocols::McbpIpv6Plain:
        return "McbpIpv6";
    case TransportProtocols::McbpSsl:
        return "McbpSsl";
    case TransportProtocols::McbpIpv6Ssl:
        return "McbpIpv6Ssl";
    }
    throw std::logic_error("Unknown transport");
#endif
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

void TestappTest::CreateTestBucket()
{
    auto& conn = connectionMap.getConnection(false);

    // Reconnect to the server so we know we're on a "fresh" connection
    // to the server (and not one that might have been cached by the
    // idle-timer, but not yet noticed on the client side)
    conn.reconnect();
    conn.authenticate("@admin", "password", "PLAIN");

    mcd_env->getTestBucket().setUpBucket(bucketName, "", conn);

    // Reconnect the object to avoid others to reuse the admin creds
    conn.reconnect();
}

void TestappTest::DeleteTestBucket() {
    auto& conn = connectionMap.getConnection(false);
    conn.reconnect();
    conn.authenticate("@admin", "password", "PLAIN");
    try {
        conn.deleteBucket(bucketName);
    } catch (const ConnectionError& error) {
        EXPECT_FALSE(error.isNotFound()) << "Delete bucket ["
        << bucketName
        << "] failed with: "
                                         << error.what();
    }
}

TestBucketImpl& TestappTest::GetTestBucket()
{
    return mcd_env->getTestBucket();
}

// Per-test-case set-up.
// Called before the first test in this test case.
void TestappTest::SetUpTestCase() {
    token = 0xdeadbeef;
    memcached_cfg = generate_config(0);
    start_memcached_server();

    if (HasFailure()) {
        std::cerr << "Error in TestappTest::SetUpTestCase, terminating process"
                  << std::endl;

        exit(EXIT_FAILURE);
    } else {
        CreateTestBucket();
    }
}

// Per-test-case tear-down.
// Called after the last test in this test case.
void TestappTest::TearDownTestCase() {
    if (sock != INVALID_SOCKET) {
        cb::net::closesocket(sock);
    }

    if (server_pid != reinterpret_cast<pid_t>(-1)) {
        DeleteTestBucket();
    }
    stop_memcached_server();
}

void TestappTest::reconfigure_client_cert_auth(const std::string& state,
                                               const std::string& path,
                                               const std::string& prefix,
                                               const std::string& delimiter) {
    memcached_cfg["client_cert_auth"] = {};
    memcached_cfg["client_cert_auth"]["state"] = state;
    memcached_cfg["client_cert_auth"]["path"] = path;
    memcached_cfg["client_cert_auth"]["prefix"] = prefix;
    memcached_cfg["client_cert_auth"]["delimiter"] = delimiter;
    // update the server to use this!
    reconfigure();
}

void TestappTest::setClientCertData(MemcachedConnection& connection) {
    connection.setSslCertFile(SOURCE_ROOT +
                              std::string("/tests/cert/client.pem"));
    connection.setSslKeyFile(SOURCE_ROOT +
                             std::string("/tests/cert/client.key"));
}

std::string get_sasl_mechs(void) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;

    size_t plen = mcbp_raw_command(buffer.bytes,
                                   sizeof(buffer.bytes),
                                   cb::mcbp::ClientOpcode::SaslListMechs,
                                   NULL,
                                   0,
                                   NULL,
                                   0);

    safe_send(buffer.bytes, plen, false);
    safe_recv_packet(&buffer, sizeof(buffer));
    mcbp_validate_response_header(&buffer.response,
                                  cb::mcbp::ClientOpcode::SaslListMechs,
                                  cb::mcbp::Status::Success);

    std::string ret;
    ret.assign(buffer.bytes + sizeof(buffer.response.bytes),
               buffer.response.message.header.response.getBodylen());
    return ret;
}

cb::mcbp::Status TestappTest::sasl_auth(const char* username,
                                        const char* password) {
    cb::sasl::client::ClientContext client(
            [username]() -> std::string { return username; },
            [password]() -> std::string { return password; },
            get_sasl_mechs());

    auto client_data = client.start();
    EXPECT_EQ(cb::sasl::Error::OK, client_data.first);
    if (::testing::Test::HasFailure()) {
        // Can't continue if we didn't suceed in starting SASL auth.
        return cb::mcbp::Status::Einternal;
    }

    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;

    size_t plen = mcbp_raw_command(buffer.bytes,
                                   sizeof(buffer.bytes),
                                   cb::mcbp::ClientOpcode::SaslAuth,
                                   client.getName().data(),
                                   client.getName().size(),
                                   client_data.second.data(),
                                   client_data.second.size());

    safe_send(buffer.bytes, plen, false);
    safe_recv_packet(&buffer, sizeof(buffer));

    bool stepped = false;

    while (buffer.response.message.header.response.getStatus() ==
           cb::mcbp::Status::AuthContinue) {
        stepped = true;
        size_t datalen = buffer.response.message.header.response.getBodylen() -
                         buffer.response.message.header.response.getKeylen() -
                         buffer.response.message.header.response.getExtlen();

        size_t dataoffset =
                sizeof(buffer.response.bytes) +
                buffer.response.message.header.response.getKeylen() +
                buffer.response.message.header.response.getExtlen();

        client_data = client.step(
                cb::const_char_buffer{buffer.bytes + dataoffset, datalen});
        EXPECT_EQ(cb::sasl::Error::CONTINUE, client_data.first);

        plen = mcbp_raw_command(buffer.bytes,
                                sizeof(buffer.bytes),
                                cb::mcbp::ClientOpcode::SaslStep,
                                client.getName().data(),
                                client.getName().size(),
                                client_data.second.data(),
                                client_data.second.size());

        safe_send(buffer.bytes, plen, false);

        safe_recv_packet(&buffer, sizeof(buffer));
    }

    if (stepped) {
        mcbp_validate_response_header(
                &buffer.response,
                cb::mcbp::ClientOpcode::SaslStep,
                buffer.response.message.header.response.getStatus());
    } else {
        mcbp_validate_response_header(
                &buffer.response,
                cb::mcbp::ClientOpcode::SaslAuth,
                buffer.response.message.header.response.getStatus());
    }

    return buffer.response.message.header.response.getStatus();
}

bool TestappTest::isJSON(cb::const_char_buffer value) {
    JSON_checker::Validator validator;
    const auto* ptr = reinterpret_cast<const uint8_t*>(value.data());
    return validator.validate(ptr, value.size());
}

// per test setup function.
void TestappTest::SetUp() {
    verify_server_running();
    current_phase = phase_plain;
    sock = connect_to_server_plain(port);
    ASSERT_NE(INVALID_SOCKET, sock);

    // Set ewouldblock_engine test harness to default mode.
    ewouldblock_engine_configure(ENGINE_EWOULDBLOCK, EWBEngineMode::First,
                                 /*unused*/0);

    enabled_hello_features.clear();

    const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
    name.assign(info->test_case_name());
    name.append("_");
    name.append(info->name());
    std::replace(name.begin(), name.end(), '/', '_');
}

// per test tear-down function.
void TestappTest::TearDown() {
    cb::net::closesocket(sock);
}

const std::string TestappTest::bucketName = "default";

// per test setup function.
void McdTestappTest::SetUp() {
    verify_server_running();
    if (getProtocolParam() == TransportProtocols::McbpPlain) {
        current_phase = phase_plain;
        sock = connect_to_server_plain(port);
        ASSERT_NE(INVALID_SOCKET, sock);
    } else {
        current_phase = phase_ssl;
        sock_ssl = connect_to_server_ssl(ssl_port);
        ASSERT_NE(INVALID_SOCKET, sock_ssl);
    }

    set_json_feature(hasJSONSupport() == ClientJSONSupport::Yes);

    // Set ewouldblock_engine test harness to default mode.
    ewouldblock_engine_configure(ENGINE_EWOULDBLOCK, EWBEngineMode::First,
                                 /*unused*/0);

    setCompressionMode("off");
}

// per test tear-down function.
void McdTestappTest::TearDown() {
    if (getProtocolParam() == TransportProtocols::McbpPlain) {
        cb::net::closesocket(sock);
    } else {
        cb::net::closesocket(sock_ssl);
        sock_ssl = INVALID_SOCKET;
        destroy_ssl_socket();
    }
}

ClientJSONSupport McdTestappTest::hasJSONSupport() const {
    return getJSONParam();
}

void TestappTest::setCompressionMode(const std::string& compression_mode) {
    mcd_env->getTestBucket().setCompressionMode(
            getAdminConnection(), bucketName, compression_mode);
}

void TestappTest::setMinCompressionRatio(float min_compression_ratio) {
    mcd_env->getTestBucket().setMinCompressionRatio(
            getAdminConnection(),
            bucketName,
            std::to_string(min_compression_ratio));
}

std::string McdTestappTest::PrintToStringCombinedName(
        const ::testing::TestParamInfo<
                ::testing::tuple<TransportProtocols, ClientJSONSupport>>&
                info) {
    return to_string(::testing::get<0>(info.param)) + "_" +
           to_string(::testing::get<1>(info.param));
}

std::string CERTIFICATE_PATH(const std::string& file) {
#ifdef WIN32
    return std::string("\\tests\\cert\\") + file;
#else
    return std::string("/tests/cert/") + file;
#endif
}
static std::string get_errmaps_dir() {
    std::string dir(SOURCE_ROOT);
    dir += "/etc/couchbase/kv/error_maps";
    cb::io::sanitizePath(dir);
    return dir;
}

nlohmann::json TestappTest::generate_config(uint16_t ssl_port) {
    const std::string cwd = cb::io::getcwd();
    const std::string pem_path = cwd + CERTIFICATE_PATH("testapp.pem");
    const std::string cert_path = cwd + CERTIFICATE_PATH("testapp.cert");

    nlohmann::json ret;
    ret["logger"]["unit_test"] = true;
    if (memcached_verbose == 0) {
        ret["logger"]["console"] = false;
    }

    ret["verbosity"] = memcached_verbose;
    ret["stdin_listener"] = false;
    ret["interfaces"][0]["port"] = 0;
    ret["interfaces"][0]["ipv4"] = "optional";
    ret["interfaces"][0]["ipv6"] = "optional";
    ret["interfaces"][0]["maxconn"] = Testapp::MAX_CONNECTIONS;
    ret["interfaces"][0]["backlog"] = Testapp::BACKLOG;
    ret["interfaces"][0]["host"] = "*";
    ret["interfaces"][0]["protocol"] = "memcached";
    ret["interfaces"][0]["management"] = true;
    ret["interfaces"][1]["port"] = ssl_port;
    ret["interfaces"][1]["ipv4"] = "optional";
    ret["interfaces"][1]["ipv6"] = "optional";
    ret["interfaces"][1]["maxconn"] = Testapp::MAX_CONNECTIONS;
    ret["interfaces"][1]["backlog"] = Testapp::BACKLOG;
    ret["interfaces"][1]["host"] = "*";
    ret["interfaces"][1]["protocol"] = "memcached";
    ret["interfaces"][1]["management"] = true;
    ret["interfaces"][1]["ssl"]["key"] = pem_path;
    ret["interfaces"][1]["ssl"]["cert"] = cert_path;
    ret["datatype_json"] = true;
    ret["datatype_snappy"] = true;
    ret["xattr_enabled"] = true;
    ret["dedupe_nmvb_maps"] = false;
    ret["active_external_users_push_interval"] = "30 m";
    ret["error_maps_dir"] = get_errmaps_dir();
    ret["audit_file"] = mcd_env->getAuditFilename();
    ret["rbac_file"] = mcd_env->getRbacFilename();
    ret["ssl_cipher_list"] = "HIGH";
    ret["ssl_minimum_protocol"] = "tlsv1";
    ret["opcode_attributes_override"]["version"] = 1;
    ret["opcode_attributes_override"]["EWB_CTL"]["slow"] = 50;
    return ret;
}

nlohmann::json TestappTest::generate_config() {
    return generate_config(ssl_port);
}

void write_config_to_file(const std::string& config, const std::string& fname) {
    FILE *fp = fopen(fname.c_str(), "w");

    if (fp == nullptr) {
        throw std::system_error(errno,
                                std::system_category(),
                                "Failed to open file \"" + fname + "\"");
    } else {
        fprintf(fp, "%s", config.c_str());
        fclose(fp);
    }
}

/**
 * Load and parse the content of a file into a cJSON array
 *
 * @param file the name of the file
 * @return the decoded cJSON representation
 * @throw a string if something goes wrong
 */
static unique_cJSON_ptr loadJsonFile(const std::string& file) {
    unique_cJSON_ptr ret(cJSON_Parse(cb::io::loadFile(file).c_str()));
    if (ret.get() == nullptr) {
        throw std::logic_error("Failed to parse: " + file);
    }

    return ret;
}

void TestappTest::verify_server_running() {
    if (embedded_memcached_server) {
        // we don't monitor this thread...
        return ;
    }

    if (reinterpret_cast<pid_t>(-1) == server_pid) {
        std::cerr << "Server not running (server_pid == -1)" << std::endl;
        exit(EXIT_FAILURE);
    }

#ifdef WIN32
    DWORD status;
    if (!GetExitCodeProcess(server_pid, &status)) {
        std::cerr << "GetExitCodeProcess: failed: " << cb_strerror()
                  << std::endl;
        exit(EXIT_FAILURE);
    }
    if (status != STILL_ACTIVE) {
        std::cerr << "memcached process is not active: Exit code " << status
                  << std::endl;
        exit(EXIT_FAILURE);
    }
#else
    int status;
    pid_t ret = waitpid(server_pid, &status, WNOHANG);

    if (ret == static_cast<pid_t>(-1)) {
        std::cerr << "waitpid() failed with: " << strerror(errno) << std::endl;
        exit(EXIT_FAILURE);
    }

    if (server_pid == ret) {
        std::cerr << "waitpid status     : " << status << std::endl
                  << "WIFEXITED(status)  : " << WIFEXITED(status) << std::endl
                  << "WEXITSTATUS(status): " << WEXITSTATUS(status) << std::endl
                  << "WIFSIGNALED(status): " << WIFSIGNALED(status) << std::endl
                  << "WTERMSIG(status)   : " << WTERMSIG(status) << std::endl
                  << "WCOREDUMP(status)  : " << WCOREDUMP(status) << std::endl;
        exit(EXIT_FAILURE);
    }
#endif
}

void TestappTest::parse_portnumber_file(in_port_t& port_out,
                                        in_port_t& ssl_port_out) {
    FILE* fp;
    // I've seen that running under valgrind startup of the processes
    // might be slow, and getting even worse if the machine is under
    // load. Instead of having a "false timeout" just because the
    // server is slow, lets set the deadline to a high value so that
    // if we hit it we have a real problem and not just a loaded
    // server (rebuilding all of the source one more time is just
    // putting more load on the servers).
    using std::chrono::minutes;
    using std::chrono::seconds;
    const auto timeout = seconds(minutes(5)).count();
    const time_t deadline = time(NULL) + timeout;
    // Wait up to timeout seconds for the port file to be created.
    do {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        fp = fopen(portnumber_file.c_str(), "r");
        if (fp != nullptr) {
            break;
        }

        verify_server_running();
    } while (time(NULL) < deadline);

    ASSERT_NE(nullptr, fp) << "Timed out after " << timeout
                           << "s waiting for memcached port file '"
                           << portnumber_file << "' to be created.";
    fclose(fp);

    port_out = (in_port_t)-1;
    ssl_port_out = (in_port_t)-1;

    unique_cJSON_ptr portnumbers;
    portnumbers = loadJsonFile(portnumber_file);
    ASSERT_NE(nullptr, portnumbers);
    connectionMap.initialize(portnumbers.get());

    cJSON* array = cJSON_GetObjectItem(portnumbers.get(), "ports");
    ASSERT_NE(nullptr, array) << "ports not found in portnumber file";

    auto numEntries = cJSON_GetArraySize(array);
    for (int ii = 0; ii < numEntries; ++ii) {
        auto obj = cJSON_GetArrayItem(array, ii);

        auto protocol = cJSON_GetObjectItem(obj, "protocol");
        if (strcmp(protocol->valuestring, "memcached") != 0) {
            // the new test use the connectionmap
            continue;
        }

        auto family = cJSON_GetObjectItem(obj, "family");
        if (strcmp(family->valuestring, "AF_INET") != 0) {
            // For now we don't test IPv6
            continue;
        }
        auto ssl = cJSON_GetObjectItem(obj, "ssl");
        ASSERT_NE(nullptr, ssl);
        auto port = cJSON_GetObjectItem(obj, "port");
        ASSERT_NE(nullptr, port);

        in_port_t* out_port;
        if (ssl->type == cJSON_True) {
            out_port = &ssl_port_out;
        } else {
            out_port = &port_out;
        }
        *out_port = static_cast<in_port_t>(port->valueint);
    }

    EXPECT_EQ(0, remove(portnumber_file.c_str()));
}

extern "C" int memcached_main(int argc, char** argv);

extern "C" void memcached_server_thread_main(void *arg) {
    char *argv[4];
    int argc = 0;
    argv[argc++] = const_cast<char*>("./memcached");
    argv[argc++] = const_cast<char*>("-C");
    argv[argc++] = reinterpret_cast<char*>(arg);

    // Reset getopt()'s optind so memcached_main starts from the first
    // argument.
    optind = 1;

    memcached_main(argc, argv);
}

void TestappTest::spawn_embedded_server() {
    char *filename= mcd_port_filename_env + strlen("MEMCACHED_PORT_FILENAME=");
    snprintf(mcd_port_filename_env,
             sizeof(mcd_port_filename_env),
             "MEMCACHED_PORT_FILENAME=memcached_ports.%lu.%lu",
             (long)cb_getpid(),
             (unsigned long)time(NULL));
    remove(filename);
    portnumber_file.assign(filename);
    putenv(mcd_port_filename_env);

    ASSERT_EQ(0, cb_create_thread(&memcached_server_thread,
                                  memcached_server_thread_main,
                                  const_cast<char*>(config_file.c_str()),
                                  0));
}


void TestappTest::start_external_server() {
    char *filename= mcd_port_filename_env + strlen("MEMCACHED_PORT_FILENAME=");
    snprintf(mcd_parent_monitor_env,
             sizeof(mcd_parent_monitor_env),
             "MEMCACHED_PARENT_MONITOR=%lu",
             (unsigned long)cb_getpid());
    putenv(mcd_parent_monitor_env);

    snprintf(mcd_port_filename_env,
             sizeof(mcd_port_filename_env),
             "MEMCACHED_PORT_FILENAME=memcached_ports.%lu.%lu",
             (long)cb_getpid(),
             (unsigned long)time(NULL));
    remove(filename);
    portnumber_file.assign(filename);
    static char topkeys_env[] = "MEMCACHED_TOP_KEYS=10";
    putenv(topkeys_env);

#ifdef WIN32
    STARTUPINFO sinfo;
    PROCESS_INFORMATION pinfo;
    memset(&sinfo, 0, sizeof(sinfo));
    memset(&pinfo, 0, sizeof(pinfo));
    sinfo.cb = sizeof(sinfo);

    char commandline[1024];
    sprintf(commandline, "memcached.exe -C %s", config_file.c_str());

    putenv(mcd_port_filename_env);

    if (!CreateProcess("memcached.exe", commandline,
                       NULL, NULL, /*bInheritHandles*/FALSE,
                       0,
                       NULL, NULL, &sinfo, &pinfo)) {

        std::cerr << "Failed to start process: " << cb_strerror() << std::endl;
        exit(EXIT_FAILURE);
    }

    server_pid = pinfo.hProcess;
#else
    server_pid = fork();
    ASSERT_NE(reinterpret_cast<pid_t>(-1), server_pid);

    if (server_pid == 0) {
        /* Child */
        const char *argv[20];
        int arg = 0;
        putenv(mcd_port_filename_env);

        if (getenv("RUN_UNDER_VALGRIND") != nullptr) {
            argv[arg++] = "valgrind";
            argv[arg++] = "--log-file=valgrind.%p.log";
            argv[arg++] = "--leak-check=full";
    #if defined(__APPLE__)
            /* Needed to ensure debugging symbols are up-to-date. */
            argv[arg++] = "--dsymutil=yes";
    #endif
        }

        if (getenv("RUN_UNDER_PERF") != nullptr) {
            argv[arg++] = "perf";
            argv[arg++] = "record";
            argv[arg++] = "--call-graph";
            argv[arg++] = "dwarf";
        }

        argv[arg++] = "./memcached";
        argv[arg++] = "-C";
        argv[arg++] = config_file.c_str();

        argv[arg++] = nullptr;
        cb_assert(execvp(argv[0], const_cast<char **>(argv)) != -1);
    }
#endif // !WIN32
}

SOCKET create_connect_plain_socket(in_port_t port)
{
    auto sock = cb::net::new_socket("", port, AF_INET);
    if (sock == INVALID_SOCKET) {
        ADD_FAILURE() << "Failed to connect socket to 127.0.0.1:" << port;
    }
    return sock;
}

SOCKET connect_to_server_plain(in_port_t port) {
    return create_connect_plain_socket(port);
}

static SOCKET connect_to_server_ssl(in_port_t ssl_port) {
    SOCKET sock = create_connect_ssl_socket(ssl_port);
    if (sock == INVALID_SOCKET) {
        ADD_FAILURE() << "Failed to connect SSL socket to port" << ssl_port;
        return INVALID_SOCKET;
    }

    return sock;
}

/*
    re-connect to server.
    Uses global port and ssl_port values.
    New socket-fd written to global "sock" and "ssl_bio"
*/
void reconnect_to_server() {
    if (current_phase == phase_ssl) {
        cb::net::closesocket(sock_ssl);
        destroy_ssl_socket();

        sock_ssl = connect_to_server_ssl(ssl_port);
        ASSERT_NE(INVALID_SOCKET, sock_ssl);
    } else {
        cb::net::closesocket(sock);
        sock = connect_to_server_plain(port);
        ASSERT_NE(INVALID_SOCKET, sock);
    }
}

static void set_feature(cb::mcbp::Feature feature, bool enable) {
    // First update the currently enabled features.
    if (enable) {
        enabled_hello_features.insert(feature);
    } else {
        enabled_hello_features.erase(feature);
    }

    // Now send the new HELLO message to the server.
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;
    const char *useragent = "testapp";
    const size_t agentlen = strlen(useragent);

    // Calculate body location and populate the body.
    char* const body_ptr = buffer.bytes + sizeof(buffer.request.message.header);
    memcpy(body_ptr, useragent, agentlen);

    size_t bodylen = agentlen;
    for (auto feature : enabled_hello_features) {
        const uint16_t wire_feature = htons(uint16_t(feature));
        memcpy(body_ptr + bodylen,
               reinterpret_cast<const char*>(&wire_feature), sizeof(wire_feature));
        bodylen += sizeof(wire_feature);
    }

    // Fill in the header at the start of the buffer.
    memset(buffer.bytes, 0, sizeof(buffer.request.message.header));
    buffer.request.message.header.request.setMagic(
            cb::mcbp::Magic::ClientRequest);
    buffer.request.message.header.request.setOpcode(
            cb::mcbp::ClientOpcode::Hello);
    buffer.request.message.header.request.keylen = htons((uint16_t)agentlen);
    buffer.request.message.header.request.bodylen =
            htonl(gsl::narrow<uint32_t>(bodylen));

    safe_send(buffer.bytes,
              sizeof(buffer.request.message.header) + bodylen, false);

    safe_recv(&buffer.response, sizeof(buffer.response));
    const size_t response_bodylen =
            buffer.response.message.header.response.getBodylen();
    EXPECT_EQ(bodylen - agentlen, response_bodylen);
    for (auto feature : enabled_hello_features) {
        uint16_t wire_feature;
        safe_recv(&wire_feature, sizeof(wire_feature));
        wire_feature = ntohs(wire_feature);
        EXPECT_EQ(uint16_t(feature), wire_feature);
    }
}

void set_datatype_feature(bool enable) {
    set_feature(cb::mcbp::Feature::JSON, enable);
    set_feature(cb::mcbp::Feature::SNAPPY, enable);
}

std::pair<cb::mcbp::Status, std::string> fetch_value(const std::string& key) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } send, receive;
    const size_t len = mcbp_raw_command(send.bytes,
                                        sizeof(send.bytes),
                                        cb::mcbp::ClientOpcode::Get,
                                        key.data(),
                                        key.size(),
                                        NULL,
                                        0);
    safe_send(send.bytes, len, false);
    EXPECT_TRUE(safe_recv_packet(receive.bytes, sizeof(receive.bytes)));

    const auto status = receive.response.message.header.response.getStatus();
    if (status == cb::mcbp::Status::Success) {
        const char* ptr = receive.bytes + sizeof(receive.response) + 4;
        const size_t vallen =
                receive.response.message.header.response.getBodylen() - 4;
        return std::make_pair(cb::mcbp::Status::Success,
                              std::string(ptr, vallen));
    } else {
        return std::make_pair(status, "");
    }
}

void validate_object(const char *key, const std::string& expected_value) {
    union {
        protocol_binary_request_no_extras request;
        char bytes[1024];
    } send;
    size_t len = mcbp_raw_command(send.bytes,
                                  sizeof(send.bytes),
                                  cb::mcbp::ClientOpcode::Get,
                                  key,
                                  strlen(key),
                                  NULL,
                                  0);
    safe_send(send.bytes, len, false);

    std::vector<char> receive;
    safe_recv_packet(receive);

    auto* response = reinterpret_cast<protocol_binary_response_no_extras*>(receive.data());
    mcbp_validate_response_header(
            response, cb::mcbp::ClientOpcode::Get, cb::mcbp::Status::Success);
    char* ptr = receive.data() + sizeof(*response) + 4;
    if (response->message.header.response.getStatus() ==
        cb::mcbp::Status::Success) {
        size_t vallen = response->message.header.response.getBodylen() - 4;
        std::string actual(ptr, vallen);
        EXPECT_EQ(expected_value, actual);
    }
}

void validate_flags(const char *key, uint32_t expected_flags) {
    union {
        protocol_binary_request_no_extras request;
        char bytes[1024];
    } send;
    size_t len = mcbp_raw_command(send.bytes,
                                  sizeof(send.bytes),
                                  cb::mcbp::ClientOpcode::Get,
                                  key,
                                  strlen(key),
                                  NULL,
                                  0);
    safe_send(send.bytes, len, false);

    std::vector<char> receive(4096);
    safe_recv_packet(receive.data(), receive.size());

    auto* response = reinterpret_cast<protocol_binary_response_no_extras*>(receive.data());
    mcbp_validate_response_header(
            response, cb::mcbp::ClientOpcode::Get, cb::mcbp::Status::Success);

    auto extras = response->message.header.response.getExtdata();
    EXPECT_EQ(4, extras.size());
    uint32_t actual_flags =
            ntohl(*reinterpret_cast<const uint32_t*>(extras.data()));
    EXPECT_EQ(expected_flags, actual_flags);
}

void delete_object(const char* key, bool ignore_missing) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } send, receive;
    size_t len = mcbp_raw_command(send.bytes,
                                  sizeof(send.bytes),
                                  cb::mcbp::ClientOpcode::Delete,
                                  key,
                                  strlen(key),
                                  NULL,
                                  0);
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    if (ignore_missing &&
        receive.response.message.header.response.getStatus() ==
                cb::mcbp::Status::KeyEnoent) {
        /* Ignore. Just using this for cleanup then */
        return;
    }
    mcbp_validate_response_header(&receive.response,
                                  cb::mcbp::ClientOpcode::Delete,
                                  cb::mcbp::Status::Success);
}

void TestappTest::start_memcached_server() {
    config_file = cb::io::mktemp("memcached_testapp.json");
    write_config_to_file(memcached_cfg.dump(2), config_file);

    server_start_time = time(0);

    if (embedded_memcached_server) {
        spawn_embedded_server();
    } else {
        start_external_server();
    }
    parse_portnumber_file(port, ssl_port);
}

void store_object_w_datatype(const std::string& key,
                             cb::const_char_buffer value,
                             uint32_t flags,
                             uint32_t expiration,
                             cb::mcbp::Datatype datatype) {
    cb::mcbp::request::MutationPayload extras;
    static_assert(sizeof(extras) == 8, "Unexpected extras size");
    extras.setFlags(flags);
    extras.setExpiration(expiration);

    std::vector<uint8_t> buffer(sizeof(cb::mcbp::Request) + sizeof(extras) +
                                key.size() + value.size());
    cb::mcbp::FrameBuilder<cb::mcbp::Request> builder(
            {buffer.data(), buffer.size()});

    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::Set);
    builder.setDatatype(datatype);
    builder.setOpaque(0xdeadbeef);
    builder.setExtras(extras.getBuffer());
    builder.setKey({reinterpret_cast<const uint8_t*>(key.data()), key.size()});
    builder.setValue(
            {reinterpret_cast<const uint8_t*>(value.data()), value.size()});

    auto frame = builder.getFrame()->getFrame();

    safe_send(frame.data(), frame.size(), false);

    union {
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } receive;

    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(&receive.response,
                                  cb::mcbp::ClientOpcode::Set,
                                  cb::mcbp::Status::Success);
}

void store_document(const std::string& key,
                    const std::string& value,
                    uint32_t flags,
                    uint32_t exptime,
                    bool compress) {
    if (compress) {
        bool disable_snappy = false;
        if (enabled_hello_features.count(cb::mcbp::Feature::SNAPPY) == 0) {
            // We need to enable snappy
            set_feature(cb::mcbp::Feature::SNAPPY, true);
            disable_snappy = true;
        }
        cb::compression::Buffer deflated;
        cb::compression::deflate(
                cb::compression::Algorithm::Snappy, value, deflated);

        store_object_w_datatype(key.c_str(),
                                deflated,
                                flags,
                                exptime,
                                cb::mcbp::Datatype::Snappy);
        if (disable_snappy) {
            set_feature(cb::mcbp::Feature::SNAPPY, false);
        }
    } else {
        store_object_w_datatype(
                key.c_str(), value, flags, exptime, cb::mcbp::Datatype::Raw);
    }
}

void set_json_feature(bool enable) {
    set_feature(cb::mcbp::Feature::JSON, enable);
}

void set_mutation_seqno_feature(bool enable) {
    set_feature(cb::mcbp::Feature::MUTATION_SEQNO, enable);
}

void TestappTest::waitForShutdown(bool killed) {
#ifdef WIN32
    ASSERT_EQ(WAIT_OBJECT_0, WaitForSingleObject(server_pid, 60000));
    DWORD exit_code = NULL;
    GetExitCodeProcess(server_pid, &exit_code);
    EXPECT_EQ(0, exit_code);
#else
    int status;
    pid_t ret;
    while (true) {
        ret = waitpid(server_pid, &status, 0);
        if (ret == reinterpret_cast<pid_t>(-1) && errno == EINTR) {
            // Just loop again
            continue;
        }
        break;
    }
    ASSERT_NE(reinterpret_cast<pid_t>(-1), ret)
        << "waitpid failed: " << strerror(errno);
    bool correctShutdown = killed ? WIFSIGNALED(status) : WIFEXITED(status);
    EXPECT_TRUE(correctShutdown)
        << "waitpid status     : " << status << std::endl
        << "WIFEXITED(status)  : " << WIFEXITED(status) << std::endl
        << "WEXITSTATUS(status): " << WEXITSTATUS(status) << std::endl
        << "WIFSIGNALED(status): " << WIFSIGNALED(status) << std::endl
        << "WTERMSIG(status)   : " << WTERMSIG(status) << " ("
        << strsignal(WTERMSIG(status)) << ")" << std::endl
        << "WCOREDUMP(status)  : " << WCOREDUMP(status) << std::endl;
    EXPECT_EQ(0, WEXITSTATUS(status));
#endif
    server_pid = reinterpret_cast<pid_t>(-1);
}


void TestappTest::stop_memcached_server() {

    connectionMap.invalidate();
    if (sock != INVALID_SOCKET) {
        cb::net::closesocket(sock);
        sock = INVALID_SOCKET;
    }

    if (embedded_memcached_server) {
        shutdown_server();
        cb_join_thread(memcached_server_thread);
    }

    if (server_pid != reinterpret_cast<pid_t>(-1)) {
#ifdef WIN32
        TerminateProcess(server_pid, 0);
        waitForShutdown();
#else
        if (kill(server_pid, SIGTERM) == 0) {
            waitForShutdown();
        }
#endif
    }

    if (!config_file.empty()) {
        EXPECT_NE(-1, remove(config_file.c_str()));
        config_file.clear();
    }
}


ssize_t socket_send(SOCKET s, const char *buf, size_t len)
{
    return cb::net::send(s, buf, len, 0);
}

static ssize_t phase_send(const void *buf, size_t len) {
    if (current_phase == phase_ssl) {
        return phase_send_ssl(buf, len);
    } else {
        return socket_send(sock, reinterpret_cast<const char*>(buf), len);
    }
}

ssize_t socket_recv(SOCKET s, char *buf, size_t len)
{
    return cb::net::recv(s, buf, len, 0);
}

ssize_t phase_recv(void *buf, size_t len) {
    if (current_phase == phase_ssl) {
        return phase_recv_ssl(buf, len);
    } else {
        return socket_recv(sock, reinterpret_cast<char*>(buf), len);
    }
}

static const bool dump_socket_traffic =
        getenv("TESTAPP_PACKET_DUMP") != nullptr;

static const std::string phase_get_errno() {
    if (current_phase == phase_ssl) {
        /* could do with more work here, but so far this has sufficed */
        return "SSL error";
    }
    return cb_strerror();
}

void safe_send(const void* buf, size_t len, bool hickup)
{
    size_t offset = 0;
    const char* ptr = reinterpret_cast<const char*>(buf);
    do {
        size_t num_bytes = len - offset;
        ssize_t nw;
        if (hickup) {
            if (num_bytes > 1024) {
                num_bytes = (rand() % 1023) + 1;
            }
        }

        nw = phase_send(ptr + offset, num_bytes);

        if (nw == -1) {
            if (errno != EINTR) {
                fprintf(stderr,
                        "Failed to write: %s\n",
                        phase_get_errno().c_str());
                print_backtrace_to_file(stderr);
                abort();
            }
        } else {
            if (hickup) {
                std::this_thread::sleep_for(std::chrono::microseconds(100));
            }

            if (dump_socket_traffic) {
                if (current_phase == phase_ssl) {
                    std::cerr << "SSL";
                } else {
                    std::cerr << "PLAIN";
                }
                std::cerr << "> ";
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

bool safe_recv(void *buf, size_t len) {
    size_t offset = 0;
    if (len == 0) {
        return true;
    }
    do {

        ssize_t nr = phase_recv(((char*)buf) + offset, len - offset);

        if (nr == -1) {
            EXPECT_EQ(EINTR, errno) << "Failed to read: " << phase_get_errno();
        } else {
            if (nr == 0 && allow_closed_read) {
                return false;
            }
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
    info.resize(sizeof(protocol_binary_response_header));
    auto *header = reinterpret_cast<protocol_binary_response_header*>(info.data());

    if (!safe_recv(header, sizeof(*header))) {
        return false;
    }

    if (dump_socket_traffic) {
        if (current_phase == phase_ssl) {
            std::cerr << "SSL";
        } else {
            std::cerr << "PLAIN";
        }
        std::cerr << "< ";
        for (size_t ii = 0; ii < sizeof(*header); ++ii) {
            std::cerr << "0x" << std::hex << std::setfill('0') << std::setw(2)
                      << uint32_t(header->bytes[ii]) << ", ";
        }
    }

    auto bodylen = header->response.getBodylen();

    // Set response to NULL, because the underlying buffer may change.
    header = nullptr;

    info.resize(sizeof(*header) + bodylen);
    auto ret = safe_recv(info.data() + sizeof(*header), bodylen);

    if (dump_socket_traffic) {
        uint8_t* ptr = (uint8_t*)(info.data() + sizeof(*header));
        for (size_t ii = 0; ii < bodylen; ++ii) {
            std::cerr << "0x" << std::hex << std::setfill('0') << std::setw(2)
                      << uint32_t(ptr[ii]) << ", ";
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
    StaticBufInfo(void *buf_, size_t len_)
        : buf(reinterpret_cast<char*>(buf_)), len(len_) {
    }
    size_t size(size_t) const { return len; }
    char *data() { return buf; }

    void resize(size_t n) {
        if (n > len) {
            throw std::runtime_error("Cannot enlarge buffer!");
        }
    }

    char *buf;
    const size_t len;
};

bool safe_recv_packet(void *buf, size_t size) {
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
void TestappTest::ewouldblock_engine_configure(ENGINE_ERROR_CODE err_code,
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
    safe_send(buffer.data(), buffer.size(), false);

    buffer.resize(1024);
    safe_recv_packet(buffer.data(), buffer.size());
    mcbp_validate_response_header(
            reinterpret_cast<protocol_binary_response_no_extras*>(
                    buffer.data()),
            cb::mcbp::ClientOpcode::EwouldblockCtl,
            cb::mcbp::Status::Success);
}

void TestappTest::ewouldblock_engine_disable() {
    // Value for err_code doesn't matter...
    ewouldblock_engine_configure(ENGINE_EWOULDBLOCK, EWBEngineMode::Next_N, 0);
}

void TestappTest::reconfigure() {
    write_config_to_file(memcached_cfg.dump(2), config_file);
    auto& conn = getAdminConnection();

    BinprotGenericCommand req{cb::mcbp::ClientOpcode::ConfigReload, {}, {}};
    BinprotResponse resp;
    conn.executeCommand(req, resp);
    ASSERT_TRUE(resp.isSuccess()) << "Failed to reconfigure the server";
    conn.reconnect();
}

void TestappTest::runCreateXattr(const std::string& path,
                                 const std::string& value,
                                 bool macro,
                                 cb::mcbp::Status expectedStatus) {
    auto& connection = getConnection();

    BinprotSubdocCommand cmd;
    cmd.setOp(cb::mcbp::ClientOpcode::SubdocDictAdd);
    cmd.setKey(name);
    cmd.setPath(path);
    cmd.setValue(value);
    if (macro) {
        cmd.addPathFlags(SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_EXPAND_MACROS |
                         SUBDOC_FLAG_MKDIR_P);
    } else {
        cmd.addPathFlags(SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_MKDIR_P);
    }

    connection.sendCommand(cmd);

    BinprotResponse resp;
    connection.recvResponse(resp);
    EXPECT_EQ(expectedStatus, resp.getStatus());
}

void TestappTest::createXattr(const std::string& path,
                              const std::string& value,
                              bool macro) {
    runCreateXattr(path, value, macro, cb::mcbp::Status::Success);
}

BinprotSubdocResponse TestappTest::runGetXattr(
        const std::string& path,
        bool deleted,
        cb::mcbp::Status expectedStatus) {
    auto& connection = getConnection();

    BinprotSubdocCommand cmd;
    cmd.setOp(cb::mcbp::ClientOpcode::SubdocGet);
    cmd.setKey(name);
    cmd.setPath(path);
    if (deleted) {
        cmd.addPathFlags(SUBDOC_FLAG_XATTR_PATH);
        cmd.addDocFlags(mcbp::subdoc::doc_flag::AccessDeleted);
    } else {
        cmd.addPathFlags(SUBDOC_FLAG_XATTR_PATH);
    }
    connection.sendCommand(cmd);

    BinprotSubdocResponse resp;
    connection.recvResponse(resp);
    auto status = resp.getStatus();
    if (deleted && status == cb::mcbp::Status::SubdocSuccessDeleted) {
        status = cb::mcbp::Status::Success;
    }

    if (status != expectedStatus) {
        throw ConnectionError("runGetXattr() failed: ", resp);
    }
    return resp;
}

BinprotSubdocResponse TestappTest::getXattr(const std::string& path,
                                            bool deleted) {
    return runGetXattr(path, deleted, cb::mcbp::Status::Success);
}

int TestappTest::getResponseCount(cb::mcbp::Status statusCode) {
    unique_cJSON_ptr stats(cJSON_Parse(
            cJSON_GetObjectItem(
                    getConnection().stats("responses detailed").get(),
                    "responses")
                    ->valuestring));
    std::stringstream stream;
    stream << std::hex << uint16_t(statusCode);
    const auto *obj = cJSON_GetObjectItem(stats.get(), stream.str().c_str());
    if (obj == nullptr) {
        return 0;
    }

    return gsl::narrow<int>(obj->valueint);
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
    return prepare(connectionMap.getConnection(false, AF_INET));
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
             cb::mcbp::Feature::SELECT_BUCKET}};
    if (hasSnappySupport() == ClientSnappySupport::Yes) {
        features.push_back(cb::mcbp::Feature::SNAPPY);
    }

    if (hasJSONSupport() == ClientJSONSupport::Yes) {
        features.push_back(cb::mcbp::Feature::JSON);
    }

    connection.reconnect();
    connection.setFeatures("testapp", features);
    return connection;
}

nlohmann::json TestappTest::memcached_cfg;
std::string TestappTest::portnumber_file;
std::string TestappTest::config_file;
ConnectionMap TestappTest::connectionMap;
uint64_t TestappTest::token;
cb_thread_t TestappTest::memcached_server_thread;

int main(int argc, char **argv) {
    // We need to set MEMCACHED_UNIT_TESTS to enable the use of
    // the ewouldblock engine..
    static char envvar[80];
    snprintf(envvar, sizeof(envvar), "MEMCACHED_UNIT_TESTS=true");
    putenv(envvar);

    ::testing::InitGoogleTest(&argc, argv);

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


#ifdef __sun
    {
        // Use coreadm to set up a corefile pattern to ensure that the corefiles
        // created from the unit tests (of testapp or memcached) don't
        // overwrite each other
        std::string coreadm =
                "coreadm -p core.%%f.%%p " + std::to_string(cb_getpid());
        system(coreadm.c_str());
    }
#endif

    std::string engine_name("default");
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

    /*
     * If not running in embedded mode we need the McdEnvironment to manageSSL
     * initialization and shutdown.
     */
    mcd_env = new McdEnvironment(
            !embedded_memcached_server, engine_name, engine_config);

    ::testing::AddGlobalTestEnvironment(mcd_env);

    cb_initialize_sockets();

#if !defined(WIN32)
    /*
     * When shutting down SSL connections the SSL layer may attempt to
     * write to the underlying socket. If the socket has been closed
     * on the server side then this will raise a SIGPIPE (and
     * terminate the test program). This is Bad.
     * Therefore ignore SIGPIPE signals; we can use errno == EPIPE if
     * we need that information.
     */
    if (sigignore(SIGPIPE) == -1) {
        std::cerr << "Fatal: failed to ignore SIGPIPE; sigaction" << std::endl;
        return 1;
    }
#endif

    return RUN_ALL_TESTS();
}

/* Request stats
 * @return a map of stat key & values in the server response.
 */
stats_response_t request_stats() {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;
    stats_response_t result;

    size_t len = mcbp_raw_command(buffer.bytes,
                                  sizeof(buffer.bytes),
                                  cb::mcbp::ClientOpcode::Stat,
                                  NULL,
                                  0,
                                  NULL,
                                  0);

    safe_send(buffer.bytes, len, false);
    while (true) {
        safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
        mcbp_validate_response_header(&buffer.response,
                                      cb::mcbp::ClientOpcode::Stat,
                                      cb::mcbp::Status::Success);

        const char* key_ptr(
                buffer.bytes + sizeof(buffer.response) +
                buffer.response.message.header.response.getExtlen());
        const size_t key_len(
                buffer.response.message.header.response.getKeylen());

        // key length zero indicates end of the stats.
        if (key_len == 0) {
            break;
        }

        const char* val_ptr(key_ptr + key_len);
        const size_t val_len(
                buffer.response.message.header.response.getBodylen() - key_len -
                buffer.response.message.header.response.getExtlen());

        result.insert(std::make_pair(std::string(key_ptr, key_len),
                                     std::string(val_ptr, val_len)));
    }

    return result;
}

// Extracts a single statistic from the set of stats, returning as
// a uint64_t
uint64_t extract_single_stat(const stats_response_t& stats,
                                      const char* name) {
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

    uint8_t blob[sizeof(cb::mcbp::Request) + sizeof(payload)];
    cb::mcbp::FrameBuilder<cb::mcbp::Request> builder({blob, sizeof(blob)});
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::AdjustTimeofday);
    builder.setExtras(payload.getBuffer());
    builder.setOpaque(0xdeadbeef);

    auto request = builder.getFrame()->getFrame();
    safe_send(request.data(), request.size(), false);

    union {
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  cb::mcbp::ClientOpcode::AdjustTimeofday,
                                  cb::mcbp::Status::Success);
}
