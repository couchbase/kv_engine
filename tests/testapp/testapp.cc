/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <getopt.h>
#include <fcntl.h>
#include <ctype.h>
#include <time.h>
#include <evutil.h>
#include <snappy-c.h>
#include <gtest/gtest.h>

#include <atomic>
#include <algorithm>
#include <string>
#include <vector>

#include "testapp.h"
#include "testapp_subdoc.h"

#include <memcached/util.h>
#include <memcached/config_parser.h>
#include <cbsasl/cbsasl.h>
#include <platform/cb_malloc.h>
#include <platform/platform.h>
#include <fstream>
#include <platform/dirutils.h>
#include <platform/backtrace.h>
#include "memcached/openssl.h"
#include "utilities.h"
#include "utilities/protocol2text.h"
#include "daemon/topkeys.h"

#ifdef WIN32
#include <process.h>
#define getpid() _getpid()
#endif

McdEnvironment* mcd_env = nullptr;

#define CFG_FILE_PATTERN "memcached_testapp.json.XXXXXX"
using Testapp::MAX_CONNECTIONS;
using Testapp::BACKLOG;

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
static SSL_CTX *ssl_ctx = NULL;
static SSL *ssl = NULL;
static BIO *bio = NULL;
static BIO *ssl_bio_r = NULL;
static BIO *ssl_bio_w = NULL;

// used in embedded mode to shutdown memcached
extern void shutdown_server();

std::set<protocol_binary_hello_features_t> enabled_hello_features;

bool memcached_verbose = false;
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

static void destroy_ssl_socket();

void set_allow_closed_read(bool enabled) {
    allow_closed_read = enabled;
}

bool sock_is_ssl() {
    return current_phase == phase_ssl;
}

time_t get_server_start_time() {
    return server_start_time;
}

std::string to_string(const Transport& transport) {
#ifdef JETBRAINS_CLION_IDE
    return std::to_string(int(transport));
#else
    switch (transport) {
    case Transport::Plain:
        return "Plain";
    case Transport::SSL:
        return "SSL";
    case Transport::PlainIpv6:
        return "PlainIpv6";
    case Transport::SslIpv6:
        return "SslIpv6";
    }
    throw std::logic_error("Unknown transport");
#endif
}

std::ostream& operator << (std::ostream& os, const Transport& t)
{
    os << to_string(t);
    return os;
}

void TestappTest::CreateTestBucket()
{
    static const std::string BUCKET_NAME("default");
    static const std::string BUCKET_CONFIG(
            "default_engine.so;keep_deleted=true");

    auto& conn = connectionMap.getConnection(Protocol::Memcached, false);
    // Reconnect to the server so we know we're on a "fresh" connetion
    // to the server (and not one that might have been catched by the
    // idle-timer, but not yet noticed on the client side)
    conn.reconnect();
    conn.authenticate("_admin", "password", "PLAIN");

    conn.createBucket(BUCKET_NAME, BUCKET_CONFIG,
                      Greenstack::BucketType::EWouldBlock);

    // Reconnect the object to avoid others to reuse the admin creds
    conn.reconnect();
}

// Per-test-case set-up.
// Called before the first test in this test case.
void TestappTest::SetUpTestCase() {
    token = 0xdeadbeef;
    memcached_cfg.reset(generate_config(0));
    start_memcached_server(memcached_cfg.get());

    if (HasFailure()) {
        server_pid = reinterpret_cast<pid_t>(-1);
    } else {
        CreateTestBucket();
    }
}

// Per-test-case tear-down.
// Called after the last test in this test case.
void TestappTest::TearDownTestCase() {
    if (sock != INVALID_SOCKET) {
       closesocket(sock);
    }

    if (server_pid != reinterpret_cast<pid_t>(-1)) {
        current_phase = phase_plain;
        sock = connect_to_server_plain(port);
        ASSERT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, sasl_auth("_admin",
                                                              "password"));
        union {
            protocol_binary_request_delete_bucket request;
            protocol_binary_response_no_extras response;
            char bytes[1024];
        } buffer;

        size_t plen = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                       PROTOCOL_BINARY_CMD_DELETE_BUCKET,
                                       "default", strlen("default"),
                                       NULL, 0);

        safe_send(buffer.bytes, plen, false);
        safe_recv_packet(&buffer, sizeof(buffer));

        mcbp_validate_response_header(&buffer.response,
                                      PROTOCOL_BINARY_CMD_DELETE_BUCKET,
                                      PROTOCOL_BINARY_RESPONSE_SUCCESS);
    }
    stop_memcached_server();
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
}

// per test tear-down function.
void TestappTest::TearDown() {
    closesocket(sock);
}

// Per-test-case set-up.
// Called before the first test in this test case.
void McdTestappTest::SetUpTestCase() {
    memcached_cfg.reset(generate_config(0));

    start_memcached_server(memcached_cfg.get());

    if (HasFailure()) {
        server_pid = reinterpret_cast<pid_t>(-1);
    } else {
        CreateTestBucket();
    }
}
// per test setup function.
void McdTestappTest::SetUp() {
    verify_server_running();
    if (GetParam() == Transport::Plain) {
        current_phase = phase_plain;
        sock = connect_to_server_plain(port);
        ASSERT_NE(INVALID_SOCKET, sock);
    } else {
        current_phase = phase_ssl;
        sock_ssl = connect_to_server_ssl(ssl_port);
        ASSERT_NE(INVALID_SOCKET, sock_ssl);
    }

    // Set ewouldblock_engine test harness to default mode.
    ewouldblock_engine_configure(ENGINE_EWOULDBLOCK, EWBEngineMode::First,
                                 /*unused*/0);
}

// per test tear-down function.
void McdTestappTest::TearDown() {
    if (GetParam() == Transport::Plain) {
        closesocket(sock);
    } else {
        closesocket(sock_ssl);
        destroy_ssl_socket();
    }
}

#ifdef WIN32
static void log_network_error(const char* prefix) {
    LPVOID error_msg;
    DWORD err = WSAGetLastError();

    if (FormatMessage(FORMAT_MESSAGE_ALLOCATE_BUFFER |
                      FORMAT_MESSAGE_FROM_SYSTEM |
                      FORMAT_MESSAGE_IGNORE_INSERTS,
                      NULL, err, 0,
                      (LPTSTR)&error_msg, 0, NULL) != 0) {
        fprintf(stderr, prefix, error_msg);
        LocalFree(error_msg);
    } else {
        fprintf(stderr, prefix, "unknown error");
    }
}
#else
static void log_network_error(const char* prefix) {
    fprintf(stderr, prefix, strerror(errno));
}
#endif

std::string CERTIFICATE_PATH(const std::string& file) {
#ifdef WIN32
    return std::string("\\tests\\cert\\") + file;
#else
    return std::string("/tests/cert/") + file;
#endif
}
static std::string get_errmaps_dir() {
    std::string dir(SOURCE_ROOT);
    dir += "/etc/error_maps";
#ifdef WIN32
    std::replace(dir.begin(), dir.end(), '/', '\\');
#endif
    return dir;
}

cJSON* TestappTest::generate_config(uint16_t ssl_port)
{
    cJSON *root = cJSON_CreateObject();
    cJSON *array = cJSON_CreateArray();
    cJSON *obj = nullptr;
    cJSON *obj_ssl = nullptr;

    const std::string cwd = cb::io::getcwd();
    const std::string pem_path = cwd + CERTIFICATE_PATH("testapp.pem");
    const std::string cert_path = cwd + CERTIFICATE_PATH("testapp.cert");

    if (memcached_verbose) {
        cJSON_AddNumberToObject(root, "verbosity", 2);
    } else {
        obj = cJSON_CreateObject();
        cJSON_AddStringToObject(obj, "module", "blackhole_logger.so");
        cJSON_AddItemToArray(array, obj);
    }

    obj = cJSON_CreateObject();
    cJSON_AddStringToObject(obj, "module", "testapp_extension.so");
    cJSON_AddItemToArray(array, obj);

    cJSON_AddItemToObject(root, "extensions", array);

    // Build up the interface array
    array = cJSON_CreateArray();

    // One interface using the memcached binary protocol
    obj = cJSON_CreateObject();
    cJSON_AddNumberToObject(obj, "port", 0);
    cJSON_AddTrueToObject(obj, "ipv4");
    cJSON_AddTrueToObject(obj, "ipv6");
    cJSON_AddNumberToObject(obj, "maxconn", MAX_CONNECTIONS);
    cJSON_AddNumberToObject(obj, "backlog", BACKLOG);
    cJSON_AddStringToObject(obj, "host", "*");
    cJSON_AddStringToObject(obj, "protocol", "memcached");
    cJSON_AddTrueToObject(obj, "management");
    cJSON_AddItemToArray(array, obj);

    // One interface using the memcached binary protocol over SSL
    obj = cJSON_CreateObject();
    cJSON_AddNumberToObject(obj, "port", ssl_port);
    cJSON_AddNumberToObject(obj, "maxconn", MAX_CONNECTIONS);
    cJSON_AddNumberToObject(obj, "backlog", BACKLOG);
    cJSON_AddTrueToObject(obj, "ipv4");
    cJSON_AddTrueToObject(obj, "ipv6");
    cJSON_AddStringToObject(obj, "host", "*");
    cJSON_AddStringToObject(obj, "protocol", "memcached");
    obj_ssl = cJSON_CreateObject();
    cJSON_AddStringToObject(obj_ssl, "key", pem_path.c_str());
    cJSON_AddStringToObject(obj_ssl, "cert", cert_path.c_str());
    cJSON_AddItemToObject(obj, "ssl", obj_ssl);
    cJSON_AddItemToArray(array, obj);

#if 0
    // Disable the greenstack while we're refactoring to use bufferevents

    // One interface using the greenstack protocol
    obj = cJSON_CreateObject();
    cJSON_AddNumberToObject(obj, "port", 0);
    cJSON_AddNumberToObject(obj, "maxconn", MAX_CONNECTIONS);
    cJSON_AddNumberToObject(obj, "backlog", BACKLOG);
    cJSON_AddTrueToObject(obj, "ipv4");
    cJSON_AddTrueToObject(obj, "ipv6");
    cJSON_AddStringToObject(obj, "host", "*");
    cJSON_AddStringToObject(obj, "protocol", "greenstack");
    cJSON_AddItemToArray(array, obj);

    // One interface using the greenstack protocol over SSL
    obj = cJSON_CreateObject();
    cJSON_AddNumberToObject(obj, "port", 0);
    cJSON_AddNumberToObject(obj, "maxconn", MAX_CONNECTIONS);
    cJSON_AddNumberToObject(obj, "backlog", BACKLOG);
    cJSON_AddTrueToObject(obj, "ipv4");
    cJSON_AddTrueToObject(obj, "ipv6");
    cJSON_AddStringToObject(obj, "host", "*");
    cJSON_AddStringToObject(obj, "protocol", "greenstack");
    obj_ssl = cJSON_CreateObject();
    cJSON_AddStringToObject(obj_ssl, "key", pem_path.c_str());
    cJSON_AddStringToObject(obj_ssl, "cert", cert_path.c_str());
    cJSON_AddItemToObject(obj, "ssl", obj_ssl);
    cJSON_AddItemToArray(array, obj);
#endif

    cJSON_AddItemToObject(root, "interfaces", array);

    cJSON_AddTrueToObject(root, "datatype_support");
    cJSON_AddStringToObject(root, "audit_file",
                            mcd_env->getAuditFilename().c_str());
    cJSON_AddStringToObject(root, "error_maps_dir", get_errmaps_dir().c_str());
    cJSON_AddTrueToObject(root, "xattr_enabled");
    cJSON_AddStringToObject(root, "rbac_file",
                            mcd_env->getRbacFilename().c_str());

    return root;
}

cJSON* TestappTest::generate_config() {
    return generate_config(ssl_port);
}

int write_config_to_file(const std::string& config, const std::string& fname) {
    FILE *fp = fopen(fname.c_str(), "w");

    if (fp == nullptr) {
        return -1;
    } else {
        fprintf(fp, "%s", config.c_str());
        fclose(fp);
    }

    return 0;
}

/**
 * Load and parse the content of a file into a cJSON array
 *
 * @param file the name of the file
 * @return the decoded cJSON representation
 * @throw a string if something goes wrong
 */
unique_cJSON_ptr loadJsonFile(const std::string &file) {
    std::ifstream myfile(file, std::ios::in | std::ios::binary);
    if (!myfile.is_open()) {
        std::string msg("Failed to open file: ");
        msg.append(file);
        throw msg;
    }

    std::string str((std::istreambuf_iterator<char>(myfile)),
                    std::istreambuf_iterator<char>());

    myfile.close();
    unique_cJSON_ptr ret(cJSON_Parse(str.c_str()));
    if (ret.get() == nullptr) {
        std::string msg("Failed to parse file: ");
        msg.append(file);
        throw msg;
    }

    return ret;
}

void TestappTest::verify_server_running() {
    if (embedded_memcached_server) {
        // we don't monitor this thread...
        return ;
    }

    ASSERT_NE(reinterpret_cast<pid_t>(-1), server_pid);

#ifdef WIN32
    DWORD status;
    ASSERT_TRUE(GetExitCodeProcess(server_pid, &status));
    ASSERT_EQ(STILL_ACTIVE, status);
#else

    int status;
    pid_t ret = waitpid(server_pid, &status, WNOHANG);

    EXPECT_NE(static_cast<pid_t>(-1), ret)
                << "waitpid() failed with: " << strerror(errno);
    EXPECT_NE(server_pid, ret)
                << "waitpid status     : " << status << std::endl
                << "WIFEXITED(status)  : " << WIFEXITED(status) << std::endl
                << "WEXITSTATUS(status): " << WEXITSTATUS(status) << std::endl
                << "WIFSIGNALED(status): " << WIFSIGNALED(status) << std::endl
                << "WTERMSIG(status)   : " << WTERMSIG(status) << std::endl
                << "WCOREDUMP(status)  : " << WCOREDUMP(status) << std::endl;
    ASSERT_EQ(0, ret) << "The server isn't running..";
#endif
}

void TestappTest::parse_portnumber_file(in_port_t& port_out,
                                        in_port_t& ssl_port_out,
                                        int timeout) {
    FILE* fp;
    const time_t deadline = time(NULL) + timeout;
    // Wait up to timeout seconds for the port file to be created.
    do {
        usleep(50);
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
    ASSERT_NO_THROW(portnumbers = loadJsonFile(portnumber_file));
    ASSERT_NE(nullptr, portnumbers);
    ASSERT_NO_THROW(connectionMap.initialize(portnumbers.get()));

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

    memcached_main(argc, argv);


}

void TestappTest::spawn_embedded_server() {
    char *filename= mcd_port_filename_env + strlen("MEMCACHED_PORT_FILENAME=");
    snprintf(mcd_port_filename_env, sizeof(mcd_port_filename_env),
             "MEMCACHED_PORT_FILENAME=memcached_ports.%lu.%lu",
             (long)getpid(), (unsigned long)time(NULL));
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
    snprintf(mcd_parent_monitor_env, sizeof(mcd_parent_monitor_env),
             "MEMCACHED_PARENT_MONITOR=%lu", (unsigned long)getpid());
    putenv(mcd_parent_monitor_env);

    snprintf(mcd_port_filename_env, sizeof(mcd_port_filename_env),
             "MEMCACHED_PORT_FILENAME=memcached_ports.%lu.%lu",
             (long)getpid(), (unsigned long)time(NULL));
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
        LPVOID error_msg;
        DWORD err = GetLastError();

        if (FormatMessage(FORMAT_MESSAGE_ALLOCATE_BUFFER |
                          FORMAT_MESSAGE_FROM_SYSTEM |
                          FORMAT_MESSAGE_IGNORE_INSERTS,
                          NULL, err, 0,
                          (LPTSTR)&error_msg, 0, NULL) != 0) {
            fprintf(stderr, "Failed to start process: %s\n", error_msg);
            LocalFree(error_msg);
        } else {
            fprintf(stderr, "Failed to start process: unknown error\n");
        }
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

        if (getenv("RUN_UNDER_VALGRIND") != NULL) {
            argv[arg++] = "valgrind";
            argv[arg++] = "--log-file=valgrind.%p.log";
            argv[arg++] = "--leak-check=full";
    #if defined(__APPLE__)
            /* Needed to ensure debugging symbols are up-to-date. */
            argv[arg++] = "--dsymutil=yes";
    #endif
        }
        argv[arg++] = "./memcached";
        argv[arg++] = "-C";
        argv[arg++] = config_file.c_str();

        argv[arg++] = NULL;
        cb_assert(execvp(argv[0], const_cast<char **>(argv)) != -1);
    }
#endif // !WIN32
}

static struct addrinfo *lookuphost(const char *hostname, in_port_t port)
{
    struct addrinfo *ai = 0;
    struct addrinfo hints;
    char service[NI_MAXSERV];
    int error;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_protocol = IPPROTO_TCP;
    hints.ai_socktype = SOCK_STREAM;

    (void)snprintf(service, NI_MAXSERV, "%d", port);
    if ((error = getaddrinfo(hostname, service, &hints, &ai)) != 0) {
#ifdef WIN32
        log_network_error("getaddrinfo(): %s\r\n");
#else
       if (error != EAI_SYSTEM) {
          fprintf(stderr, "getaddrinfo(): %s\n", gai_strerror(error));
       } else {
          perror("getaddrinfo()");
       }
#endif
    }

    return ai;
}

SOCKET create_connect_plain_socket(in_port_t port)
{
    struct addrinfo *ai = lookuphost("127.0.0.1", port);
    SOCKET sock = INVALID_SOCKET;
    if (ai != NULL) {
       if ((sock = socket(ai->ai_family, ai->ai_socktype,
                          ai->ai_protocol)) != INVALID_SOCKET) {
          if (connect(sock, ai->ai_addr, (socklen_t)ai->ai_addrlen) == SOCKET_ERROR) {
             log_network_error("Failed to connect socket: %s\n");
             closesocket(sock);
             sock = INVALID_SOCKET;
          }
       } else {
          fprintf(stderr, "Failed to create socket: %s\n", strerror(errno));
       }

       freeaddrinfo(ai);
    }

    int nodelay_flag = 1;
#if defined(WIN32)
    char* ptr = reinterpret_cast<char*>(&nodelay_flag);
#else
    void* ptr = reinterpret_cast<void*>(&nodelay_flag);
#endif
    EXPECT_EQ(0, setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, ptr,
                            sizeof(nodelay_flag)));

    return sock;
}

static SOCKET create_connect_ssl_socket(in_port_t port) {
    char port_str[32];
    snprintf(port_str, 32, "%d", port);

    EXPECT_EQ(nullptr, ssl_ctx);
    EXPECT_EQ(nullptr, bio);
    EXPECT_EQ(0, create_ssl_connection(&ssl_ctx, &bio, "127.0.0.1", port_str,
                                       NULL, NULL, 1));

    /* SSL "trickery". To ensure we have full control over send/receive of data.
       create_ssl_connection will have negotiated the SSL connection, now:
       1. steal the underlying FD
       2. Switch out the BIO_ssl_connect BIO for a plain memory BIO

       Now send/receive is done under our control. byte by byte, large chunks etc...
    */
    int sfd = BIO_get_fd(bio, NULL);
    BIO_get_ssl(bio, &ssl);

    EXPECT_EQ(nullptr, ssl_bio_r);
    ssl_bio_r = BIO_new(BIO_s_mem());

    EXPECT_EQ(nullptr, ssl_bio_w);
    ssl_bio_w = BIO_new(BIO_s_mem());

    // Note: previous BIOs attached to 'bio' freed as a result of this call.
    SSL_set_bio(ssl, ssl_bio_r, ssl_bio_w);

    return sfd;
}

static void destroy_ssl_socket() {
    BIO_free_all(bio);
    bio = nullptr;
    ssl_bio_r = nullptr;
    ssl_bio_w = nullptr;

    SSL_CTX_free(ssl_ctx);
    ssl_ctx = nullptr;
}

SOCKET connect_to_server_plain(in_port_t port) {
    SOCKET sock = create_connect_plain_socket(port);
    if (sock == INVALID_SOCKET) {
        ADD_FAILURE() << "Failed to connect socket to port" << port;
        return INVALID_SOCKET;
    }

    return sock;
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
        closesocket(sock_ssl);
        destroy_ssl_socket();

        sock_ssl = connect_to_server_ssl(ssl_port);
        ASSERT_NE(INVALID_SOCKET, sock_ssl);
    } else {
        closesocket(sock);
        sock = connect_to_server_plain(port);
        ASSERT_NE(INVALID_SOCKET, sock);
    }
}

void TestappTest::start_memcached_server(cJSON* config) {

    char config_file_pattern [] = CFG_FILE_PATTERN;
    strncpy(config_file_pattern, CFG_FILE_PATTERN, sizeof(config_file_pattern));
    ASSERT_NE(cb_mktemp(config_file_pattern), nullptr);
    config_file = config_file_pattern;

    char* config_string = cJSON_Print(config);

    ASSERT_EQ(0, write_config_to_file(config_string, config_file.c_str()));
    cJSON_Free(config_string);

    // We need to set MEMCACHED_UNIT_TESTS to enable the use of
    // the ewouldblock engine..
    static char envvar[80];
    snprintf(envvar, sizeof(envvar), "MEMCACHED_UNIT_TESTS=true");
    putenv(envvar);

    server_start_time = time(0);

    if (embedded_memcached_server) {
        spawn_embedded_server();
    } else {
        start_external_server();
    }
    parse_portnumber_file(port, ssl_port, 30);
}


/**
 * Waits for server to shutdown.  It assumes that the server is
 * already in the process of being shutdown
 */
void TestappTest::waitForShutdown(void) {
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
    EXPECT_TRUE(WIFEXITED(status))
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
        closesocket(sock);
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


/**
 * Set the session control token in memcached (this token is used
 * to validate the shutdown command)
 */
void TestappTest::setControlToken(void) {
    std::vector<char> message(32);
    mcbp_raw_command(message.data(), message.size(),
                     PROTOCOL_BINARY_CMD_SET_CTRL_TOKEN,
                     nullptr, 0, nullptr, 0);
    char* ptr = reinterpret_cast<char*>(&token);
    memcpy(message.data() + 24, ptr, sizeof(token));
    safe_send(message.data(), message.size(), false);
    uint8_t buffer[1024];
    safe_recv_packet(buffer, sizeof(buffer));
    auto* rsp = reinterpret_cast<protocol_binary_response_no_extras*>(buffer);
    mcbp_validate_response_header(rsp, PROTOCOL_BINARY_CMD_SET_CTRL_TOKEN,
                                  PROTOCOL_BINARY_RESPONSE_SUCCESS);
}

/**
 * Send the shutdown message to the server and read the response
 * back and compare it with the expected result
 */
void TestappTest::sendShutdown(protocol_binary_response_status status) {
    // build the shutdown packet
    std::vector<char> packet(24);
    mcbp_raw_command(packet.data(), packet.size(),
                     PROTOCOL_BINARY_CMD_SHUTDOWN,
                     nullptr, 0, nullptr, 0);
    char* ptr = reinterpret_cast<char*>(&token);
    memcpy(packet.data() + 16, ptr, sizeof(token));
    safe_send(packet.data(), packet.size(), false);
    uint8_t buffer[1024];
    safe_recv_packet(buffer, sizeof(buffer));
    auto* rsp = reinterpret_cast<protocol_binary_response_no_extras*>(buffer);
    mcbp_validate_response_header(rsp, PROTOCOL_BINARY_CMD_SHUTDOWN, status);
}


static ssize_t socket_send(SOCKET s, const char *buf, size_t len)
{
#ifdef WIN32
    return send(s, buf, (int)len, 0);
#else
    return send(s, buf, len, 0);
#endif
}

static ssize_t phase_send(const void *buf, size_t len) {
    ssize_t rv = 0, send_rv = 0;
    if (current_phase == phase_ssl) {
        long send_len = 0;
        char *send_buf = NULL;
        /* push the data through SSL into the BIO */
        rv = (ssize_t)SSL_write(ssl, (const char*)buf, (int)len);
        send_len = BIO_get_mem_data(ssl_bio_w, &send_buf);

        send_rv = socket_send(sock_ssl, send_buf, send_len);

        if (send_rv > 0) {
            EXPECT_EQ(send_len, send_rv);
            (void)BIO_reset(ssl_bio_w);
        } else {
            /* flag failure to user */
            rv = send_rv;
        }
    } else {
        rv = socket_send(sock, reinterpret_cast<const char*>(buf), len);
    }
    return rv;
}

static ssize_t socket_recv(SOCKET s, char *buf, size_t len)
{
#ifdef WIN32
    return recv(s, buf, (int)len, 0);
#else
    return recv(s, buf, len, 0);
#endif
}

ssize_t phase_recv(void *buf, size_t len) {

    ssize_t rv = 0;
    if (current_phase == phase_ssl) {
        /* can we read some data? */
        while((rv = SSL_peek(ssl, buf, (int)len)) == -1)
        {
            /* nope, keep feeding SSL until we can */
            rv = socket_recv(sock_ssl, reinterpret_cast<char*>(buf), len);

            if(rv > 0) {
                /* write into the BIO what came off the network */
                BIO_write(ssl_bio_r, buf, rv);
            } else if(rv == 0) {
                return rv; /* peer closed */
            }
        }
        /* now pull the data out and return */
        rv = SSL_read(ssl, buf, (int)len);
    } else {
        rv = socket_recv(sock, reinterpret_cast<char*>(buf), len);
    }
    return rv;
}

char ssl_error_string[256];
int ssl_error_string_len = 256;

static char* phase_get_errno() {
    char * rv = 0;
    if (current_phase == phase_ssl) {
        /* could do with more work here, but so far this has sufficed */
        snprintf(ssl_error_string, ssl_error_string_len, "SSL error\n");
        rv = ssl_error_string;
    } else {
        rv = strerror(errno);
    }
    return rv;
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
                fprintf(stderr, "Failed to write: %s\n", phase_get_errno());
                print_backtrace_to_file(stderr);
                abort();
            }
        } else {
            if (hickup) {
#ifndef WIN32
                usleep(100);
#endif
            }
            offset += nw;
        }
    } while (offset < len);
}

void safe_send(const BinprotCommand& cmd, bool hickup) {
    std::vector<uint8_t> buf;
    cmd.encode(buf);
    safe_send(buf.data(), buf.size(), hickup);
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

    header->response.keylen = ntohs(header->response.keylen);
    header->response.status = ntohs(header->response.status);
    auto bodylen = header->response.bodylen = ntohl(header->response.bodylen);

    // Set response to NULL, because the underlying buffer may change.
    header = nullptr;

    info.resize(sizeof(*header) + bodylen);
    return safe_recv(info.data() + sizeof(*header), bodylen);
}

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

bool safe_recv_packet(BinprotResponse& resp) {
    resp.clear();

    std::vector<uint8_t> buf;
    if (!safe_recv_packet(buf)) {
        return false;
    }
    resp.assign(std::move(buf));
    return true;
}

bool safe_do_command(const BinprotCommand& cmd, BinprotResponse& resp, uint16_t status) {
    safe_send(cmd, false);
    if (!safe_recv_packet(resp)) {
        return false;
    }

    protocol_binary_response_no_extras mcresp;
    mcresp.message.header = const_cast<const BinprotResponse&>(resp).getHeader();
    mcbp_validate_response_header(&mcresp, cmd.getOp(), status);
    return !::testing::Test::HasFailure();
}

// Configues the ewouldblock_engine to use the given mode; value
// is a mode-specific parameter.
void TestappTest::ewouldblock_engine_configure(ENGINE_ERROR_CODE err_code,
                                               const EWBEngineMode& mode,
                                               uint32_t value) {
    union {
        request_ewouldblock_ctl request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;

    size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                  PROTOCOL_BINARY_CMD_EWOULDBLOCK_CTL,
                                  NULL, 0, NULL, 0);
    buffer.request.message.body.mode = htonl(static_cast<uint32_t>(mode));
    buffer.request.message.body.value = htonl(value);
    buffer.request.message.body.inject_error = htonl(err_code);

    safe_send(buffer.bytes, len, false);

    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_EWOULDBLOCK_CTL,
                                  PROTOCOL_BINARY_RESPONSE_SUCCESS);
}

void TestappTest::ewouldblock_engine_disable() {
    // Value for err_code doesn't matter...
    ewouldblock_engine_configure(ENGINE_EWOULDBLOCK, EWBEngineMode::Next_N, 0);
}

MemcachedConnection& TestappTest::getConnection() {
    return prepare(connectionMap.getConnection());
}

MemcachedConnection& TestappTest::getAdminConnection() {
    auto& conn = getConnection();
    conn.authenticate("_admin", "password", "PLAIN");
    return conn;
}

MemcachedConnection& TestappTest::prepare(MemcachedConnection& connection) {
    connection.reconnect();
    if (connection.getProtocol() == Protocol::Memcached) {
        auto& c = dynamic_cast<MemcachedBinprotConnection&>(connection);
        c.setDatatypeSupport(true);
        c.setMutationSeqnoSupport(true);
        c.setXerrorSupport(true);
        c.setXattrSupport(true);
    } else {
#ifdef ENABLE_GREENSTACK
        auto& c = dynamic_cast<MemcachedGreenstackConnection&>(connection);
    c.hello("memcached_testapp", "1,0", "BucketTest");
#else
        throw std::logic_error(
            "TestappClientTest::prepare: built without Greenstack support");
#endif
    }
    return connection;
}

INSTANTIATE_TEST_CASE_P(Transport,
                        McdTestappTest,
                        ::testing::Values(Transport::Plain, Transport::SSL),
                        ::testing::PrintToStringParamName());

unique_cJSON_ptr TestappTest::memcached_cfg;
std::string TestappTest::portnumber_file;
std::string TestappTest::config_file;
ConnectionMap TestappTest::connectionMap;
uint64_t TestappTest::token;
cb_thread_t TestappTest::memcached_server_thread;

int main(int argc, char **argv) {
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
            "coreadm -p core.%%f.%%p " + std::to_string(getpid());
        system(coreadm.c_str());
    }
#endif

    int cmd;
    while ((cmd = getopt(argc, argv, "ve")) != EOF) {
        switch (cmd) {
        case 'v':
            memcached_verbose = true;
            break;
        case 'e':
            embedded_memcached_server = true;
            break;
        default:
            std::cerr << "Usage: " << argv[0] << " [-v] [-i]" << std::endl
                      << std::endl
                      << "  -v Verbose - Print verbose memcached output "
                      << "to stderr.\n" << std::endl
                      << "  -e Embedded - Run the memcached daemon in the "
                      << "same process (for debugging only..)" << std::endl;
            return 1;
        }
    }
    /*
     * If not running in embedded mode we need the McdEnvironment to manageSSL
     * initialization and shutdown.
     */
    mcd_env = new McdEnvironment(!embedded_memcached_server);
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

    size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                  PROTOCOL_BINARY_CMD_STAT,
                                  NULL, 0, NULL, 0);

    safe_send(buffer.bytes, len, false);
    while (true) {
        safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
        mcbp_validate_response_header(&buffer.response,
                                      PROTOCOL_BINARY_CMD_STAT,
                                      PROTOCOL_BINARY_RESPONSE_SUCCESS);

        const char* key_ptr(buffer.bytes + sizeof(buffer.response) +
                            buffer.response.message.header.response.extlen);
        const size_t key_len(buffer.response.message.header.response.keylen);

        // key length zero indicates end of the stats.
        if (key_len == 0) {
            break;
        }

        const char* val_ptr(key_ptr + key_len);
        const size_t val_len(buffer.response.message.header.response.bodylen -
                             key_len -
                             buffer.response.message.header.response.extlen);
        EXPECT_GT(val_len, 0u);

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
