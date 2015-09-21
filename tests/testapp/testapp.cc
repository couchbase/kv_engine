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
#include "testapp_rbac.h"
#include "testapp_subdoc.h"

#include <memcached/util.h>
#include <memcached/config_parser.h>
#include <cbsasl/cbsasl.h>
#include "extensions/protocol/testapp_extension.h"
#include <platform/platform.h>
#include <fstream>
#include "memcached/openssl.h"
#include "programs/utilities.h"
#include "utilities/protocol2text.h"
#include "daemon/topkeys.h"

#ifdef WIN32
#include <process.h>
#define getpid() _getpid()
#endif

McdEnvironment* mcd_env = nullptr;

#define CFG_FILE_PATTERN "memcached_testapp.json.XXXXXX"
#define RBAC_FILE_PATTERN "testapp_rbac.json.XXXXXX"

#define MAX_CONNECTIONS 1000
#define BACKLOG 1024

/* test phases (bitmasks) */
#define phase_plain 0x2
#define phase_ssl 0x4

#define phase_max 4
static int current_phase = 0;

pid_t server_pid = pid_t(-1);
in_port_t port = -1;
static in_port_t ssl_port = -1;
SOCKET sock = INVALID_SOCKET;
static SOCKET sock_ssl;
static std::atomic<bool> allow_closed_read;
static time_t server_start_time = 0;
static SSL_CTX *ssl_ctx = NULL;
static SSL *ssl = NULL;
static BIO *bio = NULL;
static BIO *ssl_bio_r = NULL;
static BIO *ssl_bio_w = NULL;

std::set<protocol_binary_hello_features> enabled_hello_features;

bool memcached_verbose = false;

/* static storage for the different environment variables set by
 * putenv().
 *
 * (These must be static as putenv() essentially 'takes ownership' of
 * the provided array, so it is unsafe to use an automatic variable.
 * However, if we use the result of malloc() (i.e. the heap) then
 * memory leak checkers (e.g. Valgrind) will report the memory as
 * leaked as it's impossible to free it).
 */
static char mcd_parent_monitor_env[80];
static char mcd_port_filename_env[80];

static SOCKET connect_to_server_ssl(in_port_t ssl_port);

static void destroy_ssl_socket();

std::ostream& operator << (std::ostream& os, const Transport& t)
{
    switch (t) {
    case Transport::Plain:
        os << "Transport::Plain";
        break;
    case Transport::SSL:
        os << "Transport::SSL";
        break;
    }
    return os;
}

void TestappTest::CreateTestBucket()
{
    // We need to create the bucket
    int phase = current_phase;
    current_phase = phase_plain;

    sock = connect_to_server_plain(port);
    ASSERT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS, sasl_auth("_admin",
                                                          "password"));
    union {
        protocol_binary_request_create_bucket request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;

    char cfg[80];
    memset(cfg, 0, sizeof(cfg));
    snprintf(cfg, sizeof(cfg), "ewouldblock_engine.so%cdefault_engine.so",
             0);

    size_t plen = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                   PROTOCOL_BINARY_CMD_CREATE_BUCKET,
                                   "default", strlen("default"),
                                   cfg, sizeof(cfg));


    safe_send(buffer.bytes, plen, false);
    safe_recv_packet(&buffer, sizeof(buffer));

    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_CREATE_BUCKET,
                                  PROTOCOL_BINARY_RESPONSE_SUCCESS);

    closesocket(sock);
    sock = INVALID_SOCKET;

    current_phase = phase;
}

// Per-test-case set-up.
// Called before the first test in this test case.
void TestappTest::SetUpTestCase() {
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
    ASSERT_NE(reinterpret_cast<pid_t>(-1), server_pid);
    current_phase = phase_plain;
    sock = connect_to_server_plain(port);
    ASSERT_NE(INVALID_SOCKET, sock);

    // Set ewouldblock_engine test harness to default mode.
    ewouldblock_engine_configure(ENGINE_EWOULDBLOCK, EWBEngineMode_FIRST,
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
    ASSERT_NE(reinterpret_cast<pid_t>(-1), server_pid);
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
    ewouldblock_engine_configure(ENGINE_EWOULDBLOCK, EWBEngineMode_FIRST,
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

#ifdef WIN32
#define CERTIFICATE_PATH(file) ("\\tests\\cert\\"#file)
#else
#define CERTIFICATE_PATH(file) ("/tests/cert/"#file)
#endif

void get_working_current_directory(char* out_buf, int out_buf_len) {
    bool ok = false;
#ifdef WIN32
    ok = GetCurrentDirectory(out_buf_len, out_buf) != 0;
#else
    ok = getcwd(out_buf, out_buf_len) != NULL;
#endif
    /* memcached may throw a warning, but let's push through */
    if (!ok) {
        fprintf(stderr, "Failed to determine current working directory");
        strncpy(out_buf, ".", out_buf_len);
    }
}

cJSON* TestappTest::generate_config(uint16_t ssl_port)
{
    cJSON *root = cJSON_CreateObject();
    cJSON *array = cJSON_CreateArray();
    cJSON *obj = cJSON_CreateObject();
    cJSON *obj_ssl = NULL;
    char pem_path[256];
    char cert_path[256];
    char rbac_path[256];

    get_working_current_directory(pem_path, 256);
    strncpy(cert_path, pem_path, 256);
    snprintf(rbac_path, sizeof(rbac_path), "%s/%s", pem_path, mcd_env->getRbacFilename());
    strncat(pem_path, CERTIFICATE_PATH(testapp.pem), 256);
    strncat(cert_path, CERTIFICATE_PATH(testapp.cert), 256);

    cJSON_AddStringToObject(obj, "module", "ewouldblock_engine.so");
    cJSON_AddStringToObject(obj, "config", "default_engine.so");
    cJSON_AddItemToObject(root, "engine", obj);

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
    cJSON_AddNumberToObject(obj, "maxconn", MAX_CONNECTIONS);
    cJSON_AddNumberToObject(obj, "backlog", BACKLOG);
    cJSON_AddStringToObject(obj, "host", "*");
    cJSON_AddStringToObject(obj, "protocol", "memcached");
    cJSON_AddItemToArray(array, obj);

    // One interface using the memcached binary protocol over SSL
    obj = cJSON_CreateObject();
    cJSON_AddNumberToObject(obj, "port", ssl_port);
    cJSON_AddNumberToObject(obj, "maxconn", MAX_CONNECTIONS);
    cJSON_AddNumberToObject(obj, "backlog", BACKLOG);
    cJSON_AddStringToObject(obj, "host", "*");
    cJSON_AddStringToObject(obj, "protocol", "memcached");
    obj_ssl = cJSON_CreateObject();
    cJSON_AddStringToObject(obj_ssl, "key", pem_path);
    cJSON_AddStringToObject(obj_ssl, "cert", cert_path);
    cJSON_AddItemToObject(obj, "ssl", obj_ssl);
    cJSON_AddItemToArray(array, obj);

    cJSON_AddItemToObject(root, "interfaces", array);

    cJSON_AddStringToObject(root, "admin", "");
    cJSON_AddTrueToObject(root, "datatype_support");
    cJSON_AddStringToObject(root, "rbac_file", rbac_path);

    return root;
}

int write_config_to_file(const char* config, const char *fname) {
    FILE *fp;
    if ((fp = fopen(fname, "w")) == NULL) {
        return -1;
    } else {
        fprintf(fp, "%s", config);
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
static cJSON* loadJsonFile(const std::string &file) {
    std::ifstream myfile(file, std::ios::in | std::ios::binary);
    if (!myfile.is_open()) {
        std::string msg("Failed to open file: ");
        msg.append(file);
        throw msg;
    }

    std::string str((std::istreambuf_iterator<char>(myfile)),
                    std::istreambuf_iterator<char>());

    myfile.close();
    cJSON *obj = cJSON_Parse(str.c_str());
    if (obj == nullptr) {
        std::string msg("Failed to parse file: ");
        msg.append(file);
        throw msg;
    }

    return obj;
}

/**
 * Function to start the server and let it listen on a random port.
 * Set <code>server_pid</code> to the pid of the process
 *
 * @param port_out where to store the TCP port number the server is
 *                 listening on
 * @param daemon set to true if you want to run the memcached server
 *               as a daemon process
 * @param timeout Number of seconds to wait for server to start before
 *                giving up.
 */
void TestappTest::start_server(in_port_t *port_out, in_port_t *ssl_port_out,
                               bool daemon, int timeout)
{
    char *filename= mcd_port_filename_env + strlen("MEMCACHED_PORT_FILENAME=");
    snprintf(mcd_parent_monitor_env, sizeof(mcd_parent_monitor_env),
             "MEMCACHED_PARENT_MONITOR=%lu", (unsigned long)getpid());
    putenv(mcd_parent_monitor_env);

    snprintf(mcd_port_filename_env, sizeof(mcd_port_filename_env),
             "MEMCACHED_PORT_FILENAME=memcached_ports.%lu.%lu",
             (long)getpid(), (unsigned long)time(NULL));
    remove(filename);

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

    FILE* fp;
    const time_t deadline = time(NULL) + timeout;
    // Wait up to timeout seconds for the port file to be created.
    do {
        usleep(50);
        fp = fopen(filename, "r");
        if (fp != nullptr) {
            break;
        }

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
    } while (time(NULL) < deadline);

    ASSERT_NE(nullptr, fp) << "Timed out after " << timeout
                           << "s waiting for memcached port file '" << filename << "' to be created.";
    fclose(fp);

    *port_out = (in_port_t)-1;
    *ssl_port_out = (in_port_t)-1;

    cJSON* portnumbers;
    ASSERT_NO_THROW(portnumbers = loadJsonFile(filename));
    ASSERT_NE(nullptr, portnumbers);

    cJSON* array = cJSON_GetObjectItem(portnumbers, "ports");
    ASSERT_NE(nullptr, array) << "ports not found in portnumber file";

    auto numEntries = cJSON_GetArraySize(array);
    for (int ii = 0; ii < numEntries; ++ii) {
        auto obj = cJSON_GetArrayItem(array, ii);
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
            out_port = ssl_port_out;
        } else {
            out_port = port_out;
        }
        *out_port = static_cast<in_port_t>(port->valueint);
    }
    cJSON_Delete(portnumbers);
    EXPECT_EQ(0, remove(filename));
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

static SOCKET create_connect_plain_socket(in_port_t port)
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
static void reconnect_to_server() {
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
    start_server(&port, &ssl_port, false, 30);
}

void TestappTest::stop_memcached_server() {

    if (sock != INVALID_SOCKET) {
        closesocket(sock);
        sock = INVALID_SOCKET;
    }

    if (server_pid != reinterpret_cast<pid_t>(-1)) {
#ifdef WIN32
        TerminateProcess(server_pid, 0);
        WaitForSingleObject(server_pid, INFINITE);
        DWORD exit_code = NULL;
        GetExitCodeProcess(server_pid, &exit_code);
        EXPECT_EQ(0, exit_code);
#else
        if (kill(server_pid, SIGTERM) == 0) {
            /* Wait for the process to be gone... */
            int status;
            waitpid(server_pid, &status, 0);
            EXPECT_TRUE(WIFEXITED(status));
            EXPECT_EQ(0, WEXITSTATUS(status));
        }
#endif
        server_pid = pid_t(-1);
    }

    if (!config_file.empty()) {
        EXPECT_NE(-1, remove(config_file.c_str()));
        config_file.clear();
    }
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

static ssize_t phase_recv(void *buf, size_t len) {

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

static bool safe_recv(void *buf, size_t len) {
    size_t offset = 0;
    if (len == 0) {
        return true;
    }
    do {

        ssize_t nr = phase_recv(((char*)buf) + offset, len - offset);

        if (nr == -1) {
            if (errno != EINTR) {
                fprintf(stderr, "Failed to read: %s\n", phase_get_errno());
                abort();
            }
        } else {
            if (nr == 0 && allow_closed_read) {
                return false;
            }
            cb_assert(nr != 0);
            offset += nr;
        }
    } while (offset < len);

    return true;
}

bool safe_recv_packet(void *buf, size_t size) {
    protocol_binary_response_no_extras *response =
            reinterpret_cast<protocol_binary_response_no_extras*>(buf);
    size_t len;

    cb_assert(size >= sizeof(*response));
    if (!safe_recv(response, sizeof(*response))) {
        return false;
    }
    response->message.header.response.keylen = ntohs(response->message.header.response.keylen);
    response->message.header.response.status = ntohs(response->message.header.response.status);
    response->message.header.response.bodylen = ntohl(response->message.header.response.bodylen);

    len = sizeof(*response);
    char* ptr = reinterpret_cast<char*>(buf);
    ptr += len;
    cb_assert(size >= (sizeof(*response) + response->message.header.response.bodylen));
    if (!safe_recv(ptr, response->message.header.response.bodylen)) {
        return false;
    }

    return true;
}

size_t storage_command(char*buf,
                       size_t bufsz,
                       uint8_t cmd,
                       const void* key,
                       size_t keylen,
                       const void* dta,
                       size_t dtalen,
                       uint32_t flags,
                       uint32_t exp)
{
    /* all of the storage commands use the same command layout */
    size_t key_offset;
    protocol_binary_request_set *request =
        reinterpret_cast<protocol_binary_request_set*>(buf);
    cb_assert(bufsz >= sizeof(*request) + keylen + dtalen);

    memset(request, 0, sizeof(*request));
    request->message.header.request.magic = PROTOCOL_BINARY_REQ;
    request->message.header.request.opcode = cmd;
    request->message.header.request.keylen = htons((uint16_t)keylen);
    request->message.header.request.extlen = 8;
    request->message.header.request.bodylen = htonl((uint32_t)(keylen + 8 + dtalen));
    request->message.header.request.opaque = 0xdeadbeef;
    request->message.body.flags = htonl(flags);
    request->message.body.expiration = htonl(exp);

    key_offset = sizeof(protocol_binary_request_no_extras) + 8;

    memcpy(buf + key_offset, key, keylen);
    if (dta != NULL) {
        memcpy(buf + key_offset + keylen, dta, dtalen);
    }

    return key_offset + keylen + dtalen;
}

// Configues the ewouldblock_engine to use the given mode; value
// is a mode-specific parameter.
void TestappTest::ewouldblock_engine_configure(ENGINE_ERROR_CODE err_code,
                                                  EWBEngine_Mode mode,
                                                  uint32_t value) {
    union {
        request_ewouldblock_ctl request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;

    size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                  PROTOCOL_BINARY_CMD_EWOULDBLOCK_CTL,
                                  NULL, 0, NULL, 0);
    buffer.request.message.body.mode = htonl(mode);
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
    ewouldblock_engine_configure(ENGINE_EWOULDBLOCK, EWBEngineMode_NEXT_N, 0);
}

// Note: retained as a seperate function as other tests call this.
void test_noop(void) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;

    size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                  PROTOCOL_BINARY_CMD_NOOP,
                                  NULL, 0, NULL, 0);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response, PROTOCOL_BINARY_CMD_NOOP,
                                  PROTOCOL_BINARY_RESPONSE_SUCCESS);
}

TEST_P(McdTestappTest, Noop) {
    test_noop();
}

void test_quit_impl(uint8_t cmd) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;
    size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                  cmd, NULL, 0, NULL, 0);

    safe_send(buffer.bytes, len, false);
    if (cmd == PROTOCOL_BINARY_CMD_QUIT) {
        safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
        mcbp_validate_response_header(&buffer.response,
                                      PROTOCOL_BINARY_CMD_QUIT,
                                      PROTOCOL_BINARY_RESPONSE_SUCCESS);
    }

    /* Socket should be closed now, read should return 0 */
    EXPECT_EQ(0, phase_recv(buffer.bytes, sizeof(buffer.bytes)));

    reconnect_to_server();
}

TEST_P(McdTestappTest, Quit) {
    test_quit_impl(PROTOCOL_BINARY_CMD_QUIT);
}

TEST_P(McdTestappTest, QuitQ) {
    test_quit_impl(PROTOCOL_BINARY_CMD_QUITQ);
}

void test_set_impl(const char *key, uint8_t cmd) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } send, receive;
    uint64_t value = 0xdeadbeefdeadcafe;
    size_t len = storage_command(send.bytes, sizeof(send.bytes), cmd,
                                 key, strlen(key), &value, sizeof(value),
                                 0, 0);

    /* Set should work over and over again */
    int ii;
    for (ii = 0; ii < 10; ++ii) {
        safe_send(send.bytes, len, false);
        if (cmd == PROTOCOL_BINARY_CMD_SET) {
            safe_recv_packet(receive.bytes, sizeof(receive.bytes));
            mcbp_validate_response_header(&receive.response, cmd,
                                          PROTOCOL_BINARY_RESPONSE_SUCCESS);
        }
    }

    if (cmd == PROTOCOL_BINARY_CMD_SETQ) {
        return test_noop();
    }

    send.request.message.header.request.cas = receive.response.message.header.response.cas;
    safe_send(send.bytes, len, false);
    if (cmd == PROTOCOL_BINARY_CMD_SET) {
        safe_recv_packet(receive.bytes, sizeof(receive.bytes));
        mcbp_validate_response_header(&receive.response, cmd,
                                      PROTOCOL_BINARY_RESPONSE_SUCCESS);
        EXPECT_NE(receive.response.message.header.response.cas,
                  send.request.message.header.request.cas);
    } else {
        return test_noop();
    }
}

TEST_P(McdTestappTest, Set) {
    test_set_impl("test_set", PROTOCOL_BINARY_CMD_SET);
}

TEST_P(McdTestappTest, SetQ) {
    test_set_impl("test_setq", PROTOCOL_BINARY_CMD_SETQ);
}

static enum test_return test_add_impl(const char *key, uint8_t cmd) {
    uint64_t value = 0xdeadbeefdeadcafe;
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } send, receive;
    size_t len = storage_command(send.bytes, sizeof(send.bytes), cmd, key,
                                 strlen(key), &value, sizeof(value),
                                 0, 0);

    /* Add should only work the first time */
    int ii;
    for (ii = 0; ii < 10; ++ii) {
        safe_send(send.bytes, len, false);
        if (ii == 0) {
            if (cmd == PROTOCOL_BINARY_CMD_ADD) {
                safe_recv_packet(receive.bytes, sizeof(receive.bytes));
                mcbp_validate_response_header(&receive.response, cmd,
                                              PROTOCOL_BINARY_RESPONSE_SUCCESS);
            }
        } else {
            safe_recv_packet(receive.bytes, sizeof(receive.bytes));
            mcbp_validate_response_header(&receive.response, cmd,
                                          PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS);
        }
    }

    /* And verify that it doesn't work with the "correct" CAS */
    /* value */
    send.request.message.header.request.cas = receive.response.message.header.response.cas;
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(&receive.response, cmd,
                                  PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS);

    delete_object(key);

    return TEST_PASS;
}

TEST_P(McdTestappTest, Add) {
    test_add_impl("test_add", PROTOCOL_BINARY_CMD_ADD);
}

TEST_P(McdTestappTest, AddQ) {
    test_add_impl("test_addq", PROTOCOL_BINARY_CMD_ADDQ);
}

static enum test_return test_replace_impl(const char* key, uint8_t cmd) {
    uint64_t value = 0xdeadbeefdeadcafe;
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } send, receive;
    int ii;
    size_t len = storage_command(send.bytes, sizeof(send.bytes), cmd,
                                 key, strlen(key), &value, sizeof(value),
                                 0, 0);
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(&receive.response, cmd,
                                  PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);
    len = storage_command(send.bytes, sizeof(send.bytes),
                          PROTOCOL_BINARY_CMD_ADD,
                          key, strlen(key), &value, sizeof(value), 0, 0);
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(&receive.response, PROTOCOL_BINARY_CMD_ADD,
                                  PROTOCOL_BINARY_RESPONSE_SUCCESS);

    len = storage_command(send.bytes, sizeof(send.bytes), cmd,
                          key, strlen(key), &value, sizeof(value), 0, 0);
    for (ii = 0; ii < 10; ++ii) {
        safe_send(send.bytes, len, false);
        if (cmd == PROTOCOL_BINARY_CMD_REPLACE) {
            safe_recv_packet(receive.bytes, sizeof(receive.bytes));
            mcbp_validate_response_header(&receive.response,
                                          PROTOCOL_BINARY_CMD_REPLACE,
                                          PROTOCOL_BINARY_RESPONSE_SUCCESS);
        }
    }

    if (cmd == PROTOCOL_BINARY_CMD_REPLACEQ) {
        test_noop();
    }

    delete_object(key);

    return TEST_PASS;
}

TEST_P(McdTestappTest, Replace) {
    test_replace_impl("test_replace", PROTOCOL_BINARY_CMD_REPLACE);
}

TEST_P(McdTestappTest, ReplaceQ) {
    test_replace_impl("test_replaceq", PROTOCOL_BINARY_CMD_REPLACEQ);
}

static enum test_return test_delete_impl(const char *key, uint8_t cmd) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } send, receive;
    size_t len = mcbp_raw_command(send.bytes, sizeof(send.bytes), cmd,
                                  key, strlen(key), NULL, 0);

    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(&receive.response, cmd,
                                  PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);
    len = storage_command(send.bytes, sizeof(send.bytes),
                          PROTOCOL_BINARY_CMD_ADD,
                          key, strlen(key), NULL, 0, 0, 0);
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(&receive.response, PROTOCOL_BINARY_CMD_ADD,
                                  PROTOCOL_BINARY_RESPONSE_SUCCESS);

    len = mcbp_raw_command(send.bytes, sizeof(send.bytes),
                           cmd, key, strlen(key), NULL, 0);
    safe_send(send.bytes, len, false);

    if (cmd == PROTOCOL_BINARY_CMD_DELETE) {
        safe_recv_packet(receive.bytes, sizeof(receive.bytes));
        mcbp_validate_response_header(&receive.response,
                                      PROTOCOL_BINARY_CMD_DELETE,
                                      PROTOCOL_BINARY_RESPONSE_SUCCESS);
    }

    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(&receive.response, cmd,
                                  PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);

    return TEST_PASS;
}

TEST_P(McdTestappTest, Delete) {
    test_delete_impl("test_delete", PROTOCOL_BINARY_CMD_DELETE);
}

TEST_P(McdTestappTest, DeleteQ) {
    test_delete_impl("test_deleteq", PROTOCOL_BINARY_CMD_DELETEQ);
}

static enum test_return test_delete_cas_impl(const char *key, bool bad) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } send, receive;
    size_t len;
    len = storage_command(send.bytes, sizeof(send.bytes),
                          PROTOCOL_BINARY_CMD_SET,
                          key, strlen(key), NULL, 0, 0, 0);
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(&receive.response, PROTOCOL_BINARY_CMD_SET,
                                  PROTOCOL_BINARY_RESPONSE_SUCCESS);
    len = mcbp_raw_command(send.bytes, sizeof(send.bytes),
                           PROTOCOL_BINARY_CMD_DELETE, key, strlen(key), NULL,
                           0);

    send.request.message.header.request.cas = receive.response.message.header.response.cas;
    if (bad) {
        ++send.request.message.header.request.cas;
    }
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    if (bad) {
        mcbp_validate_response_header(&receive.response,
                                      PROTOCOL_BINARY_CMD_DELETE,
                                      PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS);
    } else {
        mcbp_validate_response_header(&receive.response,
                                      PROTOCOL_BINARY_CMD_DELETE,
                                      PROTOCOL_BINARY_RESPONSE_SUCCESS);
    }

    return TEST_PASS;
}


TEST_P(McdTestappTest, DeleteCAS) {
    test_delete_cas_impl("test_delete_cas", false);
}

TEST_P(McdTestappTest, DeleteBadCAS) {
    test_delete_cas_impl("test_delete_bad_cas", true);
}

TEST_P(McdTestappTest, DeleteMutationSeqno) {
    /* Enable mutation seqno support, then call the normal delete test. */
    set_mutation_seqno_feature(true);
    test_delete_impl("test_delete_mutation_seqno", PROTOCOL_BINARY_CMD_DELETE);
    set_mutation_seqno_feature(false);
}

static enum test_return test_get_impl(const char *key, uint8_t cmd) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } send, receive;
    int ii;
    size_t len = mcbp_raw_command(send.bytes, sizeof(send.bytes), cmd,
                                  key, strlen(key), NULL, 0);

    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(&receive.response, cmd,
                                  PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);

    len = storage_command(send.bytes, sizeof(send.bytes),
                          PROTOCOL_BINARY_CMD_ADD,
                          key, strlen(key), NULL, 0,
                          0, 0);
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(&receive.response, PROTOCOL_BINARY_CMD_ADD,
                                  PROTOCOL_BINARY_RESPONSE_SUCCESS);

    /* run a little pipeline test ;-) */
    len = 0;
    for (ii = 0; ii < 10; ++ii) {
        union {
            protocol_binary_request_no_extras request;
            char bytes[1024];
        } temp;
        size_t l = mcbp_raw_command(temp.bytes, sizeof(temp.bytes),
                                    cmd, key, strlen(key), NULL, 0);
        memcpy(send.bytes + len, temp.bytes, l);
        len += l;
    }

    safe_send(send.bytes, len, false);
    for (ii = 0; ii < 10; ++ii) {
        safe_recv_packet(receive.bytes, sizeof(receive.bytes));
        mcbp_validate_response_header(&receive.response, cmd,
                                      PROTOCOL_BINARY_RESPONSE_SUCCESS);
    }

    delete_object(key);
    return TEST_PASS;
}

TEST_P(McdTestappTest, Get) {
    test_get_impl("test_get", PROTOCOL_BINARY_CMD_GET);
}

TEST_P(McdTestappTest, GetK) {
    test_get_impl("test_getk", PROTOCOL_BINARY_CMD_GETK);
}

static enum test_return test_getq_impl(const char *key, uint8_t cmd) {
    const char *missing = "test_getq_missing";
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } send, temp, receive;
    size_t len = storage_command(send.bytes, sizeof(send.bytes),
                                 PROTOCOL_BINARY_CMD_ADD,
                                 key, strlen(key), NULL, 0,
                                 0, 0);
    size_t len2 = mcbp_raw_command(temp.bytes, sizeof(temp.bytes), cmd,
                                   missing, strlen(missing), NULL, 0);
    /* I need to change the first opaque so that I can separate the two
     * return packets */
    temp.request.message.header.request.opaque = 0xfeedface;
    memcpy(send.bytes + len, temp.bytes, len2);
    len += len2;

    len2 = mcbp_raw_command(temp.bytes, sizeof(temp.bytes), cmd,
                            key, strlen(key), NULL, 0);
    memcpy(send.bytes + len, temp.bytes, len2);
    len += len2;

    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(&receive.response, PROTOCOL_BINARY_CMD_ADD,
                                  PROTOCOL_BINARY_RESPONSE_SUCCESS);
    /* The first GETQ shouldn't return anything */
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(&receive.response, cmd,
                                  PROTOCOL_BINARY_RESPONSE_SUCCESS);

    delete_object(key);
    return TEST_PASS;
}

TEST_P(McdTestappTest, GetQ) {
    EXPECT_EQ(TEST_PASS,
              test_getq_impl("test_getq", PROTOCOL_BINARY_CMD_GETQ));
}

TEST_P(McdTestappTest, GetKQ) {
    EXPECT_EQ(TEST_PASS,
              test_getq_impl("test_getkq", PROTOCOL_BINARY_CMD_GETKQ));
}

static enum test_return test_incr_impl(const char* key, uint8_t cmd) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response_header;
        protocol_binary_response_incr response;
        char bytes[1024];
    } send, receive;
    size_t len = mcbp_arithmetic_command(send.bytes, sizeof(send.bytes), cmd,
                                         key, strlen(key), 1, 0, 0);

    int ii;
    for (ii = 0; ii < 10; ++ii) {
        safe_send(send.bytes, len, false);
        if (cmd == PROTOCOL_BINARY_CMD_INCREMENT) {
            safe_recv_packet(receive.bytes, sizeof(receive.bytes));
            mcbp_validate_response_header(&receive.response_header, cmd,
                                          PROTOCOL_BINARY_RESPONSE_SUCCESS);
            mcbp_validate_arithmetic(&receive.response, ii);
        }
    }

    if (cmd == PROTOCOL_BINARY_CMD_INCREMENTQ) {
        test_noop();
    }

    delete_object(key);
    return TEST_PASS;
}

TEST_P(McdTestappTest, Incr) {
    test_incr_impl("test_incr", PROTOCOL_BINARY_CMD_INCREMENT);
}

TEST_P(McdTestappTest, IncrQ) {
    test_incr_impl("test_incrq", PROTOCOL_BINARY_CMD_INCREMENTQ);
}

static enum test_return test_incr_invalid_cas_impl(const char* key, uint8_t cmd) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response_header;
        protocol_binary_response_incr response;
        char bytes[1024];
    } send, receive;
    size_t len = mcbp_arithmetic_command(send.bytes, sizeof(send.bytes), cmd,
                                         key, strlen(key), 1, 0, 0);

    send.request.message.header.request.cas = 5;
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(&receive.response_header, cmd,
                                  PROTOCOL_BINARY_RESPONSE_EINVAL);
    return TEST_PASS;
}

TEST_P(McdTestappTest, InvalidCASIncr) {
    test_incr_invalid_cas_impl("test_incr", PROTOCOL_BINARY_CMD_INCREMENT);
}

TEST_P(McdTestappTest, InvalidCASIncrQ) {
    test_incr_invalid_cas_impl("test_incrq", PROTOCOL_BINARY_CMD_INCREMENTQ);
}

TEST_P(McdTestappTest, InvalidCASDecr) {
    test_incr_invalid_cas_impl("test_decr", PROTOCOL_BINARY_CMD_DECREMENT);
}

TEST_P(McdTestappTest, InvalidCASDecrQ) {
    test_incr_invalid_cas_impl("test_decrq", PROTOCOL_BINARY_CMD_DECREMENTQ);
}

static enum test_return test_decr_impl(const char* key, uint8_t cmd) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response_header;
        protocol_binary_response_decr response;
        char bytes[1024];
    } send, receive;
    size_t len = mcbp_arithmetic_command(send.bytes, sizeof(send.bytes), cmd,
                                         key, strlen(key), 1, 9, 0);

    int ii;
    for (ii = 9; ii >= 0; --ii) {
        safe_send(send.bytes, len, false);
        if (cmd == PROTOCOL_BINARY_CMD_DECREMENT) {
            safe_recv_packet(receive.bytes, sizeof(receive.bytes));
            mcbp_validate_response_header(&receive.response_header, cmd,
                                          PROTOCOL_BINARY_RESPONSE_SUCCESS);
            mcbp_validate_arithmetic(&receive.response, ii);
        }
    }

    /* decr on 0 should not wrap */
    safe_send(send.bytes, len, false);
    if (cmd == PROTOCOL_BINARY_CMD_DECREMENT) {
        safe_recv_packet(receive.bytes, sizeof(receive.bytes));
        mcbp_validate_response_header(&receive.response_header, cmd,
                                      PROTOCOL_BINARY_RESPONSE_SUCCESS);
        mcbp_validate_arithmetic(&receive.response, 0);
    } else {
        test_noop();
    }

    delete_object(key);
    return TEST_PASS;
}

TEST_P(McdTestappTest, Decr) {
    test_decr_impl("test_decr",PROTOCOL_BINARY_CMD_DECREMENT);
}

TEST_P(McdTestappTest, DecrQ) {
    test_decr_impl("test_decrq", PROTOCOL_BINARY_CMD_DECREMENTQ);
}

TEST_P(McdTestappTest, IncrMutationSeqno) {
    /* Enable mutation seqno support, then call the normal incr test. */
    set_mutation_seqno_feature(true);
    test_incr_impl("test_incr_mutation_seqno", PROTOCOL_BINARY_CMD_INCREMENT);
    set_mutation_seqno_feature(false);
}

TEST_P(McdTestappTest, DecrMutationSeqno) {
    /* Enable mutation seqno support, then call the normal decr test. */
    set_mutation_seqno_feature(true);
    test_decr_impl("test_decr_mutation_seqno", PROTOCOL_BINARY_CMD_DECREMENT);
    set_mutation_seqno_feature(false);
}

TEST_P(McdTestappTest, Version) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;

    size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                  PROTOCOL_BINARY_CMD_VERSION,
                                  NULL, 0, NULL, 0);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response, PROTOCOL_BINARY_CMD_VERSION,
                                  PROTOCOL_BINARY_RESPONSE_SUCCESS);
}

static void test_flush_impl(const std::string &key, uint8_t cmd) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } send, receive;

    store_object(key.c_str(), "world");
    size_t len = mcbp_flush_command(send.bytes, sizeof(send.bytes), cmd, 0,
                                    false);
    safe_send(send.bytes, len, false);
    if (cmd == PROTOCOL_BINARY_CMD_FLUSH) {
        safe_recv_packet(receive.bytes, sizeof(receive.bytes));
        mcbp_validate_response_header(&receive.response, cmd,
                                      PROTOCOL_BINARY_RESPONSE_SUCCESS);
    }

    len = mcbp_raw_command(send.bytes, sizeof(send.bytes),
                           PROTOCOL_BINARY_CMD_GET, key.c_str(), key.length(),
                           NULL, 0);
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(&receive.response, PROTOCOL_BINARY_CMD_GET,
                                  PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);
}

TEST_P(McdTestappTest, Flush) {
    test_flush_impl("test_flush", PROTOCOL_BINARY_CMD_FLUSH);
}

TEST_P(McdTestappTest, FlushQ) {
    test_flush_impl("test_flushq", PROTOCOL_BINARY_CMD_FLUSHQ);
}

static void test_flush_with_extlen_impl(const std::string &key, uint8_t cmd) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } send, receive;

    store_object(key.c_str(), "world");
    size_t len = mcbp_flush_command(send.bytes, sizeof(send.bytes), cmd, 0,
                                    true);
    safe_send(send.bytes, len, false);
    if (cmd == PROTOCOL_BINARY_CMD_FLUSH) {
        safe_recv_packet(receive.bytes, sizeof(receive.bytes));
        mcbp_validate_response_header(&receive.response, cmd,
                                      PROTOCOL_BINARY_RESPONSE_SUCCESS);
    }

    len = mcbp_raw_command(send.bytes, sizeof(send.bytes),
                           PROTOCOL_BINARY_CMD_GET, key.c_str(), key.length(),
                           NULL, 0);
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(&receive.response, PROTOCOL_BINARY_CMD_GET,
                                  PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);
}

TEST_P(McdTestappTest, FlushWithExtlen) {
    test_flush_with_extlen_impl("test_flush_extlen", PROTOCOL_BINARY_CMD_FLUSH);
}

TEST_P(McdTestappTest, FlushQWithExtlen) {
    test_flush_with_extlen_impl("test_flushq_extlen", PROTOCOL_BINARY_CMD_FLUSHQ);
}

TEST_P(McdTestappTest, DelayedFlushNotSupported) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } send, receive;
    std::string key("DelayedFlushNotSupported");
    store_object(key.c_str(), "world");

    size_t len = mcbp_flush_command(send.bytes, sizeof(send.bytes),
                                    PROTOCOL_BINARY_CMD_FLUSH, 2, true);
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(&receive.response, PROTOCOL_BINARY_CMD_FLUSH,
                                  PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);

    len = mcbp_flush_command(send.bytes, sizeof(send.bytes),
                             PROTOCOL_BINARY_CMD_FLUSHQ, 2, true);
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(&receive.response, PROTOCOL_BINARY_CMD_FLUSHQ,
                                  PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);

    // Verify that the key is still there!
    len = mcbp_raw_command(send.bytes, sizeof(send.bytes),
                           PROTOCOL_BINARY_CMD_GET, key.c_str(), key.length(),
                           NULL, 0);
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(&receive.response, PROTOCOL_BINARY_CMD_GET,
                                  PROTOCOL_BINARY_RESPONSE_SUCCESS);

    delete_object(key.c_str());
}

TEST_P(McdTestappTest, CAS) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } send, receive;
    uint64_t value = 0xdeadbeefdeadcafe;
    size_t len = storage_command(send.bytes, sizeof(send.bytes),
                                 PROTOCOL_BINARY_CMD_SET,
                                 "FOO", 3, &value, sizeof(value), 0, 0);

    send.request.message.header.request.cas = 0x7ffffff;
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(&receive.response, PROTOCOL_BINARY_CMD_SET,
                                  PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);

    send.request.message.header.request.cas = 0x0;
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(&receive.response, PROTOCOL_BINARY_CMD_SET,
                                  PROTOCOL_BINARY_RESPONSE_SUCCESS);

    send.request.message.header.request.cas = receive.response.message.header.response.cas;
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(&receive.response, PROTOCOL_BINARY_CMD_SET,
                                  PROTOCOL_BINARY_RESPONSE_SUCCESS);

    send.request.message.header.request.cas = receive.response.message.header.response.cas - 1;
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(&receive.response, PROTOCOL_BINARY_CMD_SET,
                                  PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS);

    // Cleanup
    delete_object("FOO");
}

void test_concat_impl(const char *key, uint8_t cmd) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } send, receive;
    const char *value = "world";
    char *ptr;
    size_t len = mcbp_raw_command(send.bytes, sizeof(send.bytes), cmd,
                                  key, strlen(key), value, strlen(value));


    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(&receive.response, cmd,
                                  PROTOCOL_BINARY_RESPONSE_NOT_STORED);

    len = storage_command(send.bytes, sizeof(send.bytes),
                          PROTOCOL_BINARY_CMD_ADD,
                          key, strlen(key), value, strlen(value), 0, 0);
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(&receive.response, PROTOCOL_BINARY_CMD_ADD,
                                  PROTOCOL_BINARY_RESPONSE_SUCCESS);

    len = mcbp_raw_command(send.bytes, sizeof(send.bytes), cmd,
                           key, strlen(key), value, strlen(value));
    safe_send(send.bytes, len, false);

    if (cmd == PROTOCOL_BINARY_CMD_APPEND || cmd == PROTOCOL_BINARY_CMD_PREPEND) {
        safe_recv_packet(receive.bytes, sizeof(receive.bytes));
        mcbp_validate_response_header(&receive.response, cmd,
                                      PROTOCOL_BINARY_RESPONSE_SUCCESS);
    } else {
        len = mcbp_raw_command(send.bytes, sizeof(send.bytes),
                               PROTOCOL_BINARY_CMD_NOOP,
                               NULL, 0, NULL, 0);
        safe_send(send.bytes, len, false);
        safe_recv_packet(receive.bytes, sizeof(receive.bytes));
        mcbp_validate_response_header(&receive.response,
                                      PROTOCOL_BINARY_CMD_NOOP,
                                      PROTOCOL_BINARY_RESPONSE_SUCCESS);
    }

    len = mcbp_raw_command(send.bytes, sizeof(send.bytes),
                           PROTOCOL_BINARY_CMD_GETK,
                           key, strlen(key), NULL, 0);

    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(&receive.response, PROTOCOL_BINARY_CMD_GETK,
                                  PROTOCOL_BINARY_RESPONSE_SUCCESS);

    EXPECT_EQ(strlen(key), receive.response.message.header.response.keylen);
    EXPECT_EQ((strlen(key) + 2*strlen(value) + 4),
              receive.response.message.header.response.bodylen);

    ptr = receive.bytes;
    ptr += sizeof(receive.response);
    ptr += 4;

    EXPECT_EQ(0, memcmp(ptr, key, strlen(key)));
    ptr += strlen(key);
    EXPECT_EQ(0, memcmp(ptr, value, strlen(value)));
    ptr += strlen(value);
    EXPECT_EQ(0, memcmp(ptr, value, strlen(value)));

    // Cleanup
    delete_object(key);
}

TEST_P(McdTestappTest, Append) {
    test_concat_impl("test_append", PROTOCOL_BINARY_CMD_APPEND);
}

TEST_P(McdTestappTest, Prepend) {
    test_concat_impl("test_prepend", PROTOCOL_BINARY_CMD_PREPEND);
}

TEST_P(McdTestappTest, AppendQ) {
    test_concat_impl("test_appendq", PROTOCOL_BINARY_CMD_APPENDQ);
}

TEST_P(McdTestappTest, PrependQ) {
    test_concat_impl("test_prependq", PROTOCOL_BINARY_CMD_PREPENDQ);
}

TEST_P(McdTestappTest, Stat) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;

    size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                  PROTOCOL_BINARY_CMD_STAT,
                                  NULL, 0, NULL, 0);

    safe_send(buffer.bytes, len, false);
    do {
        safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
        mcbp_validate_response_header(&buffer.response,
                                      PROTOCOL_BINARY_CMD_STAT,
                                      PROTOCOL_BINARY_RESPONSE_SUCCESS);
    } while (buffer.response.message.header.response.keylen != 0);
}

TEST_P(McdTestappTest, StatConnections) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[2048];
    } buffer;

    size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                  PROTOCOL_BINARY_CMD_STAT,
                                  "connections", strlen("connections"), NULL, 0);

    safe_send(buffer.bytes, len, false);
    do {
        safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
        mcbp_validate_response_header(&buffer.response,
                                      PROTOCOL_BINARY_CMD_STAT,
                                      PROTOCOL_BINARY_RESPONSE_SUCCESS);
    } while (buffer.response.message.header.response.keylen != 0);
}

TEST_P(McdTestappTest, Scrub) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } send, recv;

    size_t len = mcbp_raw_command(send.bytes, sizeof(send.bytes),
                                  PROTOCOL_BINARY_CMD_SCRUB, NULL, 0, NULL, 0);

    // Retry if scrubber is already running.
    do {
        safe_send(send.bytes, len, false);
        safe_recv_packet(recv.bytes, sizeof(recv.bytes));
    } while (recv.response.message.header.response.status == PROTOCOL_BINARY_RESPONSE_EBUSY);

    mcbp_validate_response_header(&recv.response, PROTOCOL_BINARY_CMD_SCRUB,
                                  PROTOCOL_BINARY_RESPONSE_SUCCESS);
}

TEST_P(McdTestappTest, Roles) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;

    size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                  PROTOCOL_BINARY_CMD_ASSUME_ROLE,
                                  "unknownrole", 11, NULL, 0);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_ASSUME_ROLE,
                                  PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);


    /* assume the statistics role */
    len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                           PROTOCOL_BINARY_CMD_ASSUME_ROLE,
                           "statistics", 10, NULL, 0);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_ASSUME_ROLE,
                                  PROTOCOL_BINARY_RESPONSE_SUCCESS);

    /* At this point I should get an EACCESS if I tried to run NOOP */
    len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                           PROTOCOL_BINARY_CMD_NOOP,
                           NULL, 0, NULL, 0);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response, PROTOCOL_BINARY_CMD_NOOP,
                                  PROTOCOL_BINARY_RESPONSE_EACCESS);

    /* But I should be allowed to run a stat */
    len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                           PROTOCOL_BINARY_CMD_STAT,
                           NULL, 0, NULL, 0);

    safe_send(buffer.bytes, len, false);
    do {
        safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
        mcbp_validate_response_header(&buffer.response,
                                      PROTOCOL_BINARY_CMD_STAT,
                                      PROTOCOL_BINARY_RESPONSE_SUCCESS);
    } while (buffer.response.message.header.response.keylen != 0);

    /* Drop the role */
    len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                           PROTOCOL_BINARY_CMD_ASSUME_ROLE,
                           NULL, 0, NULL, 0);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_ASSUME_ROLE,
                                  PROTOCOL_BINARY_RESPONSE_SUCCESS);

    /* And noop should work again! */
    test_noop();
}

std::atomic<bool> hickup_thread_running;

static void binary_hickup_recv_verification_thread(void *arg) {
    protocol_binary_response_no_extras *response =
            reinterpret_cast<protocol_binary_response_no_extras*>(malloc(65*1024));
    if (response != NULL) {
        while (safe_recv_packet(response, 65*1024)) {
            /* Just validate the packet format */
            mcbp_validate_response_header(response,
                                          response->message.header.response.opcode,
                                          response->message.header.response.status);
        }
        free(response);
    }
    hickup_thread_running = false;
    allow_closed_read = false;
}

static enum test_return test_pipeline_hickup_chunk(void *buffer, size_t buffersize) {
    off_t offset = 0;
    char *key[256] = {0};
    uint64_t value = 0xfeedfacedeadbeef;

    while (hickup_thread_running &&
           offset + sizeof(protocol_binary_request_no_extras) < buffersize) {
        union {
            protocol_binary_request_no_extras request;
            char bytes[65 * 1024];
        } command;
        uint8_t cmd = (uint8_t)(rand() & 0xff);
        size_t len;
        size_t keylen = (rand() % 250) + 1;

        switch (cmd) {
        case PROTOCOL_BINARY_CMD_ADD:
        case PROTOCOL_BINARY_CMD_ADDQ:
        case PROTOCOL_BINARY_CMD_REPLACE:
        case PROTOCOL_BINARY_CMD_REPLACEQ:
        case PROTOCOL_BINARY_CMD_SET:
        case PROTOCOL_BINARY_CMD_SETQ:
            len = storage_command(command.bytes, sizeof(command.bytes), cmd,
                                  key, keylen , &value, sizeof(value),
                                  0, 0);
            break;
        case PROTOCOL_BINARY_CMD_APPEND:
        case PROTOCOL_BINARY_CMD_APPENDQ:
        case PROTOCOL_BINARY_CMD_PREPEND:
        case PROTOCOL_BINARY_CMD_PREPENDQ:
            len = mcbp_raw_command(command.bytes, sizeof(command.bytes), cmd,
                                   key, keylen, &value, sizeof(value));
            break;
        case PROTOCOL_BINARY_CMD_NOOP:
            len = mcbp_raw_command(command.bytes, sizeof(command.bytes), cmd,
                                   NULL, 0, NULL, 0);
            break;
        case PROTOCOL_BINARY_CMD_DELETE:
        case PROTOCOL_BINARY_CMD_DELETEQ:
            len = mcbp_raw_command(command.bytes, sizeof(command.bytes), cmd,
                                   key, keylen, NULL, 0);
            break;
        case PROTOCOL_BINARY_CMD_DECREMENT:
        case PROTOCOL_BINARY_CMD_DECREMENTQ:
        case PROTOCOL_BINARY_CMD_INCREMENT:
        case PROTOCOL_BINARY_CMD_INCREMENTQ:
            len = mcbp_arithmetic_command(command.bytes, sizeof(command.bytes),
                                          cmd,
                                          key, keylen, 1, 0, 0);
            break;
        case PROTOCOL_BINARY_CMD_VERSION:
            len = mcbp_raw_command(command.bytes, sizeof(command.bytes),
                                   PROTOCOL_BINARY_CMD_VERSION,
                                   NULL, 0, NULL, 0);
            break;
        case PROTOCOL_BINARY_CMD_GET:
        case PROTOCOL_BINARY_CMD_GETK:
        case PROTOCOL_BINARY_CMD_GETKQ:
        case PROTOCOL_BINARY_CMD_GETQ:
            len = mcbp_raw_command(command.bytes, sizeof(command.bytes), cmd,
                                   key, keylen, NULL, 0);
            break;

        case PROTOCOL_BINARY_CMD_STAT:
            len = mcbp_raw_command(command.bytes, sizeof(command.bytes),
                                   PROTOCOL_BINARY_CMD_STAT,
                                   NULL, 0, NULL, 0);
            break;

        default:
            /* don't run commands we don't know */
            continue;
        }

        if ((len + offset) < buffersize) {
            memcpy(((char*)buffer) + offset, command.bytes, len);
            offset += (off_t)len;
        } else {
            break;
        }
    }
    safe_send(buffer, offset, true);

    return TEST_PASS;
}

TEST_P(McdTestappTest, PipelineHickup)
{
    std::vector<char> buffer(65 * 1024);
    int ii;
    cb_thread_t tid;
    int ret;
    size_t len;

    allow_closed_read = true;
    hickup_thread_running = true;
    if ((ret = cb_create_thread(&tid, binary_hickup_recv_verification_thread,
                                NULL, 0)) != 0) {
        FAIL() << "Can't create thread: " << strerror(ret);
    }

    /* Allow the thread to start */
#ifdef WIN32
    Sleep(1);
#else
    usleep(250);
#endif

    for (ii = 0; ii < 2; ++ii) {
        test_pipeline_hickup_chunk(buffer.data(), buffer.size());
    }

    /* send quit to shut down the read thread ;-) */
    len = mcbp_raw_command(buffer.data(), buffer.size(),
                           PROTOCOL_BINARY_CMD_QUIT,
                           NULL, 0, NULL, 0);
    safe_send(buffer.data(), len, false);

    cb_join_thread(tid);

    reconnect_to_server();
}

TEST_P(McdTestappTest, IOCTL_Get) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;

    /* NULL key is invalid. */
    size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                  PROTOCOL_BINARY_CMD_IOCTL_GET, NULL, 0, NULL,
                                  0);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_IOCTL_GET,
                                  PROTOCOL_BINARY_RESPONSE_EINVAL);
}

TEST_P(McdTestappTest, IOCTL_Set) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;

    /* NULL key is invalid. */
    size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                  PROTOCOL_BINARY_CMD_IOCTL_SET, NULL, 0, NULL,
                                  0);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_IOCTL_SET,
                                  PROTOCOL_BINARY_RESPONSE_EINVAL);

    /* Very long (> IOCTL_KEY_LENGTH) is invalid. */
    {
        char long_key[128 + 1] = {0};
        len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                               PROTOCOL_BINARY_CMD_IOCTL_SET, long_key,
                               sizeof(long_key), NULL, 0);

        safe_send(buffer.bytes, len, false);
        safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
        mcbp_validate_response_header(&buffer.response,
                                      PROTOCOL_BINARY_CMD_IOCTL_SET,
                                      PROTOCOL_BINARY_RESPONSE_EINVAL);
    }

    /* release_free_memory always returns OK, regardless of how much was freed.*/
    {
        char cmd[] = "release_free_memory";
        len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                               PROTOCOL_BINARY_CMD_IOCTL_SET, cmd, strlen(cmd),
                               NULL, 0);

        safe_send(buffer.bytes, len, false);
        safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
        mcbp_validate_response_header(&buffer.response,
                                      PROTOCOL_BINARY_CMD_IOCTL_SET,
                                      PROTOCOL_BINARY_RESPONSE_SUCCESS);
    }
}

#if defined(HAVE_TCMALLOC)
TEST_P(McdTestappTest, IOCTL_TCMallocAggrDecommit) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;

    /* tcmalloc.aggressive_memory_decommit should return zero or one. */
    char cmd[] = "tcmalloc.aggressive_memory_decommit";
    size_t value;
    size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                  PROTOCOL_BINARY_CMD_IOCTL_GET, cmd,
                                  strlen(cmd),
                                  NULL, 0);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_IOCTL_GET,
                                  PROTOCOL_BINARY_RESPONSE_SUCCESS);

    EXPECT_GT(buffer.response.message.header.response.bodylen, 0);
    value = atoi(buffer.bytes + sizeof(buffer.response));
    cb_assert(value == 0 || value == 1);


    /* Check that tcmalloc.aggressive_memory_decommit can be changed, and that
       the value reads correctly. */
    {
        char value_buf[16];
        size_t new_value = 1 - value; /* flip between 1 <-> 0 */
        snprintf(value_buf, sizeof(value_buf), "%zd", new_value);

        len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                               PROTOCOL_BINARY_CMD_IOCTL_SET, cmd, strlen(cmd),
                               value_buf, strlen(value_buf));

        safe_send(buffer.bytes, len, false);
        safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
        mcbp_validate_response_header(&buffer.response,
                                      PROTOCOL_BINARY_CMD_IOCTL_SET,
                                      PROTOCOL_BINARY_RESPONSE_SUCCESS);
    }
}
#endif /* defined(HAVE_TCMALLOC) */

TEST_P(McdTestappTest, Config_ValidateCurrentConfig) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;

    /* identity config is valid. */
    char* config_string = cJSON_Print(memcached_cfg.get());
    size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                  PROTOCOL_BINARY_CMD_CONFIG_VALIDATE, NULL, 0,
                                  config_string, strlen(config_string));
    cJSON_Free(config_string);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_CONFIG_VALIDATE,
                                  PROTOCOL_BINARY_RESPONSE_SUCCESS);
}

TEST_P(McdTestappTest, Config_Validate_Empty) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;

    /* empty config is invalid */
    size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                  PROTOCOL_BINARY_CMD_CONFIG_VALIDATE, NULL, 0,
                                  NULL, 0);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_CONFIG_VALIDATE,
                                  PROTOCOL_BINARY_RESPONSE_EINVAL);
}

TEST_P(McdTestappTest, Config_ValidateInvalidJSON) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;

    /* non-JSON config is invalid */
    char non_json[] = "[something which isn't JSON]";
    size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                  PROTOCOL_BINARY_CMD_CONFIG_VALIDATE, NULL, 0,
                                  non_json, strlen(non_json));

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_CONFIG_VALIDATE,
                                  PROTOCOL_BINARY_RESPONSE_EINVAL);
}

TEST_P(McdTestappTest, Config_ValidateAdminNotDynamic) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;

    /* 'admin' cannot be changed */
    cJSON* dynamic = cJSON_CreateObject();
    char* dyn_string = NULL;
    cJSON_AddStringToObject(dynamic, "admin", "not_me");
    dyn_string = cJSON_Print(dynamic);
    cJSON_Delete(dynamic);
    size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                  PROTOCOL_BINARY_CMD_CONFIG_VALIDATE, NULL, 0,
                                  dyn_string, strlen(dyn_string));
    cJSON_Free(dyn_string);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_CONFIG_VALIDATE,
                                  PROTOCOL_BINARY_RESPONSE_EINVAL);
}

TEST_P(McdTestappTest, Config_ValidateThreadsNotDynamic) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;

    /* 'threads' cannot be changed */
    cJSON* dynamic = cJSON_CreateObject();
    char* dyn_string = NULL;
    cJSON_AddNumberToObject(dynamic, "threads", 99);
    dyn_string = cJSON_Print(dynamic);
    cJSON_Delete(dynamic);
    size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                  PROTOCOL_BINARY_CMD_CONFIG_VALIDATE, NULL, 0,
                                  dyn_string, strlen(dyn_string));
    cJSON_Free(dyn_string);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_CONFIG_VALIDATE,
                                  PROTOCOL_BINARY_RESPONSE_EINVAL);
}

TEST_P(McdTestappTest, Config_ValidateInterface) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;

    /* 'interfaces' - should be able to change max connections */
    cJSON* dynamic = generate_config(ssl_port);
    char* dyn_string = NULL;
    cJSON* iface_list = cJSON_GetObjectItem(dynamic, "interfaces");
    cJSON* iface = cJSON_GetArrayItem(iface_list, 0);
    cJSON_ReplaceItemInObject(iface, "maxconn",
                              cJSON_CreateNumber(MAX_CONNECTIONS * 2));
    dyn_string = cJSON_Print(dynamic);
    cJSON_Delete(dynamic);
    size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                  PROTOCOL_BINARY_CMD_CONFIG_VALIDATE, NULL, 0,
                                  dyn_string, strlen(dyn_string));
    cJSON_Free(dyn_string);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_CONFIG_VALIDATE,
                                  PROTOCOL_BINARY_RESPONSE_SUCCESS);
}

TEST_P(McdTestappTest, Config_Reload) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;

    if (GetParam() != Transport::Plain) {
        return;
    }

    /* reload identity config */
    {
        size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                      PROTOCOL_BINARY_CMD_CONFIG_RELOAD, NULL,
                                      0,
                                      NULL, 0);

        safe_send(buffer.bytes, len, false);
        safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
        mcbp_validate_response_header(&buffer.response,
                                      PROTOCOL_BINARY_CMD_CONFIG_RELOAD,
                                      PROTOCOL_BINARY_RESPONSE_SUCCESS);
    }

    /* Change max_conns on first interface. */
    {
        cJSON *dynamic = generate_config(ssl_port);
        char* dyn_string = NULL;
        cJSON *iface_list = cJSON_GetObjectItem(dynamic, "interfaces");
        cJSON *iface = cJSON_GetArrayItem(iface_list, 0);
        cJSON_ReplaceItemInObject(iface, "maxconn",
                                  cJSON_CreateNumber(MAX_CONNECTIONS * 2));
        dyn_string = cJSON_Print(dynamic);
        cJSON_Delete(dynamic);
        if (write_config_to_file(dyn_string, config_file.c_str()) == -1) {
            cJSON_Free(dyn_string);
            FAIL() << "Failed to write config to file";
        }
        cJSON_Free(dyn_string);

        size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                      PROTOCOL_BINARY_CMD_CONFIG_RELOAD, NULL,
                                      0,
                                      NULL, 0);

        safe_send(buffer.bytes, len, false);
        safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
        mcbp_validate_response_header(&buffer.response,
                                      PROTOCOL_BINARY_CMD_CONFIG_RELOAD,
                                      PROTOCOL_BINARY_RESPONSE_SUCCESS);
    }

    /* Change backlog on first interface. */
    {
        cJSON *dynamic = generate_config(ssl_port);
        char* dyn_string = NULL;
        cJSON *iface_list = cJSON_GetObjectItem(dynamic, "interfaces");
        cJSON *iface = cJSON_GetArrayItem(iface_list, 0);
        cJSON_ReplaceItemInObject(iface, "backlog",
                                  cJSON_CreateNumber(BACKLOG * 2));
        dyn_string = cJSON_Print(dynamic);
        cJSON_Delete(dynamic);
        if (write_config_to_file(dyn_string, config_file.c_str()) == -1) {
            cJSON_Free(dyn_string);
            FAIL() << "Failed to write config to file";
        }
        cJSON_Free(dyn_string);

        size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                      PROTOCOL_BINARY_CMD_CONFIG_RELOAD, NULL,
                                      0,
                                      NULL, 0);

        safe_send(buffer.bytes, len, false);
        safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
        mcbp_validate_response_header(&buffer.response,
                                      PROTOCOL_BINARY_CMD_CONFIG_RELOAD,
                                      PROTOCOL_BINARY_RESPONSE_SUCCESS);
    }

    /* Change tcp_nodelay on first interface. */
    {
        cJSON *dynamic = generate_config(ssl_port);
        char* dyn_string = NULL;
        cJSON *iface_list = cJSON_GetObjectItem(dynamic, "interfaces");
        cJSON *iface = cJSON_GetArrayItem(iface_list, 0);
        cJSON_AddFalseToObject(iface, "tcp_nodelay");
        dyn_string = cJSON_Print(dynamic);
        cJSON_Delete(dynamic);
        if (write_config_to_file(dyn_string, config_file.c_str()) == -1) {
            cJSON_Free(dyn_string);
            FAIL() << "Failed to write config to file";
        }
        cJSON_Free(dyn_string);

        size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                      PROTOCOL_BINARY_CMD_CONFIG_RELOAD, NULL,
                                      0,
                                      NULL, 0);

        safe_send(buffer.bytes, len, false);
        safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
        mcbp_validate_response_header(&buffer.response,
                                      PROTOCOL_BINARY_CMD_CONFIG_RELOAD,
                                      PROTOCOL_BINARY_RESPONSE_SUCCESS);
    }

    /* Check that invalid (corrupted) file is rejected (and we don't
       leak any memory in the process). */
    {
        cJSON *dynamic = generate_config(ssl_port);
        char* dyn_string = cJSON_Print(dynamic);
        cJSON_Delete(dynamic);
        // Corrupt the JSON by replacing first opening brace '{' with
        // a closing '}'.
        char* ptr = strchr(dyn_string, '{');
        ASSERT_NE(nullptr, ptr);
        *ptr = '}';
        if (write_config_to_file(dyn_string, config_file.c_str()) == -1) {
            cJSON_Free(dyn_string);
            FAIL() << "Failed to write config to file";
        }
        cJSON_Free(dyn_string);

        size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                      PROTOCOL_BINARY_CMD_CONFIG_RELOAD, NULL,
                                      0,
                                      NULL, 0);

        safe_send(buffer.bytes, len, false);
        safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
        mcbp_validate_response_header(&buffer.response,
                                      PROTOCOL_BINARY_CMD_CONFIG_RELOAD,
                                      PROTOCOL_BINARY_RESPONSE_SUCCESS);
    }

    /* Restore original configuration. */
    cJSON *dynamic = generate_config(ssl_port);
    char* dyn_string = cJSON_Print(dynamic);
    cJSON_Delete(dynamic);
    if (write_config_to_file(dyn_string, config_file.c_str()) == -1) {
        cJSON_Free(dyn_string);
        FAIL() << "Failed to write config to file";
    }
    cJSON_Free(dyn_string);

    size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                  PROTOCOL_BINARY_CMD_CONFIG_RELOAD, NULL, 0,
                                  NULL, 0);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_CONFIG_RELOAD,
                                  PROTOCOL_BINARY_RESPONSE_SUCCESS);
}

TEST_P(McdTestappTest, Config_Reload_SSL) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;

    if (GetParam() != Transport::SSL) {
        return;
    }

    /* Change ssl cert/key on second interface. */
    cJSON *dynamic = generate_config(ssl_port);
    char* dyn_string = NULL;
    cJSON *iface_list = cJSON_GetObjectItem(dynamic, "interfaces");
    cJSON *iface = cJSON_GetArrayItem(iface_list, 1);
    cJSON *ssl = cJSON_GetObjectItem(iface, "ssl");

    char pem_path[256];
    char cert_path[256];
    get_working_current_directory(pem_path, 256);
    strncpy(cert_path, pem_path, 256);
    strncat(pem_path, CERTIFICATE_PATH(testapp2.pem), 256);
    strncat(cert_path, CERTIFICATE_PATH(testapp2.cert), 256);

    cJSON_ReplaceItemInObject(ssl, "key", cJSON_CreateString(pem_path));
    cJSON_ReplaceItemInObject(ssl, "cert", cJSON_CreateString(cert_path));
    dyn_string = cJSON_Print(dynamic);
    cJSON_Delete(dynamic);
    if (write_config_to_file(dyn_string, config_file.c_str()) == -1) {
        cJSON_Free(dyn_string);
        FAIL() << "Failed to write config to file";
    }
    cJSON_Free(dyn_string);

    size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                  PROTOCOL_BINARY_CMD_CONFIG_RELOAD, NULL, 0,
                                  NULL, 0);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_CONFIG_RELOAD,
                                  PROTOCOL_BINARY_RESPONSE_SUCCESS);
}

TEST_P(McdTestappTest, Audit_Put) {
    union {
        protocol_binary_request_audit_put request;
        protocol_binary_response_audit_put response;
        char bytes[1024];
    }buffer;

    buffer.request.message.body.id = 0;

    size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                  PROTOCOL_BINARY_CMD_AUDIT_PUT, NULL, 0,
                                  "{}", 2);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_AUDIT_PUT,
                                  PROTOCOL_BINARY_RESPONSE_SUCCESS);
}

TEST_P(McdTestappTest, Audit_ConfigReload) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    }buffer;

    size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                  PROTOCOL_BINARY_CMD_AUDIT_CONFIG_RELOAD,
                                  NULL, 0, NULL, 0);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_AUDIT_CONFIG_RELOAD,
                                  PROTOCOL_BINARY_RESPONSE_SUCCESS);
}


TEST_P(McdTestappTest, Verbosity) {
    union {
        protocol_binary_request_verbosity request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;

    int ii;
    for (ii = 10; ii > -1; --ii) {
        size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                      PROTOCOL_BINARY_CMD_VERBOSITY,
                                      NULL, 0, NULL, 0);
        buffer.request.message.header.request.extlen = 4;
        buffer.request.message.header.request.bodylen = ntohl(4);
        buffer.request.message.body.level = (uint32_t)ntohl(ii);
        safe_send(buffer.bytes, len + sizeof(4), false);
        safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
        mcbp_validate_response_header(&buffer.response,
                                      PROTOCOL_BINARY_CMD_VERBOSITY,
                                      PROTOCOL_BINARY_RESPONSE_SUCCESS);
    }
}

std::pair<protocol_binary_response_status, std::string>
fetch_value(const std::string& key) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } send, receive;
    const size_t len = mcbp_raw_command(send.bytes, sizeof(send.bytes),
                                        PROTOCOL_BINARY_CMD_GET,
                                        key.data(), key.size(), NULL, 0);
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));

    protocol_binary_response_status status =
            protocol_binary_response_status(receive.response.message.header.response.status);
    if (status == PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        const char* ptr = receive.bytes + sizeof(receive.response) + 4;
        const size_t vallen = receive.response.message.header.response.bodylen - 4;
        return std::make_pair(PROTOCOL_BINARY_RESPONSE_SUCCESS,
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
    size_t len = mcbp_raw_command(send.bytes, sizeof(send.bytes),
                                  PROTOCOL_BINARY_CMD_GET,
                                  key, strlen(key), NULL, 0);
    safe_send(send.bytes, len, false);

    std::vector<char> receive;
    receive.resize(sizeof(protocol_binary_response_get) + expected_value.size());

    safe_recv_packet(receive.data(), receive.size());

    auto* response = reinterpret_cast<protocol_binary_response_no_extras*>(receive.data());
    mcbp_validate_response_header(response, PROTOCOL_BINARY_CMD_GET,
                                  PROTOCOL_BINARY_RESPONSE_SUCCESS);
    char* ptr = receive.data() + sizeof(*response) + 4;
    size_t vallen = response->message.header.response.bodylen - 4;
    std::string actual(ptr, vallen);
    EXPECT_EQ(expected_value, actual);
}

void store_object(const char *key, const char *value, bool validate) {
    std::vector<char> send;
    send.resize(sizeof(protocol_binary_request_set) + strlen(key) +
                strlen(value));

    size_t len = storage_command(send.data(), send.size(),
                                 PROTOCOL_BINARY_CMD_SET,
                                 key, strlen(key), value, strlen(value),
                                 0, 0);

    safe_send(send.data(), len, false);

    union {
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } receive;
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(&receive.response, PROTOCOL_BINARY_CMD_SET,
                                  PROTOCOL_BINARY_RESPONSE_SUCCESS);

    if (validate) {
        validate_object(key, value);
    }
}

void delete_object(const char* key) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } send, receive;
    size_t len = mcbp_raw_command(send.bytes, sizeof(send.bytes),
                                  PROTOCOL_BINARY_CMD_DELETE, key, strlen(key),
                                  NULL, 0);
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(&receive.response, PROTOCOL_BINARY_CMD_DELETE,
                                  PROTOCOL_BINARY_RESPONSE_SUCCESS);
}

TEST_P(McdTestappTest, Hello) {
    union {
        protocol_binary_request_hello request;
        protocol_binary_response_hello response;
        char bytes[1024];
    } buffer;
    const char *useragent = "hello world";
    uint16_t features[3];
    uint16_t *ptr;
    size_t len;

    features[0] = htons(PROTOCOL_BINARY_FEATURE_DATATYPE);
    features[1] = htons(PROTOCOL_BINARY_FEATURE_TCPNODELAY);
    features[2] = htons(PROTOCOL_BINARY_FEATURE_MUTATION_SEQNO);

    memset(buffer.bytes, 0, sizeof(buffer.bytes));

    len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                           PROTOCOL_BINARY_CMD_HELLO,
                           useragent, strlen(useragent), features,
                           sizeof(features));

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_HELLO,
                                  PROTOCOL_BINARY_RESPONSE_SUCCESS);

    EXPECT_EQ(6u, buffer.response.message.header.response.bodylen);
    ptr = (uint16_t*)(buffer.bytes + sizeof(buffer.response));
    EXPECT_EQ(PROTOCOL_BINARY_FEATURE_DATATYPE, ntohs(*ptr));
    ptr++;
    EXPECT_EQ(PROTOCOL_BINARY_FEATURE_TCPNODELAY, ntohs(*ptr));
    ptr++;
    EXPECT_EQ(PROTOCOL_BINARY_FEATURE_MUTATION_SEQNO, ntohs(*ptr));

    features[0] = 0xffff;
    len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                           PROTOCOL_BINARY_CMD_HELLO,
                           useragent, strlen(useragent), features,
                           2);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_HELLO,
                                  PROTOCOL_BINARY_RESPONSE_SUCCESS);
    EXPECT_EQ(0u, buffer.response.message.header.response.bodylen);

    len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                           PROTOCOL_BINARY_CMD_HELLO,
                           useragent, strlen(useragent), features,
                           sizeof(features) - 1);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_HELLO,
                                  PROTOCOL_BINARY_RESPONSE_EINVAL);
}

static void set_feature(const protocol_binary_hello_features feature,
                        bool enable) {

    // First update the currently enabled features.
    if (enable) {
        enabled_hello_features.insert(feature);
    } else {
        enabled_hello_features.erase(feature);
    }

    // Now send the new HELLO message to the server.
    union {
        protocol_binary_request_hello request;
        protocol_binary_response_hello response;
        char bytes[1024];
    } buffer;
    const char *useragent = "testapp";
    const size_t agentlen = strlen(useragent);

    // Calculate body location and populate the body.
    char* const body_ptr = buffer.bytes + sizeof(buffer.request.message.header);
    memcpy(body_ptr, useragent, agentlen);

    size_t bodylen = agentlen;
    for (auto feature : enabled_hello_features) {
        const uint16_t wire_feature = htons(feature);
        memcpy(body_ptr + bodylen,
               reinterpret_cast<const char*>(&wire_feature), sizeof(wire_feature));
        bodylen += sizeof(wire_feature);
    }

    // Fill in the header at the start of the buffer.
    memset(buffer.bytes, 0, sizeof(buffer.request.message.header));
    buffer.request.message.header.request.magic = PROTOCOL_BINARY_REQ;
    buffer.request.message.header.request.opcode = PROTOCOL_BINARY_CMD_HELLO;
    buffer.request.message.header.request.keylen = htons((uint16_t)agentlen);
    buffer.request.message.header.request.bodylen = htonl(bodylen);

    safe_send(buffer.bytes,
              sizeof(buffer.request.message.header) + bodylen, false);

    safe_recv(&buffer.response, sizeof(buffer.response));
    const size_t response_bodylen = ntohl(buffer.response.message.header.response.bodylen);
    EXPECT_EQ(bodylen - agentlen, response_bodylen);
    for (auto feature : enabled_hello_features) {
        uint16_t wire_feature;
        safe_recv(&wire_feature, sizeof(wire_feature));
        wire_feature = ntohs(wire_feature);
        EXPECT_EQ(feature, wire_feature);
    }
}

void set_datatype_feature(bool enable) {
    set_feature(PROTOCOL_BINARY_FEATURE_DATATYPE, enable);
}

void set_mutation_seqno_feature(bool enable) {
    set_feature(PROTOCOL_BINARY_FEATURE_MUTATION_SEQNO, enable);
}

enum test_return store_object_w_datatype(const char *key,
                                         const void *data, size_t datalen,
                                         bool deflate, bool json)
{
    protocol_binary_request_no_extras request;
    int keylen = (int)strlen(key);
    char extra[8] = { 0 };
    uint8_t datatype = PROTOCOL_BINARY_RAW_BYTES;
    if (deflate) {
        datatype |= PROTOCOL_BINARY_DATATYPE_COMPRESSED;
    }

    if (json) {
        datatype |= PROTOCOL_BINARY_DATATYPE_JSON;
    }

    memset(request.bytes, 0, sizeof(request));
    request.message.header.request.magic = PROTOCOL_BINARY_REQ;
    request.message.header.request.opcode = PROTOCOL_BINARY_CMD_SET;
    request.message.header.request.datatype = datatype;
    request.message.header.request.extlen = 8;
    request.message.header.request.keylen = htons((uint16_t)keylen);
    request.message.header.request.bodylen = htonl((uint32_t)(keylen + datalen + 8));
    request.message.header.request.opaque = 0xdeadbeef;

    safe_send(&request.bytes, sizeof(request.bytes), false);
    safe_send(extra, sizeof(extra), false);
    safe_send(key, strlen(key), false);
    safe_send(data, datalen, false);

    union {
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } receive;

    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(&receive.response, PROTOCOL_BINARY_CMD_SET,
                                  PROTOCOL_BINARY_RESPONSE_SUCCESS);
    return TEST_PASS;
}

static void get_object_w_datatype(const char *key,
                                  const void *data, size_t datalen,
                                  bool deflate, bool json,
                                  bool conversion)
{
    protocol_binary_response_no_extras response;
    protocol_binary_request_no_extras request;
    int keylen = (int)strlen(key);
    uint32_t flags;
    uint8_t datatype = PROTOCOL_BINARY_RAW_BYTES;
    uint32_t len;

    if (deflate) {
        datatype |= PROTOCOL_BINARY_DATATYPE_COMPRESSED;
    }

    if (json) {
        datatype |= PROTOCOL_BINARY_DATATYPE_JSON;
    }

    memset(request.bytes, 0, sizeof(request));
    request.message.header.request.magic = PROTOCOL_BINARY_REQ;
    request.message.header.request.opcode = PROTOCOL_BINARY_CMD_GET;
    request.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    request.message.header.request.keylen = htons((uint16_t)keylen);
    request.message.header.request.bodylen = htonl(keylen);

    safe_send(&request.bytes, sizeof(request.bytes), false);
    safe_send(key, strlen(key), false);

    safe_recv(&response.bytes, sizeof(response.bytes));
    if (ntohs(response.message.header.response.status != PROTOCOL_BINARY_RESPONSE_SUCCESS)) {
        fprintf(stderr, "Failed to retrieve object!: %d\n",
                (int)ntohs(response.message.header.response.status));
        abort();
    }

    len = ntohl(response.message.header.response.bodylen);
    cb_assert(len > 4);
    safe_recv(&flags, sizeof(flags));
    len -= 4;
    std::vector<char> body(len);
    safe_recv(body.data(), len);

    if (conversion) {
        cb_assert(response.message.header.response.datatype == PROTOCOL_BINARY_RAW_BYTES);
    } else {
        cb_assert(response.message.header.response.datatype == datatype);
    }

    cb_assert(len == datalen);
    cb_assert(memcmp(data, body.data(), body.size()) == 0);
}

TEST_P(McdTestappTest, DatatypeJSON) {
    const char body[] = "{ \"value\" : 1234123412 }";
    set_datatype_feature(true);
    store_object_w_datatype("myjson", body, strlen(body), false, true);

    get_object_w_datatype("myjson", body, strlen(body), false, true, false);

    set_datatype_feature(false);
    get_object_w_datatype("myjson", body, strlen(body), false, true, true);
}

TEST_P(McdTestappTest, DatatypeJSONWithoutSupport) {
    const char body[] = "{ \"value\" : 1234123412 }";
    set_datatype_feature(false);
    store_object_w_datatype("myjson", body, strlen(body), false, false);

    get_object_w_datatype("myjson", body, strlen(body), false, false, false);

    set_datatype_feature(true);
    get_object_w_datatype("myjson", body, strlen(body), false, true, false);
}

/* Compress the specified document, storing the compressed result in the
 * {deflated}.
 * Caller is responsible for free()ing deflated when no longer needed.
 */
size_t compress_document(const char* data, size_t datalen, char** deflated) {

    // Calculate maximum compressed length and allocate a buffer of that size.
    size_t deflated_len = snappy_max_compressed_length(datalen);
    *deflated = (char*)malloc(deflated_len);

    snappy_status status = snappy_compress(data, datalen, *deflated,
                                           &deflated_len);

    cb_assert(status == SNAPPY_OK);

    return deflated_len;
}

TEST_P(McdTestappTest, DatatypeCompressed) {
    const char inflated[] = "aaaaaaaaabbbbbbbccccccdddddd";
    size_t inflated_len = strlen(inflated);
    char* deflated;
    size_t deflated_len = compress_document(inflated, inflated_len, &deflated);

    set_datatype_feature(true);
    store_object_w_datatype("mycompressed", deflated, deflated_len,
                            /*compressed*/true, /*JSON*/false);

    get_object_w_datatype("mycompressed", deflated, deflated_len,
                          true, false, false);

    set_datatype_feature(false);
    get_object_w_datatype("mycompressed", inflated, inflated_len,
                          true, false, true);

    free(deflated);
}

TEST_P(McdTestappTest, DatatypeCompressedJSON) {
    const char inflated[] = "{ \"value\" : \"aaaaaaaaabbbbbbbccccccdddddd\" }";
    size_t inflated_len = strlen(inflated);

    char* deflated;
    size_t deflated_len = compress_document(inflated, inflated_len, &deflated);

    set_datatype_feature(true);

    store_object_w_datatype("mycompressedjson", deflated, deflated_len,
                            /*compressed*/true, /*JSON*/true);

    get_object_w_datatype("mycompressedjson", deflated, deflated_len,
                          true, true, false);

    set_datatype_feature(false);
    get_object_w_datatype("mycompressedjson", inflated, inflated_len,
                          true, true, true);

    free(deflated);
}

TEST_P(McdTestappTest, DatatypeInvalid) {
    protocol_binary_request_no_extras request;
    union {
        protocol_binary_response_no_extras response;
        char buffer[1024];
    } res;
    uint16_t code;

    set_datatype_feature(false);

    memset(request.bytes, 0, sizeof(request));
    request.message.header.request.magic = PROTOCOL_BINARY_REQ;
    request.message.header.request.opcode = PROTOCOL_BINARY_CMD_NOOP;
    request.message.header.request.datatype = 1;

    safe_send(&request.bytes, sizeof(request.bytes), false);
    safe_recv_packet(res.buffer, sizeof(res.buffer));

    code = res.response.message.header.response.status;
    ASSERT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, code);

    reconnect_to_server();

    set_datatype_feature(false);
    request.message.header.request.datatype = 4;
    safe_send(&request.bytes, sizeof(request.bytes), false);
    safe_recv_packet(res.buffer, sizeof(res.buffer));
    code = res.response.message.header.response.status;
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_EINVAL, code);

    reconnect_to_server();
}

static uint64_t get_session_ctrl_token(void) {
    union {
        protocol_binary_request_get_ctrl_token request;
        protocol_binary_response_get_ctrl_token response;
        char bytes[1024];
    } buffer;
    uint64_t ret;

    memset(buffer.bytes, 0, sizeof(buffer));
    buffer.request.message.header.request.magic = PROTOCOL_BINARY_REQ;
    buffer.request.message.header.request.opcode = PROTOCOL_BINARY_CMD_GET_CTRL_TOKEN;

    safe_send(buffer.bytes, sizeof(buffer.request), false);
    safe_recv_packet(&buffer.response, sizeof(buffer.bytes));

    cb_assert(htons(buffer.response.message.header.response.status) ==
                PROTOCOL_BINARY_RESPONSE_SUCCESS);

    ret = ntohll(buffer.response.message.header.response.cas);
    cb_assert(ret != 0);

    return ret;
}

static void prepare_set_session_ctrl_token(protocol_binary_request_set_ctrl_token *req,
                                           uint64_t old, uint64_t new_cas)
{
    memset(req, 0, sizeof(*req));
    req->message.header.request.magic = PROTOCOL_BINARY_REQ;
    req->message.header.request.opcode = PROTOCOL_BINARY_CMD_SET_CTRL_TOKEN;
    req->message.header.request.extlen = sizeof(uint64_t);
    req->message.header.request.bodylen = htonl(sizeof(uint64_t));
    req->message.header.request.cas = htonll(old);
    req->message.body.new_cas = htonll(new_cas);
}

TEST_P(McdTestappTest, SessionCtrlToken) {
    union {
        protocol_binary_request_set_ctrl_token request;
        protocol_binary_response_set_ctrl_token response;
        char bytes[1024];
    } buffer;

    uint64_t old_token = get_session_ctrl_token();
    uint64_t new_token = 0x0102030405060708;

    /* Validate that you may successfully set the token to a legal value */
    prepare_set_session_ctrl_token(&buffer.request, old_token, new_token);
    safe_send(buffer.bytes, sizeof(buffer.request), false);
    cb_assert(safe_recv_packet(&buffer.response, sizeof(buffer.bytes)));

    cb_assert(buffer.response.message.header.response.status ==
              PROTOCOL_BINARY_RESPONSE_SUCCESS);
    cb_assert(new_token == ntohll(buffer.response.message.header.response.cas));
    old_token = new_token;

    /* Validate that you can't set it to 0 */
    prepare_set_session_ctrl_token(&buffer.request, old_token, 0);
    safe_send(buffer.bytes, sizeof(buffer.request), false);
    cb_assert(safe_recv_packet(&buffer.response, sizeof(buffer.bytes)));
    cb_assert(buffer.response.message.header.response.status ==
              PROTOCOL_BINARY_RESPONSE_EINVAL);
    cb_assert(old_token == get_session_ctrl_token());

    /* Validate that you can't set it by providing an incorrect cas */
    prepare_set_session_ctrl_token(&buffer.request, old_token + 1, new_token - 1);
    safe_send(buffer.bytes, sizeof(buffer.request), false);
    cb_assert(safe_recv_packet(&buffer.response, sizeof(buffer.bytes)));

    cb_assert(buffer.response.message.header.response.status ==
              PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS);
    cb_assert(new_token == ntohll(buffer.response.message.header.response.cas));
    cb_assert(new_token == get_session_ctrl_token());

    /* Validate that you may set it by overriding the cas with 0 */
    prepare_set_session_ctrl_token(&buffer.request, 0, 0xdeadbeef);
    safe_send(buffer.bytes, sizeof(buffer.request), false);
    cb_assert(safe_recv_packet(&buffer.response, sizeof(buffer.bytes)));
    cb_assert(buffer.response.message.header.response.status ==
              PROTOCOL_BINARY_RESPONSE_SUCCESS);
    cb_assert(0xdeadbeef == ntohll(buffer.response.message.header.response.cas));
    cb_assert(0xdeadbeef == get_session_ctrl_token());
}

TEST_P(McdTestappTest, MB_10114) {
    char buffer[512] = {0};
    const char *key = "mb-10114";
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } send, receive;
    size_t len;

    // Disable ewouldblock_engine - not wanted / needed for this MB regression test.
    ewouldblock_engine_disable();

    store_object(key, "world");
    do {
        len = mcbp_raw_command(send.bytes, sizeof(send.bytes),
                               PROTOCOL_BINARY_CMD_APPEND,
                               key, strlen(key), buffer, sizeof(buffer));
        safe_send(send.bytes, len, false);
        safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    } while (receive.response.message.header.response.status == PROTOCOL_BINARY_RESPONSE_SUCCESS);

    cb_assert(receive.response.message.header.response.status == PROTOCOL_BINARY_RESPONSE_E2BIG);

    /* We should be able to delete it */
    len = mcbp_raw_command(send.bytes, sizeof(send.bytes),
                           PROTOCOL_BINARY_CMD_DELETE,
                           key, strlen(key), NULL, 0);
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(&receive.response, PROTOCOL_BINARY_CMD_DELETE,
                                  PROTOCOL_BINARY_RESPONSE_SUCCESS);
}

TEST_P(McdTestappTest, DCP_Noop) {
    union {
        protocol_binary_request_dcp_noop request;
        protocol_binary_response_dcp_noop response;
        char bytes[1024];
    } buffer;

    size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                  PROTOCOL_BINARY_CMD_DCP_NOOP,
                                  NULL, 0, NULL, 0);

    /*
     * Default engine don't support DCP, so just check that
     * it detects that and if the packet use incorrect format
     */
    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_DCP_NOOP,
                                  PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);

    len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                           PROTOCOL_BINARY_CMD_DCP_NOOP,
                           "d", 1, "f", 1);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_DCP_NOOP,
                                  PROTOCOL_BINARY_RESPONSE_EINVAL);
}

TEST_P(McdTestappTest, DCP_BufferAck) {
    union {
        protocol_binary_request_dcp_buffer_acknowledgement request;
        protocol_binary_response_dcp_noop response;
        char bytes[1024];
    } buffer;

    size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                  PROTOCOL_BINARY_CMD_DCP_BUFFER_ACKNOWLEDGEMENT,
                                  NULL, 0, "asdf", 4);
    buffer.request.message.header.request.extlen = 4;

    /*
     * Default engine don't support DCP, so just check that
     * it detects that and if the packet use incorrect format
     */
    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_DCP_BUFFER_ACKNOWLEDGEMENT,
                                  PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);

    len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                           PROTOCOL_BINARY_CMD_DCP_BUFFER_ACKNOWLEDGEMENT,
                           "d", 1, "ffff", 4);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_DCP_BUFFER_ACKNOWLEDGEMENT,
                                  PROTOCOL_BINARY_RESPONSE_EINVAL);

    len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                           PROTOCOL_BINARY_CMD_DCP_BUFFER_ACKNOWLEDGEMENT,
                           NULL, 0, "fff", 3);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_DCP_BUFFER_ACKNOWLEDGEMENT,
                                  PROTOCOL_BINARY_RESPONSE_EINVAL);
}

TEST_P(McdTestappTest, DCP_Control) {
    union {
        protocol_binary_request_dcp_control request;
        protocol_binary_response_dcp_control response;
        char bytes[1024];
    } buffer;

    size_t len;

    len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                           PROTOCOL_BINARY_CMD_DCP_CONTROL,
                           "foo", 3, "bar", 3);

    /*
     * Default engine don't support DCP, so just check that
     * it detects that and if the packet use incorrect format
     */
    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_DCP_CONTROL,
                                  PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);

    len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                           PROTOCOL_BINARY_CMD_DCP_CONTROL,
                           NULL, 0, NULL, 0);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_DCP_CONTROL,
                                  PROTOCOL_BINARY_RESPONSE_EINVAL);

    len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                           PROTOCOL_BINARY_CMD_DCP_CONTROL,
                           NULL, 0, "fff", 3);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_DCP_CONTROL,
                                  PROTOCOL_BINARY_RESPONSE_EINVAL);

    len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                           PROTOCOL_BINARY_CMD_DCP_CONTROL,
                           "foo", 3, NULL, 0);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_DCP_CONTROL,
                                  PROTOCOL_BINARY_RESPONSE_EINVAL);
}

TEST_P(McdTestappTest, ISASL_Refresh) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;

    size_t len;

    len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                           PROTOCOL_BINARY_CMD_ISASL_REFRESH,
                           NULL, 0, NULL, 0);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_ISASL_REFRESH,
                                  PROTOCOL_BINARY_RESPONSE_SUCCESS);
}

/*
    Using a memcached protocol extesnsion, shift the time
*/
static void adjust_memcached_clock(uint64_t clock_shift) {
    union {
        protocol_binary_adjust_time request;
        protocol_binary_adjust_time_response response;
        char bytes[1024];
    } buffer;

    size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                  PROTOCOL_BINARY_CMD_ADJUST_TIMEOFDAY,
                                  NULL, 0, NULL, 0);

    buffer.request.message.body.offset = htonll(clock_shift);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_ADJUST_TIMEOFDAY,
                                  PROTOCOL_BINARY_RESPONSE_SUCCESS);
}

/* expiry, wait1 and wait2 need to be crafted so that
   1. sleep(wait1) and key exists
   2. sleep(wait2) and key should now have expired.
*/
static enum test_return test_expiry(const char* key, time_t expiry,
                                    time_t wait1, int clock_shift) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } send, receive;

    uint64_t value = 0xdeadbeefdeadcafe;
    size_t len = 0;
    len = storage_command(send.bytes, sizeof(send.bytes), PROTOCOL_BINARY_CMD_SET,
                                 key, strlen(key), &value, sizeof(value),
                                 0, (uint32_t)expiry);

    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(&receive.response, PROTOCOL_BINARY_CMD_SET,
                                  PROTOCOL_BINARY_RESPONSE_SUCCESS);

    adjust_memcached_clock(clock_shift);

#ifdef WIN32
    Sleep((DWORD)(wait1 * 1000));
#else
    sleep(wait1);
#endif

    memset(send.bytes, 0, 1024);
    len = mcbp_raw_command(send.bytes, sizeof(send.bytes),
                           PROTOCOL_BINARY_CMD_GET,
                           key, strlen(key), NULL, 0);

    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(&receive.response, PROTOCOL_BINARY_CMD_GET,
                                  PROTOCOL_BINARY_RESPONSE_SUCCESS);

    return TEST_PASS;
}

TEST_P(McdTestappTest, ExpiryRelativeWithClockChangeBackwards) {
    /*
       Just test for MB-11548
       120 second expiry.
       Set clock back by some amount that's before the time we started memcached.
       wait 2 seconds (allow mc time to tick)
       (defect was that time went negative and expired keys immediatley)
    */
    time_t now = time(0);
    test_expiry("test_expiry_relative_with_clock_change_backwards",
                120, 2, (int)(0 - ((now - server_start_time) * 2)));
}

void McdTestappTest::test_set_huge_impl(const char *key, uint8_t cmd,
                                        int result, bool pipeline,
                                        int iterations, int message_size) {

    // This is a large, long test. Disable ewouldblock_engine while
    // running it to speed it up.
    ewouldblock_engine_disable();

    /* some error case may return a body in the response */
    char receive[sizeof(protocol_binary_response_no_extras) + 32];
    const size_t len = message_size + sizeof(protocol_binary_request_set) + strlen(key);
    std::vector<char> set_message(len);
    char* message = set_message.data() + (sizeof(protocol_binary_request_set) + strlen(key));
    int ii;
    memset(message, 0xb0, message_size);

    cb_assert(len == storage_command(set_message.data(), len, cmd, key,
                                     strlen(key), NULL, message_size,
                                     0, 0));

    for (ii = 0; ii < iterations; ++ii) {
        safe_send(set_message.data(), len, false);
        if (!pipeline) {
            if (cmd == PROTOCOL_BINARY_CMD_SET) {
                safe_recv_packet(&receive, sizeof(receive));
                mcbp_validate_response_header(
                    (protocol_binary_response_no_extras*)receive, cmd, result);
            }
        }
    }

    if (pipeline && cmd == PROTOCOL_BINARY_CMD_SET) {
        for (ii = 0; ii < iterations; ++ii) {
            safe_recv_packet(&receive, sizeof(receive));
            mcbp_validate_response_header(
                (protocol_binary_response_no_extras*)receive, cmd, result);
        }
    }
}

TEST_P(McdTestappTest, SetHuge) {
    test_set_huge_impl("test_set_huge", PROTOCOL_BINARY_CMD_SET,
                       PROTOCOL_BINARY_RESPONSE_SUCCESS, false, 10,
                       1023 * 1024);
}

TEST_P(McdTestappTest, SetE2BIG) {
    test_set_huge_impl("test_set_e2big", PROTOCOL_BINARY_CMD_SET,
                       PROTOCOL_BINARY_RESPONSE_E2BIG, false, 10,
                       1024 * 1024);
}

TEST_P(McdTestappTest, SetQHuge) {
    test_set_huge_impl("test_setq_huge", PROTOCOL_BINARY_CMD_SETQ,
                       PROTOCOL_BINARY_RESPONSE_SUCCESS, false, 10,
                       1023 * 1024);
}

TEST_P(McdTestappTest, PipelineHuge) {
    test_set_huge_impl("test_pipeline_huge", PROTOCOL_BINARY_CMD_SET,
                       PROTOCOL_BINARY_RESPONSE_SUCCESS, true, 200,
                       1023 * 1024);
}

/* support set, get, delete */
void test_pipeline_impl(int cmd, int result, const char* key_root,
                        uint32_t messages_in_stream, size_t value_size) {
    size_t largest_protocol_packet = sizeof(protocol_binary_request_set); /* set has the largest protocol message */
    size_t key_root_len = strlen(key_root);
    size_t key_digit_len = 5; /*append 00001, 00002 etc.. to key_root */
    const size_t buffer_len = (largest_protocol_packet + key_root_len +
                               key_digit_len + value_size) * messages_in_stream;
    size_t out_message_len = 0, in_message_len = 0, send_len = 0, receive_len = 0;
    std::vector<uint8_t> buffer(buffer_len); /* space for creating and receiving a stream */
    std::vector<char> key(key_root_len + key_digit_len + 1); /* space for building keys */
    uint8_t* current_message = buffer.data();
    int session = 0; /* something to stick in opaque */

    session = rand() % 100;

    cb_assert(messages_in_stream <= 99999);

    /* now figure out the correct send and receive lengths */
    if (cmd == PROTOCOL_BINARY_CMD_SET) {
        /* set, sends key and a value */
        out_message_len = sizeof(protocol_binary_request_set) + key_root_len + key_digit_len + value_size;
        /* receives a plain response, no extra */
        in_message_len = sizeof(protocol_binary_response_no_extras);
    } else if (cmd == PROTOCOL_BINARY_CMD_GET) {
        /* get sends key */
        out_message_len = sizeof(protocol_binary_request_get) + key_root_len + key_digit_len;
        /* receives a response + value */
        in_message_len = sizeof(protocol_binary_response_no_extras) + 4 + value_size;

        if (result == PROTOCOL_BINARY_RESPONSE_SUCCESS) {
            /* receives a response + flags + value */
            in_message_len = sizeof(protocol_binary_response_no_extras) + 4 + value_size;
        } else {
            /* receives a response + string error */
            in_message_len = sizeof(protocol_binary_response_no_extras) + 9;
        }
    } else if (cmd == PROTOCOL_BINARY_CMD_DELETE) {
        /* delete sends key */
        out_message_len = sizeof(protocol_binary_request_get) + key_root_len + key_digit_len;
        /* receives a plain response, no extra */
        in_message_len = sizeof(protocol_binary_response_no_extras);
    } else {
        FAIL() << "invalid cmd (" << cmd << ") in test_pipeline_impl";
    }

    send_len    = out_message_len * messages_in_stream;
    receive_len = in_message_len * messages_in_stream;

    /* entire buffer and thus any values are 0xaf */
    std::fill(buffer.begin(), buffer.end(), 0xaf);

    for (uint32_t ii = 0; ii < messages_in_stream; ii++) {
        snprintf(key.data(), key_root_len + key_digit_len + 1, "%s%05d", key_root, ii);
        if (PROTOCOL_BINARY_CMD_SET == cmd) {
            protocol_binary_request_set* this_req = (protocol_binary_request_set*)current_message;
            current_message += storage_command((char*)current_message,
                                               out_message_len, cmd,
                                               key.data(), strlen(key.data()),
                                               NULL, value_size, 0, 0);
            this_req->message.header.request.opaque = htonl((session << 8) | ii);
        } else {
            protocol_binary_request_no_extras* this_req = (protocol_binary_request_no_extras*)current_message;
            current_message += mcbp_raw_command((char*)current_message,
                                                out_message_len, cmd,
                                                key.data(), strlen(key.data()),
                                                NULL, 0);
            this_req->message.header.request.opaque = htonl((session << 8) | ii);
        }
    }

    cb_assert(buffer.size() >= send_len);

    safe_send(buffer.data(), send_len, false);

    std::fill(buffer.begin(), buffer.end(), 0);

    /* and get it all back in the same buffer */
    cb_assert(buffer.size() >= receive_len);

    safe_recv(buffer.data(), receive_len);
    current_message = buffer.data();
    for (uint32_t ii = 0; ii < messages_in_stream; ii++) {
        protocol_binary_response_no_extras* message = (protocol_binary_response_no_extras*)current_message;

        uint32_t bodylen = ntohl(message->message.header.response.bodylen);
        uint8_t  extlen  = message->message.header.response.extlen;
        uint16_t status  = ntohs(message->message.header.response.status);
        uint32_t opq     = ntohl(message->message.header.response.opaque);

        cb_assert(status == result);
        cb_assert(opq == ((session << 8)|ii));

        /* a value? */
        if (bodylen != 0 && result == PROTOCOL_BINARY_RESPONSE_SUCCESS) {
            uint8_t* value = current_message + sizeof(protocol_binary_response_no_extras) + extlen;
            for (size_t jj = 0; jj < value_size; jj++) {
                cb_assert(value[jj] == 0xaf);
            }
            current_message = current_message + bodylen + sizeof(protocol_binary_response_no_extras);
        } else {
            current_message = (uint8_t*)(message + 1);
        }
    }
}

TEST_P(McdTestappTest, PipelineSet) {
    // This is a large, long test. Disable ewouldblock_engine while
    // running it to speed it up.
    ewouldblock_engine_disable();

    /*
      MB-11203 would break at iteration 529 where we happen to send 57916 bytes in 1 pipe
      this triggered some edge cases in our SSL recv code.
    */
    for (int ii = 1; ii < 1000; ii++) {
        test_pipeline_impl(PROTOCOL_BINARY_CMD_SET,
                           PROTOCOL_BINARY_RESPONSE_SUCCESS, "key_set_pipe",
                           100, ii);
        test_pipeline_impl(PROTOCOL_BINARY_CMD_DELETE,
                           PROTOCOL_BINARY_RESPONSE_SUCCESS, "key_set_pipe",
                           100, ii);
    }
}

TEST_P(McdTestappTest, PipelineSetGetDel) {
    const char key_root[] = "key_set_get_del";

    // This is a large, long test. Disable ewouldblock_engine while
    // running it to speed it up.
    ewouldblock_engine_disable();

    test_pipeline_impl(PROTOCOL_BINARY_CMD_SET,
                       PROTOCOL_BINARY_RESPONSE_SUCCESS, key_root, 5000, 256);

    test_pipeline_impl(PROTOCOL_BINARY_CMD_GET,
                       PROTOCOL_BINARY_RESPONSE_SUCCESS, key_root, 5000, 256);

    test_pipeline_impl(PROTOCOL_BINARY_CMD_DELETE,
                       PROTOCOL_BINARY_RESPONSE_SUCCESS, key_root, 5000, 256);
}

TEST_P(McdTestappTest, PipelineSetDel) {
    // This is a large, long test. Disable ewouldblock_engine while
    // running it to speed it up.
    ewouldblock_engine_disable();

    test_pipeline_impl(PROTOCOL_BINARY_CMD_SET,
                       PROTOCOL_BINARY_RESPONSE_SUCCESS, "key_root",
                       5000, 256);

    test_pipeline_impl(PROTOCOL_BINARY_CMD_DELETE,
                       PROTOCOL_BINARY_RESPONSE_SUCCESS, "key_root",
                       5000, 256);
}

/* Send one character to the SSL port, then check memcached correctly closes
 * the connection (and doesn't hold it open for ever trying to read) more bytes
 * which will never come.
 */
TEST_P(McdTestappTest, MB_12762_SSLHandshakeHang) {

    // Requires SSL.
    if (current_phase != phase_ssl) {
        return;
    }

    /* Setup: Close the existing (handshaked) SSL connection, and create a
     * 'plain' TCP connection to the SSL port - i.e. without any SSL handshake.
     */
    closesocket(sock_ssl);
    sock_ssl = create_connect_plain_socket(ssl_port);

    /* Send a payload which is NOT a valid SSL handshake: */
    char buf[] = {'a', '\n'};
#if defined(WIN32)
    ssize_t len = send(sock_ssl, buf, (int)sizeof(buf), 0);
#else
    ssize_t len = send(sock_ssl, buf, sizeof(buf), 0);
#endif
    cb_assert(len == 2);

    /* Done writing, close the socket for writing. This triggers the bug: a
     * conn_read -> conn_waiting -> conn_read ... loop in memcached */
#if defined(WIN32)
    int res = shutdown(sock_ssl, SD_SEND);
#else
    int res = shutdown(sock_ssl, SHUT_WR);
#endif
    cb_assert(res == 0);

    /* Check status of the FD - expected to be ready (as it's just been closed
     * by peer), and should not have hit the timeout.
     */
    fd_set fdset;
    FD_ZERO(&fdset);
    FD_SET(sock_ssl, &fdset);
    struct timeval timeout = {0};
    timeout.tv_sec = 5;
    int ready_fds = select((int)(sock_ssl + 1), &fdset, NULL, NULL, &timeout);
    cb_assert(ready_fds == 1);

    /* Verify that attempting to read from the socket returns 0 (peer has
     * indeed closed the connection).
     */
    len = recv(sock_ssl, buf, 1, 0);
    cb_assert(len == 0);

    /* Restore the SSL connection to a sane state :) */
    reconnect_to_server();
}

std::string get_sasl_mechs(void) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;

    size_t plen = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                   PROTOCOL_BINARY_CMD_SASL_LIST_MECHS,
                                   NULL, 0, NULL, 0);

    safe_send(buffer.bytes, plen, false);
    safe_recv_packet(&buffer, sizeof(buffer));
    mcbp_validate_response_header(&buffer.response,
                                  PROTOCOL_BINARY_CMD_SASL_LIST_MECHS,
                                  PROTOCOL_BINARY_RESPONSE_SUCCESS);

    std::string ret;
    ret.assign(buffer.bytes + sizeof(buffer.response.bytes),
               buffer.response.message.header.response.bodylen);
    return ret;
}


TEST_P(McdTestappTest, SASL_ListMech) {
    std::string mech(get_sasl_mechs());
    EXPECT_NE(0u, mech.size());
}

struct my_sasl_ctx {
    const char *username;
    cbsasl_secret_t *secret;
};

static int sasl_get_username(void *context, int id, const char **result,
                             unsigned int *len)
{
    struct my_sasl_ctx *ctx = reinterpret_cast<struct my_sasl_ctx *>(context);
    if (!context || !result || (id != CBSASL_CB_USER && id != CBSASL_CB_AUTHNAME)) {
        return CBSASL_BADPARAM;
    }

    *result = ctx->username;
    if (len) {
        *len = (unsigned int)strlen(*result);
    }

    return CBSASL_OK;
}

static int sasl_get_password(cbsasl_conn_t *conn, void *context, int id,
                             cbsasl_secret_t **psecret)
{
    struct my_sasl_ctx *ctx = reinterpret_cast<struct my_sasl_ctx *>(context);
    if (!conn || ! psecret || id != CBSASL_CB_PASS || ctx == NULL) {
        return CBSASL_BADPARAM;
    }

    *psecret = ctx->secret;
    return CBSASL_OK;
}

uint16_t TestappTest::sasl_auth(const char *username, const char *password) {
    cbsasl_error_t err;
    const char *data;
    unsigned int len;
    const char *chosenmech;
    struct my_sasl_ctx context;
    cbsasl_callback_t sasl_callbacks[4];
    cbsasl_conn_t *client;
    std::string mech(get_sasl_mechs());

    sasl_callbacks[0].id = CBSASL_CB_USER;
    sasl_callbacks[0].proc = (int( *)(void)) &sasl_get_username;
    sasl_callbacks[0].context = &context;
    sasl_callbacks[1].id = CBSASL_CB_AUTHNAME;
    sasl_callbacks[1].proc = (int( *)(void)) &sasl_get_username;
    sasl_callbacks[1].context = &context;
    sasl_callbacks[2].id = CBSASL_CB_PASS;
    sasl_callbacks[2].proc = (int( *)(void)) &sasl_get_password;
    sasl_callbacks[2].context = &context;
    sasl_callbacks[3].id = CBSASL_CB_LIST_END;
    sasl_callbacks[3].proc = NULL;
    sasl_callbacks[3].context = NULL;

    context.username = username;
    context.secret = reinterpret_cast<cbsasl_secret_t*>(calloc(1, 100));
    memcpy(context.secret->data, password, strlen(password));
    context.secret->len = (unsigned long)strlen(password);

    err = cbsasl_client_new(NULL, NULL, NULL, NULL, sasl_callbacks, 0, &client);
    cb_assert(err == CBSASL_OK);
    err = cbsasl_client_start(client, mech.c_str(), NULL, &data, &len, &chosenmech);
    cb_assert(err == CBSASL_OK);

    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;

    size_t plen = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                   PROTOCOL_BINARY_CMD_SASL_AUTH,
                                   chosenmech, strlen(chosenmech),
                                   data, len);

    safe_send(buffer.bytes, plen, false);
    safe_recv_packet(&buffer, sizeof(buffer));

    bool stepped = false;

    while (buffer.response.message.header.response.status == PROTOCOL_BINARY_RESPONSE_AUTH_CONTINUE) {
        stepped = true;
        int datalen = buffer.response.message.header.response.bodylen -
            buffer.response.message.header.response.keylen -
            buffer.response.message.header.response.extlen;

        int dataoffset = sizeof(buffer.response.bytes) +
            buffer.response.message.header.response.keylen +
            buffer.response.message.header.response.extlen;

        err = cbsasl_client_step(client, buffer.bytes + dataoffset, datalen,
                                 NULL, &data, &len);

        plen = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                PROTOCOL_BINARY_CMD_SASL_STEP,
                                chosenmech, strlen(chosenmech), data, len);

        safe_send(buffer.bytes, plen, false);

        safe_recv_packet(&buffer, sizeof(buffer));
    }

    if (stepped) {
        mcbp_validate_response_header(&buffer.response,
                                      PROTOCOL_BINARY_CMD_SASL_STEP,
                                      buffer.response.message.header.response.status);
    } else {
        mcbp_validate_response_header(&buffer.response,
                                      PROTOCOL_BINARY_CMD_SASL_AUTH,
                                      buffer.response.message.header.response.status);
    }
    free(context.secret);
    cbsasl_dispose(&client);

    return buffer.response.message.header.response.status;
}

TEST_P(McdTestappTest, SASL_Success) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_SUCCESS,
              sasl_auth("_admin", "password"));
}

TEST_P(McdTestappTest, SASL_Fail) {
    EXPECT_EQ(PROTOCOL_BINARY_RESPONSE_AUTH_ERROR,
              sasl_auth("_admin", "asdf"));
}

TEST_P(McdTestappTest, ExceedMaxPacketSize)
{
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } send, receive;
    memset(send.bytes, 0, sizeof(send.bytes));

    storage_command(send.bytes, sizeof(send.bytes),
                    PROTOCOL_BINARY_CMD_SET,
                    "key", 3, NULL, 0, 0, 0);
    send.request.message.header.request.bodylen = ntohl(31*1024*1024);
    safe_send(send.bytes, sizeof(send.bytes), false);

    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    mcbp_validate_response_header(&receive.response, PROTOCOL_BINARY_CMD_SET,
                                  PROTOCOL_BINARY_RESPONSE_EINVAL);

    reconnect_to_server();
}

/**
 * Returns the current access count of the test key ("someval") as an integer
 * via the old string format.
 */
int get_topkeys_legacy_value(const std::string& wanted_key) {

    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[8192];
    } buffer;

    size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                  PROTOCOL_BINARY_CMD_STAT,
                                  "topkeys", strlen("topkeys"),
                                  NULL, 0);
    safe_send(buffer.bytes, len, false);

    // We expect a variable number of response packets (one per top key);
    // take them all off the wire, recording the one for our key which we
    // parse at the end.
    std::string value;
    while (true) {
        safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
        mcbp_validate_response_header(&buffer.response,
                                      PROTOCOL_BINARY_CMD_STAT,
                                      PROTOCOL_BINARY_RESPONSE_SUCCESS);

        const char* key_ptr(buffer.bytes + sizeof(buffer.response) +
                            buffer.response.message.header.response.extlen);
        size_t key_len(buffer.response.message.header.response.keylen);

        // A packet with key length zero indicates end of the stats.
        if (key_len == 0) {
            break;
        }

        const std::string key(key_ptr, key_len);
        if (key == wanted_key) {
            // Got our key. Save the value to one side; and finish consuming
            // the STAT reponse packets.
            EXPECT_EQ(0u, value.size())
                << "Unexpectedly found a second topkey for wanted key '" << wanted_key;

            const char* val_ptr(key_ptr + key_len);
            const size_t val_len(buffer.response.message.header.response.bodylen -
                                 key_len -
                                 buffer.response.message.header.response.extlen);
            EXPECT_GT(val_len, 0);
            value = std::string(val_ptr, val_len);
        }
    };

    if (value.size() > 0) {
        // Extract the 'get_hits' stat (which actually the aggregate of all
        // operations now).

        const std::string token("get_hits=");
        auto pos = value.find(token);
        EXPECT_NE(std::string::npos, pos)
            << "Failed to locate '" << token << "' substring in topkey '"
            << wanted_key << "' value '" << value << "'";

        // Move iterator to the other side of the equals sign (the value) and
        // erase before that point.
        pos += token.size();
        value.erase(0, pos);

        return std::stoi(value);
    } else {
        // If we got here then we failed to find the given key.
        return 0;
    }
}

/**
 * Accesses the current value of the key via the JSON formatted topkeys return.
 * @return True if the specified key was found (and sets count to the
 *         keys' access count) or false if not found.
 */
bool get_topkeys_json_value(const std::string& key, int& count) {

    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[8192];
    } buffer;

    size_t len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                                  PROTOCOL_BINARY_CMD_STAT,
                                  "topkeys_json", strlen("topkeys_json"),
                                  NULL, 0);
    safe_send(buffer.bytes, len, false);

    // Expect 1 valid packet followed by 1 null
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response, PROTOCOL_BINARY_CMD_STAT,
                                  PROTOCOL_BINARY_RESPONSE_SUCCESS);

    EXPECT_NE(0, buffer.response.message.header.response.keylen);

    const char* val_ptr = buffer.bytes + (sizeof(buffer.response) +
             buffer.response.message.header.response.keylen +
             buffer.response.message.header.response.extlen);
    const size_t vallen(buffer.response.message.header.response.bodylen -
                        buffer.response.message.header.response.keylen -
                        buffer.response.message.header.response.extlen);
    EXPECT_GT(vallen, 0);
    const std::string value(val_ptr, vallen);

    // Consume NULL stats packet.
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response, PROTOCOL_BINARY_CMD_STAT,
                                  PROTOCOL_BINARY_RESPONSE_SUCCESS);
    EXPECT_EQ(0, buffer.response.message.header.response.keylen);

    // Check for response string

    cJSON* json_value = cJSON_Parse(value.c_str());
    EXPECT_NE(nullptr, json_value)
        << "Failed to parse response string '" << value << "' to JSON";

    cJSON* topkeys = cJSON_GetObjectItem(json_value, "topkeys");
    EXPECT_NE(nullptr, topkeys);

    // Search the array for the specified key's information.
    for (int ii = 0; ii < cJSON_GetArraySize(topkeys); ii++) {
        cJSON *record = cJSON_GetArrayItem(topkeys, ii);

        cJSON* current_key = cJSON_GetObjectItem(record, "key");
        EXPECT_NE(nullptr, current_key);

        if (key == current_key->valuestring) {
            cJSON* access_count = cJSON_GetObjectItem(record, "access_count");
            EXPECT_NE(nullptr, access_count);
            count = access_count->valueint;
            cJSON_Delete(json_value);
            return true;
        }
    }

    cJSON_Delete(json_value);
    return false;
}

/**
 * Set a key a number of times and assert that the return value matches the
 * change after the number of set operations.
 */
static void test_set_topkeys(const std::string& key, const int operations) {

    // In theory we should start with no record of a key; but there's no
    // explicit way to clear topkeys; and a previous test run against the same
    // memcached instance may have used the same key.
    // Therefore for robustness don't assume the key doesn't exist; and fetch
    // the initial count.
    int initial_count = 0;
    get_topkeys_json_value(key, initial_count);

    int ii;
    size_t len;
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;
    memset(buffer.bytes, 0, sizeof(buffer));

    /* Send CMD_SET for current key 'sum' number of times (and validate
     * response). */
    for (ii = 0; ii < operations; ii++) {
        len = storage_command(buffer.bytes, sizeof(buffer.bytes),
                              PROTOCOL_BINARY_CMD_SET, key.c_str(),
                              key.length(),
                              "foo", strlen("foo"), 0, 0);

        safe_send(buffer.bytes, len, false);

        safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
        mcbp_validate_response_header(&buffer.response, PROTOCOL_BINARY_CMD_SET,
                                      PROTOCOL_BINARY_RESPONSE_SUCCESS);
    }

    EXPECT_EQ(initial_count + operations, get_topkeys_legacy_value(key));
    int json_value;
    EXPECT_TRUE(get_topkeys_json_value(key, json_value));
    EXPECT_EQ(initial_count + operations, json_value);
}


/**
 * Get a key a number of times and assert that the return value matches the
 * change after the number of get operations.
 */
static void test_get_topkeys(const std::string& key, int operations) {
    int initial_count;
    ASSERT_TRUE(get_topkeys_json_value(key, initial_count));

    int ii;
    size_t len;
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[2048];
    } buffer;
    memset(buffer.bytes, 0, sizeof(buffer));

    for (ii = 0; ii < operations; ii++) {
        len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                               PROTOCOL_BINARY_CMD_GET, key.c_str(),
                               key.length(),
                               NULL, 0);
        safe_send(buffer.bytes, len, false);

        safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
        mcbp_validate_response_header(&buffer.response, PROTOCOL_BINARY_CMD_GET,
                                      PROTOCOL_BINARY_RESPONSE_SUCCESS);
    }

    const int expected_count = initial_count + operations;
    EXPECT_EQ(expected_count, get_topkeys_legacy_value(key))
        << "Unexpected topkeys legacy count for key:" << key;
    int json_value;
    EXPECT_TRUE(get_topkeys_json_value(key, json_value));
    EXPECT_EQ(expected_count, json_value);
}

/**
 * Delete a key and assert that the return value matches the change
 * after the delete operation.
 */
static void test_delete_topkeys(const std::string& key) {
    int initial_count;
    ASSERT_TRUE(get_topkeys_json_value(key, initial_count));

    size_t len;
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;
    memset(buffer.bytes, 0, sizeof(buffer));

    len = mcbp_raw_command(buffer.bytes, sizeof(buffer.bytes),
                           PROTOCOL_BINARY_CMD_DELETE, key.c_str(),
                           key.length(),
                           NULL, 0);
    safe_send(buffer.bytes, len, false);

    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    mcbp_validate_response_header(&buffer.response, PROTOCOL_BINARY_CMD_DELETE,
                                  PROTOCOL_BINARY_RESPONSE_SUCCESS);

    EXPECT_EQ(initial_count + 1, get_topkeys_legacy_value(key))
        << "Unexpected topkeys legacy count for key:" << key;
    int json_value;
    EXPECT_TRUE(get_topkeys_json_value(key, json_value));
    EXPECT_EQ(initial_count + 1, json_value);
}


/**
 * Test for JSON document formatted topkeys (part of bucket_engine). Tests for
 * correct values when issuing CMD_SET, CMD_GET, and CMD_DELETE.
 */
TEST_P(McdTestappTest, test_topkeys) {

    /* Perform sets on a few different keys. */
    test_set_topkeys("key1", 1);
    test_set_topkeys("key2", 2);
    test_set_topkeys("key3", 3);

    test_get_topkeys("key1", 10);

    test_delete_topkeys("key1");
}

INSTANTIATE_TEST_CASE_P(PlainOrSSL,
                        McdTestappTest,
                        ::testing::Values(Transport::Plain, Transport::SSL));

void McdEnvironment::SetUp() {
    // Create an rbac config file for use for all tests
    cJSON *rbac = generate_rbac_config();
    char *rbac_text = cJSON_Print(rbac);

    char rbac_file_pattern [] = RBAC_FILE_PATTERN;
    strncpy(rbac_file_pattern, RBAC_FILE_PATTERN, sizeof(rbac_file_pattern));
    ASSERT_NE(cb_mktemp(rbac_file_pattern), nullptr);
    rbac_file_name = rbac_file_pattern;

    ASSERT_EQ(0, write_config_to_file(rbac_text, rbac_file_name.c_str()));

    cJSON_Free(rbac_text);
    cJSON_Delete(rbac);

    // Create an isasl file for all tests.
    isasl_file_name = "isasl." + std::to_string(getpid()) +
                      "." + std::to_string(time(NULL)) + ".pw";

    // write out user/passwords
    std::ofstream isasl(isasl_file_name,
                        std::ofstream::out | std::ofstream::trunc);
    ASSERT_TRUE(isasl.is_open());
    isasl << "_admin password " << std::endl;
    for (int ii = 0; ii < COUCHBASE_MAX_NUM_BUCKETS; ++ii) {
        std::stringstream line;
        line << "mybucket_" << std::setfill('0') << std::setw(3) << ii;
        line << " mybucket_" << std::setfill('0') << std::setw(3) << ii
             << " " << std::endl;
        isasl << line.rdbuf();
    }

    // Add the file to the exec environment
    snprintf(isasl_env_var, sizeof(isasl_env_var),
             "ISASL_PWFILE=%s", isasl_file_name.c_str());
    putenv(isasl_env_var);
}

void McdEnvironment::TearDown() {
    // Cleanup RBAC config file.
    EXPECT_NE(-1, remove(rbac_file_name.c_str()));

    // Cleanup isasl file
    EXPECT_NE(-1, remove(isasl_file_name.c_str()));

    shutdown_openssl();
}

char McdEnvironment::isasl_env_var[256];
unique_cJSON_ptr TestappTest::memcached_cfg;
std::string TestappTest::config_file;

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);

#ifdef __sun
    {
        // Use coreadm to set up a corefile pattern to ensure that the corefiles
        // created from the unit tests (of testapp or memcached) don't
        // overwrite each other
        std::string coreadm =
            "coreadm -p core.%%f.%%p %lu" + std::to_string(getpid());
        system(coreadm.c_str());
    }
#endif

    int cmd;
    while ((cmd = getopt(argc, argv, "v")) != EOF) {
        switch (cmd) {
        case 'v':
            memcached_verbose = true;
            break;
        default:
            std::cerr << "Usage: " << argv[0] << " [-v]" << std::endl
                      << std::endl
                      << "  -v Verbose - Print verbose memcached output "
                      << "to stderr.\n" << std::endl;
            return 1;
        }
    }
    mcd_env = new McdEnvironment();
    ::testing::AddGlobalTestEnvironment(mcd_env);

    cb_initialize_sockets();

    return RUN_ALL_TESTS();
}
