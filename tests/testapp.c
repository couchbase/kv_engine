/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <fcntl.h>
#include <ctype.h>
#include <time.h>
#include <evutil.h>
#include <snappy-c.h>
#include <cJSON.h>


#include "daemon/cache.h"
#include <memcached/util.h>
#include <memcached/protocol_binary.h>
#include <memcached/config_parser.h>
#include <cbsasl/cbsasl.h>
#include "extensions/protocol/fragment_rw.h"
#include "extensions/protocol/testapp_extension.h"
#include <platform/platform.h>
#include "memcached/openssl.h"
#include "programs/utilities.h"

#ifdef WIN32
#include <process.h>
#define getpid() _getpid()
#endif


/* Set the read/write commands differently than the default values
 * so that we can verify that the override works
 */
static uint8_t read_command = 0xe1;
static uint8_t write_command = 0xe2;

static cJSON *json_config = NULL;
const char *config_string = NULL;
char config_file[] = "memcached_testapp.json.XXXXXX";
char rbac_file[] = "testapp_rbac.json.XXXXXX";

#define TMP_TEMPLATE "/tmp/test_file.XXXXXXX"
#define MAX_CONNECTIONS 1000
#define BACKLOG 1024

enum test_return { TEST_SKIP, TEST_PASS, TEST_FAIL };

/* test phases (bitmasks) */
#define phase_setup 0x1
#define phase_plain 0x2
#define phase_ssl 0x4
#define phase_cleanup 0x8

#define phase_max 4
static int current_phase = 0;

/* by default all phases enabled (but cmd line args may disable some). */
static int phases_enabled = phase_setup | phase_plain | phase_ssl | phase_cleanup;

static pid_t server_pid;
static in_port_t port = -1;
static in_port_t ssl_port = -1;
static SOCKET sock;
static SOCKET sock_ssl;
static bool allow_closed_read = false;
static time_t server_start_time = 0;
static SSL_CTX *ssl_ctx = NULL;
static SSL *ssl = NULL;
static BIO *ssl_bio_r = NULL;
static BIO *ssl_bio_w = NULL;

/* Returns true if the specified test phase is enabled. */
static bool phase_enabled(int phase) {
    return phases_enabled & phase;
}

static enum test_return cache_create_test(void)
{
    cache_t *cache = cache_create("test", sizeof(uint32_t), sizeof(char*),
                                  NULL, NULL);
    cb_assert(cache != NULL);
    cache_destroy(cache);
    return TEST_PASS;
}

const uint64_t constructor_pattern = 0xdeadcafebabebeef;

static int cache_constructor(void *buffer, void *notused1, int notused2) {
    uint64_t *ptr = buffer;
    *ptr = constructor_pattern;
    return 0;
}

static enum test_return cache_constructor_test(void)
{
    uint64_t *ptr;
    uint64_t pattern;
    cache_t *cache = cache_create("test", sizeof(uint64_t), sizeof(uint64_t),
                                  cache_constructor, NULL);


    cb_assert(cache != NULL);
    ptr = cache_alloc(cache);
    pattern = *ptr;
    cache_free(cache, ptr);
    cache_destroy(cache);
    return (pattern == constructor_pattern) ? TEST_PASS : TEST_FAIL;
}

static int cache_fail_constructor(void *buffer, void *notused1, int notused2) {
    return 1;
}

static enum test_return cache_fail_constructor_test(void)
{
    enum test_return ret = TEST_PASS;
    uint64_t *ptr;
    cache_t *cache = cache_create("test", sizeof(uint64_t), sizeof(uint64_t),
                                  cache_fail_constructor, NULL);
    cb_assert(cache != NULL);
    ptr = cache_alloc(cache);
    if (ptr != NULL) {
        ret = TEST_FAIL;
    }
    cache_destroy(cache);
    return ret;
}

static void *destruct_data = 0;

static void cache_destructor(void *buffer, void *notused) {
    destruct_data = buffer;
}

static enum test_return cache_destructor_test(void)
{
    char *ptr;
    cache_t *cache = cache_create("test", sizeof(uint32_t), sizeof(char*),
                                  NULL, cache_destructor);
    cb_assert(cache != NULL);
    ptr = cache_alloc(cache);
    cache_free(cache, ptr);
    cache_destroy(cache);

    return (ptr == destruct_data) ? TEST_PASS : TEST_FAIL;
}

static enum test_return cache_reuse_test(void)
{
    int ii;
    cache_t *cache = cache_create("test", sizeof(uint32_t), sizeof(char*),
                                  NULL, NULL);
    char *ptr = cache_alloc(cache);
    cache_free(cache, ptr);
    for (ii = 0; ii < 100; ++ii) {
        char *p = cache_alloc(cache);
        cb_assert(p == ptr);
        cache_free(cache, ptr);
    }
    cache_destroy(cache);
    return TEST_PASS;
}


static enum test_return cache_bulkalloc(size_t datasize)
{
    cache_t *cache = cache_create("test", datasize, sizeof(char*),
                                  NULL, NULL);
#define ITERATIONS 1024
    void *ptr[ITERATIONS];
    int ii;
    for (ii = 0; ii < ITERATIONS; ++ii) {
        ptr[ii] = cache_alloc(cache);
        cb_assert(ptr[ii] != 0);
        memset(ptr[ii], 0xff, datasize);
    }

    for (ii = 0; ii < ITERATIONS; ++ii) {
        cache_free(cache, ptr[ii]);
    }

#undef ITERATIONS
    cache_destroy(cache);
    return TEST_PASS;
}

static enum test_return test_issue_161(void)
{
    enum test_return ret = cache_bulkalloc(1);
    if (ret == TEST_PASS) {
        ret = cache_bulkalloc(512);
    }

    return ret;
}

static enum test_return cache_redzone_test(void)
{
#if !defined(HAVE_UMEM_H) && !defined(NDEBUG) && !defined(WIN32)
    cache_t *cache = cache_create("test", sizeof(uint32_t), sizeof(char*),
                                  NULL, NULL);

    /* Ignore SIGABORT */
    struct sigaction old_action;
    struct sigaction action;
    char *p;
    char old;

    memset(&action, 0, sizeof(action));
    action.sa_handler = SIG_IGN;
    sigemptyset(&action.sa_mask);
    sigaction(SIGABRT, &action, &old_action);

    /* check memory debug.. */
    p = cache_alloc(cache);
    old = *(p - 1);
    *(p - 1) = 0;
    cache_free(cache, p);
    cb_assert(cache_error == -1);
    *(p - 1) = old;

    p[sizeof(uint32_t)] = 0;
    cache_free(cache, p);
    cb_assert(cache_error == 1);

    /* restore signal handler */
    sigaction(SIGABRT, &old_action, NULL);

    cache_destroy(cache);

    return TEST_PASS;
#else
    return TEST_SKIP;
#endif
}

static enum test_return test_safe_strtoul(void) {
    uint32_t val;
    cb_assert(safe_strtoul("123", &val));
    cb_assert(val == 123);
    cb_assert(safe_strtoul("+123", &val));
    cb_assert(val == 123);
    cb_assert(!safe_strtoul("", &val));  /* empty */
    cb_assert(!safe_strtoul("123BOGUS", &val));  /* non-numeric */
    /* Not sure what it does, but this works with ICC :/
       cb_assert(!safe_strtoul("92837498237498237498029383", &val)); // out of range
    */

    /* extremes: */
    cb_assert(safe_strtoul("4294967295", &val)); /* 2**32 - 1 */
    cb_assert(val == 4294967295L);
    /* This actually works on 64-bit ubuntu
       cb_assert(!safe_strtoul("4294967296", &val)); 2**32
    */
    cb_assert(!safe_strtoul("-1", &val));  /* negative */
    return TEST_PASS;
}


static enum test_return test_safe_strtoull(void) {
    uint64_t val;
    uint64_t exp = -1;
    cb_assert(safe_strtoull("123", &val));
    cb_assert(val == 123);
    cb_assert(safe_strtoull("+123", &val));
    cb_assert(val == 123);
    cb_assert(!safe_strtoull("", &val));  /* empty */
    cb_assert(!safe_strtoull("123BOGUS", &val));  /* non-numeric */
    cb_assert(!safe_strtoull("92837498237498237498029383", &val)); /* out of range */

    /* extremes: */
    cb_assert(safe_strtoull("18446744073709551615", &val)); /* 2**64 - 1 */
    cb_assert(val == exp);
    cb_assert(!safe_strtoull("18446744073709551616", &val)); /* 2**64 */
    cb_assert(!safe_strtoull("-1", &val));  /* negative */
    return TEST_PASS;
}

static enum test_return test_safe_strtoll(void) {
    int64_t val;
    int64_t exp = 1;
    exp <<= 63;
    exp -= 1;
    cb_assert(safe_strtoll("123", &val));
    cb_assert(val == 123);
    cb_assert(safe_strtoll("+123", &val));
    cb_assert(val == 123);
    cb_assert(safe_strtoll("-123", &val));
    cb_assert(val == -123);
    cb_assert(!safe_strtoll("", &val));  /* empty */
    cb_assert(!safe_strtoll("123BOGUS", &val));  /* non-numeric */
    cb_assert(!safe_strtoll("92837498237498237498029383", &val)); /* out of range */

    /* extremes: */
    cb_assert(!safe_strtoll("18446744073709551615", &val)); /* 2**64 - 1 */
    cb_assert(safe_strtoll("9223372036854775807", &val)); /* 2**63 - 1 */

    cb_assert(val == exp); /* 9223372036854775807LL); */
    /*
      cb_assert(safe_strtoll("-9223372036854775808", &val)); // -2**63
      cb_assert(val == -9223372036854775808LL);
    */
    cb_assert(!safe_strtoll("-9223372036854775809", &val)); /* -2**63 - 1 */

    /* We'll allow space to terminate the string.  And leading space. */
    cb_assert(safe_strtoll(" 123 foo", &val));
    cb_assert(val == 123);
    return TEST_PASS;
}

static enum test_return test_safe_strtol(void) {
    int32_t val;
    cb_assert(safe_strtol("123", &val));
    cb_assert(val == 123);
    cb_assert(safe_strtol("+123", &val));
    cb_assert(val == 123);
    cb_assert(safe_strtol("-123", &val));
    cb_assert(val == -123);
    cb_assert(!safe_strtol("", &val));  /* empty */
    cb_assert(!safe_strtol("123BOGUS", &val));  /* non-numeric */
    cb_assert(!safe_strtol("92837498237498237498029383", &val)); /* out of range */

    /* extremes: */
    /* This actually works on 64-bit ubuntu
       cb_assert(!safe_strtol("2147483648", &val)); // (expt 2.0 31.0)
    */
    cb_assert(safe_strtol("2147483647", &val)); /* (- (expt 2.0 31) 1) */
    cb_assert(val == 2147483647L);
    /* This actually works on 64-bit ubuntu
       cb_assert(!safe_strtol("-2147483649", &val)); // (- (expt -2.0 31) 1)
    */

    /* We'll allow space to terminate the string.  And leading space. */
    cb_assert(safe_strtol(" 123 foo", &val));
    cb_assert(val == 123);
    return TEST_PASS;
}

static enum test_return test_safe_strtof(void) {
    float val;
    cb_assert(safe_strtof("123", &val));
    cb_assert(val == 123.00f);
    cb_assert(safe_strtof("+123", &val));
    cb_assert(val == 123.00f);
    cb_assert(safe_strtof("-123", &val));
    cb_assert(val == -123.00f);
    cb_assert(!safe_strtof("", &val));  /* empty */
    cb_assert(!safe_strtof("123BOGUS", &val));  /* non-numeric */

    /* We'll allow space to terminate the string.  And leading space. */
    cb_assert(safe_strtof(" 123 foo", &val));
    cb_assert(val == 123.00f);

    cb_assert(safe_strtof("123.23", &val));
    cb_assert(val == 123.23f);

    cb_assert(safe_strtof("123.00", &val));
    cb_assert(val == 123.00f);

    return TEST_PASS;
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

static void get_working_current_directory(char* out_buf, int out_buf_len) {
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

static cJSON *generate_config(void)
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
    snprintf(rbac_path, sizeof(rbac_path), "%s/%s", pem_path, rbac_file);
    strncat(pem_path, CERTIFICATE_PATH(testapp.pem), 256);
    strncat(cert_path, CERTIFICATE_PATH(testapp.cert), 256);

    cJSON_AddStringToObject(obj, "module", "default_engine.so");
    cJSON_AddItemReferenceToObject(root, "engine", obj);

    obj = cJSON_CreateObject();
    cJSON_AddStringToObject(obj, "module", "blackhole_logger.so");
    cJSON_AddItemToArray(array, obj);
    obj = cJSON_CreateObject();
    cJSON_AddStringToObject(obj, "module", "fragment_rw_ops.so");
    cJSON_AddStringToObject(obj, "config", "r=225;w=226");
    cJSON_AddItemToArray(array, obj);
    obj = cJSON_CreateObject();
    cJSON_AddStringToObject(obj, "module", "testapp_extension.so");
    cJSON_AddItemToArray(array, obj);

    cJSON_AddItemReferenceToObject(root, "extensions", array);

    array = cJSON_CreateArray();
    obj = cJSON_CreateObject();
    obj_ssl = cJSON_CreateObject();

#ifdef WIN32
    cJSON_AddNumberToObject(obj, "port", 11211);
#else
    cJSON_AddNumberToObject(obj, "port", 0);
#endif
    cJSON_AddNumberToObject(obj, "maxconn", MAX_CONNECTIONS);
    cJSON_AddNumberToObject(obj, "backlog", BACKLOG);
    cJSON_AddStringToObject(obj, "host", "*");
    cJSON_AddItemToArray(array, obj);

    if (phase_enabled(phase_ssl)) {
        obj = cJSON_CreateObject();
        cJSON_AddNumberToObject(obj, "port", 11996);
        cJSON_AddNumberToObject(obj, "maxconn", MAX_CONNECTIONS);
        cJSON_AddNumberToObject(obj, "backlog", BACKLOG);
        cJSON_AddStringToObject(obj, "host", "*");
        cJSON_AddItemToObject(obj, "ssl", obj_ssl = cJSON_CreateObject());
        cJSON_AddStringToObject(obj_ssl, "key", pem_path);
        cJSON_AddStringToObject(obj_ssl, "cert", cert_path);
        cJSON_AddItemToArray(array, obj);
    }
    cJSON_AddItemReferenceToObject(root, "interfaces", array);

    cJSON_AddStringToObject(root, "admin", "");
    cJSON_AddTrueToObject(root, "datatype_support");
    cJSON_AddStringToObject(root, "rbac_file", rbac_path);

    return root;
}

static cJSON *generate_rbac_config(void)
{
    cJSON *root = cJSON_CreateObject();
    cJSON *prof;
    cJSON *obj;
    cJSON *array;
    cJSON *array2;

    /* profiles */
    array = cJSON_CreateArray();

    prof = cJSON_CreateObject();
    cJSON_AddStringToObject(prof, "name", "system");
    cJSON_AddStringToObject(prof, "description", "system internal");
    obj = cJSON_CreateObject();
    cJSON_AddStringToObject(obj, "allow", "all");
    cJSON_AddItemReferenceToObject(prof, "memcached", obj);
    cJSON_AddItemToArray(array, prof);

    prof = cJSON_CreateObject();
    cJSON_AddStringToObject(prof, "name", "statistics");
    cJSON_AddStringToObject(prof, "description", "only stat and assume");
    obj = cJSON_CreateObject();

    array2 = cJSON_CreateArray();
    cJSON_AddItemToArray(array2, cJSON_CreateString("stat"));
    cJSON_AddItemToArray(array2, cJSON_CreateString("assume_role"));
    cJSON_AddItemReferenceToObject(obj, "allow", array2);
    cJSON_AddItemReferenceToObject(prof, "memcached", obj);
    cJSON_AddItemToArray(array, prof);

    cJSON_AddItemReferenceToObject(root, "profiles", array);

    /* roles */
    array = cJSON_CreateArray();
    prof = cJSON_CreateObject();
    cJSON_AddStringToObject(prof, "name", "statistics");
    cJSON_AddStringToObject(prof, "profiles", "statistics");

    cJSON_AddItemToArray(array, prof);
    cJSON_AddItemReferenceToObject(root, "roles", array);

    /* users */
    array = cJSON_CreateArray();
    prof = cJSON_CreateObject();
    cJSON_AddStringToObject(prof, "login", "*");
    cJSON_AddStringToObject(prof, "profiles", "system");
    cJSON_AddStringToObject(prof, "roles", "statistics");

    cJSON_AddItemToArray(array, prof);
    cJSON_AddItemReferenceToObject(root, "users", array);

    return root;
}

static int write_config_to_file(const char* config, const char *fname) {
    FILE *fp;
    if ((fp = fopen(fname, "w")) == NULL) {
        return -1;
    } else {
        fprintf(fp, "%s", config);
        fclose(fp);
    }

    return 0;
}

#ifdef WIN32
static HANDLE start_server(in_port_t *port_out, in_port_t *ssl_port_out, bool daemon, int timeout) {
    STARTUPINFO sinfo;
    PROCESS_INFORMATION pinfo;
    char *commandline = malloc(1024);
    char env[80];
    sprintf_s(env, sizeof(env), "MEMCACHED_PARENT_MONITOR=%u", GetCurrentProcessId());
    putenv(env);

    memset(&sinfo, 0, sizeof(sinfo));
    memset(&pinfo, 0, sizeof(pinfo));
    sinfo.cb = sizeof(sinfo);

    sprintf(commandline, "memcached.exe -C %s", config_file);

    if (!CreateProcess("memcached.exe",
                       commandline,
                       NULL, NULL, FALSE, CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW, NULL, NULL, &sinfo, &pinfo)) {
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
    /* Do a short sleep to let the other process to start */
    Sleep(1);
    CloseHandle(pinfo.hThread);

    *port_out = 11211;
    *ssl_port_out = 11996;
    return pinfo.hProcess;
}
#else
/**
 * Function to start the server and let it listen on a random port
 *
 * @param port_out where to store the TCP port number the server is
 *                 listening on
 * @param daemon set to true if you want to run the memcached server
 *               as a daemon process
 * @return the pid of the memcached server
 */

static pid_t start_server(in_port_t *port_out, in_port_t *ssl_port_out, bool daemon, int timeout) {
    char environment[80];
    char *filename= environment + strlen("MEMCACHED_PORT_FILENAME=");
#ifdef __sun
    char coreadm[128];
#endif
    pid_t pid;
    FILE *fp;
    char buffer[80];

    char env[80];
    snprintf(env, sizeof(env), "MEMCACHED_PARENT_MONITOR=%lu", (unsigned long)getpid());
    putenv(env);

    snprintf(environment, sizeof(environment),
             "MEMCACHED_PORT_FILENAME=/tmp/ports.%lu", (long)getpid());
    remove(filename);

#ifdef __sun
    /* I want to name the corefiles differently so that they don't
       overwrite each other
    */
    snprintf(coreadm, sizeof(coreadm),
             "coreadm -p core.%%f.%%p %lu", (unsigned long)getpid());
    system(coreadm);
#endif

    pid = fork();
    cb_assert(pid != -1);

    if (pid == 0) {
        /* Child */
        char *argv[20];
        int arg = 0;
        char tmo[24];

        snprintf(tmo, sizeof(tmo), "%u", timeout);
        putenv(environment);

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
        argv[arg++] = (char*)config_file;

        argv[arg++] = NULL;
        cb_assert(execvp(argv[0], argv) != -1);
    }

    /* Yeah just let us "busy-wait" for the file to be created ;-) */
    while (access(filename, F_OK) == -1) {
        usleep(10);
    }

    fp = fopen(filename, "r");
    if (fp == NULL) {
        fprintf(stderr, "Failed to open the file containing port numbers: %s\n",
                strerror(errno));
        cb_assert(false);
    }

    *port_out = (in_port_t)-1;
    *ssl_port_out = (in_port_t)-1;

    while ((fgets(buffer, sizeof(buffer), fp)) != NULL) {
        if (strncmp(buffer, "TCP INET: ", 10) == 0) {
            int32_t val;
            cb_assert(safe_strtol(buffer + 10, &val));
            if (*port_out == (in_port_t)-1) {
                *port_out = (in_port_t)val;
            } else {
                *ssl_port_out = (in_port_t)val;
            }
        }
    }
    fclose(fp);
    cb_assert(remove(filename) == 0);

    return pid;
}
#endif

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



static SOCKET create_connect_plain_socket(const char *hostname, in_port_t port, bool nonblock)
{
    struct addrinfo *ai = lookuphost(hostname, port);
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

static SOCKET create_connect_ssl_socket(const char *hostname, in_port_t port, bool nonblocking) {
    char port_str[32];
    int sfd = 0;
    BIO* temp_bio = NULL;

    snprintf(port_str, 32, "%d", port);
    create_ssl_connection(&ssl_ctx, &temp_bio, hostname, port_str, NULL, NULL, 1);

    if (ssl_bio_r) {
        BIO_free(ssl_bio_r);
    }
    if (ssl_bio_w) {
        BIO_free(ssl_bio_w);
    }

    /* SSL "trickery". To ensure we have full control over send/receive of data.
       create_ssl_connection will have negotiated the SSL connection, now:
       1. steal the underlying FD
       2. Switch out the BIO_ssl_connect BIO for a plain memory BIO

       Now send/receive is done under our control. byte by byte, large chunks etc...
    */
    sfd = BIO_get_fd(temp_bio, NULL);
    BIO_get_ssl(temp_bio, &ssl);
    ssl_bio_r = BIO_new(BIO_s_mem());
    ssl_bio_w = BIO_new(BIO_s_mem());
    SSL_set_bio(ssl, ssl_bio_r, ssl_bio_w);
    return sfd;
}

static void connect_to_server_plain(in_port_t port, bool nonblocking) {
    sock = create_connect_plain_socket("127.0.0.1", port, nonblocking);

    if (nonblocking) {
        if (evutil_make_socket_nonblocking(sock) == -1) {
            fprintf(stderr, "evutil_make_socket_nonblocking failed\n");
            abort();
        }
    }
}

static void connect_to_server_ssl(in_port_t ssl_port, bool nonblocking) {
    sock_ssl = create_connect_ssl_socket("127.0.0.1", ssl_port, nonblocking);

    if (nonblocking) {
        if (evutil_make_socket_nonblocking(sock_ssl) == -1) {
            fprintf(stderr, "evutil_make_socket_nonblocking failed\n");
            abort();
        }
    }
}

/*
    re-connect to server.
    Uses global port and ssl_port values.
    New socket-fd written to global "sock" and "ssl_bio"
*/
static void reconnect_to_server(bool nonblocking) {
    if (current_phase == phase_ssl) {
        closesocket(sock_ssl);
        connect_to_server_ssl(ssl_port, nonblocking);
    } else {
        closesocket(sock);
        connect_to_server_plain(port, nonblocking);
    }
}

static enum test_return test_vperror(void) {
#ifdef WIN32
    return TEST_SKIP;
#else
    int rv = 0;
    int oldstderr = dup(STDERR_FILENO);
    char tmpl[sizeof(TMP_TEMPLATE)+1];
    int newfile;
    char buf[80] = {0};
    FILE *efile;
    char *prv;
    char expected[80] = {0};

    strncpy(tmpl, TMP_TEMPLATE, sizeof(TMP_TEMPLATE)+1);

    newfile = mkstemp(tmpl);
    cb_assert(newfile > 0);
    rv = dup2(newfile, STDERR_FILENO);
    cb_assert(rv == STDERR_FILENO);
    rv = close(newfile);
    cb_assert(rv == 0);

    errno = EIO;
    vperror("Old McDonald had a farm.  %s", "EI EIO");

    /* Restore stderr */
    rv = dup2(oldstderr, STDERR_FILENO);
    cb_assert(rv == STDERR_FILENO);


    /* Go read the file */
    efile = fopen(tmpl, "r");
    cb_assert(efile);
    prv = fgets(buf, sizeof(buf), efile);
    cb_assert(prv);
    fclose(efile);

    unlink(tmpl);

    snprintf(expected, sizeof(expected),
             "Old McDonald had a farm.  EI EIO: %s\n", strerror(EIO));

    /*
    fprintf(stderr,
            "\nExpected:  ``%s''"
            "\nGot:       ``%s''\n", expected, buf);
    */

    return strcmp(expected, buf) == 0 ? TEST_PASS : TEST_FAIL;
#endif
}

static char* trim(char* ptr) {
    char *start = ptr;
    char *end;

    while (isspace(*start)) {
        ++start;
    }
    end = start + strlen(start) - 1;
    if (end != start) {
        while (isspace(*end)) {
            *end = '\0';
            --end;
        }
    }
    return start;
}

static enum test_return test_config_parser(void) {
    bool bool_val = false;
    size_t size_val = 0;
    ssize_t ssize_val = 0;
    float float_val = 0;
    char *string_val = 0;
    int ii;
    char buffer[1024];
    FILE *cfg;
    char outfile[sizeof(TMP_TEMPLATE)+1];
    char cfgfile[sizeof(TMP_TEMPLATE)+1];
    FILE *error;

    /* Set up the different items I can handle */
    struct config_item items[7];
    memset(&items, 0, sizeof(items));
    ii = 0;
    items[ii].key = "bool";
    items[ii].datatype = DT_BOOL;
    items[ii].value.dt_bool = &bool_val;
    ++ii;

    items[ii].key = "size_t";
    items[ii].datatype = DT_SIZE;
    items[ii].value.dt_size = &size_val;
    ++ii;

    items[ii].key = "ssize_t";
    items[ii].datatype = DT_SSIZE;
    items[ii].value.dt_ssize = &ssize_val;
    ++ii;

    items[ii].key = "float";
    items[ii].datatype = DT_FLOAT;
    items[ii].value.dt_float = &float_val;
    ++ii;

    items[ii].key = "string";
    items[ii].datatype = DT_STRING;
    items[ii].value.dt_string = &string_val;
    ++ii;

    items[ii].key = "config_file";
    items[ii].datatype = DT_CONFIGFILE;
    ++ii;

    items[ii].key = NULL;
    ++ii;

    cb_assert(ii == 7);
    strncpy(outfile, TMP_TEMPLATE, sizeof(TMP_TEMPLATE)+1);
    strncpy(cfgfile, TMP_TEMPLATE, sizeof(TMP_TEMPLATE)+1);

    assert(cb_mktemp(outfile) != NULL);
    error = fopen(outfile, "w");

    cb_assert(error != NULL);
    cb_assert(parse_config("", items, error) == 0);
    /* Nothing should be found */
    for (ii = 0; ii < 5; ++ii) {
        cb_assert(!items[0].found);
    }

    cb_assert(parse_config("bool=true", items, error) == 0);
    cb_assert(bool_val);
    /* only bool should be found */
    cb_assert(items[0].found);
    items[0].found = false;
    for (ii = 0; ii < 5; ++ii) {
        cb_assert(!items[0].found);
    }

    /* It should allow illegal keywords */
    cb_assert(parse_config("pacman=dead", items, error) == 1);
    /* and illegal values */
    cb_assert(parse_config("bool=12", items, error) == -1);
    cb_assert(!items[0].found);
    /* and multiple occurences of the same value */
    cb_assert(parse_config("size_t=1; size_t=1024", items, error) == 0);
    cb_assert(items[1].found);
    cb_assert(size_val == 1024);
    items[1].found = false;

    /* Empty string */
    /* XXX:  This test fails on Linux, but works on OS X.
    cb_assert(parse_config("string=", items, error) == 0);
    cb_assert(items[4].found);
    cb_assert(strcmp(string_val, "") == 0);
    items[4].found = false;
    */
    /* Plain string */
    cb_assert(parse_config("string=sval", items, error) == 0);
    cb_assert(items[4].found);
    cb_assert(strcmp(string_val, "sval") == 0);
    items[4].found = false;
    /* Leading space */
    cb_assert(parse_config("string= sval", items, error) == 0);
    cb_assert(items[4].found);
    cb_assert(strcmp(string_val, "sval") == 0);
    items[4].found = false;
    /* Escaped leading space */
    cb_assert(parse_config("string=\\ sval", items, error) == 0);
    cb_assert(items[4].found);
    cb_assert(strcmp(string_val, " sval") == 0);
    items[4].found = false;
    /* trailing space */
    cb_assert(parse_config("string=sval ", items, error) == 0);
    cb_assert(items[4].found);
    cb_assert(strcmp(string_val, "sval") == 0);
    items[4].found = false;
    /* escaped trailing space */
    cb_assert(parse_config("string=sval\\ ", items, error) == 0);
    cb_assert(items[4].found);
    cb_assert(strcmp(string_val, "sval ") == 0);
    items[4].found = false;
    /* escaped stop char */
    cb_assert(parse_config("string=sval\\;blah=x", items, error) == 0);
    cb_assert(items[4].found);
    cb_assert(strcmp(string_val, "sval;blah=x") == 0);
    items[4].found = false;
    /* middle space */
    cb_assert(parse_config("string=s val", items, error) == 0);
    cb_assert(items[4].found);
    cb_assert(strcmp(string_val, "s val") == 0);
    items[4].found = false;

    /* And all of the variables */
    cb_assert(parse_config("bool=true;size_t=1024;float=12.5;string=somestr",
                        items, error) == 0);
    cb_assert(bool_val);
    cb_assert(size_val == 1024);
    cb_assert(float_val == 12.5f);
    cb_assert(strcmp(string_val, "somestr") == 0);
    for (ii = 0; ii < 5; ++ii) {
        items[ii].found = false;
    }

    cb_assert(parse_config("size_t=1k", items, error) == 0);
    cb_assert(items[1].found);
    cb_assert(size_val == 1024);
    items[1].found = false;
    cb_assert(parse_config("size_t=1m", items, error) == 0);
    cb_assert(items[1].found);
    cb_assert(size_val == 1024*1024);
    items[1].found = false;
    cb_assert(parse_config("size_t=1g", items, error) == 0);
    cb_assert(items[1].found);
    cb_assert(size_val == 1024*1024*1024);
    items[1].found = false;
    cb_assert(parse_config("size_t=1K", items, error) == 0);
    cb_assert(items[1].found);
    cb_assert(size_val == 1024);
    items[1].found = false;
    cb_assert(parse_config("size_t=1M", items, error) == 0);
    cb_assert(items[1].found);
    cb_assert(size_val == 1024*1024);
    items[1].found = false;
    cb_assert(parse_config("size_t=1G", items, error) == 0);
    cb_assert(items[1].found);
    cb_assert(size_val == 1024*1024*1024);
    items[1].found = false;

    cb_assert(cb_mktemp(cfgfile) != NULL);
    cfg = fopen(cfgfile, "w");
    cb_assert(cfg != NULL);
    fprintf(cfg, "# This is a config file\nbool=true\nsize_t=1023\nfloat=12.4\n");
    fclose(cfg);
    sprintf(buffer, "config_file=%s", cfgfile);
    cb_assert(parse_config(buffer, items, error) == 0);
    cb_assert(bool_val);
    cb_assert(size_val == 1023);
    cb_assert(float_val == 12.4f);
    fclose(error);

    remove(cfgfile);
    /* Verify that I received the error messages ;-) */
    error = fopen(outfile, "r");
    cb_assert(error);

    cb_assert(fgets(buffer, sizeof(buffer), error));
    cb_assert(strcmp("Unsupported key: <pacman>", trim(buffer)) == 0);
    cb_assert(fgets(buffer, sizeof(buffer), error));
    cb_assert(strcmp("Invalid entry, Key: <bool> Value: <12>", trim(buffer)) == 0);
    cb_assert(fgets(buffer, sizeof(buffer), error));
    cb_assert(strcmp("WARNING: Found duplicate entry for \"size_t\"", trim(buffer)) == 0);
    cb_assert(fgets(buffer, sizeof(buffer), error) == NULL);

    remove(outfile);
    return TEST_PASS;
}

static char *isasl_file;

static enum test_return start_memcached_server(void) {
    cJSON *rbac = generate_rbac_config();
    char *rbac_text = cJSON_Print(rbac);

    if (cb_mktemp(rbac_file) == NULL) {
        return TEST_FAIL;
    }
    if (write_config_to_file(rbac_text, rbac_file) == -1) {
        return TEST_FAIL;
    }
    cJSON_Free(rbac_text);
    cJSON_Delete(rbac);

    json_config = generate_config();
    config_string = cJSON_Print(json_config);
    if (cb_mktemp(config_file) == NULL) {
        return TEST_FAIL;
    }
    if (write_config_to_file(config_string, config_file) == -1) {
        return TEST_FAIL;
    }

    char fname[1024];
    snprintf(fname, sizeof(fname), "isasl.%lu.%lu.pw",
             (unsigned long)getpid(),
             (unsigned long)time(NULL));
    isasl_file = strdup(fname);
    cb_assert(isasl_file != NULL);

    FILE *fp = fopen(isasl_file, "w");
    cb_assert(fp != NULL);
    fprintf(fp, "_admin password \n");
    fclose(fp);
    char env[1024];
    snprintf(env, sizeof(env), "ISASL_PWFILE=%s", isasl_file);
    putenv(strdup(env));

    server_start_time = time(0);
    server_pid = start_server(&port, &ssl_port, false, 600);
    return TEST_PASS;
}

static enum test_return test_connect_to_server(void) {
    if (current_phase == phase_ssl) {
        connect_to_server_ssl(ssl_port, false);
    } else {
        connect_to_server_plain(port, false);
    }
    return TEST_PASS;
}

static enum test_return stop_memcached_server(void) {
    closesocket(sock);
    sock = INVALID_SOCKET;
#ifdef WIN32
    TerminateProcess(server_pid, 0);
#else
    if (kill(server_pid, SIGTERM) == 0) {
        /* Wait for the process to be gone... */
        while (kill(server_pid, 0) == 0) {
            sleep(1);
            waitpid(server_pid, NULL, WNOHANG);
        }
    }
#endif

    remove(config_file);
    remove(isasl_file);
    free(isasl_file);
    remove(rbac_file);

    return TEST_PASS;
}

static ssize_t phase_send(const void *buf, size_t len) {
    ssize_t rv = 0, send_rv = 0;
    if (current_phase == phase_ssl) {
        long send_len = 0;
        char *send_buf = NULL;
        /* push the data through SSL into the BIO */
        rv = (ssize_t)SSL_write(ssl, (const char*)buf, len);
        send_len = BIO_get_mem_data(ssl_bio_w, &send_buf);

#ifdef WIN32
        send_rv = send(sock_ssl, send_buf, (int)send_len, 0);
#else
        send_rv = send(sock_ssl, send_buf, send_len, 0);
#endif

        if (send_rv > 0) {
            cb_assert(send_len == send_rv);
            (void)BIO_reset(ssl_bio_w);
        } else {
            /* flag failure to user */
            rv = send_rv;
        }
    } else {
#ifdef WIN32
        rv = send(sock, buf, (int)len, 0);
#else
        rv = send(sock, buf, len, 0);
#endif
    }
    return rv;
}

static ssize_t phase_recv(void *buf, size_t len) {

    ssize_t rv = 0;
    if (current_phase == phase_ssl) {
        /* can we read some data? */
        while((rv = SSL_peek(ssl, buf, len)) == -1)
        {
            /* nope, keep feeding SSL until we can */
#ifdef WIN32
            rv = recv(sock_ssl, buf, (int)len, 0);
#else
            rv = recv(sock_ssl, buf, len, 0);
#endif

            if(rv > 0) {
                /* write into the BIO what came off the network */
                BIO_write(ssl_bio_r, buf, rv);
            } else if(rv == 0) {
                return rv; /* peer closed */
            }
        }
        /* now pull the data out and return */
        rv = SSL_read(ssl, buf, len);
    }
    else {
#ifdef WIN32
        rv = recv(sock, buf, (int)len, 0);
#else
        rv = recv(sock, buf, len, 0);
#endif
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

static void safe_send(const void* buf, size_t len, bool hickup)
{
    off_t offset = 0;
    const char* ptr = buf;
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
    off_t offset = 0;
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

static bool safe_recv_packet(void *buf, size_t size) {
    protocol_binary_response_no_extras *response = buf;
    char *ptr;
    size_t len;

    cb_assert(size >= sizeof(*response));
    if (!safe_recv(response, sizeof(*response))) {
        return false;
    }
    response->message.header.response.keylen = ntohs(response->message.header.response.keylen);
    response->message.header.response.status = ntohs(response->message.header.response.status);
    response->message.header.response.bodylen = ntohl(response->message.header.response.bodylen);

    len = sizeof(*response);
    ptr = buf;
    ptr += len;
    cb_assert(size >= (sizeof(*response) + response->message.header.response.bodylen));
    if (!safe_recv(ptr, response->message.header.response.bodylen)) {
        return false;
    }

    return true;
}

static off_t storage_command(char*buf,
                             size_t bufsz,
                             uint8_t cmd,
                             const void* key,
                             size_t keylen,
                             const void* dta,
                             size_t dtalen,
                             uint32_t flags,
                             uint32_t exp) {
    /* all of the storage commands use the same command layout */
    off_t key_offset;
    protocol_binary_request_set *request = (void*)buf;
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

    return (off_t)(key_offset + keylen + dtalen);
}

static off_t raw_command(char* buf,
                         size_t bufsz,
                         uint8_t cmd,
                         const void* key,
                         size_t keylen,
                         const void* dta,
                         size_t dtalen) {
    /* all of the storage commands use the same command layout */
    off_t key_offset;
    protocol_binary_request_no_extras *request = (void*)buf;
    cb_assert(bufsz >= sizeof(*request) + keylen + dtalen);

    memset(request, 0, sizeof(*request));
    if (cmd == read_command || cmd == write_command) {
        request->message.header.request.extlen = 8;
    } else if (cmd == PROTOCOL_BINARY_CMD_AUDIT_PUT) {
        request->message.header.request.extlen = 4;
    }
    request->message.header.request.magic = PROTOCOL_BINARY_REQ;
    request->message.header.request.opcode = cmd;
    request->message.header.request.keylen = htons((uint16_t)keylen);
    request->message.header.request.bodylen = htonl((uint32_t)(keylen + dtalen + request->message.header.request.extlen));
    request->message.header.request.opaque = 0xdeadbeef;

    key_offset = sizeof(protocol_binary_request_no_extras) +
        request->message.header.request.extlen;

    if (key != NULL) {
        memcpy(buf + key_offset, key, keylen);
    }
    if (dta != NULL) {
        memcpy(buf + key_offset + keylen, dta, dtalen);
    }

    return (off_t)(sizeof(*request) + keylen + dtalen + request->message.header.request.extlen);
}

static off_t flush_command(char* buf, size_t bufsz, uint8_t cmd, uint32_t exptime, bool use_extra) {
    off_t size;
    protocol_binary_request_flush *request = (void*)buf;
    cb_assert(bufsz > sizeof(*request));

    memset(request, 0, sizeof(*request));
    request->message.header.request.magic = PROTOCOL_BINARY_REQ;
    request->message.header.request.opcode = cmd;

    size = sizeof(protocol_binary_request_no_extras);
    if (use_extra) {
        request->message.header.request.extlen = 4;
        request->message.body.expiration = htonl(exptime);
        request->message.header.request.bodylen = htonl(4);
        size += 4;
    }

    request->message.header.request.opaque = 0xdeadbeef;

    return size;
}

static off_t arithmetic_command(char* buf,
                                size_t bufsz,
                                uint8_t cmd,
                                const void* key,
                                size_t keylen,
                                uint64_t delta,
                                uint64_t initial,
                                uint32_t exp) {
    off_t key_offset;
    protocol_binary_request_incr *request = (void*)buf;
    cb_assert(bufsz > sizeof(*request) + keylen);

    memset(request, 0, sizeof(*request));
    request->message.header.request.magic = PROTOCOL_BINARY_REQ;
    request->message.header.request.opcode = cmd;
    request->message.header.request.keylen = htons((uint16_t)keylen);
    request->message.header.request.extlen = 20;
    request->message.header.request.bodylen = htonl((uint32_t)(keylen + 20));
    request->message.header.request.opaque = 0xdeadbeef;
    request->message.body.delta = htonll(delta);
    request->message.body.initial = htonll(initial);
    request->message.body.expiration = htonl(exp);

    key_offset = sizeof(protocol_binary_request_no_extras) + 20;

    memcpy(buf + key_offset, key, keylen);
    return (off_t)(key_offset + keylen);
}

static void validate_response_header(protocol_binary_response_no_extras *response,
                                     uint8_t cmd, uint16_t status)
{
    cb_assert(response->message.header.response.magic == PROTOCOL_BINARY_RES);
    cb_assert(response->message.header.response.opcode == cmd);
    cb_assert(response->message.header.response.datatype == PROTOCOL_BINARY_RAW_BYTES);
    if (status == PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND) {
        if (response->message.header.response.status == PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED) {
            response->message.header.response.status = PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND;
        }
    }
    cb_assert(response->message.header.response.status == status);
    cb_assert(response->message.header.response.opaque == 0xdeadbeef);

    if (status == PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        switch (cmd) {
        case PROTOCOL_BINARY_CMD_ADDQ:
        case PROTOCOL_BINARY_CMD_APPENDQ:
        case PROTOCOL_BINARY_CMD_DECREMENTQ:
        case PROTOCOL_BINARY_CMD_DELETEQ:
        case PROTOCOL_BINARY_CMD_FLUSHQ:
        case PROTOCOL_BINARY_CMD_INCREMENTQ:
        case PROTOCOL_BINARY_CMD_PREPENDQ:
        case PROTOCOL_BINARY_CMD_QUITQ:
        case PROTOCOL_BINARY_CMD_REPLACEQ:
        case PROTOCOL_BINARY_CMD_SETQ:
            cb_assert("Quiet command shouldn't return on success" == NULL);
        default:
            break;
        }

        switch (cmd) {
        case PROTOCOL_BINARY_CMD_ADD:
        case PROTOCOL_BINARY_CMD_REPLACE:
        case PROTOCOL_BINARY_CMD_SET:
        case PROTOCOL_BINARY_CMD_APPEND:
        case PROTOCOL_BINARY_CMD_PREPEND:
            cb_assert(response->message.header.response.keylen == 0);
            cb_assert(response->message.header.response.extlen == 0);
            cb_assert(response->message.header.response.bodylen == 0);
            cb_assert(response->message.header.response.cas != 0);
            break;
        case PROTOCOL_BINARY_CMD_FLUSH:
        case PROTOCOL_BINARY_CMD_NOOP:
        case PROTOCOL_BINARY_CMD_QUIT:
        case PROTOCOL_BINARY_CMD_DELETE:
            cb_assert(response->message.header.response.keylen == 0);
            cb_assert(response->message.header.response.extlen == 0);
            cb_assert(response->message.header.response.bodylen == 0);
            break;

        case PROTOCOL_BINARY_CMD_DECREMENT:
        case PROTOCOL_BINARY_CMD_INCREMENT:
            cb_assert(response->message.header.response.keylen == 0);
            cb_assert(response->message.header.response.extlen == 0);
            cb_assert(response->message.header.response.bodylen == 8);
            cb_assert(response->message.header.response.cas != 0);
            break;

        case PROTOCOL_BINARY_CMD_STAT:
            cb_assert(response->message.header.response.extlen == 0);
            /* key and value exists in all packets except in the terminating */
            cb_assert(response->message.header.response.cas == 0);
            break;

        case PROTOCOL_BINARY_CMD_VERSION:
            cb_assert(response->message.header.response.keylen == 0);
            cb_assert(response->message.header.response.extlen == 0);
            cb_assert(response->message.header.response.bodylen != 0);
            cb_assert(response->message.header.response.cas == 0);
            break;

        case PROTOCOL_BINARY_CMD_GET:
        case PROTOCOL_BINARY_CMD_GETQ:
            cb_assert(response->message.header.response.keylen == 0);
            cb_assert(response->message.header.response.extlen == 4);
            cb_assert(response->message.header.response.cas != 0);
            break;

        case PROTOCOL_BINARY_CMD_GETK:
        case PROTOCOL_BINARY_CMD_GETKQ:
            cb_assert(response->message.header.response.keylen != 0);
            cb_assert(response->message.header.response.extlen == 4);
            cb_assert(response->message.header.response.cas != 0);
            break;

        default:
            /* Undefined command code */
            break;
        }
    } else {
        cb_assert(response->message.header.response.cas == 0);
        cb_assert(response->message.header.response.extlen == 0);
        if (cmd != PROTOCOL_BINARY_CMD_GETK) {
            cb_assert(response->message.header.response.keylen == 0);
        }
    }
}

static enum test_return test_noop(void) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;

    size_t len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                             PROTOCOL_BINARY_CMD_NOOP,
                             NULL, 0, NULL, 0);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    validate_response_header(&buffer.response, PROTOCOL_BINARY_CMD_NOOP,
                             PROTOCOL_BINARY_RESPONSE_SUCCESS);

    return TEST_PASS;
}

static enum test_return test_quit_impl(uint8_t cmd) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;
    size_t len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                             cmd, NULL, 0, NULL, 0);

    safe_send(buffer.bytes, len, false);
    if (cmd == PROTOCOL_BINARY_CMD_QUIT) {
        safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
        validate_response_header(&buffer.response, PROTOCOL_BINARY_CMD_QUIT,
                                 PROTOCOL_BINARY_RESPONSE_SUCCESS);
    }

    /* Socket should be closed now, read should return 0 */
    cb_assert(phase_recv(buffer.bytes, sizeof(buffer.bytes)) == 0);

    reconnect_to_server(false);

    return TEST_PASS;
}

static enum test_return test_quit(void) {
    return test_quit_impl(PROTOCOL_BINARY_CMD_QUIT);
}

static enum test_return test_quitq(void) {
    return test_quit_impl(PROTOCOL_BINARY_CMD_QUITQ);
}

static enum test_return test_set_impl(const char *key, uint8_t cmd) {
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
            validate_response_header(&receive.response, cmd,
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
        validate_response_header(&receive.response, cmd,
                                 PROTOCOL_BINARY_RESPONSE_SUCCESS);
        cb_assert(receive.response.message.header.response.cas != send.request.message.header.request.cas);
    } else {
        return test_noop();
    }

    return TEST_PASS;
}

static enum test_return test_set(void) {
    return test_set_impl("test_set", PROTOCOL_BINARY_CMD_SET);
}

static enum test_return test_setq(void) {
    return test_set_impl("test_setq", PROTOCOL_BINARY_CMD_SETQ);
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
                validate_response_header(&receive.response, cmd,
                                         PROTOCOL_BINARY_RESPONSE_SUCCESS);
            }
        } else {
            safe_recv_packet(receive.bytes, sizeof(receive.bytes));
            validate_response_header(&receive.response, cmd,
                                     PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS);
        }
    }

    /* And verify that it doesn't work with the "correct" CAS */
    /* value */
    send.request.message.header.request.cas = receive.response.message.header.response.cas;
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    validate_response_header(&receive.response, cmd,
                             PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS);
    return TEST_PASS;
}

static enum test_return test_add(void) {
    return test_add_impl("test_add", PROTOCOL_BINARY_CMD_ADD);
}

static enum test_return test_addq(void) {
    return test_add_impl("test_addq", PROTOCOL_BINARY_CMD_ADDQ);
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
    validate_response_header(&receive.response, cmd,
                             PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);
    len = storage_command(send.bytes, sizeof(send.bytes),
                          PROTOCOL_BINARY_CMD_ADD,
                          key, strlen(key), &value, sizeof(value), 0, 0);
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    validate_response_header(&receive.response, PROTOCOL_BINARY_CMD_ADD,
                             PROTOCOL_BINARY_RESPONSE_SUCCESS);

    len = storage_command(send.bytes, sizeof(send.bytes), cmd,
                          key, strlen(key), &value, sizeof(value), 0, 0);
    for (ii = 0; ii < 10; ++ii) {
        safe_send(send.bytes, len, false);
        if (cmd == PROTOCOL_BINARY_CMD_REPLACE) {
            safe_recv_packet(receive.bytes, sizeof(receive.bytes));
            validate_response_header(&receive.response,
                                     PROTOCOL_BINARY_CMD_REPLACE,
                                     PROTOCOL_BINARY_RESPONSE_SUCCESS);
        }
    }

    if (cmd == PROTOCOL_BINARY_CMD_REPLACEQ) {
        test_noop();
    }

    return TEST_PASS;
}

static enum test_return test_replace(void) {
    return test_replace_impl("test_replace",
                                    PROTOCOL_BINARY_CMD_REPLACE);
}

static enum test_return test_replaceq(void) {
    return test_replace_impl("test_replaceq",
                                    PROTOCOL_BINARY_CMD_REPLACEQ);
}

static enum test_return test_delete_impl(const char *key, uint8_t cmd) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } send, receive;
    size_t len = raw_command(send.bytes, sizeof(send.bytes), cmd,
                             key, strlen(key), NULL, 0);

    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    validate_response_header(&receive.response, cmd,
                             PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);
    len = storage_command(send.bytes, sizeof(send.bytes),
                          PROTOCOL_BINARY_CMD_ADD,
                          key, strlen(key), NULL, 0, 0, 0);
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    validate_response_header(&receive.response, PROTOCOL_BINARY_CMD_ADD,
                             PROTOCOL_BINARY_RESPONSE_SUCCESS);

    len = raw_command(send.bytes, sizeof(send.bytes),
                      cmd, key, strlen(key), NULL, 0);
    safe_send(send.bytes, len, false);

    if (cmd == PROTOCOL_BINARY_CMD_DELETE) {
        safe_recv_packet(receive.bytes, sizeof(receive.bytes));
        validate_response_header(&receive.response, PROTOCOL_BINARY_CMD_DELETE,
                                 PROTOCOL_BINARY_RESPONSE_SUCCESS);
    }

    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    validate_response_header(&receive.response, cmd,
                             PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);

    return TEST_PASS;
}

static enum test_return test_delete(void) {
    return test_delete_impl("test_delete",
                                   PROTOCOL_BINARY_CMD_DELETE);
}

static enum test_return test_deleteq(void) {
    return test_delete_impl("test_deleteq",
                                   PROTOCOL_BINARY_CMD_DELETEQ);
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
    validate_response_header(&receive.response, PROTOCOL_BINARY_CMD_SET,
                             PROTOCOL_BINARY_RESPONSE_SUCCESS);
    len = raw_command(send.bytes, sizeof(send.bytes),
                       PROTOCOL_BINARY_CMD_DELETE, key, strlen(key), NULL, 0);

    send.request.message.header.request.cas = receive.response.message.header.response.cas;
    if (bad) {
        ++send.request.message.header.request.cas;
    }
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    if (bad) {
        validate_response_header(&receive.response, PROTOCOL_BINARY_CMD_DELETE,
                                 PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS);
    } else {
        validate_response_header(&receive.response, PROTOCOL_BINARY_CMD_DELETE,
                                 PROTOCOL_BINARY_RESPONSE_SUCCESS);
    }

    return TEST_PASS;
}


static enum test_return test_delete_cas(void) {
    return test_delete_cas_impl("test_delete_cas", false);
}

static enum test_return test_delete_bad_cas(void) {
    return test_delete_cas_impl("test_delete_bad_cas", true);
}

static enum test_return test_get_impl(const char *key, uint8_t cmd) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } send, receive;
    int ii;
    size_t len = raw_command(send.bytes, sizeof(send.bytes), cmd,
                             key, strlen(key), NULL, 0);

    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    validate_response_header(&receive.response, cmd,
                             PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);

    len = storage_command(send.bytes, sizeof(send.bytes),
                          PROTOCOL_BINARY_CMD_ADD,
                          key, strlen(key), NULL, 0,
                          0, 0);
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    validate_response_header(&receive.response, PROTOCOL_BINARY_CMD_ADD,
                             PROTOCOL_BINARY_RESPONSE_SUCCESS);

    /* run a little pipeline test ;-) */
    len = 0;
    for (ii = 0; ii < 10; ++ii) {
        union {
            protocol_binary_request_no_extras request;
            char bytes[1024];
        } temp;
        size_t l = raw_command(temp.bytes, sizeof(temp.bytes),
                               cmd, key, strlen(key), NULL, 0);
        memcpy(send.bytes + len, temp.bytes, l);
        len += l;
    }

    safe_send(send.bytes, len, false);
    for (ii = 0; ii < 10; ++ii) {
        safe_recv_packet(receive.bytes, sizeof(receive.bytes));
        validate_response_header(&receive.response, cmd,
                                 PROTOCOL_BINARY_RESPONSE_SUCCESS);
    }

    return TEST_PASS;
}

static enum test_return test_get(void) {
    return test_get_impl("test_get", PROTOCOL_BINARY_CMD_GET);
}

static enum test_return test_getk(void) {
    return test_get_impl("test_getk", PROTOCOL_BINARY_CMD_GETK);
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
    size_t len2 = raw_command(temp.bytes, sizeof(temp.bytes), cmd,
                             missing, strlen(missing), NULL, 0);
    /* I need to change the first opaque so that I can separate the two
     * return packets */
    temp.request.message.header.request.opaque = 0xfeedface;
    memcpy(send.bytes + len, temp.bytes, len2);
    len += len2;

    len2 = raw_command(temp.bytes, sizeof(temp.bytes), cmd,
                       key, strlen(key), NULL, 0);
    memcpy(send.bytes + len, temp.bytes, len2);
    len += len2;

    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    validate_response_header(&receive.response, PROTOCOL_BINARY_CMD_ADD,
                             PROTOCOL_BINARY_RESPONSE_SUCCESS);
    /* The first GETQ shouldn't return anything */
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    validate_response_header(&receive.response, cmd,
                             PROTOCOL_BINARY_RESPONSE_SUCCESS);

    return TEST_PASS;
}

static enum test_return test_getq(void) {
    return test_getq_impl("test_getq", PROTOCOL_BINARY_CMD_GETQ);
}

static enum test_return test_getkq(void) {
    return test_getq_impl("test_getkq", PROTOCOL_BINARY_CMD_GETKQ);
}

static enum test_return test_incr_impl(const char* key, uint8_t cmd) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response_header;
        protocol_binary_response_incr response;
        char bytes[1024];
    } send, receive;
    size_t len = arithmetic_command(send.bytes, sizeof(send.bytes), cmd,
                                    key, strlen(key), 1, 0, 0);

    int ii;
    for (ii = 0; ii < 10; ++ii) {
        safe_send(send.bytes, len, false);
        if (cmd == PROTOCOL_BINARY_CMD_INCREMENT) {
            safe_recv_packet(receive.bytes, sizeof(receive.bytes));
            validate_response_header(&receive.response_header, cmd,
                                     PROTOCOL_BINARY_RESPONSE_SUCCESS);
            cb_assert(ntohll(receive.response.message.body.value) == ii);
        }
    }

    if (cmd == PROTOCOL_BINARY_CMD_INCREMENTQ) {
        test_noop();
    }
    return TEST_PASS;
}

static enum test_return test_incr(void) {
    return test_incr_impl("test_incr",
                                 PROTOCOL_BINARY_CMD_INCREMENT);
}

static enum test_return test_incrq(void) {
    return test_incr_impl("test_incrq",
                                 PROTOCOL_BINARY_CMD_INCREMENTQ);
}

static enum test_return test_incr_invalid_cas_impl(const char* key, uint8_t cmd) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response_header;
        protocol_binary_response_incr response;
        char bytes[1024];
    } send, receive;
    size_t len = arithmetic_command(send.bytes, sizeof(send.bytes), cmd,
                                    key, strlen(key), 1, 0, 0);

    send.request.message.header.request.cas = 5;
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    validate_response_header(&receive.response_header, cmd,
                             PROTOCOL_BINARY_RESPONSE_EINVAL);
    return TEST_PASS;
}

static enum test_return test_invalid_cas_incr(void) {
    return test_incr_invalid_cas_impl("test_incr",
                                             PROTOCOL_BINARY_CMD_INCREMENT);
}

static enum test_return test_invalid_cas_incrq(void) {
    return test_incr_invalid_cas_impl("test_incrq",
                                             PROTOCOL_BINARY_CMD_INCREMENTQ);
}

static enum test_return test_invalid_cas_decr(void) {
    return test_incr_invalid_cas_impl("test_incr",
                                             PROTOCOL_BINARY_CMD_DECREMENT);
}

static enum test_return test_invalid_cas_decrq(void) {
    return test_incr_invalid_cas_impl("test_incrq",
                                             PROTOCOL_BINARY_CMD_DECREMENTQ);
}

static enum test_return test_decr_impl(const char* key, uint8_t cmd) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response_header;
        protocol_binary_response_decr response;
        char bytes[1024];
    } send, receive;
    size_t len = arithmetic_command(send.bytes, sizeof(send.bytes), cmd,
                                    key, strlen(key), 1, 9, 0);

    int ii;
    for (ii = 9; ii >= 0; --ii) {
        safe_send(send.bytes, len, false);
        if (cmd == PROTOCOL_BINARY_CMD_DECREMENT) {
            safe_recv_packet(receive.bytes, sizeof(receive.bytes));
            validate_response_header(&receive.response_header, cmd,
                                     PROTOCOL_BINARY_RESPONSE_SUCCESS);
            cb_assert(ntohll(receive.response.message.body.value) == ii);
        }
    }

    /* decr on 0 should not wrap */
    safe_send(send.bytes, len, false);
    if (cmd == PROTOCOL_BINARY_CMD_DECREMENT) {
        safe_recv_packet(receive.bytes, sizeof(receive.bytes));
        validate_response_header(&receive.response_header, cmd,
                                 PROTOCOL_BINARY_RESPONSE_SUCCESS);
        cb_assert(ntohll(receive.response.message.body.value) == 0);
    } else {
        test_noop();
    }

    return TEST_PASS;
}

static enum test_return test_decr(void) {
    return test_decr_impl("test_decr",
                                 PROTOCOL_BINARY_CMD_DECREMENT);
}

static enum test_return test_decrq(void) {
    return test_decr_impl("test_decrq",
                                 PROTOCOL_BINARY_CMD_DECREMENTQ);
}

static enum test_return test_version(void) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;

    size_t len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                             PROTOCOL_BINARY_CMD_VERSION,
                             NULL, 0, NULL, 0);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    validate_response_header(&buffer.response, PROTOCOL_BINARY_CMD_VERSION,
                             PROTOCOL_BINARY_RESPONSE_SUCCESS);

    return TEST_PASS;
}

static enum test_return test_flush_impl(const char *key, uint8_t cmd) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } send, receive;
    int ii;

    size_t len = storage_command(send.bytes, sizeof(send.bytes),
                                 PROTOCOL_BINARY_CMD_ADD,
                                 key, strlen(key), NULL, 0, 0, 0);
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    validate_response_header(&receive.response, PROTOCOL_BINARY_CMD_ADD,
                             PROTOCOL_BINARY_RESPONSE_SUCCESS);

    len = flush_command(send.bytes, sizeof(send.bytes), cmd, 2, true);
    safe_send(send.bytes, len, false);
    if (cmd == PROTOCOL_BINARY_CMD_FLUSH) {
        safe_recv_packet(receive.bytes, sizeof(receive.bytes));
        validate_response_header(&receive.response, cmd,
                                 PROTOCOL_BINARY_RESPONSE_SUCCESS);
    }

    len = raw_command(send.bytes, sizeof(send.bytes), PROTOCOL_BINARY_CMD_GET,
                      key, strlen(key), NULL, 0);
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    validate_response_header(&receive.response, PROTOCOL_BINARY_CMD_GET,
                             PROTOCOL_BINARY_RESPONSE_SUCCESS);

#ifdef WIN32
    Sleep(2000);
#else
    sleep(2);
#endif
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    validate_response_header(&receive.response, PROTOCOL_BINARY_CMD_GET,
                             PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);

    for (ii = 0; ii < 2; ++ii) {
        len = storage_command(send.bytes, sizeof(send.bytes),
                              PROTOCOL_BINARY_CMD_ADD,
                              key, strlen(key), NULL, 0, 0, 0);
        safe_send(send.bytes, len, false);
        safe_recv_packet(receive.bytes, sizeof(receive.bytes));
        validate_response_header(&receive.response, PROTOCOL_BINARY_CMD_ADD,
                                 PROTOCOL_BINARY_RESPONSE_SUCCESS);

        len = flush_command(send.bytes, sizeof(send.bytes), cmd, 0, ii == 0);
        safe_send(send.bytes, len, false);
        if (cmd == PROTOCOL_BINARY_CMD_FLUSH) {
            safe_recv_packet(receive.bytes, sizeof(receive.bytes));
            validate_response_header(&receive.response, cmd,
                                     PROTOCOL_BINARY_RESPONSE_SUCCESS);
        }

        len = raw_command(send.bytes, sizeof(send.bytes),
                          PROTOCOL_BINARY_CMD_GET,
                          key, strlen(key), NULL, 0);
        safe_send(send.bytes, len, false);
        safe_recv_packet(receive.bytes, sizeof(receive.bytes));
        validate_response_header(&receive.response, PROTOCOL_BINARY_CMD_GET,
                                 PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);
    }

    return TEST_PASS;
}

static enum test_return test_flush(void) {
    return test_flush_impl("test_flush",
                                  PROTOCOL_BINARY_CMD_FLUSH);
}

static enum test_return test_flushq(void) {
    return test_flush_impl("test_flushq",
                                  PROTOCOL_BINARY_CMD_FLUSHQ);
}

static enum test_return test_cas(void) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } send, receive;
    uint64_t value = 0xdeadbeefdeadcafe;
    size_t len = flush_command(send.bytes, sizeof(send.bytes), PROTOCOL_BINARY_CMD_FLUSH,
                               0, false);
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    validate_response_header(&receive.response, PROTOCOL_BINARY_CMD_FLUSH,
                             PROTOCOL_BINARY_RESPONSE_SUCCESS);

    len = storage_command(send.bytes, sizeof(send.bytes), PROTOCOL_BINARY_CMD_SET,
                          "FOO", 3, &value, sizeof(value), 0, 0);

    send.request.message.header.request.cas = 0x7ffffff;
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    validate_response_header(&receive.response, PROTOCOL_BINARY_CMD_SET,
                             PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);

    send.request.message.header.request.cas = 0x0;
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    validate_response_header(&receive.response, PROTOCOL_BINARY_CMD_SET,
                             PROTOCOL_BINARY_RESPONSE_SUCCESS);

    send.request.message.header.request.cas = receive.response.message.header.response.cas;
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    validate_response_header(&receive.response, PROTOCOL_BINARY_CMD_SET,
                             PROTOCOL_BINARY_RESPONSE_SUCCESS);

    send.request.message.header.request.cas = receive.response.message.header.response.cas - 1;
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    validate_response_header(&receive.response, PROTOCOL_BINARY_CMD_SET,
                             PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS);
    return TEST_PASS;
}

static enum test_return test_concat_impl(const char *key, uint8_t cmd) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } send, receive;
    const char *value = "world";
    char *ptr;
    size_t len = raw_command(send.bytes, sizeof(send.bytes), cmd,
                              key, strlen(key), value, strlen(value));


    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    validate_response_header(&receive.response, cmd,
                             PROTOCOL_BINARY_RESPONSE_NOT_STORED);

    len = storage_command(send.bytes, sizeof(send.bytes),
                          PROTOCOL_BINARY_CMD_ADD,
                          key, strlen(key), value, strlen(value), 0, 0);
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    validate_response_header(&receive.response, PROTOCOL_BINARY_CMD_ADD,
                             PROTOCOL_BINARY_RESPONSE_SUCCESS);

    len = raw_command(send.bytes, sizeof(send.bytes), cmd,
                      key, strlen(key), value, strlen(value));
    safe_send(send.bytes, len, false);

    if (cmd == PROTOCOL_BINARY_CMD_APPEND || cmd == PROTOCOL_BINARY_CMD_PREPEND) {
        safe_recv_packet(receive.bytes, sizeof(receive.bytes));
        validate_response_header(&receive.response, cmd,
                                 PROTOCOL_BINARY_RESPONSE_SUCCESS);
    } else {
        len = raw_command(send.bytes, sizeof(send.bytes), PROTOCOL_BINARY_CMD_NOOP,
                          NULL, 0, NULL, 0);
        safe_send(send.bytes, len, false);
        safe_recv_packet(receive.bytes, sizeof(receive.bytes));
        validate_response_header(&receive.response, PROTOCOL_BINARY_CMD_NOOP,
                                 PROTOCOL_BINARY_RESPONSE_SUCCESS);
    }

    len = raw_command(send.bytes, sizeof(send.bytes), PROTOCOL_BINARY_CMD_GETK,
                      key, strlen(key), NULL, 0);

    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    validate_response_header(&receive.response, PROTOCOL_BINARY_CMD_GETK,
                             PROTOCOL_BINARY_RESPONSE_SUCCESS);

    cb_assert(receive.response.message.header.response.keylen == strlen(key));
    cb_assert(receive.response.message.header.response.bodylen == (strlen(key) + 2*strlen(value) + 4));

    ptr = receive.bytes;
    ptr += sizeof(receive.response);
    ptr += 4;

    cb_assert(memcmp(ptr, key, strlen(key)) == 0);
    ptr += strlen(key);
    cb_assert(memcmp(ptr, value, strlen(value)) == 0);
    ptr += strlen(value);
    cb_assert(memcmp(ptr, value, strlen(value)) == 0);

    return TEST_PASS;
}

static enum test_return test_append(void) {
    return test_concat_impl("test_append",
                                   PROTOCOL_BINARY_CMD_APPEND);
}

static enum test_return test_prepend(void) {
    return test_concat_impl("test_prepend",
                                   PROTOCOL_BINARY_CMD_PREPEND);
}

static enum test_return test_appendq(void) {
    return test_concat_impl("test_appendq",
                                   PROTOCOL_BINARY_CMD_APPENDQ);
}

static enum test_return test_prependq(void) {
    return test_concat_impl("test_prependq",
                                   PROTOCOL_BINARY_CMD_PREPENDQ);
}

static enum test_return test_stat(void) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;

    size_t len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                             PROTOCOL_BINARY_CMD_STAT,
                             NULL, 0, NULL, 0);

    safe_send(buffer.bytes, len, false);
    do {
        safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
        validate_response_header(&buffer.response, PROTOCOL_BINARY_CMD_STAT,
                                 PROTOCOL_BINARY_RESPONSE_SUCCESS);
    } while (buffer.response.message.header.response.keylen != 0);

    return TEST_PASS;
}

static enum test_return test_stat_connections(void) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;

    size_t len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                             PROTOCOL_BINARY_CMD_STAT,
                             "connections", strlen("connections"), NULL, 0);

    safe_send(buffer.bytes, len, false);
    do {
        safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
        validate_response_header(&buffer.response, PROTOCOL_BINARY_CMD_STAT,
                                 PROTOCOL_BINARY_RESPONSE_SUCCESS);
    } while (buffer.response.message.header.response.keylen != 0);

    return TEST_PASS;
}

static enum test_return test_scrub(void) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;

    size_t len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                             PROTOCOL_BINARY_CMD_SCRUB,
                             NULL, 0, NULL, 0);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    validate_response_header(&buffer.response, PROTOCOL_BINARY_CMD_SCRUB,
                             PROTOCOL_BINARY_RESPONSE_SUCCESS);

    return TEST_PASS;
}


static enum test_return test_roles(void) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;

    size_t len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                             PROTOCOL_BINARY_CMD_ASSUME_ROLE,
                             "unknownrole", 11, NULL, 0);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    validate_response_header(&buffer.response, PROTOCOL_BINARY_CMD_ASSUME_ROLE,
                             PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);


    /* assume the statistics role */
    len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                             PROTOCOL_BINARY_CMD_ASSUME_ROLE,
                             "statistics", 10, NULL, 0);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    validate_response_header(&buffer.response, PROTOCOL_BINARY_CMD_ASSUME_ROLE,
                             PROTOCOL_BINARY_RESPONSE_SUCCESS);

    /* At this point I should get an EACCESS if I tried to run NOOP */
    len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                      PROTOCOL_BINARY_CMD_NOOP,
                      NULL, 0, NULL, 0);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    validate_response_header(&buffer.response, PROTOCOL_BINARY_CMD_NOOP,
                             PROTOCOL_BINARY_RESPONSE_EACCESS);

    /* But I should be allowed to run a stat */
    len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                             PROTOCOL_BINARY_CMD_STAT,
                             NULL, 0, NULL, 0);

    safe_send(buffer.bytes, len, false);
    do {
        safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
        validate_response_header(&buffer.response, PROTOCOL_BINARY_CMD_STAT,
                                 PROTOCOL_BINARY_RESPONSE_SUCCESS);
    } while (buffer.response.message.header.response.keylen != 0);

    /* Drop the role */
    len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                             PROTOCOL_BINARY_CMD_ASSUME_ROLE,
                             NULL, 0, NULL, 0);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    validate_response_header(&buffer.response, PROTOCOL_BINARY_CMD_ASSUME_ROLE,
                             PROTOCOL_BINARY_RESPONSE_SUCCESS);

    /* And noop should work again! */
    return test_noop();
}

volatile bool hickup_thread_running;

static void binary_hickup_recv_verification_thread(void *arg) {
    protocol_binary_response_no_extras *response = malloc(65*1024);
    if (response != NULL) {
        while (safe_recv_packet(response, 65*1024)) {
            /* Just validate the packet format */
            validate_response_header(response,
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
    char *key[256];
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
            len = raw_command(command.bytes, sizeof(command.bytes), cmd,
                              key, keylen, &value, sizeof(value));
            break;
        case PROTOCOL_BINARY_CMD_FLUSH:
        case PROTOCOL_BINARY_CMD_FLUSHQ:
            len = raw_command(command.bytes, sizeof(command.bytes), cmd,
                              NULL, 0, NULL, 0);
            break;
        case PROTOCOL_BINARY_CMD_NOOP:
            len = raw_command(command.bytes, sizeof(command.bytes), cmd,
                              NULL, 0, NULL, 0);
            break;
        case PROTOCOL_BINARY_CMD_DELETE:
        case PROTOCOL_BINARY_CMD_DELETEQ:
            len = raw_command(command.bytes, sizeof(command.bytes), cmd,
                             key, keylen, NULL, 0);
            break;
        case PROTOCOL_BINARY_CMD_DECREMENT:
        case PROTOCOL_BINARY_CMD_DECREMENTQ:
        case PROTOCOL_BINARY_CMD_INCREMENT:
        case PROTOCOL_BINARY_CMD_INCREMENTQ:
            len = arithmetic_command(command.bytes, sizeof(command.bytes), cmd,
                                     key, keylen, 1, 0, 0);
            break;
        case PROTOCOL_BINARY_CMD_VERSION:
            len = raw_command(command.bytes, sizeof(command.bytes),
                             PROTOCOL_BINARY_CMD_VERSION,
                             NULL, 0, NULL, 0);
            break;
        case PROTOCOL_BINARY_CMD_GET:
        case PROTOCOL_BINARY_CMD_GETK:
        case PROTOCOL_BINARY_CMD_GETKQ:
        case PROTOCOL_BINARY_CMD_GETQ:
            len = raw_command(command.bytes, sizeof(command.bytes), cmd,
                             key, keylen, NULL, 0);
            break;

        case PROTOCOL_BINARY_CMD_STAT:
            len = raw_command(command.bytes, sizeof(command.bytes),
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

static enum test_return test_pipeline_hickup(void)
{
    size_t buffersize = 65 * 1024;
    void *buffer = malloc(buffersize);
    int ii;
    cb_thread_t tid;
    int ret;
    size_t len;

    allow_closed_read = true;
    hickup_thread_running = true;
    if ((ret = cb_create_thread(&tid, binary_hickup_recv_verification_thread, NULL, 0)) != 0) {
        fprintf(stderr, "Can't create thread: %s\n", strerror(ret));
        free(buffer);
        return TEST_FAIL;
    }

    /* Allow the thread to start */
#ifdef WIN32
    Sleep(1);
#else
    usleep(250);
#endif

    srand((int)time(NULL));
    for (ii = 0; ii < 2; ++ii) {
        test_pipeline_hickup_chunk(buffer, buffersize);
    }

    /* send quitq to shut down the read thread ;-) */
    len = raw_command(buffer, buffersize, PROTOCOL_BINARY_CMD_QUITQ,
                      NULL, 0, NULL, 0);
    safe_send(buffer, len, false);

    cb_join_thread(tid);
    free(buffer);

    reconnect_to_server(false);
    return TEST_PASS;
}

static enum test_return test_ioctl(void) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;

    /* NULL key is invalid. */
    size_t len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                             PROTOCOL_BINARY_CMD_IOCTL_SET, NULL, 0, NULL, 0);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    validate_response_header(&buffer.response, PROTOCOL_BINARY_CMD_IOCTL_SET,
                             PROTOCOL_BINARY_RESPONSE_EINVAL);

    /* release_free_memory always returns OK, regardless of how much was freed.*/
    {
        char cmd[] = "release_free_memory";
        len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                          PROTOCOL_BINARY_CMD_IOCTL_SET, cmd, strlen(cmd),
                          NULL, 0);

        safe_send(buffer.bytes, len, false);
        safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
        validate_response_header(&buffer.response, PROTOCOL_BINARY_CMD_IOCTL_SET,
                                 PROTOCOL_BINARY_RESPONSE_SUCCESS);
    }

    return TEST_PASS;
}

static enum test_return test_config_validate(void) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;

    /* identity config is valid. */
    size_t len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                             PROTOCOL_BINARY_CMD_CONFIG_VALIDATE, NULL, 0,
                             config_string, strlen(config_string));

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    validate_response_header(&buffer.response,
                             PROTOCOL_BINARY_CMD_CONFIG_VALIDATE,
                             PROTOCOL_BINARY_RESPONSE_SUCCESS);

    /* empty config is invalid */
    len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                      PROTOCOL_BINARY_CMD_CONFIG_VALIDATE, NULL, 0,
                      NULL, 0);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    validate_response_header(&buffer.response,
                             PROTOCOL_BINARY_CMD_CONFIG_VALIDATE,
                             PROTOCOL_BINARY_RESPONSE_EINVAL);

    /* non-JSON config is invalid */
    {
        char non_json[] = "[something which isn't JSON]";
        len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                          PROTOCOL_BINARY_CMD_CONFIG_VALIDATE, NULL, 0,
                          non_json, strlen(non_json));

        safe_send(buffer.bytes, len, false);
        safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
        validate_response_header(&buffer.response,
                                 PROTOCOL_BINARY_CMD_CONFIG_VALIDATE,
                                 PROTOCOL_BINARY_RESPONSE_EINVAL);
    }

    /* 'admin' cannot be changed */
    {
        cJSON *dynamic = cJSON_CreateObject();
        char* dyn_string = NULL;
        cJSON_AddStringToObject(dynamic, "admin", "not_me");
        dyn_string = cJSON_Print(dynamic);
        len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                          PROTOCOL_BINARY_CMD_CONFIG_VALIDATE, NULL, 0,
                          dyn_string, strlen(dyn_string));

        safe_send(buffer.bytes, len, false);
        safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
        validate_response_header(&buffer.response,
                                 PROTOCOL_BINARY_CMD_CONFIG_VALIDATE,
                                 PROTOCOL_BINARY_RESPONSE_EINVAL);
    }

    /* 'threads' cannot be changed */
    {
        cJSON *dynamic = cJSON_CreateObject();
        char* dyn_string = NULL;
        cJSON_AddNumberToObject(dynamic, "threads", 99);
        dyn_string = cJSON_Print(dynamic);
        len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                          PROTOCOL_BINARY_CMD_CONFIG_VALIDATE, NULL, 0,
                          dyn_string, strlen(dyn_string));
        free(dyn_string);

        safe_send(buffer.bytes, len, false);
        safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
        validate_response_header(&buffer.response,
                                 PROTOCOL_BINARY_CMD_CONFIG_VALIDATE,
                                 PROTOCOL_BINARY_RESPONSE_EINVAL);
    }

    /* 'interfaces' - should be able to change max connections */
    {
        cJSON *dynamic = generate_config();
        char* dyn_string = NULL;
        cJSON *iface_list = cJSON_GetObjectItem(dynamic, "interfaces");
        cJSON *iface = cJSON_GetArrayItem(iface_list, 0);
        cJSON_ReplaceItemInObject(iface, "maxconn",
                                  cJSON_CreateNumber(MAX_CONNECTIONS * 2));
        dyn_string = cJSON_Print(dynamic);
        len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                          PROTOCOL_BINARY_CMD_CONFIG_VALIDATE, NULL, 0,
                          dyn_string, strlen(dyn_string));
        free(dyn_string);

        safe_send(buffer.bytes, len, false);
        safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
        validate_response_header(&buffer.response,
                                 PROTOCOL_BINARY_CMD_CONFIG_VALIDATE,
                                 PROTOCOL_BINARY_RESPONSE_SUCCESS);
    }

    return TEST_PASS;
}

static enum test_return test_config_reload(void) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;

    /* reload identity config */
    {
        size_t len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                                 PROTOCOL_BINARY_CMD_CONFIG_RELOAD, NULL, 0,
                                 NULL, 0);

        safe_send(buffer.bytes, len, false);
        safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
        validate_response_header(&buffer.response,
                                 PROTOCOL_BINARY_CMD_CONFIG_RELOAD,
                                 PROTOCOL_BINARY_RESPONSE_SUCCESS);
    }

    /* Change max_conns on first interface. */
    {
        cJSON *dynamic = generate_config();
        char* dyn_string = NULL;
        cJSON *iface_list = cJSON_GetObjectItem(dynamic, "interfaces");
        cJSON *iface = cJSON_GetArrayItem(iface_list, 0);
        cJSON_ReplaceItemInObject(iface, "maxconn",
                                  cJSON_CreateNumber(MAX_CONNECTIONS * 2));
        dyn_string = cJSON_Print(dynamic);
        free(dyn_string);
        if (write_config_to_file(dyn_string, config_file) == -1) {
            return TEST_FAIL;
        }

        size_t len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                                 PROTOCOL_BINARY_CMD_CONFIG_RELOAD, NULL, 0,
                                 NULL, 0);

        safe_send(buffer.bytes, len, false);
        safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
        validate_response_header(&buffer.response,
                                 PROTOCOL_BINARY_CMD_CONFIG_RELOAD,
                                 PROTOCOL_BINARY_RESPONSE_SUCCESS);
    }

    /* Change backlog on first interface. */
    {
        cJSON *dynamic = generate_config();
        char* dyn_string = NULL;
        cJSON *iface_list = cJSON_GetObjectItem(dynamic, "interfaces");
        cJSON *iface = cJSON_GetArrayItem(iface_list, 0);
        cJSON_ReplaceItemInObject(iface, "backlog",
                                  cJSON_CreateNumber(BACKLOG * 2));
        dyn_string = cJSON_Print(dynamic);
        free(dyn_string);
        if (write_config_to_file(dyn_string, config_file) == -1) {
            return TEST_FAIL;
        }

        size_t len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                                 PROTOCOL_BINARY_CMD_CONFIG_RELOAD, NULL, 0,
                                 NULL, 0);

        safe_send(buffer.bytes, len, false);
        safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
        validate_response_header(&buffer.response,
                                 PROTOCOL_BINARY_CMD_CONFIG_RELOAD,
                                 PROTOCOL_BINARY_RESPONSE_SUCCESS);
    }

    /* Change tcp_nodelay on first interface. */
    {
        cJSON *dynamic = generate_config();
        char* dyn_string = NULL;
        cJSON *iface_list = cJSON_GetObjectItem(dynamic, "interfaces");
        cJSON *iface = cJSON_GetArrayItem(iface_list, 0);
        cJSON_AddFalseToObject(iface, "tcp_nodelay");
        dyn_string = cJSON_Print(dynamic);
        free(dyn_string);
        if (write_config_to_file(dyn_string, config_file) == -1) {
            return TEST_FAIL;
        }

        size_t len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                                 PROTOCOL_BINARY_CMD_CONFIG_RELOAD, NULL, 0,
                                 NULL, 0);

        safe_send(buffer.bytes, len, false);
        safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
        validate_response_header(&buffer.response,
                                 PROTOCOL_BINARY_CMD_CONFIG_RELOAD,
                                 PROTOCOL_BINARY_RESPONSE_SUCCESS);
    }

    return TEST_PASS;
}

static enum test_return test_config_reload_ssl(void) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;

    /* Change ssl cert/key on second interface. */
    cJSON *dynamic = generate_config();
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
    free(dyn_string);
    if (write_config_to_file(dyn_string, config_file) == -1) {
        return TEST_FAIL;
    }

    size_t len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                             PROTOCOL_BINARY_CMD_CONFIG_RELOAD, NULL, 0,
                             NULL, 0);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    validate_response_header(&buffer.response,
                             PROTOCOL_BINARY_CMD_CONFIG_RELOAD,
                             PROTOCOL_BINARY_RESPONSE_SUCCESS);

    return TEST_PASS;
}

static enum test_return test_audit_put(void) {
    union {
        protocol_binary_request_audit_put request;
        protocol_binary_response_audit_put response;
        char bytes[1024];
    }buffer;

    size_t len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                             PROTOCOL_BINARY_CMD_AUDIT_PUT, NULL, 0,
                             "{}", 2);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    validate_response_header(&buffer.response,
                             PROTOCOL_BINARY_CMD_AUDIT_PUT,
                             PROTOCOL_BINARY_RESPONSE_SUCCESS);
    return TEST_PASS;
}

static enum test_return test_audit_config_reload(void) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    }buffer;

    size_t len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                             PROTOCOL_BINARY_CMD_AUDIT_CONFIG_RELOAD,
                             NULL, 0, NULL, 0);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    validate_response_header(&buffer.response,
                             PROTOCOL_BINARY_CMD_AUDIT_CONFIG_RELOAD,
                             PROTOCOL_BINARY_RESPONSE_SUCCESS);
    return TEST_PASS;
}


static enum test_return test_verbosity(void) {
    union {
        protocol_binary_request_verbosity request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;

    int ii;
    for (ii = 10; ii > -1; --ii) {
        size_t len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                                 PROTOCOL_BINARY_CMD_VERBOSITY,
                                 NULL, 0, NULL, 0);
        buffer.request.message.header.request.extlen = 4;
        buffer.request.message.header.request.bodylen = ntohl(4);
        buffer.request.message.body.level = (uint32_t)ntohl(ii);
        safe_send(buffer.bytes, len + sizeof(4), false);
        safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
        validate_response_header(&buffer.response,
                                 PROTOCOL_BINARY_CMD_VERBOSITY,
                                 PROTOCOL_BINARY_RESPONSE_SUCCESS);
    }

    return TEST_PASS;
}

static enum test_return validate_object(const char *key, const char *value) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } send, receive;
    char *ptr;
    size_t len = raw_command(send.bytes, sizeof(send.bytes),
                             PROTOCOL_BINARY_CMD_GET,
                             key, strlen(key), NULL, 0);
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    validate_response_header(&receive.response, PROTOCOL_BINARY_CMD_GET,
                             PROTOCOL_BINARY_RESPONSE_SUCCESS);
    cb_assert(receive.response.message.header.response.bodylen - 4 == strlen(value));
    ptr = receive.bytes + sizeof(receive.response) + 4;
    cb_assert(memcmp(value, ptr, strlen(value)) == 0);

    return TEST_PASS;
}

static enum test_return store_object(const char *key, char *value) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } send, receive;
    size_t len = storage_command(send.bytes, sizeof(send.bytes),
                                 PROTOCOL_BINARY_CMD_SET,
                                 key, strlen(key), value, strlen(value),
                                 0, 0);

    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    validate_response_header(&receive.response, PROTOCOL_BINARY_CMD_SET,
                             PROTOCOL_BINARY_RESPONSE_SUCCESS);

    validate_object(key, value);
    return TEST_PASS;
}

static enum test_return test_read(void) {
    union {
        protocol_binary_request_read request;
        protocol_binary_response_read response;
        char bytes[1024];
    } buffer;
    size_t len;
    char *ptr;

    store_object("hello", "world");

    len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                      read_command, "hello",
                      strlen("hello"), NULL, 0);
    buffer.request.message.body.offset = htonl(1);
    buffer.request.message.body.length = htonl(3);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    validate_response_header(&buffer.response, read_command,
                             PROTOCOL_BINARY_RESPONSE_SUCCESS);
    cb_assert(buffer.response.message.header.response.bodylen == 3);
    ptr = buffer.bytes + sizeof(buffer.response);
    cb_assert(memcmp(ptr, "orl", 3) == 0);


    len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                      read_command, "hello",
                      strlen("hello"), NULL, 0);
    buffer.request.message.body.offset = htonl(7);
    buffer.request.message.body.length = htonl(2);
    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    validate_response_header(&buffer.response, read_command,
                             PROTOCOL_BINARY_RESPONSE_ERANGE);

    len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                      read_command, "myhello",
                      strlen("myhello"), NULL, 0);
    buffer.request.message.body.offset = htonl(0);
    buffer.request.message.body.length = htonl(5);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    validate_response_header(&buffer.response, read_command,
                             PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);
    return TEST_PASS;
}

static enum test_return test_write(void) {
    union {
        protocol_binary_request_read request;
        protocol_binary_response_read response;
        char bytes[1024];
    } buffer;
    size_t len;

    store_object("hello", "world");

    len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                             write_command, "hello",
                             strlen("hello"), "bubba", 5);
    buffer.request.message.body.offset = htonl(0);
    buffer.request.message.body.length = htonl(5);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    validate_response_header(&buffer.response, write_command,
                             PROTOCOL_BINARY_RESPONSE_SUCCESS);
    validate_object("hello", "bubba");

    len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                             write_command, "hello",
                             strlen("hello"), "zz", 2);
    buffer.request.message.body.offset = htonl(2);
    buffer.request.message.body.length = htonl(2);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    validate_response_header(&buffer.response, write_command,
                             PROTOCOL_BINARY_RESPONSE_SUCCESS);
    validate_object("hello", "buzza");

    len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                             write_command, "hello",
                             strlen("hello"), "zz", 2);
    buffer.request.message.body.offset = htonl(7);
    buffer.request.message.body.length = htonl(2);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    validate_response_header(&buffer.response, write_command,
                             PROTOCOL_BINARY_RESPONSE_ERANGE);

    len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                             write_command, "hello",
                             strlen("hello"), "bb", 2);
    buffer.request.message.body.offset = htonl(2);
    buffer.request.message.body.length = htonl(2);
    buffer.request.message.header.request.cas = 1;

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    validate_response_header(&buffer.response, write_command,
                             PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS);

    len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                      write_command, "myhello",
                      strlen("myhello"), "bubba", 5);
    buffer.request.message.body.offset = htonl(0);
    buffer.request.message.body.length = htonl(5);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    validate_response_header(&buffer.response, write_command,
                             PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);
    return TEST_PASS;
}

static enum test_return test_hello(void) {
    union {
        protocol_binary_request_hello request;
        protocol_binary_response_hello response;
        char bytes[1024];
    } buffer;
    const char *useragent = "hello world";
    uint16_t features[2];
    uint16_t *ptr;
    size_t len;

    features[0] = htons(PROTOCOL_BINARY_FEATURE_DATATYPE);
    features[1] = htons(PROTOCOL_BINARY_FEATURE_TCPNODELAY);

    memset(buffer.bytes, 0, sizeof(buffer.bytes));

    len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                      PROTOCOL_BINARY_CMD_HELLO,
                      useragent, strlen(useragent), features,
                      sizeof(features));

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    validate_response_header(&buffer.response,
                             PROTOCOL_BINARY_CMD_HELLO,
                             PROTOCOL_BINARY_RESPONSE_SUCCESS);

    cb_assert(buffer.response.message.header.response.bodylen == 4);
    ptr = (uint16_t*)(buffer.bytes + sizeof(buffer.response));
    cb_assert(ntohs(*ptr) == PROTOCOL_BINARY_FEATURE_DATATYPE);
    ptr++;
    cb_assert(ntohs(*ptr) == PROTOCOL_BINARY_FEATURE_TCPNODELAY);

    features[0] = 0xffff;
    len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                             PROTOCOL_BINARY_CMD_HELLO,
                             useragent, strlen(useragent), features,
                             2);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    validate_response_header(&buffer.response,
                             PROTOCOL_BINARY_CMD_HELLO,
                             PROTOCOL_BINARY_RESPONSE_SUCCESS);
    cb_assert(buffer.response.message.header.response.bodylen == 0);

    len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                             PROTOCOL_BINARY_CMD_HELLO,
                             useragent, strlen(useragent), features,
                             sizeof(features) - 1);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    validate_response_header(&buffer.response,
                             PROTOCOL_BINARY_CMD_HELLO,
                             PROTOCOL_BINARY_RESPONSE_EINVAL);

    return TEST_PASS;
}

static void set_datatype_feature(bool enable) {
    union {
        protocol_binary_request_hello request;
        protocol_binary_response_hello response;
        char bytes[1024];
    } buffer;
    const char *useragent = "testapp";
    uint16_t feature = htons(PROTOCOL_BINARY_FEATURE_DATATYPE);
    size_t len = strlen(useragent);

    memset(buffer.bytes, 0, sizeof(buffer));
    buffer.request.message.header.request.magic = PROTOCOL_BINARY_REQ;
    buffer.request.message.header.request.opcode = PROTOCOL_BINARY_CMD_HELLO;
    buffer.request.message.header.request.keylen = htons((uint16_t)len);
    if (enable) {
        buffer.request.message.header.request.bodylen = htonl((uint32_t)len + 2);
    } else {
        buffer.request.message.header.request.bodylen = htonl((uint32_t)len);
    }
    memcpy(buffer.bytes + 24, useragent, len);
    memcpy(buffer.bytes + 24 + len, &feature, 2);

    safe_send(buffer.bytes,
              sizeof(buffer.request) + ntohl(buffer.request.message.header.request.bodylen), false);

    safe_recv(&buffer.response, sizeof(buffer.response));
    len = ntohl(buffer.response.message.header.response.bodylen);
    if (enable) {
        cb_assert(len == 2);
        safe_recv(&feature, sizeof(feature));
        cb_assert(feature == htons(PROTOCOL_BINARY_FEATURE_DATATYPE));
    } else {
        cb_assert(len == 0);
    }
}

static void store_object_w_datatype(const char *key,
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
    request.message.header.request.opcode = PROTOCOL_BINARY_CMD_SETQ;
    request.message.header.request.datatype = datatype;
    request.message.header.request.extlen = 8;
    request.message.header.request.keylen = htons((uint16_t)keylen);
    request.message.header.request.bodylen = htonl((uint32_t)(keylen + datalen + 8));

    safe_send(&request.bytes, sizeof(request.bytes), false);
    safe_send(extra, sizeof(extra), false);
    safe_send(key, strlen(key), false);
    safe_send(data, datalen, false);
}

static void get_object_w_datatype(const char *key,
                                  const void *data, size_t datalen,
                                  bool deflate, bool json,
                                  bool conversion)
{
    protocol_binary_response_no_extras response;
    protocol_binary_request_no_extras request;
    char *body;
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
    cb_assert((body = malloc(len)) != NULL);
    safe_recv(body, len);

    if (conversion) {
        cb_assert(response.message.header.response.datatype == PROTOCOL_BINARY_RAW_BYTES);
    } else {
        cb_assert(response.message.header.response.datatype == datatype);
    }

    cb_assert(len == datalen);
    cb_assert(memcmp(data, body, len) == 0);
    free(body);
}

static enum test_return test_datatype_json(void) {
    const char body[] = "{ \"value\" : 1234123412 }";
    set_datatype_feature(true);
    store_object_w_datatype("myjson", body, strlen(body), false, true);

    get_object_w_datatype("myjson", body, strlen(body), false, true, false);

    set_datatype_feature(false);
    get_object_w_datatype("myjson", body, strlen(body), false, true, true);

    return TEST_PASS;
}

static enum test_return test_datatype_json_without_support(void) {
    const char body[] = "{ \"value\" : 1234123412 }";
    set_datatype_feature(false);
    store_object_w_datatype("myjson", body, strlen(body), false, false);

    get_object_w_datatype("myjson", body, strlen(body), false, false, false);

    set_datatype_feature(true);
    get_object_w_datatype("myjson", body, strlen(body), false, true, false);

    return TEST_PASS;
}

static enum test_return test_datatype_compressed(void) {
    const char inflated[] = "aaaaaaaaabbbbbbbccccccdddddd";
    size_t inflated_len = strlen(inflated);
    char deflated[256];
    size_t deflated_len = 256;
    snappy_status status;

    status = snappy_compress(inflated, inflated_len,
                             deflated, &deflated_len);

    if (status != SNAPPY_OK) {
        fprintf(stderr, "Failed to compress data\n");
        abort();
    }

    set_datatype_feature(true);
    store_object_w_datatype("mycompressed", deflated, deflated_len,
                            true, false);

    get_object_w_datatype("mycompressed", deflated, deflated_len,
                          true, false, false);

    set_datatype_feature(false);
    get_object_w_datatype("mycompressed", inflated, inflated_len,
                          true, false, true);

    return TEST_PASS;
}

static enum test_return test_datatype_compressed_json(void) {
    const char inflated[] = "{ \"value\" : \"aaaaaaaaabbbbbbbccccccdddddd\" }";
    size_t inflated_len = strlen(inflated);
    char deflated[256];
    size_t deflated_len = 256;
    snappy_status status;

    status = snappy_compress(inflated, inflated_len,
                             deflated, &deflated_len);

    if (status != SNAPPY_OK) {
        fprintf(stderr, "Failed to compress data\n");
        abort();
    }

    set_datatype_feature(true);
    store_object_w_datatype("mycompressedjson", deflated, deflated_len,
                            true, true);

    get_object_w_datatype("mycompressedjson", deflated, deflated_len,
                          true, true, false);

    set_datatype_feature(false);
    get_object_w_datatype("mycompressedjson", inflated, inflated_len,
                          true, true, true);

    return TEST_PASS;
}

static enum test_return test_invalid_datatype(void) {
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
    cb_assert(code == PROTOCOL_BINARY_RESPONSE_EINVAL);

    reconnect_to_server(false);

    set_datatype_feature(false);
    request.message.header.request.datatype = 4;
    safe_send(&request.bytes, sizeof(request.bytes), false);
    safe_recv_packet(res.buffer, sizeof(res.buffer));
    code = res.response.message.header.response.status;
    cb_assert(code == PROTOCOL_BINARY_RESPONSE_EINVAL);

    reconnect_to_server(false);

    return TEST_PASS;
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
                                           uint64_t old, uint64_t new)
{
    memset(req, 0, sizeof(*req));
    req->message.header.request.magic = PROTOCOL_BINARY_REQ;
    req->message.header.request.opcode = PROTOCOL_BINARY_CMD_SET_CTRL_TOKEN;
    req->message.header.request.extlen = sizeof(uint64_t);
    req->message.header.request.bodylen = htonl(sizeof(uint64_t));
    req->message.header.request.cas = htonll(old);
    req->message.body.new_cas = htonll(new);
}

static enum test_return test_session_ctrl_token(void) {
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

    return TEST_PASS;
}

static enum test_return test_mb_10114(void) {
    char buffer[512];
    const char *key = "mb-10114";
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } send, receive;
    size_t len;

    store_object(key, "world");
    do {
        len = raw_command(send.bytes, sizeof(send.bytes),
                          PROTOCOL_BINARY_CMD_APPEND,
                          key, strlen(key), buffer, sizeof(buffer));
        safe_send(send.bytes, len, false);
        safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    } while (receive.response.message.header.response.status == PROTOCOL_BINARY_RESPONSE_SUCCESS);

    cb_assert(receive.response.message.header.response.status == PROTOCOL_BINARY_RESPONSE_E2BIG);

    /* We should be able to delete it */
    len = raw_command(send.bytes, sizeof(send.bytes),
                      PROTOCOL_BINARY_CMD_DELETE,
                      key, strlen(key), NULL, 0);
    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    validate_response_header(&receive.response, PROTOCOL_BINARY_CMD_DELETE,
                             PROTOCOL_BINARY_RESPONSE_SUCCESS);

    return TEST_PASS;
}

static enum test_return test_dcp_noop(void) {
    union {
        protocol_binary_request_dcp_noop request;
        protocol_binary_response_dcp_noop response;
        char bytes[1024];
    } buffer;

    size_t len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                             PROTOCOL_BINARY_CMD_DCP_NOOP,
                             NULL, 0, NULL, 0);

    /*
     * Default engine don't support DCP, so just check that
     * it detects that and if the packet use incorrect format
     */
    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    validate_response_header(&buffer.response, PROTOCOL_BINARY_CMD_DCP_NOOP,
                             PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);

    len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                             PROTOCOL_BINARY_CMD_DCP_NOOP,
                             "d", 1, "f", 1);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    validate_response_header(&buffer.response, PROTOCOL_BINARY_CMD_DCP_NOOP,
                             PROTOCOL_BINARY_RESPONSE_EINVAL);

    return TEST_PASS;
}

static enum test_return test_dcp_buffer_ack(void) {
    union {
        protocol_binary_request_dcp_buffer_acknowledgement request;
        protocol_binary_response_dcp_noop response;
        char bytes[1024];
    } buffer;

    size_t len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                             PROTOCOL_BINARY_CMD_DCP_BUFFER_ACKNOWLEDGEMENT,
                             NULL, 0, "asdf", 4);
    buffer.request.message.header.request.extlen = 4;

    /*
     * Default engine don't support DCP, so just check that
     * it detects that and if the packet use incorrect format
     */
    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    validate_response_header(&buffer.response,
                             PROTOCOL_BINARY_CMD_DCP_BUFFER_ACKNOWLEDGEMENT,
                             PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);

    len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                             PROTOCOL_BINARY_CMD_DCP_BUFFER_ACKNOWLEDGEMENT,
                             "d", 1, "ffff", 4);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    validate_response_header(&buffer.response,
                             PROTOCOL_BINARY_CMD_DCP_BUFFER_ACKNOWLEDGEMENT,
                             PROTOCOL_BINARY_RESPONSE_EINVAL);

    len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                             PROTOCOL_BINARY_CMD_DCP_BUFFER_ACKNOWLEDGEMENT,
                             NULL, 0, "fff", 3);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    validate_response_header(&buffer.response,
                             PROTOCOL_BINARY_CMD_DCP_BUFFER_ACKNOWLEDGEMENT,
                             PROTOCOL_BINARY_RESPONSE_EINVAL);

    return TEST_PASS;
}

static enum test_return test_dcp_control(void) {
    union {
        protocol_binary_request_dcp_control request;
        protocol_binary_response_dcp_control response;
        char bytes[1024];
    } buffer;

    size_t len;

    len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                             PROTOCOL_BINARY_CMD_DCP_CONTROL,
                             "foo", 3, "bar", 3);

    /*
     * Default engine don't support DCP, so just check that
     * it detects that and if the packet use incorrect format
     */
    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    validate_response_header(&buffer.response,
                             PROTOCOL_BINARY_CMD_DCP_CONTROL,
                             PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED);

    len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                      PROTOCOL_BINARY_CMD_DCP_CONTROL,
                      NULL, 0, NULL, 0);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    validate_response_header(&buffer.response,
                             PROTOCOL_BINARY_CMD_DCP_CONTROL,
                             PROTOCOL_BINARY_RESPONSE_EINVAL);

    len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                      PROTOCOL_BINARY_CMD_DCP_CONTROL,
                      NULL, 0, "fff", 3);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    validate_response_header(&buffer.response,
                             PROTOCOL_BINARY_CMD_DCP_CONTROL,
                             PROTOCOL_BINARY_RESPONSE_EINVAL);

    len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                      PROTOCOL_BINARY_CMD_DCP_CONTROL,
                      "foo", 3, NULL, 0);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    validate_response_header(&buffer.response,
                             PROTOCOL_BINARY_CMD_DCP_CONTROL,
                             PROTOCOL_BINARY_RESPONSE_EINVAL);

    return TEST_PASS;
}

static enum test_return test_isasl_refresh(void) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;

    size_t len;

    len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                      PROTOCOL_BINARY_CMD_ISASL_REFRESH,
                      NULL, 0, NULL, 0);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    validate_response_header(&buffer.response,
                             PROTOCOL_BINARY_CMD_ISASL_REFRESH,
                             PROTOCOL_BINARY_RESPONSE_SUCCESS);

    return TEST_PASS;
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

    size_t len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                             PROTOCOL_BINARY_CMD_ADJUST_TIMEOFDAY,
                             NULL, 0, NULL, 0);

    buffer.request.message.body.offset = htonll(clock_shift);

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    validate_response_header(&buffer.response, PROTOCOL_BINARY_CMD_ADJUST_TIMEOFDAY,
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
    validate_response_header(&receive.response, PROTOCOL_BINARY_CMD_SET,
                             PROTOCOL_BINARY_RESPONSE_SUCCESS);

    adjust_memcached_clock(clock_shift);

#ifdef WIN32
    Sleep(wait1 * 1000);
#else
    sleep(wait1);
#endif

    memset(send.bytes, 0, 1024);
    len = raw_command(send.bytes, sizeof(send.bytes), PROTOCOL_BINARY_CMD_GET,
                      key, strlen(key), NULL, 0);

    safe_send(send.bytes, len, false);
    safe_recv_packet(receive.bytes, sizeof(receive.bytes));
    validate_response_header(&receive.response, PROTOCOL_BINARY_CMD_GET,
                             PROTOCOL_BINARY_RESPONSE_SUCCESS);

    return TEST_PASS;
}

static enum test_return test_expiry_relative_with_clock_change_backwards(void) {
    /*
       Just test for MB-11548
       120 second expiry.
       Set clock back by some amount that's before the time we started memcached.
       wait 2 seconds (allow mc time to tick)
       (defect was that time went negative and expired keys immediatley)
    */
    time_t now = time(0);
    return test_expiry("test_expiry_relative_with_clock_change_backwards", 120, 2, 0 - ((now - server_start_time) * 2));
}

static enum test_return test_set_huge_impl(const char *key,
                                                uint8_t cmd,
                                                int result,
                                                bool pipeline,
                                                int iterations,
                                                int message_size) {

    enum test_return rv = TEST_PASS;

    /* some error case may return a body in the response */
    char receive[sizeof(protocol_binary_response_no_extras) + 32];
    size_t len = message_size + sizeof(protocol_binary_request_set) + strlen(key);
    char* set_message = malloc(len);
    char* message = set_message + (sizeof(protocol_binary_request_set) + strlen(key));
    int ii;
    memset(message, 0xb0, message_size);

    cb_assert(len == storage_command(set_message, len, cmd,
                          key, strlen(key), NULL,
                          message_size, 0, 0));

    for (ii = 0; ii < iterations; ++ii) {
        if (pipeline) {
            safe_send(set_message, len, false);
        } else {
            safe_send(set_message, len, false);

            if (cmd == PROTOCOL_BINARY_CMD_SET) {
                safe_recv_packet(&receive, sizeof(receive));
                validate_response_header((protocol_binary_response_no_extras*)receive, cmd, result);
            }
        }
    }

    if (pipeline && cmd == PROTOCOL_BINARY_CMD_SET) {
        for (ii = 0; ii < iterations; ++ii) {
            safe_recv_packet(&receive, sizeof(receive));
            validate_response_header((protocol_binary_response_no_extras*)receive, cmd, result);
        }
    }

    free(set_message);
    return rv;
}

static enum test_return test_set_huge(void) {
    return test_set_huge_impl("test_set_huge",
                                PROTOCOL_BINARY_CMD_SET,
                                PROTOCOL_BINARY_RESPONSE_SUCCESS,
                                false,
                                10,
                                1023 * 1024);
}

static enum test_return test_set_e2big(void) {
    return test_set_huge_impl("test_set_e2big",
                                PROTOCOL_BINARY_CMD_SET,
                                PROTOCOL_BINARY_RESPONSE_E2BIG,
                                false,
                                10,
                                1024 * 1024);
}

static enum test_return test_setq_huge(void) {
    return test_set_huge_impl("test_setq_huge",
                                PROTOCOL_BINARY_CMD_SET,
                                PROTOCOL_BINARY_RESPONSE_SUCCESS,
                                false,
                                10,
                                1023 * 1024);
}

static enum test_return test_pipeline_huge(void) {
    return test_set_huge_impl("test_pipeline_huge",
                                PROTOCOL_BINARY_CMD_SET,
                                PROTOCOL_BINARY_RESPONSE_SUCCESS,
                                true,
                                8000,
                                1023 * 1024);
}

/* support set, get, delete */
static enum test_return test_pipeline_impl(int cmd,
                                                int result,
                                                char* key_root,
                                                uint32_t messages_in_stream,
                                                size_t value_size) {
    enum test_return rv = TEST_PASS;

    size_t largest_protocol_packet = sizeof(protocol_binary_request_set); /* set has the largest protocol message */
    size_t key_root_len = strlen(key_root);
    size_t key_digit_len = 5; /*append 00001, 00002 etc.. to key_root */
    size_t buffer_len = (largest_protocol_packet + key_root_len + key_digit_len + value_size) * messages_in_stream;
    size_t out_message_len = 0, in_message_len = 0, send_len = 0, receive_len = 0;
    uint8_t* buffer = malloc(buffer_len); /* space for creating and receiving a stream */
    char* key = malloc(key_root_len + key_digit_len + 1); /* space for building keys */
    uint8_t* current_message = buffer;
    int ii = 0; /* i i captain */
    int session = 0; /* something to stick in opaque */

    srand(time(0));
    session = rand() % 100;

    cb_assert(buffer != NULL);
    cb_assert(key != NULL);
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
        fprintf(stderr, "invalid cmd (%d) in test_pipeline_impl", cmd);
        rv = TEST_FAIL;
    }

    send_len    = out_message_len * messages_in_stream;
    receive_len = in_message_len * messages_in_stream;

    /* entire buffer and thus any values are 0xaf */
    memset(buffer, 0xaf, buffer_len);

    for (ii = 0; ii < messages_in_stream; ii++) {
        snprintf(key, key_root_len + key_digit_len + 1, "%s%05d", key_root, ii);
        if (PROTOCOL_BINARY_CMD_SET == cmd) {
            protocol_binary_request_set* this_req = (protocol_binary_request_set*)current_message;
            current_message = current_message + storage_command((char*)current_message, out_message_len, cmd,
                                             key, strlen(key), NULL,
                                             value_size, 0, 0);
            this_req->message.header.request.opaque = htonl((session << 8) | ii);
        } else {
            protocol_binary_request_no_extras* this_req = (protocol_binary_request_no_extras*)current_message;
            current_message = current_message + raw_command((char*)current_message, out_message_len, cmd,
                                             key, strlen(key), NULL, 0);
            this_req->message.header.request.opaque = htonl((session << 8) | ii);
        }
    }

    cb_assert(buffer_len >= send_len);

    safe_send(buffer, send_len, false);

    memset(buffer, 0, buffer_len);

    /* and get it all back in the same buffer */
    cb_assert(buffer_len >= receive_len);

    safe_recv(buffer, receive_len);
    current_message = buffer;
    for (ii = 0; ii < messages_in_stream; ii++) {
        protocol_binary_response_no_extras* message = (protocol_binary_response_no_extras*)current_message;

        uint32_t bodylen = ntohl(message->message.header.response.bodylen);
        uint8_t  extlen  = message->message.header.response.extlen;
        uint16_t status  = ntohs(message->message.header.response.status);
        uint32_t opq     = ntohl(message->message.header.response.opaque);

        cb_assert(status == result);
        cb_assert(opq == ((session << 8)|ii));

        /* a value? */
        if (bodylen != 0 && result == PROTOCOL_BINARY_RESPONSE_SUCCESS) {
            int jj = 0;
            uint8_t* value = current_message + sizeof(protocol_binary_response_no_extras) + extlen;
            for (jj = 0; jj < value_size; jj++) {
                cb_assert(value[jj] == 0xaf);
            }
            current_message = current_message + bodylen + sizeof(protocol_binary_response_no_extras);
        } else {
            current_message = (uint8_t*)(message + 1);
        }
    }

    free(buffer);
    free(key);
    return rv;
}

static enum test_return test_pipeline_set(void) {
    enum test_return rv;
    int ii = 1;
    /*
      MB-11203 would break at iteration 529 where we happen to send 57916 bytes in 1 pipe
      this triggered some edge cases in our SSL recv code.
    */
    for (ii = 1; ii < 1000; ii++) {
        rv = test_pipeline_impl(PROTOCOL_BINARY_CMD_SET,
                                    PROTOCOL_BINARY_RESPONSE_SUCCESS,
                                    "key_set_pipe", 100, ii);
        if (rv == TEST_PASS) {
            rv = test_pipeline_impl(PROTOCOL_BINARY_CMD_DELETE,
                                     PROTOCOL_BINARY_RESPONSE_SUCCESS,
                                     "key_set_pipe", 100, ii);
        }
    }
    return rv;
}

static enum test_return test_pipeline_set_get_del(void) {
    char* key_root = "key_set_get_del";
    enum test_return rv;

    rv = test_pipeline_impl(PROTOCOL_BINARY_CMD_SET,
                                PROTOCOL_BINARY_RESPONSE_SUCCESS,
                                key_root, 5000, 256);

    if (rv == TEST_PASS) {
        rv = test_pipeline_impl(PROTOCOL_BINARY_CMD_GET,
                                    PROTOCOL_BINARY_RESPONSE_SUCCESS,
                                     key_root, 5000, 256);
        if (rv == TEST_PASS) {
            rv = test_pipeline_impl(PROTOCOL_BINARY_CMD_DELETE,
                              PROTOCOL_BINARY_RESPONSE_SUCCESS,
                               key_root, 5000, 256);
        }
    }
    return rv;
}

static enum test_return test_pipeline_set_del(void) {
    enum test_return rv = test_pipeline_impl(PROTOCOL_BINARY_CMD_SET,
                                PROTOCOL_BINARY_RESPONSE_SUCCESS,
                                "key_root", 5000, 256);

    if (rv == TEST_PASS) {
        rv = test_pipeline_impl(PROTOCOL_BINARY_CMD_DELETE,
                                    PROTOCOL_BINARY_RESPONSE_SUCCESS,
                                    "key_root", 5000, 256);
    }

    return rv;
}

/* Send one character to the SSL port, then check memcached correctly closes
 * the connection (and doesn't hold it open for ever trying to read) more bytes
 * which will never come.
 */
static enum test_return test_mb_12762_ssl_handshake_hang(void) {

    cb_assert(current_phase == phase_ssl);
    /* Setup: Close the existing (handshaked) SSL connection, and create a
     * 'plain' TCP connection to the SSL port - i.e. without any SSL handshake.
     */
    closesocket(sock_ssl);
    sock_ssl = create_connect_plain_socket("127.0.0.1", ssl_port, false);

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
    int ready_fds = select(sock_ssl+1, &fdset, NULL, NULL, &timeout);
    cb_assert(ready_fds == 1);

    /* Verify that attempting to read from the socket returns 0 (peer has
     * indeed closed the connection).
     */
    len = recv(sock_ssl, buf, 1, 0);
    cb_assert(len == 0);

    /* Restore the SSL connection to a sane state :) */
    reconnect_to_server(false);

    return TEST_PASS;
}

static char *get_sasl_mechs(void) {
    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;

    size_t plen = raw_command(buffer.bytes, sizeof(buffer.bytes),
                              PROTOCOL_BINARY_CMD_SASL_LIST_MECHS,
                              NULL, 0, NULL, 0);

    safe_send(buffer.bytes, plen, false);
    safe_recv_packet(&buffer, sizeof(buffer));
    validate_response_header(&buffer.response,
                             PROTOCOL_BINARY_CMD_SASL_LIST_MECHS,
                             PROTOCOL_BINARY_RESPONSE_SUCCESS);

    char *ret = malloc(buffer.response.message.header.response.bodylen + 1);
    cb_assert(ret != NULL);
    memcpy(ret, buffer.bytes + sizeof(buffer.response.bytes),
           buffer.response.message.header.response.bodylen);
    ret[buffer.response.message.header.response.bodylen] = '\0';
    return ret;
}


static enum test_return test_sasl_list_mech(void) {
    char *mech = get_sasl_mechs();
    cb_assert(mech != NULL);
    free(mech);
    return TEST_PASS;
}

struct my_sasl_ctx {
    const char *username;
    cbsasl_secret_t *secret;
};

static int sasl_get_username(void *context, int id, const char **result,
                             unsigned int *len)
{
    struct my_sasl_ctx *ctx = context;
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
    struct my_sasl_ctx *ctx = context;
    if (!conn || ! psecret || id != CBSASL_CB_PASS || ctx == NULL) {
        return CBSASL_BADPARAM;
    }

    *psecret = ctx->secret;
    return CBSASL_OK;
}

static uint16_t sasl_auth(const char *username, const char *password) {
    cbsasl_error_t err;
    const char *data;
    unsigned int len;
    const char *chosenmech;
    struct my_sasl_ctx context;
    cbsasl_callback_t sasl_callbacks[4];
    cbsasl_conn_t *client;
    char *mech = get_sasl_mechs();

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
    context.secret = calloc(1, 100);
    memcpy(context.secret->data, password, strlen(password));
    context.secret->len = strlen(password);

    err = cbsasl_client_new(NULL, NULL, NULL, NULL, sasl_callbacks, 0, &client);
    cb_assert(err == CBSASL_OK);
    err = cbsasl_client_start(client, mech, NULL, &data, &len, &chosenmech);
    cb_assert(err == CBSASL_OK);

    union {
        protocol_binary_request_no_extras request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;

    size_t plen = raw_command(buffer.bytes, sizeof(buffer.bytes),
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

        plen = raw_command(buffer.bytes, sizeof(buffer.bytes),
                           PROTOCOL_BINARY_CMD_SASL_STEP,
                           chosenmech, strlen(chosenmech), data, len);

        safe_send(buffer.bytes, plen, false);

        safe_recv_packet(&buffer, sizeof(buffer));
    }

    if (stepped) {
        validate_response_header(&buffer.response,
                                 PROTOCOL_BINARY_CMD_SASL_STEP,
                                 buffer.response.message.header.response.status);
    } else {
        validate_response_header(&buffer.response,
                                 PROTOCOL_BINARY_CMD_SASL_AUTH,
                                 buffer.response.message.header.response.status);
    }
    free(mech);
    free(context.secret);
    cbsasl_dispose(&client);

    return buffer.response.message.header.response.status;
}

static enum test_return test_sasl_success(void) {
    if (sasl_auth("_admin", "password") == PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        return TEST_PASS;
    } else {
        return TEST_FAIL;
    }
}

static enum test_return test_sasl_fail(void) {
    if (sasl_auth("_admin", "asdf") == PROTOCOL_BINARY_RESPONSE_AUTH_ERROR) {
        return TEST_PASS;
    } else {
        return TEST_FAIL;
    }
}

typedef enum test_return (*TEST_FUNC)(void);
struct testcase {
    const char *description;
    TEST_FUNC function;
    const int phases; /*1 bit per phase*/
};

/*
  Test cases currently run as follows.

  Either with plain_sockets only -> TESTCASE
  or
  plain_sockets and ssl ->  TESTCASE_SSL_MODE
*/
#define TESTCASE_SETUP(desc, func) {desc, func, phase_setup}
#define TESTCASE_PLAIN(desc, func) {desc, func, phase_plain}
#define TESTCASE_PLAIN_AND_SSL(desc, func) {desc, func, (phase_plain|phase_ssl)}
#define TESTCASE_SSL(desc, func) {desc, func, phase_ssl}
#define TESTCASE_CLEANUP(desc, func) {desc, func, phase_cleanup}

struct testcase testcases[] = {
    TESTCASE_PLAIN("cache_create", cache_create_test),
    TESTCASE_PLAIN("cache_constructor", cache_constructor_test),
    TESTCASE_PLAIN("cache_constructor_fail", cache_fail_constructor_test),
    TESTCASE_PLAIN("cache_destructor", cache_destructor_test),
    TESTCASE_PLAIN("cache_reuse", cache_reuse_test),
    TESTCASE_PLAIN("cache_redzone", cache_redzone_test),
    TESTCASE_PLAIN("issue_161", test_issue_161),
    TESTCASE_PLAIN("strtof", test_safe_strtof),
    TESTCASE_PLAIN("strtol", test_safe_strtol),
    TESTCASE_PLAIN("strtoll", test_safe_strtoll),
    TESTCASE_PLAIN("strtoul", test_safe_strtoul),
    TESTCASE_PLAIN("strtoull", test_safe_strtoull),
    TESTCASE_PLAIN("vperror", test_vperror),
    TESTCASE_PLAIN("config_parser", test_config_parser),
    /* The following tests all run towards the same server */
    TESTCASE_SETUP("start_server", start_memcached_server),
    TESTCASE_PLAIN_AND_SSL("connect", test_connect_to_server),
    TESTCASE_PLAIN_AND_SSL("noop", test_noop),
    TESTCASE_PLAIN_AND_SSL("sasl list mech", test_sasl_list_mech),
    TESTCASE_PLAIN_AND_SSL("sasl fail", test_sasl_fail),
    TESTCASE_PLAIN_AND_SSL("sasl success", test_sasl_success),
    TESTCASE_PLAIN_AND_SSL("quit", test_quit),
    TESTCASE_PLAIN_AND_SSL("quitq", test_quitq),
    TESTCASE_PLAIN_AND_SSL("set", test_set),
    TESTCASE_PLAIN_AND_SSL("setq", test_setq),
    TESTCASE_PLAIN_AND_SSL("add", test_add),
    TESTCASE_PLAIN_AND_SSL("addq", test_addq),
    TESTCASE_PLAIN_AND_SSL("replace", test_replace),
    TESTCASE_PLAIN_AND_SSL("replaceq", test_replaceq),
    TESTCASE_PLAIN_AND_SSL("delete", test_delete),
    TESTCASE_PLAIN_AND_SSL("delete_cas", test_delete_cas),
    TESTCASE_PLAIN_AND_SSL("delete_bad_cas", test_delete_bad_cas),
    TESTCASE_PLAIN_AND_SSL("deleteq", test_deleteq),
    TESTCASE_PLAIN_AND_SSL("get", test_get),
    TESTCASE_PLAIN_AND_SSL("getq", test_getq),
    TESTCASE_PLAIN_AND_SSL("getk", test_getk),
    TESTCASE_PLAIN_AND_SSL("getkq", test_getkq),
    TESTCASE_PLAIN_AND_SSL("incr", test_incr),
    TESTCASE_PLAIN_AND_SSL("incrq", test_incrq),
    TESTCASE_PLAIN_AND_SSL("decr", test_decr),
    TESTCASE_PLAIN_AND_SSL("decrq", test_decrq),
    TESTCASE_PLAIN_AND_SSL("incr_invalid_cas", test_invalid_cas_incr),
    TESTCASE_PLAIN_AND_SSL("incrq_invalid_cas", test_invalid_cas_incrq),
    TESTCASE_PLAIN_AND_SSL("decr_invalid_cas", test_invalid_cas_decr),
    TESTCASE_PLAIN_AND_SSL("decrq_invalid_cas", test_invalid_cas_decrq),
    TESTCASE_PLAIN_AND_SSL("version", test_version),
    TESTCASE_PLAIN_AND_SSL("flush", test_flush),
    TESTCASE_PLAIN_AND_SSL("flushq", test_flushq),
    TESTCASE_PLAIN_AND_SSL("cas", test_cas),
    TESTCASE_PLAIN_AND_SSL("append", test_append),
    TESTCASE_PLAIN_AND_SSL("appendq", test_appendq),
    TESTCASE_PLAIN_AND_SSL("prepend", test_prepend),
    TESTCASE_PLAIN_AND_SSL("prependq", test_prependq),
    TESTCASE_PLAIN_AND_SSL("stat", test_stat),
    TESTCASE_PLAIN_AND_SSL("stat_connections", test_stat_connections),
    TESTCASE_PLAIN_AND_SSL("roles", test_roles),
    TESTCASE_PLAIN_AND_SSL("scrub", test_scrub),
    TESTCASE_PLAIN_AND_SSL("verbosity", test_verbosity),
    TESTCASE_PLAIN_AND_SSL("read", test_read),
    TESTCASE_PLAIN_AND_SSL("write", test_write),
    TESTCASE_PLAIN_AND_SSL("MB-10114", test_mb_10114),
    TESTCASE_SSL("MB-12762-ssl_handshake_hang", test_mb_12762_ssl_handshake_hang),
    TESTCASE_PLAIN_AND_SSL("dcp_noop", test_dcp_noop),
    TESTCASE_PLAIN_AND_SSL("dcp_buffer_acknowledgment", test_dcp_buffer_ack),
    TESTCASE_PLAIN_AND_SSL("dcp_control", test_dcp_control),
    TESTCASE_PLAIN_AND_SSL("hello", test_hello),
    TESTCASE_PLAIN_AND_SSL("isasl_refresh", test_isasl_refresh),

    TESTCASE_PLAIN_AND_SSL("ioctl", test_ioctl),
    TESTCASE_PLAIN_AND_SSL("config_validate", test_config_validate),
    TESTCASE_PLAIN("config_reload", test_config_reload),
    TESTCASE_SSL("config_reload_ssl", test_config_reload_ssl),
    TESTCASE_PLAIN_AND_SSL("audit_put", test_audit_put),
    TESTCASE_PLAIN_AND_SSL("audit_config_reload", test_audit_config_reload),
    TESTCASE_PLAIN_AND_SSL("datatype_json", test_datatype_json),
    TESTCASE_PLAIN_AND_SSL("datatype_json_without_support", test_datatype_json_without_support),
    TESTCASE_PLAIN_AND_SSL("datatype_compressed", test_datatype_compressed),
    TESTCASE_PLAIN_AND_SSL("datatype_compressed_json", test_datatype_compressed_json),
    TESTCASE_PLAIN_AND_SSL("invalid_datatype", test_invalid_datatype),
    TESTCASE_PLAIN_AND_SSL("session_ctrl_token", test_session_ctrl_token),
    TESTCASE_PLAIN_AND_SSL("expiry_relative_with_clock_change", test_expiry_relative_with_clock_change_backwards),
    TESTCASE_PLAIN("pipeline_hickup", test_pipeline_hickup),
    TESTCASE_PLAIN_AND_SSL("set_huge", test_set_huge),
    TESTCASE_PLAIN_AND_SSL("setq_huge", test_setq_huge),
    TESTCASE_PLAIN_AND_SSL("set_e2big", test_set_e2big),
    TESTCASE_PLAIN_AND_SSL("pipeline_huge",  test_pipeline_huge),
    TESTCASE_SSL("pipeline_mb-11203",test_pipeline_set),
    TESTCASE_PLAIN_AND_SSL("pipeline_1", test_pipeline_set_get_del),
    TESTCASE_PLAIN_AND_SSL("pipeline_2", test_pipeline_set_del),
    TESTCASE_CLEANUP("stop_server", stop_memcached_server),
    TESTCASE_PLAIN(NULL, NULL)
};

int main(int argc, char **argv)
{
    int exitcode = 0;
    int ii = 0;
    enum test_return ret;

    int test_phase_order[] = {phase_setup, phase_plain, phase_ssl, phase_cleanup};
    char* test_phase_strings[] = {"setup", "plain", "SSL", "cleanup"};

    /* If user specifies a particular mode (plain/SSL), only enable that phase.
     */
    if (argc > 1) {
        if (strcmp("plain", argv[1]) == 0) {
            /* disable SSL phase */
            phases_enabled &= ~phase_ssl;
        } else if (strcmp("ssl", argv[1]) == 0 || strcmp("SSL", argv[1]) == 0) {
            /* disable plain phase */
            phases_enabled &= ~phase_plain;
        } else {
            fprintf(stderr, "Error: Unrecognized argument '%s'\n", argv[1]);
            exit(1);
        }
    }

    cb_initialize_sockets();
    /* Use unbuffered stdio */
    setbuf(stdout, NULL);
    setbuf(stderr, NULL);

    /* loop through the test phases and runs tests applicable to each phase*/
    for (int phase_index = 0; phase_index < phase_max; phase_index++) {
        current_phase = test_phase_order[phase_index];
        if (!phase_enabled(current_phase)) {
            continue;
        }

        for (ii = 0; testcases[ii].description != NULL; ++ii) {
            int jj;

            if ((current_phase & testcases[ii].phases) != current_phase) {
                continue;
            }

            fprintf(stdout, "\r");
            for (jj = 0; jj < 60; ++jj) {
                fprintf(stdout, " ");
            }
            fprintf(stdout, "\rRunning %04d %s (%s) - ", ii + 1, testcases[ii].description, test_phase_strings[phase_index]);
            fflush(stdout);
            ret = testcases[ii].function();
            if (ret == TEST_SKIP) {
                fprintf(stdout, " SKIP\n");
            } else if (ret != TEST_PASS) {
                fprintf(stdout, " FAILED\n");
                exitcode = 1;
            }
            fflush(stdout);
        }
    }

    fprintf(stdout, "\r                                     \n");
    return exitcode;
}
