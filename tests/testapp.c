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
#include "extensions/protocol/fragment_rw.h"
#include <platform/platform.h>

/* Set the read/write commands differently than the default values
 * so that we can verify that the override works
 */
static uint8_t read_command = 0xe1;
static uint8_t write_command = 0xe2;

const char config_file[] = "memcached_testapp.json";

#define TMP_TEMPLATE "/tmp/test_file.XXXXXXX"

enum test_return { TEST_SKIP, TEST_PASS, TEST_FAIL };

static pid_t server_pid;
static in_port_t port;
static SOCKET sock;
static bool allow_closed_read = false;

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


static int generate_config(const char *fname)
{
    FILE *fp;
    cJSON *root = cJSON_CreateObject();
    cJSON *array = cJSON_CreateArray();
    cJSON *obj = cJSON_CreateObject();

    cJSON_AddStringToObject(obj, "module", "default_engine.so");
    cJSON_AddItemReferenceToObject(root, "engine", obj);

    obj = cJSON_CreateObject();
    cJSON_AddStringToObject(obj, "module", "blackhole_logger.so");
    cJSON_AddItemToArray(array, obj);
    obj = cJSON_CreateObject();
    cJSON_AddStringToObject(obj, "module", "fragment_rw_ops.so");
    cJSON_AddStringToObject(obj, "config", "r=225;w=226");
    cJSON_AddItemToArray(array, obj);

    cJSON_AddItemReferenceToObject(root, "extensions", array);

    array = cJSON_CreateArray();
    obj = cJSON_CreateObject();
#ifdef WIN32
    cJSON_AddNumberToObject(obj, "port", 11211);
#else
    cJSON_AddNumberToObject(obj, "port", 0);
#endif
    cJSON_AddNumberToObject(obj, "maxconn", 1000);
    cJSON_AddNumberToObject(obj, "backlog", 1024);
    cJSON_AddStringToObject(obj, "host", "*");
    cJSON_AddItemToArray(array, obj);

    cJSON_AddItemReferenceToObject(root, "interfaces", array);

    cJSON_AddStringToObject(root, "admin", "");
    cJSON_AddTrueToObject(root, "datatype_support");

    if ((fp = fopen(fname, "w")) == NULL) {
        return -1;
    } else {
        fprintf(fp, "%s", cJSON_Print(root));
        fclose(fp);
    }

    return 0;
}

#ifdef WIN32
static HANDLE start_server(in_port_t *port_out, bool daemon, int timeout) {
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
static pid_t start_server(in_port_t *port_out, bool daemon, int timeout) {
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

        argv[arg++] = "./memcached";
        argv[arg++] = "-C";
        argv[arg++] = (char*)config_file;

        argv[arg++] = NULL;
        cb_assert(execv(argv[0], argv) != -1);
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
    while ((fgets(buffer, sizeof(buffer), fp)) != NULL) {
        if (strncmp(buffer, "TCP INET: ", 10) == 0) {
            int32_t val;
            cb_assert(safe_strtol(buffer + 10, &val));
            *port_out = (in_port_t)val;
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

static SOCKET connect_server(const char *hostname, in_port_t port, bool nonblock)
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
          } else if (nonblock) {
#ifdef WIN32
              if (evutil_make_socket_nonblocking(sock) == -1) {
                abort();
              }
#else
              int flags = fcntl(sock, F_GETFL, 0);
              if (flags < 0 || fcntl(sock, F_SETFL, flags | O_NONBLOCK) < 0) {
                  fprintf(stderr, "Failed to enable nonblocking mode: %s\n",
                          strerror(errno));
                  close(sock);
                  sock = -1;
              }
#endif
          }
       } else {
          fprintf(stderr, "Failed to create socket: %s\n", strerror(errno));
       }

       freeaddrinfo(ai);
    }
    return sock;
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
#ifndef WIN32
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
    int newfile;
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

    newfile = mkstemp(outfile);
    cb_assert(newfile > 0);
    error = fdopen(newfile, "w");

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

    newfile = mkstemp(cfgfile);
    cb_assert(newfile > 0);
    cfg = fdopen(newfile, "w");
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
#else
    return TEST_SKIP;
#endif
}

static enum test_return start_memcached_server(void) {
    if (generate_config(config_file) == -1) {
        return TEST_FAIL;
    }

    server_pid = start_server(&port, false, 600);
    sock = connect_server("127.0.0.1", port, false);

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
    return TEST_PASS;
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

#ifdef WIN32
        nw = send(sock, ptr + offset, (int)num_bytes, 0);
#else
        nw = send(sock, ptr + offset, num_bytes, 0);
#endif
        if (nw == -1) {
            if (errno != EINTR) {
                fprintf(stderr, "Failed to write: %s\n", strerror(errno));
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
#ifdef WIN32
        ssize_t nr = recv(sock, ((char*)buf) + offset, (int)(len - offset), 0);
#else
        ssize_t nr = recv(sock, ((char*)buf) + offset, len - offset, 0);
#endif
        if (nr == -1) {
            if (errno != EINTR) {
                fprintf(stderr, "Failed to read: %s\n", strerror(errno));
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

    cb_assert(size > sizeof(*response));
    if (!safe_recv(response, sizeof(*response))) {
        return false;
    }
    response->message.header.response.keylen = ntohs(response->message.header.response.keylen);
    response->message.header.response.status = ntohs(response->message.header.response.status);
    response->message.header.response.bodylen = ntohl(response->message.header.response.bodylen);

    len = sizeof(*response);
    ptr = buf;
    ptr += len;
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
    cb_assert(bufsz > sizeof(*request) + keylen + dtalen);

    memset(request, 0, sizeof(*request));
    request->message.header.request.magic = PROTOCOL_BINARY_REQ;
    request->message.header.request.opcode = cmd;
    request->message.header.request.keylen = htons((uint16_t)keylen);
    request->message.header.request.extlen = 8;
    request->message.header.request.bodylen = htonl((uint32_t)(keylen + 8 + dtalen));
    request->message.header.request.opaque = 0xdeadbeef;
    request->message.body.flags = flags;
    request->message.body.expiration = exp;

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
    cb_assert(bufsz > sizeof(*request) + keylen + dtalen);

    memset(request, 0, sizeof(*request));
    if (cmd == read_command || cmd == write_command) {
        request->message.header.request.extlen = 8;
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

static enum test_return test_binary_noop(void) {
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

static enum test_return test_binary_quit_impl(uint8_t cmd) {
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
    cb_assert(recv(sock, buffer.bytes, sizeof(buffer.bytes), 0) == 0);
    closesocket(sock);
    sock = connect_server("127.0.0.1", port, false);

    return TEST_PASS;
}

static enum test_return test_binary_quit(void) {
    return test_binary_quit_impl(PROTOCOL_BINARY_CMD_QUIT);
}

static enum test_return test_binary_quitq(void) {
    return test_binary_quit_impl(PROTOCOL_BINARY_CMD_QUITQ);
}

static enum test_return test_binary_set_impl(const char *key, uint8_t cmd) {
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
        return test_binary_noop();
    }

    send.request.message.header.request.cas = receive.response.message.header.response.cas;
    safe_send(send.bytes, len, false);
    if (cmd == PROTOCOL_BINARY_CMD_SET) {
        safe_recv_packet(receive.bytes, sizeof(receive.bytes));
        validate_response_header(&receive.response, cmd,
                                 PROTOCOL_BINARY_RESPONSE_SUCCESS);
        cb_assert(receive.response.message.header.response.cas != send.request.message.header.request.cas);
    } else {
        return test_binary_noop();
    }

    return TEST_PASS;
}

static enum test_return test_binary_set(void) {
    return test_binary_set_impl("test_binary_set", PROTOCOL_BINARY_CMD_SET);
}

static enum test_return test_binary_setq(void) {
    return test_binary_set_impl("test_binary_setq", PROTOCOL_BINARY_CMD_SETQ);
}

static enum test_return test_binary_add_impl(const char *key, uint8_t cmd) {
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

static enum test_return test_binary_add(void) {
    return test_binary_add_impl("test_binary_add", PROTOCOL_BINARY_CMD_ADD);
}

static enum test_return test_binary_addq(void) {
    return test_binary_add_impl("test_binary_addq", PROTOCOL_BINARY_CMD_ADDQ);
}

static enum test_return test_binary_replace_impl(const char* key, uint8_t cmd) {
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
        test_binary_noop();
    }

    return TEST_PASS;
}

static enum test_return test_binary_replace(void) {
    return test_binary_replace_impl("test_binary_replace",
                                    PROTOCOL_BINARY_CMD_REPLACE);
}

static enum test_return test_binary_replaceq(void) {
    return test_binary_replace_impl("test_binary_replaceq",
                                    PROTOCOL_BINARY_CMD_REPLACEQ);
}

static enum test_return test_binary_delete_impl(const char *key, uint8_t cmd) {
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

static enum test_return test_binary_delete(void) {
    return test_binary_delete_impl("test_binary_delete",
                                   PROTOCOL_BINARY_CMD_DELETE);
}

static enum test_return test_binary_deleteq(void) {
    return test_binary_delete_impl("test_binary_deleteq",
                                   PROTOCOL_BINARY_CMD_DELETEQ);
}

static enum test_return test_binary_delete_cas_impl(const char *key, bool bad) {
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


static enum test_return test_binary_delete_cas(void) {
    return test_binary_delete_cas_impl("test_binary_delete_cas", false);
}

static enum test_return test_binary_delete_bad_cas(void) {
    return test_binary_delete_cas_impl("test_binary_delete_bad_cas", true);
}

static enum test_return test_binary_get_impl(const char *key, uint8_t cmd) {
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

static enum test_return test_binary_get(void) {
    return test_binary_get_impl("test_binary_get", PROTOCOL_BINARY_CMD_GET);
}

static enum test_return test_binary_getk(void) {
    return test_binary_get_impl("test_binary_getk", PROTOCOL_BINARY_CMD_GETK);
}

static enum test_return test_binary_getq_impl(const char *key, uint8_t cmd) {
    const char *missing = "test_binary_getq_missing";
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

static enum test_return test_binary_getq(void) {
    return test_binary_getq_impl("test_binary_getq", PROTOCOL_BINARY_CMD_GETQ);
}

static enum test_return test_binary_getkq(void) {
    return test_binary_getq_impl("test_binary_getkq", PROTOCOL_BINARY_CMD_GETKQ);
}

static enum test_return test_binary_incr_impl(const char* key, uint8_t cmd) {
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
        test_binary_noop();
    }
    return TEST_PASS;
}

static enum test_return test_binary_incr(void) {
    return test_binary_incr_impl("test_binary_incr",
                                 PROTOCOL_BINARY_CMD_INCREMENT);
}

static enum test_return test_binary_incrq(void) {
    return test_binary_incr_impl("test_binary_incrq",
                                 PROTOCOL_BINARY_CMD_INCREMENTQ);
}

static enum test_return test_binary_incr_invalid_cas_impl(const char* key, uint8_t cmd) {
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

static enum test_return test_binary_invalid_cas_incr(void) {
    return test_binary_incr_invalid_cas_impl("test_binary_incr",
                                             PROTOCOL_BINARY_CMD_INCREMENT);
}

static enum test_return test_binary_invalid_cas_incrq(void) {
    return test_binary_incr_invalid_cas_impl("test_binary_incrq",
                                             PROTOCOL_BINARY_CMD_INCREMENTQ);
}

static enum test_return test_binary_invalid_cas_decr(void) {
    return test_binary_incr_invalid_cas_impl("test_binary_incr",
                                             PROTOCOL_BINARY_CMD_DECREMENT);
}

static enum test_return test_binary_invalid_cas_decrq(void) {
    return test_binary_incr_invalid_cas_impl("test_binary_incrq",
                                             PROTOCOL_BINARY_CMD_DECREMENTQ);
}

static enum test_return test_binary_decr_impl(const char* key, uint8_t cmd) {
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
        test_binary_noop();
    }

    return TEST_PASS;
}

static enum test_return test_binary_decr(void) {
    return test_binary_decr_impl("test_binary_decr",
                                 PROTOCOL_BINARY_CMD_DECREMENT);
}

static enum test_return test_binary_decrq(void) {
    return test_binary_decr_impl("test_binary_decrq",
                                 PROTOCOL_BINARY_CMD_DECREMENTQ);
}

static enum test_return test_binary_version(void) {
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

static enum test_return test_binary_flush_impl(const char *key, uint8_t cmd) {
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

static enum test_return test_binary_flush(void) {
    return test_binary_flush_impl("test_binary_flush",
                                  PROTOCOL_BINARY_CMD_FLUSH);
}

static enum test_return test_binary_flushq(void) {
    return test_binary_flush_impl("test_binary_flushq",
                                  PROTOCOL_BINARY_CMD_FLUSHQ);
}

static enum test_return test_binary_cas(void) {
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

static enum test_return test_binary_concat_impl(const char *key, uint8_t cmd) {
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

static enum test_return test_binary_append(void) {
    return test_binary_concat_impl("test_binary_append",
                                   PROTOCOL_BINARY_CMD_APPEND);
}

static enum test_return test_binary_prepend(void) {
    return test_binary_concat_impl("test_binary_prepend",
                                   PROTOCOL_BINARY_CMD_PREPEND);
}

static enum test_return test_binary_appendq(void) {
    return test_binary_concat_impl("test_binary_appendq",
                                   PROTOCOL_BINARY_CMD_APPENDQ);
}

static enum test_return test_binary_prependq(void) {
    return test_binary_concat_impl("test_binary_prependq",
                                   PROTOCOL_BINARY_CMD_PREPENDQ);
}

static enum test_return test_binary_stat(void) {
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

static enum test_return test_binary_scrub(void) {
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

static enum test_return test_binary_pipeline_hickup_chunk(void *buffer, size_t buffersize) {
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

static enum test_return test_binary_pipeline_hickup(void)
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
        test_binary_pipeline_hickup_chunk(buffer, buffersize);
    }

    /* send quitq to shut down the read thread ;-) */
    len = raw_command(buffer, buffersize, PROTOCOL_BINARY_CMD_QUITQ,
                      NULL, 0, NULL, 0);
    safe_send(buffer, len, false);

    cb_join_thread(tid);
    free(buffer);
    return TEST_PASS;
}

static enum test_return test_binary_ioctl(void) {
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

static enum test_return test_binary_verbosity(void) {
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

static enum test_return test_binary_read(void) {
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

static enum test_return test_binary_write(void) {
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

static enum test_return test_binary_bad_tap_ttl(void) {
    union {
        protocol_binary_request_tap_flush request;
        protocol_binary_response_no_extras response;
        char bytes[1024];
    } buffer;

    size_t len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                             PROTOCOL_BINARY_CMD_TAP_FLUSH,
                             NULL, 0, NULL, 0);

    buffer.request.message.header.request.extlen = 8;
    buffer.request.message.header.request.bodylen = ntohl(8);
    buffer.request.message.body.tap.enginespecific_length = 0;
    buffer.request.message.body.tap.flags = 0;
    buffer.request.message.body.tap.ttl = 0;
    buffer.request.message.body.tap.res1 = 0;
    buffer.request.message.body.tap.res2 = 0;
    buffer.request.message.body.tap.res3 = 0;
    len += 8;

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    validate_response_header(&buffer.response,
                             PROTOCOL_BINARY_CMD_TAP_FLUSH,
                             PROTOCOL_BINARY_RESPONSE_EINVAL);
    return TEST_PASS;
}

static enum test_return test_binary_hello(void) {
    union {
        protocol_binary_request_hello request;
        protocol_binary_response_hello response;
        char bytes[1024];
    } buffer;
    const char *useragent = "hello world";
    uint16_t feature = htons(PROTOCOL_BINARY_FEATURE_DATATYPE);
    uint16_t *ptr;
    size_t len;

    memset(buffer.bytes, 0, sizeof(buffer.bytes));

    len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                      PROTOCOL_BINARY_CMD_HELLO,
                      useragent, strlen(useragent), &feature,
                      sizeof(feature));

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    validate_response_header(&buffer.response,
                             PROTOCOL_BINARY_CMD_HELLO,
                             PROTOCOL_BINARY_RESPONSE_SUCCESS);

    cb_assert(buffer.response.message.header.response.bodylen == 2);
    ptr = (uint16_t*)(buffer.bytes + sizeof(buffer.response));
    cb_assert(ntohs(*ptr) == PROTOCOL_BINARY_FEATURE_DATATYPE);

    feature = 0xffff;
    len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                             PROTOCOL_BINARY_CMD_HELLO,
                             useragent, strlen(useragent), &feature,
                             sizeof(feature));

    safe_send(buffer.bytes, len, false);
    safe_recv_packet(buffer.bytes, sizeof(buffer.bytes));
    validate_response_header(&buffer.response,
                             PROTOCOL_BINARY_CMD_HELLO,
                             PROTOCOL_BINARY_RESPONSE_SUCCESS);
    cb_assert(buffer.response.message.header.response.bodylen == 0);


    len = raw_command(buffer.bytes, sizeof(buffer.bytes),
                             PROTOCOL_BINARY_CMD_HELLO,
                             useragent, strlen(useragent), &feature,
                             sizeof(feature) - 1);

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

static enum test_return test_binary_datatype_json(void) {
    const char body[] = "{ \"value\" : 1234123412 }";
    set_datatype_feature(true);
    store_object_w_datatype("myjson", body, strlen(body), false, true);

    get_object_w_datatype("myjson", body, strlen(body), false, true, false);

    set_datatype_feature(false);
    get_object_w_datatype("myjson", body, strlen(body), false, true, true);

    return TEST_PASS;
}

static enum test_return test_binary_datatype_json_without_support(void) {
    const char body[] = "{ \"value\" : 1234123412 }";
    set_datatype_feature(false);
    store_object_w_datatype("myjson", body, strlen(body), false, false);

    get_object_w_datatype("myjson", body, strlen(body), false, false, false);

    set_datatype_feature(true);
    get_object_w_datatype("myjson", body, strlen(body), false, true, false);

    return TEST_PASS;
}

static enum test_return test_binary_datatype_compressed(void) {
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

static enum test_return test_binary_datatype_compressed_json(void) {
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

static enum test_return test_binary_invalid_datatype(void) {
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
    sock = connect_server("127.0.0.1", port, false);

    set_datatype_feature(false);
    request.message.header.request.datatype = 4;
    safe_send(&request.bytes, sizeof(request.bytes), false);
    safe_recv_packet(res.buffer, sizeof(res.buffer));
    code = res.response.message.header.response.status;
    cb_assert(code == PROTOCOL_BINARY_RESPONSE_EINVAL);
    sock = connect_server("127.0.0.1", port, false);

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

static enum test_return test_binary_dcp_noop(void) {
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

static enum test_return test_binary_dcp_buffer_ack(void) {
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

static enum test_return test_binary_dcp_control(void) {
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

typedef enum test_return (*TEST_FUNC)(void);
struct testcase {
    const char *description;
    TEST_FUNC function;
};

struct testcase testcases[] = {
    { "cache_create", cache_create_test },
    { "cache_constructor", cache_constructor_test },
    { "cache_constructor_fail", cache_fail_constructor_test },
    { "cache_destructor", cache_destructor_test },
    { "cache_reuse", cache_reuse_test },
    { "cache_redzone", cache_redzone_test },
    { "issue_161", test_issue_161 },
    { "strtof", test_safe_strtof },
    { "strtol", test_safe_strtol },
    { "strtoll", test_safe_strtoll },
    { "strtoul", test_safe_strtoul },
    { "strtoull", test_safe_strtoull },
    { "vperror", test_vperror },
    { "config_parser", test_config_parser },
    /* The following tests all run towards the same server */
    { "start_server", start_memcached_server },
    { "binary_noop", test_binary_noop },
    { "binary_quit", test_binary_quit },
    { "binary_quitq", test_binary_quitq },
    { "binary_set", test_binary_set },
    { "binary_setq", test_binary_setq },
    { "binary_add", test_binary_add },
    { "binary_addq", test_binary_addq },
    { "binary_replace", test_binary_replace },
    { "binary_replaceq", test_binary_replaceq },
    { "binary_delete", test_binary_delete },
    { "binary_delete_cas", test_binary_delete_cas },
    { "binary_delete_bad_cas", test_binary_delete_bad_cas },
    { "binary_deleteq", test_binary_deleteq },
    { "binary_get", test_binary_get },
    { "binary_getq", test_binary_getq },
    { "binary_getk", test_binary_getk },
    { "binary_getkq", test_binary_getkq },
    { "binary_incr", test_binary_incr },
    { "binary_incrq", test_binary_incrq },
    { "binary_decr", test_binary_decr },
    { "binary_decrq", test_binary_decrq },
    { "binary_incr_invalid_cas", test_binary_invalid_cas_incr },
    { "binary_incrq_invalid_cas", test_binary_invalid_cas_incrq },
    { "binary_decr_invalid_cas", test_binary_invalid_cas_decr },
    { "binary_decrq_invalid_cas", test_binary_invalid_cas_decrq },
    { "binary_version", test_binary_version },
    { "binary_flush", test_binary_flush },
    { "binary_flushq", test_binary_flushq },
    { "binary_cas", test_binary_cas },
    { "binary_append", test_binary_append },
    { "binary_appendq", test_binary_appendq },
    { "binary_prepend", test_binary_prepend },
    { "binary_prependq", test_binary_prependq },
    { "binary_stat", test_binary_stat },
    { "binary_scrub", test_binary_scrub },
    { "binary_verbosity", test_binary_verbosity },
    { "binary_read", test_binary_read },
    { "binary_write", test_binary_write },
    { "binary_bad_tap_ttl", test_binary_bad_tap_ttl },
    { "MB-10114", test_mb_10114 },
    { "binary_dcp_noop", test_binary_dcp_noop },
    { "binary_dcp_buffer_acknowledgment", test_binary_dcp_buffer_ack },
    { "binary_dcp_control", test_binary_dcp_control },
    { "binary_hello", test_binary_hello },
    { "binary_ioctl", test_binary_ioctl },
    { "binary_datatype_json", test_binary_datatype_json },
    { "binary_datatype_json_without_support", test_binary_datatype_json_without_support },
    { "binary_datatype_compressed", test_binary_datatype_compressed },
    { "binary_datatype_compressed_json", test_binary_datatype_compressed_json },
    { "binary_invalid_datatype", test_binary_invalid_datatype },
    { "session_ctrl_token", test_session_ctrl_token },
    { "binary_pipeline_hickup", test_binary_pipeline_hickup },
    { "stop_server", stop_memcached_server },
    { NULL, NULL }
};

int main(int argc, char **argv)
{
    int exitcode = 0;
    int ii = 0;
    enum test_return ret;

    cb_initialize_sockets();
    /* Use unbuffered stdio */
    setbuf(stdout, NULL);
    setbuf(stderr, NULL);

    for (ii = 0; testcases[ii].description != NULL; ++ii) {
        int jj;
        fprintf(stdout, "\r");
        for (jj = 0; jj < 60; ++jj) {
            fprintf(stdout, " ");
        }
        fprintf(stdout, "\rRunning %04d %s - ", ii + 1, testcases[ii].description);
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

    fprintf(stdout, "\r                                     \n");
    return exitcode;
}
