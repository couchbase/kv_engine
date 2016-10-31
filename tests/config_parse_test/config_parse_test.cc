/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

/*
 * unit tests for config_parse.c
 */

#include <daemon/config_parse.cc>
#include <platform/cb_malloc.h>
#include <platform/platform.h>

#include <vector>
#include <string>
#include <iostream>

#if defined(WIN32)
#include <io.h> /* for mktemp*/
#define F_OK 0
#endif

/* Test function variables / context *****************************************/

char* error_msg = NULL;

struct test_ctx {
    char* error_msg;
    cJSON *config;  /* Initial config, as a JSON object. */
    cJSON *dynamic; /* Subsequent dynamic config, as a JSON object. */
    cJSON *errors;
    char *ssl_file;
};

/* Get the path to a temporary directory. Returns NULL if one could
 * not be found.
 */
static std::string get_temp_dir(void) {
    static std::string location;

    if (location.empty()) {
        const std::vector<std::string> locations = { "TEMP", "TMP",
                                                     "TMPDIR", "/tmp",
                                                     "/var/tmp" };
        for (auto& val : locations) {
            if (val.at(0) == '/') {
                if (access(val.c_str(), F_OK) != -1) {
                    location.assign(val);
                    break;
                }
            } else {
                char* value = getenv(val.c_str());

                if (value != NULL) {
                    location.assign(value);
                    break;
                }
            }
        }
        if (location.empty()) {
            std::cerr << "Failed to locate temporary directory" << std::endl;
            exit(EXIT_FAILURE);
        }
    }

    return location;
}

/* Generates a temporary filename, and creates a file of that name.
 * Caller is responsible for cb_free()ing the string and unlink()ing the file
 * after use.
 */
static char* generate_temp_file(void) {
    char templ[1024];
    std::string tempdir = get_temp_dir();
    if (tempdir.empty()) {
        return NULL;
    }
    tempdir.append("/config_parse_test_XXXXXX");
#ifdef WIN32
    // Make sure that the path is in windows format
    std::replace(tempdir.begin(), tempdir.end(), '/', '\\');
#endif

    strcpy(templ, tempdir.c_str());

    if (cb_mktemp(templ) == NULL) {
        fprintf(stderr, "FATAL: failed to create temporary file: %s\n",
                strerror(errno));
        return NULL;
    }

    return cb_strdup(templ);
}

/* helper to convert dynamic JSON config to char*, validate and then free it */
static bool validate_dynamic_JSON_changes(struct test_ctx* ctx) {
    char* dynamic_string = cJSON_Print(ctx->dynamic);
    bool result = validate_proposed_config_changes(dynamic_string,
                                                   ctx->errors);
    cJSON_Free(dynamic_string);
    return result;
}


/* 'mock' functions / variables **********************************************/

/* mocks */

void calculate_maxconns(void) {
    /* do nothing */
}

bool load_extension(const char *soname, const char *config) {
    return true;
}

void perform_callbacks(ENGINE_EVENT_TYPE type,
                       const void *data,
                       const void *c) {
    /* do nothing */
}

const char* get_server_version(void) {
    return "";
}

ListeningPort *get_listening_port_instance(const in_port_t port) {
    static ListeningPort l(11210, nullptr, false, 10, false,
                                  Protocol::Memcached);
    return &l;
}

void initialize_breakpad(const breakpad_settings_t* settings) {
    /* do nothing */
}

void set_ssl_cipher_list(const char *list) {
    /* do nothing */
}

void set_ssl_protocol_mask(const std::string& mask) {
    // Do nothing
}

long decode_ssl_protocol(const std::string& protocol) {
    // Do nothing
    return 0L;
}

/* settings, as used by config_parse.c */
struct settings settings;
Connection *listen_conn = NULL;
/******************************************************************************
 *  setup / teardown functions
 */

/* baseline settings for use with dynamic testing (shared between multiple
 * tests)
 */
static cJSON* get_baseline_settings(const char* temp_file)
{
    cJSON* baseline = cJSON_CreateObject();

#ifdef WIN32
    cJSON_AddStringToObject(baseline, "root", "c:\\program files");
#else
    cJSON_AddStringToObject(baseline, "root", "/tmp");
#endif

    cJSON_AddStringToObject(baseline, "ssl_cipher_list", "HIGH");

    cJSON_AddStringToObject(baseline, "admin", "my_admin");
    cJSON_AddNumberToObject(baseline, "threads", 1);
    {
        cJSON *iface = cJSON_CreateObject();
        cJSON *iface_list = cJSON_CreateArray();
        cJSON_AddStringToObject(iface, "host", "my_host");
        cJSON_AddNumberToObject(iface, "port", 1234);
        cJSON_AddTrueToObject(iface, "ipv4");
        cJSON_AddTrueToObject(iface, "ipv6");
        cJSON_AddNumberToObject(iface, "maxconn", 10);
        cJSON_AddNumberToObject(iface, "backlog", 10);
        cJSON_AddTrueToObject(iface, "tcp_nodelay");
        {
            cJSON *ssl = cJSON_CreateObject();
            cJSON_AddStringToObject(ssl, "key", temp_file);
            cJSON_AddStringToObject(ssl, "cert", temp_file);
            cJSON_AddItemToObject(iface, "ssl", ssl);
        }
        cJSON_AddItemToArray(iface_list, iface);
        cJSON_AddItemToObject(baseline, "interfaces", iface_list);
    }
    {
        cJSON *ext = cJSON_CreateObject();
        cJSON *ext_list = cJSON_CreateArray();
        cJSON_AddStringToObject(ext, "module", "extension.so");
        cJSON_AddStringToObject(ext, "config", "config_string_for_module");
        cJSON_AddItemToArray(ext_list, ext);
        cJSON_AddItemToObject(baseline, "extensions", ext_list);
    }
    cJSON_AddTrueToObject(baseline, "require_sasl");
    cJSON_AddFalseToObject(baseline, "require_init");
    cJSON_AddNumberToObject(baseline, "default_reqs_per_event", 1);
    cJSON_AddNumberToObject(baseline, "reqs_per_event_low_priority", 5);
    cJSON_AddNumberToObject(baseline, "reqs_per_event_med_priority", 10);
    cJSON_AddNumberToObject(baseline, "reqs_per_event_high_priority", 20);
    cJSON_AddNumberToObject(baseline, "verbosity", 1);
    cJSON_AddNumberToObject(baseline, "connection_idle_time", 300);
    cJSON_AddNumberToObject(baseline, "bio_drain_buffer_sz", 1);
    cJSON_AddFalseToObject(baseline, "rbac_privilege_debug");
    cJSON_AddTrueToObject(baseline, "datatype_support");
    {
        cJSON *breakpad = cJSON_CreateObject();
        cJSON_AddTrueToObject(breakpad, "enabled");
        cJSON_AddStringToObject(breakpad, "minidump_dir", "minidump_dir");
        cJSON_AddStringToObject(breakpad, "content", "default");
        cJSON_AddItemToObject(baseline, "breakpad", breakpad);
    }

    return baseline;
}

/* setup / teardown functions */

/* setup for "normal" config checking */
static void setup(struct test_ctx *ctx) {
    ctx->config = cJSON_CreateObject();
    error_msg = NULL;
    memset(&settings, 0, sizeof(settings));
}

/* teardown for "normal" config checking */
static void teardown(struct test_ctx *ctx) {
    free_settings(&settings);
    free(error_msg);
    cJSON_Delete(ctx->config);
}

static void setup_interfaces(struct test_ctx *ctx) {
    ctx->config = cJSON_Parse("{ \"interfaces\" :"
                       "    ["
                       "        {"
                       "            \"maxconn\" : 12,"
                       "            \"backlog\" : 34,"
                       "            \"port\" : 12345,"
                       "            \"host\" : \"my_host\""
                       "        }"
                       "    ]"
                       "}");
    cb_assert(ctx->config != NULL);
    error_msg = NULL;
    memset(&settings, 0, sizeof(settings));
}

static void setup_breakpad(struct test_ctx *ctx) {
    ctx->config = cJSON_Parse("{ \"breakpad\" :"
                       "    {"
                       "        \"enabled\" : false"
                       "    }"
                       "}");
    cb_assert(ctx->config != NULL);
    error_msg = NULL;
    memset(&settings, 0, sizeof(settings));
}

static void setup_dynamic(struct test_ctx *ctx) {
    memset(&settings, 0, sizeof(settings));

    ctx->ssl_file = generate_temp_file();
    cb_assert(ctx->ssl_file != NULL);
    ctx->config = get_baseline_settings(ctx->ssl_file);
    ctx->dynamic = get_baseline_settings(ctx->ssl_file);
    ctx->errors = cJSON_CreateArray();

    /* Load the baseline config into (global) settings struct. */
    cb_assert(parse_JSON_config(ctx->config, &settings, &ctx->error_msg));
}

static void teardown_dynamic(struct test_ctx *ctx) {
    free_settings(&settings);

    cJSON_Delete(ctx->errors);
    cJSON_Delete(ctx->dynamic);
    cJSON_Delete(ctx->config);
    unlink(ctx->ssl_file);
    cb_free(ctx->ssl_file);
}


/* tests */
static void test_admin_1(struct test_ctx *ctx) {
    /* Empty string should disable admin */
    cJSON_AddStringToObject(ctx->config, "admin", "");
    cb_assert(parse_JSON_config(ctx->config, &settings, &error_msg));
    cb_assert(settings.disable_admin);
}

static void test_admin_2(struct test_ctx *ctx) {
    /* non-empty should set admin to that */
    cJSON_AddStringToObject(ctx->config, "admin", "my_admin");
    cb_assert(parse_JSON_config(ctx->config, &settings, &error_msg));
    cb_assert(settings.disable_admin == false);
    cb_assert(strcmp(settings.admin, "my_admin") == 0);
}

static void test_admin_3(struct test_ctx *ctx) {
    /* admin must be a string */
    cJSON_AddNumberToObject(ctx->config, "admin", 1.0);
    cb_assert(parse_JSON_config(ctx->config, &settings, &error_msg) == false);
    cb_assert(error_msg != NULL);
}

static void test_ssl_list_cipher_1(struct test_ctx *ctx) {
    cJSON_AddStringToObject(ctx->config, "ssl_cipher_list", "HIGH");
    cb_assert(parse_JSON_config(ctx->config, &settings, &error_msg));
    cb_assert(settings.ssl_cipher_list != NULL);
    cb_assert(strcmp(settings.ssl_cipher_list, "HIGH") == 0);
}

static void test_ssl_list_cipher_2(struct test_ctx *ctx) {
    cJSON_AddStringToObject(ctx->config, "ssl_cipher_list", "");
    cb_assert(parse_JSON_config(ctx->config, &settings, &error_msg));
    cb_assert(settings.ssl_cipher_list == NULL);
}

static void test_threads_1(struct test_ctx *ctx) {
    /* Valid value should update settings.num_threads */
    cJSON_AddNumberToObject(ctx->config, "threads", 6);
    cb_assert(parse_JSON_config(ctx->config, &settings, &error_msg));
    cb_assert(settings.num_threads == 6);
}

static void test_threads_2(struct test_ctx *ctx) {
    /* Valid integer as string should work */
    cJSON_AddStringToObject(ctx->config, "threads", "7");
    cb_assert(parse_JSON_config(ctx->config, &settings, &error_msg));
    cb_assert(settings.num_threads == 7);
}

static void test_threads_3(struct test_ctx *ctx) {
    /* non-int should error */
    cJSON_AddStringToObject(ctx->config, "threads", "eight");
    cb_assert(parse_JSON_config(ctx->config, &settings, &error_msg) == false);
    cb_assert(error_msg != NULL);
}

static void test_interfaces_1(struct test_ctx *ctx) {
    /* test basic parsing */
    cb_assert(ctx->config != NULL);
    cb_assert(parse_JSON_config(ctx->config, &settings, &error_msg));
    cb_assert(settings.interfaces[0].maxconn = 12);
    cb_assert(settings.interfaces[0].backlog = 34);
    cb_assert(settings.interfaces[0].port = 12345);
}

static void test_interfaces_2(struct test_ctx *ctx) {
    /* port out of range should fail. */
    cJSON *iface = cJSON_GetArrayItem(cJSON_GetObjectItem(ctx->config,
                                                          "interfaces"), 0);
    cJSON_ReplaceItemInObject(iface, "port", cJSON_CreateNumber(100000));
    cb_assert(parse_JSON_config(ctx->config, &settings, &error_msg) == false);
    cb_assert(error_msg != NULL);
}

static void test_interfaces_3(struct test_ctx *ctx) {
    /* non-string host should fail. */
    cJSON *iface = cJSON_GetArrayItem(cJSON_GetObjectItem(ctx->config,
                                                          "interfaces"), 0);
    cJSON_ReplaceItemInObject(iface, "host", cJSON_CreateNumber(1));
    cb_assert(parse_JSON_config(ctx->config, &settings, &error_msg) == false);
    cb_assert(error_msg != NULL);
}

static void test_interfaces_4(struct test_ctx *ctx) {
    /* must have at least one of IPv4 & IPv6 */
    cJSON *iface = cJSON_GetArrayItem(cJSON_GetObjectItem(ctx->config,
                                                          "interfaces"), 0);
    cJSON_AddFalseToObject(iface, "ipv4");
    cJSON_AddFalseToObject(iface, "ipv6");
    cb_assert(parse_JSON_config(ctx->config, &settings, &error_msg) == false);
    cb_assert(error_msg != NULL);
}

static void test_interfaces_5(struct test_ctx *ctx) {
    /* detect memcached protocol */
    cJSON *iface = cJSON_GetArrayItem(cJSON_GetObjectItem(ctx->config,
                                                          "interfaces"), 0);
    cJSON_AddStringToObject(iface, "protocol", "memcached");
    cb_assert(parse_JSON_config(ctx->config, &settings, &error_msg));
    cb_assert(error_msg == NULL);
}

static void test_interfaces_6(struct test_ctx *ctx) {
    /* detect greenstack protocol */
    cJSON *iface = cJSON_GetArrayItem(cJSON_GetObjectItem(ctx->config,
                                                          "interfaces"), 0);
    cJSON_AddStringToObject(iface, "protocol", "greenstack");
    cb_assert(parse_JSON_config(ctx->config, &settings, &error_msg));
    cb_assert(error_msg == NULL);
}

static void test_interfaces_7(struct test_ctx *ctx) {
    /* detect illegal protocol */
    cJSON *iface = cJSON_GetArrayItem(cJSON_GetObjectItem(ctx->config,
                                                          "interfaces"), 0);
    cJSON_AddStringToObject(iface, "protocol", "bubba");
    cb_assert(!parse_JSON_config(ctx->config, &settings, &error_msg));
    cb_assert(error_msg != NULL);
}

static void test_management_interface_1(struct test_ctx *ctx) {
    /* detect a management tag set to true */
    cJSON *iface = cJSON_GetArrayItem(cJSON_GetObjectItem(ctx->config,
                                                          "interfaces"), 0);
    cJSON_AddTrueToObject(iface, "management");
    cb_assert(parse_JSON_config(ctx->config, &settings, &error_msg));
    cb_assert(error_msg == NULL);
    cb_assert(settings.interfaces[0].management);
}

static void test_management_interface_2(struct test_ctx *ctx) {
    /* detect a management tag set to false */
    cJSON *iface = cJSON_GetArrayItem(cJSON_GetObjectItem(ctx->config,
                                                          "interfaces"), 0);
    cJSON_AddFalseToObject(iface, "management");
    cb_assert(parse_JSON_config(ctx->config, &settings, &error_msg));
    cb_assert(error_msg == NULL);
    cb_assert(!settings.interfaces[0].management);
}

static void test_management_interface_3(struct test_ctx *ctx) {
    /* detect that a missing management tag means false */
    cJSON *iface = cJSON_GetArrayItem(cJSON_GetObjectItem(ctx->config,
                                                          "interfaces"), 0);
    cb_assert(cJSON_GetObjectItem(iface, "management") == nullptr);
    cb_assert(parse_JSON_config(ctx->config, &settings, &error_msg));
    cb_assert(error_msg == NULL);
    cb_assert(!settings.interfaces[0].management);
}

static void test_interfaces_duplicate_port(struct test_ctx *ctx) {
    /* Can't have two different interfaces with the same port number. */

    /* 1. Create a second interface, with same port as first. */
    cJSON *iface_list = cJSON_GetObjectItem(ctx->config, "interfaces");

    cJSON *iface1 = cJSON_GetArrayItem(iface_list, 0);
    cJSON *iface2 = cJSON_CreateObject();
    cJSON_AddStringToObject(iface2, "host", "my_host");
    cJSON_AddItemReferenceToObject(iface2, "port", cJSON_GetObjectItem(iface1, "port"));
    cJSON_AddTrueToObject(iface2, "ipv4");
    cJSON_AddTrueToObject(iface2, "ipv6");
    cJSON_AddNumberToObject(iface2, "maxconn", 10);
    cJSON_AddNumberToObject(iface2, "backlog", 10);
    cJSON_AddTrueToObject(iface2, "tcp_nodelay");
    cJSON_AddItemToArray(iface_list, iface2);

    cb_assert(parse_JSON_config(ctx->config, &settings, &error_msg) == false);
    cb_assert(error_msg != NULL);
}

static void test_breakpad_1(struct test_ctx *ctx) {
    /* Test breakpad with baseline config (breakpad disabled). */
    cb_assert(parse_JSON_config(ctx->config, &settings, &error_msg));
    cb_assert(settings.breakpad.enabled == false);
}

static void test_breakpad_2(struct test_ctx *ctx) {
    /* Can't enable without specifying a minidump_dir. */
    cJSON *breakpad = cJSON_GetObjectItem(ctx->config, "breakpad");
    cJSON_ReplaceItemInObject(breakpad, "enabled", cJSON_CreateTrue());
    cb_assert(parse_JSON_config(ctx->config, &settings, &error_msg) == false);
    cb_assert(error_msg != NULL);
}

static void test_breakpad_3(struct test_ctx *ctx) {
    /* Content can only be 'default. */
    cJSON *breakpad = cJSON_GetObjectItem(ctx->config, "breakpad");
    cJSON_ReplaceItemInObject(breakpad, "enabled", cJSON_CreateTrue());
    cJSON_AddStringToObject(breakpad, "minidump_dir", "minidump_dir");
    cJSON_AddStringToObject(breakpad, "content", "custom");
    cb_assert(parse_JSON_config(ctx->config, &settings, &error_msg) == false);
    cb_assert(error_msg != NULL);
}

static void test_dynamic_same(struct test_ctx *ctx) {
    /* Identity config should be valid */
    cb_assert(validate_dynamic_JSON_changes(ctx));
}

static void test_dynamic_admin(struct test_ctx *ctx) {
    /* Admin cannot be changed */
    cJSON_ReplaceItemInObject(ctx->dynamic, "admin", cJSON_CreateString("different_admin"));
    cb_assert(validate_dynamic_JSON_changes(ctx) == false);
    cb_assert(cJSON_GetArraySize(ctx->errors) == 1);
}

static void test_dynamic_threads(struct test_ctx *ctx) {
    /* Threads cannot be changed */
    cJSON_ReplaceItemInObject(ctx->dynamic, "threads", cJSON_CreateNumber(9));
    cb_assert(validate_dynamic_JSON_changes(ctx) == false);
    cb_assert(cJSON_GetArraySize(ctx->errors) == 1);
}

static void test_dynamic_interfaces_count(struct test_ctx *ctx) {
    /* Count of interfaces cannot be changed */
    cJSON *iface = cJSON_CreateObject();
    cJSON_AddItemToArray(cJSON_GetObjectItem(ctx->dynamic, "interfaces"),
                         iface);
    cb_assert(validate_dynamic_JSON_changes(ctx) == false);
    cb_assert(cJSON_GetArraySize(ctx->errors) == 1);
}

static void test_dynamic_interfaces_host(struct test_ctx *ctx) {
    /* Cannot change host at runtime */
    cJSON *iface = cJSON_GetArrayItem(cJSON_GetObjectItem(ctx->dynamic,
                                                          "interfaces"), 0);
    cJSON_ReplaceItemInObject(iface, "host",
                              cJSON_CreateString("different_host"));
    cb_assert(validate_dynamic_JSON_changes(ctx) == false);
    cb_assert(cJSON_GetArraySize(ctx->errors) == 1);
}

static void test_dynamic_interfaces_port(struct test_ctx *ctx) {
    /* Cannot change port at runtime */
    cJSON *iface = cJSON_GetArrayItem(cJSON_GetObjectItem(ctx->dynamic,
                                                          "interfaces"), 0);
    cJSON_ReplaceItemInObject(iface, "port",
                              cJSON_CreateNumber(5678));
    cb_assert(validate_dynamic_JSON_changes(ctx) == false);
    cb_assert(cJSON_GetArraySize(ctx->errors) == 1);
}

static void test_dynamic_interfaces_ipv4(struct test_ctx *ctx) {
    /* Cannot change IPv4 at runtime */
    cJSON *iface = cJSON_GetArrayItem(cJSON_GetObjectItem(ctx->dynamic,
                                                          "interfaces"), 0);
    cJSON_ReplaceItemInObject(iface, "ipv4", cJSON_CreateFalse());
    cb_assert(validate_dynamic_JSON_changes(ctx) == false);
    cb_assert(cJSON_GetArraySize(ctx->errors) == 1);
}

static void test_dynamic_interfaces_ipv6(struct test_ctx *ctx) {
    /* Cannot change IPv6 at runtime */
    cJSON *iface = cJSON_GetArrayItem(cJSON_GetObjectItem(ctx->dynamic,
                                                          "interfaces"), 0);
    cJSON_ReplaceItemInObject(iface, "ipv6", cJSON_CreateFalse());
    cb_assert(validate_dynamic_JSON_changes(ctx) == false);
    cb_assert(cJSON_GetArraySize(ctx->errors) == 1);
}

static void test_dynamic_interfaces_maxconn(struct test_ctx *ctx) {
    /* CAN change maxxconn */
    cJSON *iface = cJSON_GetArrayItem(cJSON_GetObjectItem(ctx->dynamic,
                                                          "interfaces"), 0);
    cJSON_ReplaceItemInObject(iface, "maxconn", cJSON_CreateNumber(100));
    cb_assert(validate_dynamic_JSON_changes(ctx));
}

static void test_dynamic_interfaces_backlog(struct test_ctx *ctx) {
    /* CAN change backlog */
    cJSON *iface = cJSON_GetArrayItem(cJSON_GetObjectItem(ctx->dynamic,
                                                          "interfaces"), 0);
    cJSON_ReplaceItemInObject(iface, "backlog", cJSON_CreateNumber(100));
    cb_assert(validate_dynamic_JSON_changes(ctx));
}

static void test_dynamic_interfaces_tcp_nodelay(struct test_ctx *ctx) {
    /* CAN change tcp_nodelay */
    cJSON *iface = cJSON_GetArrayItem(cJSON_GetObjectItem(ctx->dynamic,
                                                          "interfaces"), 0);
    cJSON_ReplaceItemInObject(iface, "tcp_nodelay", cJSON_CreateFalse());
    cb_assert(validate_dynamic_JSON_changes(ctx));
}

static void test_dynamic_interfaces_ssl(struct test_ctx *ctx) {
    /* CAN change SSL settings */
    char *new_file = generate_temp_file();
    cb_assert(new_file != NULL);
    cJSON *iface = cJSON_GetArrayItem(cJSON_GetObjectItem(ctx->dynamic,
                                                          "interfaces"), 0);
    cJSON* ssl = cJSON_GetObjectItem(iface, "ssl");

    /* Change SSH key */

    cJSON_ReplaceItemInObject(ssl, "key", cJSON_CreateString(new_file));
    cb_assert(validate_dynamic_JSON_changes(ctx));

    /* Change SSH cert */
    cJSON_ReplaceItemInObject(ssl, "cert", cJSON_CreateString(new_file));
    cb_assert(validate_dynamic_JSON_changes(ctx));
    cb_free(new_file);
}

static void test_dynamic_extensions_count(struct test_ctx *ctx) {
    /* Cannot change number of extensions */
    cJSON *ext = cJSON_CreateObject();
    cJSON_AddItemToArray(cJSON_GetObjectItem(ctx->dynamic, "extensions"),
                         ext);
    cb_assert(validate_dynamic_JSON_changes(ctx) == false);
    cb_assert(cJSON_GetArraySize(ctx->errors) == 1);
}

static void test_dynamic_extensions_module(struct test_ctx *ctx) {
    /* Cannot change extension module */
    cJSON *ext = cJSON_GetArrayItem(cJSON_GetObjectItem(ctx->dynamic,
                                                          "extensions"), 0);
    cJSON_ReplaceItemInObject(ext, "module", cJSON_CreateString("different.so"));
    cb_assert(validate_dynamic_JSON_changes(ctx) == false);
    cb_assert(cJSON_GetArraySize(ctx->errors) == 1);
}

static void test_dynamic_extensions_config(struct test_ctx *ctx) {
    /* Cannot change extension module */
    cJSON *ext = cJSON_GetArrayItem(cJSON_GetObjectItem(ctx->dynamic,
                                                          "extensions"), 0);
    cJSON_ReplaceItemInObject(ext, "config",
                              cJSON_CreateString("different_config_for_module"));
    cb_assert(validate_dynamic_JSON_changes(ctx) == false);
    cb_assert(cJSON_GetArraySize(ctx->errors) == 1);
}

static void test_dynamic_require_sasl(struct test_ctx *ctx) {
    /* Cannot change equire_sasl */
    cJSON_ReplaceItemInObject(ctx->dynamic, "require_sasl", cJSON_CreateFalse());
    cb_assert(validate_dynamic_JSON_changes(ctx) == false);
    cb_assert(cJSON_GetArraySize(ctx->errors) == 1);
}

static void test_dynamic_require_init(struct test_ctx *ctx) {
    /* Cannot change require_init */
    cJSON_ReplaceItemInObject(ctx->dynamic, "require_init", cJSON_CreateTrue());
    cb_assert(validate_dynamic_JSON_changes(ctx) == false);
    cb_assert(cJSON_GetArraySize(ctx->errors) == 1);
}

static void test_dynamic_reqs_per_event(struct test_ctx *ctx) {
    /* CAN change reqs_per_event */
    cJSON_ReplaceItemInObject(ctx->dynamic, "default_reqs_per_event", cJSON_CreateNumber(2));
    cb_assert(validate_dynamic_JSON_changes(ctx));
}

static void test_dynamic_verbosity(struct test_ctx *ctx) {
    /* CAN change verbosity */
    cJSON_ReplaceItemInObject(ctx->dynamic, "verbosity", cJSON_CreateNumber(2));
    cb_assert(validate_dynamic_JSON_changes(ctx));
}

static void test_dynamic_connection_idle_time(struct test_ctx *ctx) {
    /* CAN change connection_idle_time */
    cJSON_ReplaceItemInObject(ctx->dynamic, "connection_idle_time",
                              cJSON_CreateNumber(600));
    cb_assert(validate_dynamic_JSON_changes(ctx));
}

static void test_dynamic_bio_drain_buffer_sz(struct test_ctx *ctx) {
    /* Cannot change dynamic_bio_drain_buffer_sz */
    cJSON_ReplaceItemInObject(ctx->dynamic, "bio_drain_buffer_sz",
                              cJSON_CreateNumber(2));
    cb_assert(validate_dynamic_JSON_changes(ctx) == false);
    cb_assert(cJSON_GetArraySize(ctx->errors) == 1);
}

static void test_dynamic_datatype(struct test_ctx *ctx) {
    /* Cannot change datatype */
    cJSON_ReplaceItemInObject(ctx->dynamic, "datatype_support", cJSON_CreateFalse());
    cb_assert(validate_dynamic_JSON_changes(ctx) == false);
    cb_assert(cJSON_GetArraySize(ctx->errors) == 1);
}

static void test_dynamic_root(struct test_ctx *ctx) {
    /* Cannot change root */
#ifdef WIN32
    cJSON_ReplaceItemInObject(ctx->dynamic, "root", cJSON_CreateString("c:\\"));
#else
    cJSON_ReplaceItemInObject(ctx->dynamic, "root", cJSON_CreateString("/var"));
#endif
    cb_assert(validate_dynamic_JSON_changes(ctx) == false);
    cb_assert(cJSON_GetArraySize(ctx->errors) == 1);
}

static void setup_invalid_root(struct test_ctx *ctx) {
    ctx->config = cJSON_Parse("{\"root\":\"/it/would/suck/if/you/had/this\"}");
    error_msg = NULL;
    memset(&settings, 0, sizeof(settings));
}

static void test_invalid_root(struct test_ctx *ctx) {
    cb_assert(parse_JSON_config(ctx->config, &settings, &error_msg) == false);
    cb_assert(strstr(error_msg, "/it/would/suck/if/you/had/this") != 0);
}

static void teardown_invalid_root(struct test_ctx *ctx) {
    free(error_msg);
    cJSON_Delete(ctx->config);
    cJSON_Free((char*)settings.config);
}

static void setup_max_packet_size(struct test_ctx *ctx) {
    ctx->config = cJSON_Parse("{\"max_packet_size\": 30}");
    error_msg = NULL;
    memset(&settings, 0, sizeof(settings));
}

static void test_max_packet_size(struct test_ctx *ctx) {
    cb_assert(parse_JSON_config(ctx->config, &settings, &error_msg) == true);
    cb_assert(error_msg == NULL);
    cb_assert(settings.max_packet_size == (30 * 1024 * 1024));
}

static void teardown_max_packet_size(struct test_ctx *ctx) {
    cJSON_Delete(ctx->config);
    cJSON_Free((char*)settings.config);
}

static void test_dynamic_ssl_cipher_list_1(struct test_ctx *ctx) {
    cJSON_ReplaceItemInObject(ctx->dynamic, "ssl_cipher_list",
                              cJSON_CreateString("DEFAULT"));
    cb_assert(validate_dynamic_JSON_changes(ctx));
    cb_assert(cJSON_GetArraySize(ctx->errors) == 0);
}

static void test_dynamic_ssl_cipher_list_2(struct test_ctx *ctx) {
    cJSON_ReplaceItemInObject(ctx->dynamic, "ssl_cipher_list",
                              cJSON_CreateString(""));
    cb_assert(validate_dynamic_JSON_changes(ctx));
    cb_assert(cJSON_GetArraySize(ctx->errors) == 0);
}

static void test_dynamic_breakpad_1(struct test_ctx *ctx) {
    /* Check enabled can be changed from true -> false. */
    cJSON *breakpad = cJSON_GetObjectItem(ctx->dynamic, "breakpad");
    cJSON *enabled = cJSON_GetObjectItem(breakpad, "enabled");
    cb_assert(enabled->type == cJSON_True);
    cJSON_ReplaceItemInObject(breakpad, "enabled", cJSON_CreateFalse());
    cb_assert(validate_dynamic_JSON_changes(ctx));
    cb_assert(cJSON_GetArraySize(ctx->errors) == 0);
}

static void test_dynamic_breakpad_2(struct test_ctx *ctx) {
    /* Check minidump_dir can be changed. */
    cJSON *breakpad = cJSON_GetObjectItem(ctx->dynamic, "breakpad");
    cJSON_ReplaceItemInObject(breakpad, "minidump_dir",
                              cJSON_CreateString("new_minidump_dir"));
    cb_assert(validate_dynamic_JSON_changes(ctx));
    cb_assert(cJSON_GetArraySize(ctx->errors) == 0);
}

/* callback from recnfig */
void auth_set_privilege_debug(bool enable) {
}

static void test_dynamic_privilege_debug(struct test_ctx *ctx) {
    cJSON_ReplaceItemInObject(ctx->dynamic, "rbac_privilege_debug",
                              cJSON_CreateTrue());
    cb_assert(validate_dynamic_JSON_changes(ctx));
    cb_assert(cJSON_GetArraySize(ctx->errors) == 0);
}

typedef void (*test_func)(struct test_ctx* ctx);

int main(void)
{
    struct TestCase {
        const char* name;
        test_func setup;
        test_func run;
        test_func teardown;
    };
    std::vector<TestCase> tests = {
        { "admin_1", setup, test_admin_1, teardown },
        { "admin_2", setup, test_admin_2, teardown },
        { "admin_3", setup, test_admin_3, teardown },
        { "ssl_cipher_list_1", setup, test_ssl_list_cipher_1, teardown },
        { "ssl_cipher_list_2", setup, test_ssl_list_cipher_2, teardown },
        { "threads_1", setup, test_threads_1, teardown },
        { "threads_2", setup, test_threads_2, teardown },
        { "threads_3", setup, test_threads_3, teardown },
        { "interfaces_1", setup_interfaces, test_interfaces_1, teardown },
        { "interfaces_2", setup_interfaces, test_interfaces_2, teardown },
        { "interfaces_3", setup_interfaces, test_interfaces_3, teardown },
        { "interfaces_4", setup_interfaces, test_interfaces_4, teardown },
        { "interfaces_5", setup_interfaces, test_interfaces_5, teardown },
        { "interfaces_6", setup_interfaces, test_interfaces_6, teardown },
        { "interfaces_7", setup_interfaces, test_interfaces_7, teardown },
        { "management_interface_true", setup_interfaces, test_management_interface_1, teardown },
        { "management_interface_false", setup_interfaces, test_management_interface_2, teardown },
        { "management_interface_missing", setup_interfaces, test_management_interface_3, teardown },
        { "interfaces_duplicate", setup_interfaces, test_interfaces_duplicate_port, teardown },
        { "root invalid path", setup_invalid_root, test_invalid_root, teardown_invalid_root },
        { "max_packet_size", setup_max_packet_size, test_max_packet_size, teardown_max_packet_size },
        { "breakpad_1", setup_breakpad, test_breakpad_1, teardown },
        { "breakpad_2", setup_breakpad, test_breakpad_2, teardown },
        { "breakpad_3", setup_breakpad, test_breakpad_3, teardown },
        { "dynamic_same", setup_dynamic, test_dynamic_same, teardown_dynamic },
        { "dynamic_admin", setup_dynamic, test_dynamic_admin, teardown_dynamic },
        { "dynamic_threads", setup_dynamic, test_dynamic_threads, teardown_dynamic },
        { "dynamic_interfaces_count", setup_dynamic, test_dynamic_interfaces_count, teardown_dynamic },
        { "dynamic_interfaces_host", setup_dynamic, test_dynamic_interfaces_host, teardown_dynamic },
        { "dynamic_interfaces_port", setup_dynamic, test_dynamic_interfaces_port, teardown_dynamic },
        { "dynamic_interfaces_ipv4", setup_dynamic, test_dynamic_interfaces_ipv4, teardown_dynamic },
        { "dynamic_interfaces_ipv6", setup_dynamic, test_dynamic_interfaces_ipv6, teardown_dynamic },
        { "dynamic_interfaces_maxconn", setup_dynamic, test_dynamic_interfaces_maxconn, teardown_dynamic },
        { "dynamic_interfaces_backlog", setup_dynamic, test_dynamic_interfaces_backlog, teardown_dynamic },
        { "dynamic_interfaces_tcp_nodelay", setup_dynamic, test_dynamic_interfaces_tcp_nodelay, teardown_dynamic },
        { "dynamic_interfaces_ssl", setup_dynamic, test_dynamic_interfaces_ssl, teardown_dynamic },
        { "dynamic_extensions_count", setup_dynamic, test_dynamic_extensions_count, teardown_dynamic },
        { "dynamic_extensions_module", setup_dynamic, test_dynamic_extensions_module, teardown_dynamic },
        { "dynamic_extensions_config", setup_dynamic, test_dynamic_extensions_config, teardown_dynamic },
        { "dynamic_require_sasl", setup_dynamic, test_dynamic_require_sasl, teardown_dynamic },
        { "dynamic_require_init", setup_dynamic, test_dynamic_require_init, teardown_dynamic },
        { "dynamic_reqs_per_event", setup_dynamic, test_dynamic_reqs_per_event, teardown_dynamic },
        { "dynamic_verbosity", setup_dynamic, test_dynamic_verbosity, teardown_dynamic },
        { "dynamic_connection_idle_time", setup_dynamic, test_dynamic_connection_idle_time, teardown_dynamic },
        { "dynamic_bio_drain_buffer_sz", setup_dynamic, test_dynamic_bio_drain_buffer_sz, teardown_dynamic },
        { "dynamic_datatype", setup_dynamic, test_dynamic_datatype, teardown_dynamic },
        { "root", setup_dynamic, test_dynamic_root, teardown_dynamic },
        { "dynamic_ssl_cipher_list_1", setup_dynamic, test_dynamic_ssl_cipher_list_1, teardown_dynamic },
        { "dynamic_ssl_cipher_list_2", setup_dynamic, test_dynamic_ssl_cipher_list_2, teardown_dynamic },
        { "dynamic_breakpad_1", setup_dynamic, test_dynamic_breakpad_1, teardown_dynamic },
        { "dynamic_breakpad_2", setup_dynamic, test_dynamic_breakpad_2, teardown_dynamic },
        { "dynamic_privilege_debug", setup_dynamic, test_dynamic_privilege_debug, teardown_dynamic },

    };

    setbuf(stdout, NULL);
    /* run tests */
    int ii = 0;
    for (auto& c : tests)
    {
        struct test_ctx ctx;
        printf("\r                                                         ");
        printf("\rRunning test %02d - %s", ii++, c.name);
        c.setup(&ctx);
        c.run(&ctx);
        c.teardown(&ctx);
    }
    printf("\rAll tests passed                                         \n");

    return EXIT_SUCCESS;
}
