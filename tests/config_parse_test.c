/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

/*
 * unit tests for config_parse.c
 */

#include <daemon/config_parse.c>

#if defined(WIN32)
#include <io.h> /* for mktemp*/
#endif

/* Test function variables / context *****************************************/

char* error_msg = NULL;

struct test_ctx {
    char* error_msg;
    cJSON *config;
    cJSON *dynamic;
    cJSON *errors;
    char *ssl_file;
};

/* 'mock' functions / variables **********************************************/

/* mocks */
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

/* global 'mock' settings, as used by config_parse.c */
struct settings settings;

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

typedef void (*test_func)(struct test_ctx* ctx);

int main(void)
{
    struct {
        const char* name;
        test_func setup;
        test_func run;
        test_func teardown;
    } tests[] = {
        { "admin_1", setup, test_admin_1, teardown },
        { "admin_2", setup, test_admin_2, teardown },
        { "admin_3", setup, test_admin_3, teardown },
        { "threads_1", setup, test_threads_1, teardown },
        { "threads_2", setup, test_threads_2, teardown },
        { "threads_3", setup, test_threads_3, teardown },
        { "interfaces_1", setup_interfaces, test_interfaces_1, teardown },
        { "interfaces_2", setup_interfaces, test_interfaces_2, teardown },
        { "interfaces_3", setup_interfaces, test_interfaces_3, teardown },
        { "interfaces_4", setup_interfaces, test_interfaces_4, teardown },
    };
    int i;

    /* run tests */
    for (i = 0; i < sizeof(tests) / sizeof(tests[0]); i++)
    {
        struct test_ctx ctx;
        printf("\rRunning test %02d - %s", i, tests[i].name);
        tests[i].setup(&ctx);
        tests[i].run(&ctx);
        tests[i].teardown(&ctx);
    }

    return EXIT_SUCCESS;
}
