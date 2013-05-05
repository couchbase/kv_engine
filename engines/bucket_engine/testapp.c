/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <string.h>
#include <stdint.h>
#include <ctype.h>
#include <assert.h>
#include <time.h>
#include <errno.h>
#include <platform/platform.h>

#include "genhash.h"

#include "bucket_engine.h"

#include <memcached/engine.h>
#include "memcached/util.h"

#include "bucket_engine_internal.h"

#ifdef WIN32
#define BUCKET_ENGINE_PATH "bucket_engine.dll"
#define ENGINE_PATH "bucket_engine_mock_engine.dll"
#define DEFAULT_CONFIG "engine=bucket_engine_mock_engine.dll;default=true;admin=admin;auto_create=false"
#define DEFAULT_CONFIG_NO_DEF "engine=bucket_engine_mock_engine.dll;default=false;admin=admin;auto_create=false"
#define DEFAULT_CONFIG_AC "engine=bucket_engine_mock_engine.dll;default=true;admin=admin;auto_create=true"
#else
#define BUCKET_ENGINE_PATH "bucket_engine.so"
#define ENGINE_PATH "bucket_engine_mock_engine.so"
#define DEFAULT_CONFIG "engine=bucket_engine_mock_engine.so;default=true;admin=admin;auto_create=false"
#define DEFAULT_CONFIG_NO_DEF "engine=bucket_engine_mock_engine.so;default=false;admin=admin;auto_create=false"
#define DEFAULT_CONFIG_AC "engine=bucket_engine_mock_engine.so;default=true;admin=admin;auto_create=true"
#endif

#define MOCK_CONFIG_NO_ALLOC "no_alloc"

#define CONN_MAGIC 0xbeefcafe

cb_mutex_t notify_mutex;
cb_cond_t notify_cond;
ENGINE_ERROR_CODE notify_code;

static void delay(void) {
#ifdef WIN32
    Sleep(1);
#else
    usleep(1000);
#endif
}

static void notify_io_complete(const void *cookie, ENGINE_ERROR_CODE code) {
    (void)cookie;
    cb_mutex_enter(&notify_mutex);
    notify_code = code;
    cb_cond_signal(&notify_cond);
    cb_mutex_exit(&notify_mutex);
}

protocol_binary_response_status last_status = 0;
char *last_key = NULL;
char *last_body = NULL;

genhash_t* stats_hash;

enum test_result {
    SUCCESS = 11,
    FAIL    = 13,
    DIED    = 14,
    CORE    = 15,
    PENDING = 19
};

struct test {
    const char *name;
    enum test_result (*tfun)(ENGINE_HANDLE *, ENGINE_HANDLE_V1 *);
    const char *cfg;
};

struct connstruct {
    uint64_t magic;
    const char *uname;
    const char *config;
    void *engine_data;
    bool connected;
    struct connstruct *next;
};

struct engine_event_handler {
    EVENT_CALLBACK cb;
    const void *cb_data;
    struct engine_event_handler *next;
};

static struct connstruct *connstructs;

static cb_mutex_t connstructs_mutex;

static struct engine_event_handler *engine_event_handlers[MAX_ENGINE_EVENT_TYPE + 1];

static void perform_callbacks(ENGINE_EVENT_TYPE type,
                              const void *data,
                              const void *cookie) {
    struct connstruct *c = (struct connstruct*) cookie;
    struct engine_event_handler *h;
    for (h = engine_event_handlers[type]; h; h = h->next) {
        h->cb(c, type, data, h->cb_data);
    }
}

static const char* get_server_version(void) {
    return "bucket mock";
}

static void get_auth_data(const void *cookie, auth_data_t *data) {
    struct connstruct *c = (struct connstruct *)cookie;
    if (c != NULL) {
        data->username = c->uname;
        data->config = c->config;
    }
}

static void mock_connect(struct connstruct *c) {
    cb_mutex_enter(&connstructs_mutex);
    c->connected = true;
    cb_mutex_exit(&connstructs_mutex);

    perform_callbacks(ON_CONNECT, NULL, c);
    if (c->uname) {
        auth_data_t ad;
        get_auth_data(c, &ad);
        perform_callbacks(ON_AUTH, (const void*)&ad, c);
    }
}

static void mock_disconnect(struct connstruct *c) {
    bool old_value;
    cb_mutex_enter(&connstructs_mutex);
    if ((old_value = c->connected)) {
        c->connected = false;
    }
    cb_mutex_exit(&connstructs_mutex);
    if (old_value) {
        perform_callbacks(ON_DISCONNECT, NULL, c);
    }
}

static struct connstruct *mk_conn(const char *user, const char *config) {
    struct connstruct *rv = calloc(sizeof(struct connstruct), 1);
    assert(rv);
    rv->magic = CONN_MAGIC;
    rv->uname = user ? strdup(user) : NULL;
    rv->config = config ? strdup(config) : NULL;
    rv->connected = false;
    cb_mutex_enter(&connstructs_mutex);
    rv->next = connstructs;
    connstructs = rv;
    cb_mutex_exit(&connstructs_mutex);
    mock_connect(rv);
    return rv;
}

static void register_callback(ENGINE_HANDLE *eh,
                              ENGINE_EVENT_TYPE type,
                              EVENT_CALLBACK cb,
                              const void *cb_data) {
    struct engine_event_handler *h =
        calloc(sizeof(struct engine_event_handler), 1);
    assert(h);
    h->cb = cb;
    h->cb_data = cb_data;
    h->next = engine_event_handlers[type];
    engine_event_handlers[type] = h;
    (void)eh;
}

static void store_engine_specific(const void *cookie,
                                  void *engine_data) {
    if (cookie) {
        struct connstruct *c = (struct connstruct *)cookie;
        assert(c->magic == CONN_MAGIC);
        c->engine_data = engine_data;
    }
}

static void *get_engine_specific(const void *cookie) {
    struct connstruct *c = (struct connstruct *)cookie;
    assert(c == NULL || c->magic == CONN_MAGIC);
    return c ? c->engine_data : NULL;
}

static ENGINE_ERROR_CODE reserve_cookie(const void *cookie)
{
    (void)cookie;
    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE release_cookie(const void *cookie)
{
    (void)cookie;
    return ENGINE_SUCCESS;
}

static void *create_stats(void) {
    /* XXX: Not sure if ``big buffer'' is right in faking this part of
       the server. */
    void *s = calloc(1, 256);
    assert(s);
    return s;
}

static void destroy_stats(void *s) {
    assert(s);
    free(s);
}

static void logger_log(EXTENSION_LOG_LEVEL severity,
                       const void* client_cookie,
                       const char *fmt, ...)
{
    (void)severity;
    (void)client_cookie;
    (void)fmt;
}

static const char *logger_get_name(void) {
    return "blackhole logger";
}

static EXTENSION_LOGGER_DESCRIPTOR blackhole_logger_descriptor;

static void *get_extension(extension_type_t type) {
    void *ret = NULL;
    if (type == EXTENSION_LOGGER) {
        blackhole_logger_descriptor.get_name = logger_get_name;
        blackhole_logger_descriptor.log = logger_log;
        ret = &blackhole_logger_descriptor;
    }
    return ret;
}

static rel_time_t get_current_time(void) {
    return (rel_time_t)time(NULL);
}

/**
 * Callback the engines may call to get the public server interface
 * @param interface the requested interface from the server
 * @return pointer to a structure containing the interface. The client should
 *         know the layout and perform the proper casts.
 */
static SERVER_HANDLE_V1 *get_server_api(void)
{
    static SERVER_CORE_API core_api;
    static SERVER_COOKIE_API cookie_api;
    static SERVER_STAT_API server_stat_api;
    static SERVER_EXTENSION_API extension_api;
    static SERVER_CALLBACK_API callback_api;
    static SERVER_HANDLE_V1 rv;

    core_api.server_version = get_server_version;
    core_api.get_current_time = get_current_time;
    core_api.parse_config = parse_config;

    cookie_api.get_auth_data = get_auth_data;
    cookie_api.store_engine_specific = store_engine_specific;
    cookie_api.get_engine_specific = get_engine_specific;
    cookie_api.notify_io_complete = notify_io_complete;
    cookie_api.reserve = reserve_cookie;
    cookie_api.release = release_cookie;

    server_stat_api.new_stats = create_stats;
    server_stat_api.release_stats = destroy_stats;

    extension_api.get_extension = get_extension;

    callback_api.register_callback = register_callback;
    callback_api.perform_callbacks = perform_callbacks;

    rv.interface = 1;
    rv.core = &core_api;
    rv.stat = &server_stat_api;
    rv.extension = &extension_api;
    rv.callback = &callback_api;
    rv.cookie = &cookie_api;

    return &rv;
}

static bool add_response(const void *key, uint16_t keylen,
                         const void *ext, uint8_t extlen,
                         const void *body, uint32_t bodylen,
                         uint8_t datatype, uint16_t status,
                         uint64_t cas, const void *cookie) {
    (void)ext;
    (void)extlen;
    (void)datatype;
    (void)cas;
    (void)cookie;
    last_status = status;
    if (last_body) {
        free(last_body);
        last_body = NULL;
    }
    if (bodylen > 0) {
        last_body = malloc(bodylen);
        assert(last_body);
        memcpy(last_body, body, bodylen);
    }
    if (last_key) {
        free(last_key);
        last_key = NULL;
    }
    if (keylen > 0) {
        last_key = malloc(keylen);
        assert(last_key);
        memcpy(last_key, key, keylen);
    }
    return true;
}

static ENGINE_HANDLE *load_engine(const char *soname, const char *config_str) {
    ENGINE_ERROR_CODE error;
    ENGINE_HANDLE *engine = NULL;
    /* Hack to remove the warning from C99 */
    union my_hack {
        CREATE_INSTANCE create;
        void* voidptr;
    } my_create;
    void *symbol;
    char *errmsg;
    cb_dlhandle_t handle = cb_dlopen(soname, &errmsg);
    if (handle == NULL) {
        fprintf(stderr, "Failed to open library \"%s\": %s\n",
                soname ? soname : "self", errmsg);
        free(errmsg);
        return NULL;
    }

    symbol = cb_dlsym(handle, "create_instance", &errmsg);
    if (symbol == NULL) {
        fprintf(stderr,
                "Could not find symbol \"create_instance\" in %s: %s\n",
                soname ? soname : "self", errmsg);
        free(errmsg);
        return NULL;
    }
    my_create.voidptr = symbol;

    /* request a instance with protocol version 1 */
    error = (*my_create.create)(1, get_server_api, &engine);

    if (error != ENGINE_SUCCESS || engine == NULL) {
        fprintf(stderr, "Failed to create instance. Error code: %d\n", error);
        cb_dlclose(handle);
        return NULL;
    }

    if (engine->interface == 1) {
        ENGINE_HANDLE_V1 *v1 = (ENGINE_HANDLE_V1*)engine;
        if (v1->initialize(engine, config_str) != ENGINE_SUCCESS) {
            v1->destroy(engine, false);
            fprintf(stderr, "Failed to initialize instance. Error code: %d\n",
                    error);
            cb_dlclose(handle);
            return NULL;
        }
    } else {
        fprintf(stderr, "Unsupported interface level\n");
        cb_dlclose(handle);
        return NULL;
    }

    return engine;
}

/* ---------------------------------------------------------------------- */
/* The actual test stuff... */
/* ---------------------------------------------------------------------- */

static bool item_eq(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                    const void *c1, item *item1,
                    const void *c2, item *item2) {
    item_info i1;
    item_info i2;
    i1.nvalue = 1;
    i2.nvalue = 1;

    if (!h1->get_item_info(h, c1, item1, &i1) ||
        !h1->get_item_info(h, c2, item2, &i2))
        return false;

    return i1.exptime == i2.exptime
        && i1.flags == i2.flags
        && i1.nkey == i2.nkey
        && i1.nbytes == i2.nbytes
        && i1.nvalue == i2.nvalue
        && memcmp(i1.key, i2.key, i1.nkey) == 0
        && memcmp(i1.value[0].iov_base, i2.value[0].iov_base,
                  i1.nbytes) == 0;
}

static void assert_item_eq(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                           const void *c1, item *i1,
                           const void *c2, item *i2) {
    assert(item_eq(h, h1, c1, i1, c2, i2));
}

/* Convenient storage abstraction */
static void store(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                  const void *cookie,
                  const char *key, const char *value,
                  item **outitem) {

    item *itm = NULL;
    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;
    item_info info;
    info.nvalue = 1;

    rv = h1->allocate(h, cookie, &itm,
                      key, strlen(key),
                      strlen(value), 9258, 3600);
    assert(rv == ENGINE_SUCCESS);

    assert(h1->get_item_info(h, cookie, itm, &info));
    assert(info.nvalue == 1);
    assert(info.value[0].iov_base);
    assert(value);

    memcpy((char*)info.value[0].iov_base, value, strlen(value));

    rv = h1->store(h, cookie, itm, 0, OPERATION_SET, 0);
    assert(rv == ENGINE_SUCCESS);

    if (outitem) {
        *outitem = itm;
    }
}

static enum test_result test_default_storage(ENGINE_HANDLE *h,
                                             ENGINE_HANDLE_V1 *h1) {
    item *itm = NULL, *fetched_item;
    const void *cookie = mk_conn(NULL, NULL);
    char *key = "somekey";
    char *value = "some value";
    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;
    item_info info;
    info.nvalue = 1;

    rv = h1->allocate(h, cookie, &itm,
                      key, strlen(key),
                      strlen(value), 9258, 3600);
    assert(rv == ENGINE_SUCCESS);

    assert(h1->get_item_info(h, cookie, itm, &info));

    memcpy((char*)info.value[0].iov_base, value, strlen(value));

    rv = h1->store(h, cookie, itm, 0, OPERATION_SET, 0);
    assert(rv == ENGINE_SUCCESS);

    rv = h1->get(h, cookie, &fetched_item, key, strlen(key), 0);
    assert(rv == ENGINE_SUCCESS);

    assert_item_eq(h, h1, cookie, itm, cookie, fetched_item);

    /* no effect, but increases coverage. */
    h1->reset_stats(h, cookie);

    return SUCCESS;
}

static enum test_result test_default_storage_key_overrun(ENGINE_HANDLE *h,
                                                         ENGINE_HANDLE_V1 *h1) {
    item *itm = NULL, *fetched_item;
    const void *cookie = mk_conn(NULL, NULL);
    char *key = "somekeyx";
    char *value = "some value";
    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;
    item_info info;
    info.nvalue = 1;

    rv = h1->allocate(h, cookie, &itm,
                      key, strlen(key)-1,
                      strlen(value), 9258, 3600);
    assert(rv == ENGINE_SUCCESS);

    h1->get_item_info(h, cookie, itm, &info);

    memcpy((char*)info.value[0].iov_base, value, strlen(value));

    rv = h1->store(h, cookie, itm, 0, OPERATION_SET, 0);
    assert(rv == ENGINE_SUCCESS);

    rv = h1->get(h, cookie, &fetched_item, "somekey", strlen("somekey"), 0);
    assert(rv == ENGINE_SUCCESS);

    assert_item_eq(h, h1, cookie, itm, cookie, fetched_item);

    h1->get_item_info(h, cookie, fetched_item, &info);

    rv = h1->remove(h, cookie, info.key, info.nkey, &info.cas, 0);
    assert(rv == ENGINE_SUCCESS);

    return SUCCESS;
}

static enum test_result test_default_unlinked_remove(ENGINE_HANDLE *h,
                                                     ENGINE_HANDLE_V1 *h1) {
    item *itm = NULL;
    const void *cookie = mk_conn(NULL, NULL);
    char *key = "somekeyx";
    const char *value = "the value";
    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;
    uint64_t cas = 0;

    rv = h1->allocate(h, cookie, &itm,
                      key, strlen(key)-1,
                      strlen(value), 9258, 3600);
    assert(rv == ENGINE_SUCCESS);
    rv = h1->remove(h, cookie, key, strlen(key), &cas, 0);
    assert(rv == ENGINE_KEY_ENOENT);

    return SUCCESS;
}

static enum test_result test_two_engines_no_autocreate(ENGINE_HANDLE *h,
                                                       ENGINE_HANDLE_V1 *h1) {
    item *itm = NULL, *fetched_item;
    const void *cookie = mk_conn("autouser", NULL);
    char *key = "somekey";
    char *value = "some value";
    uint64_t cas_out = 0, result = 0;
    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;
    uint64_t cas = 0;

    rv = h1->allocate(h, cookie, &itm,
                      key, strlen(key),
                      strlen(value), 9258, 3600);
    assert(rv == ENGINE_DISCONNECT);

    rv = h1->store(h, cookie, itm, 0, OPERATION_SET, 0);
    assert(rv == ENGINE_DISCONNECT);

    rv = h1->get(h, cookie, &fetched_item, key, strlen(key), 0);
    assert(rv == ENGINE_DISCONNECT);

    rv = h1->remove(h, cookie, key, strlen(key), &cas, 0);
    assert(rv == ENGINE_DISCONNECT);

    rv = h1->arithmetic(h, cookie, key, strlen(key),
                        true, true, 1, 1, 0, &cas_out, &result, 0);
    assert(rv == ENGINE_DISCONNECT);

    /* no effect, but increases coverage. */
    h1->reset_stats(h, cookie);

    return SUCCESS;
}

static enum test_result test_no_default_storage(ENGINE_HANDLE *h,
                                                ENGINE_HANDLE_V1 *h1) {
    item *itm = NULL, *fetched_item;
    const void *cookie = mk_conn(NULL, NULL);
    char *key = "somekey";
    char *value = "some value";
    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;

    rv = h1->allocate(h, cookie, &itm,
                      key, strlen(key),
                      strlen(value), 9258, 3600);
    assert(rv == ENGINE_DISCONNECT);

    rv = h1->get(h, cookie, &fetched_item, key, strlen(key), 0);
    assert(rv == ENGINE_DISCONNECT);

    return SUCCESS;
}

static enum test_result test_two_engines(ENGINE_HANDLE *h,
                                         ENGINE_HANDLE_V1 *h1) {
    item *item1, *item2, *fetched_item1 = NULL, *fetched_item2 = NULL;
    const void *cookie1 = mk_conn("user1", NULL), *cookie2 = mk_conn("user2", NULL);
    char *key = "somekey";
    char *value1 = "some value1", *value2 = "some value 2";
    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;

    store(h, h1, cookie1, key, value1, &item1);
    store(h, h1, cookie2, key, value2, &item2);

    rv = h1->get(h, cookie1, &fetched_item1, key, strlen(key), 0);
    assert(rv == ENGINE_SUCCESS);
    rv = h1->get(h, cookie2, &fetched_item2, key, strlen(key), 0);
    assert(rv == ENGINE_SUCCESS);

    assert(!item_eq(h, h1, cookie1, fetched_item1, cookie2, fetched_item2));
    assert_item_eq(h, h1, cookie1, item1, cookie1, fetched_item1);
    assert_item_eq(h, h1, cookie2, item2, cookie2, fetched_item2);

    return SUCCESS;
}

static enum test_result test_two_engines_del(ENGINE_HANDLE *h,
                                             ENGINE_HANDLE_V1 *h1) {
    item *item1, *item2, *fetched_item1 = NULL, *fetched_item2 = NULL;
    const void *cookie1 = mk_conn("user1", NULL), *cookie2 = mk_conn("user2", NULL);
    char *key = "somekey";
    char *value1 = "some value1", *value2 = "some value 2";
    ENGINE_ERROR_CODE rv;
    uint64_t cas = 0;

    store(h, h1, cookie1, key, value1, &item1);
    store(h, h1, cookie2, key, value2, &item2);

    /* Delete an item */
    rv = h1->remove(h, cookie1, key, strlen(key), &cas, 0);
    assert(rv == ENGINE_SUCCESS);

    rv = h1->get(h, cookie1, &fetched_item1, key, strlen(key), 0);
    assert(rv == ENGINE_KEY_ENOENT);
    assert(fetched_item1 == NULL);
    rv = h1->get(h, cookie2, &fetched_item2, key, strlen(key), 0);
    assert(rv == ENGINE_SUCCESS);

    assert_item_eq(h, h1, cookie1, item2, cookie2, fetched_item2);

    return SUCCESS;
}

static enum test_result test_two_engines_flush(ENGINE_HANDLE *h,
                                               ENGINE_HANDLE_V1 *h1) {
    item *item1, *item2, *fetched_item1 = NULL, *fetched_item2 = NULL;
    const void *cookie1 = mk_conn("user1", NULL), *cookie2 = mk_conn("user2", NULL);
    char *key = "somekey";
    char *value1 = "some value1", *value2 = "some value 2";
    ENGINE_ERROR_CODE rv;

    store(h, h1, cookie1, key, value1, &item1);
    store(h, h1, cookie2, key, value2, &item2);

    /* flush it */
    rv = h1->flush(h, cookie1, 0);
    assert(rv == ENGINE_SUCCESS);

    rv = h1->get(h, cookie1, &fetched_item1, key, strlen(key), 0);
    assert(rv == ENGINE_KEY_ENOENT);
    assert(fetched_item1 == NULL);
    rv = h1->get(h, cookie2, &fetched_item2, key, strlen(key), 0);
    assert(rv == ENGINE_SUCCESS);

    assert_item_eq(h, h1, cookie2, item2, cookie2, fetched_item2);

    return SUCCESS;
}

static enum test_result test_arith(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const void *cookie1 = mk_conn("user1", NULL), *cookie2 = mk_conn("user2", NULL);
    char *key = "somekey";
    uint64_t result = 0, cas = 0;
    ENGINE_ERROR_CODE rv;

    /* Initialize the first one. */
    rv = h1->arithmetic(h, cookie1, key, strlen(key),
                        true, true, 1, 1, 0, &cas, &result, 0);
    assert(rv == ENGINE_SUCCESS);
    assert(cas == 0);
    assert(result == 1);

    /* Fail an init of the second one. */
    rv = h1->arithmetic(h, cookie2, key, strlen(key),
                        true, false, 1, 1, 0, &cas, &result, 0);
    assert(rv == ENGINE_KEY_ENOENT);

    /* Update the first again. */
    rv = h1->arithmetic(h, cookie1, key, strlen(key),
                        true, true, 1, 1, 0, &cas, &result, 0);
    assert(rv == ENGINE_SUCCESS);
    assert(cas == 0);
    assert(result == 2);

    return SUCCESS;
}

static enum test_result test_get_info(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    const engine_info* info = h1->get_info(h);
    return strncmp(info->description, "Bucket engine", 13) == 0 ? SUCCESS : FAIL;
}

static void* create_packet4(uint8_t opcode, const char *key, const char *val,
                            size_t vlen) {
    void *pkt_raw = calloc(1,
                           sizeof(protocol_binary_request_header)
                           + strlen(key)
                           + vlen);
    protocol_binary_request_header *req =
        (protocol_binary_request_header*)pkt_raw;
    assert(pkt_raw);
    req->request.opcode = opcode;
    req->request.bodylen = htonl(strlen(key) + vlen);
    req->request.keylen = htons(strlen(key));
    memcpy((char*)pkt_raw + sizeof(protocol_binary_request_header),
           key, strlen(key));
    memcpy((char*)pkt_raw + sizeof(protocol_binary_request_header) + strlen(key),
           val, vlen);
    return pkt_raw;
}

static void* create_packet(uint8_t opcode, const char *key, const char *val) {
    return create_packet4(opcode, key, val, strlen(val));
}

static void* create_create_bucket_pkt(const char *user, const char *path,
                                       const char *args) {
    char buf[1024];
    snprintf(buf, sizeof(buf), "%s%c%s", path, 0, args);
    return create_packet4(CREATE_BUCKET, user,
                          buf, strlen(path) + strlen(args) + 1);
}

static enum test_result test_create_bucket(ENGINE_HANDLE *h,
                                           ENGINE_HANDLE_V1 *h1) {
    const void *adm_cookie = mk_conn("admin", NULL);
    const char *key = "somekey";
    const char *value = "the value";
    item *itm;
    void *pkt;
    ENGINE_ERROR_CODE rv;

    rv = h1->allocate(h, mk_conn("someuser", NULL), &itm,
                      key, strlen(key),
                      strlen(value), 9258, 3600);
    assert(rv == ENGINE_DISCONNECT);

    pkt = create_create_bucket_pkt("someuser", ENGINE_PATH, "");
    rv = h1->unknown_command(h, adm_cookie, pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_SUCCESS);
    assert(last_status == 0);

    rv = h1->allocate(h, mk_conn("someuser", NULL), &itm,
                      key, strlen(key),
                      strlen(value), 9258, 3600);
    assert(rv == ENGINE_SUCCESS);

    return SUCCESS;
}

static enum test_result test_double_create_bucket(ENGINE_HANDLE *h,
                                                  ENGINE_HANDLE_V1 *h1) {
    const void *adm_cookie = mk_conn("admin", NULL);
    ENGINE_ERROR_CODE rv;
    void *pkt = create_create_bucket_pkt("someuser", ENGINE_PATH, "");
    rv = h1->unknown_command(h, adm_cookie, pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_SUCCESS);
    assert(last_status == 0);

    pkt = create_create_bucket_pkt("someuser", ENGINE_PATH, "");
    rv = h1->unknown_command(h, adm_cookie, pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_SUCCESS);
    assert(last_status == PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS);

    return SUCCESS;
}

static enum test_result test_create_bucket_with_params(ENGINE_HANDLE *h,
                                                       ENGINE_HANDLE_V1 *h1) {
    const void *adm_cookie = mk_conn("admin", NULL), *other_cookie = mk_conn("someuser", NULL);
    const char *key = "somekey";
    const char *value = "the value";
    item *itm;
    ENGINE_ERROR_CODE rv;
    void *pkt;

    rv = h1->allocate(h, adm_cookie, &itm,
                      key, strlen(key),
                      strlen(value), 9258, 3600);
    assert(rv == ENGINE_DISCONNECT);

    pkt = create_create_bucket_pkt("someuser", ENGINE_PATH, "no_alloc");
    rv = h1->unknown_command(h, adm_cookie, pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_SUCCESS);
    assert(last_status == 0);

    rv = h1->allocate(h, other_cookie, &itm,
                      key, strlen(key),
                      strlen(value), 9258, 3600);
    assert(rv == ENGINE_DISCONNECT);

    return SUCCESS;
}

static enum test_result test_admin_user(ENGINE_HANDLE *h,
                                        ENGINE_HANDLE_V1 *h1) {
    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;

    /* Test with no user. */
    void *pkt = create_create_bucket_pkt("newbucket", ENGINE_PATH, "");
    rv = h1->unknown_command(h, mk_conn(NULL, NULL), pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_ENOTSUP);

    /* Test with non-admin */
    pkt = create_create_bucket_pkt("newbucket", ENGINE_PATH, "");
    rv = h1->unknown_command(h, mk_conn("notadmin", NULL), pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_ENOTSUP);

    /* Test with admin */
    pkt = create_create_bucket_pkt("newbucket", ENGINE_PATH, "");
    rv = h1->unknown_command(h, mk_conn("admin", NULL), pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_SUCCESS);
    assert(last_status == 0);

    return SUCCESS;
}

static enum test_result do_test_delete_bucket(ENGINE_HANDLE *h,
                                              ENGINE_HANDLE_V1 *h1,
                                              bool delete_on_same_connection) {
    const void *adm_cookie = mk_conn("admin", NULL);
    const char *key = "somekey";
    const char *value = "the value";
    item *itm;
    ENGINE_ERROR_CODE rv;
    const void *other_cookie;
    void *pkt = create_create_bucket_pkt("someuser", ENGINE_PATH, "");
    rv = h1->unknown_command(h, adm_cookie, pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_SUCCESS);
    assert(last_status == 0);

    if (delete_on_same_connection) {
        pkt = create_packet(SELECT_BUCKET, "someuser", "");
        rv = h1->unknown_command(h, adm_cookie, pkt, add_response);
        free(pkt);
        assert(rv == ENGINE_SUCCESS);
        assert(last_status == 0);
    }

    other_cookie = mk_conn("someuser", NULL);
    rv = h1->allocate(h, other_cookie, &itm,
                      key, strlen(key),
                      strlen(value), 9258, 3600);
    assert(rv == ENGINE_SUCCESS);

    pkt = create_packet(DELETE_BUCKET, "someuser", "force=false");
    cb_mutex_enter(&notify_mutex);
    notify_code = ENGINE_FAILED;
    rv = h1->unknown_command(h, adm_cookie, pkt, add_response);
    assert(rv == ENGINE_EWOULDBLOCK);
    cb_cond_wait(&notify_cond, &notify_mutex);
    assert(notify_code == ENGINE_SUCCESS);
    rv = h1->unknown_command(h, adm_cookie, pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_SUCCESS);

    pkt = create_packet(DELETE_BUCKET, "someuser", "force=false");
    rv = h1->unknown_command(h, adm_cookie, pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_SUCCESS);
    assert(last_status == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);

    rv = h1->allocate(h, other_cookie, &itm,
                      key, strlen(key),
                      strlen(value), 9258, 3600);
    assert(rv == ENGINE_DISCONNECT);

    return SUCCESS;
}

static enum test_result test_delete_bucket(ENGINE_HANDLE *h,
                                           ENGINE_HANDLE_V1 *h1) {
    return do_test_delete_bucket(h, h1, false);
}

static enum test_result test_delete_bucket_sameconnection(ENGINE_HANDLE *h,
                                                          ENGINE_HANDLE_V1 *h1) {
    return do_test_delete_bucket(h, h1, true);
}

struct handle_pair {
    ENGINE_HANDLE *h;
    ENGINE_HANDLE_V1 *h1;
};

static void conc_del_bucket_thread(void *arg) {
    struct handle_pair *hp = arg;

    bool have_connection = false;
    void *cokie = NULL;
    while (true) {
        static const char *key = "somekey";
        static size_t klen = 7;
        static size_t vlen = 9;
        item *itm;
        ENGINE_ERROR_CODE rv;

        cokie = have_connection ? cokie : mk_conn("someuser", NULL);
        have_connection = true;

        rv = hp->h1->allocate(hp->h, cokie, &itm,
                              key, klen,
                              vlen, 9258, 3600);
        if (rv == ENGINE_DISCONNECT) {
            break;
        }

        assert(rv == ENGINE_SUCCESS);

        hp->h1->release(hp->h, cokie, itm);

        if (rand() % 3 == 0) {
            have_connection = false;
            mock_disconnect(cokie);
        }
    }
    mock_disconnect(cokie);
}

static enum test_result test_release(ENGINE_HANDLE *h,
                                     ENGINE_HANDLE_V1 *h1) {
    void *cokie = mk_conn("someuser", NULL);
    item *itm;
    const void *key = "release_me";
    const size_t klen = strlen(key);
    const size_t vlen = 81985;
    ENGINE_ERROR_CODE rv = h1->allocate(h, cokie, &itm,
                                        key, klen,
                                        vlen, 9258, 3600);
    assert(rv == ENGINE_SUCCESS);
    h1->release(h, cokie, itm);

    return SUCCESS;
}

static int getenv_int_with_default(const char *env_var, int default_value) {
    char *val = getenv(env_var);
    char *ptr;
    long lrv;

    if (!val) {
        return default_value;
    }
    lrv = (int)strtol(val, &ptr, 10);
    if (*val && !*ptr) {
        int rv = (int)lrv;
        if ((long)lrv == lrv) {
            return rv;
        }
    }
    return default_value;
}

static enum test_result do_test_delete_bucket_concurrent(ENGINE_HANDLE *h,
                                                         ENGINE_HANDLE_V1 *h1,
                                                         bool keep_one_refcount)
{
    struct bucket_engine *bucket_engine = (struct bucket_engine *)h;
    const void *adm_cookie = mk_conn("admin", NULL);
    proxied_engine_handle_t *peh;
    int n_threads;
    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;
    struct handle_pair hp;
    int i;
    void *other_cookie = mk_conn("someuser", NULL);
    item *itm;
    const char* key = "testkey";
    const char* value = "testvalue";
    cb_thread_t *threads;

    void *pkt = create_create_bucket_pkt("someuser", ENGINE_PATH, "");
    rv = h1->unknown_command(h, adm_cookie, pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_SUCCESS);
    assert(last_status == 0);

    peh = genhash_find(bucket_engine->engines, "someuser", strlen("someuser"));
    assert(peh);

    assert(peh->refcount == 1);
    if (keep_one_refcount) {
        peh->refcount++;
    }

    n_threads = getenv_int_with_default("DELETE_BUCKET_CONCURRENT_THREADS", 17);
    if (n_threads < 1) {
        n_threads = 1;
    }
    threads = calloc(n_threads, sizeof(*threads));
    assert(threads != NULL);
    hp.h = h;
    hp.h1 = h1;

    for (i = 0; i < n_threads; i++) {
        int r = cb_create_thread(&threads[i], conc_del_bucket_thread, &hp, 0);
        assert(r == 0);
    }

    delay();

    pkt = create_packet(DELETE_BUCKET, "someuser", "force=false");

    cb_mutex_enter(&notify_mutex);
    notify_code = ENGINE_FAILED;
    rv = h1->unknown_command(h, adm_cookie, pkt, add_response);
    assert(rv == ENGINE_EWOULDBLOCK);
    cb_cond_wait(&notify_cond, &notify_mutex);
    assert(notify_code == ENGINE_SUCCESS);
    rv = h1->unknown_command(h, adm_cookie, pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_SUCCESS);

    rv = h1->allocate(h, other_cookie, &itm,
                      key, strlen(key),
                      strlen(value), 9258, 3600);
    assert(rv == ENGINE_DISCONNECT);
    mock_disconnect(other_cookie);
    for (i = 0; i < n_threads; i++) {
        int r = cb_join_thread(threads[i]);
        assert(r == 0);
    }

    if (keep_one_refcount) {
        assert(peh->refcount == 1);
        assert(peh->state == STATE_NULL);
        assert(bucket_engine->shutdown.bucket_counter == 1);
    }

    cb_mutex_enter(&bucket_engine->shutdown.mutex);
    if (keep_one_refcount) {
        assert(peh->refcount == 1);
        peh->refcount = 0;
        assert(bucket_engine->shutdown.bucket_counter == 1);
        cb_cond_broadcast(&bucket_engine->shutdown.refcount_cond);
    }
    /* we cannot use shutdown.cond because it'll only be signalled
     * when in_progress is set, but we don't want to set in_progress
     * to avoid aborting normal "refcount drops to 0" loop. */
    while (bucket_engine->shutdown.bucket_counter == 1) {
        cb_mutex_exit(&bucket_engine->shutdown.mutex);
        delay();
        cb_mutex_enter(&bucket_engine->shutdown.mutex);
    }
    assert(bucket_engine->shutdown.bucket_counter == 0);
    cb_mutex_exit(&bucket_engine->shutdown.mutex);

    cb_mutex_initialize(&notify_mutex);
    free(threads);

    return SUCCESS;
}


static enum test_result test_delete_bucket_concurrent(ENGINE_HANDLE *h,
                                                      ENGINE_HANDLE_V1 *h1) {
    return do_test_delete_bucket_concurrent(h, h1, true);
}

static enum test_result test_delete_bucket_concurrent_multi(ENGINE_HANDLE *h,
                                                            ENGINE_HANDLE_V1 *h1) {
    enum test_result rv = SUCCESS;
    int i = getenv_int_with_default("DELETE_BUCKET_CONCURRENT_ITERATIONS", 100);
    if (i < 1) {
        i = 1;
    }
    while (--i >= 0) {
        rv = do_test_delete_bucket_concurrent(h, h1, i & 1);
        if (rv != SUCCESS) {
            break;
        }
    }
    return rv;
}

static enum test_result test_delete_bucket_shutdown_race(ENGINE_HANDLE *h,
                                                         ENGINE_HANDLE_V1 *h1)
{
    const void *adm_cookie = mk_conn("admin", NULL);
    ENGINE_ERROR_CODE rv;
    const void *cookie1;
    item *item1;
    const char *key = "somekey";
    const char *value1 = "some value1";
    void *pkt;

    pkt = create_create_bucket_pkt("mybucket", ENGINE_PATH, "");
    rv = h1->unknown_command(h, adm_cookie, pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_SUCCESS);
    assert(last_status == 0);

    cookie1 = mk_conn("mybucket", NULL);
    store(h, h1, cookie1, key, value1, &item1);

    pkt = create_packet(DELETE_BUCKET, "mybucket", "force=false");
    cb_mutex_enter(&notify_mutex);
    notify_code = ENGINE_FAILED;

    rv = h1->unknown_command(h, adm_cookie, pkt, add_response);
    assert(rv == ENGINE_EWOULDBLOCK);
    cb_cond_wait(&notify_cond, &notify_mutex);
    assert(notify_code == ENGINE_SUCCESS);

    /* we've got one ref-count open for the bucket, so we should have */
    /* a deadlock if we try to shut down the bucket now... */
    /* There is actually a bug in bucket_engine that allows us to */
    /* call destroy twice without any side effects (it doesn't free */
    /* the memory allocated for the engine handle).. */
    /* We do however need to clear out the connstructs list */
    /* to avoid having on_disconnect handling to be sent */
    /* to the free'd engine (it is called by the framework before */
    /* it runs destroy, but we've performed the destroy) */
    h1->destroy(h, false);
    connstructs = NULL;

    return SUCCESS;
}

static enum test_result test_bucket_name_validation(ENGINE_HANDLE *h,
                                                    ENGINE_HANDLE_V1 *h1) {

    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;
    void *pkt = create_create_bucket_pkt("bucket one", ENGINE_PATH, "");
    rv = h1->unknown_command(h, mk_conn("admin", NULL), pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_SUCCESS);
    assert(last_status == PROTOCOL_BINARY_RESPONSE_NOT_STORED);

    pkt = create_create_bucket_pkt("", ENGINE_PATH, "");
    rv = h1->unknown_command(h, mk_conn("admin", NULL), pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_SUCCESS);
    assert(last_status == PROTOCOL_BINARY_RESPONSE_NOT_STORED);

    return SUCCESS;
}

static enum test_result test_list_buckets_none(ENGINE_HANDLE *h,
                                               ENGINE_HANDLE_V1 *h1) {
    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;
    /* Go find all the buckets. */
    void *pkt = create_packet(LIST_BUCKETS, "", "");
    rv = h1->unknown_command(h, mk_conn("admin", NULL), pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_SUCCESS);
    assert(last_status == 0);

    /* Now verify the body looks alright. */
    assert(last_body == NULL);

    return SUCCESS;
}

static enum test_result test_list_buckets_one(ENGINE_HANDLE *h,
                                              ENGINE_HANDLE_V1 *h1) {

    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;

    /* Create a bucket first. */
    void *pkt = create_create_bucket_pkt("bucket1", ENGINE_PATH, "");
    rv = h1->unknown_command(h, mk_conn("admin", NULL), pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_SUCCESS);
    assert(last_status == 0);

    /* Now go find all the buckets. */
    pkt = create_packet(LIST_BUCKETS, "", "");
    rv = h1->unknown_command(h, mk_conn("admin", NULL), pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_SUCCESS);
    assert(last_status == 0);

    /* Now verify the body looks alright. */
    assert(strncmp(last_body, "bucket1", 7) == 0);

    return SUCCESS;
}

static enum test_result test_list_buckets_two(ENGINE_HANDLE *h,
                                              ENGINE_HANDLE_V1 *h1) {
    const void *cookie = mk_conn("admin", NULL);
    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;
    /* Create two buckets first. */
    void *pkt = create_create_bucket_pkt("bucket1", ENGINE_PATH, "");
    rv = h1->unknown_command(h, cookie, pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_SUCCESS);
    assert(last_status == 0);

    pkt = create_create_bucket_pkt("bucket2", ENGINE_PATH, "");
    rv = h1->unknown_command(h, cookie, pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_SUCCESS);
    assert(last_status == 0);

    /* Now go find all the buckets. */
    pkt = create_packet(LIST_BUCKETS, "", "");
    rv = h1->unknown_command(h, cookie, pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_SUCCESS);
    assert(last_status == 0);

    /* Now verify the body looks alright. */
    assert(memcmp(last_body, "bucket1 bucket2", 15) == 0
           || memcmp(last_body, "bucket2 bucket1", 15) == 0);

    return SUCCESS;
}

static enum test_result test_unknown_call(ENGINE_HANDLE *h,
                                          ENGINE_HANDLE_V1 *h1) {
    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;
    void *pkt = create_create_bucket_pkt("someuser", ENGINE_PATH, "");
    rv = h1->unknown_command(h, mk_conn("admin", NULL), pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_SUCCESS);
    assert(last_status == 0);

    pkt = create_packet(0xfe, "somekey", "someval");
    rv = h1->unknown_command(h, mk_conn("someuser", NULL), pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_ENOTSUP);

    return SUCCESS;
}

static enum test_result test_select_no_admin(ENGINE_HANDLE *h,
                                             ENGINE_HANDLE_V1 *h1) {
    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;
    void *pkt = create_create_bucket_pkt("someuser", ENGINE_PATH, "");
    rv = h1->unknown_command(h, mk_conn("admin", NULL), pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_SUCCESS);
    assert(last_status == 0);

    pkt = create_packet(SELECT_BUCKET, "stuff", "");
    rv = h1->unknown_command(h, mk_conn("notadmin", NULL), pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_ENOTSUP);

    return SUCCESS;
}

static enum test_result test_select_no_bucket(ENGINE_HANDLE *h,
                                              ENGINE_HANDLE_V1 *h1) {
    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;
    void *pkt = create_packet(SELECT_BUCKET, "stuff", "");
    rv = h1->unknown_command(h, mk_conn("admin", NULL), pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_SUCCESS);
    assert(last_status == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT);

    return SUCCESS;
}

static enum test_result test_select(ENGINE_HANDLE *h,
                                    ENGINE_HANDLE_V1 *h1) {
    item *item1, *fetched_item1 = NULL, *fetched_item2;
    const void *cookie1 = mk_conn("user1", NULL), *admin = mk_conn("admin", NULL);
    char *key = "somekey";
    char *value1 = "some value1";
    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;
    void *pkt;

    store(h, h1, cookie1, key, value1, &item1);
    rv = h1->get(h, cookie1, &fetched_item1, key, strlen(key), 0);
    assert(rv == ENGINE_SUCCESS);
    rv = h1->get(h, admin, &fetched_item2, key, strlen(key), 0);
    assert(rv == ENGINE_KEY_ENOENT);

    assert_item_eq(h, h1, cookie1, item1, cookie1, fetched_item1);

    pkt = create_packet(SELECT_BUCKET, "user1", "");
    rv = h1->unknown_command(h, admin, pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_SUCCESS);
    assert(last_status == 0);

    rv = h1->get(h, admin, &fetched_item2, key, strlen(key), 0);
    assert(rv == ENGINE_SUCCESS);
    assert_item_eq(h, h1, cookie1, item1, admin, fetched_item2);

    return SUCCESS;
}

static void add_stats(const char *key, const uint16_t klen,
                      const char *val, const uint32_t vlen,
                      const void *cookie) {
    (void)cookie;
    genhash_update(stats_hash, key, klen, val, vlen);
}

static enum test_result test_stats(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;

    rv = h1->get_stats(h, mk_conn("user", NULL), NULL, 0, add_stats);
    assert(rv == ENGINE_SUCCESS);
    assert(genhash_size(stats_hash) == 2);

    assert(memcmp("0",
                  genhash_find(stats_hash, "bucket_conns", strlen("bucket_conns")),
                  1) == 0);
    assert(genhash_find(stats_hash, "bucket_active_conns",
                        strlen("bucket_active_conns")) != NULL);

    return SUCCESS;
}

static enum test_result test_stats_bucket(ENGINE_HANDLE *h,
                                          ENGINE_HANDLE_V1 *h1) {
    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;
    const void *adm_cookie = mk_conn("admin", NULL);

    void *pkt = create_create_bucket_pkt("someuser", ENGINE_PATH, "");
    rv = h1->unknown_command(h, adm_cookie, pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_SUCCESS);
    assert(last_status == 0);

    rv = h1->get_stats(h, mk_conn("user", NULL), "bucket", 6, add_stats);
    assert(rv == ENGINE_FAILED);
    assert(genhash_size(stats_hash) == 0);

    rv = h1->get_stats(h, adm_cookie, "bucket", 6, add_stats);
    assert(rv == ENGINE_SUCCESS);
    assert(genhash_size(stats_hash) == 1);

    assert(NULL == genhash_find(stats_hash, "bucket_conns", strlen("bucket_conns")));

    assert(memcmp("running",
                  genhash_find(stats_hash, "someuser", strlen("someuser")),
                  7) == 0);

    return SUCCESS;
}

static enum test_result test_unknown_call_no_bucket(ENGINE_HANDLE *h,
                                                    ENGINE_HANDLE_V1 *h1) {

    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;

    void *pkt = create_packet(0xfe, "somekey", "someval");
    rv = h1->unknown_command(h, mk_conn("someuser", NULL), pkt, add_response);
    free(pkt);
    assert(rv == ENGINE_DISCONNECT);

    return SUCCESS;
}

static enum test_result test_auto_config(ENGINE_HANDLE *h,
                                         ENGINE_HANDLE_V1 *h1) {
    item *itm = NULL;
    const void *cookie = mk_conn("someuser", MOCK_CONFIG_NO_ALLOC);
    char *key = "somekey";
    char *value = "some value";
    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;

    rv = h1->allocate(h, cookie, &itm,
                      key, strlen(key),
                      strlen(value), 9258, 3600);
    assert(rv == ENGINE_ENOMEM);

    return SUCCESS;
}

static enum test_result test_get_tap_iterator(ENGINE_HANDLE *h,
                                              ENGINE_HANDLE_V1 *h1) {
    /* This is run for its side effect of not crashing. */
    const void *cookie = mk_conn(NULL, NULL);
    tap_event_t e;
    TAP_ITERATOR ti = h1->get_tap_iterator(h, cookie,
                                           NULL, 0, 0, NULL, 0);
    assert(ti != NULL);
    do {
        item *it;
        void *engine_specific;
        uint16_t nengine_specific;
        uint8_t ttl;
        uint16_t flags;
        uint32_t seqno;
        uint16_t vbucket;
        e = ti(h, cookie, &it, &engine_specific, &nengine_specific, &ttl,
               &flags, &seqno, &vbucket);
    } while (e != TAP_DISCONNECT);

    mock_disconnect((void*)cookie);

    return SUCCESS;
}

static enum test_result test_tap_notify(ENGINE_HANDLE *h,
                                        ENGINE_HANDLE_V1 *h1) {
    ENGINE_ERROR_CODE ec = h1->tap_notify(h, mk_conn("someuser", ""),
                                          NULL, 0, 0, 0, TAP_MUTATION, 0,
                                          "akey", 4,
                                          0, 0, 0,
                                          "aval", 4, 0);
    assert(ec == ENGINE_SUCCESS);
    return SUCCESS;
}

#define MAX_CONNECTIONS_IN_POOL 1000
struct {
    cb_mutex_t mutex;
    int connected;
    int pend_close;
    TAP_ITERATOR iter;
    struct connstruct *conn;
} connection_pool[MAX_CONNECTIONS_IN_POOL];

static void init_connection_pool(void) {
    int ii;
    for (ii = 0; ii < MAX_CONNECTIONS_IN_POOL; ++ii) {
        connection_pool[ii].conn = mk_conn(NULL, NULL);
        mock_disconnect(connection_pool[ii].conn);
        connection_pool[ii].connected = 0;
        connection_pool[ii].pend_close = 0;
        connection_pool[ii].iter = NULL;
        cb_mutex_initialize(&connection_pool[ii].mutex);
    }
}

static void cleanup_connection_pool(ENGINE_HANDLE *h) {
    int ii;
    for (ii = 0; ii < MAX_CONNECTIONS_IN_POOL; ++ii) {
        if (connection_pool[ii].pend_close) {
            tap_event_t e = connection_pool[ii].iter(h,
                                                     connection_pool[ii].conn,
                                                     NULL, NULL, NULL,
                                                     NULL, NULL, NULL, NULL);
            assert(e == TAP_DISCONNECT);
        }
        mock_disconnect(connection_pool[ii].conn);
        cb_mutex_destroy(&connection_pool[ii].mutex);
    }
}

static void network_io_thread(void *arg) {
    int num_ops = 500000;
    ENGINE_HANDLE *h = arg;
    ENGINE_HANDLE_V1 *h1 = arg;
    int ii;
    for (ii = 0; ii < num_ops; ++ii) {
        long idx = (rand() & 0xffff) % 1000;
        cb_mutex_enter(&connection_pool[idx].mutex);
        if (!connection_pool[idx].pend_close) {
            if (!connection_pool[idx].connected) {
                mock_connect(connection_pool[idx].conn);
                connection_pool[idx].connected = 1;
                if (h != NULL) {
                    /* run tap connect */
                    TAP_ITERATOR ti;
                    ti = h1->get_tap_iterator(h,
                                              connection_pool[idx].conn,
                                              NULL, 0, 0, NULL, 0);
                    assert(ti != NULL);
                    connection_pool[idx].iter = ti;
                    connection_pool[idx].pend_close = 1;
                }
            } else {
                mock_disconnect(connection_pool[idx].conn);
                connection_pool[idx].connected = 0;
            }
        } else {
            tap_event_t e = connection_pool[idx].iter(h,
                                                     connection_pool[idx].conn,
                                                     NULL, NULL, NULL,
                                                     NULL, NULL, NULL, NULL);
            assert(e == TAP_DISCONNECT);
            connection_pool[idx].pend_close = 0;
        }
        cb_mutex_exit(&connection_pool[idx].mutex);
    }
}

static enum test_result test_concurrent_connect_disconnect(ENGINE_HANDLE *h,
                                                           ENGINE_HANDLE_V1 *h1) {
#define num_workers 10
    cb_thread_t workers[num_workers];
    int i;

    (void)h1;
    init_connection_pool();
    for (i = 0; i < num_workers; i++) {
        int rc = cb_create_thread(&workers[i], network_io_thread, NULL, 0);
        assert(rc == 0);
    }

    for (i = 0; i < num_workers; i++) {
        int rc = cb_join_thread(workers[i]);
        assert(rc == 0);
    }

#undef num_workers
    cleanup_connection_pool(h);
    return SUCCESS;
}

static enum test_result test_concurrent_connect_disconnect_tap(ENGINE_HANDLE *h,
                                                               ENGINE_HANDLE_V1 *h1) {
#define num_workers 40
    cb_thread_t workers[num_workers];
    int i;
    (void)h1;
    init_connection_pool();
    for (i = 0; i < num_workers; i++) {
        int rc = cb_create_thread(&workers[i], network_io_thread, h, 0);
        assert(rc == 0);
    }

    for (i = 0; i < num_workers; i++) {
        int rc = cb_join_thread(workers[i]);
        assert(rc == 0);
    }
#undef num_workers

    return SUCCESS;
}

static enum test_result test_topkeys(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;
    const void *adm_cookie = mk_conn("admin", NULL);
    int cmd;
    char *val;
    void *pkt = create_create_bucket_pkt("someuser", ENGINE_PATH, "");
    rv = h1->unknown_command(h, adm_cookie, pkt, add_response);
    free(pkt);


    pkt = create_packet(CMD_GET_REPLICA, "somekey", "someval");
    rv = h1->unknown_command(h, adm_cookie, pkt, add_response);
    free(pkt);

    for (cmd = 0x90; cmd < 0xff; ++cmd) {
        pkt = create_packet(cmd, "somekey", "someval");
        rv = h1->unknown_command(h, adm_cookie, pkt, add_response);
        free(pkt);
    }

    rv = h1->get_stats(h, adm_cookie, "topkeys", 7, add_stats);
    assert(rv == ENGINE_SUCCESS);
    assert(genhash_size(stats_hash) == 1);
    val = genhash_find(stats_hash, "somekey", strlen("somekey"));
    assert(val != NULL);
    assert(strstr(val, "get_replica=1,evict=1,getl=1,unlock=1,get_meta=2,set_meta=2,del_meta=2") != NULL);
    return SUCCESS;
}

static ENGINE_HANDLE_V1 *start_your_engines(const char *cfg) {
    ENGINE_HANDLE_V1 *h = (ENGINE_HANDLE_V1 *)load_engine(BUCKET_ENGINE_PATH, cfg);
    assert(h);
    return h;
}

static int report_test(enum test_result r) {
    int rc = 0;
    char *msg = NULL;
    bool color_enabled = getenv("TESTAPP_ENABLE_COLOR") != NULL;
    int color = 0;
    char color_str[8] = { 0 };
    char *reset_color = "\033[m";
    switch(r) {
    case SUCCESS:
        msg="OK";
        color = 32;
        break;
    case FAIL:
        color = 31;
        msg="FAIL";
        rc = 1;
        break;
    case DIED:
        color = 31;
        msg = "DIED";
        rc = 1;
        break;
    case CORE:
        color = 31;
        msg = "CORE DUMPED";
        rc = 1;
        break;
    case PENDING:
        color = 33;
        msg = "PENDING";
        break;
    default:
        color = 31;
        msg = "UNKNOWN";
    }
    if (color_enabled) {
        snprintf(color_str, sizeof(color_str), "\033[%dm", color);
    }
    printf("%s%s%s\n", color_str, msg, color_enabled ? reset_color : "");
    return rc;
}

static void disconnect_all_connections(struct connstruct *c) {
    while (c) {
        struct connstruct *next = c->next;
        mock_disconnect(c);
        free((void*)c->uname);
        free((void*)c->config);
        free(c);
        c = next;
    }
}

static void destroy_event_handlers_rec(struct engine_event_handler *h) {
    if (h) {
        destroy_event_handlers_rec(h->next);
        free(h);
    }
}

static void destroy_event_handlers(void) {
    int i = 0;
    for (i = 0; i < MAX_ENGINE_EVENT_TYPE; i++) {
        destroy_event_handlers_rec(engine_event_handlers[i]);
        engine_event_handlers[i] = NULL;
    }
}

static int hash_key_eq(const void *key, size_t nk,
                       const void *other, size_t no) {
    return nk == no && (memcmp(key, other, nk) == 0);
}

static void* hash_strdup(const void *x, size_t n) {
    char *rv = calloc(n + 1, sizeof(char));
    assert(rv);
    return memcpy(rv, x, n);
}

static int execute_test(struct test test) {
    enum test_result ret = PENDING;

    if (test.tfun != NULL) {
        ENGINE_HANDLE_V1 *h;
        struct hash_ops stats_hash_ops;

        /* Initialize the stats collection thingy */
        stats_hash_ops.hashfunc = genhash_string_hash;
        stats_hash_ops.hasheq = hash_key_eq;
        stats_hash_ops.dupKey = hash_strdup;
        stats_hash_ops.dupValue = hash_strdup;
        stats_hash_ops.freeKey = free;
        stats_hash_ops.freeValue = free;

        last_status = 0xff;
        stats_hash = genhash_init(25, stats_hash_ops);

        /* Start the engines and go */
        h = start_your_engines(test.cfg ? test.cfg : DEFAULT_CONFIG);
        ret = test.tfun((ENGINE_HANDLE*)h, h);
        /* we expect all threads to be dead so no need to guard
         * concurrent connstructs access anymore */
        disconnect_all_connections(connstructs);
        destroy_event_handlers();
        connstructs = NULL;
        h->destroy((ENGINE_HANDLE*)h, false);
        genhash_free(stats_hash);
    }

    return (int)ret;
}

struct warmer_arg {
    union {
        ENGINE_HANDLE *h;
        ENGINE_HANDLE_V1 *h1;
    } handles;
    int tid;
};

static void bench_warmer(void *arg) {
    struct warmer_arg *wa = arg;
    char key[32];
    const void *cookie = mk_conn("bench", NULL);
    int i;

    snprintf(key, sizeof(key), "k%d", wa->tid);
    for (i = 0; i < 10000000; i++) {
        item *itm = NULL;
        store(wa->handles.h, wa->handles.h1, cookie, key, "v", &itm);
        assert(itm);
    }
}

static void runBench(void) {
    ENGINE_HANDLE_V1 *h1 = start_your_engines(DEFAULT_CONFIG);
    ENGINE_HANDLE *h = (ENGINE_HANDLE*)h1;
    const void *adm_cookie = mk_conn("admin", NULL);
    void *pkt = create_create_bucket_pkt("bench", ENGINE_PATH, "");
    ENGINE_ERROR_CODE rv = h1->unknown_command(h, adm_cookie, pkt,
                                               add_response);
#define NUM_WORKERS 4
    cb_thread_t workers[NUM_WORKERS];
    struct warmer_arg args[NUM_WORKERS];
    int i;
    int rc;

    free(pkt);
    assert(rv == ENGINE_SUCCESS);
    assert(last_status == 0);

    for (i = 0; i < NUM_WORKERS; i++) {
        args[i].handles.h = h;
        args[i].tid = i;
        rc = cb_create_thread(&workers[i], bench_warmer, &args[i], 0);
        assert(rc == 0);
    }

    for (i = 0; i < NUM_WORKERS; i++) {
        rc = cb_join_thread(workers[i]);
        assert(rc == 0);
    }
#undef NUM_WORKERS
}

int main(int argc, char **argv) {
    int i;
    int rc;
    int maxtests;
    int errors = 0;

    struct test tests[] = {
        {"get info", test_get_info, NULL},
        {"default storage", test_default_storage, NULL},
        {"default storage key overrun", test_default_storage_key_overrun, NULL},
        {"default unlinked remove", test_default_unlinked_remove, NULL},
        {"no default storage",
         test_no_default_storage,
#ifdef WIN32
         "engine=bucket_engine_mock_engine.dll;default=false"
#else
         "engine=bucket_engine_mock_engine.so;default=false"
#endif
        },
        {"user storage with no default",
         test_two_engines,
#ifdef WIN32
         "engine=bucket_engine_mock_engine.dll;default=false"
#else
         "engine=bucket_engine_mock_engine.so;default=false"
#endif
        },
        {"distinct storage", test_two_engines, DEFAULT_CONFIG_AC},
        {"distinct storage (no auto-create)", test_two_engines_no_autocreate,
         DEFAULT_CONFIG_NO_DEF},
        {"delete from one of two nodes", test_two_engines_del,
         DEFAULT_CONFIG_AC},
        {"flush from one of two nodes", test_two_engines_flush,
         DEFAULT_CONFIG_AC},
        {"isolated arithmetic", test_arith, DEFAULT_CONFIG_AC},
        {"create bucket", test_create_bucket, DEFAULT_CONFIG_NO_DEF},
        {"double create bucket", test_double_create_bucket,
         DEFAULT_CONFIG_NO_DEF},
        {"create bucket with params", test_create_bucket_with_params,
         DEFAULT_CONFIG_NO_DEF},
        {"bucket name verification", test_bucket_name_validation, NULL},
        {"delete bucket", test_delete_bucket,
         DEFAULT_CONFIG_NO_DEF},
        {"delete bucket (same connection)", test_delete_bucket_sameconnection,
         DEFAULT_CONFIG_NO_DEF},
        {"concurrent access delete bucket", test_delete_bucket_concurrent,
         DEFAULT_CONFIG_NO_DEF},
        {"concurrent access delete bucket multiple times", test_delete_bucket_concurrent_multi,
         DEFAULT_CONFIG_NO_DEF},
        {"delete bucket shutdwn race", test_delete_bucket_shutdown_race,
         DEFAULT_CONFIG_NO_DEF},
        {"list buckets with none", test_list_buckets_none, NULL},
        {"list buckets with one", test_list_buckets_one, NULL},
        {"list buckets", test_list_buckets_two, NULL},
        {"fail to select a bucket when not admin", test_select_no_admin, NULL},
        {"select a bucket as admin", test_select, DEFAULT_CONFIG_AC},
        {"fail to select non-existent bucket as admin",
         test_select_no_bucket, NULL},
        {"stats call", test_stats, NULL},
        {"stats bucket call", test_stats_bucket, NULL},
        {"release call", test_release, NULL},
        {"unknown call delegation", test_unknown_call, NULL},
        {"unknown call delegation (no bucket)", test_unknown_call_no_bucket,
         DEFAULT_CONFIG_NO_DEF},
        {"admin verification", test_admin_user, NULL},
        {"auto create with config", test_auto_config,
         DEFAULT_CONFIG_AC},
        {"get tap iterator", test_get_tap_iterator, NULL},
        {"tap notify", test_tap_notify, NULL},
        {"concurrent connect/disconnect",
         test_concurrent_connect_disconnect, NULL },
        {"concurrent connect/disconnect (tap)",
         test_concurrent_connect_disconnect_tap, NULL },
        {"topkeys", test_topkeys, NULL },
        {NULL, NULL, NULL}
    };

    cb_mutex_initialize(&connstructs_mutex);
    cb_mutex_initialize(&notify_mutex);
    cb_cond_initialize(&notify_cond);

    putenv("MEMCACHED_TOP_KEYS=10");
    for (maxtests = 0; tests[maxtests].name; maxtests++) {
    }
    if (argc == 2) {
        /* Run a certain test */
        int testno = atoi(argv[1]);
        if (testno >= maxtests) {
            fprintf(stderr, "Invalid test specified\n");
            exit(EXIT_FAILURE);
        }

        exit(execute_test(tests[testno]));
    } else {
        /* iterate through all of the tests */
        for (i = 0; tests[i].name; i++) {
            char cmd[1024];
            enum test_result ret;

            fprintf(stdout, "Running %s... ", tests[i].name);
            fflush(stdout);
            snprintf(cmd, sizeof(cmd), "%s %d", argv[0], i);

            rc = system(cmd);
#ifdef WIN32
            ret = (enum test_result)rc;
#else
            if (WIFEXITED(rc)) {
                ret = (enum test_result)WEXITSTATUS(rc);
#ifdef WCOREDUMP
            } else if (WIFSIGNALED(rc) && WCOREDUMP(rc)) {
                ret = CORE;
#endif
            } else {
                ret = DIED;
            }
#endif
            errors += report_test(ret);
        }
    }

    if (getenv("BUCKET_ENGINE_BENCH") != NULL) {
        runBench();
    }

    if (errors == 0) {
        fprintf(stdout, "All tests pass\n");
        return EXIT_SUCCESS;
    } else {
        fprintf(stderr, "One or more tests failed\n");
        return EXIT_FAILURE;
    }
}
