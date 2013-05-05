/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include <memcached/engine.h>
#include "genhash.h"

#include "bucket_engine.h"

#define MAGIC 0xbeefcafe

#define ITEM_LINKED 1
#define ITEM_WITH_CAS 2

struct mock_stats {
    int get_reqs;
    int set_reqs;
    int current;
};

struct mock_engine {
    ENGINE_HANDLE_V1 engine;
    uint64_t magic;
    SERVER_HANDLE_V1 *server;
    bool initialized;
    genhash_t *hashtbl;
    struct mock_stats stats;
    int disconnects;
    uint64_t magic2;

    union {
      engine_info engine_info;
      char buffer[sizeof(engine_info) +
                  (sizeof(feature_info) * LAST_REGISTERED_ENGINE_FEATURE)];
    } info;
};

typedef struct mock_item {
    uint64_t cas;
    rel_time_t exptime; /**< When the item will expire (relative to process
                             * startup) */
    uint32_t nbytes; /**< The total size of the data (in bytes) */
    uint32_t flags; /**< Flags associated with the item (in network byte order)*/
    uint8_t clsid; /** class id for the object */
    uint16_t nkey; /**< The total length of the key (in bytes) */
} mock_item;

MEMCACHED_PUBLIC_API
ENGINE_ERROR_CODE create_instance(uint64_t interface,
                                  GET_SERVER_API gsapi,
                                  ENGINE_HANDLE **handle);

static const engine_info* mock_get_info(ENGINE_HANDLE* handle);
static ENGINE_ERROR_CODE mock_initialize(ENGINE_HANDLE* handle,
                                         const char* config_str);
static void mock_destroy(ENGINE_HANDLE* handle,
                         const bool force);
static ENGINE_ERROR_CODE mock_item_allocate(ENGINE_HANDLE* handle,
                                            const void* cookie,
                                            item **item,
                                            const void* key,
                                            const size_t nkey,
                                            const size_t nbytes,
                                            const int flags,
                                            const rel_time_t exptime);
static ENGINE_ERROR_CODE mock_item_delete(ENGINE_HANDLE* handle,
                                          const void* cookie,
                                          const void* key,
                                          const size_t nkey,
                                          uint64_t* cas,
                                          uint16_t vbucket);
static void mock_item_release(ENGINE_HANDLE* handle,
                              const void *cookie, item* item);
static ENGINE_ERROR_CODE mock_get(ENGINE_HANDLE* handle,
                                  const void* cookie,
                                  item** item,
                                  const void* key,
                                  const int nkey,
                                  uint16_t vbucket);
static ENGINE_ERROR_CODE mock_get_stats(ENGINE_HANDLE* handle,
                                        const void *cookie,
                                        const char *stat_key,
                                        int nkey,
                                        ADD_STAT add_stat);
static void mock_reset_stats(ENGINE_HANDLE* handle, const void *cookie);
static ENGINE_ERROR_CODE mock_store(ENGINE_HANDLE* handle,
                                    const void *cookie,
                                    item* item,
                                    uint64_t *cas,
                                    ENGINE_STORE_OPERATION operation,
                                    uint16_t vbucket);
static ENGINE_ERROR_CODE mock_arithmetic(ENGINE_HANDLE* handle,
                                         const void* cookie,
                                         const void* key,
                                         const int nkey,
                                         const bool increment,
                                         const bool create,
                                         const uint64_t delta,
                                         const uint64_t initial,
                                         const rel_time_t exptime,
                                         uint64_t *cas,
                                         uint64_t *result,
                                         uint16_t vbucket);
static ENGINE_ERROR_CODE mock_flush(ENGINE_HANDLE* handle,
                                    const void* cookie, time_t when);
static ENGINE_ERROR_CODE mock_unknown_command(ENGINE_HANDLE* handle,
                                              const void* cookie,
                                              protocol_binary_request_header *request,
                                              ADD_RESPONSE response);
static char* item_get_data(const item* item);
static const char* item_get_key(const item* item);
static void item_set_cas(ENGINE_HANDLE* handle, const void *cookie,
                         item* item, uint64_t val);
static uint64_t item_get_cas(const item* item);

static bool get_item_info(ENGINE_HANDLE *handle, const void *cookie,
                          const item* item, item_info *item_info);

static void handle_disconnect(const void *cookie,
                              ENGINE_EVENT_TYPE type,
                              const void *event_data,
                              const void *cb_data) {
    struct mock_engine *h = (struct mock_engine*)cb_data;
    (void)cookie;
    (void)event_data;
    assert(type == ON_DISCONNECT);
    ++h->disconnects;
}

static tap_event_t mock_tap_iterator(ENGINE_HANDLE* handle,
                                     const void *cookie,
                                     item **it,
                                     void **engine_specific,
                                     uint16_t *nengine_specific,
                                     uint8_t *ttl,
                                     uint16_t *flags,
                                     uint32_t *seqno,
                                     uint16_t *vbucket)
{
    struct mock_engine *e = (struct mock_engine*)handle;
    e->server->cookie->release(cookie);
    (void)it; (void)engine_specific; (void)nengine_specific;
    (void)ttl;(void)flags; (void)seqno; (void)vbucket;
    return TAP_DISCONNECT;
}



static TAP_ITERATOR mock_get_tap_iterator(ENGINE_HANDLE* handle, const void* cookie,
                                          const void* client, size_t nclient,
                                          uint32_t flags,
                                          const void* userdata, size_t nuserdata) {
    struct mock_engine *e = (struct mock_engine*)handle;
    (void)cookie;
    (void)client;
    (void)nclient;
    (void)flags;
    (void)userdata;
    (void)nuserdata;
    assert(e->magic == MAGIC);
    assert(e->magic2 == MAGIC);

    e->server->cookie->reserve(cookie);
    return mock_tap_iterator;
}

static ENGINE_ERROR_CODE mock_tap_notify(ENGINE_HANDLE* handle,
                                         const void *cookie,
                                         void *engine_specific,
                                         uint16_t nengine,
                                         uint8_t ttl,
                                         uint16_t tap_flags,
                                         tap_event_t tap_event,
                                         uint32_t tap_seqno,
                                         const void *key,
                                         size_t nkey,
                                         uint32_t flags,
                                         uint32_t exptime,
                                         uint64_t cas,
                                         const void *data,
                                         size_t ndata,
                                         uint16_t vbucket) {
    struct mock_engine *e = (struct mock_engine*)handle;
    (void)e;
    (void)cookie;
    (void)engine_specific;
    (void)nengine;
    (void)ttl;
    (void)tap_flags;
    (void)tap_event;
    (void)tap_seqno;
    (void)key;
    (void)nkey;
    (void)flags;
    (void)exptime;
    (void)cas;
    (void)data;
    (void)ndata;
    (void)vbucket;
    assert(e->magic == MAGIC);
    assert(e->magic2 == MAGIC);
    return ENGINE_SUCCESS;
}

MEMCACHED_PUBLIC_API
ENGINE_ERROR_CODE create_instance(uint64_t interface,
                                  GET_SERVER_API gsapi,
                                  ENGINE_HANDLE **handle) {
    struct mock_engine *h;
    if (interface != 1) {
        return ENGINE_ENOTSUP;
    }

    h = calloc(sizeof(struct mock_engine), 1);
    assert(h);
    h->engine.interface.interface = 1;
    h->engine.get_info = mock_get_info;
    h->engine.initialize = mock_initialize;
    h->engine.destroy = mock_destroy;
    h->engine.allocate = mock_item_allocate;
    h->engine.remove = mock_item_delete;
    h->engine.release = mock_item_release;
    h->engine.get = mock_get;
    h->engine.get_stats = mock_get_stats;
    h->engine.reset_stats = mock_reset_stats;
    h->engine.store = mock_store;
    h->engine.arithmetic = mock_arithmetic;
    h->engine.flush = mock_flush;
    h->engine.unknown_command = mock_unknown_command;
    h->engine.item_set_cas = item_set_cas;
    h->engine.get_item_info = get_item_info;
    h->engine.get_tap_iterator = mock_get_tap_iterator;
    h->engine.tap_notify = mock_tap_notify;

    h->server = gsapi();

    h->magic = MAGIC;
    h->magic2 = MAGIC;

    h->info.engine_info.description = "Mock engine v0.2";
    h->info.engine_info.num_features = 0;

    *handle = (ENGINE_HANDLE *)h;

    return ENGINE_SUCCESS;
}

static struct mock_engine* get_handle(ENGINE_HANDLE* handle) {
    struct mock_engine *e = (struct mock_engine*)handle;
    assert(e->magic == MAGIC);
    assert(e->magic2 == MAGIC);
    return e;
}

static const engine_info* mock_get_info(ENGINE_HANDLE* handle) {
    return &get_handle(handle)->info.engine_info;
}

static int my_hash_eq(const void *k1, size_t nkey1,
                      const void *k2, size_t nkey2) {
    return nkey1 == nkey2 && memcmp(k1, k2, nkey1) == 0;
}

static void* hash_strdup(const void *k, size_t nkey) {
    void *rv = calloc(nkey, 1);
    assert(rv);
    memcpy(rv, k, nkey);
    return rv;
}

static void* noop_dup(const void* ob, size_t vlen) {
    (void)vlen;
    return (void*)ob;
}

static void noop_free(void* ob) {
    (void)ob;
    /* Nothing */
}

static struct hash_ops my_hash_ops;

static ENGINE_ERROR_CODE mock_initialize(ENGINE_HANDLE* handle,
                                         const char* config_str) {
    struct mock_engine* se = get_handle(handle);
    assert(!se->initialized);

    my_hash_ops.hashfunc = genhash_string_hash;
    my_hash_ops.hasheq = my_hash_eq;
    my_hash_ops.dupKey = hash_strdup;
    my_hash_ops.dupValue = noop_dup;
    my_hash_ops.freeKey = free;
    my_hash_ops.freeValue = noop_free;

    assert(my_hash_ops.dupKey);

    if (strcmp(config_str, "no_alloc") != 0) {
        se->hashtbl = genhash_init(1, my_hash_ops);
        assert(se->hashtbl);
    }

    se->server->callback->register_callback((ENGINE_HANDLE*)se, ON_DISCONNECT,
                                            handle_disconnect, se);

    se->initialized = true;

    return ENGINE_SUCCESS;
}

static void mock_destroy(ENGINE_HANDLE* handle,
                         const bool force) {
    struct mock_engine* se = get_handle(handle);
    (void)force;

    if (se->initialized) {
        se->initialized = false;
        genhash_free(se->hashtbl);
        free(se);
    }
}

static genhash_t *get_ht(ENGINE_HANDLE *handle) {
    struct mock_engine* se = get_handle(handle);
    assert(se->initialized);
    return se->hashtbl;
}

static ENGINE_ERROR_CODE mock_item_allocate(ENGINE_HANDLE* handle,
                                            const void* cookie,
                                            item **it,
                                            const void* key,
                                            const size_t nkey,
                                            const size_t nbytes,
                                            const int flags,
                                            const rel_time_t exptime) {
    (void)cookie;
    /* Only perform allocations if there's a hashtable. */
    if (get_ht(handle) != NULL) {
        size_t to_alloc = sizeof(mock_item) + nkey + nbytes;
        *it = calloc(to_alloc, 1);
    } else {
        *it = NULL;
    }
    /* If an allocation was requested *and* worked, fill and report success */
    if (*it) {
        mock_item* i = (mock_item*) *it;
        i->exptime = exptime;
        i->nbytes = nbytes;
        i->flags = flags;
        i->nkey = nkey;
        memcpy((char*)item_get_key(i), key, nkey);
        return ENGINE_SUCCESS;
    } else {
        return ENGINE_ENOMEM;
    }
}

static ENGINE_ERROR_CODE mock_item_delete(ENGINE_HANDLE* handle,
                                          const void* cookie,
                                          const void* key,
                                          const size_t nkey,
                                          uint64_t* cas,
                                          uint16_t vbucket) {
    int r = genhash_delete_all(get_ht(handle), key, nkey);
    (void)cookie;
    (void)cas;
    (void)vbucket;
    return r > 0 ? ENGINE_SUCCESS : ENGINE_KEY_ENOENT;
}

static void mock_item_release(ENGINE_HANDLE* handle,
                              const void *cookie, item* itm) {
    (void)handle;
    (void)cookie;
    free(itm);
}

static ENGINE_ERROR_CODE mock_get(ENGINE_HANDLE* handle,
                                  const void* cookie,
                                  item** itm,
                                  const void* key,
                                  const int nkey,
                                  uint16_t vbucket) {
    (void)cookie;
    (void)vbucket;
    *itm = genhash_find(get_ht(handle), key, nkey);

    return *itm ? ENGINE_SUCCESS : ENGINE_KEY_ENOENT;
}

static ENGINE_ERROR_CODE mock_get_stats(ENGINE_HANDLE* handle,
                                        const void* cookie,
                                        const char* stat_key,
                                        int nkey,
                                        ADD_STAT add_stat)
{
    (void)handle;
    (void)cookie;
    (void)stat_key;
    (void)nkey;
    (void)add_stat;
    /* TODO:  Implement */
    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE mock_store(ENGINE_HANDLE* handle,
                                    const void *cookie,
                                    item* itm,
                                    uint64_t *cas,
                                    ENGINE_STORE_OPERATION operation,
                                    uint16_t vbucket) {
    mock_item* it = (mock_item*)itm;
    (void)cookie;
    (void)cas;
    (void)vbucket;
    (void)operation;
    genhash_update(get_ht(handle), item_get_key(itm), it->nkey, itm, 0);
    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE mock_arithmetic(ENGINE_HANDLE* handle,
                                         const void* cookie,
                                         const void* key,
                                         const int nkey,
                                         const bool increment,
                                         const bool create,
                                         const uint64_t delta,
                                         const uint64_t initial,
                                         const rel_time_t exptime,
                                         uint64_t *cas,
                                         uint64_t *result,
                                         uint16_t vbucket) {
    item *item_in = NULL, *item_out = NULL;
    int flags = 0;
    char buf[32];
    ENGINE_ERROR_CODE rv;
    (void)increment;
    (void)vbucket;
    *cas = 0;

    if (mock_get(handle, cookie, &item_in, key, nkey, 0) == ENGINE_SUCCESS) {
        /* Found, just do the math. */
        /* This is all int stuff, just to make it easy. */
        *result = atoi(item_get_data(item_in));
        *result += delta;
        flags = ((mock_item*) item_in)->flags;
    } else if (create) {
        /* Not found, do the initialization */
        *result = initial;
    } else {
        /* Reject. */
        return ENGINE_KEY_ENOENT;
    }

    snprintf(buf, sizeof(buf), "%"PRIu64, *result);
    if((rv = mock_item_allocate(handle, cookie, &item_out,
                                key, nkey,
                                strlen(buf) + 1,
                                flags, exptime)) != ENGINE_SUCCESS) {
        return rv;
    }
    memcpy(item_get_data(item_out), buf, strlen(buf) + 1);
    mock_store(handle, cookie, item_out, 0, OPERATION_SET, 0);
    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE mock_flush(ENGINE_HANDLE* handle,
                                    const void* cookie, time_t when) {
    (void)cookie;
    (void)when;
    genhash_clear(get_ht(handle));
    return ENGINE_SUCCESS;
}

static void mock_reset_stats(ENGINE_HANDLE* handle, const void *cookie) {
    (void)handle;
    (void)cookie;
    /* TODO:  Implement */
}

static ENGINE_ERROR_CODE mock_unknown_command(ENGINE_HANDLE* handle,
                                              const void* cookie,
                                              protocol_binary_request_header *request,
                                              ADD_RESPONSE response)
{
    switch (request->request.opcode) {
    case CMD_GET_REPLICA:
    case CMD_EVICT_KEY:
    case CMD_GET_LOCKED:
    case CMD_UNLOCK_KEY:
    case CMD_GET_META:
    case CMD_GETQ_META:
    case CMD_SET_WITH_META:
    case CMD_SETQ_WITH_META:
    case CMD_DEL_WITH_META:
    case CMD_DELQ_WITH_META:
        return ENGINE_SUCCESS;
    }

    (void)handle; (void)cookie; (void)response;
    return ENGINE_ENOTSUP;
}

static uint64_t item_get_cas(const item* itm)
{
    const mock_item* it = (mock_item*)itm;
    return it->cas;
}

static void item_set_cas(ENGINE_HANDLE *handle, const void *cookie,
                         item* itm, uint64_t val)
{
    mock_item* it = (mock_item*)itm;
    (void)handle;
    (void)cookie;
    it->cas = val;
}

static const char* item_get_key(const item* itm)
{
    const mock_item* it = (mock_item*)itm;
    char *ret = (void*)(it + 1);
    return ret;
}

static char* item_get_data(const item* itm)
{
    const mock_item* it = (mock_item*)itm;
    return ((char*)item_get_key(itm)) + it->nkey;
}

static bool get_item_info(ENGINE_HANDLE *handle, const void *cookie,
                          const item* itm, item_info *itm_info)
{
    mock_item* it = (mock_item*)itm;
    (void)handle;
    (void)cookie;
    if (itm_info->nvalue < 1) {
        return false;
    }
    itm_info->cas = item_get_cas(it);
    itm_info->exptime = it->exptime;
    itm_info->nbytes = it->nbytes;
    itm_info->flags = it->flags;
    itm_info->clsid = it->clsid;
    itm_info->nkey = it->nkey;
    itm_info->nvalue = 1;
    itm_info->key = item_get_key(it);
    itm_info->value[0].iov_base = item_get_data(it);
    itm_info->value[0].iov_len = it->nbytes;
    return true;
}
