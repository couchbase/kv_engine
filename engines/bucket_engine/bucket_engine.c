/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <dlfcn.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/time.h>
#ifndef WIN32
#include <arpa/inet.h>
#else
#include <winsock.h>
#endif

#include <assert.h>
#include <stddef.h>
#include <stdarg.h>

#include <memcached/engine.h>
#include "genhash.h"
#include "topkeys.h"
#include "bucket_engine.h"
#include "bucket_engine_internal.h"

static rel_time_t (*get_current_time)(void);
static EXTENSION_LOGGER_DESCRIPTOR *logger;

#if defined(HAVE_ATOMIC_H) && defined(__SUNPRO_C)
#include <atomic.h>
static inline int ATOMIC_ADD(volatile int *dest, int value) {
    return atomic_add_int_nv((volatile unsigned int *)dest, value);
}

static inline int ATOMIC_INCR(volatile int *dest) {
    return atomic_inc_32_nv((volatile unsigned int *)dest);
}

static inline int ATOMIC_DECR(volatile int *dest) {
    return atomic_dec_32_nv((volatile unsigned int *)dest);
}

static inline int ATOMIC_CAS(volatile bucket_state_t *dest, int prev, int next) {
    return (prev == atomic_cas_uint((volatile uint_t*)dest, (uint_t)prev,
                                    (uint_t)next));
}
#else
#define ATOMIC_ADD(i, by) __sync_add_and_fetch(i, by)
#define ATOMIC_INCR(i) ATOMIC_ADD(i, 1)
#define ATOMIC_DECR(i) ATOMIC_ADD(i, -1)
#define ATOMIC_CAS(ptr, oldval, newval) \
            __sync_bool_compare_and_swap(ptr, oldval, newval)
#endif

static ENGINE_ERROR_CODE (*upstream_reserve_cookie)(const void *cookie);
static ENGINE_ERROR_CODE (*upstream_release_cookie)(const void *cookie);
static ENGINE_ERROR_CODE bucket_engine_reserve_cookie(const void *cookie);
static ENGINE_ERROR_CODE bucket_engine_release_cookie(const void *cookie);

struct bucket_list {
    char *name;
    int namelen;
    proxied_engine_handle_t *peh;
    struct bucket_list *next;
};

MEMCACHED_PUBLIC_API
ENGINE_ERROR_CODE create_instance(uint64_t interface,
                                  GET_SERVER_API gsapi,
                                  ENGINE_HANDLE **handle);

static const engine_info* bucket_get_info(ENGINE_HANDLE* handle);

static ENGINE_ERROR_CODE bucket_initialize(ENGINE_HANDLE* handle,
                                           const char* config_str);
static void bucket_destroy(ENGINE_HANDLE* handle,
                           const bool force);
static ENGINE_ERROR_CODE bucket_item_allocate(ENGINE_HANDLE* handle,
                                              const void* cookie,
                                              item **item,
                                              const void* key,
                                              const size_t nkey,
                                              const size_t nbytes,
                                              const int flags,
                                              const rel_time_t exptime);
static ENGINE_ERROR_CODE bucket_item_delete(ENGINE_HANDLE* handle,
                                            const void* cookie,
                                            const void* key,
                                            const size_t nkey,
                                            uint64_t* cas,
                                            uint16_t vbucket);
static void bucket_item_release(ENGINE_HANDLE* handle,
                                const void *cookie,
                                item* item);
static ENGINE_ERROR_CODE bucket_get(ENGINE_HANDLE* handle,
                                    const void* cookie,
                                    item** item,
                                    const void* key,
                                    const int nkey,
                                    uint16_t vbucket);
static ENGINE_ERROR_CODE bucket_get_stats(ENGINE_HANDLE* handle,
                                          const void *cookie,
                                          const char *stat_key,
                                          int nkey,
                                          ADD_STAT add_stat);
static void *bucket_get_stats_struct(ENGINE_HANDLE* handle,
                                                    const void *cookie);
static ENGINE_ERROR_CODE bucket_aggregate_stats(ENGINE_HANDLE* handle,
                                                const void* cookie,
                                                void (*callback)(void*, void*),
                                                void *stats);
static void bucket_reset_stats(ENGINE_HANDLE* handle, const void *cookie);
static ENGINE_ERROR_CODE bucket_store(ENGINE_HANDLE* handle,
                                      const void *cookie,
                                      item* item,
                                      uint64_t *cas,
                                      ENGINE_STORE_OPERATION operation,
                                      uint16_t vbucket);
static ENGINE_ERROR_CODE bucket_arithmetic(ENGINE_HANDLE* handle,
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
static ENGINE_ERROR_CODE bucket_flush(ENGINE_HANDLE* handle,
                                      const void* cookie, time_t when);
static ENGINE_ERROR_CODE initialize_configuration(struct bucket_engine *me,
                                                  const char *cfg_str);
static ENGINE_ERROR_CODE bucket_unknown_command(ENGINE_HANDLE* handle,
                                                const void* cookie,
                                                protocol_binary_request_header *request,
                                                ADD_RESPONSE response);

static bool bucket_get_item_info(ENGINE_HANDLE *handle,
                                 const void *cookie,
                                 const item* item,
                                 item_info *item_info);

static void bucket_item_set_cas(ENGINE_HANDLE *handle, const void *cookie,
                                item *item, uint64_t cas);

static ENGINE_ERROR_CODE bucket_tap_notify(ENGINE_HANDLE* handle,
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
                                           uint16_t vbucket);

static TAP_ITERATOR bucket_get_tap_iterator(ENGINE_HANDLE* handle, const void* cookie,
                                            const void* client, size_t nclient,
                                            uint32_t flags,
                                            const void* userdata, size_t nuserdata);

static size_t bucket_errinfo(ENGINE_HANDLE *handle, const void* cookie,
                             char *buffer, size_t buffsz);

static ENGINE_HANDLE *load_engine(void **dlhandle, const char *soname);

static bool is_authorized(ENGINE_HANDLE* handle, const void* cookie);

static void free_engine_handle(proxied_engine_handle_t *);

static bool list_buckets(struct bucket_engine *e, struct bucket_list **blist);
static void bucket_list_free(struct bucket_list *blist);
static void maybe_start_engine_shutdown(proxied_engine_handle_t *e);


/**
 * This is the one and only instance of the bucket engine.
 */
struct bucket_engine bucket_engine = {
    .engine = {
        .interface = {
            .interface = 1
        },
        .get_info         = bucket_get_info,
        .initialize       = bucket_initialize,
        .destroy          = bucket_destroy,
        .allocate         = bucket_item_allocate,
        .remove           = bucket_item_delete,
        .release          = bucket_item_release,
        .get              = bucket_get,
        .store            = bucket_store,
        .arithmetic       = bucket_arithmetic,
        .flush            = bucket_flush,
        .get_stats        = bucket_get_stats,
        .reset_stats      = bucket_reset_stats,
        .get_stats_struct = bucket_get_stats_struct,
        .aggregate_stats  = bucket_aggregate_stats,
        .unknown_command  = bucket_unknown_command,
        .tap_notify       = bucket_tap_notify,
        .get_tap_iterator = bucket_get_tap_iterator,
        .item_set_cas     = bucket_item_set_cas,
        .get_item_info    = bucket_get_item_info,
        .errinfo          = bucket_errinfo
    },
    .initialized = false,
    .shutdown = {
        .in_progress = false,
        .bucket_counter = 0,
        .mutex = PTHREAD_MUTEX_INITIALIZER,
        .cond = PTHREAD_COND_INITIALIZER,
        .refcount_cond = PTHREAD_COND_INITIALIZER
    },
    .info.engine_info = {
        .description = "Bucket engine v0.2",
        .num_features = 1,
        .features = {
            {.feature = ENGINE_FEATURE_MULTI_TENANCY,
             .description = "Multi tenancy"}
        }
    },
};

/**
 * To help us detect if we're using free'd memory, let's write a
 * pattern to the memory before releasing it. That makes it more easy
 * to identify in a core file if we're operating on a freed memory area
 */
static void release_memory(void *ptr, size_t size)
{
    memset(ptr, 0xae, size);
    free(ptr);
}


/* Internal utility functions */

/**
 * pthread_mutex_lock should _never_ fail, and instead
 * of clutter the code with a lot of tests this logic is moved
 * here.
 */
void must_lock(pthread_mutex_t *mutex)
{
    int rv = pthread_mutex_lock(mutex);
    if (rv != 0) {
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "FATAL: Failed to lock mutex: %d", rv);
        abort();
    }
}

/**
 * pthread_mutex_unlock should _never_ fail, and instead
 * of clutter the code with a lot of tests this logic is moved
 * here.
 */
void must_unlock(pthread_mutex_t *mutex)
{
    int rv = pthread_mutex_unlock(mutex);
    if (rv != 0) {
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "FATAL: Failed to release mutex: %d", rv);
        abort();
    }
}

/**
 * Access to the global list of engines is protected by a single lock.
 * To make the code more readable we're using a separate function
 * to acquire the lock
 */
static void lock_engines(void)
{
    must_lock(&bucket_engine.engines_mutex);
}

/**
 * This is the corresponding function to release the lock for
 * the list of engines.
 */
static void unlock_engines(void)
{
    must_unlock(&bucket_engine.engines_mutex);
}

/**
 * Convert a bucket state (enum) t a textual string
 */
static const char * bucket_state_name(bucket_state_t s) {
    const char * rv = NULL;
    switch(s) {
    case STATE_NULL: rv = "NULL"; break;
    case STATE_RUNNING: rv = "running"; break;
    case STATE_STOPPING: rv = "stopping"; break;
    case STATE_STOPPED: rv = "stopped"; break;
    }
    assert(rv);
    return rv;
}

/**
 * Helper function to get a pointer to the server API
 */
static SERVER_HANDLE_V1 *bucket_get_server_api(void) {
    return &bucket_engine.server;
}

/**
 * Helper structure used by find_bucket_by_engine
 */
struct bucket_find_by_handle_data {
    /** The engine we're searching for */
    ENGINE_HANDLE *needle;
    /** The engine-handle for this engine */
    proxied_engine_handle_t *peh;
};

/**
 * A callback function used by genhash_iter to locate the engine handle
 * object for a given engine.
 *
 * Runs with engines lock held.
 *
 * @param key not used
 * @param nkey not used
 * @param val the engine handle stored at this position in the hash
 * @param nval not used
 * @param args pointer to a bucket_find_by_handle_data structure
 *             used to pass the search cirtera into the function and
 *             return the object (if found).
 */
static void find_bucket_by_engine(const void* key, size_t nkey,
                                  const void *val, size_t nval,
                                  void *args) {
    (void)key;
    (void)nkey;
    (void)nval;
    struct bucket_find_by_handle_data *find_data = args;
    assert(find_data);
    assert(find_data->needle);

    const proxied_engine_handle_t *peh = val;
    if (find_data->needle == peh->pe.v0) {
        find_data->peh = (proxied_engine_handle_t *)peh;
    }
}

/**
 * bucket_engine intercepts the calls from the underlying engine to
 * register callbacks. During startup bucket engine registers a callback
 * for ON_DISCONNECT in memcached, so we should always be notified
 * whenever a client disconnects. The underlying engine may however also
 * want this notification, so we intercept their attemt to register
 * callbacks and forward the callback to the correct engine.
 *
 * This function will _always_ be called while we're holding the global
 * lock for the hash table (during the call to "initialize" in the
 * underlying engine. It is therefore safe to try to traverse the
 * engines list.
 */
static void bucket_register_callback(ENGINE_HANDLE *eh,
                                     ENGINE_EVENT_TYPE type,
                                     EVENT_CALLBACK cb, const void *cb_data) {

    /* For simplicity, we're not going to test every combination until
       we need them. */
    assert(type == ON_DISCONNECT);

    /* Assume this always happens while holding the hash table lock. */
    /* This is called from underlying engine 'initialize' handler
     * which we invoke with engines_mutex held */
    struct bucket_find_by_handle_data find_data = { .needle = eh,
                                                    .peh = NULL };

    genhash_iter(bucket_engine.engines, find_bucket_by_engine, &find_data);

    if (find_data.peh) {
        find_data.peh->cb = cb;
        find_data.peh->cb_data = cb_data;
        find_data.peh->wants_disconnects = true;
    } else if (bucket_engine.has_default && eh == bucket_engine.default_engine.pe.v0){
        bucket_engine.default_engine.cb = cb;
        bucket_engine.default_engine.cb_data = cb_data;
        bucket_engine.default_engine.wants_disconnects = true;
    }
}

/**
 * The engine api allows the underlying engine to perform various callbacks
 * This isn't implemented in bucket engine as of today.
 */
static void bucket_perform_callbacks(ENGINE_EVENT_TYPE type,
                                     const void *data, const void *cookie) {
    (void)type;
    (void)data;
    (void)cookie;
    abort(); /* Not implemented */
}

/**
 * Store engine-specific data in the engine-specific section of this
 * cookie's data stored in the memcached core. The "upstream" cookie
 * should have been registered during the "ON_CONNECT" callback, so it
 * would be a bug if it isn't here anymore
 */
static void bucket_store_engine_specific(const void *cookie, void *engine_data) {
    engine_specific_t *es;
    es = bucket_engine.upstream_server->cookie->get_engine_specific(cookie);
    assert(es);
    es->engine_specific = engine_data;
}

/**
 * Get the engine-specific data from the engine-specific section of
 * this cookies data stored in the memcached core.
 */
static void* bucket_get_engine_specific(const void *cookie) {
    engine_specific_t *es = bucket_engine.upstream_server->cookie->get_engine_specific(cookie);
    assert(es);
    return es->engine_specific;
}

/**
 * We don't allow the underlying engines to register or remove extensions
 */
static bool bucket_register_extension(extension_type_t type,
                                      void *extension) {
    (void)type;
    (void)extension;
    logger->log(EXTENSION_LOG_WARNING, NULL,
                "Extension support isn't implemented in this version "
                "of bucket_engine");
    return false;
}

/**
 * Since you can't register an extension this function should _never_ be
 * called...
 */
static void bucket_unregister_extension(extension_type_t type, void *extension) {
    (void)type;
    (void)extension;
    logger->log(EXTENSION_LOG_WARNING, NULL,
                "Extension support isn't implemented in this version "
                "of bucket_engine");
    abort(); /* No extensions registered, none can unregister */
}

/**
 * Get a given extension type from the memcached core.
 * @todo Why do we overload this when all we do is wrap it directly?
 */
static void* bucket_get_extension(extension_type_t type) {
    return bucket_engine.upstream_server->extension->get_extension(type);
}

/* Engine API functions */

/**
 * This is the public entry point for bucket_engine. It is called by
 * the memcached core and is responsible for doing basic allocation and
 * initialization of the one and only instance of the bucket_engine object.
 *
 * The "normal" initialization is performed in bucket_initialize which is
 * called from the memcached core after a successful call to create_instance.
 */
ENGINE_ERROR_CODE create_instance(uint64_t interface,
                                  GET_SERVER_API gsapi,
                                  ENGINE_HANDLE **handle) {
    if (interface != 1) {
        return ENGINE_ENOTSUP;
    }

    *handle = (ENGINE_HANDLE*)&bucket_engine;
    bucket_engine.upstream_server = gsapi();
    bucket_engine.server = *bucket_engine.upstream_server;
    bucket_engine.get_server_api = bucket_get_server_api;

    /* Use our own callback API for inferior engines */
    bucket_engine.callback_api.register_callback = bucket_register_callback;
    bucket_engine.callback_api.perform_callbacks = bucket_perform_callbacks;
    bucket_engine.server.callback = &bucket_engine.callback_api;

    /* Same for extensions */
    bucket_engine.extension_api.register_extension = bucket_register_extension;
    bucket_engine.extension_api.unregister_extension = bucket_unregister_extension;
    bucket_engine.extension_api.get_extension = bucket_get_extension;
    bucket_engine.server.extension = &bucket_engine.extension_api;

    /* Override engine specific */
    bucket_engine.cookie_api = *bucket_engine.upstream_server->cookie;
    bucket_engine.server.cookie = &bucket_engine.cookie_api;
    bucket_engine.server.cookie->store_engine_specific = bucket_store_engine_specific;
    bucket_engine.server.cookie->get_engine_specific = bucket_get_engine_specific;

    upstream_reserve_cookie = bucket_engine.server.cookie->reserve;
    upstream_release_cookie = bucket_engine.server.cookie->release;

    bucket_engine.server.cookie->reserve = bucket_engine_reserve_cookie;
    bucket_engine.server.cookie->release = bucket_engine_release_cookie;

    logger = bucket_engine.server.extension->get_extension(EXTENSION_LOGGER);
    return ENGINE_SUCCESS;
}

/**
 * Grab the engine handle mutex and release the proxied engine handle.
 * The function currently allows you to call it with a NULL pointer,
 * but that should be replaced (we should have better control of if we
 * have an engine handle or not....)
 */
static void release_handle(proxied_engine_handle_t *peh) {
    if (!peh) {
        return;
    }

    int count = ATOMIC_DECR(&peh->refcount);
    assert(count >= 0);
    if (count == 0) {
        must_lock(&bucket_engine.shutdown.mutex);
        pthread_cond_broadcast(&bucket_engine.shutdown.refcount_cond);
        must_unlock(&bucket_engine.shutdown.mutex);
    }
}

/**
 * Helper function to search for a named bucket in the list of engines
 * You must wrap this call with (un)lock_engines() in order for it to
 * be mt-safe
 */
static proxied_engine_handle_t *find_bucket_inner(const char *name) {
    return genhash_find(bucket_engine.engines, name, strlen(name));
}

/**
 * If the bucket is in a runnable state, increment its reference counter
 * and return its handle. Otherwise a NIL pointer is returned.
 * The caller is responsible for releasing the handle
 * with release_handle.
 */
static proxied_engine_handle_t* retain_handle(proxied_engine_handle_t *peh) {
    proxied_engine_handle_t *rv = NULL;
    if (peh) {
        if (peh->state == STATE_RUNNING) {
            int count = ATOMIC_INCR(&peh->refcount);
            assert(count > 0);
            rv = peh;
        }
    }
    return rv;
}

/**
 * Search the list of buckets for a named bucket. If the bucket
 * exists and is in a runnable state, it's reference count is
 * incremented and returned. The caller is responsible for
 * releasing the handle with release_handle.
*/
static proxied_engine_handle_t *find_bucket(const char *name) {
    lock_engines();
    proxied_engine_handle_t *rv = retain_handle(find_bucket_inner(name));
    unlock_engines();
    return rv;
}

/**
 * Validate that the bucket name only consists of legal characters
 */
static bool has_valid_bucket_name(const char *n) {
    bool rv = n[0] != 0;
    for (; *n; n++) {
        rv &= isalpha(*n) || isdigit(*n) || *n == '.' || *n == '%' || *n == '_' || *n == '-';
    }
    return rv;
}

/**
 * Initialize a proxied engine handle. (Assumes that it's zeroed already
*/
static ENGINE_ERROR_CODE init_engine_handle(proxied_engine_handle_t *peh, const char *name, const char *module) {
    peh->stats = bucket_engine.upstream_server->stat->new_stats();
    if (peh->stats == NULL) {
        return ENGINE_ENOMEM;
    }
    if (bucket_engine.topkeys != 0) {
        peh->topkeys = calloc(TK_SHARDS, sizeof(topkeys_t *));
        for (int i = 0; i < TK_SHARDS; i++) {
            peh->topkeys[i] = topkeys_init(bucket_engine.topkeys);
        }
        if (peh->topkeys == NULL) {
            bucket_engine.upstream_server->stat->release_stats(peh->stats);
            peh->stats = NULL;
            return ENGINE_ENOMEM;
        }
    }
    peh->refcount = 1;
    peh->name = strdup(name);
    if (peh->name == NULL) {
        return ENGINE_ENOMEM;
    }
    peh->name_len = strlen(peh->name);

    if (module && strstr(module, "default_engine") != 0) {
        peh->tap_iterator_disabled = true;
    }

    peh->state = STATE_RUNNING;
    return ENGINE_SUCCESS;
}

/**
 * Release the allocated resources within a proxied engine handle.
 * Use free_engine_handle if you like to release the memory for the
 * proxied engine handle itself...
 */
static void uninit_engine_handle(proxied_engine_handle_t *peh) {
    bucket_engine.upstream_server->stat->release_stats(peh->stats);
    if (peh->topkeys != NULL) {
        for (int i = 0; i < TK_SHARDS; i++) {
            topkeys_free(peh->topkeys[i]);
        }
        free(peh->topkeys);
    }
    release_memory((void*)peh->name, peh->name_len);
    /* Note: looks like current engine API allows engine to keep some
     * connections reserved past destroy call return. This implies
     * that doing dlclose is raceful and thus we should not do it.
     *
     * Currently we also have issue with tcmalloc integration on
     * windows where apparently unloading ep.so is causing some
     * troubles in tcmalloc. */
    /*
     * if (peh->dlhandle) {
     *     dlclose(peh->dlhandle);
     * }
     */
}

/**
 * Release all resources used by a proxied engine handle and
 * invalidate the proxied engine handle itself.
 */
static void free_engine_handle(proxied_engine_handle_t *peh) {
    uninit_engine_handle(peh);
    release_memory(peh, sizeof(*peh));
}

/**
 * Creates bucket and places it's handle into *e_out. NOTE: that
 * caller is responsible for calling release_handle on that handle
 */
static ENGINE_ERROR_CODE create_bucket_UNLOCKED(struct bucket_engine *e,
                                                const char *bucket_name,
                                                const char *path,
                                                const char *config,
                                                proxied_engine_handle_t **e_out,
                                                char *msg, size_t msglen) {

    ENGINE_ERROR_CODE rv;

    if (!has_valid_bucket_name(bucket_name)) {
        return ENGINE_EINVAL;
    }

    proxied_engine_handle_t *peh = calloc(sizeof(proxied_engine_handle_t), 1);
    if (peh == NULL) {
        return ENGINE_ENOMEM;
    }
    rv = init_engine_handle(peh, bucket_name, path);
    if (rv != ENGINE_SUCCESS) {
        release_memory(peh, sizeof(*peh));
        return rv;
    }

    rv = ENGINE_FAILED;

    peh->pe.v0 = load_engine(&peh->dlhandle, path);

    if (!peh->pe.v0) {
        free_engine_handle(peh);
        if (msg) {
            snprintf(msg, msglen, "Failed to load engine.");
        }
        return rv;
    }

    proxied_engine_handle_t *tmppeh = find_bucket_inner(bucket_name);
    if (tmppeh == NULL) {
        genhash_update(e->engines, bucket_name, strlen(bucket_name), peh, 0);

        // This was already verified, but we'll check it anyway
        assert(peh->pe.v0->interface == 1);

        rv = ENGINE_SUCCESS;

        if (peh->pe.v1->initialize(peh->pe.v0, config) != ENGINE_SUCCESS) {
            peh->pe.v1->destroy(peh->pe.v0, false);
            genhash_delete_all(e->engines, bucket_name, strlen(bucket_name));
            if (msg) {
                snprintf(msg, msglen,
                         "Failed to initialize instance. Error code: %d\n", rv);
            }
            rv = ENGINE_FAILED;
        }
    } else {
        if (msg) {
            snprintf(msg, msglen,
                     "Bucket exists: %s", bucket_state_name(tmppeh->state));
        }
        peh->pe.v1->destroy(peh->pe.v0, true);
        rv = ENGINE_KEY_EEXISTS;
    }

    if (rv == ENGINE_SUCCESS) {
        if (e_out) {
            *e_out = peh;
        } else {
            release_handle(peh);
        }
    } else {
        free_engine_handle(peh);
    }

    return rv;
}

/**
 * The client returned from the call inside the engine. If this was the
 * last client inside the engine, and the engine is scheduled for removal
 * it should be safe to nuke the engine :)
 *
 * @param engine the proxied engine
 */
static void release_engine_handle(proxied_engine_handle_t *engine) {
    assert(engine->clients > 0);
    int count = ATOMIC_DECR(&engine->clients);
    assert(count >= 0);
    if (count == 0 && engine->state == STATE_STOPPING) {
        maybe_start_engine_shutdown(engine);
    }
}

/**
 * Returns engine handle for this connection.
 * All access to underlying engine must go through this function, because
 * we keep a counter of how many cookies that are currently calling into
 * the engine..
 *
 * NOTE: this cannot ever return engine handle that's in STATE_STOPPED
 * and if returns non-null it also prevents STATE_STOPPED to be
 * reached until release_engine_handle is called that'll decrement
 * clients counter. Here's why:
 *
 * Assume it returned non-null but engine's state is
 * STATE_STOPPED. But that means state was changed after it was
 * observed to be STATE_RUNNING in this function. And because we never
 * change from running to stopped it changed twice. Because STATE_RUNNING was seen after incrementing clients count here's sequence of inter-dependendent events:
 *
 * - we bump clients count
 *
 * - we observe STATE_RUNNING (and that also implies didn't
     have STATE_STOPPED & STATE_STOPPING in past because we don't
     change from STOPPING/STOPPED back to RUNNING)
 *
 * - some other thread changes STATE_RUNNING to STATE_STOPPING
 *
 * - somebody sets STATE_STOPPED (see
     maybe_start_engine_shutdown). But that implies that somebody
     first observed STATE_STOPPING and _then_ observed clients ==
     0. Which assuming nobody decrements it without first incrementing
     it cannot happen because our bumped clients count prevents that.
 *
 * Q.E.D.
 */
static proxied_engine_handle_t *get_engine_handle(ENGINE_HANDLE *h,
                                                  const void *cookie) {
    struct bucket_engine *e = (struct bucket_engine*)h;
    engine_specific_t *es;
    es = e->upstream_server->cookie->get_engine_specific(cookie);
    assert(es);

    proxied_engine_handle_t *peh = es->peh;
    if (!peh) {
        if (e->default_engine.pe.v0) {
            peh = &e->default_engine;
        } else {
            return NULL;
        }
    }

    int count = ATOMIC_INCR(&peh->clients);
    assert(count > 0);

    if (peh->state != STATE_RUNNING) {
        release_engine_handle(peh);
        peh = NULL;
    }

    return peh;
}

/**
 * Returns engine handle for this connection.
 * All access to underlying engine must go through this function, because
 * we keep a counter of how many cookies that are currently calling into
 * the engine..
 */
static proxied_engine_handle_t *try_get_engine_handle(ENGINE_HANDLE *h,
                                                      const void *cookie) {
    struct bucket_engine *e = (struct bucket_engine*)h;
    engine_specific_t *es;
    es = e->upstream_server->cookie->get_engine_specific(cookie);
    if (es == NULL || es->peh == NULL) {
        return NULL;
    }
    proxied_engine_handle_t *peh = es->peh;
    proxied_engine_handle_t *ret = peh;

    int count = ATOMIC_INCR(&peh->clients);
    assert(count > 0);
    if (peh->state != STATE_RUNNING) {
        release_engine_handle(peh);
        ret = NULL;
    }

    return ret;
}

/**
 * Create an engine specific section for the cookie
 */
static void create_engine_specific(struct bucket_engine *e,
                                   const void *cookie) {
    engine_specific_t *es;
    es = e->upstream_server->cookie->get_engine_specific(cookie);
    assert(es == NULL);
    es = calloc(1, sizeof(engine_specific_t));
    assert(es);
    es->reserved = ES_CONNECTED_FLAG;
    e->upstream_server->cookie->store_engine_specific(cookie, es);
}

/**
 * Set the engine handle for a cookie (create if it doesn't exist)
 */
static proxied_engine_handle_t* set_engine_handle(ENGINE_HANDLE *h,
                                                  const void *cookie,
                                                  proxied_engine_handle_t *peh) {
    (void)h;
    engine_specific_t *es;
    es = bucket_engine.upstream_server->cookie->get_engine_specific(cookie);
    assert(es);

    /* we cannot switch bucket for connection that's reserved. With
     * current code at least. */
    assert((es->reserved & ~ES_CONNECTED_FLAG) == 0);

    proxied_engine_handle_t *old = es->peh;
    // In with the new
    es->peh = retain_handle(peh);

    // out with the old (this may be NULL if we did't have an associated
    // strucure...
    release_handle(old);
    return es->peh;
}

/**
 * Helper function to convert an ENGINE_HANDLE* to a bucket engine pointer
 * without a cast
 */
static inline struct bucket_engine* get_handle(ENGINE_HANDLE* handle) {
    return (struct bucket_engine*)handle;
}

/**
 * Implementation of the the get_info function in the engine interface
 */
static const engine_info* bucket_get_info(ENGINE_HANDLE* handle) {
    return &(get_handle(handle)->info.engine_info);
}

/***********************************************************
 **       Implementation of functions used by genhash     **
 **********************************************************/

/**
 * Function used by genhash to check if two keys differ
 */
static int my_hash_eq(const void *k1, size_t nkey1,
                      const void *k2, size_t nkey2) {
    return nkey1 == nkey2 && memcmp(k1, k2, nkey1) == 0;
}

/**
 * Function used by genhash to create a copy of a key
 */
static void* hash_strdup(const void *k, size_t nkey) {
    void *rv = calloc(nkey, 1);
    assert(rv);
    memcpy(rv, k, nkey);
    return rv;
}

/**
 * Function used by genhash to create a copy of the value (this is
 * the proxied engine handle). We don't copy that value, instead
 * we increase the reference count.
 */
static void* refcount_dup(const void* ob, size_t vlen) {
    (void)vlen;
    proxied_engine_handle_t *peh = (proxied_engine_handle_t *)ob;
    assert(peh);
    int count = ATOMIC_INCR(&peh->refcount);
    assert(count > 0);
    return (void*)ob;
}

/**
 * Function used by genhash to release an object.
 */
static void engine_hash_free(void* ob) {
    proxied_engine_handle_t *peh = (proxied_engine_handle_t *)ob;
    assert(peh);
    release_handle(peh);
    peh->state = STATE_NULL;
}

/**
 * Try to load a shared object and create an engine.
 *
 * @param dlhandle The pointer to the loaded object (OUT). The caller is
 *                 responsible for calling dlcose() to release the resources
 *                 if the function succeeds.
 * @param soname The name of the shared object to load
 * @return A pointer to the created instance, or NULL if anything
 *         failed.
 */
static ENGINE_HANDLE *load_engine(void **dlhandle, const char *soname) {
    ENGINE_HANDLE *engine = NULL;
    /* Hack to remove the warning from C99 */
    union my_hack {
        CREATE_INSTANCE create;
        void* voidptr;
    } my_create = {.create = NULL };

    void *handle = dlopen(soname, RTLD_NOW | RTLD_LOCAL);
    if (handle == NULL) {
        const char *msg = dlerror();
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Failed to open library \"%s\": %s\n",
                    soname ? soname : "self",
                    msg ? msg : "unknown error");
        return NULL;
    }

    void *symbol = dlsym(handle, "create_instance");
    if (symbol == NULL) {
        logger->log(EXTENSION_LOG_WARNING, NULL,
                "Could not find symbol \"create_instance\" in %s: %s\n",
                soname ? soname : "self",
                dlerror());
        return NULL;
    }
    my_create.voidptr = symbol;

    /* request a instance with protocol version 1 */
    ENGINE_ERROR_CODE error = (*my_create.create)(1,
                                                  bucket_engine.get_server_api,
                                                  &engine);

    if (error != ENGINE_SUCCESS || engine == NULL) {
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Failed to create instance. Error code: %d\n", error);
        dlclose(handle);
        return NULL;
    }

    *dlhandle = handle;
    return engine;
}

/***********************************************************
 **  Implementation of callbacks from the memcached core  **
 **********************************************************/

/**
 * Handle the situation when a connection is disconnected
 * from the upstream. Propagate the command downstream and
 * release the allocated resources for the connection
 * unless it is reserved.
 *
 * @param cookie the cookie representing the connection that was closed
 * @param type The kind of event (should be ON_DISCONNECT)
 * @param event_data not used
 * @param cb_data The bucket instance in use
 */
static void handle_disconnect(const void *cookie,
                              ENGINE_EVENT_TYPE type,
                              const void *event_data,
                              const void *cb_data)
{
    assert(type == ON_DISCONNECT);
    struct bucket_engine *e = (struct bucket_engine*)cb_data;
    engine_specific_t *es;

    logger->log(EXTENSION_LOG_DETAIL, cookie,
                "Handle disconnect for: %p", cookie);
    es = e->upstream_server->cookie->get_engine_specific(cookie);
    if (es == NULL) {
        logger->log(EXTENSION_LOG_DETAIL, cookie,
                    "The connection is no longer known to bucket_engine: %p",
                    cookie);
        return;
    }
    assert(es);

    proxied_engine_handle_t *peh = es->peh;
    if (peh == NULL) {
        logger->log(EXTENSION_LOG_DETAIL, cookie,
                    "The connection is not connected to an engine %p", cookie);
        // Not attached to an engine!
        // Release the allocated memory, and clear the cookie data
        // upstream
        assert(es->reserved == ES_CONNECTED_FLAG);
        release_memory(es, sizeof(*es));
        e->upstream_server->cookie->store_engine_specific(cookie, NULL);
        return;
    }

    proxied_engine_handle_t *cb_peh;
    cb_peh = try_get_engine_handle((ENGINE_HANDLE *)e, cookie);

    bool do_callback = cb_peh != NULL && peh->wants_disconnects;
    if (do_callback) {
        logger->log(EXTENSION_LOG_DETAIL, NULL,
                    "Send disconnect call to engine %p cookie %p",
                    peh, cookie);
        peh->cb(cookie, type, event_data, peh->cb_data);
    }

    if (cb_peh != NULL) {
        release_engine_handle(cb_peh);
    }

    /*
     * We can't release the bucket engine yet, because the connection is
     * still reserved
     */
    if (es->reserved != ES_CONNECTED_FLAG) {
        logger->log(EXTENSION_LOG_DETAIL, cookie,
                    "We can't complete the shutdown due to reservations %p",
                    cookie);
        return;
    }

    logger->log(EXTENSION_LOG_DETAIL, cookie, "Complete the shutdown of %p",
                cookie);

    /* We don't expect concurrent calls to reserve because of
     * restriction that reserve can be only called from upcall. And
     * memcached will not upcall this while doing upcall for something
     * else (e.g. tap_notify or tap_itertator). */
    /* NOTE: that concurrent release is ok */
    int count = ATOMIC_ADD(&es->reserved, -ES_CONNECTED_FLAG);
    if (count == 0) {
        /* if we're last just clear this thing */
        // Release all the memory and clear the cookie data upstream.
        release_memory(es, sizeof(*es));
        e->upstream_server->cookie->store_engine_specific(cookie, NULL);
    }
    /* we now have one less connection holding reference to this peh.
     *
     * NOTE: we have es->peh still has this peh, and es->reserved now
     * guards peh 'alive'-dness so connection's engine-specific will
     * still not outlive peh. */
    release_handle(peh);
}

/**
 * Callback from the memcached core for a new connection. Associate
 * it with the default bucket (if it exists) and create an engine
 * specific structure.
 *
 * @param cookie the cookie representing the connection
 * @param type The kind of event (should be ON_CONNECT)
 * @param event_data not used
 * @param cb_data The bucket instance in use
 */
static void handle_connect(const void *cookie,
                           ENGINE_EVENT_TYPE type,
                           const void *event_data,
                           const void *cb_data) {
    assert(type == ON_CONNECT);
    (void)event_data;
    struct bucket_engine *e = (struct bucket_engine*)cb_data;

    proxied_engine_handle_t *peh = NULL;
    if (e->default_bucket_name != NULL) {
        // Assign a default named bucket (if there is one).
        peh = find_bucket(e->default_bucket_name);
        if (!peh && e->auto_create) {
            lock_engines();
            create_bucket_UNLOCKED(e, e->default_bucket_name,
                                   e->default_engine_path,
                                   e->default_bucket_config, &peh, NULL, 0);
            unlock_engines();
        }
    } else {
        // Assign the default bucket (if there is one).
        peh = e->default_engine.pe.v0 ? &e->default_engine : NULL;
        if (peh != NULL) {
            /* increment refcount because final release_handle will
             * decrement it */
            proxied_engine_handle_t *t = retain_handle(peh);
            assert(t == peh);
        }
    }

    create_engine_specific(e, cookie);
    set_engine_handle((ENGINE_HANDLE*)e, cookie, peh);
    release_handle(peh);
}

/**
 * Callback from the memcached core that a cookie succesfully
 * authenticated itself. Associate the cookie with the bucket it is
 * authenticated to.
 *
 * @param cookie the cookie representing the connection
 * @param type The kind of event (should be ON_AUTH)
 * @param event_data The authentication data
 * @param cb_data The bucket instance in use
 */
static void handle_auth(const void *cookie,
                        ENGINE_EVENT_TYPE type,
                        const void *event_data,
                        const void *cb_data) {
    assert(type == ON_AUTH);
    struct bucket_engine *e = (struct bucket_engine*)cb_data;

    const auth_data_t *auth_data = (const auth_data_t*)event_data;
    proxied_engine_handle_t *peh = find_bucket(auth_data->username);
    if (!peh && e->auto_create) {
        lock_engines();
        create_bucket_UNLOCKED(e, auth_data->username, e->default_engine_path,
                               auth_data->config ? auth_data->config : "", &peh, NULL, 0);
        unlock_engines();
    }
    set_engine_handle((ENGINE_HANDLE*)e, cookie, peh);
    release_handle(peh);
}

/**
 * Initialize the default bucket.
 */
static ENGINE_ERROR_CODE init_default_bucket(struct bucket_engine* se)
{
    ENGINE_ERROR_CODE ret;
    memset(&se->default_engine, 0, sizeof(se->default_engine));
    if ((ret = init_engine_handle(&se->default_engine, "",
                                  se->default_engine_path)) != ENGINE_SUCCESS) {
        return ret;
    }
    se->default_engine.pe.v0 = load_engine(&se->default_engine.dlhandle,
                                           se->default_engine_path);
    ENGINE_HANDLE_V1 *dv1 = (ENGINE_HANDLE_V1*)se->default_engine.pe.v0;
    if (!dv1) {
        return ENGINE_FAILED;
    }

    ret = dv1->initialize(se->default_engine.pe.v0, se->default_bucket_config);
    if (ret != ENGINE_SUCCESS) {
        dv1->destroy(se->default_engine.pe.v0, false);
    }

    return ret;
}

/**
 * This is the implementation of the "initialize" function in the engine
 * interface. It is called right after create_instance if memcached liked
 * the interface we returned. Perform all initialization and load the
 * default bucket (if specified in the config string).
 */
static ENGINE_ERROR_CODE bucket_initialize(ENGINE_HANDLE* handle,
                                           const char* config_str) {
    struct bucket_engine* se = get_handle(handle);
    assert(!se->initialized);

    char *tenv = getenv("MEMCACHED_TOP_KEYS");
    if (tenv != NULL) {
        se->topkeys = atoi(tenv);
        if (se->topkeys < 0) {
            se->topkeys = 0;
        }
    }

    get_current_time = bucket_engine.upstream_server->core->get_current_time;


#ifdef HAVE_PTHREAD_MUTEX_ERRORCHECK
    bucket_engine.mutexattr = &bucket_engine.mutexattr_storage;

    if (pthread_mutexattr_init(bucket_engine.mutexattr) != 0 ||
        pthread_mutexattr_settype(bucket_engine.mutexattr,
                                  PTHREAD_MUTEX_ERRORCHECK) != 0)
    {
        return ENGINE_FAILED;
    }
#else
    bucket_engine.mutexattr = NULL;
#endif

    if (pthread_mutex_init(&se->engines_mutex, bucket_engine.mutexattr) != 0) {
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "Error initializing mutex for bucket engine.\n");
        return ENGINE_FAILED;
    }

    ENGINE_ERROR_CODE ret = initialize_configuration(se, config_str);
    if (ret != ENGINE_SUCCESS) {
        return ret;
    }

    static struct hash_ops my_hash_ops = {
        .hashfunc = genhash_string_hash,
        .hasheq = my_hash_eq,
        .dupKey = hash_strdup,
        .dupValue = refcount_dup,
        .freeKey = free,
        .freeValue = engine_hash_free
    };

    se->engines = genhash_init(1, my_hash_ops);
    if (se->engines == NULL) {
        return ENGINE_ENOMEM;
    }

    se->upstream_server->callback->register_callback(handle, ON_CONNECT,
                                                     handle_connect, se);
    se->upstream_server->callback->register_callback(handle, ON_AUTH,
                                                     handle_auth, se);
    se->upstream_server->callback->register_callback(handle, ON_DISCONNECT,
                                                     handle_disconnect, se);

    // Initialization is useful to know if we *can* start up an
    // engine, but we check flags here to see if we should have and
    // shut it down if not.
    if (se->has_default) {
        if ((ret = init_default_bucket(se)) != ENGINE_SUCCESS) {
            genhash_free(se->engines);
            return ret;
        }
    }


    se->initialized = true;
    return ENGINE_SUCCESS;
}

/**
 * During normal shutdown we want to shut down all of the engines
 * cleanly. The bucket_shutdown_engine is an implementation of a
 * "genhash iterator", so it is called once for each engine
 * stored in the hash table.
 *
 * No client connections should be running during the invocation
 * of this function, so we don't have to check if there is any
 * threads currently calling into the engine.
 */
static void bucket_shutdown_engine(const void* key, size_t nkey,
                                   const void *val, size_t nval,
                                   void *args) {
    (void)key; (void)nkey; (void)nval; (void)args;
    const proxied_engine_handle_t *peh = val;
    if (peh->pe.v0) {
        logger->log(EXTENSION_LOG_INFO, NULL,
                    "Shutting down \"%s\"\n", peh->name);
        peh->pe.v1->destroy(peh->pe.v0, false);
        logger->log(EXTENSION_LOG_INFO, NULL,
                    "Completed shutdown of \"%s\"\n", peh->name);
    }
}

/**
 * This is the implementation of the "destroy" function in the engine
 * interface. It is called from memcached when memcached is shutting down,
 * and memcached will never again reference this object when the function
 * returns. Try to shut down all of the loaded engines cleanly.
 *
 * @todo we should probably pass the force variable down to the iterator.
 *       Right now the core will always specify false here, but that may
 *       change in the future...
 *
 */
static void bucket_destroy(ENGINE_HANDLE* handle,
                           const bool force) {
    (void)force;
    struct bucket_engine* se = get_handle(handle);

    if (!se->initialized) {
        return;
    }

    must_lock(&bucket_engine.shutdown.mutex);
    bucket_engine.shutdown.in_progress = true;
    /* kick bucket deletion threads in butt broadcasting in_progress = true condition */
    pthread_cond_broadcast(&bucket_engine.shutdown.refcount_cond);
    // Ensure that we don't race with another thread shutting down a bucket
    while (bucket_engine.shutdown.bucket_counter) {
        pthread_cond_wait(&bucket_engine.shutdown.cond,
                          &bucket_engine.shutdown.mutex);
    }
    must_unlock(&bucket_engine.shutdown.mutex);

    genhash_iter(se->engines, bucket_shutdown_engine, NULL);

    if (se->has_default) {
        uninit_engine_handle(&se->default_engine);
    }

    genhash_free(se->engines);
    se->engines = NULL;
    free(se->default_engine_path);
    se->default_engine_path = NULL;
    free(se->admin_user);
    se->admin_user = NULL;
    free(se->default_bucket_name);
    se->default_bucket_name = NULL;
    free(se->default_bucket_config);
    se->default_bucket_config = NULL;
    pthread_mutex_destroy(&se->engines_mutex);
    se->initialized = false;
}

/**
 * The deletion (shutdown) of a bucket is performed by its own thread
 * for simplicity (since we can't block the worker threads while we're
 * waiting for all of the connections to leave the engine).
 *
 * The state for the proxied_engine_handle should be "STOPPING" before
 * the thread is started, so that no new connections are allowed access
 * into the engine. Since we don't have any connections calling functions
 * into the engine we can safely start shutdown of the engine, but we can't
 * delete the proxied engine handle until all of the connections has
 * released their reference to the proxied engine handle.
 */
static void *engine_shutdown_thread(void *arg) {
    bool skip;
    // XXX:  Move state from STOPPED -> NULL.  This is an unbucket.
    must_lock(&bucket_engine.shutdown.mutex);
    skip = bucket_engine.shutdown.in_progress;
    if (!skip) {
        ++bucket_engine.shutdown.bucket_counter;
    }
    must_unlock(&bucket_engine.shutdown.mutex);

    if (skip) {
        // Skip shutdown because we're racing the global shutdown..
        return NULL;
    }

    proxied_engine_handle_t *peh = arg;
    logger->log(EXTENSION_LOG_INFO, NULL,
                "Started thread to shut down \"%s\"\n", peh->name);

    // Sanity check
    assert(peh->state == STATE_STOPPED);
    /*
     * Note we can check for peh->clients == 0 but that's not actually
     * right because get_engine_handle can temporarily increment it.
     */

    logger->log(EXTENSION_LOG_INFO, NULL,
                "Destroy engine \"%s\"\n", peh->name);
    peh->pe.v1->destroy(peh->pe.v0, peh->force_shutdown);
    logger->log(EXTENSION_LOG_INFO, NULL,
                "Engine \"%s\" destroyed\n", peh->name);

    peh->pe.v1 = NULL;

    // Unlink it from the engine table so that others may create
    // it while we're waiting for the remaining clients to disconnect
    logger->log(EXTENSION_LOG_INFO, NULL,
                "Unlink \"%s\" from engine table\n", peh->name);
    lock_engines();
    int upd = genhash_delete_all(bucket_engine.engines,
                                 peh->name, peh->name_len);
    assert(upd == 1);
    assert(genhash_find(bucket_engine.engines,
                        peh->name, peh->name_len) == NULL);
    unlock_engines();

    if (peh->cookie != NULL) {
        logger->log(EXTENSION_LOG_INFO, NULL,
                    "Notify %p that \"%s\" is deleted", peh->cookie, peh->name);
        bucket_engine.upstream_server->cookie->notify_io_complete(peh->cookie,
                                                                  ENGINE_SUCCESS);
    }

    /* NOTE: that even though DECR in release_handle happens without
     * lock, engine_shutdown_thread cannot miss wakeup event. That's
     * because broadcast happens under lock. Here's why.
     *
     * Suppose engine_shutdown_thread went to cond_wait sleep with
     * refcount = 0 and was never awaken (we want to prove by
     * contradiction that this cannot happen). But we know it have
     * observed refcount > 0. This means concurrent release_handle
     * decremented it after we've observed refcount value. But we know
     * that if this happened, release_handle would go and broadcast
     * signal. But our assumtion tells us we've missed this
     * broadcast. But this cannot happen because nobody can do
     * broadcast between us observing refcount value and going to
     * sleep because we're holding mutex that broadcast takes.
     */
    must_lock(&bucket_engine.shutdown.mutex);
    while (peh->refcount > 0 && !bucket_engine.shutdown.in_progress) {
        logger->log(EXTENSION_LOG_INFO, NULL,
                    "There are %d references to \"%s\".. waiting more\n",
                    peh->refcount, peh->name);

        pthread_cond_wait(&bucket_engine.shutdown.refcount_cond,
                          &bucket_engine.shutdown.mutex);
    }
    must_unlock(&bucket_engine.shutdown.mutex);

    logger->log(EXTENSION_LOG_INFO, NULL,
                "Release all resources for engine \"%s\"\n", peh->name);

    /* and free it */
    free_engine_handle(peh);

    must_lock(&bucket_engine.shutdown.mutex);
    --bucket_engine.shutdown.bucket_counter;
    if (bucket_engine.shutdown.in_progress && bucket_engine.shutdown.bucket_counter == 0){
        pthread_cond_signal(&bucket_engine.shutdown.cond);
    }
    must_unlock(&bucket_engine.shutdown.mutex);

    return NULL;
}

/**
 * Check to see if we should start shutdown of the specified engine. The
 * critera for starting shutdown is that no clients are currently calling
 * into the engine, and that someone requested shutdown of that engine.
 *
 * Note: we always call it with refcount protecting bucket from being
 * deleted under us.
 */
static void maybe_start_engine_shutdown(proxied_engine_handle_t *e) {
    assert(e->state == STATE_STOPPING || e->state == STATE_STOPPED || e->state == STATE_NULL);
    /* observing 'state' before clients == 0 is _crucial_. See
     * get_engine_handle. */
    if (e->state == STATE_STOPPING && e->clients == 0 && ATOMIC_CAS(&e->state, STATE_STOPPING, STATE_STOPPED)) {
        // Spin off a new thread to shut down the engine..
        pthread_attr_t attr;
        pthread_t tid;
        if (pthread_attr_init(&attr) != 0 ||
            pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED) != 0 ||
            pthread_create(&tid, &attr, engine_shutdown_thread, e) != 0)
        {
            logger->log(EXTENSION_LOG_WARNING, NULL,
                        "Failed to start shutdown of \"%s\"!", e->name);
            abort();
        }
        pthread_attr_destroy(&attr);
    }
}

/**
 * Implementation of the "item_allocate" function in the engine
 * specification. Look up the correct engine and call into the
 * underlying engine if the underlying engine is "running". Disconnect
 * the caller if the engine isn't "running" anymore.
 */
static ENGINE_ERROR_CODE bucket_item_allocate(ENGINE_HANDLE* handle,
                                              const void* cookie,
                                              item **itm,
                                              const void* key,
                                              const size_t nkey,
                                              const size_t nbytes,
                                              const int flags,
                                              const rel_time_t exptime) {

    proxied_engine_handle_t *peh = get_engine_handle(handle, cookie);
    if (peh != NULL) {
        ENGINE_ERROR_CODE ret;
        ret = peh->pe.v1->allocate(peh->pe.v0, cookie, itm, key,
                                   nkey, nbytes, flags, exptime);
        release_engine_handle(peh);
        return ret;
    } else {
        return ENGINE_DISCONNECT;
    }
}

/**
 * Implementation of the "item_delete" function in the engine
 * specification. Look up the correct engine and call into the
 * underlying engine if the underlying engine is "running". Disconnect
 * the caller if the engine isn't "running" anymore.
 */
static ENGINE_ERROR_CODE bucket_item_delete(ENGINE_HANDLE* handle,
                                            const void* cookie,
                                            const void* key,
                                            const size_t nkey,
                                            uint64_t* cas,
                                            uint16_t vbucket) {
    proxied_engine_handle_t *peh = get_engine_handle(handle, cookie);
    if (peh) {
        ENGINE_ERROR_CODE ret;
        ret = peh->pe.v1->remove(peh->pe.v0, cookie, key, nkey, cas, vbucket);
        release_engine_handle(peh);

        if (ret == ENGINE_SUCCESS) {
            TK(peh->topkeys, delete_hits, key, nkey, get_current_time());
        } else if (ret == ENGINE_KEY_ENOENT) {
            TK(peh->topkeys, delete_misses, key, nkey, get_current_time());
        } else if (ret == ENGINE_KEY_EEXISTS) {
            TK(peh->topkeys, cas_badval, key, nkey, get_current_time());
        }

        return ret;
    } else {
        return ENGINE_DISCONNECT;
    }
}

/**
 * Implementation of the "item_release" function in the engine
 * specification. Look up the correct engine and call into the
 * underlying engine if the underlying engine is "running".
 */
static void bucket_item_release(ENGINE_HANDLE* handle,
                                const void *cookie,
                                item* itm) {
    proxied_engine_handle_t *peh = try_get_engine_handle(handle, cookie);
    if (peh) {
        peh->pe.v1->release(peh->pe.v0, cookie, itm);
        release_engine_handle(peh);
    } else {
        logger->log(EXTENSION_LOG_DEBUG, NULL,
                    "Potential memory leak. Failed to get engine handle for %p",
                    cookie);
    }
}

/**
 * Implementation of the "get" function in the engine
 * specification. Look up the correct engine and call into the
 * underlying engine if the underlying engine is "running". Disconnect
 * the caller if the engine isn't "running" anymore.
 */
static ENGINE_ERROR_CODE bucket_get(ENGINE_HANDLE* handle,
                                    const void* cookie,
                                    item** itm,
                                    const void* key,
                                    const int nkey,
                                    uint16_t vbucket) {
    proxied_engine_handle_t *peh = get_engine_handle(handle, cookie);
    if (peh) {
        ENGINE_ERROR_CODE ret;
        ret = peh->pe.v1->get(peh->pe.v0, cookie, itm, key, nkey, vbucket);

        if (ret == ENGINE_SUCCESS) {
            TK(peh->topkeys, get_hits, key, nkey, get_current_time());
        } else if (ret == ENGINE_KEY_ENOENT) {
            TK(peh->topkeys, get_misses, key, nkey, get_current_time());
        }

        release_engine_handle(peh);
        return ret;
    } else {
        return ENGINE_DISCONNECT;
    }
}

static void add_engine(const void *key, size_t nkey,
                       const void *val, size_t nval,
                       void *arg) {
    (void)nval;
    struct bucket_list **blist_ptr = (struct bucket_list **)arg;
    struct bucket_list *n = calloc(sizeof(struct bucket_list), 1);
    n->name = (char*)key;
    n->namelen = nkey;
    n->peh = (proxied_engine_handle_t*) val;
    assert(n->peh);

    /* we must not leak dead buckets outside of engines_mutex. Those
     * can be freed by bucket destructor at any time (when
     * engines_mutex is not held) */
    if (retain_handle(n->peh) == NULL) {
        free(n);
        return;
    }

    n->next = *blist_ptr;
    *blist_ptr = n;
}

static bool list_buckets(struct bucket_engine *e, struct bucket_list **blist) {
    lock_engines();
    genhash_iter(e->engines, add_engine, blist);
    unlock_engines();
    return true;
}

static void bucket_list_free(struct bucket_list *blist) {
    struct bucket_list *p = blist;
    while (p) {
        release_handle(p->peh);
        struct bucket_list *tmp = p->next;
        free(p);
        p = tmp;
    }
}

/**
 * Implementation of the "aggregate_stats" function in the engine
 * specification. Look up the correct engine and call into the
 * underlying engine if the underlying engine is "running". Disconnect
 * the caller if the engine isn't "running" anymore.
 */
static ENGINE_ERROR_CODE bucket_aggregate_stats(ENGINE_HANDLE* handle,
                                                const void* cookie,
                                                void (*callback)(void*, void*),
                                                void *stats) {
    (void)cookie;
    struct bucket_engine *e = (struct bucket_engine*)handle;
    struct bucket_list *blist = NULL;
    if (! list_buckets(e, &blist)) {
        return ENGINE_FAILED;
    }

    struct bucket_list *p = blist;
    while (p) {
        callback(p->peh->stats, stats);
        p = p->next;
    }

    bucket_list_free(blist);
    return ENGINE_SUCCESS;
}

struct stat_context {
    ADD_STAT add_stat;
    const void *cookie;
};

static void stat_ht_builder(const void *key, size_t nkey,
                            const void *val, size_t nval,
                            void *arg) {
    (void)nval;
    assert(arg);
    struct stat_context *ctx = (struct stat_context*)arg;
    proxied_engine_handle_t *bucket = (proxied_engine_handle_t*)val;
    const char * const bucketState = bucket_state_name(bucket->state);
    ctx->add_stat(key, nkey, bucketState, strlen(bucketState),
                  ctx->cookie);
}

/**
 * Get bucket-engine specific statistics
 */
static ENGINE_ERROR_CODE get_bucket_stats(ENGINE_HANDLE* handle,
                                          const void *cookie,
                                          ADD_STAT add_stat) {

    if (!is_authorized(handle, cookie)) {
        return ENGINE_FAILED;
    }

    struct bucket_engine *e = (struct bucket_engine*)handle;
    struct stat_context sctx = {.add_stat = add_stat, .cookie = cookie};

    lock_engines();
    genhash_iter(e->engines, stat_ht_builder, &sctx);
    unlock_engines();
    return ENGINE_SUCCESS;
}

/**
 * Implementation of the "get_stats" function in the engine
 * specification. Look up the correct engine and call into the
 * underlying engine if the underlying engine is "running". Disconnect
 * the caller if the engine isn't "running" anymore.
 */
static ENGINE_ERROR_CODE bucket_get_stats(ENGINE_HANDLE* handle,
                                          const void* cookie,
                                          const char* stat_key,
                                          int nkey,
                                          ADD_STAT add_stat) {
    // Intercept bucket stats.
    if (nkey == (sizeof("bucket") - 1) &&
        memcmp("bucket", stat_key, nkey) == 0) {
        return get_bucket_stats(handle, cookie, add_stat);
    }

    ENGINE_ERROR_CODE rc = ENGINE_DISCONNECT;
    proxied_engine_handle_t *peh = get_engine_handle(handle, cookie);

    if (peh) {
        if (nkey == (sizeof("topkeys") - 1) &&
            memcmp("topkeys", stat_key, nkey) == 0) {
            rc = topkeys_stats(peh->topkeys, TK_SHARDS, cookie, get_current_time(),
                               add_stat);
        } else {
            rc = peh->pe.v1->get_stats(peh->pe.v0, cookie, stat_key,
                                       nkey, add_stat);
            if (nkey == 0) {
                char statval[20];
                snprintf(statval, sizeof(statval), "%d", peh->refcount - 1);
                add_stat("bucket_conns", sizeof("bucket_conns") - 1, statval,
                         strlen(statval), cookie);
                snprintf(statval, sizeof(statval), "%d", peh->clients);
                add_stat("bucket_active_conns", sizeof("bucket_active_conns") -1,
                         statval, strlen(statval), cookie);
            }
        }
        release_engine_handle(peh);
    }
    return rc;
}

/**
 * Implementation of the "get_stats_struct" function in the engine
 * specification. Look up the correct engine and and verify it's
 * state.
 */
static void *bucket_get_stats_struct(ENGINE_HANDLE* handle,
                                     const void* cookie)
{
    void *ret = NULL;
    proxied_engine_handle_t *peh = try_get_engine_handle(handle, cookie);
    if (peh) {
        ret = peh->stats;
        release_engine_handle(peh);
    }

    return ret;
}

/**
 * Implementation of the "store" function in the engine
 * specification. Look up the correct engine and call into the
 * underlying engine if the underlying engine is "running". Disconnect
 * the caller if the engine isn't "running" anymore.
 */
static ENGINE_ERROR_CODE bucket_store(ENGINE_HANDLE* handle,
                                      const void *cookie,
                                      item* itm,
                                      uint64_t *cas,
                                      ENGINE_STORE_OPERATION operation,
                                      uint16_t vbucket) {
    proxied_engine_handle_t *peh = get_engine_handle(handle, cookie);
    if (peh) {
        ENGINE_ERROR_CODE ret;
        ret = peh->pe.v1->store(peh->pe.v0, cookie, itm, cas, operation, vbucket);
        if (ret != ENGINE_EWOULDBLOCK && peh->topkeys) {
            item_info itm_info = { .nvalue = 1 };
            if (peh->pe.v1->get_item_info(peh->pe.v0, cookie, itm, &itm_info)) {
                const void* key = itm_info.key;
                const int nkey = itm_info.nkey;

                if (operation != OPERATION_CAS) {
                    TK(peh->topkeys, cmd_set, key, nkey, get_current_time());
                } else {
                    if (ret == ENGINE_SUCCESS) {
                        TK(peh->topkeys, cas_hits, key, nkey,
                           get_current_time());
                    } else if (ret == ENGINE_KEY_EEXISTS) {
                        TK(peh->topkeys, cas_badval, key, nkey,
                           get_current_time());
                    } else if (ret == ENGINE_KEY_ENOENT) {
                        TK(peh->topkeys, cas_misses, key, nkey,
                           get_current_time());
                    }
                }
            }
        }
        release_engine_handle(peh);
        return ret;
    } else {
        return ENGINE_DISCONNECT;
    }
}

/**
 * Implementation of the "arithmetic" function in the engine
 * specification. Look up the correct engine and call into the
 * underlying engine if the underlying engine is "running". Disconnect
 * the caller if the engine isn't "running" anymore.
 */
static ENGINE_ERROR_CODE bucket_arithmetic(ENGINE_HANDLE* handle,
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
    proxied_engine_handle_t *peh = get_engine_handle(handle, cookie);
    if (peh) {
        ENGINE_ERROR_CODE ret;
        ret = peh->pe.v1->arithmetic(peh->pe.v0, cookie, key, nkey,
                                increment, create, delta, initial,
                                exptime, cas, result, vbucket);


        if (ret == ENGINE_SUCCESS) {
            if (increment) {
                TK(peh->topkeys, incr_hits, key, nkey, get_current_time());
            } else {
                TK(peh->topkeys, decr_hits, key, nkey, get_current_time());

            }
        } else if (ret == ENGINE_KEY_ENOENT) {
            if (increment) {
                TK(peh->topkeys, incr_misses, key, nkey, get_current_time());
            } else {
                TK(peh->topkeys, decr_misses, key, nkey, get_current_time());

            }
        }

        release_engine_handle(peh);
        return ret;
    } else {
        return ENGINE_DISCONNECT;
    }
}

/**
 * Implementation of the "flush" function in the engine
 * specification. Look up the correct engine and call into the
 * underlying engine if the underlying engine is "running". Disconnect
 * the caller if the engine isn't "running" anymore.
 */
static ENGINE_ERROR_CODE bucket_flush(ENGINE_HANDLE* handle,
                                      const void* cookie, time_t when) {
    proxied_engine_handle_t *peh = get_engine_handle(handle, cookie);
    if (peh) {
        ENGINE_ERROR_CODE ret;
        ret = peh->pe.v1->flush(peh->pe.v0, cookie, when);
        release_engine_handle(peh);
        return ret;
    } else {
        return ENGINE_DISCONNECT;
    }
}

/**
 * Implementation of the "reset_stats" function in the engine
 * specification. Look up the correct engine and call into the
 * underlying engine if the underlying engine is "running".
 */
static void bucket_reset_stats(ENGINE_HANDLE* handle, const void *cookie) {
    proxied_engine_handle_t *peh = try_get_engine_handle(handle, cookie);
    if (peh) {
        peh->pe.v1->reset_stats(peh->pe.v0, cookie);
        release_engine_handle(peh);
    }
}

/**
 * Implementation of the "get_item_info" function in the engine
 * specification. Look up the correct engine and call into the
 * underlying engine if the underlying engine is "running".
 */
static bool bucket_get_item_info(ENGINE_HANDLE *handle,
                                 const void *cookie,
                                 const item* itm,
                                 item_info *itm_info) {
    bool ret = false;
    proxied_engine_handle_t *peh = try_get_engine_handle(handle, cookie);
    if (peh) {
        ret = peh->pe.v1->get_item_info(peh->pe.v0, cookie, itm, itm_info);
        release_engine_handle(peh);
    }

    return ret;
}

/**
 * Implementation of the "item_set_cas" function in the engine
 * specification. Look up the correct engine and call into the
 * underlying engine if the underlying engine is "running".
 */
static void bucket_item_set_cas(ENGINE_HANDLE *handle, const void *cookie,
                                item *itm, uint64_t cas) {

    proxied_engine_handle_t *peh = try_get_engine_handle(handle, cookie);
    if (peh) {
        peh->pe.v1->item_set_cas(peh->pe.v0, cookie, itm, cas);
        release_engine_handle(peh);
    } else {
        logger->log(EXTENSION_LOG_WARNING, NULL,
                    "The engine is no longer there... %p", cookie);
    }
}

/**
 * Implenentation of the tap notify in the bucket engine. Verify
 * that the bucket exists (and is in the correct state) before
 * wrapping into the engines implementationof tap notify.
 */
static ENGINE_ERROR_CODE bucket_tap_notify(ENGINE_HANDLE* handle,
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
    proxied_engine_handle_t *peh = get_engine_handle(handle, cookie);
    if (peh) {
        ENGINE_ERROR_CODE ret;
        ret = peh->pe.v1->tap_notify(peh->pe.v0, cookie, engine_specific,
                                nengine, ttl, tap_flags, tap_event, tap_seqno,
                                key, nkey, flags, exptime, cas, data, ndata,
                                vbucket);
        release_engine_handle(peh);
        return ret;
    } else {
        return ENGINE_DISCONNECT;
    }
}

/**
 * A specialized tap iterator that verifies that the bucket it is
 * connected to actually exists and is in the correct state before
 * calling into the engine.
 */
static tap_event_t bucket_tap_iterator_shim(ENGINE_HANDLE* handle,
                                            const void *cookie,
                                            item **itm,
                                            void **engine_specific,
                                            uint16_t *nengine_specific,
                                            uint8_t *ttl,
                                            uint16_t *flags,
                                            uint32_t *seqno,
                                            uint16_t *vbucket) {
    proxied_engine_handle_t *e = get_engine_handle(handle, cookie);
    if (e && e->tap_iterator) {
        assert(e->pe.v0 != handle);
        tap_event_t ret;
        ret = e->tap_iterator(e->pe.v0, cookie, itm,
                              engine_specific, nengine_specific,
                              ttl, flags, seqno, vbucket);


        release_engine_handle(e);
        return ret;
    } else {
        return TAP_DISCONNECT;
    }
}

/**
 * Implementation of the get_tap_iterator from the engine API.
 * If the cookie is associated with an engine who supports a tap
 * iterator we should return the internal shim iterator so that we
 * verify access every time we try to iterate.
 */
static TAP_ITERATOR bucket_get_tap_iterator(ENGINE_HANDLE* handle, const void* cookie,
                                            const void* client, size_t nclient,
                                            uint32_t flags,
                                            const void* userdata, size_t nuserdata) {
    TAP_ITERATOR ret = NULL;

    proxied_engine_handle_t *e = get_engine_handle(handle, cookie);
    if (e) {
        if (!e->tap_iterator_disabled) {
            e->tap_iterator = e->pe.v1->get_tap_iterator(e->pe.v0, cookie,
                                                         client, nclient,
                                                         flags, userdata, nuserdata);
            ret = e->tap_iterator ? bucket_tap_iterator_shim : NULL;
        }
        release_engine_handle(e);
    }

    return ret;
}


/**
 * Implementation of the errinfo function in the engine api.
 * If the cookie is connected to an engine should proxy the function down
 * into the engine
 */
static size_t bucket_errinfo(ENGINE_HANDLE *handle, const void* cookie,
                             char *buffer, size_t buffsz) {
    proxied_engine_handle_t *peh = try_get_engine_handle(handle, cookie);
    size_t ret = 0;

    if (peh) {
        if (peh->pe.v1->errinfo) {
            ret = peh->pe.v1->errinfo(peh->pe.v0, cookie, buffer, buffsz);
        }
        release_engine_handle(peh);
    }

    return ret;
}

/**
 * Initialize configuration is called during the initialization of
 * bucket_engine. It tries to parse the configuration string to pick
 * out the legal configuration options, and store them in the
 * one and only instance of bucket_engine.
 */
static ENGINE_ERROR_CODE initialize_configuration(struct bucket_engine *me,
                                                  const char *cfg_str) {
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    me->auto_create = true;

    if (cfg_str != NULL) {
        struct config_item items[] = {
            { .key = "engine",
              .datatype = DT_STRING,
              .value.dt_string = &me->default_engine_path },
            { .key = "admin",
              .datatype = DT_STRING,
              .value.dt_string = &me->admin_user },
            { .key = "default",
              .datatype = DT_BOOL,
              .value.dt_bool = &me->has_default },
            { .key = "default_bucket_name",
              .datatype = DT_STRING,
              .value.dt_string = &me->default_bucket_name },
            { .key = "default_bucket_config",
              .datatype = DT_STRING,
              .value.dt_string = &me->default_bucket_config },
            { .key = "auto_create",
              .datatype = DT_BOOL,
              .value.dt_bool = &me->auto_create },
            { .key = "config_file",
              .datatype = DT_CONFIGFILE },
            { .key = NULL}
        };

        int r = me->upstream_server->core->parse_config(cfg_str, items, stderr);
        if (r == 0) {
            if (!items[0].found) {
                me->default_engine_path = NULL;
            }
            if (!items[1].found) {
                me->admin_user = NULL;
            }
            if (!items[3].found) {
                me->default_bucket_name = NULL;
            }
            if (!items[4].found) {
                me->default_bucket_config = strdup("");
            }
        } else {
            ret = ENGINE_FAILED;
        }
    }

    return ret;
}

/***********************************************************
 ** Implementation of the bucket-engine specific commands **
 **********************************************************/

/**
 * EXTRACT_KEY is a small helper macro that creates a character array
 * containing a zero-terminated version of the key in the buffer.
 */
#define EXTRACT_KEY(req, out)                                       \
    protocol_binary_request_no_extras *myptr = (void*)req;          \
    char out[ntohs(myptr->message.header.request.keylen) + 1];      \
    memcpy(out, ((char*)request) + sizeof(myptr->message.header) +  \
                myptr->message.header.request.extlen,               \
           ntohs(myptr->message.header.request.keylen));            \
    out[ntohs(myptr->message.header.request.keylen)] = 0x00;

/**
 * Implementation of the "CREATE" command.
 */
static ENGINE_ERROR_CODE handle_create_bucket(ENGINE_HANDLE* handle,
                                              const void* cookie,
                                              protocol_binary_request_header *request,
                                              ADD_RESPONSE response) {
    struct bucket_engine *e = (void*)handle;
    protocol_binary_request_create_bucket *breq = (void*)request;

    EXTRACT_KEY(breq, keyz);

    size_t bodylen = ntohl(breq->message.header.request.bodylen)
        - ntohs(breq->message.header.request.keylen);

    if (bodylen >= (1 << 16)) { // 64k ought to be enough for anybody
        return ENGINE_DISCONNECT;
    }

    char spec[bodylen + 1];
    memcpy(spec, ((char*)request) + sizeof(breq->message.header)
           + ntohs(breq->message.header.request.keylen), bodylen);
    spec[bodylen] = 0x00;

    if (spec[0] == 0) {
        const char *msg = "Invalid request.";
        response(msg, strlen(msg), "", 0, "", 0, 0,
                 PROTOCOL_BINARY_RESPONSE_EINVAL, 0, cookie);
        return ENGINE_SUCCESS;
    }
    char *config = "";
    if (strlen(spec) < bodylen) {
        config = spec + strlen(spec)+1;
    }

    const size_t msglen = 1024;
    char msg[msglen];
    msg[0] = 0;
    lock_engines();
    ENGINE_ERROR_CODE ret = create_bucket_UNLOCKED(e, keyz, spec, config,
                                                   NULL, msg, msglen);
    unlock_engines();

    protocol_binary_response_status rc;
    switch(ret) {
    case ENGINE_SUCCESS:
        rc = PROTOCOL_BINARY_RESPONSE_SUCCESS;
        break;
    case ENGINE_KEY_EEXISTS:
        rc = PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS;
        break;
    default:
        rc = PROTOCOL_BINARY_RESPONSE_NOT_STORED;
    }

    response(NULL, 0, NULL, 0, msg, strlen(msg), 0, rc, 0, cookie);

    return ENGINE_SUCCESS;
}

/**
 * Implementation of the "DELETE" command. The delete command shuts down
 * the engine and waits for it's termination before sending the response
 * back to the caller. The user may specify if we should run a gracefull
 * shutdown (let the engine persist everything etc), or if it should
 * just stop as fast as possible. Please note that bucket_engine can only
 * notify the engine about this, because we need to wait until the engine
 * reports that it is done (otherwise it may still have threads running
 * etc).
 *
 * We can't block the client thread while waiting for the engine to shut
 * down, so instead we store the pointer to the request in the user-specific
 * data section to preserve the information before we return EWOULDBLOCK
 * back to the client.
 */
static ENGINE_ERROR_CODE handle_delete_bucket(ENGINE_HANDLE* handle,
                                              const void* cookie,
                                              protocol_binary_request_header *request,
                                              ADD_RESPONSE response) {
    (void)handle;
    void *userdata = bucket_get_engine_specific(cookie);
    if (userdata == NULL) {
        protocol_binary_request_delete_bucket *breq = (void*)request;

        EXTRACT_KEY(breq, keyz);

        size_t bodylen = ntohl(breq->message.header.request.bodylen)
            - ntohs(breq->message.header.request.keylen);
        if (bodylen >= (1 << 16)) {
            return ENGINE_DISCONNECT;
        }
        char config[bodylen + 1];
        memcpy(config, ((char*)request) + sizeof(breq->message.header)
               + ntohs(breq->message.header.request.keylen), bodylen);
        config[bodylen] = 0x00;

        bool force = false;
        if (config[0] != 0) {
            struct config_item items[2] = {
                {.key = "force",
                 .datatype = DT_BOOL,
                 .value.dt_bool = &force},
                {.key = NULL}
            };

            if (bucket_get_server_api()->core->parse_config(config, items,
                                                            stderr) != 0) {
                const char *msg = "Invalid config parameters";
                response(msg, strlen(msg), "", 0, "", 0, 0,
                         PROTOCOL_BINARY_RESPONSE_EINVAL, 0, cookie);
                return ENGINE_SUCCESS;
            }
        }

        bool found = false;
        proxied_engine_handle_t *peh = find_bucket(keyz);

        if (peh) {
            /* bumped clients count protects transition from
             * STATE_RUNNING to STATE_STOPPED while peh->cookie is not
             * yet set. */
            int count = ATOMIC_INCR(&peh->clients);
            assert(count > 0);
            if (ATOMIC_CAS(&peh->state, STATE_RUNNING, STATE_STOPPING)) {
                peh->cookie = cookie;
                found = true;
                peh->force_shutdown = force;
            }
            /* it'll decrement clients and also initiate bucket
             * shutdown when there are no active clients */
            release_engine_handle(peh);

            // If we're deleting the bucket we're connected to we need
            // to disconnect from the bucket in order to avoid trying
            // to grab it after it is released (since we're dropping)
            // the reference
            engine_specific_t *es;
            es = bucket_engine.upstream_server->cookie->get_engine_specific(cookie);
            assert(es);
            if (es->peh == peh) {
                set_engine_handle(handle, cookie, NULL);
            }

            // and drop reference from find_bucket
            release_handle(peh);
        }

        if (found) {
            bucket_store_engine_specific(cookie, breq);
            return ENGINE_EWOULDBLOCK;
        } else {
            const char *msg = "Not found.";
            response(NULL, 0, NULL, 0, msg, strlen(msg),
                     0, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT,
                     0, cookie);
        }
    } else {
        bucket_store_engine_specific(cookie, NULL);
        response(NULL, 0, NULL, 0, NULL, 0, 0,
                 PROTOCOL_BINARY_RESPONSE_SUCCESS, 0, cookie);
    }

    return ENGINE_SUCCESS;
}

/**
 * Implementation of the "LIST" command. This command returns a single
 * packet with the names of all the buckets separated by the space
 * character.
 */
static ENGINE_ERROR_CODE handle_list_buckets(ENGINE_HANDLE* handle,
                                             const void* cookie,
                                             protocol_binary_request_header *request,
                                             ADD_RESPONSE response) {
    (void)request;
    struct bucket_engine *e = (struct bucket_engine*)handle;

    // Accumulate the current bucket list.
    struct bucket_list *blist = NULL;
    if (! list_buckets(e, &blist)) {
        return ENGINE_FAILED;
    }

    int len = 0, n = 0;
    struct bucket_list *p = blist;
    while (p) {
        len += p->namelen;
        n++;
        p = p->next;
    }

    // Now turn it into a space-separated list.
    char *blist_txt = calloc(sizeof(char), n + len);
    assert(blist_txt);
    p = blist;
    while (p) {
        strncat(blist_txt, p->name, p->namelen);
        if (p->next) {
            strcat(blist_txt, " ");
        }
        p = p->next;
    }

    bucket_list_free(blist);

    // Response body will be "" in the case of an empty response.
    // Otherwise, it needs to account for the trailing space of the
    // above append code.
    response(NULL, 0, NULL, 0, blist_txt,
             n == 0 ? 0 : (sizeof(char) * n + len) - 1,
             0, PROTOCOL_BINARY_RESPONSE_SUCCESS, 0, cookie);
    free(blist_txt);

    return ENGINE_SUCCESS;
}

/**
 * Implementation of the "SELECT" command. The SELECT command associates
 * the cookie with the named bucket.
 */
static ENGINE_ERROR_CODE handle_select_bucket(ENGINE_HANDLE* handle,
                                              const void* cookie,
                                              protocol_binary_request_header *request,
                                              ADD_RESPONSE response) {
    protocol_binary_request_select_bucket *breq = (void*)request;

    EXTRACT_KEY(breq, keyz);

    proxied_engine_handle_t *proxied = find_bucket(keyz);
    set_engine_handle(handle, cookie, proxied);
    release_handle(proxied);

    if (proxied) {
        response(NULL, 0, NULL, 0, NULL, 0, 0,
                 PROTOCOL_BINARY_RESPONSE_SUCCESS, 0, cookie);
    } else {
        const char *msg = "Engine not found";
        response(NULL, 0, NULL, 0, msg, strlen(msg), 0,
                 PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0, cookie);
    }

    return ENGINE_SUCCESS;
}

/**
 * Check if a command opcode is one of the commands bucket_engine
 * implements. Bucket_engine used command opcodes from the reserved range
 * earlier, so in order to preserve backward compatibility we currently
 * accept both. We should however drop the deprecated ones for the
 * next release.
 */
static inline bool is_admin_command(uint8_t opcode) {
    switch (opcode) {
    case CREATE_BUCKET:
    case CREATE_BUCKET_DEPRECATED:
    case DELETE_BUCKET:
    case DELETE_BUCKET_DEPRECATED:
    case LIST_BUCKETS:
    case LIST_BUCKETS_DEPRECATED:
    case SELECT_BUCKET:
    case SELECT_BUCKET_DEPRECATED:
        return true;
    default:
        return false;
    }
}

/**
 * Check to see if this cookie is authorized as the admin user
 */
static bool is_authorized(ENGINE_HANDLE* handle, const void* cookie) {
    // During testing you might want to skip the auth phase...
    if (getenv("BUCKET_ENGINE_DIABLE_AUTH_PHASE") != NULL) {
        return true;
    }

    struct bucket_engine *e = (struct bucket_engine*)handle;
    bool rv = false;
    if (e->admin_user) {
        auth_data_t data = {.username = 0, .config = 0};
        e->upstream_server->cookie->get_auth_data(cookie, &data);
        if (data.username) {
            rv = strcmp(data.username, e->admin_user) == 0;
        }
    }
    return rv;
}

/* We know some of the commands inside ep-engine, so let's go ahead
 * and update the topkeys for them. We don't want flush the topkeys
 * cache for erronous requests from these, so ignore all misses etc
 */
static void update_topkey_command( proxied_engine_handle_t *peh,
                                   protocol_binary_request_header *request,
                                   ENGINE_ERROR_CODE rv)
{
    if (request->request.keylen == 0 || rv != ENGINE_SUCCESS) {
        return ;
    }

    uint16_t nkey = ntohs(request->request.keylen);
    EXTRACT_KEY(((protocol_binary_request_no_extras*)request), keyz);
    const void* key = keyz;

    switch (request->request.opcode) {
    case CMD_GET_REPLICA:
        TK(peh->topkeys, get_replica, key, nkey, get_current_time());
        break;
    case CMD_EVICT_KEY:
        TK(peh->topkeys, evict, key, nkey, get_current_time());
        break;
    case CMD_GET_LOCKED:
        TK(peh->topkeys, getl, key, nkey, get_current_time());
        break;
    case CMD_UNLOCK_KEY:
        TK(peh->topkeys, unlock, key, nkey, get_current_time());
        break;
    case CMD_GET_META:
    case CMD_GETQ_META:
        TK(peh->topkeys, get_meta, key, nkey, get_current_time());
        break;
    case CMD_SET_WITH_META:
    case CMD_SETQ_WITH_META:
        TK(peh->topkeys, set_meta, key, nkey, get_current_time());
        break;
    case CMD_DEL_WITH_META:
    case CMD_DELQ_WITH_META:
        TK(peh->topkeys, del_meta, key, nkey, get_current_time());
        break;
    }
}

/**
 * Handle one of the "engine-specific" commands. Bucket-engine itself
 * implements a small subset of commands, but the user needs to be
 * authorized in order to execute them. All the other commands
 * are proxied to the underlying engine.
 */
static ENGINE_ERROR_CODE bucket_unknown_command(ENGINE_HANDLE* handle,
                                                const void* cookie,
                                                protocol_binary_request_header *request,
                                                ADD_RESPONSE response)
{
    ENGINE_ERROR_CODE rv = ENGINE_ENOTSUP;
    if (is_admin_command(request->request.opcode)) {
        if (is_authorized(handle, cookie)) {
            switch(request->request.opcode) {
            case CREATE_BUCKET:
            case CREATE_BUCKET_DEPRECATED:
                rv = handle_create_bucket(handle, cookie, request, response);
                break;
            case DELETE_BUCKET:
            case DELETE_BUCKET_DEPRECATED:
                rv = handle_delete_bucket(handle, cookie, request, response);
                break;
            case LIST_BUCKETS:
            case LIST_BUCKETS_DEPRECATED:
                rv = handle_list_buckets(handle, cookie, request, response);
                break;
            case SELECT_BUCKET:
            case SELECT_BUCKET_DEPRECATED:
                rv = handle_select_bucket(handle, cookie, request, response);
                break;
            default:
                assert(false);
            }
        }
    } else {
        proxied_engine_handle_t *peh = get_engine_handle(handle, cookie);
        if (peh) {
            rv = peh->pe.v1->unknown_command(peh->pe.v0, cookie, request,
                                             response);
            update_topkey_command(peh, request, rv);
            release_engine_handle(peh);
        } else {
            rv = ENGINE_DISCONNECT;
        }
    }

    return rv;
}

/**
 * Notify bucket_engine that we want to reserve this cookie. That
 * means that bucket_engine and memcached can't release the resources
 * associated with the cookie until the downstream engine release it
 * by calling bucket_engine_release_cookie.
 *
 * @param cookie the cookie to reserve
 * @return ENGINE_SUCCESS upon success
 */
static ENGINE_ERROR_CODE bucket_engine_reserve_cookie(const void *cookie)
{
    ENGINE_ERROR_CODE ret;
    engine_specific_t *es;
    es = bucket_engine.upstream_server->cookie->get_engine_specific(cookie);

    assert(es != NULL);

    proxied_engine_handle_t *peh = es->peh;
    if (peh == NULL) {
        // The connection hasn't selected an engine, so use
        // the default engine.
        if (bucket_engine.default_engine.pe.v0 != NULL) {
            peh = &bucket_engine.default_engine;
        } else {
            return ENGINE_FAILED;
        }
    }

    /* This can only be reliably called form engine up-call so that
     * it's impossible to transition to STATE_STOPPED while we're
     * here. */
    assert(peh->clients >= 0);

    if (peh->state != STATE_RUNNING) {
        return ENGINE_FAILED;
    }

    // Reserve the cookie upstream as well
    ret = upstream_reserve_cookie(cookie);
    if (ret != ENGINE_SUCCESS) {
        return ret;
    }

    int count = ATOMIC_INCR(&peh->refcount);
    assert(count > 0);
    count = ATOMIC_INCR(&es->reserved);
    assert(count > 0);

    return ENGINE_SUCCESS;
}

/**
 * Release the the cookie from the underlying system, and allow the upstream
 * to release all resources allocated together with the cookie. The caller of
 * this function guarantees that it will <b>never</b> use the cookie again
 * (until the upstream layers provides the cookie again). We don't allow
 * semantically wrong programming, so we'll <b>CRASH</b> if the caller tries
 * to release a cookie that isn't reserved.
 *
 * @param cookie the cookie to release (this cookie <b>must</b> already be
 *               reserved by a call to bucket_engine_reserve_cookie
 * @return ENGINE_SUCCESS upon success
 */
static ENGINE_ERROR_CODE bucket_engine_release_cookie(const void *cookie)
{
    // The cookie <b>SHALL</b> be reserved before the caller may call
    // release. Lets go ahead and verify that (and crash and burn if
    // the caller tries to mess with us).
    engine_specific_t *es;
    es = bucket_engine.upstream_server->cookie->get_engine_specific(cookie);
    assert(es != NULL);
    assert((es->reserved & ~ES_CONNECTED_FLAG) > 0);
    proxied_engine_handle_t *peh = es->peh;
    assert(peh != NULL);

    // Decrement the internal reserved count, and then release it
    // in the upstream engine.
    ATOMIC_DECR(&es->reserved);
    release_handle(peh);

    if (upstream_release_cookie(cookie) != ENGINE_SUCCESS) {
        logger->log(EXTENSION_LOG_WARNING, cookie,
                    "Failed to release a reserved cookie (%p).\n"
                    "Expect a memory leak and potential hang situation "
                    "on this client",
                    cookie);
    }

    return ENGINE_SUCCESS;
}
