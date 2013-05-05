/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef BUCKET_ENGINE_INTERNAL_H
#define BUCKET_ENGINE_INTERNAL_H
#include "config.h"
#include <memcached/engine.h>
#include "genhash.h"
#include "topkeys.h"
#include "bucket_engine.h"

typedef union proxied_engine {
    ENGINE_HANDLE    *v0;
    ENGINE_HANDLE_V1 *v1;
} proxied_engine_t;

typedef enum {
    STATE_NULL,
    STATE_RUNNING,
    STATE_STOPPING,
    STATE_STOPPED
} bucket_state_t;

typedef struct proxied_engine_handle {
    const char          *name;
    size_t               name_len;
    proxied_engine_t     pe;
    void                *stats;
    topkeys_t          **topkeys;
    TAP_ITERATOR         tap_iterator;
    bool                 tap_iterator_disabled;
    /* ON_DISCONNECT handling */
    volatile bool        wants_disconnects;
    /* Force shutdown flag */
    bool                 force_shutdown;
    EVENT_CALLBACK       cb;
    const void          *cb_data;
    /* count of connections + 1 for hashtable reference + number of
     * reserved connections for this bucket + number of temporary
     * references created by find_bucket & frieds.
     *
     * count of connections is count of engine_specific instances
     * having peh equal to this engine_handle. There's only one
     * exception which is connections for which on_disconnect callback
     * was called but which are kept alive by reserved > 0. For those
     * connections we drop refcount in on_disconnect but keep peh
     * field so that bucket_engine_release_cookie can decrement peh
     * refcount.
     *
     * Handle itself can be freed when this drops to zero. This can
     * only happen when bucket is deleted (but can happen later
     * because some connection can hold pointer longer) */
    volatile int         refcount;
    volatile int clients; /* # of clients currently calling functions in the engine */
    const void *cookie;
    void *dlhandle;
    volatile bucket_state_t state;
} proxied_engine_handle_t;

#define ES_CONNECTED_FLAG 0x1000

/**
 * bucket_engine needs to store data specific to a given connection.
 * In order to do that it utilize the "engine-specific" field for a
 * cookie. Due to the fact that the underlying engine needs to be able
 * to use the field as well, we need a holder-structure to contain
 * the bucket-specific data and the underlying engine-specific data.
 */
typedef struct engine_specific {
    /** The engine this cookie is connected to */
    proxied_engine_handle_t *peh;
    /** The userdata stored by the underlying engine */
    void *engine_specific;
    /** The number of times the underlying engine tried to reserve
     * this connection */
    /* 0x1000 is added while we think memcached connection is
     * alive. We'll decrement it when processing ON_DISCONNECT
     * callback. */
    int reserved;
} engine_specific_t;


struct bucket_engine {
    ENGINE_HANDLE_V1 engine;
    SERVER_HANDLE_V1 *upstream_server;
    bool initialized;
    bool has_default;
    bool auto_create;
    char *default_engine_path;
    char *admin_user;
    char *default_bucket_name;
    char *default_bucket_config;
    proxied_engine_handle_t default_engine;
    cb_mutex_t engines_mutex;
    genhash_t *engines;
    GET_SERVER_API get_server_api;
    SERVER_HANDLE_V1 server;
    SERVER_CALLBACK_API callback_api;
    SERVER_EXTENSION_API extension_api;
    SERVER_COOKIE_API cookie_api;

    struct {
        bool in_progress; /* Is the global shutdown in progress */
        int bucket_counter; /* Number of treads currently running shutdown */
        cb_mutex_t mutex;
        cb_cond_t cond;
        /* this condition signals either in_progress being true or
         * some bucket's refcount being 0.
         *
         * This means that under some conditions (lots of buckets
         * recently deleted but still have connections assigned on
         * them) we'll have a bit of thundering herd problem, where
         * too many bucket deletion threads are woken up
         * needlessly. But that can be improved much later when and if
         * we actually find this to be a problem. */
        cb_cond_t refcount_cond;
    } shutdown;

    union {
      engine_info engine_info;
      char buffer[sizeof(engine_info) +
                  (sizeof(feature_info) * LAST_REGISTERED_ENGINE_FEATURE)];
    } info;

    int topkeys;
};

#endif
