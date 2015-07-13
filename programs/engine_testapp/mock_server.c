#include "config.h"
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <memcached/engine.h>
#include <memcached/extension.h>
#include <memcached/extension_loggers.h>
#include <memcached/allocator_hooks.h>
#include "daemon/alloc_hooks.h"

#include "mock_server.h"

#define REALTIME_MAXDELTA 60*60*24*3
#define CONN_MAGIC 0xbeefcafe

struct mock_extensions {
    EXTENSION_DAEMON_DESCRIPTOR *daemons;
    EXTENSION_LOGGER_DESCRIPTOR *logger;
};

struct mock_callbacks *mock_event_handlers[MAX_ENGINE_EVENT_TYPE + 1];
time_t process_started;     /* when the mock server was started */
rel_time_t time_travel_offset;
static cb_mutex_t time_mutex;
struct mock_connstruct *connstructs;
struct mock_extensions extensions;
EXTENSION_LOGGER_DESCRIPTOR *null_logger = NULL;
EXTENSION_LOGGER_DESCRIPTOR *stderr_logger = NULL;
EXTENSION_LOG_LEVEL log_level = EXTENSION_LOG_INFO;

/**
 * Session cas elements
 */
uint64_t session_cas;
uint8_t session_ctr;
cb_mutex_t session_mutex;

/**
 * SERVER CORE API FUNCTIONS
 */

static void mock_get_auth_data(const void *cookie, auth_data_t *data) {
    struct mock_connstruct *c = (struct mock_connstruct *)cookie;
    if (c != NULL) {
        data->username = c->uname;
        data->config = c->config;
    }
}

static void mock_store_engine_specific(const void *cookie, void *engine_data) {
    if (cookie) {
        struct mock_connstruct *c = (struct mock_connstruct *)cookie;
        cb_assert(c->magic == CONN_MAGIC);
        c->engine_data = engine_data;
    }
}

static void *mock_get_engine_specific(const void *cookie) {
    struct mock_connstruct *c = (struct mock_connstruct *)cookie;
    cb_assert(c == NULL || c->magic == CONN_MAGIC);
    return c ? c->engine_data : NULL;
}

static bool mock_is_datatype_supported(const void *cookie) {
    struct mock_connstruct *c = (struct mock_connstruct *)cookie;
    cb_assert(c == NULL || c->magic == CONN_MAGIC);
    return c->handle_datatype_support;
}

static bool mock_is_mutation_extras_supported(const void *cookie) {
    struct mock_connstruct *c = (struct mock_connstruct *)cookie;
    cb_assert(c == NULL || c->magic == CONN_MAGIC);
    return c->handle_mutation_extras;
}

static uint8_t mock_get_opcode_if_ewouldblock_set(const void *cookie) {
    struct mock_connstruct *c = (struct mock_connstruct *)cookie;
    cb_assert(c == NULL || c->magic == CONN_MAGIC);
    return 0x00;
}

static bool mock_validate_session_cas(const uint64_t cas) {
    bool ret = true;
    cb_mutex_enter(&(session_mutex));
    if (cas != 0) {
        if (session_cas != cas) {
            ret = false;
        } else {
            session_ctr++;
        }
    } else {
        session_ctr++;
    }
    cb_mutex_exit(&(session_mutex));
    return ret;
}

static void mock_decrement_session_ctr(void) {
    cb_mutex_enter(&(session_mutex));
    cb_assert(session_ctr != 0);
    session_ctr--;
    cb_mutex_exit(&(session_mutex));
}

#ifdef WIN32
static SOCKET mock_get_socket_fd(const void *cookie) {
#else
static int mock_get_socket_fd(const void *cookie) {
#endif
    struct mock_connstruct *c = (struct mock_connstruct *)cookie;
    return c->sfd;
}

static ENGINE_ERROR_CODE mock_cookie_reserve(const void *cookie) {
    struct mock_connstruct *c = (struct mock_connstruct *)cookie;
    c->references++;
    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE mock_cookie_release(const void *cookie) {
    struct mock_connstruct *c = (struct mock_connstruct *)cookie;

    c->references--;
    if (c->references == 0) {
        free(c);
    }
    return ENGINE_SUCCESS;
}

static void mock_set_priority(const void* cookie, CONN_PRIORITY priority) {
    (void) cookie;
    (void) priority;
}

static const char *mock_get_server_version(void) {
    return "mock server";
}

static uint32_t mock_hash( const void *key, size_t length, const uint32_t initval) {
    /*this is a very stupid hash indeed */
    return 1;
}

/* time-sensitive callers can call it by hand with this, outside the
   normal ever-1-second timer */
static rel_time_t mock_get_current_time(void) {
    cb_mutex_enter(&time_mutex);
#ifdef WIN32
    rel_time_t result = (rel_time_t)(time(NULL) - process_started + time_travel_offset);
#else
    struct timeval timer;
    gettimeofday(&timer, NULL);
    rel_time_t result = (rel_time_t) (timer.tv_sec - process_started + time_travel_offset);
#endif
    cb_mutex_exit(&time_mutex);
    return result;
}

static rel_time_t mock_realtime(const time_t exptime) {
    /* no. of seconds in 30 days - largest possible delta exptime */

    if (exptime == 0) return 0; /* 0 means never expire */

    if (exptime > REALTIME_MAXDELTA) {
        /* if item expiration is at/before the server started, give it an
           expiration time of 1 second after the server started.
           (because 0 means don't expire).  without this, we'd
           underflow and wrap around to some large value way in the
           future, effectively making items expiring in the past
           really expiring never */
        if (exptime <= process_started)
            return (rel_time_t)1;
        return (rel_time_t)(exptime - process_started);
    } else {
        return (rel_time_t)(exptime + mock_get_current_time());
    }
}

static void mock_notify_io_complete(const void *cookie, ENGINE_ERROR_CODE status) {
    if (cookie) {
        struct mock_connstruct *c = (struct mock_connstruct *)cookie;
        cb_mutex_enter(&c->mutex);
        c->status = status;
        cb_cond_signal(&c->cond);
        cb_mutex_exit(&c->mutex);
    }
}

static time_t mock_abstime(const rel_time_t exptime)
{
    return process_started + exptime;
}

void mock_time_travel(int by) {
    cb_mutex_enter(&time_mutex);
    time_travel_offset += by;
    cb_mutex_exit(&time_mutex);
}

static int mock_parse_config(const char *str, struct config_item items[], FILE *error) {
    return parse_config(str, items, error);
}

/**
 * SERVER STAT API FUNCTIONS
 */

static void *mock_new_independent_stats(void) {
    struct mock_stats *mockstats = calloc(sizeof(*mockstats), 1);
    return mockstats;
}

static void mock_release_independent_stats(void *stats) {
    struct mock_stats *mockstats = stats;
    free(mockstats);
}

static void mock_count_eviction(const void *cookie, const void *key, const int nkey) {
    struct mock_connstruct *c = (struct mock_connstruct *)cookie;
    c->evictions++;
}

/**
 * SERVER STAT API FUNCTIONS
 */

static bool mock_register_extension(extension_type_t type, void *extension)
{
    if (extension == NULL) {
        return false;
    }

    switch (type) {
    case EXTENSION_DAEMON:
        {
            EXTENSION_DAEMON_DESCRIPTOR *ptr;
            for (ptr =  extensions.daemons; ptr != NULL; ptr = ptr->next) {
                if (ptr == extension) {
                    return false;
                }
            }
            ((EXTENSION_DAEMON_DESCRIPTOR *)(extension))->next = extensions.daemons;
            extensions.daemons = extension;
        }
        return true;
    case EXTENSION_LOGGER:
        extensions.logger = extension;
        return true;
    default:
        return false;
    }
}

static void mock_unregister_extension(extension_type_t type, void *extension)
{
    switch (type) {
    case EXTENSION_DAEMON:
        {
            EXTENSION_DAEMON_DESCRIPTOR *prev = NULL;
            EXTENSION_DAEMON_DESCRIPTOR *ptr = extensions.daemons;

            while (ptr != NULL && ptr != extension) {
                prev = ptr;
                ptr = ptr->next;
            }

            if (ptr != NULL && prev != NULL) {
                prev->next = ptr->next;
            }

            if (ptr != NULL && extensions.daemons == ptr) {
                extensions.daemons = ptr->next;
            }
        }
        break;
    case EXTENSION_LOGGER:
        if (extensions.logger == extension) {
            if (stderr_logger == extension) {
                extensions.logger = null_logger;
            } else {
                extensions.logger = stderr_logger;
            }
        }
        break;

    default:
        ;
    }

}

static void* mock_get_extension(extension_type_t type)
{
    switch (type) {
    case EXTENSION_DAEMON:
        return extensions.daemons;

    case EXTENSION_LOGGER:
        return extensions.logger;

    default:
        return NULL;
    }
}

/**
 * SERVER CALLBACK API FUNCTIONS
 */

static void mock_register_callback(ENGINE_HANDLE *eh,
                                   ENGINE_EVENT_TYPE type,
                                   EVENT_CALLBACK cb,
                                   const void *cb_data) {
    struct mock_callbacks *h =
        calloc(sizeof(struct mock_callbacks), 1);
    cb_assert(h);
    h->cb = cb;
    h->cb_data = cb_data;
    h->next = mock_event_handlers[type];
    mock_event_handlers[type] = h;
}

static void mock_perform_callbacks(ENGINE_EVENT_TYPE type,
                                   const void *data,
                                   const void *c) {
    struct mock_callbacks *h;
    for (h = mock_event_handlers[type]; h; h = h->next) {
        h->cb(c, type, data, h->cb_data);
    }
}

void mock_init_alloc_hooks(void) {
    init_alloc_hooks();
}

/**
 * LOG API FUNCTIONS
 **/
static EXTENSION_LOGGER_DESCRIPTOR* mock_get_logger(void) {
    return extensions.logger;
}

static EXTENSION_LOG_LEVEL mock_get_log_level(void) {
    return log_level;
}

static void mock_set_log_level(EXTENSION_LOG_LEVEL severity) {
    log_level = severity;
}

SERVER_HANDLE_V1 *get_mock_server_api(void)
{
   static SERVER_CORE_API core_api;
   static SERVER_COOKIE_API server_cookie_api;
   static SERVER_STAT_API server_stat_api;
   static SERVER_EXTENSION_API extension_api;
   static SERVER_CALLBACK_API callback_api;
   static SERVER_LOG_API log_api;
   static ALLOCATOR_HOOKS_API hooks_api;
   static SERVER_HANDLE_V1 rv;
   static int init;
   if (!init) {
      init = 1;
      core_api.server_version = mock_get_server_version;
      core_api.hash = mock_hash;
      core_api.realtime = mock_realtime;
      core_api.get_current_time = mock_get_current_time;
      core_api.abstime = mock_abstime;
      core_api.parse_config = mock_parse_config;

      server_cookie_api.get_auth_data = mock_get_auth_data;
      server_cookie_api.store_engine_specific = mock_store_engine_specific;
      server_cookie_api.get_engine_specific = mock_get_engine_specific;
      server_cookie_api.is_datatype_supported = mock_is_datatype_supported;
      server_cookie_api.is_mutation_extras_supported = mock_is_mutation_extras_supported;
      server_cookie_api.get_opcode_if_ewouldblock_set = mock_get_opcode_if_ewouldblock_set;
      server_cookie_api.validate_session_cas = mock_validate_session_cas;
      server_cookie_api.decrement_session_ctr = mock_decrement_session_ctr;
      server_cookie_api.get_socket_fd = mock_get_socket_fd;
      server_cookie_api.notify_io_complete = mock_notify_io_complete;
      server_cookie_api.reserve = mock_cookie_reserve;
      server_cookie_api.release = mock_cookie_release;
      server_cookie_api.set_priority = mock_set_priority;

      server_stat_api.new_stats = mock_new_independent_stats;
      server_stat_api.release_stats = mock_release_independent_stats;
      server_stat_api.evicting = mock_count_eviction;

      extension_api.register_extension = mock_register_extension;
      extension_api.unregister_extension = mock_unregister_extension;
      extension_api.get_extension = mock_get_extension;

      callback_api.register_callback = mock_register_callback;
      callback_api.perform_callbacks = mock_perform_callbacks;

      log_api.get_logger = mock_get_logger;
      log_api.get_level = mock_get_log_level;
      log_api.set_level = mock_set_log_level;

      hooks_api.add_new_hook = mc_add_new_hook;
      hooks_api.remove_new_hook = mc_remove_new_hook;
      hooks_api.add_delete_hook = mc_add_delete_hook;
      hooks_api.remove_delete_hook = mc_remove_delete_hook;
      hooks_api.get_extra_stats_size = mc_get_extra_stats_size;
      hooks_api.get_allocator_stats = mc_get_allocator_stats;
      hooks_api.get_allocation_size = mc_get_allocation_size;
      hooks_api.get_detailed_stats = mc_get_detailed_stats;
      hooks_api.release_free_memory = mc_release_free_memory;
      hooks_api.enable_thread_cache = mc_enable_thread_cache;

      rv.interface = 1;
      rv.core = &core_api;
      rv.stat = &server_stat_api;
      rv.extension = &extension_api;
      rv.callback = &callback_api;
      rv.log = &log_api;
      rv.cookie = &server_cookie_api;
      rv.alloc_hooks = &hooks_api;
   }

   return &rv;
}

void init_mock_server(bool log_to_stderr) {
    process_started = time(0);
    null_logger = get_null_logger();
    stderr_logger = get_stderr_logger();
    extensions.logger = log_to_stderr ? stderr_logger : null_logger;
    session_cas = 0x0102030405060708;
    session_ctr = 0;
    cb_mutex_initialize(&session_mutex);
    cb_mutex_initialize(&time_mutex);
}

struct mock_connstruct *mk_mock_connection(const char *user, const char *config) {
    struct mock_connstruct *rv = calloc(sizeof(struct mock_connstruct), 1);
    auth_data_t ad;
    cb_assert(rv);
    rv->magic = CONN_MAGIC;
    rv->uname = user ? strdup(user) : NULL;
    rv->config = config ? strdup(config) : NULL;
    rv->connected = true;
    rv->next = connstructs;
    rv->evictions = 0;
    rv->sfd = 0; /* TODO make this more realistic */
    rv->status = ENGINE_SUCCESS;
    connstructs = rv;
    mock_perform_callbacks(ON_CONNECT, NULL, rv);
    if (rv->uname) {
        mock_get_auth_data(rv, &ad);
        mock_perform_callbacks(ON_AUTH, (const void*)&ad, rv);
    }

    cb_mutex_initialize(&rv->mutex);
    cb_cond_initialize(&rv->cond);

    return rv;
}

const void *create_mock_cookie(void) {
    struct mock_connstruct *rv = calloc(sizeof(struct mock_connstruct), 1);
    cb_assert(rv);
    rv->magic = CONN_MAGIC;
    rv->connected = true;
    rv->status = ENGINE_SUCCESS;
    rv->handle_ewouldblock = true;
    rv->handle_mutation_extras = true;
    rv->references = 1;
    rv->handle_datatype_support = false;
    cb_mutex_initialize(&rv->mutex);
    cb_cond_initialize(&rv->cond);

    return rv;
}

void destroy_mock_cookie(const void *cookie) {
    struct mock_connstruct *c = (struct mock_connstruct *)cookie;
    disconnect_mock_connection(c);
    if (c->references == 0) {
        free((void*)cookie);
    }
}

void mock_set_ewouldblock_handling(const void *cookie, bool enable) {
    struct mock_connstruct *v = (void*)cookie;
    v->handle_ewouldblock = enable;
}

void mock_set_mutation_extras_handling(const void *cookie, bool enable) {
    struct mock_connstruct *v = (void *)cookie;
    v->handle_mutation_extras = enable;
}

void mock_set_datatype_support(const void *cookie, bool enable) {
    struct mock_connstruct *v = (void *)cookie;
    v->handle_datatype_support = enable;
}

void lock_mock_cookie(const void *cookie) {
   struct mock_connstruct *c = (void*)cookie;
   cb_mutex_enter(&c->mutex);
}

void unlock_mock_cookie(const void *cookie) {
   struct mock_connstruct *c = (void*)cookie;
   cb_mutex_exit(&c->mutex);
}

void waitfor_mock_cookie(const void *cookie) {
   struct mock_connstruct *c = (void*)cookie;
   cb_cond_wait(&c->cond, &c->mutex);
}

void disconnect_mock_connection(struct mock_connstruct *c) {
    c->connected = false;
    c->references--;
    mock_perform_callbacks(ON_DISCONNECT, NULL, c);
}

void disconnect_all_mock_connections(struct mock_connstruct *c) {
    if (c) {
        disconnect_mock_connection(c);
        disconnect_all_mock_connections(c->next);
        free((void*)c->uname);
        free((void*)c->config);
        free(c);
    }
}

void destroy_mock_event_callbacks_rec(struct mock_callbacks *h) {
    if (h) {
        destroy_mock_event_callbacks_rec(h->next);
        free(h);
    }
}

void destroy_mock_event_callbacks(void) {
    int i = 0;
    for (i = 0; i < MAX_ENGINE_EVENT_TYPE; i++) {
        destroy_mock_event_callbacks_rec(mock_event_handlers[i]);
        mock_event_handlers[i] = NULL;
    }
}
