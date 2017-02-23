/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <ctype.h>
#include <unistd.h>
#include <stddef.h>
#include <inttypes.h>

#include "default_engine_internal.h"
#include "memcached/util.h"
#include "memcached/config_parser.h"
#include <platform/cb_malloc.h>
#include "engines/default_engine.h"
#include "engine_manager.h"

// The default engine don't really use vbucket uuids, but in order
// to run the unit tests and verify that we correctly convert the
// vbucket uuid to network byte order it is nice to have a value
// we may use for testing ;)
#define DEFAULT_ENGINE_VBUCKET_UUID 0xdeadbeef

static const engine_info* default_get_info(ENGINE_HANDLE* handle);
static ENGINE_ERROR_CODE default_initialize(ENGINE_HANDLE* handle,
                                            const char* config_str);
static void default_destroy(ENGINE_HANDLE* handle,
                            const bool force);
static ENGINE_ERROR_CODE default_item_allocate(ENGINE_HANDLE* handle,
                                               const void* cookie,
                                               item **item,
                                               const DocKey& key,
                                               const size_t nbytes,
                                               const int flags,
                                               const rel_time_t exptime,
                                               uint8_t datatype,
                                               uint16_t vbucket);
static std::pair<cb::unique_item_ptr, item_info> default_item_allocate_ex(ENGINE_HANDLE* handle,
                                                                          const void* cookie,
                                                                          const DocKey& key,
                                                                          const size_t nbytes,
                                                                          const size_t priv_nbytes,
                                                                          const int flags,
                                                                          const rel_time_t exptime,
                                                                          uint8_t datatype,
                                                                          uint16_t vbucket);

static ENGINE_ERROR_CODE default_item_delete(ENGINE_HANDLE* handle,
                                             const void* cookie,
                                             const DocKey& key,
                                             uint64_t* cas,
                                             uint16_t vbucket,
                                             mutation_descr_t* mut_info);

static void default_item_release(ENGINE_HANDLE* handle, const void *cookie,
                                 item* item);
static ENGINE_ERROR_CODE default_get(ENGINE_HANDLE* handle,
                                     const void* cookie,
                                     item** item,
                                     const DocKey& key,
                                     uint16_t vbucket,
                                     DocumentState);
static ENGINE_ERROR_CODE default_get_locked(ENGINE_HANDLE* handle,
                                            const void* cookie,
                                            item** item,
                                            const DocKey& key,
                                            uint16_t vbucket,
                                            uint32_t lock_timeout);
static ENGINE_ERROR_CODE default_unlock(ENGINE_HANDLE* handle,
                                        const void* cookie,
                                        const DocKey& key,
                                        uint16_t vbucket,
                                        uint64_t cas);
static ENGINE_ERROR_CODE default_get_stats(ENGINE_HANDLE* handle,
                  const void *cookie,
                  const char *stat_key,
                  int nkey,
                  ADD_STAT add_stat);
static void default_reset_stats(ENGINE_HANDLE* handle, const void *cookie);
static ENGINE_ERROR_CODE default_store(ENGINE_HANDLE* handle,
                                       const void *cookie,
                                       item* item,
                                       uint64_t *cas,
                                       ENGINE_STORE_OPERATION operation,
                                       DocumentState);
static ENGINE_ERROR_CODE default_flush(ENGINE_HANDLE* handle,
                                       const void* cookie);
static ENGINE_ERROR_CODE initalize_configuration(struct default_engine *se,
                                                 const char *cfg_str);
static ENGINE_ERROR_CODE default_unknown_command(ENGINE_HANDLE* handle,
                                                 const void* cookie,
                                                 protocol_binary_request_header *request,
                                                 ADD_RESPONSE response,
                                                 DocNamespace doc_namespace);


union vbucket_info_adapter {
    char c;
    struct vbucket_info v;
};

static void set_vbucket_state(struct default_engine *e,
                              uint16_t vbid, vbucket_state_t to) {
    union vbucket_info_adapter vi;
    vi.c = e->vbucket_infos[vbid];
    vi.v.state = to;
    e->vbucket_infos[vbid] = vi.c;
}

static vbucket_state_t get_vbucket_state(struct default_engine *e,
                                         uint16_t vbid) {
    union vbucket_info_adapter vi;
    vi.c = e->vbucket_infos[vbid];
    return vbucket_state_t(vi.v.state);
}

static bool handled_vbucket(struct default_engine *e, uint16_t vbid) {
    return e->config.ignore_vbucket
        || (get_vbucket_state(e, vbid) == vbucket_state_active);
}

/* mechanism for handling bad vbucket requests */
#define VBUCKET_GUARD(e, v) if (!handled_vbucket(e, v)) { return ENGINE_NOT_MY_VBUCKET; }

static bool get_item_info(ENGINE_HANDLE *handle, const void *cookie,
                          const item* item, item_info *item_info);

static bool set_item_info(ENGINE_HANDLE *handle, const void *cookie,
                          item* item, const item_info *itm_info);

/**
 * Given that default_engine is implemented in C and not C++ we don't have
 * a constructor for the struct to initialize the members to some sane
 * default values. Currently they're all being allocated through the
 * engine manager which keeps a local map of all engines being created.
 *
 * Once an object is in that map it may in theory be referenced, so we
 * need to ensure that the members is initialized before hitting that map.
 *
 * @todo refactor default_engine to C++ to avoid this extra hack :)
 */
void default_engine_constructor(struct default_engine* engine, bucket_id_t id)
{
    memset(engine, 0, sizeof(*engine));

    cb_mutex_initialize(&engine->slabs.lock);
    cb_mutex_initialize(&engine->items.lock);
    cb_mutex_initialize(&engine->stats.lock);
    cb_mutex_initialize(&engine->scrubber.lock);

    engine->bucket_id = id;
    engine->engine.interface.interface = 1;
    engine->engine.get_info = default_get_info;
    engine->engine.initialize = default_initialize;
    engine->engine.destroy = default_destroy;
    engine->engine.allocate = default_item_allocate;
    engine->engine.allocate_ex = default_item_allocate_ex;
    engine->engine.remove = default_item_delete;
    engine->engine.release = default_item_release;
    engine->engine.get = default_get;
    engine->engine.get_locked = default_get_locked;
    engine->engine.unlock = default_unlock;
    engine->engine.get_stats = default_get_stats;
    engine->engine.reset_stats = default_reset_stats;
    engine->engine.store = default_store;
    engine->engine.flush = default_flush;
    engine->engine.unknown_command = default_unknown_command;
    engine->engine.item_set_cas = item_set_cas;
    engine->engine.get_item_info = get_item_info;
    engine->engine.set_item_info = set_item_info;
    engine->config.verbose = 0;
    engine->config.oldest_live = 0;
    engine->config.evict_to_free = true;
    engine->config.maxbytes = 64 * 1024 * 1024;
    engine->config.preallocate = false;
    engine->config.factor = 1.25;
    engine->config.chunk_size = 48;
    engine->config.item_size_max= 1024 * 1024;
    engine->info.engine.description = "Default engine v0.1";
    engine->info.engine.num_features = 1;
    engine->info.engine.features[0].feature = ENGINE_FEATURE_LRU;
    engine->info.engine.features[engine->info.engine.num_features++].feature
        = ENGINE_FEATURE_DATATYPE;
}

extern "C" ENGINE_ERROR_CODE create_instance(uint64_t interface,
                                             GET_SERVER_API get_server_api,
                                             ENGINE_HANDLE **handle) {
   SERVER_HANDLE_V1 *api = get_server_api();
   struct default_engine *engine;

   if (interface != 1 || api == NULL) {
      return ENGINE_ENOTSUP;
   }

   if ((engine = engine_manager_create_engine()) == NULL) {
      return ENGINE_ENOMEM;
   }

   engine->server = *api;
   engine->get_server_api = get_server_api;
   engine->initialized = true;
   *handle = (ENGINE_HANDLE*)&engine->engine;
   return ENGINE_SUCCESS;
}

extern "C" void destroy_engine() {
    engine_manager_shutdown();
    assoc_destroy();
}

static struct default_engine* get_handle(ENGINE_HANDLE* handle) {
   return (struct default_engine*)handle;
}

static hash_item* get_real_item(item* item) {
    return (hash_item*)item;
}

static const engine_info* default_get_info(ENGINE_HANDLE* handle) {
    return &get_handle(handle)->info.engine;
}

static ENGINE_ERROR_CODE default_initialize(ENGINE_HANDLE* handle,
                                            const char* config_str) {
   struct default_engine* se = get_handle(handle);
   ENGINE_ERROR_CODE ret = initalize_configuration(se, config_str);
   if (ret != ENGINE_SUCCESS) {
      return ret;
   }
   se->info.engine.features[se->info.engine.num_features++].feature = ENGINE_FEATURE_CAS;

   ret = assoc_init(se);
   if (ret != ENGINE_SUCCESS) {
      return ret;
   }

   ret = slabs_init(se, se->config.maxbytes, se->config.factor,
                    se->config.preallocate);
   if (ret != ENGINE_SUCCESS) {
      return ret;
   }

   return ENGINE_SUCCESS;
}

static void default_destroy(ENGINE_HANDLE* handle, const bool force) {
    (void)force;
    engine_manager_delete_engine(get_handle(handle));
}

void destroy_engine_instance(struct default_engine* engine) {
    if (engine->initialized) {
        /* Destory the slabs cache */
        slabs_destroy(engine);

        cb_free(engine->config.uuid);

        /* Clean up the mutexes */
        cb_mutex_destroy(&engine->items.lock);
        cb_mutex_destroy(&engine->stats.lock);
        cb_mutex_destroy(&engine->slabs.lock);
        cb_mutex_destroy(&engine->scrubber.lock);

        engine->initialized = false;
    }
}

static ENGINE_ERROR_CODE default_item_allocate(ENGINE_HANDLE* handle,
                                               const void* cookie,
                                               item **item,
                                               const DocKey& key,
                                               const size_t nbytes,
                                               const int flags,
                                               const rel_time_t exptime,
                                               uint8_t datatype,
                                               uint16_t vbucket) {
    try {
        auto pair = default_item_allocate_ex(handle, cookie, key, nbytes,
                                             0, // No privileged bytes
                                             flags, exptime, datatype, vbucket);
        // Given the older API doesn't return a smart pointer, need to 'release'
        // the one we get back from allocate_ex.
        *item = pair.first.release();
        return ENGINE_SUCCESS;
    } catch (const cb::engine_error& error) {
        return ENGINE_ERROR_CODE(error.code().value());
    }
}

static std::pair<cb::unique_item_ptr, item_info> default_item_allocate_ex(ENGINE_HANDLE* handle,
                                                                          const void* cookie,
                                                                          const DocKey& key,
                                                                          const size_t nbytes,
                                                                          const size_t priv_nbytes,
                                                                          const int flags,
                                                                          const rel_time_t exptime,
                                                                          uint8_t datatype,
                                                                          uint16_t vbucket) {
    hash_item *it;

    unsigned int id;
    struct default_engine* engine = get_handle(handle);

    if (!handled_vbucket(engine, vbucket)) {
        throw cb::engine_error(cb::engine_errc::not_my_vbucket,
                               "default_item_allocate_ex");
    }

    size_t ntotal = sizeof(hash_item) + key.size() + nbytes;
    id = slabs_clsid(engine, ntotal);
    if (id == 0) {
        throw cb::engine_error(cb::engine_errc::too_big,
                               "default_item_allocate_ex: no slab class");
    }

    if ((nbytes - priv_nbytes) > engine->config.item_size_max) {
        throw cb::engine_error(cb::engine_errc::too_big,
                               "default_item_allocate_ex");
    }

    it = item_alloc(engine, key.data(), key.size(), flags,
                    engine->server.core->realtime(exptime),
                    (uint32_t)nbytes, cookie, datatype);

    if (it != NULL) {
        item_info info;
        if (!get_item_info(handle, cookie, it, &info)) {
            // This should never happen (unless we provide invalid
            // arguments)
            item_release(engine, it);
            throw cb::engine_error(cb::engine_errc::failed,
                                   "default_item_allocate_ex");
        }

        return std::make_pair(cb::unique_item_ptr(it, cb::ItemDeleter{handle}),
                              info);
    } else {
        throw cb::engine_error(cb::engine_errc::no_memory,
                               "default_item_allocate_ex");
    }
}

static ENGINE_ERROR_CODE default_item_delete(ENGINE_HANDLE* handle,
                                             const void* cookie,
                                             const DocKey& key,
                                             uint64_t* cas,
                                             uint16_t vbucket,
                                             mutation_descr_t* mut_info) {
    struct default_engine* engine = get_handle(handle);
    hash_item* it;
    uint64_t cas_in = *cas;
    VBUCKET_GUARD(engine, vbucket);

    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    do {
        it = item_get(engine, cookie, key.data(), key.size(),
                      DocumentState::Alive);
        if (it == nullptr) {
            return ENGINE_KEY_ENOENT;
        }

        if (it->locktime != 0 &&
            it->locktime > engine->server.core->get_current_time()) {
            if (cas_in != it->cas) {
                item_release(engine, it);
                return ENGINE_LOCKED;
            }
        }

        auto* deleted = item_alloc(engine, key.data(), key.size(), it->flags,
                                   it->exptime, it->nbytes, cookie,
                                   it->datatype);

        if (deleted == NULL) {
            item_release(engine, it);
            return ENGINE_TMPFAIL;
        }

        if (cas_in == 0) {
            // If the caller specified the "cas wildcard" we should set
            // the cas for the item we just fetched and do a cas
            // replace with that value
            item_set_cas(handle, cookie, deleted, it->cas);
        } else {
            // The caller specified a specific CAS value so we should
            // use that value in our cas replace
            item_set_cas(handle, cookie, deleted, cas_in);
        }

        ret = store_item(engine, deleted, cas, OPERATION_CAS,
                         cookie, DocumentState::Deleted);

        item_release(engine, it);
        item_release(engine, deleted);

        // We should only retry for race conditions if the caller specified
        // cas wildcard
    } while (ret == ENGINE_KEY_EEXISTS && cas_in == 0);

    // vbucket UUID / seqno arn't supported by default engine, so just return
    // a hardcoded vbucket uuid, and zero for the sequence number.
    mut_info->vbucket_uuid = DEFAULT_ENGINE_VBUCKET_UUID;
    mut_info->seqno = 0;

    return ret;
}

static void default_item_release(ENGINE_HANDLE* handle,
                                 const void *cookie,
                                 item* item) {
   item_release(get_handle(handle), get_real_item(item));
}

static ENGINE_ERROR_CODE default_get(ENGINE_HANDLE* handle,
                                     const void* cookie,
                                     item** item,
                                     const DocKey& key,
                                     uint16_t vbucket,
                                     DocumentState document_state) {
   struct default_engine *engine = get_handle(handle);
   VBUCKET_GUARD(engine, vbucket);

   *item = item_get(engine, cookie, key.data(), key.size(), document_state);
   if (*item != NULL) {
      return ENGINE_SUCCESS;
   } else {
      return ENGINE_KEY_ENOENT;
   }
}

static ENGINE_ERROR_CODE default_get_locked(ENGINE_HANDLE* handle,
                                            const void* cookie,
                                            item** item,
                                            const DocKey& key,
                                            uint16_t vbucket,
                                            uint32_t lock_timeout) {
    auto* engine = get_handle(handle);
    VBUCKET_GUARD(engine, vbucket);

    // memcached buckets don't offer any way for the user to configure
    // the lock settings.
    static const uint32_t default_lock_timeout = 15;
    static const uint32_t max_lock_timeout = 30;

    if (lock_timeout == 0 || lock_timeout > max_lock_timeout) {
        lock_timeout = default_lock_timeout;
    }

    // Convert the lock timeout to an absolute time
    lock_timeout += engine->server.core->get_current_time();

    hash_item* it;
    auto ret = item_get_locked(engine, cookie, &it, key.data(), key.size(),
                               lock_timeout);
    if (ret == ENGINE_SUCCESS) {
        *item = it;
    }

    return ret;
}

static ENGINE_ERROR_CODE default_unlock(ENGINE_HANDLE* handle,
                                        const void* cookie,
                                        const DocKey& key,
                                        uint16_t vbucket,
                                        uint64_t cas) {
    auto* engine = get_handle(handle);
    VBUCKET_GUARD(engine, vbucket);
    return item_unlock(engine, cookie, key.data(), key.size(), cas);
}

static ENGINE_ERROR_CODE default_get_stats(ENGINE_HANDLE* handle,
                                           const void* cookie,
                                           const char* stat_key,
                                           int nkey,
                                           ADD_STAT add_stat)
{
   struct default_engine* engine = get_handle(handle);
   ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

   if (stat_key == NULL) {
      char val[128];
      int len;

      cb_mutex_enter(&engine->stats.lock);
      len = sprintf(val, "%" PRIu64, (uint64_t)engine->stats.evictions);
      add_stat("evictions", 9, val, len, cookie);
      len = sprintf(val, "%" PRIu64, (uint64_t)engine->stats.curr_items);
      add_stat("curr_items", 10, val, len, cookie);
      len = sprintf(val, "%" PRIu64, (uint64_t)engine->stats.total_items);
      add_stat("total_items", 11, val, len, cookie);
      len = sprintf(val, "%" PRIu64, (uint64_t)engine->stats.curr_bytes);
      add_stat("bytes", 5, val, len, cookie);
      len = sprintf(val, "%" PRIu64, engine->stats.reclaimed);
      add_stat("reclaimed", 9, val, len, cookie);
      len = sprintf(val, "%" PRIu64, (uint64_t)engine->config.maxbytes);
      add_stat("engine_maxbytes", 15, val, len, cookie);
      cb_mutex_exit(&engine->stats.lock);
   } else if (strncmp(stat_key, "slabs", 5) == 0) {
      slabs_stats(engine, add_stat, cookie);
   } else if (strncmp(stat_key, "items", 5) == 0) {
      item_stats(engine, add_stat, cookie);
   } else if (strncmp(stat_key, "sizes", 5) == 0) {
      item_stats_sizes(engine, add_stat, cookie);
   } else if (strncmp(stat_key, "uuid", 4) == 0) {
       if (engine->config.uuid) {
           add_stat("uuid", 4, engine->config.uuid,
                    (uint32_t)strlen(engine->config.uuid), cookie);
       } else {
           add_stat("uuid", 4, "", 0, cookie);
       }
   } else if (strncmp(stat_key, "scrub", 5) == 0) {
      char val[128];
      int len;

      cb_mutex_enter(&engine->scrubber.lock);
      if (engine->scrubber.running) {
         add_stat("scrubber:status", 15, "running", 7, cookie);
      } else {
         add_stat("scrubber:status", 15, "stopped", 7, cookie);
      }

      if (engine->scrubber.started != 0) {
         if (engine->scrubber.stopped != 0) {
            time_t diff = engine->scrubber.started - engine->scrubber.stopped;
            len = sprintf(val, "%" PRIu64, (uint64_t)diff);
            add_stat("scrubber:last_run", 17, val, len, cookie);
         }

         len = sprintf(val, "%" PRIu64, engine->scrubber.visited);
         add_stat("scrubber:visited", 16, val, len, cookie);
         len = sprintf(val, "%" PRIu64, engine->scrubber.cleaned);
         add_stat("scrubber:cleaned", 16, val, len, cookie);
      }
      cb_mutex_exit(&engine->scrubber.lock);
   } else {
      ret = ENGINE_KEY_ENOENT;
   }

   return ret;
}

static ENGINE_ERROR_CODE default_store(ENGINE_HANDLE* handle,
                                       const void *cookie,
                                       item* item,
                                       uint64_t *cas,
                                       ENGINE_STORE_OPERATION operation,
                                       DocumentState document_state) {
    auto* engine = get_handle(handle);
    auto& config = engine->config;
    auto* it = get_real_item(item);

    if (document_state == DocumentState::Deleted && !config.keep_deleted) {
        return safe_item_unlink(engine, it);
    }

    return store_item(engine, it, cas, operation,
                      cookie, document_state);
}

static ENGINE_ERROR_CODE default_flush(ENGINE_HANDLE* handle,
                                       const void* cookie) {
   item_flush_expired(get_handle(handle));

   return ENGINE_SUCCESS;
}

static void default_reset_stats(ENGINE_HANDLE* handle, const void *cookie) {
   struct default_engine *engine = get_handle(handle);
   item_stats_reset(engine);

   cb_mutex_enter(&engine->stats.lock);
   engine->stats.evictions = 0;
   engine->stats.reclaimed = 0;
   engine->stats.total_items = 0;
   cb_mutex_exit(&engine->stats.lock);
}

static ENGINE_ERROR_CODE initalize_configuration(struct default_engine *se,
                                                 const char *cfg_str) {
   ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

   se->config.vb0 = true;

   if (cfg_str != NULL) {
       struct config_item items[13];
       int ii = 0;

       memset(&items, 0, sizeof(items));
       items[ii].key = "verbose";
       items[ii].datatype = DT_SIZE;
       items[ii].value.dt_size = &se->config.verbose;
       ++ii;

       items[ii].key = "eviction";
       items[ii].datatype = DT_BOOL;
       items[ii].value.dt_bool = &se->config.evict_to_free;
       ++ii;

       items[ii].key = "cache_size";
       items[ii].datatype = DT_SIZE;
       items[ii].value.dt_size = &se->config.maxbytes;
       ++ii;

       items[ii].key = "preallocate";
       items[ii].datatype = DT_BOOL;
       items[ii].value.dt_bool = &se->config.preallocate;
       ++ii;

       items[ii].key = "factor";
       items[ii].datatype = DT_FLOAT;
       items[ii].value.dt_float = &se->config.factor;
       ++ii;

       items[ii].key = "chunk_size";
       items[ii].datatype = DT_SIZE;
       items[ii].value.dt_size = &se->config.chunk_size;
       ++ii;

       items[ii].key = "item_size_max";
       items[ii].datatype = DT_SIZE;
       items[ii].value.dt_size = &se->config.item_size_max;
       ++ii;

       items[ii].key = "ignore_vbucket";
       items[ii].datatype = DT_BOOL;
       items[ii].value.dt_bool = &se->config.ignore_vbucket;
       ++ii;

       items[ii].key = "vb0";
       items[ii].datatype = DT_BOOL;
       items[ii].value.dt_bool = &se->config.vb0;
       ++ii;

       items[ii].key = "config_file";
       items[ii].datatype = DT_CONFIGFILE;
       ++ii;

       items[ii].key = "uuid";
       items[ii].datatype = DT_STRING;
       items[ii].value.dt_string = &se->config.uuid;
       ++ii;

       items[ii].key = "keep_deleted";
       items[ii].datatype = DT_BOOL;
       items[ii].value.dt_bool = &se->config.keep_deleted;
       ++ii;

       items[ii].key = NULL;
       ++ii;
       cb_assert(ii == 13);
       ret = ENGINE_ERROR_CODE(se->server.core->parse_config(cfg_str,
                                                             items,
                                                             stderr));
   }

   if (se->config.vb0) {
       set_vbucket_state(se, 0, vbucket_state_active);
   }

   return ret;
}

static bool set_vbucket(struct default_engine *e,
                        const void* cookie,
                        protocol_binary_request_set_vbucket *req,
                        ADD_RESPONSE response) {
    vbucket_state_t state;
    size_t bodylen = ntohl(req->message.header.request.bodylen)
        - ntohs(req->message.header.request.keylen);
    if (bodylen != sizeof(vbucket_state_t)) {
        const char *msg = "Incorrect packet format";
        return response(NULL, 0, NULL, 0, msg, (uint32_t)strlen(msg),
                        PROTOCOL_BINARY_RAW_BYTES,
                        PROTOCOL_BINARY_RESPONSE_EINVAL, 0, cookie);
    }
    memcpy(&state, &req->message.body.state, sizeof(state));
    state = vbucket_state_t(ntohl(state));

    if (!is_valid_vbucket_state_t(state)) {
        const char *msg = "Invalid vbucket state";
        return response(NULL, 0, NULL, 0, msg, (uint32_t)strlen(msg),
                        PROTOCOL_BINARY_RAW_BYTES,
                        PROTOCOL_BINARY_RESPONSE_EINVAL, 0, cookie);
    }

    set_vbucket_state(e, ntohs(req->message.header.request.vbucket), state);
    return response(NULL, 0, NULL, 0, &state, sizeof(state),
                    PROTOCOL_BINARY_RAW_BYTES,
                    PROTOCOL_BINARY_RESPONSE_SUCCESS, 0, cookie);
}

static bool get_vbucket(struct default_engine *e,
                        const void* cookie,
                        protocol_binary_request_get_vbucket *req,
                        ADD_RESPONSE response) {
    vbucket_state_t state;
    state = get_vbucket_state(e, ntohs(req->message.header.request.vbucket));
    state = vbucket_state_t(ntohl(state));

    return response(NULL, 0, NULL, 0, &state, sizeof(state),
                    PROTOCOL_BINARY_RAW_BYTES,
                    PROTOCOL_BINARY_RESPONSE_SUCCESS, 0, cookie);
}

static bool rm_vbucket(struct default_engine *e,
                       const void *cookie,
                       protocol_binary_request_header *req,
                       ADD_RESPONSE response) {
    set_vbucket_state(e, ntohs(req->request.vbucket), vbucket_state_dead);
    return response(NULL, 0, NULL, 0, NULL, 0, PROTOCOL_BINARY_RAW_BYTES,
                    PROTOCOL_BINARY_RESPONSE_SUCCESS, 0, cookie);
}

static bool scrub_cmd(struct default_engine *e,
                      const void *cookie,
                      protocol_binary_request_header *request,
                      ADD_RESPONSE response) {

    protocol_binary_response_status res = PROTOCOL_BINARY_RESPONSE_SUCCESS;
    if (!item_start_scrub(e)) {
        res = PROTOCOL_BINARY_RESPONSE_EBUSY;
    }

    return response(NULL, 0, NULL, 0, NULL, 0, PROTOCOL_BINARY_RAW_BYTES,
                    res, 0, cookie);
}

static bool touch(struct default_engine *e, const void *cookie,
                  protocol_binary_request_header *request,
                  ADD_RESPONSE response) {

    protocol_binary_request_touch *t;
    void *key;
    uint32_t exptime;
    uint16_t nkey;
    hash_item *item;


    if (request->request.extlen != 4 || request->request.keylen == 0) {
        return response(NULL, 0, NULL, 0, NULL, 0, PROTOCOL_BINARY_RAW_BYTES,
                        PROTOCOL_BINARY_RESPONSE_EINVAL, 0, cookie);
    }

    t = reinterpret_cast<protocol_binary_request_touch*>(request);
    key = t->bytes + sizeof(t->bytes);
    exptime = ntohl(t->message.body.expiration);
    nkey = ntohs(request->request.keylen);
    item = touch_item(e, cookie, key, nkey, e->server.core->realtime(exptime));

    if (item == NULL) {
        if (request->request.opcode == PROTOCOL_BINARY_CMD_GATQ) {
            return true;
        } else {
            return response(NULL, 0, NULL, 0, NULL, 0, PROTOCOL_BINARY_RAW_BYTES,
                            PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0, cookie);
        }
    } else {
        bool ret;
        if (request->request.opcode == PROTOCOL_BINARY_CMD_TOUCH) {
            ret = response(NULL, 0, NULL, 0, NULL, 0, PROTOCOL_BINARY_RAW_BYTES,
                           PROTOCOL_BINARY_RESPONSE_SUCCESS, 0, cookie);
        } else {
            ret = response(NULL, 0, &item->flags, sizeof(item->flags),
                           item_get_data(item), item->nbytes,
                           PROTOCOL_BINARY_RAW_BYTES,
                           PROTOCOL_BINARY_RESPONSE_SUCCESS,
                           item->cas, cookie);
        }
        item_release(e, item);
        return ret;
    }
}

static ENGINE_ERROR_CODE default_unknown_command(ENGINE_HANDLE* handle,
                                                 const void* cookie,
                                                 protocol_binary_request_header *request,
                                                 ADD_RESPONSE response,
                                                 DocNamespace doc_namespace)
{
    struct default_engine* e = get_handle(handle);
    bool sent;

    switch(request->request.opcode) {
    case PROTOCOL_BINARY_CMD_SCRUB:
        sent = scrub_cmd(e, cookie, request, response);
        break;
    case PROTOCOL_BINARY_CMD_DEL_VBUCKET:
        sent = rm_vbucket(e, cookie, request, response);
        break;
    case PROTOCOL_BINARY_CMD_SET_VBUCKET:
        sent = set_vbucket(e, cookie,
                reinterpret_cast<protocol_binary_request_set_vbucket*>(request),
                response);
        break;
    case PROTOCOL_BINARY_CMD_GET_VBUCKET:
        sent = get_vbucket(e, cookie,
                reinterpret_cast<protocol_binary_request_get_vbucket*>(request),
                response);
        break;
    case PROTOCOL_BINARY_CMD_TOUCH:
    case PROTOCOL_BINARY_CMD_GAT:
    case PROTOCOL_BINARY_CMD_GATQ:
        sent = touch(e, cookie, request, response);
        break;
    default:
        sent = response(NULL, 0, NULL, 0, NULL, 0, PROTOCOL_BINARY_RAW_BYTES,
                        PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND, 0, cookie);
        break;
    }

    if (sent) {
        return ENGINE_SUCCESS;
    } else {
        return ENGINE_FAILED;
    }
}

void item_set_cas(ENGINE_HANDLE *handle, const void *cookie,
                  item* item, uint64_t val)
{
    hash_item* it = get_real_item(item);
    it->cas = val;
}

hash_key* item_get_key(const hash_item* item)
{
    const char *ret = reinterpret_cast<const char*>(item + 1);
    return (hash_key*)ret;
}

char* item_get_data(const hash_item* item)
{
    const hash_key* key = item_get_key(item);
    return ((char*)key->header.full_key) + hash_key_get_key_len(key);
}

static bool get_item_info(ENGINE_HANDLE *handle, const void *cookie,
                          const item* item, item_info *item_info)
{
    hash_item* it = (hash_item*)item;
    const hash_key* key = item_get_key(it);

    auto* engine = get_handle(handle);
    if ((it->iflag & ITEM_LINKED) && it->locktime != 0 &&
        it->locktime > engine->server.core->get_current_time()) {
        // This object is locked. According to docs/Document.md we should
        // return -1 in such cases to hide the real CAS for the other clients
        // (Note the check on ITEM_LINKED.. for the actual item returned by
        // get_locked we return an item which isn't linked (copy of the
        // linked item) to allow returning the real CAS.
        item_info->cas = uint64_t(-1);
    } else {
        item_info->cas = it->cas;
    }

    item_info->vbucket_uuid = DEFAULT_ENGINE_VBUCKET_UUID;
    item_info->seqno = 0;
    item_info->exptime = it->exptime;
    item_info->nbytes = it->nbytes;
    item_info->flags = it->flags;
    item_info->nkey = hash_key_get_client_key_len(key);
    item_info->key = hash_key_get_client_key(key);
    item_info->value[0].iov_base = item_get_data(it);
    item_info->value[0].iov_len = it->nbytes;
    item_info->datatype = it->datatype;
    if (it->iflag & ITEM_ZOMBIE) {
        item_info->document_state = DocumentState::Deleted;
    } else {
        item_info->document_state = DocumentState::Alive;
    }
    return true;
}

static bool set_item_info(ENGINE_HANDLE *handle, const void *cookie,
                          item* item, const item_info *itm_info)
{
    hash_item* it = (hash_item*)item;
    if (!it) {
        return false;
    }
    it->datatype = itm_info->datatype;
    return true;
}
