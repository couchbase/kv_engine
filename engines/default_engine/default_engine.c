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
#include "engines/default_engine.h"
#include "engine_manager.h"

static const engine_info* default_get_info(ENGINE_HANDLE* handle);
static ENGINE_ERROR_CODE default_initialize(ENGINE_HANDLE* handle,
                                            const char* config_str);
static void default_destroy(ENGINE_HANDLE* handle,
                            const bool force);
static ENGINE_ERROR_CODE default_item_allocate(ENGINE_HANDLE* handle,
                                               const void* cookie,
                                               item **item,
                                               const void* key,
                                               const size_t nkey,
                                               const size_t nbytes,
                                               const int flags,
                                               const rel_time_t exptime,
                                               uint8_t datatype);
static ENGINE_ERROR_CODE default_item_delete(ENGINE_HANDLE* handle,
                                             const void* cookie,
                                             const void* key,
                                             const size_t nkey,
                                             uint64_t* cas,
                                             uint16_t vbucket,
                                             mutation_descr_t* mut_info);

static void default_item_release(ENGINE_HANDLE* handle, const void *cookie,
                                 item* item);
static ENGINE_ERROR_CODE default_get(ENGINE_HANDLE* handle,
                                     const void* cookie,
                                     item** item,
                                     const void* key,
                                     const int nkey,
                                     uint16_t vbucket);
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
                                       uint16_t vbucket);
static ENGINE_ERROR_CODE default_arithmetic(ENGINE_HANDLE* handle,
                                            const void* cookie,
                                            const void* key,
                                            const int nkey,
                                            const bool increment,
                                            const bool create,
                                            const uint64_t delta,
                                            const uint64_t initial,
                                            const rel_time_t exptime,
                                            item **item,
                                            uint8_t datatype,
                                            uint64_t *result,
                                            uint16_t vbucket);
static ENGINE_ERROR_CODE default_flush(ENGINE_HANDLE* handle,
                                       const void* cookie, time_t when);
static ENGINE_ERROR_CODE initalize_configuration(struct default_engine *se,
                                                 const char *cfg_str);
static ENGINE_ERROR_CODE default_unknown_command(ENGINE_HANDLE* handle,
                                                 const void* cookie,
                                                 protocol_binary_request_header *request,
                                                 ADD_RESPONSE response);


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
    return vi.v.state;
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

ENGINE_ERROR_CODE create_instance(uint64_t interface,
                                  GET_SERVER_API get_server_api,
                                  ENGINE_HANDLE **handle) {
   SERVER_HANDLE_V1 *api = get_server_api();
   struct default_engine *engine;

   /*
      Node local bucket-ID which persists for as long as the process.
      TODO: memcache will manage this and pass the value via create_instance and get_bucket_id
   */
   static bucket_id_t bucket_id = 0;

   if (interface != 1 || api == NULL) {
      return ENGINE_ENOTSUP;
   }

   if ((engine = engine_manager_create_engine()) == NULL) {
      return ENGINE_ENOMEM;
   }

   cb_mutex_initialize(&engine->slabs.lock);
   cb_mutex_initialize(&engine->items.lock);
   cb_mutex_initialize(&engine->stats.lock);
   cb_mutex_initialize(&engine->scrubber.lock);

   /* allocate a bucket_id so we can utilise the global hashtable safely */
   if (bucket_id + 1 == 0) {
      /* we have used all bucket ids! */
      return ENGINE_FAILED;
   }

   engine->bucket_id = bucket_id++;
   engine->engine.interface.interface = 1;
   engine->engine.get_info = default_get_info;
   engine->engine.initialize = default_initialize;
   engine->engine.destroy = default_destroy;
   engine->engine.allocate = default_item_allocate;
   engine->engine.remove = default_item_delete;
   engine->engine.release = default_item_release;
   engine->engine.get = default_get;
   engine->engine.get_stats = default_get_stats;
   engine->engine.reset_stats = default_reset_stats;
   engine->engine.store = default_store;
   engine->engine.arithmetic = default_arithmetic;
   engine->engine.flush = default_flush;
   engine->engine.unknown_command = default_unknown_command;
   engine->engine.item_set_cas = item_set_cas;
   engine->engine.get_item_info = get_item_info;
   engine->engine.set_item_info = set_item_info;
   engine->server = *api;
   engine->get_server_api = get_server_api;
   engine->initialized = true;
   engine->config.use_cas = true;
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
   *handle = (ENGINE_HANDLE*)&engine->engine;
   return ENGINE_SUCCESS;
}

void destroy_engine() {
    engine_manager_shutdown();
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

   /* fixup feature_info */
   if (se->config.use_cas) {
       se->info.engine.features[se->info.engine.num_features++].feature = ENGINE_FEATURE_CAS;
   }

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

        free(engine->config.uuid);

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
                                               const void* key,
                                               const size_t nkey,
                                               const size_t nbytes,
                                               const int flags,
                                               const rel_time_t exptime,
                                               uint8_t datatype) {
   hash_item *it;

   unsigned int id;
   struct default_engine* engine = get_handle(handle);
   size_t ntotal = sizeof(hash_item) + nkey + nbytes;
   if (engine->config.use_cas) {
      ntotal += sizeof(uint64_t);
   }
   id = slabs_clsid(engine, ntotal);
   if (id == 0) {
      return ENGINE_E2BIG;
   }

   it = item_alloc(engine, key, nkey, flags, engine->server.core->realtime(exptime),
                   (uint32_t)nbytes, cookie, datatype);

   if (it != NULL) {
      *item = it;
      return ENGINE_SUCCESS;
   } else {
      return ENGINE_ENOMEM;
   }
}

static ENGINE_ERROR_CODE default_item_delete(ENGINE_HANDLE* handle,
                                             const void* cookie,
                                             const void* key,
                                             const size_t nkey,
                                             uint64_t* cas,
                                             uint16_t vbucket,
                                             mutation_descr_t* mut_info)
{
   struct default_engine* engine = get_handle(handle);
   hash_item *it;

   VBUCKET_GUARD(engine, vbucket);

   it = item_get(engine, cookie, key, nkey);
   if (it == NULL) {
      return ENGINE_KEY_ENOENT;
   }

   if (*cas == 0 || *cas == item_get_cas(it)) {
      item_unlink(engine, it);
      item_release(engine, it);
   } else {
      item_release(engine, it);
      return ENGINE_KEY_EEXISTS;
   }

   /* vbucket UUID / seqno arn't supported by default engine, so just return
      zeros. */
   mut_info->vbucket_uuid = 0;
   mut_info->seqno = 0;

   return ENGINE_SUCCESS;
}

static void default_item_release(ENGINE_HANDLE* handle,
                                 const void *cookie,
                                 item* item) {
   item_release(get_handle(handle), get_real_item(item));
}

static ENGINE_ERROR_CODE default_get(ENGINE_HANDLE* handle,
                                     const void* cookie,
                                     item** item,
                                     const void* key,
                                     const int nkey,
                                     uint16_t vbucket) {
   struct default_engine *engine = get_handle(handle);
   VBUCKET_GUARD(engine, vbucket);

   *item = item_get(engine, cookie, key, nkey);
   if (*item != NULL) {
      return ENGINE_SUCCESS;
   } else {
      return ENGINE_KEY_ENOENT;
   }
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
      len = sprintf(val, "%"PRIu64, (uint64_t)engine->stats.evictions);
      add_stat("evictions", 9, val, len, cookie);
      len = sprintf(val, "%"PRIu64, (uint64_t)engine->stats.curr_items);
      add_stat("curr_items", 10, val, len, cookie);
      len = sprintf(val, "%"PRIu64, (uint64_t)engine->stats.total_items);
      add_stat("total_items", 11, val, len, cookie);
      len = sprintf(val, "%"PRIu64, (uint64_t)engine->stats.curr_bytes);
      add_stat("bytes", 5, val, len, cookie);
      len = sprintf(val, "%"PRIu64, engine->stats.reclaimed);
      add_stat("reclaimed", 9, val, len, cookie);
      len = sprintf(val, "%"PRIu64, (uint64_t)engine->config.maxbytes);
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
            len = sprintf(val, "%"PRIu64, (uint64_t)diff);
            add_stat("scrubber:last_run", 17, val, len, cookie);
         }

         len = sprintf(val, "%"PRIu64, engine->scrubber.visited);
         add_stat("scrubber:visited", 16, val, len, cookie);
         len = sprintf(val, "%"PRIu64, engine->scrubber.cleaned);
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
                                       uint16_t vbucket) {
    struct default_engine *engine = get_handle(handle);
    VBUCKET_GUARD(engine, vbucket);
    return store_item(engine, get_real_item(item), cas, operation,
                      cookie);
}

static ENGINE_ERROR_CODE default_arithmetic(ENGINE_HANDLE* handle,
                                            const void* cookie,
                                            const void* key,
                                            const int nkey,
                                            const bool increment,
                                            const bool create,
                                            const uint64_t delta,
                                            const uint64_t initial,
                                            const rel_time_t exptime,
                                            item **item,
                                            uint8_t datatype,
                                            uint64_t *result,
                                            uint16_t vbucket) {
   struct default_engine *engine = get_handle(handle);
   VBUCKET_GUARD(engine, vbucket);

   return arithmetic(engine, cookie, key, nkey, increment,
                     create, delta, initial, engine->server.core->realtime(exptime),
                     item, datatype, result);
}

static ENGINE_ERROR_CODE default_flush(ENGINE_HANDLE* handle,
                                       const void* cookie, time_t when) {
   item_flush_expired(get_handle(handle), when);

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
       items[ii].key = "use_cas";
       items[ii].datatype = DT_BOOL;
       items[ii].value.dt_bool = &se->config.use_cas;
       ++ii;

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

       items[ii].key = NULL;
       ++ii;
       cb_assert(ii == 13);
       ret = se->server.core->parse_config(cfg_str, items, stderr);
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
    state = ntohl(state);

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
    state = ntohl(state);

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

    t = (void*)request;
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
                           item_get_cas(item), cookie);
        }
        item_release(e, item);
        return ret;
    }
}

static ENGINE_ERROR_CODE default_unknown_command(ENGINE_HANDLE* handle,
                                                 const void* cookie,
                                                 protocol_binary_request_header *request,
                                                 ADD_RESPONSE response)
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
        sent = set_vbucket(e, cookie, (void*)request, response);
        break;
    case PROTOCOL_BINARY_CMD_GET_VBUCKET:
        sent = get_vbucket(e, cookie, (void*)request, response);
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


uint64_t item_get_cas(const hash_item* item)
{
    if (item->iflag & ITEM_WITH_CAS) {
        return *(uint64_t*)(item + 1);
    }
    return 0;
}

void item_set_cas(ENGINE_HANDLE *handle, const void *cookie,
                  item* item, uint64_t val)
{
    hash_item* it = get_real_item(item);
    if (it->iflag & ITEM_WITH_CAS) {
        *(uint64_t*)(it + 1) = val;
    }
}

hash_key* item_get_key(const hash_item* item)
{
    char *ret = (void*)(item + 1);
    if (item->iflag & ITEM_WITH_CAS) {
        ret += sizeof(uint64_t);
    }

    return (hash_key*)ret;
}

char* item_get_data(const hash_item* item)
{
    const hash_key* key = item_get_key(item);
    return ((char*)key->header.full_key) + hash_key_get_key_len(key);
}

uint8_t item_get_clsid(const hash_item* item)
{
    return 0;
}

static bool get_item_info(ENGINE_HANDLE *handle, const void *cookie,
                          const item* item, item_info *item_info)
{
    hash_item* it = (hash_item*)item;
    const hash_key* key = item_get_key(item);
    if (item_info->nvalue < 1) {
        return false;
    }
    item_info->cas = item_get_cas(it);
    item_info->vbucket_uuid = 0;
    item_info->seqno = 0;
    item_info->exptime = it->exptime;
    item_info->nbytes = it->nbytes;
    item_info->flags = it->flags;
    item_info->clsid = it->slabs_clsid;
    item_info->nkey = hash_key_get_client_key_len(key);
    item_info->nvalue = 1;
    item_info->key = hash_key_get_client_key(key);
    item_info->value[0].iov_base = item_get_data(it);
    item_info->value[0].iov_len = it->nbytes;
    item_info->datatype = it->datatype;
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
