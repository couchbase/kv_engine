/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2014 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

/* The "nobucket" is a bucket that just returnes "ENGINE_NO_BUCKET". This
 * bucket may be set as the "default" bucket for connections to avoid
 * having to check if a bucket is selected or not.
 */
#include "config.h"

#include <stdlib.h>

#include <memcached/engine.h>
#include <memcached/visibility.h>
#include <memcached/util.h>
#include <memcached/config_parser.h>

MEMCACHED_PUBLIC_API
ENGINE_ERROR_CODE create_instance(uint64_t interface,
                                  GET_SERVER_API gsa,
                                  ENGINE_HANDLE **handle);


struct engine {
    ENGINE_HANDLE_V1 engine;
    union {
        engine_info engine_info;
        char buffer[sizeof(engine_info) +
                    (sizeof(feature_info) * LAST_REGISTERED_ENGINE_FEATURE)];
    } info;
};

static struct engine* get_handle(ENGINE_HANDLE* handle)
{
    return (struct engine*)handle;
}

static const engine_info* get_info(ENGINE_HANDLE* handle)
{
    return &get_handle(handle)->info.engine_info;
}

static ENGINE_ERROR_CODE initialize(ENGINE_HANDLE* handle,
                                               const char* config_str)
{
    (void)handle;
    (void)config_str;
    return ENGINE_SUCCESS;
}

static void destroy(ENGINE_HANDLE* handle, const bool force)
{
    (void)force;
    free(handle);
}

static ENGINE_ERROR_CODE item_allocate(ENGINE_HANDLE* handle,
                                                  const void* cookie,
                                                  item **item,
                                                  const void* key,
                                                  const size_t nkey,
                                                  const size_t nbytes,
                                                  const int flags,
                                                  const rel_time_t exptime,
                                                  uint8_t datatype)
{
    return ENGINE_NO_BUCKET;
}

static ENGINE_ERROR_CODE item_delete(ENGINE_HANDLE* handle,
                                                const void* cookie,
                                                const void* key,
                                                const size_t nkey,
                                                uint64_t* cas,
                                                uint16_t vbucket,
                                                mutation_descr_t* mut_info)
{
    return ENGINE_NO_BUCKET;
}

static void item_release(ENGINE_HANDLE* handle,
                                    const void *cookie,
                                    item* item)
{

}

static ENGINE_ERROR_CODE get(ENGINE_HANDLE* handle,
                                        const void* cookie,
                                        item** item,
                                        const void* key,
                                        const int nkey,
                                        uint16_t vbucket)
{
    return ENGINE_NO_BUCKET;
}

static ENGINE_ERROR_CODE get_stats(ENGINE_HANDLE* handle,
                                              const void* cookie,
                                              const char* stat_key,
                                              int nkey,
                                              ADD_STAT add_stat)
{
    return ENGINE_NO_BUCKET;
}

static ENGINE_ERROR_CODE store(ENGINE_HANDLE* handle,
                                          const void *cookie,
                                          item* item,
                                          uint64_t *cas,
                                          ENGINE_STORE_OPERATION operation,
                                          uint16_t vbucket)
{
    return ENGINE_NO_BUCKET;
}

static ENGINE_ERROR_CODE arithmetic(ENGINE_HANDLE* handle,
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
                                               uint16_t vbucket)
{
    return ENGINE_NO_BUCKET;
}

static ENGINE_ERROR_CODE flush(ENGINE_HANDLE* handle,
                                          const void* cookie,
                                          time_t when)
{
    return ENGINE_NO_BUCKET;
}

static void reset_stats(ENGINE_HANDLE* handle,
                                   const void *cookie)
{
}

static ENGINE_ERROR_CODE unknown_command(ENGINE_HANDLE* handle,
                                                    const void* cookie,
                                                    protocol_binary_request_header *request,
                                                    ADD_RESPONSE response)
{
    return ENGINE_NO_BUCKET;
}


static void item_set_cas(ENGINE_HANDLE *handle, const void *cookie,
                         item* item, uint64_t val)
{
}

static bool get_item_info(ENGINE_HANDLE *handle, const void *cookie,
                          const item* item, item_info *item_info)
{
    return false;
}

static bool set_item_info(ENGINE_HANDLE *handle, const void *cookie,
                          item* item, const item_info *itm_info)
{
    return false;
}

static ENGINE_ERROR_CODE tap_notify(ENGINE_HANDLE* handle,
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
                                               uint8_t datatype,
                                               const void *data,
                                               size_t ndata,
                                               uint16_t vbucket) {
    return ENGINE_NO_BUCKET;
}

static TAP_ITERATOR get_tap_iterator(ENGINE_HANDLE* handle,
                                                const void* cookie,
                                                const void* client,
                                                size_t nclient,
                                                uint32_t flags,
                                                const void* userdata,
                                                size_t nuserdata)
{
    return NULL;
}

static ENGINE_ERROR_CODE dcp_step(ENGINE_HANDLE* handle,
                                  const void* cookie,
                                  struct dcp_message_producers *producers)
{
    return ENGINE_NO_BUCKET;
}

static ENGINE_ERROR_CODE dcp_open(ENGINE_HANDLE* handle,
                                  const void* cookie,
                                  uint32_t opaque,
                                  uint32_t seqno,
                                  uint32_t flags,
                                  void *name,
                                  uint16_t nname)
{
    return ENGINE_NO_BUCKET;
}

static ENGINE_ERROR_CODE dcp_add_stream(ENGINE_HANDLE* handle,
                                        const void* cookie,
                                        uint32_t opaque,
                                        uint16_t vbucket,
                                        uint32_t flags)
{
    return ENGINE_NO_BUCKET;
}

static ENGINE_ERROR_CODE dcp_close_stream(ENGINE_HANDLE* handle,
                                          const void* cookie,
                                          uint32_t opaque,
                                          uint16_t vbucket)
{
    return ENGINE_NO_BUCKET;
}

static ENGINE_ERROR_CODE dcp_stream_req(ENGINE_HANDLE* handle,
                                        const void* cookie,
                                        uint32_t flags,
                                        uint32_t opaque,
                                        uint16_t vbucket,
                                        uint64_t start_seqno,
                                        uint64_t end_seqno,
                                        uint64_t vbucket_uuid,
                                        uint64_t snap_start_seqno,
                                        uint64_t snap_end_seqno,
                                        uint64_t *rollback_seqno,
                                        dcp_add_failover_log callback)
{
    return ENGINE_NO_BUCKET;
}

static ENGINE_ERROR_CODE dcp_get_failover_log(ENGINE_HANDLE* handle,
                                              const void* cookie,
                                              uint32_t opaque,
                                              uint16_t vbucket,
                                              ENGINE_ERROR_CODE (*fl)(vbucket_failover_t*,
                                                                      size_t nentries,
                                                                      const void *cookie))
{
    return ENGINE_NO_BUCKET;
}

static ENGINE_ERROR_CODE dcp_stream_end(ENGINE_HANDLE* handle,
                                        const void* cookie,
                                        uint32_t opaque,
                                        uint16_t vbucket,
                                        uint32_t flags)
{
    return ENGINE_NO_BUCKET;
}

static ENGINE_ERROR_CODE dcp_snapshot_marker(ENGINE_HANDLE* handle,
                                             const void* cookie,
                                             uint32_t opaque,
                                             uint16_t vbucket,
                                             uint64_t start_seqno,
                                             uint64_t end_seqno,
                                             uint32_t flags)
{
    return ENGINE_NO_BUCKET;
}

static ENGINE_ERROR_CODE dcp_mutation(ENGINE_HANDLE* handle,
                                      const void* cookie,
                                      uint32_t opaque,
                                      const void *key,
                                      uint16_t nkey,
                                      const void *value,
                                      uint32_t nvalue,
                                      uint64_t cas,
                                      uint16_t vbucket,
                                      uint32_t flags,
                                      uint8_t datatype,
                                      uint64_t by_seqno,
                                      uint64_t rev_seqno,
                                      uint32_t expiration,
                                      uint32_t lock_time,
                                      const void *meta,
                                      uint16_t nmeta,
                                      uint8_t nru)
{
    return ENGINE_NO_BUCKET;
}

static ENGINE_ERROR_CODE dcp_deletion(ENGINE_HANDLE* handle,
                                      const void* cookie,
                                      uint32_t opaque,
                                      const void *key,
                                      uint16_t nkey,
                                      uint64_t cas,
                                      uint16_t vbucket,
                                      uint64_t by_seqno,
                                      uint64_t rev_seqno,
                                      const void *meta,
                                      uint16_t nmeta)
{
    return ENGINE_NO_BUCKET;
}

static ENGINE_ERROR_CODE dcp_expiration(ENGINE_HANDLE* handle,
                                        const void* cookie,
                                        uint32_t opaque,
                                        const void *key,
                                        uint16_t nkey,
                                        uint64_t cas,
                                        uint16_t vbucket,
                                        uint64_t by_seqno,
                                        uint64_t rev_seqno,
                                        const void *meta,
                                        uint16_t nmeta)
{
    return ENGINE_NO_BUCKET;
}

static  ENGINE_ERROR_CODE dcp_flush(ENGINE_HANDLE* handle,
                                    const void* cookie,
                                    uint32_t opaque,
                                    uint16_t vbucket)
{
    return ENGINE_NO_BUCKET;
}

static ENGINE_ERROR_CODE dcp_set_vbucket_state(ENGINE_HANDLE* handle,
                                               const void* cookie,
                                               uint32_t opaque,
                                               uint16_t vbucket,
                                               vbucket_state_t state)
{
    return ENGINE_NO_BUCKET;
}

ENGINE_ERROR_CODE create_instance(uint64_t interface,
                                  GET_SERVER_API gsa,
                                  ENGINE_HANDLE **handle)
{
    struct engine *engine;
    (void)gsa;

    if (interface != 1) {
        return ENGINE_ENOTSUP;
    }

    if ((engine = calloc(1, sizeof(*engine))) == NULL) {
        return ENGINE_ENOMEM;
    }

    engine->engine.interface.interface = 1;
    engine->engine.get_info = get_info;
    engine->engine.initialize = initialize;
    engine->engine.destroy = destroy;
    engine->engine.allocate = item_allocate;
    engine->engine.remove = item_delete;
    engine->engine.release = item_release;
    engine->engine.get = get;
    engine->engine.get_stats = get_stats;
    engine->engine.reset_stats = reset_stats;
    engine->engine.store = store;
    engine->engine.arithmetic = arithmetic;
    engine->engine.flush = flush;
    engine->engine.unknown_command = unknown_command;
    engine->engine.tap_notify = tap_notify;
    engine->engine.get_tap_iterator = get_tap_iterator;
    engine->engine.item_set_cas = item_set_cas;
    engine->engine.get_item_info = get_item_info;
    engine->engine.set_item_info = set_item_info;
    engine->engine.dcp.step = dcp_step;
    engine->engine.dcp.open = dcp_open;
    engine->engine.dcp.add_stream = dcp_add_stream;
    engine->engine.dcp.close_stream = dcp_close_stream;
    engine->engine.dcp.get_failover_log = dcp_get_failover_log;
    engine->engine.dcp.stream_req = dcp_stream_req;
    engine->engine.dcp.stream_end = dcp_stream_end;
    engine->engine.dcp.snapshot_marker = dcp_snapshot_marker;
    engine->engine.dcp.mutation = dcp_mutation;
    engine->engine.dcp.deletion = dcp_deletion;
    engine->engine.dcp.expiration = dcp_expiration;
    engine->engine.dcp.flush = dcp_flush;
    engine->engine.dcp.set_vbucket_state = dcp_set_vbucket_state;
    engine->info.engine_info.description = "Disconnect engine v1.0";
    engine->info.engine_info.num_features = 0;
    *handle = (void*)&engine->engine;
    return ENGINE_SUCCESS;
}
