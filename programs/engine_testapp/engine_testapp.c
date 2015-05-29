/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <getopt.h>
#include <time.h>
#include <platform/platform.h>
#include "utilities/engine_loader.h"
#include <memcached/engine_testapp.h>
#include <memcached/extension_loggers.h>
#include "mock_server.h"

#include <daemon/alloc_hooks.h>

struct mock_engine {
    ENGINE_HANDLE_V1 me;
    ENGINE_HANDLE_V1 *the_engine;
    TAP_ITERATOR iterator;
};

static bool color_enabled;

#ifndef WIN32
static sig_atomic_t alarmed;

static void alarm_handler(int sig) {
    alarmed = 1;
}
#endif

static struct mock_engine* get_handle(ENGINE_HANDLE* handle) {
    return (struct mock_engine*)handle;
}

static tap_event_t mock_tap_iterator(ENGINE_HANDLE* handle,
                                     const void *cookie, item **itm,
                                     void **es, uint16_t *nes, uint8_t *ttl,
                                     uint16_t *flags, uint32_t *seqno,
                                     uint16_t *vbucket) {
   struct mock_engine *me = get_handle(handle);
   return me->iterator((ENGINE_HANDLE*)me->the_engine, cookie, itm, es, nes,
                       ttl, flags, seqno, vbucket);
}

static const engine_info* mock_get_info(ENGINE_HANDLE* handle) {
    struct mock_engine *me = get_handle(handle);
    return me->the_engine->get_info((ENGINE_HANDLE*)me->the_engine);
}

static ENGINE_ERROR_CODE mock_initialize(ENGINE_HANDLE* handle,
                                         const char* config_str) {
    struct mock_engine *me = get_handle(handle);
    return me->the_engine->initialize((ENGINE_HANDLE*)me->the_engine, config_str);
}

static void mock_destroy(ENGINE_HANDLE* handle, const bool force) {
    struct mock_engine *me = get_handle(handle);
    me->the_engine->destroy((ENGINE_HANDLE*)me->the_engine, force);
}

static ENGINE_ERROR_CODE mock_allocate(ENGINE_HANDLE* handle,
                                       const void* cookie,
                                       item **item,
                                       const void* key,
                                       const size_t nkey,
                                       const size_t nbytes,
                                       const int flags,
                                       const rel_time_t exptime,
                                       uint8_t datatype) {
    struct mock_engine *me = get_handle(handle);
    struct mock_connstruct *c = (void*)cookie;
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    if (c == NULL) {
        c = (void*)create_mock_cookie();
    }

    c->nblocks = 0;
    cb_mutex_enter(&c->mutex);
    while (ret == ENGINE_SUCCESS &&
           (ret = me->the_engine->allocate((ENGINE_HANDLE*)me->the_engine, c,
                                           item, key, nkey,
                                           nbytes, flags,
                                           exptime, datatype)) == ENGINE_EWOULDBLOCK &&
           c->handle_ewouldblock)
    {
        ++c->nblocks;
        cb_cond_wait(&c->cond, &c->mutex);
        ret = c->status;
    }
    cb_mutex_exit(&c->mutex);

    if (c != cookie) {
        destroy_mock_cookie(c);
    }

    return ret;
}

static ENGINE_ERROR_CODE mock_remove(ENGINE_HANDLE* handle,
                                     const void* cookie,
                                     const void* key,
                                     const size_t nkey,
                                     uint64_t* cas,
                                     uint16_t vbucket,
                                     mutation_descr_t* mut_info)
{
    struct mock_engine *me = get_handle(handle);
    struct mock_connstruct *c = (void*)cookie;
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    if (c == NULL) {
        c = (void*)create_mock_cookie();
    }

    c->nblocks = 0;
    cb_mutex_enter(&c->mutex);
    while (ret == ENGINE_SUCCESS &&
           (ret = me->the_engine->remove((ENGINE_HANDLE*)me->the_engine, c, key,
                                         nkey, cas, vbucket, mut_info)) == ENGINE_EWOULDBLOCK &&
           c->handle_ewouldblock)
    {
        ++c->nblocks;
        cb_cond_wait(&c->cond, &c->mutex);
        ret = c->status;
    }
    cb_mutex_exit(&c->mutex);

    if (c != cookie) {
        destroy_mock_cookie(c);
    }

    return ret;
}

static void mock_release(ENGINE_HANDLE* handle,
                         const void *cookie,
                         item* item) {
    struct mock_engine *me = get_handle(handle);
    me->the_engine->release((ENGINE_HANDLE*)me->the_engine, cookie, item);
}

static ENGINE_ERROR_CODE mock_get(ENGINE_HANDLE* handle,
                                  const void* cookie,
                                  item** item,
                                  const void* key,
                                  const int nkey,
                                  uint16_t vbucket) {
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    struct mock_engine *me = get_handle(handle);
    struct mock_connstruct *c = (void*)cookie;
    if (c == NULL) {
        c = (void*)create_mock_cookie();
    }

    c->nblocks = 0;
    cb_mutex_enter(&c->mutex);
    while (ret == ENGINE_SUCCESS &&
           (ret = me->the_engine->get((ENGINE_HANDLE*)me->the_engine, c, item,
                                      key, nkey, vbucket)) == ENGINE_EWOULDBLOCK &&
           c->handle_ewouldblock)
    {
        ++c->nblocks;
        cb_cond_wait(&c->cond, &c->mutex);
        ret = c->status;
    }
    cb_mutex_exit(&c->mutex);

    if (c != cookie) {
        destroy_mock_cookie(c);
    }

    return ret;
}

static ENGINE_ERROR_CODE mock_get_stats(ENGINE_HANDLE* handle,
                                        const void* cookie,
                                        const char* stat_key,
                                        int nkey,
                                        ADD_STAT add_stat)
{
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    struct mock_engine *me = get_handle(handle);
    struct mock_connstruct *c = (void*)cookie;
    if (c == NULL) {
        c = (void*)create_mock_cookie();
    }

    c->nblocks = 0;
    cb_mutex_enter(&c->mutex);
    while (ret == ENGINE_SUCCESS &&
           (ret = me->the_engine->get_stats((ENGINE_HANDLE*)me->the_engine, c, stat_key,
                                            nkey, add_stat)) == ENGINE_EWOULDBLOCK &&
           c->handle_ewouldblock)
    {
        ++c->nblocks;
        cb_cond_wait(&c->cond, &c->mutex);
        ret = c->status;
    }
    cb_mutex_exit(&c->mutex);

    if (c != cookie) {
        destroy_mock_cookie(c);
    }

    return ret;
}

static ENGINE_ERROR_CODE mock_store(ENGINE_HANDLE* handle,
                                    const void *cookie,
                                    item* item,
                                    uint64_t *cas,
                                    ENGINE_STORE_OPERATION operation,
                                    uint16_t vbucket) {
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    struct mock_engine *me = get_handle(handle);
    struct mock_connstruct *c = (void*)cookie;
    if (c == NULL) {
        c = (void*)create_mock_cookie();
    }

    c->nblocks = 0;
    cb_mutex_enter(&c->mutex);
    while (ret == ENGINE_SUCCESS &&
           (ret = me->the_engine->store((ENGINE_HANDLE*)me->the_engine, c, item, cas,
                                        operation, vbucket)) == ENGINE_EWOULDBLOCK &&
           c->handle_ewouldblock)
    {
        ++c->nblocks;
        cb_cond_wait(&c->cond, &c->mutex);
        ret = c->status;
    }
    cb_mutex_exit(&c->mutex);

    if (c != cookie) {
        destroy_mock_cookie(c);
    }

    return ret;
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
                                         item  **result_item,
                                         uint8_t datatype,
                                         uint64_t *result,
                                         uint16_t vbucket) {
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    struct mock_engine *me = get_handle(handle);
    struct mock_connstruct *c = (void*)cookie;
    if (c == NULL) {
        c = (void*)create_mock_cookie();
    }

    c->nblocks = 0;
    cb_mutex_enter(&c->mutex);
    while (ret == ENGINE_SUCCESS &&
           (ret = me->the_engine->arithmetic((ENGINE_HANDLE*)me->the_engine, c, key,
                                             nkey, increment, create,
                                             delta, initial, exptime,
                                             result_item, datatype,
                                             result, vbucket)) == ENGINE_EWOULDBLOCK &&
           c->handle_ewouldblock)
    {
        ++c->nblocks;
        cb_cond_wait(&c->cond, &c->mutex);
        ret = c->status;
    }
    cb_mutex_exit(&c->mutex);

    if (c != cookie) {
        destroy_mock_cookie(c);
    }

    return ret;
}

static ENGINE_ERROR_CODE mock_flush(ENGINE_HANDLE* handle,
                                    const void* cookie, time_t when) {
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    struct mock_engine *me = get_handle(handle);
    struct mock_connstruct *c = (void*)cookie;
    if (c == NULL) {
        c = (void*)create_mock_cookie();
    }

    c->nblocks = 0;
    cb_mutex_enter(&c->mutex);
    while (ret == ENGINE_SUCCESS &&
           (ret = me->the_engine->flush((ENGINE_HANDLE*)me->the_engine, c, when)) == ENGINE_EWOULDBLOCK &&
           c->handle_ewouldblock)
    {
        ++c->nblocks;
        cb_cond_wait(&c->cond, &c->mutex);
        ret = c->status;
    }
    cb_mutex_exit(&c->mutex);

    if (c != cookie) {
        destroy_mock_cookie(c);
    }

    return ret;
}

static void mock_reset_stats(ENGINE_HANDLE* handle, const void *cookie) {
    struct mock_engine *me = get_handle(handle);
    me->the_engine->reset_stats((ENGINE_HANDLE*)me->the_engine, cookie);
}

static ENGINE_ERROR_CODE mock_unknown_command(ENGINE_HANDLE* handle,
                                              const void* cookie,
                                              protocol_binary_request_header *request,
                                              ADD_RESPONSE response)
{
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    struct mock_engine *me = get_handle(handle);
    struct mock_connstruct *c = (void*)cookie;
    if (c == NULL) {
        c = (void*)create_mock_cookie();
    }

    c->nblocks = 0;
    cb_mutex_enter(&c->mutex);
    while (ret == ENGINE_SUCCESS &&
           (ret = me->the_engine->unknown_command((ENGINE_HANDLE*)me->the_engine, c,
                                                  request, response)) == ENGINE_EWOULDBLOCK &&
           c->handle_ewouldblock)
    {
        ++c->nblocks;
        cb_cond_wait(&c->cond, &c->mutex);
        ret = c->status;
    }
    cb_mutex_exit(&c->mutex);

    if (c != cookie) {
        destroy_mock_cookie(c);
    }

    return ret;
}

static void mock_item_set_cas(ENGINE_HANDLE *handle, const void *cookie,
                              item* item, uint64_t val)
{
    struct mock_engine *me = get_handle(handle);
    me->the_engine->item_set_cas((ENGINE_HANDLE*)me->the_engine, cookie, item, val);
}


static bool mock_get_item_info(ENGINE_HANDLE *handle, const void *cookie,
                               const item* item, item_info *item_info)
{
    struct mock_engine *me = get_handle(handle);
    return me->the_engine->get_item_info((ENGINE_HANDLE*)me->the_engine,
                                         cookie, item, item_info);
}

static void *mock_get_stats_struct(ENGINE_HANDLE* handle, const void* cookie)
{
    struct mock_engine *me = get_handle(handle);
    return me->the_engine->get_stats_struct((ENGINE_HANDLE*)me->the_engine, cookie);
}

static ENGINE_ERROR_CODE mock_aggregate_stats(ENGINE_HANDLE* handle,
                                              const void* cookie,
                                              void (*callback)(void*, void*),
                                              void *vptr)
{
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    struct mock_engine *me = get_handle(handle);
    struct mock_connstruct *c = (void*)cookie;
    if (c == NULL) {
        c = (void*)create_mock_cookie();
    }

    c->nblocks = 0;
    cb_mutex_enter(&c->mutex);
    while (ret == ENGINE_SUCCESS &&
           (ret = me->the_engine->aggregate_stats((ENGINE_HANDLE*)me->the_engine, c,
                                                  callback, vptr)) == ENGINE_EWOULDBLOCK &&
           c->handle_ewouldblock)
    {
        ++c->nblocks;
        cb_cond_wait(&c->cond, &c->mutex);
        ret = c->status;
    }
    cb_mutex_exit(&c->mutex);

    if (c != cookie) {
        destroy_mock_cookie(c);
    }

    return ret;
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
                                        uint8_t datatype,
                                        const void *data,
                                        size_t ndata,
                                        uint16_t vbucket) {

    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    struct mock_engine *me = get_handle(handle);
    struct mock_connstruct *c = (void*)cookie;
    if (c == NULL) {
        c = (void*)create_mock_cookie();
    }

    c->nblocks = 0;
    cb_mutex_enter(&c->mutex);
    while (ret == ENGINE_SUCCESS &&
           (ret = me->the_engine->tap_notify((ENGINE_HANDLE*)me->the_engine, c,
                                             engine_specific, nengine, ttl, tap_flags,
                                             tap_event, tap_seqno, key, nkey, flags,
                                             exptime, cas, datatype,
                                             data, ndata, vbucket)) == ENGINE_EWOULDBLOCK &&
           c->handle_ewouldblock)
    {
        ++c->nblocks;
        cb_cond_wait(&c->cond, &c->mutex);
        ret = c->status;
    }
    cb_mutex_exit(&c->mutex);

    if (c != cookie) {
        destroy_mock_cookie(c);
    }

    return ret;
}


static TAP_ITERATOR mock_get_tap_iterator(ENGINE_HANDLE* handle, const void* cookie,
                                           const void* client, size_t nclient,
                                           uint32_t flags,
                                           const void* userdata, size_t nuserdata) {
    struct mock_engine *me = get_handle(handle);
    me->iterator = me->the_engine->get_tap_iterator((ENGINE_HANDLE*)me->the_engine, cookie,
                                                    client, nclient, flags, userdata, nuserdata);
    return (me->iterator != NULL) ? mock_tap_iterator : NULL;
}

static ENGINE_ERROR_CODE mock_dcp_step(ENGINE_HANDLE* handle,
                                       const void* cookie,
                                       struct dcp_message_producers *producers) {
    struct mock_engine *me = get_handle(handle);
    return me->the_engine->dcp.step((ENGINE_HANDLE*)me->the_engine, cookie,
                                    producers);
}

static ENGINE_ERROR_CODE mock_dcp_open(ENGINE_HANDLE* handle,
                                       const void* cookie,
                                       uint32_t opaque,
                                       uint32_t seqno,
                                       uint32_t flags,
                                       void *name,
                                       uint16_t nname) {
    struct mock_engine *me = get_handle(handle);
    return me->the_engine->dcp.open((ENGINE_HANDLE*)me->the_engine, cookie,
                                    opaque, seqno, flags, name, nname);
}

static ENGINE_ERROR_CODE mock_dcp_add_stream(ENGINE_HANDLE* handle,
                                             const void* cookie,
                                             uint32_t opaque,
                                             uint16_t vbucket,
                                             uint32_t flags) {

    struct mock_engine *me = get_handle(handle);
    struct mock_connstruct *c = (void*)cookie;
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    if (c == NULL) {
        c = (void*)create_mock_cookie();
    }

    c->nblocks = 0;
    cb_mutex_enter(&c->mutex);
    while (ret == ENGINE_SUCCESS &&
           (ret = me->the_engine->dcp.add_stream((ENGINE_HANDLE*)me->the_engine, c,
                                                 opaque, vbucket, flags))
                                                == ENGINE_EWOULDBLOCK &&
           c->handle_ewouldblock)
        {
            ++c->nblocks;
            cb_cond_wait(&c->cond, &c->mutex);
            ret = c->status;
        }
    cb_mutex_exit(&c->mutex);

    if (c != cookie) {
        destroy_mock_cookie(c);
    }

    return ret;
}

static ENGINE_ERROR_CODE mock_dcp_close_stream(ENGINE_HANDLE* handle,
                                               const void* cookie,
                                               uint32_t opaque,
                                               uint16_t vbucket) {
    struct mock_engine *me = get_handle(handle);
    return me->the_engine->dcp.close_stream((ENGINE_HANDLE*)me->the_engine,
                                            cookie, opaque, vbucket);
}

static ENGINE_ERROR_CODE mock_dcp_stream_req(ENGINE_HANDLE* handle,
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
                                             dcp_add_failover_log callback) {
    struct mock_engine *me = get_handle(handle);
    return me->the_engine->dcp.stream_req((ENGINE_HANDLE*)me->the_engine,
                                          cookie, flags, opaque, vbucket,
                                          start_seqno, end_seqno, vbucket_uuid,
                                          snap_start_seqno, snap_end_seqno,
                                          rollback_seqno, callback);
}

static ENGINE_ERROR_CODE mock_dcp_get_failover_log(ENGINE_HANDLE* handle,
                                                   const void* cookie,
                                                   uint32_t opaque,
                                                   uint16_t vbucket,
                                                   dcp_add_failover_log cb) {
    struct mock_engine *me = get_handle(handle);
    return me->the_engine->dcp.get_failover_log((ENGINE_HANDLE*)me->the_engine,
                                                cookie, opaque, vbucket, cb);
}

static ENGINE_ERROR_CODE mock_dcp_stream_end(ENGINE_HANDLE* handle,
                                             const void* cookie,
                                             uint32_t opaque,
                                             uint16_t vbucket,
                                             uint32_t flags) {
    struct mock_engine *me = get_handle(handle);
    return me->the_engine->dcp.stream_end((ENGINE_HANDLE*)me->the_engine,
                                          cookie, opaque, vbucket, flags);
}

static ENGINE_ERROR_CODE mock_dcp_snapshot_marker(ENGINE_HANDLE* handle,
                                                  const void* cookie,
                                                  uint32_t opaque,
                                                  uint16_t vbucket,
                                                  uint64_t start_seqno,
                                                  uint64_t end_seqno,
                                                  uint32_t flags) {
    struct mock_engine *me = get_handle(handle);
    return me->the_engine->dcp.snapshot_marker((ENGINE_HANDLE*)me->the_engine,
                                               cookie, opaque, vbucket,
                                               start_seqno, end_seqno, flags);
}

static ENGINE_ERROR_CODE mock_dcp_mutation(ENGINE_HANDLE* handle,
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
                                           uint64_t bySeqno,
                                           uint64_t revSeqno,
                                           uint32_t expiration,
                                           uint32_t lockTime,
                                           const void *meta,
                                           uint16_t nmeta,
                                           uint8_t nru) {

    struct mock_engine *me = get_handle(handle);
    struct mock_connstruct *c = (void*)cookie;
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    if (c == NULL) {
        c = (void*)create_mock_cookie();
    }

    c->nblocks = 0;
    cb_mutex_enter(&c->mutex);
    while (ret == ENGINE_SUCCESS &&
           (ret = me->the_engine->dcp.mutation((ENGINE_HANDLE*)me->the_engine, c, opaque, key,
                                               nkey, value, nvalue, cas, vbucket, flags,
                                               datatype, bySeqno, revSeqno, expiration,
                                               lockTime, meta, nmeta, nru)) == ENGINE_EWOULDBLOCK &&
           c->handle_ewouldblock)
        {
            ++c->nblocks;
            cb_cond_wait(&c->cond, &c->mutex);
            ret = c->status;
        }
    cb_mutex_exit(&c->mutex);

    if (c != cookie) {
        destroy_mock_cookie(c);
    }

    return ret;
}

static ENGINE_ERROR_CODE mock_dcp_deletion(ENGINE_HANDLE* handle,
                                           const void* cookie,
                                           uint32_t opaque,
                                           const void *key,
                                           uint16_t nkey,
                                           uint64_t cas,
                                           uint16_t vbucket,
                                           uint64_t bySeqno,
                                           uint64_t revSeqno,
                                           const void *meta,
                                           uint16_t nmeta) {

    struct mock_engine *me = get_handle(handle);
    struct mock_connstruct *c = (void*)cookie;
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    if (c == NULL) {
        c = (void*)create_mock_cookie();
    }

    c->nblocks = 0;
    cb_mutex_enter(&c->mutex);
    while (ret == ENGINE_SUCCESS &&
           (ret = me->the_engine->dcp.deletion((ENGINE_HANDLE*)me->the_engine, c,
                                               opaque, key, nkey, cas, vbucket, bySeqno,
                                               revSeqno, meta, nmeta)) == ENGINE_EWOULDBLOCK &&
           c->handle_ewouldblock)
        {
            ++c->nblocks;
            cb_cond_wait(&c->cond, &c->mutex);
            ret = c->status;
        }
    cb_mutex_exit(&c->mutex);

    if (c != cookie) {
        destroy_mock_cookie(c);
    }

    return ret;
}

static ENGINE_ERROR_CODE mock_dcp_expiration(ENGINE_HANDLE* handle,
                                             const void* cookie,
                                             uint32_t opaque,
                                             const void *key,
                                             uint16_t nkey,
                                             uint64_t cas,
                                             uint16_t vbucket,
                                             uint64_t bySeqno,
                                             uint64_t revSeqno,
                                             const void *meta,
                                             uint16_t nmeta) {

    struct mock_engine *me = get_handle(handle);
    struct mock_connstruct *c = (void*)cookie;
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    if (c == NULL) {
        c = (void*)create_mock_cookie();
    }

    c->nblocks = 0;
    cb_mutex_enter(&c->mutex);
    while (ret == ENGINE_SUCCESS &&
           (ret = me->the_engine->dcp.expiration((ENGINE_HANDLE*)me->the_engine, c,
                                                 opaque, key, nkey, cas, vbucket, bySeqno,
                                                 revSeqno, meta, nmeta)) == ENGINE_EWOULDBLOCK &&
           c->handle_ewouldblock)
        {
            ++c->nblocks;
            cb_cond_wait(&c->cond, &c->mutex);
            ret = c->status;
        }
    cb_mutex_exit(&c->mutex);

    if (c != cookie) {
        destroy_mock_cookie(c);
    }

    return ret;
}

static ENGINE_ERROR_CODE mock_dcp_flush(ENGINE_HANDLE* handle,
                                        const void* cookie,
                                        uint32_t opaque,
                                        uint16_t vbucket) {

    struct mock_engine *me = get_handle(handle);
    struct mock_connstruct *c = (void*)cookie;
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    if (c == NULL) {
        c = (void*)create_mock_cookie();
    }

    c->nblocks = 0;
    cb_mutex_enter(&c->mutex);
    while (ret == ENGINE_SUCCESS &&
           (ret = me->the_engine->dcp.flush((ENGINE_HANDLE*)me->the_engine, c, opaque,
                                            vbucket)) == ENGINE_EWOULDBLOCK &&
           c->handle_ewouldblock)
        {
            ++c->nblocks;
            cb_cond_wait(&c->cond, &c->mutex);
            ret = c->status;
        }
    cb_mutex_exit(&c->mutex);

    if (c != cookie) {
        destroy_mock_cookie(c);
    }

    return ret;
}

static ENGINE_ERROR_CODE mock_dcp_set_vbucket_state(ENGINE_HANDLE* handle,
                                                    const void* cookie,
                                                    uint32_t opaque,
                                                    uint16_t vbucket,
                                                    vbucket_state_t state) {
    struct mock_engine *me = get_handle(handle);
    return me->the_engine->dcp.set_vbucket_state((ENGINE_HANDLE*)me->the_engine,
                                                 cookie, opaque, vbucket,
                                                 state);
}

static ENGINE_ERROR_CODE mock_dcp_noop(ENGINE_HANDLE* handle,
                                       const void* cookie,
                                       uint32_t opaque) {
    struct mock_engine *me = get_handle(handle);
    return me->the_engine->dcp.noop((ENGINE_HANDLE*)me->the_engine,
                                    cookie, opaque);
}

static ENGINE_ERROR_CODE mock_dcp_control(ENGINE_HANDLE* handle,
                                          const void* cookie,
                                          uint32_t opaque,
                                          const void *key,
                                          uint16_t nkey,
                                          const void *value,
                                          uint32_t nvalue) {

    struct mock_engine *me = get_handle(handle);
    return me->the_engine->dcp.control((void*)me->the_engine,
                                       cookie, opaque, key,
                                       nkey, value, nvalue);
}

static ENGINE_ERROR_CODE mock_dcp_buffer_acknowledgement(ENGINE_HANDLE* handle,
                                                         const void* cookie,
                                                         uint32_t opaque,
                                                         uint16_t vbucket,
                                                         uint32_t bb) {
    struct mock_engine *me = get_handle(handle);
    return me->the_engine->dcp.buffer_acknowledgement((ENGINE_HANDLE*)me->the_engine,
                                                      cookie, opaque, vbucket, bb);
}

static ENGINE_ERROR_CODE mock_dcp_response_handler(ENGINE_HANDLE* handle,
                                                   const void* cookie,
                                                   protocol_binary_response_header *response) {
    struct mock_engine *me = get_handle(handle);
    return me->the_engine->dcp.response_handler((ENGINE_HANDLE*)me->the_engine,
                                                cookie, response);
}

EXTENSION_LOGGER_DESCRIPTOR *logger_descriptor = NULL;

static void usage(void) {
    printf("\n");
    printf("engine_testapp -E <path_to_engine_lib> -T <path_to_testlib>\n");
    printf("               [-e <engine_config>] [-h] [-X]\n");
    printf("\n");
    printf("-E <path_to_engine_lib>      Path to the engine library file. The\n");
    printf("                             engine library file is a library file\n");
    printf("                             (.so or .dll) that the contains the \n");
    printf("                             implementation of the engine being\n");
    printf("                             tested.\n");
    printf("\n");
    printf("-T <path_to_testlib>         Path to the test library file. The test\n");
    printf("                             library file is a library file (.so or\n");
    printf("                             .dll) that contains the set of tests\n");
    printf("                             to be executed.\n");
    printf("\n");
    printf("-t <timeout>                 Maximum time to run a test.\n");
    printf("-e <engine_config>           Engine configuration string passed to\n");
    printf("                             the engine.\n");
    printf("-q                           Only print errors.");
    printf("-.                           Print a . for each executed test.");
    printf("\n");
    printf("-h                           Prints this usage text.\n");
    printf("-v                           verbose output\n");
    printf("-X                           Use stderr logger instead of /dev/zero");
    printf("\n");
}

static int report_test(const char *name, time_t duration, enum test_result r, bool quiet, bool compact) {
    int rc = 0;
    char *msg = NULL;
    int color = 0;
    char color_str[8] = { 0 };
    const char *reset_color = color_enabled ? "\033[m" : "";

    switch (r) {
    case SUCCESS:
        msg="OK";
        color = 32;
        break;
    case SKIPPED:
        msg="SKIPPED";
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
    case TIMEOUT:
        color = 31;
        msg = "TIMED OUT";
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
        rc = 1;
    }

    cb_assert(msg);
    if (color_enabled) {
        snprintf(color_str, sizeof(color_str), "\033[%dm", color);
    }

    if (quiet) {
        if (r != SUCCESS) {
            printf("%s:  (%lu sec) %s%s%s\n", name, (long)duration,
                   color_str, msg, reset_color);
            fflush(stdout);
        }
    } else {
        if (compact && (r == SUCCESS || r == SKIPPED || r == PENDING)) {
            size_t len = strlen(name) + 27; /* for "Running [0/0] xxxx ..." etc */
            size_t ii;

            fprintf(stdout, "\r");
            for (ii = 0; ii < len; ++ii) {
                fprintf(stdout, " ");
            }
            fprintf(stdout, "\r");
            fflush(stdout);
        } else {
            printf("(%lu sec) %s%s%s\n", (long)duration,
                                         color_str, msg, reset_color);
        }
    }
    return rc;
}

static engine_reference* engine_ref = NULL;
static bool start_your_engine(const char *engine) {
    if ((engine_ref = load_engine(engine, logger_descriptor)) == NULL) {
        fprintf(stderr, "Failed to load engine %s.\n", engine);
        return false;
    }
    return true;
}

static void stop_your_engine() {
    unload_engine(engine_ref);
    engine_ref = NULL;
}

static ENGINE_HANDLE_V1* create_bucket(bool initialize, const char* cfg) {
    struct mock_engine* mock_engine = (struct mock_engine*)calloc(1, sizeof(struct mock_engine));
    ENGINE_HANDLE* handle = NULL;

    if (create_engine_instance(engine_ref, &get_mock_server_api, logger_descriptor, &handle)) {

        mock_engine->me.interface.interface = 1;

        mock_engine->me.get_info = mock_get_info;
        mock_engine->me.initialize = mock_initialize;
        mock_engine->me.destroy = mock_destroy;
        mock_engine->me.allocate = mock_allocate;
        mock_engine->me.remove = mock_remove;
        mock_engine->me.release = mock_release;
        mock_engine->me.get = mock_get;
        mock_engine->me.store = mock_store;
        mock_engine->me.arithmetic = mock_arithmetic;
        mock_engine->me.flush = mock_flush;
        mock_engine->me.get_stats = mock_get_stats;
        mock_engine->me.reset_stats = mock_reset_stats;
        mock_engine->me.get_stats_struct = mock_get_stats_struct;
        mock_engine->me.aggregate_stats = mock_aggregate_stats;
        mock_engine->me.unknown_command = mock_unknown_command;
        mock_engine->me.tap_notify = mock_tap_notify;
        mock_engine->me.get_tap_iterator = mock_get_tap_iterator;
        mock_engine->me.item_set_cas = mock_item_set_cas;
        mock_engine->me.get_item_info = mock_get_item_info;
        mock_engine->me.dcp.step = mock_dcp_step;
        mock_engine->me.dcp.open = mock_dcp_open;
        mock_engine->me.dcp.add_stream = mock_dcp_add_stream;
        mock_engine->me.dcp.close_stream = mock_dcp_close_stream;
        mock_engine->me.dcp.stream_req = mock_dcp_stream_req;
        mock_engine->me.dcp.get_failover_log = mock_dcp_get_failover_log;
        mock_engine->me.dcp.stream_end = mock_dcp_stream_end;
        mock_engine->me.dcp.snapshot_marker = mock_dcp_snapshot_marker;
        mock_engine->me.dcp.mutation = mock_dcp_mutation;
        mock_engine->me.dcp.deletion = mock_dcp_deletion;
        mock_engine->me.dcp.expiration = mock_dcp_expiration;
        mock_engine->me.dcp.flush = mock_dcp_flush;
        mock_engine->me.dcp.set_vbucket_state = mock_dcp_set_vbucket_state;
        mock_engine->me.dcp.noop = mock_dcp_noop;
        mock_engine->me.dcp.buffer_acknowledgement = mock_dcp_buffer_acknowledgement;
        mock_engine->me.dcp.control = mock_dcp_control;
        mock_engine->me.dcp.response_handler = mock_dcp_response_handler;

        mock_engine->the_engine = (ENGINE_HANDLE_V1*)handle;

        /* Reset all members that aren't set (to allow the users to write */
        /* testcases to verify that they initialize them.. */
        cb_assert(mock_engine->me.interface.interface == mock_engine->the_engine->interface.interface);

        if (mock_engine->the_engine->get_stats_struct == NULL) {
            mock_engine->me.get_stats_struct = NULL;
        }
        if (mock_engine->the_engine->aggregate_stats == NULL) {
            mock_engine->me.aggregate_stats = NULL;
        }
        if (mock_engine->the_engine->unknown_command == NULL) {
            mock_engine->me.unknown_command = NULL;
        }
        if (mock_engine->the_engine->tap_notify == NULL) {
            mock_engine->me.tap_notify = NULL;
        }
        if (mock_engine->the_engine->get_tap_iterator == NULL) {
            mock_engine->me.get_tap_iterator = NULL;
        }

        if (initialize) {
            if(!init_engine_instance(handle, cfg, logger_descriptor)) {
                fprintf(stderr, "Failed to init engine with config %s.\n", cfg);
                free(mock_engine);
                return NULL;
            }
        }

        return &mock_engine->me;
    } else {
        free(mock_engine);
        return NULL;
    }
}

static void destroy_bucket(ENGINE_HANDLE* handle, ENGINE_HANDLE_V1* handle_v1, bool force) {
    handle_v1->destroy(handle, force);
}

//
// Reload the engine, i.e. the shared object and reallocate a single bucket/instance
//
static void reload_engine(ENGINE_HANDLE **h, ENGINE_HANDLE_V1 **h1,
                          const char* engine, const char *cfg, bool init, bool force) {


    destroy_bucket(*h, *h1, force);
    stop_your_engine();
    start_your_engine(engine);
    *h1 = create_bucket(init, cfg);
    *h = (ENGINE_HANDLE*)(*h1);
}

static void reload_bucket(ENGINE_HANDLE **h, ENGINE_HANDLE_V1 **h1,
                          const char *cfg, bool init, bool force) {
    destroy_bucket(*h, *h1, force);
    *h1 = create_bucket(init, cfg);
    *h = (ENGINE_HANDLE*)(*h1);
}

static engine_test_t* current_testcase;

static const engine_test_t* get_current_testcase(void)
{
    return current_testcase;
}

/* Return how many bytes the memory allocator has mapped in RAM - essentially
 * application-allocated bytes plus memory in allocators own data structures
 * & freelists. This is an approximation of the the application's RSS.
 */
static size_t get_mapped_bytes(void) {
    size_t mapped_bytes;
    allocator_stats stats = {0};
    ALLOCATOR_HOOKS_API* alloc_hooks = get_mock_server_api()->alloc_hooks;
    stats.ext_stats_size = alloc_hooks->get_extra_stats_size();
    stats.ext_stats = calloc(stats.ext_stats_size, sizeof(allocator_ext_stat));

    alloc_hooks->get_allocator_stats(&stats);
    mapped_bytes = stats.heap_size - stats.free_mapped_size
                   - stats.free_unmapped_size;
    free(stats.ext_stats);
    return mapped_bytes;
}

static void release_free_memory(void) {
    get_mock_server_api()->alloc_hooks->release_free_memory();
}

static int execute_test(engine_test_t test,
                        const char *engine,
                        const char *default_cfg)
{
    enum test_result ret = PENDING;
    cb_assert(test.tfun != NULL || test.api_v2.tfun != NULL);
    bool test_api_1 = test.tfun != NULL;

    current_testcase = &test;
    if (test.prepare != NULL) {
        if ((ret = test.prepare(&test)) == SUCCESS) {
            ret = PENDING;
        }
    }

    if (ret == PENDING) {
        init_mock_server();
        /* Start the engine and go */
        if (!start_your_engine(engine)) {
            fprintf(stderr, "Failed to start engine %s\n", engine);
            return FAIL;
        }

        ENGINE_HANDLE_V1* handle_v1 = NULL;
        ENGINE_HANDLE* handle = NULL;
        if (test_api_1) {
            // all test (API1) get 1 bucket and they are welcome to ask for more.
            handle_v1 = create_bucket(true, test.cfg ? test.cfg : default_cfg);
            handle = (ENGINE_HANDLE*)handle_v1;
            if (test.test_setup != NULL && !test.test_setup(handle, handle_v1)) {
                fprintf(stderr, "Failed to run setup for test %s\n", test.name);
                return FAIL;
            }

            ret = test.tfun(handle, handle_v1);

            if (test.test_teardown != NULL && !test.test_teardown(handle, handle_v1)) {
                fprintf(stderr, "WARNING: Failed to run teardown for test %s\n", test.name);
            }

        } else {
            if (test.api_v2.test_setup != NULL && !test.api_v2.test_setup(&test)) {
                fprintf(stderr, "Failed to run setup for test %s\n", test.name);
                return FAIL;
            }


            ret = test.api_v2.tfun(&test);

            if (test.api_v2.test_teardown != NULL && !test.api_v2.test_teardown(&test)) {
                fprintf(stderr, "WARNING: Failed to run teardown for test %s\n", test.name);
            }
        }

        if (handle) {
            destroy_bucket(handle, handle_v1, false);
        }

        stop_your_engine();
        destroy_mock_event_callbacks();

        if (test.cleanup) {
            test.cleanup(&test, ret);
        }
    }

    return (int)ret;
}

static void setup_alarm_handler() {
#ifndef WIN32
    struct sigaction sig_handler;

    sig_handler.sa_handler = alarm_handler;
    sig_handler.sa_flags = 0;
    sigemptyset(&sig_handler.sa_mask);

    sigaction(SIGALRM, &sig_handler, NULL);
#endif
}

static void set_test_timeout(int timeout) {
#ifndef WIN32
    alarm(timeout);
#endif
}

static void clear_test_timeout() {
#ifndef WIN32
    alarm(0);
    alarmed = 0;
#endif
}

static int safe_append(char *buffer, const char *txt) {
    int len = 0;

    /*
     * We should probably make this a bit safer (by
     * checking if its already escaped etc, but I'll
     * do that whenever it turns out to be a problem ;-)
     */
    while (*txt) {
        switch (*txt) {
#ifndef WIN32
        case ' ':
        case '\\':
        case ';':
        case '|':
        case '&':
            buffer[len++] = '\\';
#endif
        default:
            buffer[len++] = *txt;
        }
        ++txt;
    }

    buffer[len++] = ' ';

    return len;
}

int main(int argc, char **argv) {
    int c, exitcode = 0, num_cases = 0, timeout = 0, loop_count = 0;
    bool verbose = false;
    bool quiet = false;
    bool dot = false;
    bool loop = false;
    bool terminate_on_error = false;
    const char *engine = NULL;
    const char *engine_args = NULL;
    const char *test_suite = NULL;
    const char *test_case = NULL;
    engine_test_t *testcases = NULL;
    cb_dlhandle_t handle;
    char *errmsg;
    void *symbol;
    struct test_harness harness;
    int test_case_id = -1;
    char *cmdline;

    /* Hack to remove the warning from C99 */
    union {
        GET_TESTS get_tests;
        void* voidptr;
    } my_get_test;

    /* Hack to remove the warning from C99 */
    union {
        SETUP_SUITE setup_suite;
        void* voidptr;
    } my_setup_suite;

    /* Hack to remove the warning from C99 */
    union {
        TEARDOWN_SUITE teardown_suite;
        void* voidptr;
    } my_teardown_suite;

    cb_initialize_sockets();

    init_alloc_hooks();

    memset(&my_get_test, 0, sizeof(my_get_test));
    memset(&my_setup_suite, 0, sizeof(my_setup_suite));
    memset(&my_teardown_suite, 0, sizeof(my_teardown_suite));

    logger_descriptor = get_null_logger();
    color_enabled = getenv("TESTAPP_ENABLE_COLOR") != NULL;

    /* Use unbuffered stdio */
    setbuf(stdout, NULL);
    setbuf(stderr, NULL);

    setup_alarm_handler();

    /* process arguments */
    while ((c = getopt(argc, argv,
                       "h"  /* usage */
                       "E:" /* Engine to load */
                       "e:" /* Engine options */
                       "T:" /* Library with tests to load */
                       "t:" /* Timeout */
                       "L"  /* Loop until failure */
                       "q"  /* Be more quiet (only report failures) */
                       "."  /* dot mode. */
                       "n:"  /* test case to run */
                       "v" /* verbose output */
                       "Z"  /* Terminate on first error */
                       "C:" /* Test case id */
                       "s" /* spinlock the program */
                       "X" /* Use stderr logger */
                       )) != -1) {
        switch (c) {
        case 's' : {
            int spin = 1;
            while (spin) {

            }
            break;
        }
        case 'C' :
            test_case_id = atoi(optarg);
            break;
        case 'E':
            engine = optarg;
            break;
        case 'e':
            engine_args = optarg;
            break;
        case 'h':
            usage();
            return 0;
        case 'T':
            test_suite = optarg;
            break;
        case 't':
            timeout = atoi(optarg);
            break;
        case 'L':
            loop = true;
            break;
        case 'n':
            test_case = optarg;
            break;
        case 'v' :
            verbose = true;
            break;
        case 'q':
            quiet = true;
            break;
        case '.':
            dot = true;
            break;
        case 'Z' :
            terminate_on_error = true;
            break;
        case 'X':
            logger_descriptor = get_stderr_logger();
            break;
        default:
            fprintf(stderr, "Illegal argument \"%c\"\n", c);
            return 1;
        }
    }

    /* validate args */
    if (engine == NULL) {
        fprintf(stderr, "You must provide a path to the storage engine library.\n");
        return 1;
    }

    if (test_suite == NULL) {
        fprintf(stderr, "You must provide a path to the testsuite library.\n");
        return 1;
    }

    /* load test_suite */
    handle = cb_dlopen(test_suite, &errmsg);
    if (handle == NULL) {
        fprintf(stderr, "Failed to load testsuite %s: %s\n", test_suite,
                errmsg);
        free(errmsg);
        return 1;
    }

    /* get the test cases */
    symbol = cb_dlsym(handle, "get_tests", &errmsg);
    if (symbol == NULL) {
        fprintf(stderr, "Could not find get_tests function in testsuite %s: %s\n", test_suite, errmsg);
        free(errmsg);
        return 1;
    }
    my_get_test.voidptr = symbol;
    testcases = (*my_get_test.get_tests)();

    /* set up the suite if needed */
    memset(&harness, 0, sizeof(harness));
    harness.default_engine_cfg = engine_args;
    harness.engine_path = engine;
    harness.reload_engine = reload_engine;
    harness.create_cookie = create_mock_cookie;
    harness.destroy_cookie = destroy_mock_cookie;
    harness.set_ewouldblock_handling = mock_set_ewouldblock_handling;
    harness.set_mutation_extras_handling = mock_set_mutation_extras_handling;
    harness.set_datatype_support = mock_set_datatype_support;
    harness.lock_cookie = lock_mock_cookie;
    harness.unlock_cookie = unlock_mock_cookie;
    harness.waitfor_cookie = waitfor_mock_cookie;
    harness.time_travel = mock_time_travel;
    harness.get_current_testcase = get_current_testcase;
    harness.get_mapped_bytes = get_mapped_bytes;
    harness.release_free_memory = release_free_memory;
    harness.create_bucket = create_bucket;
    harness.destroy_bucket = destroy_bucket;
    harness.reload_bucket = reload_bucket;


    for (num_cases = 0; testcases[num_cases].name; num_cases++) {
        /* Just counting */
    }

    symbol = cb_dlsym(handle, "setup_suite", &errmsg);
    if (symbol == NULL) {
        free(errmsg);
    } else {
        my_setup_suite.voidptr = symbol;
        if (!(*my_setup_suite.setup_suite)(&harness)) {
            fprintf(stderr, "Failed to set up test suite %s \n", test_suite);
            return 1;
        }
    }

    if (test_case_id != -1) {
        if (test_case_id >= num_cases) {
            fprintf(stderr, "Invalid test case id specified\n");
            exit(EXIT_FAILURE);
        }

        // check there's a test to run, some modules need cleaning up of dead tests
        // if all modules are fixed, this if/else can be removed.
        if (testcases[test_case_id].tfun || testcases[test_case_id].api_v2.tfun) {
            exit(execute_test(testcases[test_case_id], engine, engine_args));
        } else {
            exit(PENDING); // ignored tests would always return PENDING
        }
    }

    cmdline = malloc(64*1024); /* should be enough */
    if (cmdline == NULL) {
        fprintf(stderr, "Failed to allocate memory");
        exit(EXIT_FAILURE);
    }

    do {
        int i;
        bool need_newline = false;
        for (i = 0; testcases[i].name; i++) {
            int error;
            if (test_case != NULL && strcmp(test_case, testcases[i].name) != 0)
                continue;
            if (!quiet) {
                printf("Running [%04d/%04d]: %s...",
                       i + num_cases * loop_count,
                       num_cases * (loop_count + 1),
                       testcases[i].name);
                fflush(stdout);
            } else if(dot) {
                printf(".");
                need_newline = true;
                /* Add a newline every few tests */
                if ((i+1) % 70 == 0) {
                    printf("\n");
                    need_newline = false;
                }
            }
            set_test_timeout(timeout);

            {
                int ii;
                int offset = 0;
                enum test_result ecode;
                time_t start;
                time_t stop;
                int rc;
                for (ii = 0; ii < argc; ++ii) {
                    offset += safe_append(cmdline + offset, argv[ii]);
                }

                sprintf(cmdline + offset, "-C %d", i);

                start = time(NULL);
                rc = system(cmdline);
                stop = time(NULL);

#ifdef WIN32
                ecode = (enum test_result)rc;
#else
                if (WIFEXITED(rc)) {
                    ecode = (enum test_result)WEXITSTATUS(rc);
#ifdef WCOREDUMP
                } else if (WIFSIGNALED(rc) && WCOREDUMP(rc)) {
                    ecode = CORE;
#endif
                } else {
                    ecode = DIED;
                }
#endif
                error = report_test(testcases[i].name,
                                    stop - start,
                                    ecode, quiet,
                                    !verbose);
            }
            clear_test_timeout();

            if (error != 0) {
                ++exitcode;
                if (terminate_on_error) {
                    exit(EXIT_FAILURE);
                }
            }
        }

        if (need_newline) {
            printf("\n");
        }
        ++loop_count;
    } while (loop && exitcode == 0);

    /* tear down the suite if needed */
    symbol = cb_dlsym(handle, "teardown_suite", &errmsg);
    if (symbol == NULL) {
        free(errmsg);
    } else {
        my_teardown_suite.voidptr = symbol;
        if (!(*my_teardown_suite.teardown_suite)()) {
            fprintf(stderr, "Failed to teardown up test suite %s \n", test_suite);
        }
    }

    printf("# Passed %d of %d tests\n", num_cases - exitcode, num_cases);
    free(cmdline);

    return exitcode;
}
