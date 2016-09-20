/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"

#include <memcached/engine_testapp.h>

#include <cstdlib>
#include <getopt.h>
#include <map>
#include <regex>
#include <string>
#include <vector>
#include <functional>

#include "utilities/engine_loader.h"
#include "utilities/terminate_handler.h"
#include "mock_server.h"

#include <daemon/alloc_hooks.h>
#include <memcached/extension_loggers.h>
#include <platform/cb_malloc.h>

struct mock_engine {
    ENGINE_HANDLE_V1 me;
    ENGINE_HANDLE_V1 *the_engine;
    TAP_ITERATOR iterator;
};

static bool color_enabled;
static bool log_to_stderr = false;

#ifndef WIN32
static sig_atomic_t alarmed;

static void alarm_handler(int sig) {
    alarmed = 1;
}
#endif


// The handles for the 'current' engine, as used by
// execute_test. These are global as the testcase may call reload_engine() and that
// needs to update the pointers the new engine, so when execute_test is
// cleaning up it has the correct handles.
static ENGINE_HANDLE_V1* handle_v1 = NULL;
static ENGINE_HANDLE* handle = NULL;


static struct mock_engine* get_handle(ENGINE_HANDLE* handle) {
    return (struct mock_engine*)handle;
}

static ENGINE_HANDLE_V1* get_engine_v1_from_handle(ENGINE_HANDLE* handle) {
    return reinterpret_cast<ENGINE_HANDLE_V1*>(get_handle(handle)->the_engine);
}

static ENGINE_HANDLE* get_engine_from_handle(ENGINE_HANDLE* handle) {
    return reinterpret_cast<ENGINE_HANDLE*>(get_handle(handle)->the_engine);
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

// Helper function to convert a cookie (externally represented as
// void*) to the actual internal type.
static struct mock_connstruct* to_mock_connstruct(const void* cookie) {
    return const_cast<struct mock_connstruct*>
        (reinterpret_cast<const struct mock_connstruct*>(cookie));
}

/**
 * Helper function to return a mock_connstruct, either a new one or
 * an existng one.
 **/
struct mock_connstruct* get_or_create_mock_connstruct(const void* cookie) {
    struct mock_connstruct *c = to_mock_connstruct(cookie);
    if (c == NULL) {
        c = to_mock_connstruct(create_mock_cookie());
    }
    return c;
}

/**
 * Helper function to destroy a mock_connstruct if get_or_create_mock_connstruct
 * created one.
 **/
void check_and_destroy_mock_connstruct(struct mock_connstruct* c, const void* cookie){
    if (c != cookie) {
        destroy_mock_cookie(c);
    }
}

/**
 * EWOULDBLOCK wrapper.
 * Will recall "engine_function" with EWOULDBLOCK retry logic.
 **/
static ENGINE_ERROR_CODE call_engine_and_handle_EWOULDBLOCK(
                         ENGINE_HANDLE* handle,
                         struct mock_connstruct* c,
                         std::function<ENGINE_ERROR_CODE()> engine_function) {
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    c->nblocks = 0;
    cb_mutex_enter(&c->mutex);
    while (ret == ENGINE_SUCCESS &&
           (ret = engine_function()) == ENGINE_EWOULDBLOCK &&
           c->handle_ewouldblock)
    {
        ++c->nblocks;
        cb_cond_wait(&c->cond, &c->mutex);
        ret = c->status;
    }
    cb_mutex_exit(&c->mutex);

    return ret;
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
    struct mock_connstruct *c = get_or_create_mock_connstruct(cookie);
    auto engine_fn = std::bind(get_engine_v1_from_handle(handle)->allocate,
                               get_engine_from_handle(handle),
                               static_cast<const void*>(c),
                               item, key, nkey, nbytes, flags, exptime, datatype);

    ENGINE_ERROR_CODE ret = call_engine_and_handle_EWOULDBLOCK(handle, c, engine_fn);

    check_and_destroy_mock_connstruct(c, cookie);

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
    struct mock_connstruct *c = get_or_create_mock_connstruct(cookie);
    auto engine_fn = std::bind(get_engine_v1_from_handle(handle)->remove,
                               get_engine_from_handle(handle),
                               static_cast<const void*>(c),
                               key, nkey, cas, vbucket, mut_info);

    ENGINE_ERROR_CODE ret = call_engine_and_handle_EWOULDBLOCK(handle, c, engine_fn);

    check_and_destroy_mock_connstruct(c, cookie);

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
    struct mock_connstruct *c = get_or_create_mock_connstruct(cookie);
    auto engine_fn = std::bind(get_engine_v1_from_handle(handle)->get,
                               get_engine_from_handle(handle),
                               static_cast<const void*>(c),
                               item, key, nkey, vbucket);

    ENGINE_ERROR_CODE ret = call_engine_and_handle_EWOULDBLOCK(handle, c, engine_fn);

    check_and_destroy_mock_connstruct(c, cookie);
    return ret;
}

static ENGINE_ERROR_CODE mock_get_stats(ENGINE_HANDLE* handle,
                                        const void* cookie,
                                        const char* stat_key,
                                        int nkey,
                                        ADD_STAT add_stat)
{
    struct mock_connstruct *c = get_or_create_mock_connstruct(cookie);
    auto engine_fn = std::bind(get_engine_v1_from_handle(handle)->get_stats,
                               get_engine_from_handle(handle),
                               static_cast<const void*>(c),
                               stat_key, nkey, add_stat);

    ENGINE_ERROR_CODE ret = call_engine_and_handle_EWOULDBLOCK(handle, c, engine_fn);

    check_and_destroy_mock_connstruct(c, cookie);
    return ret;
}

static ENGINE_ERROR_CODE mock_store(ENGINE_HANDLE* handle,
                                    const void *cookie,
                                    item* item,
                                    uint64_t *cas,
                                    ENGINE_STORE_OPERATION operation,
                                    uint16_t vbucket) {
    struct mock_connstruct *c = get_or_create_mock_connstruct(cookie);
    auto engine_fn = std::bind(get_engine_v1_from_handle(handle)->store,
                               get_engine_from_handle(handle),
                               static_cast<const void*>(c),
                               item, cas, operation, vbucket);

    ENGINE_ERROR_CODE ret = call_engine_and_handle_EWOULDBLOCK(handle, c, engine_fn);

    check_and_destroy_mock_connstruct(c, cookie);
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
    struct mock_connstruct *c = get_or_create_mock_connstruct(cookie);
    auto engine_fn = std::bind(get_engine_v1_from_handle(handle)->arithmetic,
                               get_engine_from_handle(handle),
                               static_cast<const void*>(c),
                               key, nkey, increment, create,
                               delta, initial, exptime, result_item,
                               datatype, result, vbucket);

    ENGINE_ERROR_CODE ret = call_engine_and_handle_EWOULDBLOCK(handle, c, engine_fn);

    check_and_destroy_mock_connstruct(c, cookie);
    return ret;
}

static ENGINE_ERROR_CODE mock_flush(ENGINE_HANDLE* handle,
                                    const void* cookie, time_t when) {
    struct mock_connstruct *c = get_or_create_mock_connstruct(cookie);
    auto engine_fn = std::bind(get_engine_v1_from_handle(handle)->flush,
                               get_engine_from_handle(handle),
                               static_cast<const void*>(c),
                               when);

    ENGINE_ERROR_CODE ret = call_engine_and_handle_EWOULDBLOCK(handle, c, engine_fn);

    check_and_destroy_mock_connstruct(c, cookie);
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
    struct mock_connstruct *c = get_or_create_mock_connstruct(cookie);
    auto engine_fn = std::bind(get_engine_v1_from_handle(handle)->unknown_command,
                               get_engine_from_handle(handle),
                               static_cast<const void*>(c),
                               request, response);

    ENGINE_ERROR_CODE ret = call_engine_and_handle_EWOULDBLOCK(handle, c, engine_fn);

    check_and_destroy_mock_connstruct(c, cookie);
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

    struct mock_connstruct *c = get_or_create_mock_connstruct(cookie);
    auto engine_fn = std::bind(get_engine_v1_from_handle(handle)->tap_notify,
                               get_engine_from_handle(handle),
                               static_cast<const void*>(c),
                               engine_specific, nengine, ttl, tap_flags,
                               tap_event, tap_seqno, key, nkey, flags,
                               exptime, cas, datatype,
                               data, ndata, vbucket);

    ENGINE_ERROR_CODE ret = call_engine_and_handle_EWOULDBLOCK(handle, c, engine_fn);

    check_and_destroy_mock_connstruct(c, cookie);
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

    struct mock_connstruct *c = get_or_create_mock_connstruct(cookie);
    auto engine_fn = std::bind(get_engine_v1_from_handle(handle)->dcp.add_stream,
                               get_engine_from_handle(handle),
                               static_cast<const void*>(c),
                               opaque, vbucket, flags);

    ENGINE_ERROR_CODE ret = call_engine_and_handle_EWOULDBLOCK(handle, c, engine_fn);

    check_and_destroy_mock_connstruct(c, cookie);
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

    struct mock_connstruct *c = get_or_create_mock_connstruct(cookie);
    auto engine_fn = std::bind(get_engine_v1_from_handle(handle)->dcp.mutation,
                               get_engine_from_handle(handle),
                               static_cast<const void*>(c),
                               opaque, key, nkey, value,
                               nvalue, cas, vbucket, flags,
                               datatype, bySeqno, revSeqno, expiration,
                               lockTime, meta, nmeta, nru);

    ENGINE_ERROR_CODE ret = call_engine_and_handle_EWOULDBLOCK(handle, c, engine_fn);

    check_and_destroy_mock_connstruct(c, cookie);
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

    struct mock_connstruct *c = get_or_create_mock_connstruct(cookie);
    auto engine_fn = std::bind(get_engine_v1_from_handle(handle)->dcp.deletion,
                               get_engine_from_handle(handle),
                               static_cast<const void*>(c),
                               opaque, key, nkey, cas, vbucket, bySeqno,
                               revSeqno, meta, nmeta);

    ENGINE_ERROR_CODE ret = call_engine_and_handle_EWOULDBLOCK(handle, c, engine_fn);

    check_and_destroy_mock_connstruct(c, cookie);
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

    struct mock_connstruct *c = get_or_create_mock_connstruct(cookie);
    auto engine_fn = std::bind(get_engine_v1_from_handle(handle)->dcp.expiration,
                               get_engine_from_handle(handle),
                               static_cast<const void*>(c),
                               opaque, key, nkey, cas, vbucket, bySeqno,
                               revSeqno, meta, nmeta);

    ENGINE_ERROR_CODE ret = call_engine_and_handle_EWOULDBLOCK(handle, c, engine_fn);

    check_and_destroy_mock_connstruct(c, cookie);
    return ret;
}

static ENGINE_ERROR_CODE mock_dcp_flush(ENGINE_HANDLE* handle,
                                        const void* cookie,
                                        uint32_t opaque,
                                        uint16_t vbucket) {

    struct mock_connstruct *c = get_or_create_mock_connstruct(cookie);
    auto engine_fn = std::bind(get_engine_v1_from_handle(handle)->dcp.flush,
                               get_engine_from_handle(handle),
                               static_cast<const void*>(c),
                               opaque, vbucket);

    ENGINE_ERROR_CODE ret = call_engine_and_handle_EWOULDBLOCK(handle, c, engine_fn);

    check_and_destroy_mock_connstruct(c, cookie);
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
    ENGINE_HANDLE* h = reinterpret_cast<ENGINE_HANDLE*>(me->the_engine);
    return me->the_engine->dcp.control(h, cookie, opaque, key, nkey, value,
                                       nvalue);
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
    printf("-a <attempts>                Maximum number of attempts for a test.\n");
    printf("-t <timeout>                 Maximum time to run a test.\n");
    printf("-e <engine_config>           Engine configuration string passed to\n");
    printf("                             the engine.\n");
    printf("-q                           Only print errors.");
    printf("-.                           Print a . for each executed test.");
    printf("\n");
    printf("-h                           Prints this usage text.\n");
    printf("-v                           verbose output\n");
    printf("-X                           Use stderr logger instead of /dev/zero\n");
    printf("-n                           Regex specifying name(s) of test(s) to run\n");
}

static int report_test(const char *name, time_t duration, enum test_result r, bool quiet, bool compact) {
    int rc = 0;
    const char *msg = NULL;
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
    case SUCCESS_AFTER_RETRY:
        msg="OK AFTER RETRY";
        color = 33;
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
    if ((engine_ref = load_engine(engine, NULL, NULL, logger_descriptor)) == NULL) {
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
    struct mock_engine* mock_engine = (struct mock_engine*)cb_calloc(1, sizeof(struct mock_engine));
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
                cb_free(mock_engine);
                return NULL;
            }
        }

        return &mock_engine->me;
    } else {
        cb_free(mock_engine);
        return NULL;
    }
}

static void destroy_bucket(ENGINE_HANDLE* handle, ENGINE_HANDLE_V1* handle_v1, bool force) {
    handle_v1->destroy(handle, force);
    cb_free(handle_v1);
}

//
// Reload the engine, i.e. the shared object and reallocate a single bucket/instance
//
static void reload_engine(ENGINE_HANDLE **h, ENGINE_HANDLE_V1 **h1,
                          const char* engine, const char *cfg, bool init, bool force) {

    disconnect_all_mock_connections();
    destroy_bucket(*h, *h1, force);
    destroy_mock_event_callbacks();
    stop_your_engine();
    start_your_engine(engine);
    *h1 = create_bucket(init, cfg);
    *h = (ENGINE_HANDLE*)(*h1);
    handle_v1 = *h1;
    handle = *h;
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
    stats.ext_stats = reinterpret_cast<allocator_ext_stat*>
        (cb_calloc(stats.ext_stats_size, sizeof(allocator_ext_stat)));

    alloc_hooks->get_allocator_stats(&stats);
    mapped_bytes = stats.heap_size - stats.free_mapped_size
                   - stats.free_unmapped_size;
    cb_free(stats.ext_stats);
    return mapped_bytes;
}

static void notify_io_complete(const void *cookie, ENGINE_ERROR_CODE status) {
    get_mock_server_api()->cookie->notify_io_complete(cookie, status);
}

static void release_free_memory(void) {
    get_mock_server_api()->alloc_hooks->release_free_memory();
}

static void store_engine_specific(const void *cookie, void *engine_data) {
    get_mock_server_api()->cookie->store_engine_specific(cookie, engine_data);
}

static int execute_test(engine_test_t test,
                        const char *engine,
                        const char *default_cfg)
{
    enum test_result ret = PENDING;
    cb_assert(test.tfun != NULL || test.api_v2.tfun != NULL);
    bool test_api_1 = test.tfun != NULL;

    /**
     * Combine test.cfg (internal config parameters) and
     * default_cfg (command line parameters) for the test case.
     *
     * default_cfg will have higher priority over test.cfg in
     * case of redundant parameters.
     */
    std::string cfg;
    if (test.cfg != nullptr) {
        if (default_cfg != nullptr) {
            cfg.assign(test.cfg);
            cfg = cfg + ";" + default_cfg + ";";
            std::string token, delimiter(";");
            std::string::size_type i, j;
            std::map<std::string, std::string> map;

            while (cfg.size() != 0 &&
                    (i = cfg.find(delimiter)) != std::string::npos) {
                std::string temp(cfg.substr(0, i));
                cfg.erase(0, i + 1);
                j = temp.find("=");
                if (j == std::string::npos) {
                    continue;
                }
                std::string k(temp.substr(0, j));
                std::string v(temp.substr(j + 1, temp.size()));
                map[k] = v;
            }
            cfg.clear();
            std::map<std::string, std::string>::iterator it;
            for (it = map.begin(); it != map.end(); ++it) {
                cfg = cfg + it->first + "=" + it->second + ";";
            }
            test.cfg = cfg.c_str();
        }
    } else if (default_cfg != nullptr) {
        test.cfg = default_cfg;
    }

    current_testcase = &test;
    if (test.prepare != NULL) {
        if ((ret = test.prepare(&test)) == SUCCESS) {
            ret = PENDING;
        }
    }

    if (ret == PENDING) {
        init_mock_server(log_to_stderr);
        if (memcached_initialize_stderr_logger(get_mock_server_api) != EXTENSION_SUCCESS) {
            fprintf(stderr, "Failed to initialize log system\n");
            return 1;
        }

        /* Start the engine and go */
        if (!start_your_engine(engine)) {
            fprintf(stderr, "Failed to start engine %s\n", engine);
            return FAIL;
        }

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

        destroy_mock_event_callbacks();
        stop_your_engine();

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

/* Spawn a new process, wait for it to exit and return it's exit code.
 * @param argc Number of elements in argv; must be at least 1 (argv[0]
 *             specifies the name of the executable to run).
 * @param argv NULL-terminated array of arguments to the process; with
 *             the first element being the executable to run.
 */
static int spawn_and_wait(int argc, char* const argv[]) {
#ifdef WIN32
    STARTUPINFO sinfo;
    PROCESS_INFORMATION pinfo;
    memset(&sinfo, 0, sizeof(sinfo));
    memset(&pinfo, 0, sizeof(pinfo));
    sinfo.cb = sizeof(sinfo);

    char commandline[1024];
    cb_assert(argc > 0);
    char* offset = commandline;
    for (int i = 0; i < argc; i++) {
        offset += sprintf(offset, "%s ", argv[i]);
    }

    if (!CreateProcess(argv[0], commandline, NULL, NULL, FALSE, 0,
                       NULL, NULL, &sinfo, &pinfo)) {
        LPVOID error_msg;
        DWORD err = GetLastError();

        if (FormatMessage(FORMAT_MESSAGE_ALLOCATE_BUFFER |
                          FORMAT_MESSAGE_FROM_SYSTEM |
                          FORMAT_MESSAGE_IGNORE_INSERTS,
                          NULL, err, 0,
                          (LPTSTR)&error_msg, 0, NULL) != 0) {
            fprintf(stderr, "Failed to start process: %s\n", error_msg);
            LocalFree(error_msg);
        } else {
            fprintf(stderr, "Failed to start process: unknown error\n");
        }
        exit(EXIT_FAILURE);
    }
    WaitForSingleObject(pinfo.hProcess, INFINITE);

    // Get exit code
    DWORD exit_code;
    GetExitCodeProcess(pinfo.hProcess, &exit_code);

    // remap some of the error codes:
    if (exit_code == EXIT_FAILURE || exit_code == 3) {
        // according to https://msdn.microsoft.com/en-us/library/k089yyh0.aspx
        // abort() will call _exit(3) if no handler is called
        exit_code = int(FAIL);
    } else if (exit_code == 255) {
        // If you click the "terminate program" button in the dialog
        // opened that allows you debug a program, 255 is returned
        exit_code = int(DIED);
    }

    // Close process and thread handles.
    CloseHandle(pinfo.hProcess);
    CloseHandle(pinfo.hThread);

    return exit_code;
#else
    pid_t pid = fork();
    cb_assert(pid != -1);

    if (pid == 0) {
        /* Child */
        cb_assert(execvp(argv[0], argv) != -1);
        // Not reachable, but keep the compiler happy
        return -1;
    } else {
        int status;
        waitpid(pid, &status, 0);
        return status;
    }
#endif // !WIN32
}

static void teardown_testsuite(cb_dlhandle_t handle, const char* test_suite) {
    /* Hack to remove the warning from C99 */
    union {
        TEARDOWN_SUITE teardown_suite;
        void* voidptr;
    } my_teardown_suite;

    memset(&my_teardown_suite, 0, sizeof(my_teardown_suite));

    char *errmsg = NULL;
    void* symbol = cb_dlsym(handle, "teardown_suite", &errmsg);
    if (symbol == NULL) {
        cb_free(errmsg);
    } else {
        my_teardown_suite.voidptr = symbol;
        if (!(*my_teardown_suite.teardown_suite)()) {
            fprintf(stderr, "Failed to teardown up test suite %s \n", test_suite);
        }
    }
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
    std::regex test_case_regex(".*");
    engine_test_t *testcases = NULL;
    OutputFormat output_format(OutputFormat::Text);
    cb_dlhandle_t handle;
    char *errmsg = NULL;
    void *symbol = NULL;
    struct test_harness harness;
    int test_case_id = -1;

    /* If a testcase fails, retry up to 'attempts -1' times to allow it
       to pass - this is here to allow us to deal with intermittant
       test failures without having to manually retry the whole
       job. */
    int attempts = 1;

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


    cb_initialize_sockets();

    AllocHooks::initialize();

    memset(&my_get_test, 0, sizeof(my_get_test));
    memset(&my_setup_suite, 0, sizeof(my_setup_suite));

    color_enabled = getenv("TESTAPP_ENABLE_COLOR") != NULL;

    /* Allow 'attempts' to also be set via env variable - this allows
       commit-validation scripts to enable retries for all
       engine_testapp-driven tests trivually. */
    const char* attempts_env;
    if ((attempts_env = getenv("TESTAPP_ATTEMPTS")) != NULL) {
        attempts = atoi(attempts_env);
    }

    /* Use unbuffered stdio */
    setbuf(stdout, NULL);
    setbuf(stderr, NULL);

    setup_alarm_handler();

    install_backtrace_terminate_handler();

    /* process arguments */
    while ((c = getopt(argc, argv,
                       "a:" /* attempt tests N times before declaring them failed */
                       "h"  /* usage */
                       "E:" /* Engine to load */
                       "e:" /* Engine options */
                       "T:" /* Library with tests to load */
                       "t:" /* Timeout */
                       "L"  /* Loop until failure */
                       "q"  /* Be more quiet (only report failures) */
                       "."  /* dot mode. */
                       "n:"  /* regex for test case(s) to run */
                       "v" /* verbose output */
                       "Z"  /* Terminate on first error */
                       "C:" /* Test case id */
                       "s" /* spinlock the program */
                       "X" /* Use stderr logger */
                       "f:" /* output format. Valid values are: 'text' and 'xml' */
                       )) != -1) {
        switch (c) {
        case 'a':
            attempts = atoi(optarg);
            break;
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
        case 'f':
            if (std::string(optarg) == "text") {
                output_format = OutputFormat::Text;
            } else if (std::string(optarg) == "xml") {
                output_format = OutputFormat::XML;
            } else {
                fprintf(stderr, "Invalid option for output format '%s'. Valid "
                    "options are 'text' and 'xml'.\n", optarg);
                return 1;
            }
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
            test_case_regex = optarg;
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
            log_to_stderr = true;
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
        cb_free(errmsg);
        return 1;
    }

    /* get the test cases */
    symbol = cb_dlsym(handle, "get_tests", &errmsg);
    if (symbol == NULL) {
        fprintf(stderr, "Could not find get_tests function in testsuite %s: %s\n", test_suite, errmsg);
        cb_free(errmsg);
        return 1;
    }
    my_get_test.voidptr = symbol;
    testcases = (*my_get_test.get_tests)();

    /* set up the suite if needed */
    memset(&harness, 0, sizeof(harness));
    harness.default_engine_cfg = engine_args;
    harness.engine_path = engine;
    harness.output_format = output_format;
    harness.output_file_prefix = "output.";
    harness.reload_engine = reload_engine;
    harness.create_cookie = create_mock_cookie;
    harness.destroy_cookie = destroy_mock_cookie;
    harness.set_ewouldblock_handling = mock_set_ewouldblock_handling;
    harness.set_mutation_extras_handling = mock_set_mutation_extras_handling;
    harness.set_datatype_support = mock_set_datatype_support;
    harness.lock_cookie = lock_mock_cookie;
    harness.unlock_cookie = unlock_mock_cookie;
    harness.waitfor_cookie = waitfor_mock_cookie;
    harness.notify_io_complete = notify_io_complete;
    harness.time_travel = mock_time_travel;
    harness.get_current_testcase = get_current_testcase;
    harness.get_mapped_bytes = get_mapped_bytes;
    harness.release_free_memory = release_free_memory;
    harness.create_bucket = create_bucket;
    harness.destroy_bucket = destroy_bucket;
    harness.reload_bucket = reload_bucket;
    harness.store_engine_specific = store_engine_specific;
    harness.get_number_of_mock_cookie_references = get_number_of_mock_cookie_references;

    /* Initialize logging. */
    if (log_to_stderr) {
        logger_descriptor = get_stderr_logger();
    } else {
        logger_descriptor = get_null_logger();
    }
    get_mock_server_api()->extension->register_extension(EXTENSION_LOGGER,
                                                         logger_descriptor);

    for (num_cases = 0; testcases[num_cases].name; num_cases++) {
        /* Just counting */
    }

    symbol = cb_dlsym(handle, "setup_suite", &errmsg);
    if (symbol == NULL) {
        cb_free(errmsg);
    } else {
        my_setup_suite.voidptr = symbol;
        if (!(*my_setup_suite.setup_suite)(&harness)) {
            fprintf(stderr, "Failed to set up test suite %s \n", test_suite);
            return 1;
        }
    }

    if (test_case_id != -1) {
        int exit_code = 0;

        if (test_case_id >= num_cases) {
            fprintf(stderr, "Invalid test case id specified\n");
            exit_code = EXIT_FAILURE;
        } else if (testcases[test_case_id].tfun || testcases[test_case_id].api_v2.tfun) {
            // check there's a test to run, some modules need cleaning up of dead tests
            // if all modules are fixed, this else if can be removed.
            exit_code = execute_test(testcases[test_case_id], engine, engine_args);
        } else {
            exit_code = PENDING; // ignored tests would always return PENDING
        }
        disconnect_all_mock_connections();
        teardown_testsuite(handle, test_suite);
        cb_dlclose(handle);
        exit(exit_code);
    }

    // Setup child argv; same as parent plus additional "-C" "X" arguments.
    std::vector<std::string> child_args;
    for (int ii = 0; ii < argc; ii++) {
        child_args.push_back(argv[ii]);
    }
    child_args.push_back("-C");
    // Expand the child_args to contain space for the numeric argument to '-C'
    child_args.push_back("X");

    do {
        int i;
        bool need_newline = false;
        for (i = 0; testcases[i].name; i++) {
            int error;
            if (!std::regex_search(testcases[i].name, test_case_regex)) {
                continue;
            }
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
                enum test_result ecode = FAIL;
                time_t start;
                time_t stop;
                int rc;

                // Setup args for this test instance.
                child_args[argc + 1] = std::to_string(i);

                // Need to convert to C-style argv. +1 for null terminator.
                std::vector<char*> child_argv(child_args.size() + 1);
                for (size_t i = 0; i < child_args.size(); i++) {
                    child_argv[i] = &child_args[i][0];
                }

                for (int attempt = 0;
                     (attempt < attempts) && ((ecode != SUCCESS) &&
                                              (ecode != SUCCESS_AFTER_RETRY));
                     attempt++) {
                    start = time(NULL);
                    rc = spawn_and_wait(argc + 2, child_argv.data());
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
                    /* If we only got SUCCESS after one or more
                       retries, change result to
                       SUCCESS_AFTER_RETRY */
                    if ((ecode == SUCCESS) && (attempt > 0)) {
                        ecode = SUCCESS_AFTER_RETRY;
                    }
                    error = report_test(testcases[i].name,
                                        stop - start,
                                        ecode, quiet,
                                        !verbose);
                }
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
    teardown_testsuite(handle, test_suite);

    printf("# Passed %d of %d tests\n", num_cases - exitcode, num_cases);
    cb_dlclose(handle);

    return exitcode;
}
