/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <assert.h>

#include <memcached/engine.h>

#include "ep_engine.h"

/**
 * Helper function to avoid typing in the long cast all over the place
 * @param handle pointer to the engine
 * @return the engine as a class
 */
static inline EventuallyPersistentEngine* getHandle(ENGINE_HANDLE* handle)
{
    return reinterpret_cast<EventuallyPersistentEngine*>(handle);
}

// The Engine API specifies C linkage for the functions..
extern "C" {

    static const char* EvpGetInfo(ENGINE_HANDLE* handle)
    {
        (void)handle;
        return "EP engine v0.1";
    }

    static ENGINE_ERROR_CODE EvpInitialize(ENGINE_HANDLE* handle,
                                           const char* config_str)
    {
        return getHandle(handle)->initialize(config_str);
    }

    static void EvpDestroy(ENGINE_HANDLE* handle)
    {
        getHandle(handle)->destroy();
        delete getHandle(handle);
    }

    static ENGINE_ERROR_CODE EvpItemAllocate(ENGINE_HANDLE* handle,
                                             const void* cookie,
                                             item **item,
                                             const void* key,
                                             const size_t nkey,
                                             const size_t nbytes,
                                             const int flags,
                                             const rel_time_t exptime)
    {
        return getHandle(handle)->itemAllocate(cookie, item, key,
                                               nkey, nbytes, flags, exptime);
    }

    static ENGINE_ERROR_CODE EvpItemDelete(ENGINE_HANDLE* handle,
                                           const void* cookie,
                                           item* item)
    {
        return getHandle(handle)->itemDelete(cookie, item);
    }

    static void EvpItemRelease(ENGINE_HANDLE* handle,
                               const void *cookie,
                               item* item)
    {
        getHandle(handle)->itemRelease(cookie, item);
    }

    static ENGINE_ERROR_CODE EvpGet(ENGINE_HANDLE* handle,
                                    const void* cookie,
                                    item** item,
                                    const void* key,
                                    const int nkey)
    {
        return getHandle(handle)->get(cookie, item, key, nkey);
    }

    static ENGINE_ERROR_CODE EvpGetStats(ENGINE_HANDLE* handle,
                                         const void* cookie,
                                         const char* stat_key,
                                         int nkey,
                                         ADD_STAT add_stat)
    {
        return getHandle(handle)->getStats(cookie, stat_key, nkey, add_stat);
    }

    static ENGINE_ERROR_CODE EvpStore(ENGINE_HANDLE* handle,
                                      const void *cookie,
                                      item* item,
                                      uint64_t *cas,
                                      ENGINE_STORE_OPERATION operation)
    {
        return getHandle(handle)->store(cookie, item, cas, operation);
    }

    static ENGINE_ERROR_CODE EvpArithmetic(ENGINE_HANDLE* handle,
                                           const void* cookie,
                                           const void* key,
                                           const int nkey,
                                           const bool increment,
                                           const bool create,
                                           const uint64_t delta,
                                           const uint64_t initial,
                                           const rel_time_t exptime,
                                           uint64_t *cas,
                                           uint64_t *result)
    {
        return getHandle(handle)->arithmetic(cookie, key, nkey, increment,
                                             create, delta, initial, exptime,
                                             cas, result);
    }

    static ENGINE_ERROR_CODE EvpFlush(ENGINE_HANDLE* handle,
                                      const void* cookie, time_t when)
    {
        return getHandle(handle)->flush(cookie, when);
    }

    static void EvpResetStats(ENGINE_HANDLE* handle, const void *cookie)
    {
        (void)cookie;
        return getHandle(handle)->resetStats();
    }

    static protocol_binary_response_status stopFlusher(EventuallyPersistentEngine *e,
                                                       const char **msg) {
        return e->stopFlusher(msg);
    }

    static protocol_binary_response_status startFlusher(EventuallyPersistentEngine *e,
                                                        const char **msg) {
        return e->startFlusher(msg);
    }

    static ENGINE_ERROR_CODE EvpUnknownCommand(ENGINE_HANDLE* handle,
                                               const void* cookie,
                                               protocol_binary_request_header *request,
                                               ADD_RESPONSE response)
    {
        (void)handle;
        (void)cookie;
        (void)request;

        bool handled = true;
        protocol_binary_response_status res =
            PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND;
        const char *msg;

        EventuallyPersistentEngine *h = getHandle(handle);

        switch (request->request.opcode) {
        case CMD_STOP_PERSISTENCE:
            res = stopFlusher(h, &msg);
            break;
        case CMD_START_PERSISTENCE:
            res = startFlusher(h, &msg);
            break;
        default:
            /* unknown command */
            handled = false;
            break;
        }

        if (handled) {
            size_t msg_size = msg ? strlen(msg) : 0;
            response(msg, static_cast<uint16_t>(msg_size),
                     NULL, 0, NULL, 0,
                     PROTOCOL_BINARY_RAW_BYTES,
                     static_cast<uint16_t>(res), 0, cookie);
        } else {
            response(NULL, 0, NULL, 0, NULL, 0,
                     PROTOCOL_BINARY_RAW_BYTES,
                     PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND, 0, cookie);
        }

        return ENGINE_SUCCESS;
    }

    static uint64_t EvpItemGetCas(const item *item) {
        (void)item;
        return 0;
    }

    static void EvpItemSetCas(item *item, uint64_t cas) {
        (void)item;
        (void)cas;
        // empty
    }

    static const char *EvpItemGetKey(const item *it) {
        return static_cast<const Item*>(it)->getKey().c_str();
    }

    static char *EvpItemGetData(const item *it) {
        return const_cast<Item*>(static_cast<const Item*>(it))->getData();
    }

    static uint8_t EvpItemGetClsid(const item *item) {
        (void)item;
        return 0;
    }

    static ENGINE_ERROR_CODE EvpTapNotify(ENGINE_HANDLE* handle,
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
                                        size_t ndata)
    {
        return getHandle(handle)->tapNotify(cookie, engine_specific, nengine,
                                            ttl, tap_flags, tap_event,
                                            tap_seqno, key, nkey, flags,
                                            exptime, cas, data, ndata);
    }


    static tap_event_t EvpTapIterator(ENGINE_HANDLE* handle, const void *cookie, item **itm, void **es, uint16_t *nes, uint8_t *ttl, uint16_t *flags, uint32_t *seqno) {
        return getHandle(handle)->walkTapQueue(cookie, itm, es, nes, ttl, flags, seqno);
    }

    static TAP_ITERATOR EvpGetTapIterator(ENGINE_HANDLE* handle,
                                          const void* cookie,
                                          const void* client,
                                          size_t nclient,
                                          uint32_t flags,
                                          const void* userdata,
                                          size_t nuserdata) {
        std::string c(static_cast<const char*>(client), nclient);
        // Figure out what we want from the userdata before adding it to the API
        // to the handle
        (void)userdata;
        (void)nuserdata;
        getHandle(handle)->createTapQueue(cookie, c, flags);
        return EvpTapIterator;
    }

    void EvpHandleDisconnect(const void *cookie,
                             ENGINE_EVENT_TYPE type,
                             const void *event_data,
                             const void *cb_data)
    {
        assert(type == ON_DISCONNECT);
        assert(event_data == NULL);
        void *c = const_cast<void*>(cb_data);
        return getHandle(static_cast<ENGINE_HANDLE*>(c))->handleDisconnect(cookie);
    }


    /**
     * The only public interface to the eventually persistance engine.
     * Allocate a new instance and initialize it
     * @param interface the highest interface the server supports (we only support
     *                  interface 1)
     * @param get_server_api callback function to get the server exported API
     *                  functions
     * @param handle Where to return the new instance
     * @return ENGINE_SUCCESS on success
     */
    ENGINE_ERROR_CODE create_instance(uint64_t interface,
                                      GET_SERVER_API get_server_api,
                                      ENGINE_HANDLE **handle)
    {
        SERVER_HANDLE_V1 *api;
        api = static_cast<SERVER_HANDLE_V1 *>(get_server_api(1));
        if (interface != 1 || api == NULL) {
            return ENGINE_ENOTSUP;
        }

        EventuallyPersistentEngine *engine;
        engine = new struct EventuallyPersistentEngine(api);
        if (engine == NULL) {
            return ENGINE_ENOMEM;
        }

        ep_current_time = api->get_current_time;

        *handle = reinterpret_cast<ENGINE_HANDLE*> (engine);
        return ENGINE_SUCCESS;
    }

    static void *EvpNotifyTapIo(void*arg) {
        static_cast<EventuallyPersistentEngine*>(arg)->notifyTapIoThread();
        return NULL;
    }
} // C linkage

EventuallyPersistentEngine::EventuallyPersistentEngine(SERVER_HANDLE_V1 *sApi) :
    dbname("/tmp/test.db"), warmup(true), sqliteDb(NULL),
    epstore(NULL), databaseInitTime(0), shutdown(false)
{
    interface.interface = 1;
    ENGINE_HANDLE_V1::get_info = EvpGetInfo;
    ENGINE_HANDLE_V1::initialize = EvpInitialize;
    ENGINE_HANDLE_V1::destroy = EvpDestroy;
    ENGINE_HANDLE_V1::allocate = EvpItemAllocate;
    ENGINE_HANDLE_V1::remove = EvpItemDelete;
    ENGINE_HANDLE_V1::release = EvpItemRelease;
    ENGINE_HANDLE_V1::get = EvpGet;
    ENGINE_HANDLE_V1::get_stats = EvpGetStats;
    ENGINE_HANDLE_V1::reset_stats = EvpResetStats;
    ENGINE_HANDLE_V1::store = EvpStore;
    ENGINE_HANDLE_V1::arithmetic = EvpArithmetic;
    ENGINE_HANDLE_V1::flush = EvpFlush;
    ENGINE_HANDLE_V1::unknown_command = EvpUnknownCommand;
    ENGINE_HANDLE_V1::get_tap_iterator = EvpGetTapIterator;
    ENGINE_HANDLE_V1::tap_notify = EvpTapNotify;
    ENGINE_HANDLE_V1::item_get_cas = EvpItemGetCas;
    ENGINE_HANDLE_V1::item_set_cas = EvpItemSetCas;
    ENGINE_HANDLE_V1::item_get_key = EvpItemGetKey;
    ENGINE_HANDLE_V1::item_get_data = EvpItemGetData;
    ENGINE_HANDLE_V1::item_get_clsid = EvpItemGetClsid;
    ENGINE_HANDLE_V1::get_stats_struct = NULL;

    serverApi = *sApi;
}

void EventuallyPersistentEngine::startEngineThreads(void)
{
    if (pthread_create(&notifyThreadId, NULL, EvpNotifyTapIo, this) != 0) {
        throw std::runtime_error("Error creating thread to notify Tap connections");
    }
}
