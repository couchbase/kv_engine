/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#undef NDEBUG
#include "config.h"
#include "tap_mock_engine.h"

#include <pthread.h>
#include <cassert>
#include <cstdlib>
#include <cstring>
#include <map>
#include <string>

using namespace std;

// The engine interface use C linkage
extern "C" {

    static const engine_info* get_info(ENGINE_HANDLE* handle);
    static ENGINE_ERROR_CODE initialize(ENGINE_HANDLE* handle,
                                        const char* config_str);
    static void destroy(ENGINE_HANDLE* handle,
                        const bool force);
    static ENGINE_ERROR_CODE item_allocate(ENGINE_HANDLE* handle,
                                           const void* cookie,
                                           item **itm,
                                           const void* key,
                                           const size_t nkey,
                                           const size_t nbytes,
                                           const int flags,
                                           const rel_time_t exptime);
    static ENGINE_ERROR_CODE item_delete(ENGINE_HANDLE* handle,
                                         const void* cookie,
                                         const void* key,
                                         const size_t nkey,
                                         uint64_t cas,
                                         uint16_t vbucket);

    static void item_release(ENGINE_HANDLE* handle, const void *cookie,
                             item* it);
    static ENGINE_ERROR_CODE get(ENGINE_HANDLE* handle,
                                 const void* cookie,
                                 item** item,
                                 const void* key,
                                 const int nkey,
                                 uint16_t vbucket);
    static ENGINE_ERROR_CODE get_stats(ENGINE_HANDLE* handle,
                                       const void *cookie,
                                       const char *stat_key,
                                       int nkey,
                                       ADD_STAT add_stat);
    static void reset_stats(ENGINE_HANDLE* handle, const void *cookie);
    static ENGINE_ERROR_CODE store(ENGINE_HANDLE* handle,
                                   const void *cookie,
                                   item* item,
                                   uint64_t *cas,
                                   ENGINE_STORE_OPERATION operation,
                                   uint16_t vbucket);
    static ENGINE_ERROR_CODE arithmetic(ENGINE_HANDLE* handle,
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
    static ENGINE_ERROR_CODE flush(ENGINE_HANDLE* handle,
                                   const void* cookie, time_t when);
    static ENGINE_ERROR_CODE unknown_command(ENGINE_HANDLE* handle,
                                             const void* cookie,
                                             protocol_binary_request_header *request,
                                             ADD_RESPONSE response);

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
                                        const void *data,
                                        size_t ndata,
                                        uint16_t vbucket);

    static tap_event_t tap_walker(ENGINE_HANDLE* handle,
                                  const void *cookie, item **itm,
                                  void **es, uint16_t *nes, uint8_t *ttl,
                                  uint16_t *flags, uint32_t *seqno,
                                  uint16_t *vbucket);

    static TAP_ITERATOR get_tap_iterator(ENGINE_HANDLE* handle,
                                         const void* cookie,
                                         const void* client,
                                         size_t nclient,
                                         uint32_t flags,
                                         const void* userdata,
                                         size_t nuserdata);

    static void item_set_cas(ENGINE_HANDLE *handle, const void *cookie,
                             item* item, uint64_t val);

    static bool get_item_info(ENGINE_HANDLE *handle, const void *cookie,
                              const item* item, item_info *item_info);

    static void handle_disconnect(const void *cookie,
                                  ENGINE_EVENT_TYPE type,
                                  const void *event_data,
                                  const void *cb_data);

    static void* mock_async_io_thread_main(void *arg);
    static void* dispatch_notification(void *arg);
}

struct NotificationData {
    NotificationData(const void *c, SERVER_HANDLE_V1 *s) : cookie(c), sapi(s) {}
    SERVER_HANDLE_V1 *sapi;
    const void *cookie;
};

class Item {
public:
    Item(const void* kp, const size_t nkey, const size_t nb,
         const int fl, const rel_time_t exp) :
        key((const char*)kp, nkey), nbytes(nb), flags(fl), exptime(exp),
        cas(random())
    {
        data = new char[nbytes];
    }

    Item(const Item &o) : key(o.key), nbytes(o.nbytes), flags(o.flags),
                          exptime(o.exptime), cas(o.cas)
    {
        data = new char[nbytes];
        memcpy(data, o.data, nbytes);
    }

    ~Item()
    {
        delete []data;
    }

    const string &getKey(void) const {
        return key;
    }


    Item* clone() const {
        return new Item(*this);

    }

    void setCas(uint64_t) {
        // ignored
    }

    bool toItemInfo(item_info *info) const {
        info->cas = cas;
        info->exptime = exptime;
        info->nbytes = nbytes;
        info->flags = flags;
        info->clsid = 0;
        info->nkey = key.length();
        assert(info->nvalue > 0);
        info->nvalue = 1;
        info->key = key.data();
        info->value[0].iov_base = data;
        info->value[0].iov_len = nbytes;
        return true;
    }

private:
    string key;
    size_t nbytes;
    char *data;
    int flags;
    rel_time_t exptime;
    uint64_t cas;
};

class Mutex {
public:
    Mutex() {
        pthread_mutex_init(&mutex, NULL);

    }

    ~Mutex() {
        pthread_mutex_destroy(&mutex);
    }

    void lock() {
        assert(pthread_mutex_lock(&mutex) == 0);
    }

    void unlock() {
        assert(pthread_mutex_unlock(&mutex) == 0);
    }

private:
    pthread_mutex_t mutex;
};

class KVStore {
public:
    KVStore() {

    }

    ~KVStore() {
        clear();
    }

    void flush(void) {
        mutex.lock();
        clear();
        mutex.unlock();
    }

    bool store(const Item *itm, ENGINE_STORE_OPERATION operation,
               uint64_t *cas) {
        bool ret;
        mutex.lock();
        switch (operation) {
        case OPERATION_ADD:
            ret = add_locked(itm);
            break;
        case OPERATION_SET:
            ret = set_locked(itm);
            break;
        case OPERATION_REPLACE:
            ret = replace_locked(itm);
            break;
        case OPERATION_APPEND:
            ret = append_locked(itm);
            break;
        case OPERATION_PREPEND:
            ret = prepend_locked(itm);
            break;
        case OPERATION_CAS:
            ret = cas_locked(itm);
            break;
        default:
            abort();
        }
        mutex.unlock();

        return ret;
    }

    Item* get(const string &key) {
        Item *ret;
        mutex.lock();
        ret = get_locked(key);
        mutex.unlock();

        return ret;
    }

    bool del(const string &key, uint64_t cas) {
        bool ret;
        mutex.lock();
        ret = del_locked(key, cas);
        mutex.unlock();

        return ret;
    }

protected:
    bool add_locked(const Item *itm) {
        map<string, Item*>::iterator iter = datastore.find(itm->getKey());
        if (iter == datastore.end()) {
            Item *clone = itm->clone();
            if (clone) {
                datastore[itm->getKey()] = clone;
                return true;
            }
        }

        return false;
    }

    bool set_locked(const Item *itm) {
        Item *clone = itm->clone();
        if (clone) {
            map<string, Item*>::iterator iter = datastore.find(itm->getKey());
            if (iter != datastore.end()) {
                delete iter->second;
                datastore.erase(iter);
            }

            datastore[itm->getKey()] = clone;
            return true;
        }

        return false;
    }

    bool replace_locked(const Item *itm) {
        map<string, Item*>::iterator iter = datastore.find(itm->getKey());
        if (iter != datastore.end()) {
            Item *clone = itm->clone();
            if (clone) {
                delete iter->second;
                datastore[itm->getKey()] = clone;
                return true;
            }
        }

        return false;
    }

    bool append_locked(const Item *itm) {
        // @todo implement me
        return false;
    }

    bool prepend_locked(const Item *itm) {
        // @todo implement me
        return false;
    }

    bool cas_locked(const Item *itm) {
        // @todo implement me
        return false;
    }

    Item* get_locked(const string &key) {
        map<string, Item*>::iterator iter = datastore.find(key);
        if (iter != datastore.end()) {
            return iter->second->clone();
        }
        return NULL;
    }

    bool del_locked(const string &key, uint64_t cas) {
        map<string, Item*>::iterator iter = datastore.find(key);
        if (iter != datastore.end()) {
            delete iter->second;
            datastore.erase(iter);
            return true;
        }

        return false;
    }

    void clear(void) {
        map<string, Item*>::iterator iter;
        for (iter = datastore.begin(); iter != datastore.end(); ++iter) {
            delete iter->second;
        }
        datastore.clear();
    }

private:
    Mutex mutex;
    map<string, Item*> datastore;
};

class TapConnection {
public:
    TapConnection(const void* c,
                  const void* client,
                  size_t nclient,
                  uint32_t flags,
                  const void* userdata,
                  size_t nuserdat) :
        cookie(c), disconnect(false), blocked(false), reserved(false)
    {

    }

    void setBlocked(bool value) {
        blocked = value;
    }

    bool isBlocked(void) const {
        return blocked;
    }

    const void *getCookie(void) const {
        return cookie;
    }

    void setDisconnect(void) {
        disconnect = true;
    }

    bool isDisconnected(void) const {
        return disconnect;
    }

    void release(void) {
        assert(reserved);
        reserved = false;
        // @todo fixme
    }

    void reserve(void) {
        assert(!reserved);
        reserved = true;
    }

    bool isReserved(void) const {
        return reserved;
    }

private:
    const void *cookie;
    bool disconnect;
    bool blocked;
    bool reserved;
};

class TapConnMap {
public:
    ~TapConnMap() {
        clear();
    }

    void add(TapConnection *connection) {
        mutex.lock();
        add_locked(connection);
        mutex.unlock();
    }

    // Need to go through here to ensure we're using the same barrier
    void release(TapConnection *connection) {
        mutex.lock();
        connection->release();
        mutex.unlock();
    }

    TapConnection* get(const void *cookie) {
        TapConnection *ret;
        mutex.lock();
        ret = get_locked(cookie);
        ret->reserve();
        mutex.unlock();

        return ret;
    }

    void del(const void *cookie) {
        mutex.lock();
        del_locked(cookie);
        mutex.unlock();
    }

    void setDisconnect(const void *cookie) {
        mutex.lock();
        setDisconnect_locked(cookie);
        mutex.unlock();
    }

    void reapDisconnected(SERVER_HANDLE_V1 *api) {
        TapConnection *conn;

        do {
            mutex.lock();
            conn = getDisconnected_locked(api);
            mutex.unlock();

            if (conn != NULL) {
                // Remove our reservation of the socket
                api->cookie->release(conn->getCookie());
                delete conn;
            }
        } while (conn != NULL);
    }

    void notifySome(SERVER_HANDLE_V1 *api) {
        TapConnection *conn;

        do {
            mutex.lock();
            conn = getBlocked_locked(api);
            mutex.unlock();

            if (conn != NULL) {
                conn->setBlocked(false);
                api->cookie->notify_io_complete(conn->getCookie(),
                                                ENGINE_SUCCESS);
            }
        } while (conn != NULL);
    }

protected:
    void add_locked(TapConnection *connection) {
        map<const void *, TapConnection *>::iterator iter;
        iter = connmap.find(connection->getCookie());
        assert(iter == connmap.end());
        connmap[connection->getCookie()] = connection;
    }

    TapConnection* get_locked(const void *cookie) {
        map<const void *, TapConnection*>::iterator iter;
        iter = connmap.find(cookie);
        assert(iter != connmap.end());
        return iter->second;
    }

    void del_locked(const void *cookie) {
        map<const void *, TapConnection*>::iterator iter;
        iter = connmap.find(cookie);
        assert(iter != connmap.end());
        delete iter->second;
        connmap.erase(iter);
    }

    void setDisconnect_locked(const void *cookie) {
        map<const void *, TapConnection*>::iterator iter;
        iter = connmap.find(cookie);
        if (iter != connmap.end()) {
            iter->second->setDisconnect();
        }
    }

    TapConnection* getDisconnected_locked(SERVER_HANDLE_V1 *api) {
        map<const void *, TapConnection*>::iterator iter;
        for (iter = connmap.begin(); iter != connmap.end(); iter++) {
            if (!iter->second->isReserved() && iter->second->isDisconnected()) {
                TapConnection *conn = iter->second;
                connmap.erase(iter);

                iter = connmap.find(conn->getCookie());
                assert(iter == connmap.end());
                return conn;
            }
        }
        return NULL;
    }

    TapConnection* getBlocked_locked(SERVER_HANDLE_V1 *api) {
        map<const void *, TapConnection*>::iterator iter;
        for (iter = connmap.begin(); iter != connmap.end(); iter++) {
            if (!iter->second->isReserved() && iter->second->isBlocked()) {
                TapConnection *c = iter->second;
                return c;
            }
        }
        return NULL;
    }

    void clear(void) {
        map<const void *, TapConnection*>::iterator iter;
        for (iter = connmap.begin(); iter != connmap.end(); ++iter) {
            delete iter->second;
        }
        connmap.clear();
    }

private:
    Mutex mutex;
    map<const void *, TapConnection*> connmap;
};

class MockEngine {
public:
    MockEngine(SERVER_HANDLE_V1 *api) : sapi(api), running(false) {
        memset(&info, 0, sizeof(info));
        info.description = "TAP mock engine v0.1";
    }

    ~MockEngine() {
        if (running) {
            running = false;
            assert(pthread_join(io_thread, NULL) == 0);
        }
    }

    // Method to handle the get_info engine function
    const engine_info* getInfo() const {
        return &info;
    }

    ENGINE_ERROR_CODE initialize(ENGINE_HANDLE* handle, const char* ) {
        void *cookie = reinterpret_cast<void*>(this);
        sapi->callback->register_callback(handle, ON_DISCONNECT,
                                          ::handle_disconnect, cookie);

        running = true;
        assert(pthread_create(&io_thread, NULL, mock_async_io_thread_main,
                              cookie) == 0);

        return ENGINE_SUCCESS;
    }

    ENGINE_ERROR_CODE itemAllocate(const void* cookie,
                                   item **it,
                                   const void* key,
                                   const size_t nkey,
                                   const size_t nbytes,
                                   const int flags,
                                   const rel_time_t exptime)
    {
        // if ((random() % 10) == 1) {
        //     return dispatchNotification(cookie);
        // }

        Item *itm = new Item(key, nkey, nbytes, flags, exptime);
        if (itm == NULL) {
            return ENGINE_ENOMEM;
        }
        *it = reinterpret_cast<item*>(itm);
        return ENGINE_SUCCESS;
    }

    ENGINE_ERROR_CODE itemDelete(const void* cookie,
                                 const void* key,
                                 const size_t nkey,
                                 uint64_t cas,
                                 uint16_t vbucket)
    {
        // if ((random() % 10) == 1) {
        //     return dispatchNotification(cookie);
        // }

        string k((const char*)key, nkey);
        if (kvstore.del(k, cas)) {
            return ENGINE_SUCCESS;
        }

        return ENGINE_KEY_ENOENT;
    }

    void itemRelease(const void *cookie, item* it)
    {
        Item *itm = reinterpret_cast<Item*>(it);
        delete itm;
    }

    void itemSetCas(const void *, item* itm, uint64_t val)
    {
        Item *it = reinterpret_cast<Item*>(itm);
        it->setCas(val);
    }

    bool getItemInfo(const void *, const item* itm, item_info *item_info)
    {
        const Item *it = reinterpret_cast<const Item*>(itm);
        return it->toItemInfo(item_info);
    }


    ENGINE_ERROR_CODE get(const void*cookie,
                          item** itm,
                          const void* key,
                          const int nkey,
                          uint16_t vbucket)
    {
        if ((random() % 5) == 1) {
            return dispatchNotification(cookie);
        }

        Item *it = kvstore.get(string((const char*)key, nkey));
        if (it == NULL) {
            return ENGINE_KEY_ENOENT;
        }

        *itm = reinterpret_cast<item*>(it);
        return ENGINE_SUCCESS;
    }

    ENGINE_ERROR_CODE getStats(const void *, // cookie
                               const char *, // stat_key
                               int nkey,
                               ADD_STAT add_stat)
    {
        if (nkey != 0) {
            return ENGINE_KEY_ENOENT;
        }

        return ENGINE_SUCCESS;
    }

    void resetStats(const void * /* cookie */) {
        // EMPTY
    }

    ENGINE_ERROR_CODE store(const void *cookie,
                            item* itm,
                            uint64_t *cas,
                            ENGINE_STORE_OPERATION operation,
                            uint16_t vbucket)
    {
        if ((random() % 10) == 1) {
            return dispatchNotification(cookie);
        }

        Item *it = reinterpret_cast<Item*>(itm);
        if (kvstore.store(it, operation, cas)) {
            return ENGINE_SUCCESS;
        }

        return ENGINE_NOT_STORED;
    }

    ENGINE_ERROR_CODE flush(const void* cookie, time_t when) {
        // if ((random() % 10) == 1) {
        //     return dispatchNotification(cookie);
        // }

        if (when == 0) {
            kvstore.flush();
            return ENGINE_SUCCESS;
        }

        return ENGINE_ENOTSUP;
    }

    ENGINE_ERROR_CODE unknownCommand(const void* cookie,
                                     protocol_binary_request_header *request,
                                     ADD_RESPONSE response)
    {
        abort();
        return ENGINE_ENOTSUP;
    }

    ENGINE_ERROR_CODE tapNotify(const void *cookie,
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
                                uint16_t vbucket)
    {
        abort();
        return ENGINE_ENOTSUP;
    }

    TAP_ITERATOR getTapIterator(const void* cookie,
                                const void* client,
                                size_t nclient,
                                uint32_t flags,
                                const void* userdata,
                                size_t nuserdata)
    {
        TapConnection *conn = new TapConnection(cookie, client, nclient,
                                                flags, userdata, nuserdata);
        sapi->cookie->reserve(cookie);
        tapconnmap.add(conn);
        return tap_walker;
    }

    tap_event_t tapWalker(const void *cookie, item **itm,
                          void **es, uint16_t *nes, uint8_t *ttl,
                          uint16_t *flags, uint32_t *seqno,
                          uint16_t *vbucket)
    {
        tap_event_t ret = TAP_DISCONNECT;
        TapConnection *connection = tapconnmap.get(cookie);

        *es = NULL;
        *nes = 0;
        *ttl = 1;
        *seqno = 0;
        *flags = 0;

        long r = random() % 4;
        if (r < 1) {
            ret = TAP_NOOP;
        } else if (r < 2) {
            connection->setBlocked(true);
            ret = TAP_PAUSE;
        }

        tapconnmap.release(connection);
        return ret;
    }

    void handleDisconnect(const void *cookie, const void *event_data)
    {
        tapconnmap.setDisconnect(cookie);
    }

    void asyncIOThreadMain(void) {
        while (running) {
            usleep(random() % 200);
            if ((random() % 10) < 3) {
                tapconnmap.reapDisconnected(sapi);
            } else {
                tapconnmap.notifySome(sapi);
            }
        }
    }

protected:
    ENGINE_ERROR_CODE dispatchNotification(const void *cookie) {
        NotificationData *nd = new NotificationData(cookie, sapi);
        if (pthread_create(NULL, NULL, dispatch_notification,
                           reinterpret_cast<void*>(nd)) != 0) {
            return ENGINE_DISCONNECT;
        }

        return ENGINE_EWOULDBLOCK;
    }

private:
    // pointer to the server API
    SERVER_HANDLE_V1 *sapi;
    engine_info info;
    KVStore kvstore;
    TapConnMap tapconnmap;
    volatile bool running;
    pthread_t io_thread;
};


struct EngineGlue {
    EngineGlue(SERVER_HANDLE_V1 *api): me(api) {
        interface.interface.interface = 1;
        interface.get_info = get_info;
        interface.initialize = initialize;
        interface.destroy = destroy;
        interface.allocate = item_allocate;
        interface.remove = item_delete;
        interface.release = item_release;
        interface.get = get;
        interface.get_stats = get_stats;
        interface.reset_stats = reset_stats;
        interface.store = store;
        interface.arithmetic = NULL;
        interface.flush = flush;
        interface.unknown_command = unknown_command;
        interface.tap_notify = tap_notify;
        interface.get_tap_iterator = get_tap_iterator;
        interface.item_set_cas = item_set_cas;
        interface.get_item_info = get_item_info;
    }

    ENGINE_HANDLE_V1 interface;
    MockEngine me;
};


ENGINE_ERROR_CODE create_instance(uint64_t interface,
                                  GET_SERVER_API get_server_api,
                                  ENGINE_HANDLE **handle)
{
    // Verify that we support the current API version
    if (interface != 1) {
        return ENGINE_ENOTSUP;
    }

    EngineGlue *glue = new EngineGlue(get_server_api());
    if (glue == NULL) {
        return ENGINE_ENOMEM;
    }

    *handle = reinterpret_cast<ENGINE_HANDLE*>(glue);
    return ENGINE_SUCCESS;
}

static inline MockEngine &getHandle(ENGINE_HANDLE* handle) {
    return reinterpret_cast<EngineGlue*>(handle)->me;
}

static const engine_info* get_info(ENGINE_HANDLE* handle)
{
    return getHandle(handle).getInfo();
}

static ENGINE_ERROR_CODE initialize(ENGINE_HANDLE* handle, const char* cfg)
{
    return getHandle(handle).initialize(handle, cfg);
}

static void destroy(ENGINE_HANDLE* handle, const bool force) {
    delete reinterpret_cast<EngineGlue*>(handle);
}

static ENGINE_ERROR_CODE item_allocate(ENGINE_HANDLE* handle,
                                       const void* cookie,
                                       item **itm,
                                       const void* key,
                                       const size_t nkey,
                                       const size_t nbytes,
                                       const int flags,
                                       const rel_time_t exptime)
{
    return getHandle(handle).itemAllocate(cookie, itm, key, nkey,
                                           nbytes, flags, exptime);
}

static ENGINE_ERROR_CODE item_delete(ENGINE_HANDLE* handle,
                                     const void* cookie,
                                     const void* key,
                                     const size_t nkey,
                                     uint64_t cas,
                                     uint16_t vbucket)
{
    return getHandle(handle).itemDelete(cookie, key, nkey, cas, vbucket);
}

static void item_release(ENGINE_HANDLE* handle, const void *cookie, item* it)
{
    return getHandle(handle).itemRelease(cookie, it);
}

static ENGINE_ERROR_CODE get(ENGINE_HANDLE* handle,
                             const void* cookie,
                             item** item,
                             const void* key,
                             const int nkey,
                             uint16_t vbucket)
{
    return getHandle(handle).get(cookie, item, key, nkey, vbucket);
}

static ENGINE_ERROR_CODE get_stats(ENGINE_HANDLE* handle,
                                   const void *cookie,
                                   const char *stat_key,
                                   int nkey,
                                   ADD_STAT add_stat)
{
    return getHandle(handle).getStats(cookie, stat_key, nkey, add_stat);
}

static void reset_stats(ENGINE_HANDLE* handle, const void *cookie)
{
    getHandle(handle).resetStats(cookie);
}

static ENGINE_ERROR_CODE store(ENGINE_HANDLE* handle,
                               const void *cookie,
                               item* it,
                               uint64_t *cas,
                               ENGINE_STORE_OPERATION operation,
                               uint16_t vbucket)
{
    return getHandle(handle).store(cookie, it, cas, operation, vbucket);
}

static ENGINE_ERROR_CODE flush(ENGINE_HANDLE* handle,
                               const void* cookie, time_t when)
{
    return getHandle(handle).flush(cookie, when);
}

static ENGINE_ERROR_CODE unknown_command(ENGINE_HANDLE* handle,
                                         const void* cookie,
                                         protocol_binary_request_header *request,
                                         ADD_RESPONSE response)
{
    return getHandle(handle).unknownCommand(cookie, request, response);
}

static ENGINE_ERROR_CODE tap_notify(ENGINE_HANDLE* handle, const void *cookie,
                                    void *engine_specific, uint16_t nengine,
                                    uint8_t ttl, uint16_t tap_flags,
                                    tap_event_t tap_event, uint32_t tap_seqno,
                                    const void *key, size_t nkey,
                                    uint32_t flags, uint32_t exptime,
                                    uint64_t cas, const void *data,
                                    size_t ndata, uint16_t vbucket)
{
    return getHandle(handle).tapNotify(cookie, engine_specific, nengine,
                                        ttl, tap_flags, tap_event, tap_seqno,
                                        key, nkey, flags, exptime, cas,
                                        data, ndata, vbucket);
}

static tap_event_t tap_walker(ENGINE_HANDLE* handle,
                              const void *cookie, item **itm,
                              void **es, uint16_t *nes, uint8_t *ttl,
                              uint16_t *flags, uint32_t *seqno,
                              uint16_t *vbucket)
{
    return getHandle(handle).tapWalker(cookie, itm, es, nes, ttl, flags,
                                        seqno, vbucket);
}

static TAP_ITERATOR get_tap_iterator(ENGINE_HANDLE* handle,
                                     const void* cookie,
                                     const void* client,
                                     size_t nclient,
                                     uint32_t flags,
                                     const void* userdata,
                                     size_t nuserdata)
{
    return getHandle(handle).getTapIterator(cookie, client, nclient,
                                             flags, userdata, nuserdata);
}

void item_set_cas(ENGINE_HANDLE *handle, const void *cookie,
                  item* it, uint64_t val)
{
    return getHandle(handle).itemSetCas(cookie, it, val);
}

static bool get_item_info(ENGINE_HANDLE *handle, const void *cookie,
                          const item* item, item_info *info)
{
    return getHandle(handle).getItemInfo(cookie, item, info);
}


static void handle_disconnect(const void *cookie,
                              ENGINE_EVENT_TYPE type,
                              const void *event_data,
                              const void *cb_data)
{
    assert(type == ON_DISCONNECT);
    void *engine = const_cast<void*>(cb_data);
    MockEngine *me = reinterpret_cast<MockEngine *>(engine);
    me->handleDisconnect(cookie, event_data);
}

static void* mock_async_io_thread_main(void *arg)
{
    MockEngine *me = reinterpret_cast<MockEngine*>(arg);
    me->asyncIOThreadMain();
    return NULL;
}

static void* dispatch_notification(void *arg)
{
    NotificationData *data = reinterpret_cast<NotificationData*>(arg);
    data->sapi->cookie->notify_io_complete(data->cookie,
                                           ENGINE_SUCCESS);
    pthread_detach(pthread_self());
    delete data;
    return NULL;
}
