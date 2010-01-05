/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "locks.hh"
#include "ep.hh"
#include "sqlite-kvstore.hh"

extern "C" {
    EXPORT_FUNCTION
    ENGINE_ERROR_CODE create_instance(uint64_t interface,
                                      GET_SERVER_API get_server_api,
                                      ENGINE_HANDLE **handle);

    static const char *EvpItemGetKey(const item *it);
    static char *EvpItemGetData(const item *it);
}

/**
 * Statistic information collected by the eventually persistent engine
 */
struct engine_stats {
    /** All access to the members should be guarded by the lock */
    pthread_mutex_t lock;
    // @todo add interesting stats
};

/**
 * The Item structure we use to pass information between the memcached
 * core and the backend. Please note that the kvstore don't store these
 * objects, so we do have an extra layer of memory copying :(
 */
class Item : public item {
private:
    friend class EventuallyPersistentEngine;
    friend const char *EvpItemGetKey(const item *it);
    friend char *EvpItemGetData(const item *it);

    Item(const void* k, const size_t nk, const size_t nb,
         const int fl, const rel_time_t exp) :
        key(static_cast<const char*>(k), nk)
    {
        nkey = static_cast<uint16_t>(nk);
        nbytes = static_cast<uint32_t>(nb);
        flags = fl;
        iflag = 0;
        exptime = exp;
        data = new char[nbytes];
    }

    Item(const std::string &k, const int fl, const rel_time_t exp,
         const void *dta, const size_t nb) :
        key(k)
    {
        nkey = static_cast<uint16_t>(key.length());
        nbytes = static_cast<uint32_t>(nb);
        flags = fl;
        iflag = 0;
        exptime = exp;
        data = new char[nbytes];
        memcpy(data, dta, nbytes);
    }

    ~Item() {
        delete []data;
    }

    std::string key;
    char *data;
};

/**
 * I don't care about the set callbacks right now, but since this is a
 * demo, let's dump core if one of them fails so that we can debug it later
 */
class IgnoreCallback : public Callback<bool>
{
    virtual void callback(bool &value) {
        assert(value);
    }
};

/**
 *
 */
class EventuallyPersistentEngine : public ENGINE_HANDLE_V1 {
public:
    ENGINE_ERROR_CODE initialize(const char* config)
    {
        ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

        pthread_mutex_init(&stats.lock, NULL);

        if (config != NULL) {
            char *dbn = NULL;
            const int max_items = 4;
            struct config_item items[max_items];
            int ii = 0;
            memset(items, 0, sizeof(items));

            items[ii].key = "dbname";
            items[ii].datatype = DT_STRING;
            items[ii].value.dt_string = &dbn;

            ++ii;
            items[ii].key = "warmup";
            items[ii].datatype = DT_BOOL;
            items[ii].value.dt_bool = &warmup;

            ++ii;
            items[ii].key = "config_file";
            items[ii].datatype = DT_CONFIGFILE;

            ++ii;
            items[ii].key = NULL;

            if (serverApi.parse_config(config, items, stderr) != 0) {
                ret = ENGINE_FAILED;
            } else {
                if (dbn != NULL) {
                    dbname = dbn;
                }
            }
        }

        if (ret == ENGINE_SUCCESS) {
            Sqlite3 *db = new Sqlite3(dbname);
            EventuallyPersistentStore *epstore;
            backend = epstore = new EventuallyPersistentStore(db);

            if (backend == NULL) {
                ret = ENGINE_ENOMEM;
            } else {
                if (warmup) {
                    db->init();
                    db->dump(epstore->getLoadStorageKVPairCallback());
                } else {
                    backend->reset();
                }
            }
        }

        return ret;
    }

    void destroy()
    {
        // empty
    }

    ENGINE_ERROR_CODE itemAllocate(const void* cookie,
                                   item** item,
                                   const void* key,
                                   const size_t nkey,
                                   const size_t nbytes,
                                   const int flags,
                                   const rel_time_t exptime)
    {
        (void)cookie;
        *item = new Item(key, nkey, nbytes, flags, exptime);
        if (*item == NULL) {
            return ENGINE_ENOMEM;
        } else {
            return ENGINE_SUCCESS;
        }
    }

    ENGINE_ERROR_CODE itemDelete(const void* cookie, item *item)
    {
        (void)cookie;
        (void)item;
        return ENGINE_ENOTSUP;
    }

    void itemRelease(const void* cookie, item *item)
    {
        (void)cookie;
        delete (Item*)item;
    }

    ENGINE_ERROR_CODE get(const void* cookie,
                          item** item,
                          const void* key,
                          const int nkey)
    {
        (void)cookie;
        std::string k(static_cast<const char*>(key), nkey);
        RememberingCallback<GetValue> getCb;
        backend->get(k, getCb);
        getCb.waitForValue();
        if (getCb.val.success) {
            *item = new Item(k, nkey, 0, getCb.val.value.c_str(),
                             getCb.val.value.length());
            return ENGINE_SUCCESS;
        } else {
            return ENGINE_KEY_ENOENT;
        }
    }

    ENGINE_ERROR_CODE getStats(const void* cookie,
                               const char* stat_key,
                               int nkey,
                               ADD_STAT add_stat)
    {
        (void)cookie;
        (void)nkey;
        (void)add_stat;

        const char *key;
        LockHolder lh(&stats.lock);
        if (stat_key == NULL) {
            // @todo add interesting stats
            struct ep_stats epstats;

            EventuallyPersistentStore *epstore;
            epstore = dynamic_cast<EventuallyPersistentStore *>(backend);

            if (epstore) {
                epstore->getStats(&epstats);

                char ageS[32];
                snprintf(ageS, sizeof(ageS), "%u",
                         (unsigned int)epstats.dirtyAge);
                key = "ep_storage_age";
                add_stat(key, strlen(key), ageS, strlen(ageS), cookie);

                snprintf(ageS, sizeof(ageS), "%u",
                         (unsigned int)epstats.dirtyAgeHighWat);
                key = "ep_storage_age_highwat";
                add_stat(key, strlen(key), ageS, strlen(ageS), cookie);

                snprintf(ageS, sizeof(ageS), "%u",
                         (unsigned int)epstats.queue_size);
                key = "ep_queue_size";
                add_stat(key, strlen(key), ageS, strlen(ageS), cookie);

                snprintf(ageS, sizeof(ageS), "%u",
                         (unsigned int)epstats.flusher_todo);
                key = "ep_flusher_todo";
                add_stat(key, strlen(key), ageS, strlen(ageS), cookie);

                snprintf(ageS, sizeof(ageS), "%u",
                         (unsigned int)epstats.commit_time);
                key = "ep_commit_time";
                add_stat(key, strlen(key), ageS, strlen(ageS), cookie);
            }

            key = "dbname";
            add_stat(key, strlen(key), dbname, strlen(dbname), cookie);

            key = "warmup";
            const char *val = warmup ? "true" : "false";
            add_stat(key, strlen(key), val, strlen(val), cookie);
        }
        return ENGINE_SUCCESS;
    }

    ENGINE_ERROR_CODE store(const void *cookie,
                            item* item,
                            uint64_t *cas,
                            ENGINE_STORE_OPERATION operation)
    {
        (void)cookie;
        Item *it = static_cast<Item*>(item);
        if (operation == OPERATION_SET) {
            backend->set(it->key, it->data, it->nbytes, ignoreCallback);
            *cas = 0;
            return ENGINE_SUCCESS;
        } else {
            return ENGINE_ENOTSUP;
        }
    }

    ENGINE_ERROR_CODE flush(const void *cookie, time_t when)
    {
        (void)cookie;
        if (when != 0) {
            return ENGINE_ENOTSUP;
        }

        return ENGINE_SUCCESS;
    }

    void resetStats()
    {
        LockHolder lh(&stats.lock);
        EventuallyPersistentStore *epstore;
        epstore = dynamic_cast<EventuallyPersistentStore *>(backend);

        if (epstore) {
            epstore->resetStats();
        }

        // @todo reset statistics
    }

private:
    EventuallyPersistentEngine(SERVER_HANDLE_V1 *sApi);
    friend ENGINE_ERROR_CODE create_instance(uint64_t interface,
                                             GET_SERVER_API get_server_api,
                                             ENGINE_HANDLE **handle);

    const char *dbname;
    bool warmup;
    SERVER_HANDLE_V1 serverApi;
    IgnoreCallback ignoreCallback;
    KVStore *backend;
    struct engine_stats stats;
};
