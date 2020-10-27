/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "default_engine_internal.h"
#include "default_engine_public.h"
#include "engine_manager.h"

#include <memcached/config_parser.h>
#include <memcached/durability_spec.h>
#include <memcached/protocol_binary.h>
#include <memcached/server_cookie_iface.h>
#include <memcached/server_core_iface.h>
#include <memcached/util.h>
#include <platform/cb_malloc.h>
#include <platform/cbassert.h>

#include <unistd.h>
#include <cctype>
#include <cinttypes>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string_view>

// The default engine don't really use vbucket uuids, but in order
// to run the unit tests and verify that we correctly convert the
// vbucket uuid to network byte order it is nice to have a value
// we may use for testing ;)
#define DEFAULT_ENGINE_VBUCKET_UUID 0xdeadbeef

using namespace std::string_view_literals;

static ENGINE_ERROR_CODE initalize_configuration(struct default_engine *se,
                                                 const char *cfg_str);
union vbucket_info_adapter {
    char c;
    struct vbucket_info v;
};

static void set_vbucket_state(struct default_engine* e,
                              Vbid vbid,
                              vbucket_state_t to) {
    union vbucket_info_adapter vi;
    vi.c = e->vbucket_infos[vbid.get()];
    vi.v.state = to;
    e->vbucket_infos[vbid.get()] = vi.c;
}

static vbucket_state_t get_vbucket_state(struct default_engine* e, Vbid vbid) {
    union vbucket_info_adapter vi;
    vi.c = e->vbucket_infos[vbid.get()];
    return vbucket_state_t(vi.v.state);
}

static bool handled_vbucket(struct default_engine* e, Vbid vbid) {
    return e->config.ignore_vbucket
        || (get_vbucket_state(e, vbid) == vbucket_state_active);
}

/* mechanism for handling bad vbucket requests */
#define VBUCKET_GUARD(e, v) if (!handled_vbucket(e, v)) { return ENGINE_NOT_MY_VBUCKET; }

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
    engine->bucket_id = id;
    engine->config.verbose = 0;
    engine->config.oldest_live = 0;
    engine->config.evict_to_free = true;
    engine->config.maxbytes = 64 * 1024 * 1024;
    engine->config.preallocate = false;
    engine->config.factor = 1.25;
    engine->config.chunk_size = 48;
    engine->config.item_size_max= 1024 * 1024;
    engine->config.xattr_enabled = true;
    engine->config.compression_mode = BucketCompressionMode::Off;
    engine->config.min_compression_ratio = default_min_compression_ratio;
}

ENGINE_ERROR_CODE create_memcache_instance(GET_SERVER_API get_server_api,
                                           EngineIface** handle) {
    ServerApi* api = get_server_api();
    struct default_engine* engine;

    if (api == nullptr) {
        return ENGINE_ENOTSUP;
    }

    if ((engine = engine_manager_create_engine()) == nullptr) {
        return ENGINE_ENOMEM;
    }

    engine->server = *api;
    engine->get_server_api = get_server_api;
    engine->initialized = true;
    *handle = engine;
    return ENGINE_SUCCESS;
}

void destroy_memcache_engine() {
    engine_manager_shutdown();
    assoc_destroy();
}

static struct default_engine* get_handle(EngineIface* handle) {
    return (struct default_engine*)handle;
}

static hash_item* get_real_item(ItemIface* item) {
    return (hash_item*)item;
}

ENGINE_ERROR_CODE default_engine::initialize(const char* config_str) {
    ENGINE_ERROR_CODE ret = initalize_configuration(this, config_str);
    if (ret != ENGINE_SUCCESS) {
        return ret;
    }

    ret = assoc_init(this);
    if (ret != ENGINE_SUCCESS) {
        return ret;
    }

    ret = slabs_init(this, config.maxbytes, config.factor, config.preallocate);
    if (ret != ENGINE_SUCCESS) {
        return ret;
    }

    return ENGINE_SUCCESS;
}

void default_engine::destroy(const bool force) {
    engine_manager_delete_engine(this);
}

void destroy_engine_instance(struct default_engine* engine) {
    if (engine->initialized) {
        /* Destory the slabs cache */
        slabs_destroy(engine);

        cb_free(engine->config.uuid);
        engine->initialized = false;
    }
}

cb::EngineErrorItemPair default_engine::allocate(
        gsl::not_null<const void*> cookie,
        const DocKey& key,
        size_t nbytes,
        int flags,
        rel_time_t exptime,
        uint8_t datatype,
        Vbid vbucket) {
    try {
        auto pair = allocate_ex(cookie,
                                key,
                                nbytes,
                                0, // No privileged bytes
                                flags,
                                exptime,
                                datatype,
                                vbucket);
        return {cb::engine_errc::success, std::move(pair.first)};
    } catch (const cb::engine_error& error) {
        return cb::makeEngineErrorItemPair(
                cb::engine_errc(error.code().value()));
    }
}

std::pair<cb::unique_item_ptr, item_info> default_engine::allocate_ex(
        gsl::not_null<const void*> cookie,
        const DocKey& key,
        size_t nbytes,
        size_t priv_nbytes,
        int flags,
        rel_time_t exptime,
        uint8_t datatype,
        Vbid vbucket) {
    hash_item *it;

    unsigned int id;

    if (!handled_vbucket(this, vbucket)) {
        throw cb::engine_error(cb::engine_errc::not_my_vbucket,
                               "default_item_allocate_ex");
    }

    // MB-35696: Only the default collection is permitted
    if (!key.getCollectionID().isDefaultCollection()) {
        generate_unknown_collection_response(cookie);
        throw cb::engine_error(cb::engine_errc::unknown_collection,
                               "default_item_allocate_ex: only default "
                               "collection is supported");
    }

    size_t ntotal = sizeof(hash_item) + key.size() + nbytes;
    id = slabs_clsid(this, ntotal);
    if (id == 0) {
        throw cb::engine_error(cb::engine_errc::too_big,
                               "default_item_allocate_ex: no slab class");
    }

    if ((nbytes - priv_nbytes) > config.item_size_max) {
        throw cb::engine_error(cb::engine_errc::too_big,
                               "default_item_allocate_ex");
    }

    it = item_alloc(this,
                    key,
                    flags,
                    server.core->realtime(exptime),
                    (uint32_t)nbytes,
                    cookie,
                    datatype);

    if (it != nullptr) {
        item_info info;
        if (!get_item_info(it, &info)) {
            // This should never happen (unless we provide invalid
            // arguments)
            item_release(this, it);
            throw cb::engine_error(cb::engine_errc::failed,
                                   "default_item_allocate_ex");
        }

        return std::make_pair(cb::unique_item_ptr(it, cb::ItemDeleter{this}),
                              info);
    } else {
        throw cb::engine_error(cb::engine_errc::no_memory,
                               "default_item_allocate_ex");
    }
}

ENGINE_ERROR_CODE default_engine::remove(
        gsl::not_null<const void*> cookie,
        const DocKey& key,
        uint64_t& cas,
        Vbid vbucket,
        const std::optional<cb::durability::Requirements>& durability,
        mutation_descr_t& mut_info) {
    if (durability) {
        return ENGINE_ENOTSUP;
    }
    // MB-35696: Only the default collection is permitted
    if (!key.getCollectionID().isDefaultCollection()) {
        return ENGINE_UNKNOWN_COLLECTION;
    }

    hash_item* it;
    uint64_t cas_in = cas;
    VBUCKET_GUARD(this, vbucket);

    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    do {
        it = item_get(this, cookie, key, DocStateFilter::Alive);
        if (it == nullptr) {
            return ENGINE_KEY_ENOENT;
        }

        if (it->locktime != 0 &&
            it->locktime > server.core->get_current_time()) {
            if (cas_in != it->cas) {
                item_release(this, it);
                return ENGINE_LOCKED;
            }
        }

        auto* deleted = item_alloc(this,
                                   key,
                                   it->flags,
                                   it->exptime,
                                   0,
                                   cookie,
                                   PROTOCOL_BINARY_RAW_BYTES);

        if (deleted == nullptr) {
            item_release(this, it);
            return ENGINE_TMPFAIL;
        }

        if (cas_in == 0) {
            // If the caller specified the "cas wildcard" we should set
            // the cas for the item we just fetched and do a cas
            // replace with that value
            item_set_cas(deleted, it->cas);
        } else {
            // The caller specified a specific CAS value so we should
            // use that value in our cas replace
            item_set_cas(deleted, cas_in);
        }

        ret = store_item(this,
                         deleted,
                         &cas,
                         OPERATION_CAS,
                         cookie,
                         DocumentState::Deleted,
                         false);

        item_release(this, it);
        item_release(this, deleted);

        // We should only retry for race conditions if the caller specified
        // cas wildcard
    } while (ret == ENGINE_KEY_EEXISTS && cas_in == 0);

    // vbucket UUID / seqno arn't supported by default engine, so just return
    // a hardcoded vbucket uuid, and zero for the sequence number.
    mut_info.vbucket_uuid = DEFAULT_ENGINE_VBUCKET_UUID;
    mut_info.seqno = 0;

    return ret;
}

void default_engine::release(gsl::not_null<ItemIface*> item) {
    item_release(this, get_real_item(item));
}

cb::EngineErrorItemPair default_engine::get(
        gsl::not_null<const void*> cookie,
        const DocKey& key,
        Vbid vbucket,
        DocStateFilter documentStateFilter) {
    if (!handled_vbucket(this, vbucket)) {
        return std::make_pair(
                cb::engine_errc::not_my_vbucket,
                cb::unique_item_ptr{nullptr, cb::ItemDeleter{this}});
    }
    // MB-35696: Only the default collection is permitted
    if (!key.getCollectionID().isDefaultCollection()) {
        generate_unknown_collection_response(cookie);
        return cb::makeEngineErrorItemPair(cb::engine_errc::unknown_collection);
    }

    auto* it = item_get(this, cookie, key, documentStateFilter);
    if (it != nullptr) {
        return cb::makeEngineErrorItemPair(cb::engine_errc::success, it, this);
    } else {
        return cb::makeEngineErrorItemPair(cb::engine_errc::no_such_key);
    }
}

cb::EngineErrorItemPair default_engine::get_if(
        gsl::not_null<const void*> cookie,
        const DocKey& key,
        Vbid vbucket,
        std::function<bool(const item_info&)> filter) {
    if (!handled_vbucket(this, vbucket)) {
        return cb::makeEngineErrorItemPair(cb::engine_errc::not_my_vbucket);
    }
    // MB-35696: Only the default collection is permitted
    if (!key.getCollectionID().isDefaultCollection()) {
        generate_unknown_collection_response(cookie);
        return cb::makeEngineErrorItemPair(cb::engine_errc::unknown_collection);
    }

    cb::unique_item_ptr ret(item_get(this, cookie, key, DocStateFilter::Alive),
                            cb::ItemDeleter{this});
    if (!ret) {
        return cb::makeEngineErrorItemPair(cb::engine_errc::no_such_key);
    }

    item_info info;
    if (!get_item_info(ret.get(), &info)) {
        throw cb::engine_error(cb::engine_errc::failed,
                               "default_get_if: get_item_info failed");
    }

    if (!filter(info)) {
        ret.reset(nullptr);
    }

    return cb::makeEngineErrorItemPair(
            cb::engine_errc::success, ret.release(), this);
}

cb::EngineErrorItemPair default_engine::get_and_touch(
        gsl::not_null<const void*> cookie,
        const DocKey& key,
        Vbid vbucket,
        uint32_t expiry_time,
        const std::optional<cb::durability::Requirements>& durability) {
    if (durability) {
        return cb::makeEngineErrorItemPair(cb::engine_errc::not_supported);
    }
    // MB-35696: Only the default collection is permitted
    if (!key.getCollectionID().isDefaultCollection()) {
        generate_unknown_collection_response(cookie);
        return cb::makeEngineErrorItemPair(cb::engine_errc::unknown_collection);
    }

    if (!handled_vbucket(this, vbucket)) {
        return cb::makeEngineErrorItemPair(cb::engine_errc::not_my_vbucket);
    }

    hash_item* it = nullptr;
    auto ret = item_get_and_touch(
            this, cookie, &it, key, server.core->realtime(expiry_time));

    return cb::makeEngineErrorItemPair(
            cb::engine_errc(ret), reinterpret_cast<ItemIface*>(it), this);
}

cb::EngineErrorItemPair default_engine::get_locked(
        gsl::not_null<const void*> cookie,
        const DocKey& key,
        Vbid vbucket,
        uint32_t lock_timeout) {
    if (!handled_vbucket(this, vbucket)) {
        return cb::makeEngineErrorItemPair(cb::engine_errc::not_my_vbucket);
    }
    // MB-35696: Only the default collection is permitted
    if (!key.getCollectionID().isDefaultCollection()) {
        generate_unknown_collection_response(cookie);
        return cb::makeEngineErrorItemPair(cb::engine_errc::unknown_collection);
    }

    // memcached buckets don't offer any way for the user to configure
    // the lock settings.
    static const uint32_t default_lock_timeout = 15;
    static const uint32_t max_lock_timeout = 30;

    if (lock_timeout == 0 || lock_timeout > max_lock_timeout) {
        lock_timeout = default_lock_timeout;
    }

    // Convert the lock timeout to an absolute time
    lock_timeout += server.core->get_current_time();

    hash_item* it = nullptr;
    auto ret = item_get_locked(this, cookie, &it, key, lock_timeout);
    return cb::makeEngineErrorItemPair(cb::engine_errc(ret), it, this);
}

cb::EngineErrorMetadataPair default_engine::get_meta(
        gsl::not_null<const void*> cookie, const DocKey& key, Vbid vbucket) {
    if (!handled_vbucket(this, vbucket)) {
        return std::make_pair(cb::engine_errc::not_my_vbucket, item_info());
    }
    // MB-35696: Only the default collection is permitted
    if (!key.getCollectionID().isDefaultCollection()) {
        generate_unknown_collection_response(cookie);
        return std::make_pair(cb::engine_errc::unknown_collection, item_info());
    }

    cb::unique_item_ptr item{
            item_get(this, cookie, key, DocStateFilter::AliveOrDeleted),
            cb::ItemDeleter(this)};

    if (!item) {
        return std::make_pair(cb::engine_errc::no_such_key, item_info());
    }

    item_info info;
    if (!get_item_info(item.get(), &info)) {
        throw cb::engine_error(cb::engine_errc::failed,
                               "default_get_if: get_item_info failed");
    }

    return std::make_pair(cb::engine_errc::success, info);
}

ENGINE_ERROR_CODE default_engine::unlock(gsl::not_null<const void*> cookie,
                                         const DocKey& key,
                                         Vbid vbucket,
                                         uint64_t cas) {
    VBUCKET_GUARD(this, vbucket);
    // MB-35696: Only the default collection is permitted
    if (!key.getCollectionID().isDefaultCollection()) {
        return ENGINE_UNKNOWN_COLLECTION;
    }
    return item_unlock(this, cookie, key, cas);
}

ENGINE_ERROR_CODE default_engine::get_stats(gsl::not_null<const void*> cookie,
                                            std::string_view key,
                                            std::string_view value,
                                            const AddStatFn& add_stat) {
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    if (key.empty()) {
        add_stat("evictions"sv, std::to_string(stats.evictions.load()), cookie);
        add_stat("curr_items"sv,
                 std::to_string(stats.curr_items.load()),
                 cookie);
        add_stat("total_items"sv,
                 std::to_string(stats.total_items.load()),
                 cookie);
        add_stat("bytes"sv, std::to_string(stats.curr_bytes.load()), cookie);
        add_stat("reclaimed"sv, std::to_string(stats.reclaimed.load()), cookie);
        add_stat("engine_maxbytes"sv, std::to_string(config.maxbytes), cookie);
    } else if (key == "slabs"sv) {
        slabs_stats(this, add_stat, cookie);
    } else if (key == "items"sv) {
        item_stats(this, add_stat, cookie);
    } else if (key == "sizes"sv) {
        item_stats_sizes(this, add_stat, cookie);
    } else if (key == "uuid"sv) {
        add_stat("uuid"sv, config.uuid, cookie);
    } else if (key == "scrub"sv) {
        std::lock_guard<std::mutex> guard(scrubber.lock);
        if (scrubber.running) {
            add_stat("scrubber:status"sv, "running"sv, cookie);
        } else {
            add_stat("scrubber:status"sv, "stopped"sv, cookie);
        }

        if (scrubber.started != 0) {
            if (scrubber.stopped != 0) {
                time_t diff = scrubber.started - scrubber.stopped;
                add_stat("scrubber:last_run"sv, std::to_string(diff), cookie);
            }
            add_stat("scrubber:visited"sv,
                     std::to_string(scrubber.visited),
                     cookie);
            add_stat("scrubber:cleaned"sv,
                     std::to_string(scrubber.cleaned),
                     cookie);
        }
    } else {
        ret = ENGINE_KEY_ENOENT;
    }

    return ret;
}

ENGINE_ERROR_CODE default_engine::store(
        gsl::not_null<const void*> cookie,
        gsl::not_null<ItemIface*> item,
        uint64_t& cas,
        ENGINE_STORE_OPERATION operation,
        const std::optional<cb::durability::Requirements>& durability,
        DocumentState document_state,
        bool preserveTtl) {
    if (durability) {
        return ENGINE_ENOTSUP;
    }

    auto* it = get_real_item(item);

    if (document_state == DocumentState::Deleted && !config.keep_deleted) {
        return safe_item_unlink(this, it);
    }

    return store_item(
            this, it, &cas, operation, cookie, document_state, preserveTtl);
}

cb::EngineErrorCasPair default_engine::store_if(
        gsl::not_null<const void*> cookie,
        gsl::not_null<ItemIface*> item,
        uint64_t cas,
        ENGINE_STORE_OPERATION operation,
        const cb::StoreIfPredicate& predicate,
        const std::optional<cb::durability::Requirements>& durability,
        DocumentState document_state,
        bool preserveTtl) {
    if (durability) {
        return {cb::engine_errc::not_supported, 0};
    }

    if (predicate) {
        // Check for an existing item and call the item predicate on it.
        auto* it = get_real_item(item);
        auto* key = item_get_key(it);
        if (!key) {
            throw cb::engine_error(cb::engine_errc::failed,
                                   "default_store_if: item_get_key failed");
        }
        cb::unique_item_ptr existing(
                item_get(this, cookie, *key, DocStateFilter::Alive),
                cb::ItemDeleter{this});

        cb::StoreIfStatus status;
        if (existing.get()) {
            item_info info;
            if (!get_item_info(existing.get(), &info)) {
                throw cb::engine_error(
                        cb::engine_errc::failed,
                        "default_store_if: get_item_info failed");
            }
            status = predicate(info, {true});
        } else {
            status = predicate(std::nullopt, {true});
        }

        switch (status) {
        case cb::StoreIfStatus::Fail: {
            return {cb::engine_errc::predicate_failed, 0};
        }
        case cb::StoreIfStatus::Continue:
        case cb::StoreIfStatus::GetItemInfo: {
            break;
        }
        }
    }

    auto* it = get_real_item(item);
    auto status = store_item(
            this, it, &cas, operation, cookie, document_state, preserveTtl);
    return {cb::engine_errc(status), cas};
}

ENGINE_ERROR_CODE default_engine::flush(gsl::not_null<const void*> cookie) {
    item_flush_expired(get_handle(this));

    return ENGINE_SUCCESS;
}

void default_engine::reset_stats(gsl::not_null<const void*> cookie) {
    item_stats_reset(this);

    stats.evictions.store(0);
    stats.reclaimed.store(0);
    stats.total_items.store(0);
}

static ENGINE_ERROR_CODE initalize_configuration(struct default_engine *se,
                                                 const char *cfg_str) {
   ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

   se->config.vb0 = true;

   if (cfg_str != nullptr) {
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

       items[ii].key = nullptr;
       ++ii;
       cb_assert(ii == 13);
       ret = ENGINE_ERROR_CODE(se->server.core->parse_config(cfg_str,
                                                             items,
                                                             stderr));
   }

   if (se->config.vb0) {
       set_vbucket_state(se, Vbid(0), vbucket_state_active);
   }

   return ret;
}

static bool set_vbucket(struct default_engine* e,
                        const void* cookie,
                        const cb::mcbp::Request& request,
                        const AddResponseFn& response) {
    vbucket_state_t state;
    auto extras = request.getExtdata();
    std::copy(extras.begin(), extras.end(), reinterpret_cast<uint8_t*>(&state));
    state = vbucket_state_t(ntohl(state));

    set_vbucket_state(e, request.getVBucket(), state);
    return response({},
                    {},
                    {reinterpret_cast<const char*>(&state), sizeof(state)},
                    PROTOCOL_BINARY_RAW_BYTES,
                    cb::mcbp::Status::Success,
                    0,
                    cookie);
}

static bool get_vbucket(struct default_engine* e,
                        const void* cookie,
                        const cb::mcbp::Request& request,
                        const AddResponseFn& response) {
    vbucket_state_t state;
    state = get_vbucket_state(e, request.getVBucket());
    state = vbucket_state_t(ntohl(state));

    return response({},
                    {},
                    {reinterpret_cast<const char*>(&state), sizeof(state)},
                    PROTOCOL_BINARY_RAW_BYTES,
                    cb::mcbp::Status::Success,
                    0,
                    cookie);
}

static bool rm_vbucket(struct default_engine* e,
                       const void* cookie,
                       const cb::mcbp::Request& request,
                       const AddResponseFn& response) {
    set_vbucket_state(e, request.getVBucket(), vbucket_state_dead);
    return response({},
                    {},
                    {},
                    PROTOCOL_BINARY_RAW_BYTES,
                    cb::mcbp::Status::Success,
                    0,
                    cookie);
}

static bool scrub_cmd(struct default_engine* e,
                      const void* cookie,
                      const AddResponseFn& response) {
    auto res = cb::mcbp::Status::Success;
    if (!item_start_scrub(e)) {
        res = cb::mcbp::Status::Ebusy;
    }

    return response({}, {}, {}, PROTOCOL_BINARY_RAW_BYTES, res, 0, cookie);
}

/**
 * set_param only added to allow per bucket xattr on/off
 * and toggle between compression modes for testing purposes
 */
static bool set_param(struct default_engine* e,
                      const void* cookie,
                      const cb::mcbp::Request& request,
                      const AddResponseFn& response) {
    using cb::mcbp::request::SetParamPayload;
    auto extras = request.getExtdata();
    auto* payload = reinterpret_cast<const SetParamPayload*>(extras.data());

    // Only support protocol_binary_engine_param_flush with xattr_enabled
    if (payload->getParamType() == SetParamPayload::Type::Flush) {
        auto k = request.getKey();
        auto v = request.getValue();

        std::string_view key{reinterpret_cast<const char*>(k.data()), k.size()};
        std::string_view value{reinterpret_cast<const char*>(v.data()),
                               v.size()};

        if (key == "xattr_enabled") {
            if (value == "true") {
                e->config.xattr_enabled = true;
            } else if (value == "false") {
                e->config.xattr_enabled = false;
            } else {
                return false;
            }
        } else if (key == "compression_mode") {
            try {
                e->config.compression_mode = parseCompressionMode(
                        std::string(value.data(), value.size()));
            } catch (std::invalid_argument&) {
                return false;
            }
        } else if (key == "min_compression_ratio") {
            std::string value_str{value.data(), value.size()};
            float min_comp_ratio;
            if (!safe_strtof(value_str.c_str(), min_comp_ratio)) {
                return false;
            }

            e->config.min_compression_ratio = min_comp_ratio;
        }

        return response({},
                        {},
                        {},
                        PROTOCOL_BINARY_RAW_BYTES,
                        cb::mcbp::Status::Success,
                        0,
                        cookie);
    }
    return false;
}

ENGINE_ERROR_CODE default_engine::unknown_command(
        const void* cookie,
        const cb::mcbp::Request& request,
        const AddResponseFn& response) {
    bool sent;

    switch (request.getClientOpcode()) {
    case cb::mcbp::ClientOpcode::Scrub:
        sent = scrub_cmd(this, cookie, response);
        break;
    case cb::mcbp::ClientOpcode::DelVbucket:
        sent = rm_vbucket(this, cookie, request, response);
        break;
    case cb::mcbp::ClientOpcode::SetVbucket:
        sent = set_vbucket(this, cookie, request, response);
        break;
    case cb::mcbp::ClientOpcode::GetVbucket:
        sent = get_vbucket(this, cookie, request, response);
        break;
    case cb::mcbp::ClientOpcode::SetParam:
        sent = set_param(this, cookie, request, response);
        break;
    default:
        sent = response({},
                        {},
                        {},
                        PROTOCOL_BINARY_RAW_BYTES,
                        cb::mcbp::Status::UnknownCommand,
                        0,
                        cookie);
        break;
    }

    if (sent) {
        return ENGINE_SUCCESS;
    } else {
        return ENGINE_FAILED;
    }
}

void default_engine::item_set_cas(gsl::not_null<ItemIface*> item,
                                  uint64_t val) {
    hash_item* it = get_real_item(item);
    it->cas = val;
}

void default_engine::item_set_datatype(gsl::not_null<ItemIface*> item,
                                       protocol_binary_datatype_t val) {
    auto* it = reinterpret_cast<hash_item*>(item.get());
    it->datatype = val;
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

bool default_engine::get_item_info(gsl::not_null<const ItemIface*> item,
                                   gsl::not_null<item_info*> item_info) {
    auto* it = reinterpret_cast<const hash_item*>(item.get());
    const hash_key* key = item_get_key(it);

    // This may potentially open up for a race, but:
    // 1) If the item isn't linked anymore we don't need to mask
    //    the CAS anymore. (if the client tries to use that
    //    CAS it'll fail with an invalid cas)
    // 2) In production the memcached buckets don't use the
    //    ZOMBIE state (and if we start doing that, it is only
    //    the owner of the item pointer (the one bumping the
    //    refcount initially) which would change this. Anyone else
    //    would create a new item object and set the iflag
    //    to deleted.
    const auto iflag = it->iflag.load(std::memory_order_relaxed);

    if ((iflag & ITEM_LINKED) && it->locktime != 0 &&
        it->locktime > server.core->get_current_time()) {
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
    if (it->exptime == 0) {
        item_info->exptime = 0;
    } else {
        item_info->exptime = server.core->abstime(it->exptime);
    }
    item_info->nbytes = it->nbytes;
    item_info->flags = it->flags;
    item_info->key = {hash_key_get_client_key(key),
                      hash_key_get_client_key_len(key),
                      DocKeyEncodesCollectionId::No};
    item_info->value[0].iov_base = item_get_data(it);
    item_info->value[0].iov_len = it->nbytes;
    item_info->datatype = it->datatype;
    if (iflag & ITEM_ZOMBIE) {
        item_info->document_state = DocumentState::Deleted;
    } else {
        item_info->document_state = DocumentState::Alive;
    }
    return true;
}

cb::engine::FeatureSet default_engine::getFeatures() {
    cb::engine::FeatureSet features;
    features.emplace(cb::engine::Feature::Collections);
    return features;
}

bool default_engine::isXattrEnabled() {
    return config.xattr_enabled;
}

cb::HlcTime default_engine::getVBucketHlcNow(Vbid) {
    return {std::chrono::seconds::zero(), cb::HlcTime::Mode::Logical};
}

BucketCompressionMode default_engine::getCompressionMode() {
    return config.compression_mode;
}

float default_engine::getMinCompressionRatio() {
    return config.min_compression_ratio;
}

size_t default_engine::getMaxItemSize() {
    return config.item_size_max;
}

// Cannot set the manifest on the default engine
cb::engine_errc default_engine::set_collection_manifest(
        gsl::not_null<const void*> cookie, std::string_view json) {
    return cb::engine_errc::not_supported;
}

// always return the "epoch" manifest, default collection/scope only
cb::engine_errc default_engine::get_collection_manifest(
        gsl::not_null<const void*> cookie, const AddResponseFn& response) {
    static std::string default_manifest =
            R"({"uid" : "0",
        "scopes" : [{"name":"_default", "uid":"0",
        "collections" : [{"name":"_default","uid":"0"}]}]})";
    auto ok = response({},
                       {},
                       {default_manifest.data(), default_manifest.size()},
                       PROTOCOL_BINARY_DATATYPE_JSON,
                       cb::mcbp::Status::Success,
                       0,
                       cookie);
    return ok ? cb::engine_errc::success : cb::engine_errc::failed;
}

static std::string_view default_scope_path{"_default."};

// permit lookup of the default collection only
cb::EngineErrorGetCollectionIDResult default_engine::get_collection_id(
        gsl::not_null<const void*> cookie, std::string_view path) {
    if (std::count(path.begin(), path.end(), '.') == 1) {
        cb::engine_errc error = cb::engine_errc::unknown_scope;
        if (path == "_default._default" || path == "._default" || path == "." ||
            path == "_default.") {
            return {0, ScopeID::Default, CollectionID::Default};
        } else if (path.find(default_scope_path) == 0) {
            // path starts with "_default." so collection part is unknown
            error = cb::engine_errc::unknown_collection;
        }
        generate_unknown_collection_response(cookie);
        // Return the manifest-uid of 0 and unknown scope or collection error
        return {error, 0};
    }
    return cb::EngineErrorGetCollectionIDResult{
            cb::engine_errc::invalid_arguments};
}

// permit lookup of the default scope only
cb::EngineErrorGetScopeIDResult default_engine::get_scope_id(
        gsl::not_null<const void*> cookie, std::string_view path) {
    auto dotCount = std::count(path.begin(), path.end(), '.');
    if (dotCount <= 1) {
        // only care about everything before .
        static std::string_view dot{"."};
        auto scope = dotCount ? path.substr(0, path.find(dot)) : path;

        if (scope.empty() || path == "_default") {
            return {0, ScopeID::Default};
        }
        generate_unknown_collection_response(cookie);
        // Return just the manifest-uid of 0 which sets error to unknown_scope
        return cb::EngineErrorGetScopeIDResult{0};
    }
    return cb::EngineErrorGetScopeIDResult{cb::engine_errc::invalid_arguments};
}

// permit lookup of the default scope only
cb::EngineErrorGetScopeIDResult default_engine::get_scope_id(
        gsl::not_null<const void*> cookie,
        const DocKey& key,
        std::optional<Vbid> vbid) const {
    if (key.getCollectionID().isDefaultCollection()) {
        return cb::EngineErrorGetScopeIDResult(0, ScopeID{ScopeID::Default});
    }
    return cb::EngineErrorGetScopeIDResult(cb::engine_errc::unknown_collection);
}

void default_engine::generate_unknown_collection_response(
        const void* cookie) const {
    // Default engine does not support collection changes, so is always
    // reporting 'unknown collection' against the epoch manifest (uid 0)
    server.cookie->set_unknown_collection_error_context(
            const_cast<void*>(cookie), 0);
}
