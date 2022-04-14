/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/*
 *                "ewouldblock_engine"
 *
 * The "ewouldblock_engine" allows one to test how memcached responds when the
 * engine returns EWOULDBLOCK instead of the correct response.
 *
 * Motivation:
 *
 * The EWOULDBLOCK response code can be returned from a number of engine
 * functions, and is used to indicate that the request could not be immediately
 * fulfilled, and it "would block" if it tried to. The correct way for
 * memcached to handle this (in general) is to suspend that request until it
 * is later notified by the engine (via notify_io_complete()).
 *
 * However, engines typically return the correct response to requests
 * immediately, only rarely (and from memcached's POV non-deterministically)
 * returning EWOULDBLOCK. This makes testing of the code-paths handling
 * EWOULDBLOCK tricky.
 *
 *
 * Operation:
 * This engine, when loaded by memcached proxies requests to a "real" engine.
 * Depending on how it is configured, it can simply pass the request on to the
 * real engine, or artificially return EWOULDBLOCK back to memcached.
 *
 * See the 'Modes' enum below for the possible modes for a connection. The mode
 * can be selected by sending a `request_ewouldblock_ctl` command
 *  (opcode cb::mcbp::ClientOpcode::EwouldblockCtl).
 *
 * DCP:
 *    There is a special DCP stream named "ewb_internal" which is an
 *    endless stream of items. You may also add a number at the end
 *    e.g. "ewb_internal:10" and it'll create a stream with 10 entries.
 *    It will always send the same K-V pair.
 *    Note that we don't register for disconnect events so you might
 *    experience weirdness if you first try to use the internal dcp
 *    stream, and then later on want to use the one provided by the
 *    engine. The workaround for that is to delete the bucket
 *    in between ;-) (put them in separate test suites and it'll all
 *    be handled for you.
 *
 *    Any other stream name results in proxying the dcp request to
 *    the underlying engine's DCP implementation.
 *
 */

#include "ewouldblock_engine.h"
#include "ewouldblock_engine_public.h"

#include <gsl/gsl-lite.hpp>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <utility>

#include <logger/logger.h>
#include <memcached/collections.h>
#include <memcached/dcp.h>
#include <memcached/durability_spec.h>
#include <memcached/engine.h>
#include <memcached/server_bucket_iface.h>
#include <memcached/server_cookie_iface.h>
#include <platform/cb_malloc.h>
#include <platform/cbassert.h>
#include <platform/dirutils.h>
#include <platform/thread.h>
#include <xattr/blob.h>

class EWB_Engine;

// Mapping from wrapped handle to EWB handles.
static std::map<EngineIface*, EWB_Engine*> engine_map;

class NotificationThread : public Couchbase::Thread {
public:
    explicit NotificationThread(EWB_Engine& engine_)
        : Thread("ewb:pendingQ"), engine(engine_) {
    }

protected:
    void run() override;

protected:
    EWB_Engine& engine;
};

// Current DCP mutation `item`. We return an instance of this
// (in the dcp step() function) back to the server, and then in
// get_item_info we check if the requested item is this one.
class EwbDcpMutationItem : public ItemIface {
public:
    EwbDcpMutationItem() : key("k") {
        cb::xattr::Blob builder;
        builder.set("_ewb", "{\"internal\":true}");
        builder.set("meta", R"({"author":"jack"})");
        const auto blob = builder.finalize();
        std::copy(blob.begin(), blob.end(), std::back_inserter(value));
        // MB24971 - the body is large as it increases the probability of
        // transit returning TransmitResult::SoftError
        const std::string body(1000, 'x');
        std::copy(body.begin(), body.end(), std::back_inserter(value));
    }

    DocKey getDocKey() const override {
        return DocKey(key, DocKeyEncodesCollectionId::No);
    }

    protocol_binary_datatype_t getDataType() const override {
        return PROTOCOL_BINARY_DATATYPE_XATTR;
    }

    uint64_t getCas() const override {
        return 0;
    }

    uint32_t getFlags() const override {
        return 0;
    }

    time_t getExptime() const override {
        return 0;
    }

    std::string_view getValueView() const override {
        return {reinterpret_cast<const char*>(value.data()), value.size()};
    }

    std::string key;
    std::vector<uint8_t> value;
};

/**
 * The BlockMonitorThread represents the thread that is
 * monitoring the "lock" file. Once the file is no longer
 * there it will resume the client specified with the given
 * id.
 */
class BlockMonitorThread : public Couchbase::Thread {
public:
    BlockMonitorThread(EWB_Engine& engine_,
                       uint32_t id_,
                       const std::string file_)
        : Thread("ewb:BlockMon"),
          engine(engine_),
          id(id_),
          file(file_) {}

    /**
     * Wait for the underlying thread to reach the zombie state
     * (== terminated, but not reaped)
     */
    ~BlockMonitorThread() override {
        waitForState(Couchbase::ThreadState::Zombie);
    }

protected:
    void run() override;

private:
    EWB_Engine& engine;
    const uint32_t id;
    const std::string file;
};

/** ewouldblock_engine class */
class EWB_Engine : public EngineIface,
                   public DcpIface,
                   public DcpConnHandlerIface {
private:
    enum class Cmd {
        NONE,
        GET_INFO,
        ALLOCATE,
        REMOVE,
        GET,
        STORE,
        CAS,
        ARITHMETIC,
        LOCK,
        UNLOCK,
        FLUSH,
        GET_STATS,
        GET_META,
        UNKNOWN_COMMAND
    };

    const char* to_string(Cmd cmd);

public:
    explicit EWB_Engine(GET_SERVER_API gsa_);

    ~EWB_Engine() override;

    void initiate_shutdown() override;

    void disconnect(const CookieIface& cookie) override {
        uint64_t id = real_api->cookie->get_connection_id(cookie);
        {
            std::lock_guard<std::mutex> guard(cookie_map_mutex);
            connection_map.erase(id);
        }

        if (real_engine) {
            real_engine->disconnect(cookie);
        }
    }

    /* Returns true if the next command should have a fake error code injected.
     * @param func Address of the command function (get, store, etc).
     * @param cookie The cookie for the user's request.
     * @param[out] Error code to return.
     */
    bool should_inject_error(Cmd cmd,
                             const CookieIface* cookie,
                             cb::engine_errc& err) {
        if (is_connection_suspended(cookie)) {
            err = cb::engine_errc::would_block;
            return true;
        }

        uint64_t id = real_api->cookie->get_connection_id(*cookie);

        std::lock_guard<std::mutex> guard(cookie_map_mutex);

        auto iter = connection_map.find(id);
        if (iter == connection_map.end()) {
            return false;
        }

        if (iter->second.first != cookie) {
            // The cookie is different so it represents a different command
            connection_map.erase(iter);
            return false;
        }

        const bool inject = iter->second.second->should_inject_error(cmd, err);

        if (inject) {
            LOG_DEBUG("EWB_Engine: injecting error:{} for cmd:{}",
                      err,
                      to_string(cmd));

            if (err == cb::engine_errc::would_block) {
                const auto add_to_pending_io_ops =
                        iter->second.second->add_to_pending_io_ops();
                if (add_to_pending_io_ops) {
                    // The server expects that if EWOULDBLOCK is returned then
                    // the server should be notified in the future when the
                    // operation is ready - so add this op to the pending IO
                    // queue.
                    schedule_notification(iter->second.first,
                                          *add_to_pending_io_ops);
                }
            }
        }

        return inject;
    }

    /* Implementation of all the engine functions. ***************************/

    cb::engine_errc initialize(const std::string& config_str) override {
        // Extract the name of the real engine we will be proxying; then
        // create and initialize it.
        std::string config(config_str);
        auto seperator = config.find(";");
        std::string real_engine_name(config.substr(0, seperator));
        std::string real_engine_config;
        if (seperator != std::string::npos) {
            real_engine_config = config.substr(seperator + 1);
        }

        real_engine = real_api->bucket->createBucket(real_engine_name, gsa);

        if (!real_engine) {
            LOG_CRITICAL(
                    "ERROR: EWB_Engine::initialize(): Failed create "
                    "engine instance '{}'",
                    real_engine_name);
            std::abort();
        }

        real_engine_dcp = dynamic_cast<DcpIface*>(real_engine.get());

        engine_map[real_engine.get()] = this;
        return real_engine->initialize(real_engine_config);
    }

    void destroy(bool force) override {
        real_engine->destroy(force);
        (void)real_engine.release();
        delete this;
    }

    std::pair<cb::unique_item_ptr, item_info> allocateItem(
            const CookieIface& cookie,
            const DocKey& key,
            size_t nbytes,
            size_t priv_nbytes,
            int flags,
            rel_time_t exptime,
            uint8_t datatype,
            Vbid vbucket) override {
        cb::engine_errc err = cb::engine_errc::success;
        if (should_inject_error(Cmd::ALLOCATE, &cookie, err)) {
            throw cb::engine_error(cb::engine_errc(err), "ewb: injecting error");
        } else {
            return real_engine->allocateItem(cookie,
                                             key,
                                             nbytes,
                                             priv_nbytes,
                                             flags,
                                             exptime,
                                             datatype,
                                             vbucket);
        }
    }

    cb::engine_errc remove(
            const CookieIface& cookie,
            const DocKey& key,
            uint64_t& cas,
            Vbid vbucket,
            const std::optional<cb::durability::Requirements>& durability,
            mutation_descr_t& mut_info) override {
        cb::engine_errc err = cb::engine_errc::success;
        if (should_inject_error(Cmd::REMOVE, &cookie, err)) {
            return err;
        } else {
            return real_engine->remove(
                    cookie, key, cas, vbucket, durability, mut_info);
        }
    }

    void release(ItemIface& item) override {
        LOG_DEBUG_RAW("EWB_Engine: release");
        if (dynamic_cast<EwbDcpMutationItem*>(&item)) {
            delete &item;
        } else {
            return real_engine->release(item);
        }
    }

    cb::EngineErrorItemPair get(const CookieIface& cookie,
                                const DocKey& key,
                                Vbid vbucket,
                                DocStateFilter documentStateFilter) override {
        cb::engine_errc err = cb::engine_errc::success;
        if (should_inject_error(Cmd::GET, &cookie, err)) {
            return std::make_pair(
                    cb::engine_errc(err),
                    cb::unique_item_ptr{nullptr, cb::ItemDeleter{this}});
        } else {
            return real_engine->get(cookie, key, vbucket, documentStateFilter);
        }
    }

    cb::EngineErrorItemPair get_if(
            const CookieIface& cookie,
            const DocKey& key,
            Vbid vbucket,
            std::function<bool(const item_info&)> filter) override {
        cb::engine_errc err = cb::engine_errc::success;
        if (should_inject_error(Cmd::GET, &cookie, err)) {
            return cb::makeEngineErrorItemPair(cb::engine_errc::would_block);
        } else {
            return real_engine->get_if(cookie, key, vbucket, filter);
        }
    }

    cb::EngineErrorItemPair get_and_touch(
            const CookieIface& cookie,
            const DocKey& key,
            Vbid vbucket,
            uint32_t exptime,
            const std::optional<cb::durability::Requirements>& durability)
            override {
        cb::engine_errc err = cb::engine_errc::success;
        if (should_inject_error(Cmd::GET, &cookie, err)) {
            return cb::makeEngineErrorItemPair(cb::engine_errc::would_block);
        } else {
            return real_engine->get_and_touch(
                    cookie, key, vbucket, exptime, durability);
        }
    }

    cb::EngineErrorItemPair get_locked(const CookieIface& cookie,
                                       const DocKey& key,
                                       Vbid vbucket,
                                       uint32_t lock_timeout) override {
        cb::engine_errc err = cb::engine_errc::success;
        if (should_inject_error(Cmd::LOCK, &cookie, err)) {
            return cb::makeEngineErrorItemPair(cb::engine_errc(err));
        } else {
            return real_engine->get_locked(cookie, key, vbucket, lock_timeout);
        }
    }

    cb::engine_errc unlock(const CookieIface& cookie,
                           const DocKey& key,
                           Vbid vbucket,
                           uint64_t cas) override {
        cb::engine_errc err = cb::engine_errc::success;
        if (should_inject_error(Cmd::UNLOCK, &cookie, err)) {
            return err;
        } else {
            return real_engine->unlock(cookie, key, vbucket, cas);
        }
    }

    cb::EngineErrorMetadataPair get_meta(const CookieIface& cookie,
                                         const DocKey& key,
                                         Vbid vbucket) override {
        cb::engine_errc err = cb::engine_errc::success;
        if (should_inject_error(Cmd::GET_META, &cookie, err)) {
            return std::make_pair(cb::engine_errc(err), item_info());
        } else {
            return real_engine->get_meta(cookie, key, vbucket);
        }
    }

    cb::engine_errc store(
            const CookieIface& cookie,
            ItemIface& item,
            uint64_t& cas,
            StoreSemantics operation,
            const std::optional<cb::durability::Requirements>& durability,
            DocumentState document_state,
            bool preserveTtl) override {
        cb::engine_errc err = cb::engine_errc::success;
        Cmd opcode = (operation == StoreSemantics::CAS) ? Cmd::CAS : Cmd::STORE;
        if (should_inject_error(opcode, &cookie, err)) {
            return err;
        } else {
            return real_engine->store(cookie,
                                      item,
                                      cas,
                                      operation,
                                      durability,
                                      document_state,
                                      preserveTtl);
        }
    }

    cb::EngineErrorCasPair store_if(
            const CookieIface& cookie,
            ItemIface& item,
            uint64_t cas,
            StoreSemantics operation,
            const cb::StoreIfPredicate& predicate,
            const std::optional<cb::durability::Requirements>& durability,
            DocumentState document_state,
            bool preserveTtl) override {
        cb::engine_errc err = cb::engine_errc::success;
        Cmd opcode = (operation == StoreSemantics::CAS) ? Cmd::CAS : Cmd::STORE;
        if (should_inject_error(opcode, &cookie, err)) {
            return {cb::engine_errc(err), 0};
        } else {
            return real_engine->store_if(cookie,
                                         item,
                                         cas,
                                         operation,
                                         predicate,
                                         durability,
                                         document_state,
                                         preserveTtl);
        }
    }

    cb::engine_errc flush(const CookieIface& cookie) override {
        // Flush is a little different - it often returns EWOULDBLOCK, and
        // notify_io_complete() just tells the server it can issue it's *next*
        // command (i.e. no need to re-flush). Therefore just pass Flush
        // straight through for now.
        return real_engine->flush(cookie);
    }

    cb::engine_errc get_stats(const CookieIface& cookie,
                              std::string_view key,
                              std::string_view value,
                              const AddStatFn& add_stat) override {
        cb::engine_errc err = cb::engine_errc::success;
        if (should_inject_error(Cmd::GET_STATS, &cookie, err)) {
            return err;
        } else {
            return real_engine->get_stats(cookie, key, value, add_stat);
        }
    }

    void reset_stats(const CookieIface& cookie) override {
        return real_engine->reset_stats(cookie);
    }

    /* Handle 'unknown_command'. In additional to wrapping calls to the
     * underlying real engine, this is also used to configure
     * ewouldblock_engine itself using he CMD_EWOULDBLOCK_CTL opcode.
     */
    cb::engine_errc unknown_command(const CookieIface* cookie,
                                    const cb::mcbp::Request& req,
                                    const AddResponseFn& response) override {
        const auto opcode = req.getClientOpcode();
        if (opcode == cb::mcbp::ClientOpcode::EwouldblockCtl) {
            using cb::mcbp::request::EWB_Payload;
            const auto& payload = req.getCommandSpecifics<EWB_Payload>();
            const auto mode = static_cast<EWBEngineMode>(payload.getMode());
            const auto value = payload.getValue();
            const auto injected_error =
                    static_cast<cb::engine_errc>(payload.getInjectError());
            auto k = req.getKey();
            const std::string key(reinterpret_cast<const char*>(k.data()),
                                  k.size());
            std::shared_ptr<FaultInjectMode> new_mode = nullptr;

            // Validate mode, and construct new fault injector.
            switch (mode) {
                case EWBEngineMode::Next_N:
                    new_mode = std::make_shared<ErrOnNextN>(injected_error, value);
                    break;

                case EWBEngineMode::Random:
                    new_mode = std::make_shared<ErrRandom>(injected_error, value);
                    break;

                case EWBEngineMode::First:
                    new_mode = std::make_shared<ErrOnFirst>(injected_error);
                    break;

                case EWBEngineMode::Sequence: {
                    std::vector<cb::engine_errc> decoded;
                    for (unsigned int ii = 0;
                         ii < key.size() / sizeof(cb::engine_errc);
                         ii++) {
                        auto status = *reinterpret_cast<const uint32_t*>(
                                key.data() + (ii * sizeof(cb::engine_errc)));
                        status = ntohl(status);
                        decoded.emplace_back(cb::engine_errc(status));
                    }
                    new_mode = std::make_shared<ErrSequence>(decoded);
                    break;
                }

                case EWBEngineMode::No_Notify:
                    new_mode = std::make_shared<ErrOnNoNotify>(injected_error);
                    break;

                case EWBEngineMode::CasMismatch:
                    new_mode = std::make_shared<CASMismatch>(value);
                    break;

                case EWBEngineMode::IncrementClusterMapRevno:
                    clustermap_revno++;
                    response({},
                             {},
                             {},
                             PROTOCOL_BINARY_RAW_BYTES,
                             cb::mcbp::Status::Success,
                             0,
                             cookie);
                    return cb::engine_errc::success;

                case EWBEngineMode::BlockMonitorFile:
                    return handleBlockMonitorFile(cookie, value, key, response);

                case EWBEngineMode::Suspend:
                    return handleSuspend(cookie, value, response);

                case EWBEngineMode::Resume:
                    return handleResume(cookie, value, response);

                case EWBEngineMode::SetItemCas:
                    return setItemCas(cookie, key, value, response);

                case EWBEngineMode::CheckLogLevels:
                    return checkLogLevels(cookie, value, response);

                case EWBEngineMode::ThrowException:
                    // Reserve the cookie and schedule a release of the
                    // cookie and throw an exception for the cookie
                    {
                        auto* cookie_api = gsa()->cookie;
                        cookie_api->reserve(*cookie);
                        std::thread release{[cookie_api, cookie]() {
                            // This will block on the thread mutex
                            cookie_api->release(*cookie);
                        }};
                        release.detach();
                    }
                    throw std::runtime_error(
                            "EWB::unknown_command: you told me to throw an "
                            "exception");
            }

            if (new_mode == nullptr) {
                LOG_WARNING(
                        "EWB_Engine::unknown_command(): "
                        "Got unexpected mode={} for EWOULDBLOCK_CTL, ",
                        (unsigned int)mode);
                response({},
                         {},
                         {},
                         PROTOCOL_BINARY_RAW_BYTES,
                         cb::mcbp::Status::Einval,
                         /*cas*/ 0,
                         cookie);
                return cb::engine_errc::failed;
            } else {
                try {
                    LOG_DEBUG(
                            "EWB_Engine::unknown_command(): Setting EWB mode "
                            "to "
                            "{} for cookie {}",
                            new_mode->to_string(),
                            static_cast<const void*>(cookie));

                    uint64_t id = real_api->cookie->get_connection_id(*cookie);

                    {
                        std::lock_guard<std::mutex> guard(cookie_map_mutex);
                        connection_map.erase(id);
                        connection_map.emplace(
                                id, std::make_pair(cookie, new_mode));
                    }

                    response({},
                             {},
                             {},
                             PROTOCOL_BINARY_RAW_BYTES,
                             cb::mcbp::Status::Success,
                             /*cas*/ 0,
                             cookie);
                    return cb::engine_errc::success;
                } catch (std::bad_alloc&) {
                    return cb::engine_errc::no_memory;
                }
            }
        } else {
            cb::engine_errc err = cb::engine_errc::success;
            if (should_inject_error(Cmd::UNKNOWN_COMMAND, cookie, err)) {
                return err;
            } else {
                return real_engine->unknown_command(cookie, req, response);
            }
        }
    }

    void item_set_cas(ItemIface& item, uint64_t cas) override {
        // function cannot return EWOULDBLOCK, simply call the real_engine's
        // function directly.
        real_engine->item_set_cas(item, cas);
    }

    void item_set_datatype(ItemIface& itm,
                           protocol_binary_datatype_t datatype) override {
        // function cannot return EWOULDBLOCK, simply call the real_engine's
        // function directly.
        real_engine->item_set_datatype(itm, datatype);
    }

    bool get_item_info(const ItemIface& item, item_info& item_info) override {
        LOG_DEBUG_RAW("EWB_Engine: get_item_info");

        // This function cannot return EWOULDBLOCK - just chain to the real
        // engine's function, unless it is a request for our special DCP item.
        const auto* ewbitem = dynamic_cast<const EwbDcpMutationItem*>(&item);
        if (ewbitem) {
            item_info.cas = 0;
            item_info.vbucket_uuid = 0;
            item_info.seqno = 0;
            item_info.exptime = 0;
            item_info.nbytes = gsl::narrow<uint32_t>(ewbitem->value.size());
            item_info.flags = 0;
            item_info.datatype = PROTOCOL_BINARY_DATATYPE_XATTR;
            item_info.key = {ewbitem->key, DocKeyEncodesCollectionId::No};
            item_info.value[0].iov_base = const_cast<void*>(
                    static_cast<const void*>(ewbitem->value.data()));
            item_info.value[0].iov_len = item_info.nbytes;
            return true;
        } else {
            return real_engine->get_item_info(item, item_info);
        }
    }

    cb::engine_errc set_collection_manifest(const CookieIface& cookie,
                                            std::string_view json) override;
    cb::engine_errc get_collection_manifest(
            const CookieIface& cookie, const AddResponseFn& response) override;
    cb::EngineErrorGetCollectionIDResult get_collection_id(
            const CookieIface& cookie, std::string_view path) override;
    cb::EngineErrorGetScopeIDResult get_scope_id(
            const CookieIface& cookie, std::string_view path) override;
    cb::EngineErrorGetScopeIDResult get_scope_id(
            const CookieIface& cookie,
            CollectionID cid,
            std::optional<Vbid> vbid) const override;

    cb::engine::FeatureSet getFeatures() override {
        return real_engine->getFeatures();
    }

    bool isXattrEnabled() override {
        return real_engine->isXattrEnabled();
    }

    cb::HlcTime getVBucketHlcNow(Vbid vbucket) override {
        return real_engine->getVBucketHlcNow(vbucket);
    }

    BucketCompressionMode getCompressionMode() override {
        return real_engine->getCompressionMode();
    }

    size_t getMaxItemSize() override {
        return real_engine->getMaxItemSize();
    }

    float getMinCompressionRatio() override {
        return real_engine->getMinCompressionRatio();
    }

    cb::engine_errc setParameter(const CookieIface& cookie,
                                 EngineParamCategory category,
                                 std::string_view key,
                                 std::string_view value,
                                 Vbid vbucket) override {
        return real_engine->setParameter(cookie, category, key, value, vbucket);
    }

    cb::engine_errc compactDatabase(const CookieIface& cookie,
                                    Vbid vbid,
                                    uint64_t purge_before_ts,
                                    uint64_t purge_before_seq,
                                    bool drop_deletes) override {
        return real_engine->compactDatabase(
                cookie, vbid, purge_before_ts, purge_before_seq, drop_deletes);
    }

    std::pair<cb::engine_errc, vbucket_state_t> getVBucket(
            const CookieIface& cookie, Vbid vbid) override {
        return real_engine->getVBucket(cookie, vbid);
    }

    cb::engine_errc setVBucket(const CookieIface& cookie,
                               Vbid vbid,
                               uint64_t cas,
                               vbucket_state_t state,
                               nlohmann::json* meta) override {
        return real_engine->setVBucket(cookie, vbid, cas, state, meta);
    }

    cb::engine_errc deleteVBucket(const CookieIface& cookie,
                                  Vbid vbid,
                                  bool sync) override {
        return real_engine->deleteVBucket(cookie, vbid, sync);
    }

    ///////////////////////////////////////////////////////////////////////////
    //             All of the methods used in the DCP interface              //
    //                                                                       //
    // We don't support mocking with the DCP interface yet, so all access to //
    // the DCP interface will be proxied down to the underlying engine.      //
    ///////////////////////////////////////////////////////////////////////////
    cb::engine_errc step(const CookieIface& cookie,
                         DcpMessageProducersIface& producers) override;

    cb::engine_errc open(const CookieIface& cookie,
                         uint32_t opaque,
                         uint32_t seqno,
                         uint32_t flags,
                         std::string_view name,
                         std::string_view value) override;

    cb::engine_errc add_stream(const CookieIface& cookie,
                               uint32_t opaque,
                               Vbid vbucket,
                               uint32_t flags) override;

    cb::engine_errc close_stream(const CookieIface& cookie,
                                 uint32_t opaque,
                                 Vbid vbucket,
                                 cb::mcbp::DcpStreamId sid) override;

    cb::engine_errc stream_req(const CookieIface& cookie,
                               uint32_t flags,
                               uint32_t opaque,
                               Vbid vbucket,
                               uint64_t start_seqno,
                               uint64_t end_seqno,
                               uint64_t vbucket_uuid,
                               uint64_t snap_start_seqno,
                               uint64_t snap_end_seqno,
                               uint64_t* rollback_seqno,
                               dcp_add_failover_log callback,
                               std::optional<std::string_view> json) override;

    cb::engine_errc get_failover_log(const CookieIface& cookie,
                                     uint32_t opaque,
                                     Vbid vbucket,
                                     dcp_add_failover_log callback) override;

    cb::engine_errc stream_end(const CookieIface& cookie,
                               uint32_t opaque,
                               Vbid vbucket,
                               cb::mcbp::DcpStreamEndStatus status) override;

    cb::engine_errc snapshot_marker(
            const CookieIface& cookie,
            uint32_t opaque,
            Vbid vbucket,
            uint64_t start_seqno,
            uint64_t end_seqno,
            uint32_t flags,
            std::optional<uint64_t> high_completed_seqno,
            std::optional<uint64_t> max_visible_seqno) override;

    cb::engine_errc mutation(const CookieIface& cookie,
                             uint32_t opaque,
                             const DocKey& key,
                             cb::const_byte_buffer value,
                             size_t priv_bytes,
                             uint8_t datatype,
                             uint64_t cas,
                             Vbid vbucket,
                             uint32_t flags,
                             uint64_t by_seqno,
                             uint64_t rev_seqno,
                             uint32_t expiration,
                             uint32_t lock_time,
                             cb::const_byte_buffer meta,
                             uint8_t nru) override;

    cb::engine_errc deletion(const CookieIface& cookie,
                             uint32_t opaque,
                             const DocKey& key,
                             cb::const_byte_buffer value,
                             size_t priv_bytes,
                             uint8_t datatype,
                             uint64_t cas,
                             Vbid vbucket,
                             uint64_t by_seqno,
                             uint64_t rev_seqno,
                             cb::const_byte_buffer meta) override;

    cb::engine_errc deletion_v2(const CookieIface& cookie,
                                uint32_t opaque,
                                const DocKey& key,
                                cb::const_byte_buffer value,
                                size_t priv_bytes,
                                uint8_t datatype,
                                uint64_t cas,
                                Vbid vbucket,
                                uint64_t by_seqno,
                                uint64_t rev_seqno,
                                uint32_t delete_time) override;

    cb::engine_errc expiration(const CookieIface& cookie,
                               uint32_t opaque,
                               const DocKey& key,
                               cb::const_byte_buffer value,
                               size_t priv_bytes,
                               uint8_t datatype,
                               uint64_t cas,
                               Vbid vbucket,
                               uint64_t by_seqno,
                               uint64_t rev_seqno,
                               uint32_t deleteTime) override;

    cb::engine_errc set_vbucket_state(const CookieIface& cookie,
                                      uint32_t opaque,
                                      Vbid vbucket,
                                      vbucket_state_t state) override;

    cb::engine_errc noop(const CookieIface& cookie, uint32_t opaque) override;

    cb::engine_errc buffer_acknowledgement(const CookieIface& cookie,
                                           uint32_t opaque,
                                           Vbid vbucket,
                                           uint32_t buffer_bytes) override;

    cb::engine_errc control(const CookieIface& cookie,
                            uint32_t opaque,
                            std::string_view key,
                            std::string_view value) override;

    cb::engine_errc response_handler(
            const CookieIface& cookie,
            const cb::mcbp::Response& response) override;

    cb::engine_errc system_event(const CookieIface& cookie,
                                 uint32_t opaque,
                                 Vbid vbucket,
                                 mcbp::systemevent::id event,
                                 uint64_t bySeqno,
                                 mcbp::systemevent::version version,
                                 cb::const_byte_buffer key,
                                 cb::const_byte_buffer eventData) override;

    cb::engine_errc prepare(const CookieIface& cookie,
                            uint32_t opaque,
                            const DocKey& key,
                            cb::const_byte_buffer value,
                            size_t priv_bytes,
                            uint8_t datatype,
                            uint64_t cas,
                            Vbid vbucket,
                            uint32_t flags,
                            uint64_t by_seqno,
                            uint64_t rev_seqno,
                            uint32_t expiration,
                            uint32_t lock_time,
                            uint8_t nru,
                            DocumentState document_state,
                            cb::durability::Level level) override;
    cb::engine_errc seqno_acknowledged(const CookieIface& cookie,
                                       uint32_t opaque,
                                       Vbid vbucket,
                                       uint64_t prepared_seqno) override;
    cb::engine_errc commit(const CookieIface& cookie,
                           uint32_t opaque,
                           Vbid vbucket,
                           const DocKey& key,
                           uint64_t prepared_seqno,
                           uint64_t commit_seqno) override;
    cb::engine_errc abort(const CookieIface& cookie,
                          uint32_t opaque,
                          Vbid vbucket,
                          const DocKey& key,
                          uint64_t prepared_seqno,
                          uint64_t abort_seqno) override;

    GET_SERVER_API gsa;
    ServerApi* real_api;

    // Actual engine we are proxying requests to.
    unique_engine_ptr real_engine;

    // Pointer to DcpIface for the underlying engine we are proxying; or
    // nullptr if it doesn't implement DcpIface;
    DcpIface* real_engine_dcp = nullptr;

    std::atomic_int clustermap_revno{1};

    /**
     * The method responsible for pushing all of the notify_io_complete
     * to the frontend. It is run by notify_io_thread and not intended to
     * be called by anyone else!.
     */
    void process_notifications();
    std::unique_ptr<Couchbase::Thread> notify_io_thread;

protected:
    /**
     * Handle the control message for block monitor file
     *
     * @param cookie The cookie executing the operation
     * @param id The identifier used to represent the cookie
     * @param file The file to monitor
     * @param response callback used to send a response to the client
     * @return The standard engine error codes
     */
    cb::engine_errc handleBlockMonitorFile(const CookieIface* cookie,
                                           uint32_t id,
                                           const std::string& file,
                                           const AddResponseFn& response);

    /**
     * Handle the control message for suspend
     *
     * @param cookie The cookie executing the operation
     * @param id The identifier used to represent the cookie to resume
     *           (the use of a different id is to allow resume to
     *           be sent on a different connection)
     * @param response callback used to send a response to the client
     * @return The standard engine error codes
     */
    cb::engine_errc handleSuspend(const CookieIface* cookie,
                                  uint32_t id,
                                  const AddResponseFn& response);

    /**
     * Handle the control message for resume
     *
     * @param cookie The cookie executing the operation
     * @param id The identifier representing the connection to resume
     * @param response callback used to send a response to the client
     * @return The standard engine error codes
     */
    cb::engine_errc handleResume(const CookieIface* cookie,
                                 uint32_t id,
                                 const AddResponseFn& response);

    /**
     * @param cookie the cookie executing the operation
     * @param key ID of the item whose CAS should be changed
     * @param cas The new CAS
     * @param response Response callback used to send a response to the client
     * @return Standard engine error codes
     */
    cb::engine_errc setItemCas(const CookieIface* cookie,
                               const std::string& key,
                               uint32_t cas,
                               const AddResponseFn& response);

    cb::engine_errc checkLogLevels(const CookieIface* cookie,
                                   uint32_t value,
                                   const AddResponseFn& response);

private:
    // Shared state between the main thread of execution and the background
    // thread processing pending io ops.
    std::mutex mutex;
    std::condition_variable condvar;
    struct PendingIO {
        const CookieIface* cookie;
        cb::engine_errc status;
    };
    std::queue<PendingIO> pending_io_ops;

    std::atomic<bool> stop_notification_thread{false};

    // Base class for all fault injection modes.
    struct FaultInjectMode {
        virtual ~FaultInjectMode() = default;

        explicit FaultInjectMode(cb::engine_errc injected_error_)
            : injected_error(injected_error_) {
        }

        // In the event of injecting an EWOULDBLOCK error, should the connection
        // be added to the pending_io_ops (and subsequently notified)?
        // @returns empty if shouldn't be added, otherwise contains the
        // status code to notify with.
        virtual std::optional<cb::engine_errc> add_to_pending_io_ops() {
            return cb::engine_errc::success;
        }

        virtual bool should_inject_error(Cmd cmd, cb::engine_errc& err) = 0;

        virtual std::string to_string() const = 0;

    protected:
        cb::engine_errc injected_error;
    };

    // Subclasses for each fault inject mode: /////////////////////////////////

    class ErrOnFirst : public FaultInjectMode {
    public:
        explicit ErrOnFirst(cb::engine_errc injected_error_)
            : FaultInjectMode(injected_error_), prev_cmd(Cmd::NONE) {
        }

        bool should_inject_error(Cmd cmd, cb::engine_errc& err) override {
            // Block unless the previous command from this cookie
            // was the same - i.e. all of a connections' commands
            // will EWOULDBLOCK the first time they are called.
            bool inject = (prev_cmd != cmd);
            prev_cmd = cmd;
            if (inject) {
                err = injected_error;
            }
            return inject;
        }

        std::string to_string() const override {
            return "ErrOnFirst inject_error=" + cb::to_string(injected_error);
        }

    private:
        // Last command issued by this cookie.
        Cmd prev_cmd;
    };

    class ErrOnNextN : public FaultInjectMode {
    public:
        ErrOnNextN(cb::engine_errc injected_error_, uint32_t count_)
            : FaultInjectMode(injected_error_), count(count_) {
        }

        bool should_inject_error(Cmd cmd, cb::engine_errc& err) override {
            if (count > 0) {
                --count;
                err = injected_error;
                return true;
            } else {
                return false;
            }
        }

        std::string to_string() const override {
            return std::string("ErrOnNextN") +
                   " inject_error=" + cb::to_string(injected_error) +
                   " count=" + std::to_string(count);
        }

    private:
        // The count of commands issued that should return error.
        uint32_t count;
    };

    class ErrRandom : public FaultInjectMode {
    public:
        ErrRandom(cb::engine_errc injected_error_, uint32_t percentage_)
            : FaultInjectMode(injected_error_), percentage_to_err(percentage_) {
        }

        bool should_inject_error(Cmd cmd, cb::engine_errc& err) override {
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<uint32_t> dis(1, 100);
            if (dis(gen) < percentage_to_err) {
                err = injected_error;
                return true;
            } else {
                return false;
            }
        }

        std::string to_string() const override {
            return std::string("ErrRandom") +
                   " inject_error=" + cb::to_string(injected_error) +
                   " percentage=" + std::to_string(percentage_to_err);
        }

    private:
        // Percentage chance that the specified error should be injected.
        uint32_t percentage_to_err;
    };

    /**
     * Injects a sequence of error codes for each call to should_inject_error().
     * If the end of the given sequence is reached, then throws
     * std::logic_error.
     *
     * cb::mcbp::Status::ReservedUserStart can be used to specify that the
     * no error is injected (the original status code is returned unchanged).
     */
    class ErrSequence : public FaultInjectMode {
    public:
        /**
         * Construct with a sequence of the specified error, or the 'normal'
         * status code.
         */
        ErrSequence(cb::engine_errc injected_error_, uint32_t sequence_)
            : FaultInjectMode(injected_error_) {
            for (int ii = 0; ii < 32; ii++) {
                if ((sequence_ & (1 << ii)) != 0) {
                    sequence.push_back(cb::engine_errc(injected_error_));
                } else {
                    sequence.push_back(cb::engine_errc(-1));
                }
            }
            pos = sequence.begin();
        }

        /**
         * Construct with a specific sequence of (potentially different) status
         * codes encoded as vector of cb::engine_errc elements in the
         * request value.
         */
        explicit ErrSequence(std::vector<cb::engine_errc> sequence_)
            : FaultInjectMode(cb::engine_errc::success),
              sequence(std::move(sequence_)),
              pos(sequence.begin()) {
        }

        bool should_inject_error(Cmd cmd, cb::engine_errc& err) override {
            if (pos == sequence.end()) {
                throw std::logic_error(
                        "ErrSequence::should_inject_error() Reached end of "
                        "sequence");
            }
            bool inject = false;
            if (*pos != cb::engine_errc(-1)) {
                inject = true;
                err = cb::engine_errc(*pos);
            }
            pos++;
            return inject;
        }

        std::optional<cb::engine_errc> add_to_pending_io_ops() override {
            // If this function has been called, should_inject_error() must
            // have returned true. Return the next status code in the sequnce
            // as the result of the pending IO.
            if (pos == sequence.end()) {
                throw std::logic_error(
                        "ErrSequence::add_to_pending_io_ops() Reached end of "
                        "sequence");
            }

            return cb::engine_errc(*pos++);
        }

        std::string to_string() const override {
            std::stringstream ss;
            ss << "ErrSequence sequence=[";
            for (const auto& err : sequence) {
                if (err == cb::engine_errc(-1)) {
                    ss << "'<passthrough>',";
                } else {
                    ss << "'" << err << "',";
                }
            }
            ss << "] pos=" << pos - sequence.begin();
            return ss.str();
        }

    private:
        std::vector<cb::engine_errc> sequence;
        std::vector<cb::engine_errc>::const_iterator pos;
    };

    class ErrOnNoNotify : public FaultInjectMode {
        public:
            explicit ErrOnNoNotify(cb::engine_errc injected_error_)
                : FaultInjectMode(injected_error_), issued_return_error(false) {
            }

            std::optional<cb::engine_errc> add_to_pending_io_ops() override {
                return {};
            }

            bool should_inject_error(Cmd cmd, cb::engine_errc& err) override {
                if (!issued_return_error) {
                    issued_return_error = true;
                    err = injected_error;
                    return true;
                } else {
                    return false;
                }
            }

            std::string to_string() const override {
                return std::string("ErrOnNoNotify") +
                       " inject_error=" + cb::to_string(injected_error) +
                       " issued_return_error=" +
                       std::to_string(issued_return_error);
            }

        private:
            // Record of whether have yet issued return error.
            bool issued_return_error;
        };

    class CASMismatch : public FaultInjectMode {
    public:
        explicit CASMismatch(uint32_t count_)
            : FaultInjectMode(cb::engine_errc::key_already_exists),
              count(count_) {
        }

        bool should_inject_error(Cmd cmd, cb::engine_errc& err) override {
            if (cmd == Cmd::CAS && (count > 0)) {
                --count;
                err = injected_error;
                return true;
            } else {
                return false;
            }
        }

        std::string to_string() const override {
            return std::string("CASMismatch") +
                   " count=" + std::to_string(count);
        }

    private:
        uint32_t count;
    };

    // Map of connections (aka cookies) to their current mode.
    std::map<uint64_t,
             std::pair<const CookieIface*, std::shared_ptr<FaultInjectMode>>>
            connection_map;
    // Mutex for above map.
    std::mutex cookie_map_mutex;

    /**
     * The dcp_stream map is used to map a cookie to the count of objects
     * it should send on the stream.
     *
     * Each entry in here constists of a pair containing a boolean specifying
     * if the stream is opened or not, and a count of how many times we should
     * return data
     */
    std::map<const CookieIface*, std::pair<bool, uint64_t>> dcp_stream;

    friend class BlockMonitorThread;
    std::map<uint32_t, const CookieIface*> suspended_map;
    std::mutex suspended_map_mutex;

    bool suspend(const CookieIface* cookie, uint32_t id) {
        {
            std::lock_guard<std::mutex> guard(suspended_map_mutex);
            auto iter = suspended_map.find(id);
            if (iter == suspended_map.cend()) {
                suspended_map[id] = cookie;
                return true;
            }
        }

        return false;
    }

    bool resume(uint32_t id) {
        const CookieIface* cookie = nullptr;
        {
            std::lock_guard<std::mutex> guard(suspended_map_mutex);
            auto iter = suspended_map.find(id);
            if (iter == suspended_map.cend()) {
                return false;
            }
            cookie = iter->second;
            suspended_map.erase(iter);
        }

        schedule_notification(cookie, cb::engine_errc::success);
        return true;
    }

    bool is_connection_suspended(const CookieIface* cookie) {
        std::lock_guard<std::mutex> guard(suspended_map_mutex);
        for (const auto& c : suspended_map) {
            if (c.second == cookie) {
                LOG_DEBUG(
                        "Connection {} with id {} should be suspended for "
                        "engine {}",
                        static_cast<const void*>(c.second),
                        c.first,
                        (void*)this);

                return true;
            }
        }
        return false;
    }

    void schedule_notification(const CookieIface* cookie,
                               cb::engine_errc status) {
        {
            std::lock_guard<std::mutex> guard(mutex);
            pending_io_ops.push({cookie, status});
        }
        LOG_DEBUG("EWB_Engine: connection {} should be resumed for engine {}",
                  (void*)cookie,
                  (void*)this);

        condvar.notify_one();
    }

    // Vector to keep track of the threads we've started to ensure
    // we don't leak memory ;-)
    std::mutex threads_mutex;
    std::vector<std::unique_ptr<Couchbase::Thread> > threads;
};

EWB_Engine::EWB_Engine(GET_SERVER_API gsa_)
    : gsa(gsa_),
      real_api(gsa()),
      notify_io_thread(new NotificationThread(*this)) {
    notify_io_thread->start();
}

EWB_Engine::~EWB_Engine() {
    threads.clear();
    stop_notification_thread = true;
    condvar.notify_all();
    notify_io_thread->waitForState(Couchbase::ThreadState::Zombie);
}

cb::engine_errc EWB_Engine::step(const CookieIface& cookie,
                                 DcpMessageProducersIface& producers) {
    auto stream = dcp_stream.find(&cookie);
    if (stream != dcp_stream.end()) {
        auto& count = stream->second.second;
        // If the stream is enabled and we have data to send..
        if (stream->second.first && count > 0) {
            // This is using the internal dcp implementation which always
            // send the same item back
            auto ret = producers.mutation(
                    0xdeadbeef /*opqaue*/,
                    cb::unique_item_ptr(new EwbDcpMutationItem,
                                        cb::ItemDeleter(this)),
                    Vbid(0),
                    0 /*by_seqno*/,
                    0 /*rev_seqno*/,
                    0 /*lock_time*/,
                    0 /*nru*/,
                    {});
            --count;
            return ret;
        }
        return cb::engine_errc::would_block;
    }
    if (!real_engine_dcp) {
        return cb::engine_errc::not_supported;
    }
    return real_engine_dcp->step(cookie, producers);
}

cb::engine_errc EWB_Engine::open(const CookieIface& cookie,
                                 uint32_t opaque,
                                 uint32_t seqno,
                                 uint32_t flags,
                                 std::string_view name,
                                 std::string_view value) {
    std::string nm{name};
    if (nm.find("ewb_internal") == 0) {
        // Yeah, this is a request for the internal "magic" DCP stream
        // The user could specify the iteration count by adding a colon
        // at the end...
        auto idx = nm.rfind(":");

        if (idx != nm.npos) {
            dcp_stream[&cookie] =
                    std::make_pair(false, std::stoull(nm.substr(idx + 1)));
        } else {
            dcp_stream[&cookie] =
                    std::make_pair(false, std::numeric_limits<uint64_t>::max());
        }

        real_api->cookie->setDcpConnHandler(cookie, this);
        return cb::engine_errc::success;
    }

    if (!real_engine_dcp) {
        return cb::engine_errc::not_supported;
    } else {
        return real_engine_dcp->open(cookie, opaque, seqno, flags, name, value);
    }
}

cb::engine_errc EWB_Engine::stream_req(const CookieIface& cookie,
                                       uint32_t flags,
                                       uint32_t opaque,
                                       Vbid vbucket,
                                       uint64_t start_seqno,
                                       uint64_t end_seqno,
                                       uint64_t vbucket_uuid,
                                       uint64_t snap_start_seqno,
                                       uint64_t snap_end_seqno,
                                       uint64_t* rollback_seqno,
                                       dcp_add_failover_log callback,
                                       std::optional<std::string_view> json) {
    auto stream = dcp_stream.find(&cookie);
    if (stream != dcp_stream.end()) {
        // This is a client of our internal streams.. just let it pass
        if (start_seqno == 1) {
            *rollback_seqno = 0;
            return cb::engine_errc::rollback;
        }
        // Start the stream
        stream->second.first = true;
        return cb::engine_errc::success;
    }

    if (!real_engine_dcp) {
        return cb::engine_errc::not_supported;
    } else {
        return real_engine_dcp->stream_req(cookie,
                                           flags,
                                           opaque,
                                           vbucket,
                                           start_seqno,
                                           end_seqno,
                                           vbucket_uuid,
                                           snap_start_seqno,
                                           snap_end_seqno,
                                           rollback_seqno,
                                           callback,
                                           json);
    }
}

cb::engine_errc EWB_Engine::add_stream(const CookieIface& cookie,
                                       uint32_t opaque,
                                       Vbid vbucket,
                                       uint32_t flags) {
    if (!real_engine_dcp) {
        return cb::engine_errc::not_supported;
    } else {
        return real_engine_dcp->add_stream(cookie, opaque, vbucket, flags);
    }
}

cb::engine_errc EWB_Engine::close_stream(const CookieIface& cookie,
                                         uint32_t opaque,
                                         Vbid vbucket,
                                         cb::mcbp::DcpStreamId sid) {
    if (!real_engine_dcp) {
        return cb::engine_errc::not_supported;
    } else {
        return real_engine_dcp->close_stream(cookie, opaque, vbucket, sid);
    }
}

cb::engine_errc EWB_Engine::get_failover_log(const CookieIface& cookie,
                                             uint32_t opaque,
                                             Vbid vbucket,
                                             dcp_add_failover_log callback) {
    if (!real_engine_dcp) {
        return cb::engine_errc::not_supported;
    } else {
        return real_engine_dcp->get_failover_log(
                cookie, opaque, vbucket, callback);
    }
}

cb::engine_errc EWB_Engine::stream_end(const CookieIface& cookie,
                                       uint32_t opaque,
                                       Vbid vbucket,
                                       cb::mcbp::DcpStreamEndStatus status) {
    if (!real_engine_dcp) {
        return cb::engine_errc::not_supported;
    } else {
        return real_engine_dcp->stream_end(cookie, opaque, vbucket, status);
    }
}

cb::engine_errc EWB_Engine::snapshot_marker(
        const CookieIface& cookie,
        uint32_t opaque,
        Vbid vbucket,
        uint64_t start_seqno,
        uint64_t end_seqno,
        uint32_t flags,
        std::optional<uint64_t> high_completed_seqno,
        std::optional<uint64_t> max_visible_seqno) {
    if (!real_engine_dcp) {
        return cb::engine_errc::not_supported;
    } else {
        return real_engine_dcp->snapshot_marker(cookie,
                                                opaque,
                                                vbucket,
                                                start_seqno,
                                                end_seqno,
                                                flags,
                                                high_completed_seqno,
                                                max_visible_seqno);
    }
}

cb::engine_errc EWB_Engine::mutation(const CookieIface& cookie,
                                     uint32_t opaque,
                                     const DocKey& key,
                                     cb::const_byte_buffer value,
                                     size_t priv_bytes,
                                     uint8_t datatype,
                                     uint64_t cas,
                                     Vbid vbucket,
                                     uint32_t flags,
                                     uint64_t by_seqno,
                                     uint64_t rev_seqno,
                                     uint32_t expiration,
                                     uint32_t lock_time,
                                     cb::const_byte_buffer meta,
                                     uint8_t nru) {
    if (!real_engine_dcp) {
        return cb::engine_errc::not_supported;
    } else {
        return real_engine_dcp->mutation(cookie,
                                         opaque,
                                         key,
                                         value,
                                         priv_bytes,
                                         datatype,
                                         cas,
                                         vbucket,
                                         flags,
                                         by_seqno,
                                         rev_seqno,
                                         expiration,
                                         lock_time,
                                         meta,
                                         nru);
    }
}

cb::engine_errc EWB_Engine::deletion(const CookieIface& cookie,
                                     uint32_t opaque,
                                     const DocKey& key,
                                     cb::const_byte_buffer value,
                                     size_t priv_bytes,
                                     uint8_t datatype,
                                     uint64_t cas,
                                     Vbid vbucket,
                                     uint64_t by_seqno,
                                     uint64_t rev_seqno,
                                     cb::const_byte_buffer meta) {
    if (!real_engine_dcp) {
        return cb::engine_errc::not_supported;
    } else {
        return real_engine_dcp->deletion(cookie,
                                         opaque,
                                         key,
                                         value,
                                         priv_bytes,
                                         datatype,
                                         cas,
                                         vbucket,
                                         by_seqno,
                                         rev_seqno,
                                         meta);
    }
}

cb::engine_errc EWB_Engine::deletion_v2(const CookieIface& cookie,
                                        uint32_t opaque,
                                        const DocKey& key,
                                        cb::const_byte_buffer value,
                                        size_t priv_bytes,
                                        uint8_t datatype,
                                        uint64_t cas,
                                        Vbid vbucket,
                                        uint64_t by_seqno,
                                        uint64_t rev_seqno,
                                        uint32_t delete_time) {
    if (!real_engine_dcp) {
        return cb::engine_errc::not_supported;
    } else {
        return real_engine_dcp->deletion_v2(cookie,
                                            opaque,
                                            key,
                                            value,
                                            priv_bytes,
                                            datatype,
                                            cas,
                                            vbucket,
                                            by_seqno,
                                            rev_seqno,
                                            delete_time);
    }
}

cb::engine_errc EWB_Engine::expiration(const CookieIface& cookie,
                                       uint32_t opaque,
                                       const DocKey& key,
                                       cb::const_byte_buffer value,
                                       size_t priv_bytes,
                                       uint8_t datatype,
                                       uint64_t cas,
                                       Vbid vbucket,
                                       uint64_t by_seqno,
                                       uint64_t rev_seqno,
                                       uint32_t deleteTime) {
    if (!real_engine_dcp) {
        return cb::engine_errc::not_supported;
    } else {
        return real_engine_dcp->expiration(cookie,
                                           opaque,
                                           key,
                                           value,
                                           priv_bytes,
                                           datatype,
                                           cas,
                                           vbucket,
                                           by_seqno,
                                           rev_seqno,
                                           deleteTime);
    }
}

cb::engine_errc EWB_Engine::set_vbucket_state(const CookieIface& cookie,
                                              uint32_t opaque,
                                              Vbid vbucket,
                                              vbucket_state_t state) {
    if (!real_engine_dcp) {
        return cb::engine_errc::not_supported;
    } else {
        return real_engine_dcp->set_vbucket_state(
                cookie, opaque, vbucket, state);
    }
}

cb::engine_errc EWB_Engine::noop(const CookieIface& cookie, uint32_t opaque) {
    if (!real_engine_dcp) {
        return cb::engine_errc::not_supported;
    } else {
        return real_engine_dcp->noop(cookie, opaque);
    }
}

cb::engine_errc EWB_Engine::buffer_acknowledgement(const CookieIface& cookie,
                                                   uint32_t opaque,
                                                   Vbid vbucket,
                                                   uint32_t buffer_bytes) {
    if (!real_engine_dcp) {
        return cb::engine_errc::not_supported;
    } else {
        return real_engine_dcp->buffer_acknowledgement(
                cookie, opaque, vbucket, buffer_bytes);
    }
}

cb::engine_errc EWB_Engine::control(const CookieIface& cookie,
                                    uint32_t opaque,
                                    std::string_view key,
                                    std::string_view value) {
    if (!real_engine_dcp) {
        return cb::engine_errc::not_supported;
    } else {
        return real_engine_dcp->control(cookie, opaque, key, value);
    }
}

cb::engine_errc EWB_Engine::response_handler(
        const CookieIface& cookie, const cb::mcbp::Response& response) {
    if (!real_engine_dcp) {
        return cb::engine_errc::not_supported;
    } else {
        return real_engine_dcp->response_handler(cookie, response);
    }
}

cb::engine_errc EWB_Engine::system_event(const CookieIface& cookie,
                                         uint32_t opaque,
                                         Vbid vbucket,
                                         mcbp::systemevent::id event,
                                         uint64_t bySeqno,
                                         mcbp::systemevent::version version,
                                         cb::const_byte_buffer key,
                                         cb::const_byte_buffer eventData) {
    if (!real_engine_dcp) {
        return cb::engine_errc::not_supported;
    } else {
        return real_engine_dcp->system_event(cookie,
                                             opaque,
                                             vbucket,
                                             event,
                                             bySeqno,
                                             version,
                                             key,
                                             eventData);
    }
}

cb::engine_errc EWB_Engine::prepare(const CookieIface& cookie,
                                    uint32_t opaque,
                                    const DocKey& key,
                                    cb::const_byte_buffer value,
                                    size_t priv_bytes,
                                    uint8_t datatype,
                                    uint64_t cas,
                                    Vbid vbucket,
                                    uint32_t flags,
                                    uint64_t by_seqno,
                                    uint64_t rev_seqno,
                                    uint32_t expiration,
                                    uint32_t lock_time,
                                    uint8_t nru,
                                    DocumentState document_state,
                                    cb::durability::Level level) {
    if (!real_engine_dcp) {
        return cb::engine_errc::not_supported;
    } else {
        return real_engine_dcp->prepare(cookie,
                                        opaque,
                                        key,
                                        value,
                                        priv_bytes,
                                        datatype,
                                        cas,
                                        vbucket,
                                        flags,
                                        by_seqno,
                                        rev_seqno,
                                        expiration,
                                        lock_time,
                                        nru,
                                        document_state,
                                        level);
    }
}
cb::engine_errc EWB_Engine::seqno_acknowledged(const CookieIface& cookie,
                                               uint32_t opaque,
                                               Vbid vbucket,
                                               uint64_t prepared_seqno) {
    if (!real_engine_dcp) {
        return cb::engine_errc::not_supported;
    } else {
        return real_engine_dcp->seqno_acknowledged(
                cookie, opaque, vbucket, prepared_seqno);
    }
}
cb::engine_errc EWB_Engine::commit(const CookieIface& cookie,
                                   uint32_t opaque,
                                   Vbid vbucket,
                                   const DocKey& key,
                                   uint64_t prepared_seqno,
                                   uint64_t commit_seqno) {
    if (!real_engine_dcp) {
        return cb::engine_errc::not_supported;
    } else {
        return real_engine_dcp->commit(
                cookie, opaque, vbucket, key, prepared_seqno, commit_seqno);
    }
}

cb::engine_errc EWB_Engine::abort(const CookieIface& cookie,
                                  uint32_t opaque,
                                  Vbid vbucket,
                                  const DocKey& key,
                                  uint64_t prepared_seqno,
                                  uint64_t abort_seqno) {
    if (!real_engine_dcp) {
        return cb::engine_errc::not_supported;
    } else {
        return real_engine_dcp->abort(
                cookie, opaque, vbucket, key, prepared_seqno, abort_seqno);
    }
}

unique_engine_ptr create_ewouldblock_instance(GET_SERVER_API gsa) {
    return unique_engine_ptr{new EWB_Engine(gsa)};
}

const char* EWB_Engine::to_string(const Cmd cmd) {
    switch (cmd) {
    case Cmd::NONE:
        return "NONE";
    case Cmd::GET_INFO:
        return "GET_INFO";
    case Cmd::GET_META:
        return "GET_META";
    case Cmd::ALLOCATE:
        return "ALLOCATE";
    case Cmd::REMOVE:
        return "REMOVE";
    case Cmd::GET:
        return "GET";
    case Cmd::STORE:
        return "STORE";
    case Cmd::CAS:
        return "CAS";
    case Cmd::ARITHMETIC:
        return "ARITHMETIC";
    case Cmd::FLUSH:
        return "FLUSH";
    case Cmd::GET_STATS:
        return "GET_STATS";
    case Cmd::UNKNOWN_COMMAND:
        return "UNKNOWN_COMMAND";
    case Cmd::LOCK:
        return "LOCK";
    case Cmd::UNLOCK:
        return "UNLOCK";
    }
    throw std::invalid_argument("EWB_Engine::to_string() Unknown command");
}

void EWB_Engine::process_notifications() {
    ServerApi* server = gsa();
    LOG_DEBUG("EWB_Engine: notification thread running for engine {}",
              (void*)this);
    std::unique_lock<std::mutex> lk(mutex);
    while (!stop_notification_thread) {
        condvar.wait(lk, [this] {
            return (!pending_io_ops.empty()) || stop_notification_thread;
        });
        while (!pending_io_ops.empty()) {
            const auto op = pending_io_ops.front();
            pending_io_ops.pop();
            lk.unlock();
            LOG_DEBUG("EWB_Engine: notify {} status:{}",
                      static_cast<const void*>(op.cookie),
                      op.status);
            server->cookie->notify_io_complete(*op.cookie, op.status);
            lk.lock();
        }
    }

    LOG_DEBUG("EWB_Engine: notification thread stopping for engine {}",
              (void*)this);
}

void NotificationThread::run() {
    setRunning();
    engine.process_notifications();
}

cb::engine_errc EWB_Engine::handleBlockMonitorFile(
        const CookieIface* cookie,
        uint32_t id,
        const std::string& file,
        const AddResponseFn& response) {
    if (file.empty()) {
        return cb::engine_errc::invalid_arguments;
    }

    if (!cb::io::isFile(file)) {
        return cb::engine_errc::no_such_key;
    }

    if (!suspend(cookie, id)) {
        LOG_WARNING(
                "EWB_Engine::handleBlockMonitorFile(): "
                "Id {} already registered",
                id);
        return cb::engine_errc::key_already_exists;
    }

    try {
        std::unique_ptr<Couchbase::Thread> thread(
                new BlockMonitorThread(*this, id, file));
        thread->start();
        std::lock_guard<std::mutex> guard(threads_mutex);
        threads.emplace_back(thread.release());
    } catch (std::exception& e) {
        LOG_WARNING(
                "EWB_Engine::handleBlockMonitorFile(): Failed to create "
                "block monitor thread: {}",
                e.what());
        return cb::engine_errc::failed;
    }

    LOG_DEBUG(
            "Registered connection {} (engine {}) as {} to be"
            " suspended. Monitor file {}",
            static_cast<const void*>(cookie),
            (void*)this,
            id,
            file.c_str());

    response({},
             {},
             {},
             PROTOCOL_BINARY_RAW_BYTES,
             cb::mcbp::Status::Success,
             /*cas*/ 0,
             cookie);
    return cb::engine_errc::success;
}

cb::engine_errc EWB_Engine::handleSuspend(const CookieIface* cookie,
                                          uint32_t id,
                                          const AddResponseFn& response) {
    if (suspend(cookie, id)) {
        LOG_DEBUG("Registered connection {} as {} to be suspended",
                  static_cast<const void*>(cookie),
                  id);
        response({},
                 {},
                 {},
                 PROTOCOL_BINARY_RAW_BYTES,
                 cb::mcbp::Status::Success,
                 /*cas*/ 0,
                 cookie);
        return cb::engine_errc::success;
    } else {
        LOG_WARNING("EWB_Engine::handleSuspend(): Id {} already registered",
                    id);
        return cb::engine_errc::key_already_exists;
    }
}

cb::engine_errc EWB_Engine::handleResume(const CookieIface* cookie,
                                         uint32_t id,
                                         const AddResponseFn& response) {
    if (resume(id)) {
        LOG_DEBUG("Connection with id {} will be resumed", id);
        response({},
                 {},
                 {},
                 PROTOCOL_BINARY_RAW_BYTES,
                 cb::mcbp::Status::Success,
                 /*cas*/ 0,
                 cookie);
        return cb::engine_errc::success;
    } else {
        LOG_WARNING(
                "EWB_Engine::unknown_command(): No "
                "connection registered with id {}",
                id);
        return cb::engine_errc::invalid_arguments;
    }
}

cb::engine_errc EWB_Engine::setItemCas(const CookieIface* cookie,
                                       const std::string& key,
                                       uint32_t cas,
                                       const AddResponseFn& response) {
    uint64_t cas64 = cas;
    if (cas == static_cast<uint32_t>(-1)) {
        cas64 = LOCKED_CAS;
    }

    auto rv = real_engine->get(*cookie,
                               DocKey{key, DocKeyEncodesCollectionId::No},
                               Vbid(0),
                               DocStateFilter::Alive);
    if (rv.first != cb::engine_errc::success) {
        return cb::engine_errc(rv.first);
    }

    // item_set_cas has no return value!
    real_engine->item_set_cas(*rv.second.get(), cas64);
    response({},
             {},
             {},
             PROTOCOL_BINARY_RAW_BYTES,
             cb::mcbp::Status::Success,
             0,
             cookie);
    return cb::engine_errc::success;
}

cb::engine_errc EWB_Engine::checkLogLevels(const CookieIface* cookie,
                                           uint32_t value,
                                           const AddResponseFn& response) {
    auto level = spdlog::level::level_enum(value);
    auto rsp = cb::logger::checkLogLevels(level);

    response({},
             {},
             {},
             PROTOCOL_BINARY_RAW_BYTES,
             rsp ? cb::mcbp::Status::Success : cb::mcbp::Status::Einval,
             0,
             cookie);
    return cb::engine_errc::success;
}

cb::engine_errc EWB_Engine::set_collection_manifest(const CookieIface& cookie,
                                                    std::string_view json) {
    return real_engine->set_collection_manifest(cookie, json);
}

cb::engine_errc EWB_Engine::get_collection_manifest(
        const CookieIface& cookie, const AddResponseFn& response) {
    return real_engine->get_collection_manifest(cookie, response);
}

cb::EngineErrorGetCollectionIDResult EWB_Engine::get_collection_id(
        const CookieIface& cookie, std::string_view path) {
    return real_engine->get_collection_id(cookie, path);
}

cb::EngineErrorGetScopeIDResult EWB_Engine::get_scope_id(
        const CookieIface& cookie, std::string_view path) {
    return real_engine->get_scope_id(cookie, path);
}

void EWB_Engine::initiate_shutdown() {
    if (real_engine) {
        real_engine->initiate_shutdown();
    }
}

cb::EngineErrorGetScopeIDResult EWB_Engine::get_scope_id(
        const CookieIface& cookie,
        CollectionID cid,
        std::optional<Vbid> vbid) const {
    return real_engine->get_scope_id(cookie, cid, std::optional<Vbid>());
}

void BlockMonitorThread::run() {
    setRunning();

    LOG_DEBUG("Block monitor for file {} started", file);

    // @todo Use the file monitoring API's to avoid this "busy" loop
    while (cb::io::isFile(file)) {
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }

    LOG_DEBUG("Block monitor for file {} stopping (file is gone)", file);
    engine.resume(id);
}
