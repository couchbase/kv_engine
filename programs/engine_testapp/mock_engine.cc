/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "mock_engine.h"
#include "mock_cookie.h"
#include "mock_server.h"

#include <memcached/collections.h>
#include <memcached/durability_spec.h>
#include <memcached/range_scan_optional_configuration.h>
#include <functional>

/**
 * EWOULDBLOCK wrapper.
 * Will recall "engine_function" with EWOULDBLOCK retry logic.
 **/
template <typename T>
static std::pair<cb::engine_errc, T> do_blocking_engine_call(
        CookieIface& cookie,
        const std::function<std::pair<cb::engine_errc, T>()>& engine_function) {
    auto& c = asMockCookie(cookie);
    std::unique_lock<std::mutex> lock(c.getMutex());

    auto ret = engine_function();
    while (ret.first == cb::engine_errc::would_block && c.isEwouldblock()) {
        const auto status = mock_waitfor_cookie(&c);

        if (status == cb::engine_errc::success) {
            ret = engine_function();
        } else {
            return {status, T()};
        }
    }

    return ret;
}

/*
 * @todo: re-factoring of these handlers - can likely reduce the duplication
 * with some work around more functions to abstract how the return type is
 * composed and inspected (is it a pair, a bird a plane?)
 */
static cb::engine_errc call_engine_and_handle_EWOULDBLOCK(
        CookieIface& cookie,
        const std::function<cb::engine_errc()>& engine_function) {
    auto& c = asMockCookie(cookie);
    std::unique_lock<std::mutex> lock(c.getMutex());

    auto ret = engine_function();
    while (ret == cb::engine_errc::would_block && c.isEwouldblock()) {
        const auto status = mock_waitfor_cookie(&c);

        if (status == cb::engine_errc::success) {
            ret = engine_function();
        } else {
            return status;
        }
    }

    return ret;
}

cb::engine_errc MockEngine::initialize(
        std::string_view config_str,
        const nlohmann::json& encryption,
        std::string_view chronicleAuthToken,
        const nlohmann::json& collectionManifest) {
    return the_engine->initialize(
            config_str, encryption, chronicleAuthToken, collectionManifest);
}

cb::engine_errc MockEngine::set_traffic_control_mode(CookieIface& cookie,
                                                     TrafficControlMode mode) {
    auto engine_fn = [this, &cookie, mode]() {
        return the_engine->set_traffic_control_mode(cookie, mode);
    };
    return call_engine_and_handle_EWOULDBLOCK(cookie, engine_fn);
}

void MockEngine::initiate_shutdown() {
    the_engine->initiate_shutdown();
}

void MockEngine::destroy(const bool force) {
    // We've got an annoying binding from the cookies to
    // the engine. As part of shutting down the engine they
    // try to call disconnect. Make sure that we first call destroy
    // which eventually delete the object (after all connections are gone)
    // at this time we should release it from the unique_ptr.
    the_engine->destroy(force);
    (void)the_engine.release();
    the_engine_dcp = nullptr;
    delete this;
}

cb::unique_item_ptr MockEngine::allocateItem(CookieIface& cookie,
                                             const DocKeyView& key,
                                             size_t nbytes,
                                             size_t priv_nbytes,
                                             uint32_t flags,
                                             rel_time_t exptime,
                                             uint8_t datatype,
                                             Vbid vbucket) {
    std::unique_lock<std::mutex> lock(asMockCookie(cookie).getMutex());
    try {
        return the_engine->allocateItem(cookie,
                                        key,
                                        nbytes,
                                        priv_nbytes,
                                        flags,
                                        exptime,
                                        datatype,
                                        vbucket);
    } catch (const cb::engine_error& error) {
        if (error.code() == cb::engine_errc::would_block) {
            throw std::logic_error(
                    "mock_allocate_ex: allocateItem should not block!");
        }
        throw;
    }
}

cb::engine_errc MockEngine::remove(
        CookieIface& cookie,
        const DocKeyView& key,
        uint64_t& cas,
        Vbid vbucket,
        const std::optional<cb::durability::Requirements>& durability,
        mutation_descr_t& mut_info) {
    auto engine_fn = [this,
                      c = std::ref(cookie),
                      k = std::cref(key),
                      cas_ = std::ref(cas),
                      vbucket,
                      dur = std::cref(durability),
                      m = std::ref(mut_info)]() {
        return the_engine->remove(c, k, cas_, vbucket, dur, m);
    };
    return call_engine_and_handle_EWOULDBLOCK(cookie, engine_fn);
}

void MockEngine::release(ItemIface& item) {
    the_engine->release(item);
}

cb::EngineErrorItemPair MockEngine::get(CookieIface& cookie,
                                        const DocKeyView& key,
                                        Vbid vbucket,
                                        DocStateFilter documentStateFilter) {
    auto engine_fn = [this,
                      &cookie,
                      k = std::cref(key),
                      vbucket,
                      documentStateFilter]() {
        return the_engine->get(cookie, k, vbucket, documentStateFilter);
    };

    return do_blocking_engine_call<cb::unique_item_ptr>(cookie, engine_fn);
}

cb::EngineErrorItemPair MockEngine::get_replica(
        CookieIface& cookie,
        const DocKeyView& key,
        Vbid vbucket,
        DocStateFilter documentStateFilter) {
    auto engine_fn = [this,
                      &cookie,
                      k = std::cref(key),
                      vbucket,
                      documentStateFilter]() {
        return the_engine->get_replica(cookie, k, vbucket, documentStateFilter);
    };

    return do_blocking_engine_call<cb::unique_item_ptr>(cookie, engine_fn);
}

cb::EngineErrorItemPair MockEngine::get_random_document(CookieIface& cookie,
                                                        CollectionID cid) {
    auto engine_fn = [this, &cookie, cid]() {
        return the_engine->get_random_document(cookie, cid);
    };

    return do_blocking_engine_call<cb::unique_item_ptr>(cookie, engine_fn);
}

cb::EngineErrorItemPair MockEngine::get_if(
        CookieIface& cookie,
        const DocKeyView& key,
        Vbid vbucket,
        const std::function<bool(const item_info&)>& filter) {
    auto engine_fn = [this, &cookie, k = std::cref(key), vbucket, filter]() {
        return the_engine->get_if(cookie, k, vbucket, filter);
    };
    return do_blocking_engine_call<cb::unique_item_ptr>(cookie, engine_fn);
}

cb::EngineErrorItemPair MockEngine::get_and_touch(
        CookieIface& cookie,
        const DocKeyView& key,
        Vbid vbucket,
        uint32_t expiryTime,
        const std::optional<cb::durability::Requirements>& durability) {
    auto engine_fn = [this,
                      &cookie,
                      k = std::cref(key),
                      vbucket,
                      expiryTime,
                      dur = std::cref(durability)]() {
        return the_engine->get_and_touch(cookie, k, vbucket, expiryTime, dur);
    };

    return do_blocking_engine_call<cb::unique_item_ptr>(cookie, engine_fn);
}

cb::EngineErrorItemPair MockEngine::get_locked(
        CookieIface& cookie,
        const DocKeyView& key,
        Vbid vbucket,
        std::chrono::seconds lock_timeout) {
    auto engine_fn =
            [this, &cookie, k = std::cref(key), vbucket, lock_timeout]() {
                return the_engine->get_locked(cookie, k, vbucket, lock_timeout);
            };

    return do_blocking_engine_call<cb::unique_item_ptr>(cookie, engine_fn);
}

cb::EngineErrorMetadataPair MockEngine::get_meta(CookieIface& cookie,
                                                 const DocKeyView& key,
                                                 Vbid vbucket) {
    auto engine_fn = [this, &cookie, k = std::cref(key), vbucket]() {
        return the_engine->get_meta(cookie, k, vbucket);
    };

    return do_blocking_engine_call<item_info>(cookie, engine_fn);
}

cb::engine_errc MockEngine::unlock(CookieIface& cookie,
                                   const DocKeyView& key,
                                   Vbid vbucket,
                                   uint64_t cas) {
    auto engine_fn = [this, &cookie, k = std::cref(key), vbucket, cas]() {
        return the_engine->unlock(cookie, k, vbucket, cas);
    };

    return call_engine_and_handle_EWOULDBLOCK(cookie, engine_fn);
}

cb::engine_errc MockEngine::get_stats(CookieIface& cookie,
                                      std::string_view key,
                                      std::string_view value,
                                      const AddStatFn& add_stat,
                                      const CheckYieldFn& check_yield) {
    auto engine_fn = [this, &cookie, key, value, &add_stat, &check_yield]() {
        return the_engine->get_stats(cookie, key, value, add_stat, check_yield);
    };

    return call_engine_and_handle_EWOULDBLOCK(cookie, engine_fn);
}

cb::engine_errc MockEngine::store(
        CookieIface& cookie,
        ItemIface& item,
        uint64_t& cas,
        StoreSemantics operation,
        const std::optional<cb::durability::Requirements>& durability,
        DocumentState document_state,
        bool preserveTtl) {
    auto engine_fn = [this,
                      &cookie,
                      &item,
                      _cas = std::ref(cas),
                      operation,
                      dur = std::cref(durability),
                      document_state,
                      preserveTtl]() {
        return the_engine->store(cookie,
                                 item,
                                 _cas,
                                 operation,
                                 dur,
                                 document_state,
                                 preserveTtl);
    };

    return call_engine_and_handle_EWOULDBLOCK(cookie, engine_fn);
}

cb::EngineErrorCasPair MockEngine::store_if(
        CookieIface& cookie,
        ItemIface& item,
        uint64_t cas,
        StoreSemantics operation,
        const cb::StoreIfPredicate& predicate,
        const std::optional<cb::durability::Requirements>& durability,
        DocumentState document_state,
        bool preserveTtl) {
    auto engine_fn = [this,
                      &cookie,
                      &item,
                      cas,
                      operation,
                      pred = std::cref(predicate),
                      dur = std::cref(durability),
                      document_state,
                      preserveTtl]() {
        return the_engine->store_if(cookie,
                                    item,
                                    cas,
                                    operation,
                                    pred,
                                    dur,
                                    document_state,
                                    preserveTtl);
    };
    return do_blocking_engine_call<uint64_t>(cookie, engine_fn);
}

cb::engine_errc MockEngine::flush(CookieIface& cookie) {
    auto engine_fn = [this, &cookie]() { return the_engine->flush(cookie); };
    return call_engine_and_handle_EWOULDBLOCK(cookie, engine_fn);
}

void MockEngine::reset_stats(CookieIface& cookie) {
    the_engine->reset_stats(cookie);
}

cb::engine_errc MockEngine::unknown_command(CookieIface& cookie,
                                            const cb::mcbp::Request& request,
                                            const AddResponseFn& response) {
    auto engine_fn = [this,
                      c = std::ref(cookie),
                      req = std::cref(request),
                      res = std::cref(response)]() {
        return the_engine->unknown_command(c, req, res);
    };

    return call_engine_and_handle_EWOULDBLOCK(cookie, engine_fn);
}

bool MockEngine::get_item_info(const ItemIface& item, item_info& item_info) {
    return the_engine->get_item_info(item, item_info);
}

cb::engine_errc MockEngine::set_collection_manifest(CookieIface& cookie,
                                                    std::string_view json) {
    auto engine_fn = [this, &cookie, json]() {
        return the_engine->set_collection_manifest(cookie, json);
    };
    return call_engine_and_handle_EWOULDBLOCK(cookie, engine_fn);
}

cb::engine_errc MockEngine::get_collection_manifest(
        CookieIface& cookie, const AddResponseFn& response) {
    return the_engine->get_collection_manifest(cookie, response);
}

cb::EngineErrorGetCollectionIDResult MockEngine::get_collection_id(
        CookieIface& cookie, std::string_view path) {
    return the_engine->get_collection_id(cookie, path);
}

cb::EngineErrorGetScopeIDResult MockEngine::get_scope_id(
        CookieIface& cookie, std::string_view path) {
    return the_engine->get_scope_id(cookie, path);
}

cb::EngineErrorGetCollectionMetaResult MockEngine::get_collection_meta(
        CookieIface& cookie, CollectionID cid, std::optional<Vbid> vbid) const {
    return the_engine->get_collection_meta(cookie, cid, vbid);
}

cb::engine_errc MockEngine::start_persistence(CookieIface& cookie) {
    auto engine_fn = [this, &cookie]() {
        return the_engine->start_persistence(cookie);
    };
    return call_engine_and_handle_EWOULDBLOCK(cookie, engine_fn);
}

cb::engine_errc MockEngine::stop_persistence(CookieIface& cookie) {
    auto engine_fn = [this, &cookie]() {
        return the_engine->stop_persistence(cookie);
    };
    return call_engine_and_handle_EWOULDBLOCK(cookie, engine_fn);
}

cb::engine_errc MockEngine::wait_for_seqno_persistence(CookieIface& cookie,
                                                       uint64_t seqno,
                                                       Vbid vbid) {
    auto engine_fn = [this, &cookie, seqno, vbid]() {
        return the_engine->wait_for_seqno_persistence(cookie, seqno, vbid);
    };
    return call_engine_and_handle_EWOULDBLOCK(cookie, engine_fn);
}

cb::engine_errc MockEngine::evict_key(CookieIface& cookie,
                                      const DocKeyView& key,
                                      Vbid vbucket) {
    auto engine_fn = [this, &cookie, k = std::cref(key), vbucket]() {
        return the_engine->evict_key(cookie, k, vbucket);
    };
    return call_engine_and_handle_EWOULDBLOCK(cookie, engine_fn);
}

cb::engine_errc MockEngine::observe(
        CookieIface& cookie,
        const DocKeyView& key,
        Vbid vbucket,
        const std::function<void(uint8_t, uint64_t)>& key_handler,
        uint64_t& persist_time_hint) {
    auto engine_fn = [this,
                      &cookie,
                      k = std::cref(key),
                      vbucket,
                      key_handler,
                      r = std::ref(persist_time_hint)]() {
        return the_engine->observe(cookie, k, vbucket, key_handler, r);
    };
    return call_engine_and_handle_EWOULDBLOCK(cookie, engine_fn);
}

cb::engine_errc MockEngine::step(CookieIface& cookie,
                                 bool throttled,
                                 DcpMessageProducersIface& producers) {
    return the_engine_dcp->step(cookie, throttled, producers);
}

cb::engine_errc MockEngine::open(CookieIface& cookie,
                                 uint32_t opaque,
                                 uint32_t seqno,
                                 cb::mcbp::DcpOpenFlag flags,
                                 std::string_view name,
                                 std::string_view value) {
    return the_engine_dcp->open(cookie, opaque, seqno, flags, name, value);
}

cb::engine_errc MockEngine::add_stream(CookieIface& cookie,
                                       uint32_t opaque,
                                       Vbid vbucket,
                                       cb::mcbp::DcpAddStreamFlag flags) {
    auto engine_fn = [this, &cookie, opaque, vbucket, flags]() {
        return the_engine_dcp->add_stream(cookie, opaque, vbucket, flags);
    };

    return call_engine_and_handle_EWOULDBLOCK(cookie, engine_fn);
}

cb::engine_errc MockEngine::close_stream(CookieIface& cookie,
                                         uint32_t opaque,
                                         Vbid vbucket,
                                         cb::mcbp::DcpStreamId sid) {
    return the_engine_dcp->close_stream(cookie, opaque, vbucket, sid);
}

cb::engine_errc MockEngine::stream_req(CookieIface& cookie,
                                       cb::mcbp::DcpAddStreamFlag flags,
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
    return the_engine_dcp->stream_req(cookie,
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

cb::engine_errc MockEngine::get_failover_log(CookieIface& cookie,
                                             uint32_t opaque,
                                             Vbid vbucket,
                                             dcp_add_failover_log cb) {
    return the_engine_dcp->get_failover_log(cookie, opaque, vbucket, cb);
}

cb::engine_errc MockEngine::stream_end(CookieIface& cookie,
                                       uint32_t opaque,
                                       Vbid vbucket,
                                       cb::mcbp::DcpStreamEndStatus status) {
    return the_engine_dcp->stream_end(cookie, opaque, vbucket, status);
}

cb::engine_errc MockEngine::snapshot_marker(
        CookieIface& cookie,
        uint32_t opaque,
        Vbid vbucket,
        uint64_t start_seqno,
        uint64_t end_seqno,
        cb::mcbp::request::DcpSnapshotMarkerFlag flags,
        std::optional<uint64_t> high_completed_seqno,
        std::optional<uint64_t> high_prepared_seqno,
        std::optional<uint64_t> max_visible_seqno,
        std::optional<uint64_t> purge_seqno) {
    return the_engine_dcp->snapshot_marker(cookie,
                                           opaque,
                                           vbucket,
                                           start_seqno,
                                           end_seqno,
                                           flags,
                                           high_completed_seqno,
                                           high_prepared_seqno,
                                           max_visible_seqno,
                                           purge_seqno);
}

cb::engine_errc MockEngine::mutation(CookieIface& cookie,
                                     uint32_t opaque,
                                     const DocKeyView& key,
                                     cb::const_byte_buffer value,
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
    auto engine_fn = [this,
                      &cookie,
                      opaque,
                      k = std::cref(key),
                      value,
                      datatype,
                      cas,
                      vbucket,
                      flags,
                      by_seqno,
                      rev_seqno,
                      expiration,
                      lock_time,
                      meta,
                      nru]() {
        return the_engine_dcp->mutation(cookie,
                                        opaque,
                                        k,
                                        value,
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
    };

    return call_engine_and_handle_EWOULDBLOCK(cookie, engine_fn);
}

cb::engine_errc MockEngine::deletion(CookieIface& cookie,
                                     uint32_t opaque,
                                     const DocKeyView& key,
                                     cb::const_byte_buffer value,
                                     uint8_t datatype,
                                     uint64_t cas,
                                     Vbid vbucket,
                                     uint64_t by_seqno,
                                     uint64_t rev_seqno,
                                     cb::const_byte_buffer meta) {
    auto engine_fn = [this,
                      &cookie,
                      opaque,
                      k = std::cref(key),
                      value,
                      datatype,
                      cas,
                      vbucket,
                      by_seqno,
                      rev_seqno,
                      meta]() {
        return the_engine_dcp->deletion(cookie,
                                        opaque,
                                        k,
                                        value,
                                        datatype,
                                        cas,
                                        vbucket,
                                        by_seqno,
                                        rev_seqno,
                                        meta);
    };

    return call_engine_and_handle_EWOULDBLOCK(cookie, engine_fn);
}

cb::engine_errc MockEngine::expiration(CookieIface& cookie,
                                       uint32_t opaque,
                                       const DocKeyView& key,
                                       cb::const_byte_buffer value,
                                       uint8_t datatype,
                                       uint64_t cas,
                                       Vbid vbucket,
                                       uint64_t by_seqno,
                                       uint64_t rev_seqno,
                                       uint32_t deleteTime) {
    auto engine_fn = [this,
                      &cookie,
                      opaque,
                      k = std::cref(key),
                      value,
                      datatype,
                      cas,
                      vbucket,
                      by_seqno,
                      rev_seqno,
                      deleteTime]() {
        return the_engine_dcp->expiration(cookie,
                                          opaque,
                                          k,
                                          value,
                                          datatype,
                                          cas,
                                          vbucket,
                                          by_seqno,
                                          rev_seqno,
                                          deleteTime);
    };

    return call_engine_and_handle_EWOULDBLOCK(cookie, engine_fn);
}

cb::engine_errc MockEngine::set_vbucket_state(CookieIface& cookie,
                                              uint32_t opaque,
                                              Vbid vbucket,
                                              vbucket_state_t state) {
    return the_engine_dcp->set_vbucket_state(cookie, opaque, vbucket, state);
}

cb::engine_errc MockEngine::noop(CookieIface& cookie, uint32_t opaque) {
    return the_engine_dcp->noop(cookie, opaque);
}

cb::engine_errc MockEngine::control(CookieIface& cookie,
                                    uint32_t opaque,
                                    std::string_view key,
                                    std::string_view value) {
    return the_engine_dcp->control(cookie, opaque, key, value);
}

cb::engine_errc MockEngine::buffer_acknowledgement(CookieIface& cookie,
                                                   uint32_t opaque,
                                                   uint32_t buffer_bytes) {
    return the_engine_dcp->buffer_acknowledgement(cookie, opaque, buffer_bytes);
}

cb::engine_errc MockEngine::response_handler(
        CookieIface& cookie, const cb::mcbp::Response& response) {
    return the_engine_dcp->response_handler(cookie, response);
}

cb::engine_errc MockEngine::system_event(CookieIface& cookie,
                                         uint32_t opaque,
                                         Vbid vbucket,
                                         mcbp::systemevent::id event,
                                         uint64_t bySeqno,
                                         mcbp::systemevent::version version,
                                         cb::const_byte_buffer key,
                                         cb::const_byte_buffer eventData) {
    return the_engine_dcp->system_event(
            cookie, opaque, vbucket, event, bySeqno, version, key, eventData);
}

cb::engine_errc MockEngine::prepare(CookieIface& cookie,
                                    uint32_t opaque,
                                    const DocKeyView& key,
                                    cb::const_byte_buffer value,
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
    return the_engine_dcp->prepare(cookie,
                                   opaque,
                                   key,
                                   value,
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

cb::engine_errc MockEngine::seqno_acknowledged(CookieIface& cookie,
                                               uint32_t opaque,
                                               Vbid vbucket,
                                               uint64_t prepared_seqno) {
    return the_engine_dcp->seqno_acknowledged(
            cookie, opaque, vbucket, prepared_seqno);
}

cb::engine_errc MockEngine::commit(CookieIface& cookie,
                                   uint32_t opaque,
                                   Vbid vbucket,
                                   const DocKeyView& key,
                                   uint64_t prepared_seqno,
                                   uint64_t commit_seqno) {
    return the_engine_dcp->commit(
            cookie, opaque, vbucket, key, prepared_seqno, commit_seqno);
}

cb::engine_errc MockEngine::abort(CookieIface& cookie,
                                  uint32_t opaque,
                                  Vbid vbucket,
                                  const DocKeyView& key,
                                  uint64_t prepared_seqno,
                                  uint64_t abort_seqno) {
    return the_engine_dcp->abort(
            cookie, opaque, vbucket, key, prepared_seqno, abort_seqno);
}

void MockEngine::disconnect(CookieIface& cookie) {
    the_engine->disconnect(cookie);
}

cb::engine_errc MockEngine::setParameter(CookieIface& cookie,
                                         EngineParamCategory category,
                                         std::string_view key,
                                         std::string_view value,
                                         Vbid vbucket) {
    auto engine_fn = [this, &cookie, category, key, value, vbucket]() {
        return the_engine->setParameter(cookie, category, key, value, vbucket);
    };

    return call_engine_and_handle_EWOULDBLOCK(cookie, engine_fn);
}

cb::engine_errc MockEngine::compactDatabase(
        CookieIface& cookie,
        Vbid vbid,
        uint64_t purge_before_ts,
        uint64_t purge_before_seq,
        bool drop_deletes,
        const std::vector<std::string>& obsolete_keys) {
    auto engine_fn = [this,
                      &cookie,
                      vbid,
                      purge_before_ts,
                      purge_before_seq,
                      drop_deletes,
                      &obsolete_keys]() {
        return the_engine->compactDatabase(cookie,
                                           vbid,
                                           purge_before_ts,
                                           purge_before_seq,
                                           drop_deletes,
                                           obsolete_keys);
    };

    return call_engine_and_handle_EWOULDBLOCK(cookie, engine_fn);
}

std::pair<cb::engine_errc, vbucket_state_t> MockEngine::getVBucket(
        CookieIface& cookie, Vbid vbid) {
    return the_engine->getVBucket(cookie, vbid);
}

cb::engine_errc MockEngine::setVBucket(CookieIface& cookie,
                                       Vbid vbid,
                                       uint64_t cas,
                                       vbucket_state_t state,
                                       nlohmann::json* meta) {
    auto engine_fn = [this, &cookie, vbid, cas, state, meta]() {
        return the_engine->setVBucket(cookie, vbid, cas, state, meta);
    };

    return call_engine_and_handle_EWOULDBLOCK(cookie, engine_fn);
}

cb::engine_errc MockEngine::deleteVBucket(CookieIface& cookie,
                                          Vbid vbid,
                                          bool sync) {
    auto engine_fn = [this, &cookie, vbid, sync]() {
        return the_engine->deleteVBucket(cookie, vbid, sync);
    };

    return call_engine_and_handle_EWOULDBLOCK(cookie, engine_fn);
}

std::pair<cb::engine_errc, cb::rangescan::Id> MockEngine::createRangeScan(
        CookieIface& cookie, const cb::rangescan::CreateParameters& params) {
    auto engine_fn = [this, &cookie, params]() {
        return the_engine->createRangeScan(cookie, params);
    };

    return do_blocking_engine_call<cb::rangescan::Id>(cookie, engine_fn);
}
