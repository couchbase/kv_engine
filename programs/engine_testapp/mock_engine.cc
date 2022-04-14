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
#include <functional>

/**
 * EWOULDBLOCK wrapper.
 * Will recall "engine_function" with EWOULDBLOCK retry logic.
 **/
template <typename T>
static std::pair<cb::engine_errc, T> do_blocking_engine_call(
        MockCookie* c,
        const std::function<std::pair<cb::engine_errc, T>()>& engine_function) {
    std::unique_lock<std::mutex> lock(c->getMutex());

    auto ret = engine_function();
    while (ret.first == cb::engine_errc::would_block && c->isEwouldblock()) {
        const auto status = mock_waitfor_cookie(c);

        if (status == cb::engine_errc::success) {
            ret = engine_function();
        } else {
            return {status, T()};
        }
    }

    return ret;
}

/*
 * @todo: We can avoid duplicating this code by turning cb::EngineErrorCasPair
 *  from a named structure into a real std::pair<engine_errc, uint64_t>.
 *  Not doing here as it requires touching a bit around.
 */
static cb::EngineErrorCasPair call_engine_and_handle_EWOULDBLOCK(
        MockCookie* c,
        const std::function<cb::EngineErrorCasPair()>& engine_function) {
    std::unique_lock<std::mutex> lock(c->getMutex());

    auto ret = engine_function();
    while (ret.status == cb::engine_errc::would_block && c->isEwouldblock()) {
        const auto status = mock_waitfor_cookie(c);

        if (status == cb::engine_errc::success) {
            ret = engine_function();
        } else {
            return {status, 0};
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
        MockCookie* c,
        const std::function<cb::engine_errc()>& engine_function) {
    std::unique_lock<std::mutex> lock(c->getMutex());

    auto ret = engine_function();
    while (ret == cb::engine_errc::would_block && c->isEwouldblock()) {
        const auto status = mock_waitfor_cookie(c);

        if (status == cb::engine_errc::success) {
            ret = engine_function();
        } else {
            return status;
        }
    }

    return ret;
}

/**
 * Helper function to return a mock_connstruct, either a new one or
 * an existng one.
 **/
MockCookie* get_or_create_mock_connstruct(const CookieIface* cookie,
                                          EngineIface* engine) {
    if (cookie == nullptr) {
        return create_mock_cookie(engine);
    }
    return cookie_to_mock_cookie(cookie);
}

/**
 * Helper function to destroy a mock_connstruct if get_or_create_mock_connstruct
 * created one.
 **/
void check_and_destroy_mock_connstruct(MockCookie* c,
                                       const CookieIface* cookie) {
    if (c != cookie) {
        destroy_mock_cookie(c);
    }
}

cb::engine_errc MockEngine::initialize(const std::string& config_str) {
    return the_engine->initialize(config_str);
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

std::pair<cb::unique_item_ptr, item_info> MockEngine::allocateItem(
        const CookieIface& cookie,
        const DocKey& key,
        size_t nbytes,
        size_t priv_nbytes,
        int flags,
        rel_time_t exptime,
        uint8_t datatype,
        Vbid vbucket) {
    auto* c = cookie_to_mock_cookie(&cookie);

    std::unique_lock<std::mutex> lock(c->getMutex());
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
        const CookieIface& cookie,
        const DocKey& key,
        uint64_t& cas,
        Vbid vbucket,
        const std::optional<cb::durability::Requirements>& durability,
        mutation_descr_t& mut_info) {
    auto engine_fn = [this,
                      c = std::cref(cookie),
                      k = std::cref(key),
                      cas_ = std::ref(cas),
                      vbucket,
                      dur = std::cref(durability),
                      m = std::ref(mut_info)]() {
        return the_engine->remove(c, k, cas_, vbucket, dur, m);
    };

    auto* construct = cookie_to_mock_cookie(&cookie);
    return call_engine_and_handle_EWOULDBLOCK(construct, engine_fn);
}

void MockEngine::release(ItemIface& item) {
    the_engine->release(item);
}

cb::EngineErrorItemPair MockEngine::get(const CookieIface& cookie,
                                        const DocKey& key,
                                        Vbid vbucket,
                                        DocStateFilter documentStateFilter) {
    auto engine_fn = [this,
                      &cookie,
                      k = std::cref(key),
                      vbucket,
                      documentStateFilter]() {
        return the_engine->get(cookie, k, vbucket, documentStateFilter);
    };

    auto* construct = cookie_to_mock_cookie(&cookie);
    return do_blocking_engine_call<cb::unique_item_ptr>(construct, engine_fn);
}

cb::EngineErrorItemPair MockEngine::get_if(
        const CookieIface& cookie,
        const DocKey& key,
        Vbid vbucket,
        std::function<bool(const item_info&)> filter) {
    auto engine_fn = [this, &cookie, k = std::cref(key), vbucket, filter]() {
        return the_engine->get_if(cookie, k, vbucket, filter);
    };
    auto* construct = cookie_to_mock_cookie(&cookie);
    return do_blocking_engine_call<cb::unique_item_ptr>(construct, engine_fn);
}

cb::EngineErrorItemPair MockEngine::get_and_touch(
        const CookieIface& cookie,
        const DocKey& key,
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

    auto* construct = cookie_to_mock_cookie(&cookie);
    return do_blocking_engine_call<cb::unique_item_ptr>(construct, engine_fn);
}

cb::EngineErrorItemPair MockEngine::get_locked(const CookieIface& cookie,
                                               const DocKey& key,
                                               Vbid vbucket,
                                               uint32_t lock_timeout) {
    auto engine_fn =
            [this, &cookie, k = std::cref(key), vbucket, lock_timeout]() {
                return the_engine->get_locked(cookie, k, vbucket, lock_timeout);
            };

    auto* construct = cookie_to_mock_cookie(&cookie);
    return do_blocking_engine_call<cb::unique_item_ptr>(construct, engine_fn);
}

cb::EngineErrorMetadataPair MockEngine::get_meta(const CookieIface& cookie,
                                                 const DocKey& key,
                                                 Vbid vbucket) {
    auto engine_fn = [this, &cookie, k = std::cref(key), vbucket]() {
        return the_engine->get_meta(cookie, k, vbucket);
    };

    auto* construct = cookie_to_mock_cookie(&cookie);
    return do_blocking_engine_call<item_info>(construct, engine_fn);
}

cb::engine_errc MockEngine::unlock(const CookieIface& cookie,
                                   const DocKey& key,
                                   Vbid vbucket,
                                   uint64_t cas) {
    auto engine_fn = [this, &cookie, k = std::cref(key), vbucket, cas]() {
        return the_engine->unlock(cookie, k, vbucket, cas);
    };

    auto* construct = cookie_to_mock_cookie(&cookie);
    return call_engine_and_handle_EWOULDBLOCK(construct, engine_fn);
}

cb::engine_errc MockEngine::get_stats(const CookieIface& cookie,
                                      std::string_view key,
                                      std::string_view value,
                                      const AddStatFn& add_stat) {
    auto engine_fn = [this, &cookie, key, value, stat = std::cref(add_stat)]() {
        return the_engine->get_stats(cookie, key, value, stat);
    };

    auto* construct = cookie_to_mock_cookie(&cookie);
    return call_engine_and_handle_EWOULDBLOCK(construct, engine_fn);
}

cb::engine_errc MockEngine::store(
        const CookieIface& cookie,
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

    auto* construct = cookie_to_mock_cookie(&cookie);

    return call_engine_and_handle_EWOULDBLOCK(construct, engine_fn);
}

cb::EngineErrorCasPair MockEngine::store_if(
        const CookieIface& cookie,
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
    auto* mockCookie = cookie_to_mock_cookie(&cookie);
    return call_engine_and_handle_EWOULDBLOCK(mockCookie, engine_fn);
}

cb::engine_errc MockEngine::flush(const CookieIface& cookie) {
    auto engine_fn = [this, &cookie]() { return the_engine->flush(cookie); };

    auto* construct = cookie_to_mock_cookie(&cookie);
    return call_engine_and_handle_EWOULDBLOCK(construct, engine_fn);
}

void MockEngine::reset_stats(const CookieIface& cookie) {
    the_engine->reset_stats(cookie);
}

cb::engine_errc MockEngine::unknown_command(const CookieIface* cookie,
                                            const cb::mcbp::Request& request,
                                            const AddResponseFn& response) {
    auto* c = get_or_create_mock_connstruct(cookie, this);
    auto engine_fn =
            [this, c, req = std::cref(request), res = std::cref(response)]() {
                return the_engine->unknown_command(c, req, res);
            };

    cb::engine_errc ret = call_engine_and_handle_EWOULDBLOCK(c, engine_fn);

    check_and_destroy_mock_connstruct(c, cookie);
    return ret;
}

void MockEngine::item_set_cas(ItemIface& item, uint64_t val) {
    the_engine->item_set_cas(item, val);
}

void MockEngine::item_set_datatype(ItemIface& item,
                                   protocol_binary_datatype_t datatype) {
    the_engine->item_set_datatype(item, datatype);
}

bool MockEngine::get_item_info(const ItemIface& item, item_info& item_info) {
    return the_engine->get_item_info(item, item_info);
}

cb::engine_errc MockEngine::set_collection_manifest(const CookieIface& cookie,
                                                    std::string_view json) {
    auto* c = get_or_create_mock_connstruct(&cookie, this);

    auto engine_fn = [this, &cookie, json]() {
        return the_engine->set_collection_manifest(cookie, json);
    };
    auto status = call_engine_and_handle_EWOULDBLOCK(c, engine_fn);
    check_and_destroy_mock_connstruct(c, &cookie);
    return status;
}
cb::engine_errc MockEngine::get_collection_manifest(
        const CookieIface& cookie, const AddResponseFn& response) {
    return the_engine->get_collection_manifest(cookie, response);
}

cb::EngineErrorGetCollectionIDResult MockEngine::get_collection_id(
        const CookieIface& cookie, std::string_view path) {
    return the_engine->get_collection_id(cookie, path);
}

cb::EngineErrorGetScopeIDResult MockEngine::get_scope_id(
        const CookieIface& cookie, std::string_view path) {
    return the_engine->get_scope_id(cookie, path);
}

cb::EngineErrorGetScopeIDResult MockEngine::get_scope_id(
        const CookieIface& cookie,
        CollectionID cid,
        std::optional<Vbid> vbid) const {
    return the_engine->get_scope_id(cookie, cid, vbid);
}

cb::engine_errc MockEngine::step(const CookieIface& cookie,
                                 DcpMessageProducersIface& producers) {
    return the_engine_dcp->step(cookie, producers);
}

cb::engine_errc MockEngine::open(const CookieIface& cookie,
                                 uint32_t opaque,
                                 uint32_t seqno,
                                 uint32_t flags,
                                 std::string_view name,
                                 std::string_view value) {
    return the_engine_dcp->open(cookie, opaque, seqno, flags, name, value);
}

cb::engine_errc MockEngine::add_stream(const CookieIface& cookie,
                                       uint32_t opaque,
                                       Vbid vbucket,
                                       uint32_t flags) {
    auto engine_fn = [this, &cookie, opaque, vbucket, flags]() {
        return the_engine_dcp->add_stream(cookie, opaque, vbucket, flags);
    };

    auto* c = cookie_to_mock_cookie(&cookie);
    return call_engine_and_handle_EWOULDBLOCK(c, engine_fn);
}

cb::engine_errc MockEngine::close_stream(const CookieIface& cookie,
                                         uint32_t opaque,
                                         Vbid vbucket,
                                         cb::mcbp::DcpStreamId sid) {
    return the_engine_dcp->close_stream(cookie, opaque, vbucket, sid);
}

cb::engine_errc MockEngine::stream_req(const CookieIface& cookie,
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

cb::engine_errc MockEngine::get_failover_log(const CookieIface& cookie,
                                             uint32_t opaque,
                                             Vbid vbucket,
                                             dcp_add_failover_log cb) {
    return the_engine_dcp->get_failover_log(cookie, opaque, vbucket, cb);
}

cb::engine_errc MockEngine::stream_end(const CookieIface& cookie,
                                       uint32_t opaque,
                                       Vbid vbucket,
                                       cb::mcbp::DcpStreamEndStatus status) {
    return the_engine_dcp->stream_end(cookie, opaque, vbucket, status);
}

cb::engine_errc MockEngine::snapshot_marker(
        const CookieIface& cookie,
        uint32_t opaque,
        Vbid vbucket,
        uint64_t start_seqno,
        uint64_t end_seqno,
        uint32_t flags,
        std::optional<uint64_t> high_completed_seqno,
        std::optional<uint64_t> max_visible_seqno) {
    return the_engine_dcp->snapshot_marker(cookie,
                                           opaque,
                                           vbucket,
                                           start_seqno,
                                           end_seqno,
                                           flags,
                                           high_completed_seqno,
                                           max_visible_seqno);
}

cb::engine_errc MockEngine::mutation(const CookieIface& cookie,
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
    auto* c = cookie_to_mock_cookie(&cookie);
    auto engine_fn = [this,
                      &cookie,
                      opaque,
                      k = std::cref(key),
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
                      nru]() {
        return the_engine_dcp->mutation(cookie,
                                        opaque,
                                        k,
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
    };

    return call_engine_and_handle_EWOULDBLOCK(c, engine_fn);
}

cb::engine_errc MockEngine::deletion(const CookieIface& cookie,
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
    auto engine_fn = [this,
                      &cookie,
                      opaque,
                      k = std::cref(key),
                      value,
                      priv_bytes,
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
                                        priv_bytes,
                                        datatype,
                                        cas,
                                        vbucket,
                                        by_seqno,
                                        rev_seqno,
                                        meta);
    };

    auto* c = cookie_to_mock_cookie(&cookie);
    return call_engine_and_handle_EWOULDBLOCK(c, engine_fn);
}

cb::engine_errc MockEngine::expiration(const CookieIface& cookie,
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
    auto engine_fn = [this,
                      &cookie,
                      opaque,
                      k = std::cref(key),
                      value,
                      priv_bytes,
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
                                          priv_bytes,
                                          datatype,
                                          cas,
                                          vbucket,
                                          by_seqno,
                                          rev_seqno,
                                          deleteTime);
    };

    auto* c = cookie_to_mock_cookie(&cookie);
    return call_engine_and_handle_EWOULDBLOCK(c, engine_fn);
}

cb::engine_errc MockEngine::set_vbucket_state(const CookieIface& cookie,
                                              uint32_t opaque,
                                              Vbid vbucket,
                                              vbucket_state_t state) {
    return the_engine_dcp->set_vbucket_state(cookie, opaque, vbucket, state);
}

cb::engine_errc MockEngine::noop(const CookieIface& cookie, uint32_t opaque) {
    return the_engine_dcp->noop(cookie, opaque);
}

cb::engine_errc MockEngine::control(const CookieIface& cookie,
                                    uint32_t opaque,
                                    std::string_view key,
                                    std::string_view value) {
    return the_engine_dcp->control(cookie, opaque, key, value);
}

cb::engine_errc MockEngine::buffer_acknowledgement(const CookieIface& cookie,
                                                   uint32_t opaque,
                                                   Vbid vbucket,
                                                   uint32_t buffer_bytes) {
    return the_engine_dcp->buffer_acknowledgement(
            cookie, opaque, vbucket, buffer_bytes);
}

cb::engine_errc MockEngine::response_handler(
        const CookieIface& cookie, const cb::mcbp::Response& response) {
    return the_engine_dcp->response_handler(cookie, response);
}

cb::engine_errc MockEngine::system_event(const CookieIface& cookie,
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

cb::engine_errc MockEngine::prepare(const CookieIface& cookie,
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
    return the_engine_dcp->prepare(cookie,
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

cb::engine_errc MockEngine::seqno_acknowledged(const CookieIface& cookie,
                                               uint32_t opaque,
                                               Vbid vbucket,
                                               uint64_t prepared_seqno) {
    return the_engine_dcp->seqno_acknowledged(
            cookie, opaque, vbucket, prepared_seqno);
}

cb::engine_errc MockEngine::commit(const CookieIface& cookie,
                                   uint32_t opaque,
                                   Vbid vbucket,
                                   const DocKey& key,
                                   uint64_t prepared_seqno,
                                   uint64_t commit_seqno) {
    return the_engine_dcp->commit(
            cookie, opaque, vbucket, key, prepared_seqno, commit_seqno);
}

cb::engine_errc MockEngine::abort(const CookieIface& cookie,
                                  uint32_t opaque,
                                  Vbid vbucket,
                                  const DocKey& key,
                                  uint64_t prepared_seqno,
                                  uint64_t abort_seqno) {
    return the_engine_dcp->abort(
            cookie, opaque, vbucket, key, prepared_seqno, abort_seqno);
}

void MockEngine::disconnect(const CookieIface& cookie) {
    the_engine->disconnect(cookie);
}

cb::engine_errc MockEngine::setParameter(const CookieIface& cookie,
                                         EngineParamCategory category,
                                         std::string_view key,
                                         std::string_view value,
                                         Vbid vbucket) {
    auto* c = cookie_to_mock_cookie(&cookie);
    auto engine_fn = [this, &cookie, category, key, value, vbucket]() {
        return the_engine->setParameter(cookie, category, key, value, vbucket);
    };

    return call_engine_and_handle_EWOULDBLOCK(c, engine_fn);
}

cb::engine_errc MockEngine::compactDatabase(const CookieIface& cookie,
                                            Vbid vbid,
                                            uint64_t purge_before_ts,
                                            uint64_t purge_before_seq,
                                            bool drop_deletes) {
    auto* c = cookie_to_mock_cookie(&cookie);
    auto engine_fn = [this,
                      &cookie,
                      vbid,
                      purge_before_ts,
                      purge_before_seq,
                      drop_deletes]() {
        return the_engine->compactDatabase(
                cookie, vbid, purge_before_ts, purge_before_seq, drop_deletes);
    };

    return call_engine_and_handle_EWOULDBLOCK(c, engine_fn);
}

std::pair<cb::engine_errc, vbucket_state_t> MockEngine::getVBucket(
        const CookieIface& cookie, Vbid vbid) {
    return the_engine->getVBucket(cookie, vbid);
}

cb::engine_errc MockEngine::setVBucket(const CookieIface& cookie,
                                       Vbid vbid,
                                       uint64_t cas,
                                       vbucket_state_t state,
                                       nlohmann::json* meta) {
    auto* c = cookie_to_mock_cookie(&cookie);
    auto engine_fn = [this, &cookie, vbid, cas, state, meta]() {
        return the_engine->setVBucket(cookie, vbid, cas, state, meta);
    };

    return call_engine_and_handle_EWOULDBLOCK(c, engine_fn);
}

cb::engine_errc MockEngine::deleteVBucket(const CookieIface& cookie,
                                          Vbid vbid,
                                          bool sync) {
    auto* c = cookie_to_mock_cookie(&cookie);
    auto engine_fn = [this, &cookie, vbid, sync]() {
        return the_engine->deleteVBucket(cookie, vbid, sync);
    };

    return call_engine_and_handle_EWOULDBLOCK(c, engine_fn);
}
