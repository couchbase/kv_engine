/*
 *     Copyright 2019 Couchbase, Inc
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

#include "mock_engine.h"
#include "mock_cookie.h"
#include "mock_server.h"

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
    c->nblocks = 0;
    std::unique_lock<std::mutex> lock(c->mutex);

    auto ret = engine_function();
    while (ret.first == cb::engine_errc::would_block && c->handle_ewouldblock) {
        ++c->nblocks;
        c->cond.wait(lock, [&c] {
            return c->num_processed_notifications != c->num_io_notifications;
        });
        c->num_processed_notifications = c->num_io_notifications;

        if (c->status == ENGINE_SUCCESS) {
            ret = engine_function();
        } else {
            return std::make_pair(cb::engine_errc(c->status), T());
        }
    }

    return ret;
}

static ENGINE_ERROR_CODE call_engine_and_handle_EWOULDBLOCK(
        MockCookie* c,
        const std::function<ENGINE_ERROR_CODE()>& engine_function) {
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    c->nblocks = 0;
    std::unique_lock<std::mutex> lock(c->mutex);

    while (ret == ENGINE_SUCCESS &&
           (ret = engine_function()) == ENGINE_EWOULDBLOCK &&
           c->handle_ewouldblock) {
        ++c->nblocks;
        c->cond.wait(lock, [&c] {
            return c->num_processed_notifications != c->num_io_notifications;
        });
        c->num_processed_notifications = c->num_io_notifications;
        ret = c->status;
    }

    return ret;
}

/**
 * Helper function to return a mock_connstruct, either a new one or
 * an existng one.
 **/
MockCookie* get_or_create_mock_connstruct(const void* cookie,
                                          EngineIface* engine) {
    if (cookie == nullptr) {
        return cookie_to_mock_cookie(create_mock_cookie(engine));
    }
    return cookie_to_mock_cookie(cookie);
}

/**
 * Helper function to destroy a mock_connstruct if get_or_create_mock_connstruct
 * created one.
 **/
void check_and_destroy_mock_connstruct(MockCookie* c, const void* cookie) {
    if (c != cookie) {
        destroy_mock_cookie(c);
    }
}

ENGINE_ERROR_CODE MockEngine::initialize(const char* config_str) {
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

cb::EngineErrorItemPair MockEngine::allocate(gsl::not_null<const void*> cookie,
                                             const DocKey& key,
                                             const size_t nbytes,
                                             const int flags,
                                             const rel_time_t exptime,
                                             uint8_t datatype,
                                             Vbid vbucket) {
    auto engine_fn = std::bind(&EngineIface::allocate,
                               the_engine.get(),
                               cookie,
                               key,
                               nbytes,
                               flags,
                               exptime,
                               datatype,
                               vbucket);

    auto* c = cookie_to_mock_cookie(cookie.get());
    return do_blocking_engine_call<cb::unique_item_ptr>(c, engine_fn);
}

std::pair<cb::unique_item_ptr, item_info> MockEngine::allocate_ex(
        gsl::not_null<const void*> cookie,
        const DocKey& key,
        const size_t nbytes,
        const size_t priv_nbytes,
        const int flags,
        const rel_time_t exptime,
        uint8_t datatype,
        Vbid vbucket) {
    auto engine_fn = std::bind(&EngineIface::allocate_ex,
                               the_engine.get(),
                               cookie,
                               key,
                               nbytes,
                               priv_nbytes,
                               flags,
                               exptime,
                               datatype,
                               vbucket);

    auto* c = cookie_to_mock_cookie(cookie.get());
    c->nblocks = 0;

    std::lock_guard<std::mutex> guard(c->mutex);

    try {
        return engine_fn();
    } catch (const cb::engine_error& error) {
        if (error.code() == cb::engine_errc::would_block) {
            throw std::logic_error(
                    "mock_allocate_ex: allocate_ex should not block!");
        }
        throw error;
    }
    throw std::logic_error("mock_allocate_ex: Should never get here");
}

ENGINE_ERROR_CODE MockEngine::remove(
        gsl::not_null<const void*> cookie,
        const DocKey& key,
        uint64_t& cas,
        Vbid vbucket,
        const boost::optional<cb::durability::Requirements>& durability,
        mutation_descr_t& mut_info) {
    auto engine_fn = std::bind(&EngineIface::remove,
                               the_engine.get(),
                               cookie,
                               key,
                               std::ref(cas),
                               vbucket,
                               durability,
                               std::ref(mut_info));
    auto* construct = cookie_to_mock_cookie(cookie.get());
    return call_engine_and_handle_EWOULDBLOCK(construct, engine_fn);
}

void MockEngine::release(gsl::not_null<item*> item) {
    the_engine->release(item);
}

cb::EngineErrorItemPair MockEngine::get(gsl::not_null<const void*> cookie,
                                        const DocKey& key,
                                        Vbid vbucket,
                                        DocStateFilter documentStateFilter) {
    auto engine_fn = std::bind(&EngineIface::get,
                               the_engine.get(),
                               cookie,
                               key,
                               vbucket,
                               documentStateFilter);

    auto* construct = cookie_to_mock_cookie(cookie.get());
    return do_blocking_engine_call<cb::unique_item_ptr>(construct, engine_fn);
}

cb::EngineErrorItemPair MockEngine::get_if(
        gsl::not_null<const void*> cookie,
        const DocKey& key,
        Vbid vbucket,
        std::function<bool(const item_info&)> filter) {
    auto engine_fn = std::bind(&EngineIface::get_if,
                               the_engine.get(),
                               cookie,
                               key,
                               vbucket,
                               filter);
    auto* construct = cookie_to_mock_cookie(cookie.get());
    return do_blocking_engine_call<cb::unique_item_ptr>(construct, engine_fn);
}

cb::EngineErrorItemPair MockEngine::get_and_touch(
        gsl::not_null<const void*> cookie,
        const DocKey& key,
        Vbid vbucket,
        uint32_t expiryTime,
        const boost::optional<cb::durability::Requirements>& durability) {
    auto engine_fn = std::bind(&EngineIface::get_and_touch,
                               the_engine.get(),
                               cookie,
                               key,
                               vbucket,
                               expiryTime,
                               durability);

    auto* construct = cookie_to_mock_cookie(cookie.get());
    return do_blocking_engine_call<cb::unique_item_ptr>(construct, engine_fn);
}

cb::EngineErrorItemPair MockEngine::get_locked(
        gsl::not_null<const void*> cookie,
        const DocKey& key,
        Vbid vbucket,
        uint32_t lock_timeout) {
    auto engine_fn = std::bind(&EngineIface::get_locked,
                               the_engine.get(),
                               cookie,
                               key,
                               vbucket,
                               lock_timeout);
    auto* construct = cookie_to_mock_cookie(cookie.get());

    return do_blocking_engine_call<cb::unique_item_ptr>(construct, engine_fn);
}

cb::EngineErrorMetadataPair MockEngine::get_meta(
        gsl::not_null<const void*> cookie, const DocKey& key, Vbid vbucket) {
    auto engine_fn = std::bind(
            &EngineIface::get_meta, the_engine.get(), cookie, key, vbucket);

    auto* construct = cookie_to_mock_cookie(cookie.get());

    return do_blocking_engine_call<item_info>(construct, engine_fn);
}

ENGINE_ERROR_CODE MockEngine::unlock(gsl::not_null<const void*> cookie,
                                     const DocKey& key,
                                     Vbid vbucket,
                                     uint64_t cas) {
    auto engine_fn = std::bind(
            &EngineIface::unlock, the_engine.get(), cookie, key, vbucket, cas);

    auto* construct = cookie_to_mock_cookie(cookie.get());
    return call_engine_and_handle_EWOULDBLOCK(construct, engine_fn);
}

ENGINE_ERROR_CODE MockEngine::get_stats(gsl::not_null<const void*> cookie,
                                        cb::const_char_buffer key,
                                        cb::const_char_buffer value,
                                        const AddStatFn& add_stat) {
    auto engine_fn = std::bind(&EngineIface::get_stats,
                               the_engine.get(),
                               cookie,
                               key,
                               value,
                               add_stat);

    auto* construct = cookie_to_mock_cookie(cookie.get());
    return call_engine_and_handle_EWOULDBLOCK(construct, engine_fn);
}

ENGINE_ERROR_CODE MockEngine::store(
        gsl::not_null<const void*> cookie,
        gsl::not_null<item*> item,
        uint64_t& cas,
        ENGINE_STORE_OPERATION operation,
        const boost::optional<cb::durability::Requirements>& durability,
        DocumentState document_state,
        bool preserveTtl) {
    auto engine_fn = std::bind(&EngineIface::store,
                               the_engine.get(),
                               cookie,
                               item,
                               std::ref(cas),
                               operation,
                               durability,
                               document_state,
                               preserveTtl);

    auto* construct = cookie_to_mock_cookie(cookie.get());

    return call_engine_and_handle_EWOULDBLOCK(construct, engine_fn);
}

ENGINE_ERROR_CODE MockEngine::flush(gsl::not_null<const void*> cookie) {
    auto engine_fn = std::bind(&EngineIface::flush, the_engine.get(), cookie);

    auto* construct = cookie_to_mock_cookie(cookie.get());
    return call_engine_and_handle_EWOULDBLOCK(construct, engine_fn);
}

void MockEngine::reset_stats(gsl::not_null<const void*> cookie) {
    the_engine->reset_stats(cookie);
}

ENGINE_ERROR_CODE MockEngine::unknown_command(const void* cookie,
                                              const cb::mcbp::Request& request,
                                              const AddResponseFn& response) {
    auto* c = get_or_create_mock_connstruct(cookie, this);
    auto engine_fn = std::bind(&EngineIface::unknown_command,
                               the_engine.get(),
                               static_cast<const void*>(c),
                               std::cref(request),
                               response);

    ENGINE_ERROR_CODE ret = call_engine_and_handle_EWOULDBLOCK(c, engine_fn);

    check_and_destroy_mock_connstruct(c, cookie);
    return ret;
}

void MockEngine::item_set_cas(gsl::not_null<item*> item, uint64_t val) {
    the_engine->item_set_cas(item, val);
}

void MockEngine::item_set_datatype(gsl::not_null<item*> item,
                                   protocol_binary_datatype_t datatype) {
    the_engine->item_set_datatype(item, datatype);
}

bool MockEngine::get_item_info(gsl::not_null<const item*> item,
                               gsl::not_null<item_info*> item_info) {
    return the_engine->get_item_info(item, item_info);
}

cb::engine_errc MockEngine::set_collection_manifest(
        gsl::not_null<const void*> cookie, cb::const_char_buffer json) {
    return the_engine->set_collection_manifest(cookie, json);
}

ENGINE_ERROR_CODE MockEngine::step(
        gsl::not_null<const void*> cookie,
        gsl::not_null<dcp_message_producers*> producers) {
    return the_engine_dcp->step(cookie, producers);
}

ENGINE_ERROR_CODE MockEngine::open(gsl::not_null<const void*> cookie,
                                   uint32_t opaque,
                                   uint32_t seqno,
                                   uint32_t flags,
                                   cb::const_char_buffer name,
                                   cb::const_char_buffer value) {
    return the_engine_dcp->open(cookie, opaque, seqno, flags, name, value);
}

ENGINE_ERROR_CODE MockEngine::add_stream(gsl::not_null<const void*> cookie,
                                         uint32_t opaque,
                                         Vbid vbucket,
                                         uint32_t flags) {
    auto* c = cookie_to_mock_cookie(cookie.get());
    auto engine_fn = std::bind(&DcpIface::add_stream,
                               the_engine_dcp,
                               static_cast<const void*>(c),
                               opaque,
                               vbucket,
                               flags);

    return call_engine_and_handle_EWOULDBLOCK(c, engine_fn);
}

ENGINE_ERROR_CODE MockEngine::close_stream(gsl::not_null<const void*> cookie,
                                           uint32_t opaque,
                                           Vbid vbucket,
                                           cb::mcbp::DcpStreamId sid) {
    return the_engine_dcp->close_stream(cookie, opaque, vbucket, sid);
}

ENGINE_ERROR_CODE MockEngine::stream_req(
        gsl::not_null<const void*> cookie,
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
        boost::optional<cb::const_char_buffer> json) {
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

ENGINE_ERROR_CODE MockEngine::get_failover_log(
        gsl::not_null<const void*> cookie,
        uint32_t opaque,
        Vbid vbucket,
        dcp_add_failover_log cb) {
    return the_engine_dcp->get_failover_log(cookie, opaque, vbucket, cb);
}

ENGINE_ERROR_CODE MockEngine::stream_end(gsl::not_null<const void*> cookie,
                                         uint32_t opaque,
                                         Vbid vbucket,
                                         uint32_t flags) {
    return the_engine_dcp->stream_end(cookie, opaque, vbucket, flags);
}

ENGINE_ERROR_CODE MockEngine::snapshot_marker(
        gsl::not_null<const void*> cookie,
        uint32_t opaque,
        Vbid vbucket,
        uint64_t start_seqno,
        uint64_t end_seqno,
        uint32_t flags,
        boost::optional<uint64_t> high_completed_seqno,
        boost::optional<uint64_t> max_visible_seqno) {
    return the_engine_dcp->snapshot_marker(cookie,
                                           opaque,
                                           vbucket,
                                           start_seqno,
                                           end_seqno,
                                           flags,
                                           high_completed_seqno,
                                           max_visible_seqno);
}

ENGINE_ERROR_CODE MockEngine::mutation(gsl::not_null<const void*> cookie,
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
    auto* c = cookie_to_mock_cookie(cookie);
    auto engine_fn = std::bind(&DcpIface::mutation,
                               the_engine_dcp,
                               static_cast<const void*>(c),
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

    return call_engine_and_handle_EWOULDBLOCK(c, engine_fn);
}

ENGINE_ERROR_CODE MockEngine::deletion(gsl::not_null<const void*> cookie,
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
    auto* c = cookie_to_mock_cookie(cookie);
    auto engine_fn = std::bind(&DcpIface::deletion,
                               the_engine_dcp,
                               static_cast<const void*>(c),
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
    return call_engine_and_handle_EWOULDBLOCK(c, engine_fn);
}

ENGINE_ERROR_CODE MockEngine::expiration(gsl::not_null<const void*> cookie,
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
    auto* c = cookie_to_mock_cookie(cookie);
    auto engine_fn = std::bind(&DcpIface::expiration,
                               the_engine_dcp,
                               static_cast<const void*>(c),
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

    return call_engine_and_handle_EWOULDBLOCK(c, engine_fn);
}

ENGINE_ERROR_CODE MockEngine::set_vbucket_state(
        gsl::not_null<const void*> cookie,
        uint32_t opaque,
        Vbid vbucket,
        vbucket_state_t state) {
    return the_engine_dcp->set_vbucket_state(cookie, opaque, vbucket, state);
}

ENGINE_ERROR_CODE MockEngine::noop(gsl::not_null<const void*> cookie,
                                   uint32_t opaque) {
    return the_engine_dcp->noop(cookie, opaque);
}

ENGINE_ERROR_CODE MockEngine::control(gsl::not_null<const void*> cookie,
                                      uint32_t opaque,
                                      cb::const_char_buffer key,
                                      cb::const_char_buffer value) {
    return the_engine_dcp->control(cookie, opaque, key, value);
}

ENGINE_ERROR_CODE MockEngine::buffer_acknowledgement(
        gsl::not_null<const void*> cookie,
        uint32_t opaque,
        Vbid vbucket,
        uint32_t buffer_bytes) {
    return the_engine_dcp->buffer_acknowledgement(
            cookie, opaque, vbucket, buffer_bytes);
}

ENGINE_ERROR_CODE MockEngine::response_handler(
        gsl::not_null<const void*> cookie,
        const protocol_binary_response_header* response) {
    return the_engine_dcp->response_handler(cookie, response);
}

ENGINE_ERROR_CODE MockEngine::system_event(gsl::not_null<const void*> cookie,
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

ENGINE_ERROR_CODE MockEngine::prepare(gsl::not_null<const void*> cookie,
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

ENGINE_ERROR_CODE MockEngine::seqno_acknowledged(
        gsl::not_null<const void*> cookie,
        uint32_t opaque,
        Vbid vbucket,
        uint64_t prepared_seqno) {
    return the_engine_dcp->seqno_acknowledged(
            cookie, opaque, vbucket, prepared_seqno);
}

ENGINE_ERROR_CODE MockEngine::commit(gsl::not_null<const void*> cookie,
                                     uint32_t opaque,
                                     Vbid vbucket,
                                     const DocKey& key,
                                     uint64_t prepared_seqno,
                                     uint64_t commit_seqno) {
    return the_engine_dcp->commit(
            cookie, opaque, vbucket, key, prepared_seqno, commit_seqno);
}

ENGINE_ERROR_CODE MockEngine::abort(gsl::not_null<const void*> cookie,
                                    uint32_t opaque,
                                    Vbid vbucket,
                                    const DocKey& key,
                                    uint64_t prepared_seqno,
                                    uint64_t abort_seqno) {
    return the_engine_dcp->abort(
            cookie, opaque, vbucket, key, prepared_seqno, abort_seqno);
}

void MockEngine::disconnect(gsl::not_null<const void*> cookie) {
    the_engine->disconnect(cookie);
}
