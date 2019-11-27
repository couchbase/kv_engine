/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include <memcached/engine_testapp.h>

#include <getopt.h>
#include <chrono>
#include <cinttypes>
#include <cstdlib>
#include <functional>
#include <map>
#include <memory>
#include <regex>
#include <string>
#include <unordered_set>
#include <vector>

#include "utilities/terminate_handler.h"
#include "mock_server.h"

#include <daemon/alloc_hooks.h>
#include <daemon/enginemap.h>
#include <logger/logger.h>
#include <memcached/dcp.h>
#include <memcached/durability_spec.h>
#include <memcached/server_cookie_iface.h>
#include <memcached/server_log_iface.h>
#include <phosphor/phosphor.h>
#include <platform/cb_malloc.h>
#include <platform/cbassert.h>
#include <platform/dirutils.h>
#include <platform/strerror.h>

struct mock_engine : public EngineIface, public DcpIface {
    ENGINE_ERROR_CODE initialize(const char* config_str) override;
    void destroy(bool force) override;

    cb::EngineErrorItemPair allocate(gsl::not_null<const void*> cookie,
                                     const DocKey& key,
                                     const size_t nbytes,
                                     const int flags,
                                     const rel_time_t exptime,
                                     uint8_t datatype,
                                     Vbid vbucket) override;
    std::pair<cb::unique_item_ptr, item_info> allocate_ex(
            gsl::not_null<const void*> cookie,
            const DocKey& key,
            size_t nbytes,
            size_t priv_nbytes,
            int flags,
            rel_time_t exptime,
            uint8_t datatype,
            Vbid vbucket) override;

    ENGINE_ERROR_CODE remove(
            gsl::not_null<const void*> cookie,
            const DocKey& key,
            uint64_t& cas,
            Vbid vbucket,
            const boost::optional<cb::durability::Requirements>& durability,
            mutation_descr_t& mut_info) override;

    void release(gsl::not_null<item*> item) override;

    cb::EngineErrorItemPair get(gsl::not_null<const void*> cookie,
                                const DocKey& key,
                                Vbid vbucket,
                                DocStateFilter documentStateFilter) override;
    cb::EngineErrorItemPair get_if(
            gsl::not_null<const void*> cookie,
            const DocKey& key,
            Vbid vbucket,
            std::function<bool(const item_info&)> filter) override;

    cb::EngineErrorMetadataPair get_meta(gsl::not_null<const void*> cookie,
                                         const DocKey& key,
                                         Vbid vbucket) override;

    cb::EngineErrorItemPair get_locked(gsl::not_null<const void*> cookie,
                                       const DocKey& key,
                                       Vbid vbucket,
                                       uint32_t lock_timeout) override;

    ENGINE_ERROR_CODE unlock(gsl::not_null<const void*> cookie,
                             const DocKey& key,
                             Vbid vbucket,
                             uint64_t cas) override;

    cb::EngineErrorItemPair get_and_touch(
            gsl::not_null<const void*> cookie,
            const DocKey& key,
            Vbid vbucket,
            uint32_t expiryTime,
            const boost::optional<cb::durability::Requirements>& durability)
            override;

    ENGINE_ERROR_CODE store(
            gsl::not_null<const void*> cookie,
            gsl::not_null<item*> item,
            uint64_t& cas,
            ENGINE_STORE_OPERATION operation,
            const boost::optional<cb::durability::Requirements>& durability,
            DocumentState document_state) override;

    ENGINE_ERROR_CODE flush(gsl::not_null<const void*> cookie) override;

    ENGINE_ERROR_CODE get_stats(gsl::not_null<const void*> cookie,
                                cb::const_char_buffer key,
                                cb::const_char_buffer value,
                                const AddStatFn& add_stat) override;

    void reset_stats(gsl::not_null<const void*> cookie) override;

    ENGINE_ERROR_CODE unknown_command(const void* cookie,
                                      const cb::mcbp::Request& request,
                                      const AddResponseFn& response) override;

    void item_set_cas(gsl::not_null<item*> item, uint64_t val) override;

    void item_set_datatype(gsl::not_null<item*> item,
                           protocol_binary_datatype_t datatype) override;

    bool get_item_info(gsl::not_null<const item*> item,
                       gsl::not_null<item_info*> item_info) override;

    cb::engine_errc set_collection_manifest(
            gsl::not_null<const void*> cookie,
            cb::const_char_buffer json) override;

    bool isXattrEnabled() override {
        return the_engine->isXattrEnabled();
    }

    BucketCompressionMode getCompressionMode() override {
        return the_engine->getCompressionMode();
    }

    size_t getMaxItemSize() override {
        return the_engine->getMaxItemSize();
    }

    float getMinCompressionRatio() override {
        return the_engine->getMinCompressionRatio();
    }

    cb::engine::FeatureSet getFeatures() override {
        return the_engine->getFeatures();
    }

    // DcpIface implementation ////////////////////////////////////////////////

    ENGINE_ERROR_CODE step(
            gsl::not_null<const void*> cookie,
            gsl::not_null<dcp_message_producers*> producers) override;

    ENGINE_ERROR_CODE open(gsl::not_null<const void*> cookie,
                           uint32_t opaque,
                           uint32_t seqno,
                           uint32_t flags,
                           cb::const_char_buffer name,
                           cb::const_char_buffer value) override;

    ENGINE_ERROR_CODE add_stream(gsl::not_null<const void*> cookie,
                                 uint32_t opaque,
                                 Vbid vbucket,
                                 uint32_t flags) override;

    ENGINE_ERROR_CODE close_stream(gsl::not_null<const void*> cookie,
                                   uint32_t opaque,
                                   Vbid vbucket,
                                   cb::mcbp::DcpStreamId sid) override;

    ENGINE_ERROR_CODE stream_req(
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
            boost::optional<cb::const_char_buffer> json) override;

    ENGINE_ERROR_CODE get_failover_log(gsl::not_null<const void*> cookie,
                                       uint32_t opaque,
                                       Vbid vbucket,
                                       dcp_add_failover_log cb) override;

    ENGINE_ERROR_CODE stream_end(gsl::not_null<const void*> cookie,
                                 uint32_t opaque,
                                 Vbid vbucket,
                                 uint32_t flags) override;

    ENGINE_ERROR_CODE snapshot_marker(
            gsl::not_null<const void*> cookie,
            uint32_t opaque,
            Vbid vbucket,
            uint64_t start_seqno,
            uint64_t end_seqno,
            uint32_t flags,
            boost::optional<uint64_t> high_completed_seqno) override;

    ENGINE_ERROR_CODE mutation(gsl::not_null<const void*> cookie,
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

    ENGINE_ERROR_CODE deletion(gsl::not_null<const void*> cookie,
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

    ENGINE_ERROR_CODE expiration(gsl::not_null<const void*> cookie,
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

    ENGINE_ERROR_CODE set_vbucket_state(gsl::not_null<const void*> cookie,
                                        uint32_t opaque,
                                        Vbid vbucket,
                                        vbucket_state_t state) override;

    ENGINE_ERROR_CODE noop(gsl::not_null<const void*> cookie,
                           uint32_t opaque) override;

    ENGINE_ERROR_CODE buffer_acknowledgement(gsl::not_null<const void*> cookie,
                                             uint32_t opaque,
                                             Vbid vbucket,
                                             uint32_t buffer_bytes) override;

    ENGINE_ERROR_CODE control(gsl::not_null<const void*> cookie,
                              uint32_t opaque,
                              cb::const_char_buffer key,
                              cb::const_char_buffer value) override;

    ENGINE_ERROR_CODE response_handler(
            gsl::not_null<const void*> cookie,
            const protocol_binary_response_header* response) override;

    ENGINE_ERROR_CODE system_event(gsl::not_null<const void*> cookie,
                                   uint32_t opaque,
                                   Vbid vbucket,
                                   mcbp::systemevent::id event,
                                   uint64_t bySeqno,
                                   mcbp::systemevent::version version,
                                   cb::const_byte_buffer key,
                                   cb::const_byte_buffer eventData) override;
    ENGINE_ERROR_CODE prepare(gsl::not_null<const void*> cookie,
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
    ENGINE_ERROR_CODE seqno_acknowledged(gsl::not_null<const void*> cookie,
                                         uint32_t opaque,
                                         Vbid vbucket,
                                         uint64_t prepared_seqno) override;
    ENGINE_ERROR_CODE commit(gsl::not_null<const void*> cookie,
                             uint32_t opaque,
                             Vbid vbucket,
                             const DocKey& key,
                             uint64_t prepared_seqno,
                             uint64_t commit_seqno) override;
    ENGINE_ERROR_CODE abort(gsl::not_null<const void*> cookie,
                            uint32_t opaque,
                            Vbid vbucket,
                            const DocKey& key,
                            uint64_t prepared_seqno,
                            uint64_t abort_seqno) override;

    EngineIface* the_engine{};

    // Pointer to DcpIface for the underlying engine we are proxying; or
    // nullptr if it doesn't implement DcpIface;
    DcpIface* the_engine_dcp = nullptr;
};

static bool color_enabled;
static bool verbose_logging = false;

// The handle for the 'current' engine, as used by execute_test.
// It needs to be globalas the testcase may call reload_engine() and that
// needs to update the pointers the new engine, so when execute_test is
// cleaning up it has the correct handle.
static EngineIface* currentEngineHandle = nullptr;

ENGINE_ERROR_CODE mock_engine::initialize(const char* config_str) {
    return the_engine->initialize(config_str);
}

void mock_engine::destroy(const bool force) {
    the_engine->destroy(force);
}

// Helper function to convert a cookie (externally represented as
// void*) to the actual internal type.
static MockCookie* to_mock_connstruct(const void* cookie) {
    return const_cast<struct MockCookie*>(
            reinterpret_cast<const struct MockCookie*>(cookie));
}

/**
 * Helper function to return a mock_connstruct, either a new one or
 * an existng one.
 **/
MockCookie* get_or_create_mock_connstruct(const void* cookie) {
    auto* c = to_mock_connstruct(cookie);
    if (c == nullptr) {
        c = to_mock_connstruct(create_mock_cookie());
    }
    return c;
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
           c->handle_ewouldblock)
    {
        ++c->nblocks;
        c->cond.wait(lock, [&c] {
            return c->num_processed_notifications != c->num_io_notifications;
        });
        c->num_processed_notifications = c->num_io_notifications;
        ret = c->status;
    }

    return ret;
}

cb::EngineErrorItemPair mock_engine::allocate(gsl::not_null<const void*> cookie,
                                              const DocKey& key,
                                              const size_t nbytes,
                                              const int flags,
                                              const rel_time_t exptime,
                                              uint8_t datatype,
                                              Vbid vbucket) {
    auto engine_fn = std::bind(&EngineIface::allocate,
                               the_engine,
                               cookie,
                               key,
                               nbytes,
                               flags,
                               exptime,
                               datatype,
                               vbucket);

    auto* c = to_mock_connstruct(cookie.get());
    return do_blocking_engine_call<cb::unique_item_ptr>(c, engine_fn);
}

std::pair<cb::unique_item_ptr, item_info> mock_engine::allocate_ex(
        gsl::not_null<const void*> cookie,
        const DocKey& key,
        const size_t nbytes,
        const size_t priv_nbytes,
        const int flags,
        const rel_time_t exptime,
        uint8_t datatype,
        Vbid vbucket) {
    auto engine_fn = std::bind(&EngineIface::allocate_ex,
                               the_engine,
                               cookie,
                               key,
                               nbytes,
                               priv_nbytes,
                               flags,
                               exptime,
                               datatype,
                               vbucket);

    auto* c = to_mock_connstruct(cookie.get());
    c->nblocks = 0;

    std::lock_guard<std::mutex> guard(c->mutex);

    try {
        return engine_fn();
    } catch (const cb::engine_error& error) {
        if (error.code() == cb::engine_errc::would_block) {
            throw std::logic_error("mock_allocate_ex: allocate_ex should not block!");
        }
        throw error;
    }
    throw std::logic_error("mock_allocate_ex: Should never get here");
}

ENGINE_ERROR_CODE mock_engine::remove(
        gsl::not_null<const void*> cookie,
        const DocKey& key,
        uint64_t& cas,
        Vbid vbucket,
        const boost::optional<cb::durability::Requirements>& durability,
        mutation_descr_t& mut_info) {
    auto engine_fn = std::bind(&EngineIface::remove,
                               the_engine,
                               cookie,
                               key,
                               std::ref(cas),
                               vbucket,
                               durability,
                               std::ref(mut_info));
    auto* construct = to_mock_connstruct(cookie.get());
    return call_engine_and_handle_EWOULDBLOCK(construct, engine_fn);
}

void mock_engine::release(gsl::not_null<item*> item) {
    the_engine->release(item);
}

cb::EngineErrorItemPair mock_engine::get(gsl::not_null<const void*> cookie,
                                         const DocKey& key,
                                         Vbid vbucket,
                                         DocStateFilter documentStateFilter) {
    auto engine_fn = std::bind(&EngineIface::get,
                               the_engine,
                               cookie,
                               key,
                               vbucket,
                               documentStateFilter);

    auto* construct = to_mock_connstruct(cookie.get());
    return do_blocking_engine_call<cb::unique_item_ptr>(construct, engine_fn);
}

cb::EngineErrorItemPair mock_engine::get_if(
        gsl::not_null<const void*> cookie,
        const DocKey& key,
        Vbid vbucket,
        std::function<bool(const item_info&)> filter) {
    auto engine_fn = std::bind(
            &EngineIface::get_if, the_engine, cookie, key, vbucket, filter);
    auto* construct = to_mock_connstruct(cookie.get());
    return do_blocking_engine_call<cb::unique_item_ptr>(construct, engine_fn);
}

cb::EngineErrorItemPair mock_engine::get_and_touch(
        gsl::not_null<const void*> cookie,
        const DocKey& key,
        Vbid vbucket,
        uint32_t expiryTime,
        const boost::optional<cb::durability::Requirements>& durability) {
    auto engine_fn = std::bind(&EngineIface::get_and_touch,
                               the_engine,
                               cookie,
                               key,
                               vbucket,
                               expiryTime,
                               durability);

    auto* construct = to_mock_connstruct(cookie.get());
    return do_blocking_engine_call<cb::unique_item_ptr>(construct, engine_fn);
}

cb::EngineErrorItemPair mock_engine::get_locked(
        gsl::not_null<const void*> cookie,
        const DocKey& key,
        Vbid vbucket,
        uint32_t lock_timeout) {
    auto engine_fn = std::bind(&EngineIface::get_locked,
                               the_engine,
                               cookie,
                               key,
                               vbucket,
                               lock_timeout);
    auto* construct = to_mock_connstruct(cookie.get());

    return do_blocking_engine_call<cb::unique_item_ptr>(construct, engine_fn);
}

cb::EngineErrorMetadataPair mock_engine::get_meta(
        gsl::not_null<const void*> cookie, const DocKey& key, Vbid vbucket) {
    auto engine_fn =
            std::bind(&EngineIface::get_meta, the_engine, cookie, key, vbucket);

    auto* construct = to_mock_connstruct(cookie.get());

    return do_blocking_engine_call<item_info>(construct, engine_fn);
}

ENGINE_ERROR_CODE mock_engine::unlock(gsl::not_null<const void*> cookie,
                                      const DocKey& key,
                                      Vbid vbucket,
                                      uint64_t cas) {
    auto engine_fn = std::bind(
            &EngineIface::unlock, the_engine, cookie, key, vbucket, cas);

    auto* construct = to_mock_connstruct(cookie.get());
    return call_engine_and_handle_EWOULDBLOCK(construct, engine_fn);
}

ENGINE_ERROR_CODE mock_engine::get_stats(gsl::not_null<const void*> cookie,
                                         cb::const_char_buffer key,
                                         cb::const_char_buffer value,
                                         const AddStatFn& add_stat) {
    auto engine_fn = std::bind(
            &EngineIface::get_stats, the_engine, cookie, key, value, add_stat);

    auto* construct = to_mock_connstruct(cookie.get());
    return call_engine_and_handle_EWOULDBLOCK(construct, engine_fn);
}

ENGINE_ERROR_CODE mock_engine::store(
        gsl::not_null<const void*> cookie,
        gsl::not_null<item*> item,
        uint64_t& cas,
        ENGINE_STORE_OPERATION operation,
        const boost::optional<cb::durability::Requirements>& durability,
        DocumentState document_state) {
    auto engine_fn = std::bind(&EngineIface::store,
                               the_engine,
                               cookie,
                               item,
                               std::ref(cas),
                               operation,
                               durability,
                               document_state);

    auto* construct = to_mock_connstruct(cookie.get());

    return call_engine_and_handle_EWOULDBLOCK(construct, engine_fn);
}

ENGINE_ERROR_CODE mock_engine::flush(gsl::not_null<const void*> cookie) {
    auto engine_fn = std::bind(&EngineIface::flush, the_engine, cookie);

    auto* construct = to_mock_connstruct(cookie.get());
    return call_engine_and_handle_EWOULDBLOCK(construct, engine_fn);
}

void mock_engine::reset_stats(gsl::not_null<const void*> cookie) {
    the_engine->reset_stats(cookie);
}

ENGINE_ERROR_CODE mock_engine::unknown_command(const void* cookie,
                                               const cb::mcbp::Request& request,
                                               const AddResponseFn& response) {
    auto* c = get_or_create_mock_connstruct(cookie);
    auto engine_fn = std::bind(&EngineIface::unknown_command,
                               the_engine,
                               static_cast<const void*>(c),
                               std::cref(request),
                               response);

    ENGINE_ERROR_CODE ret = call_engine_and_handle_EWOULDBLOCK(c, engine_fn);

    check_and_destroy_mock_connstruct(c, cookie);
    return ret;
}

void mock_engine::item_set_cas(gsl::not_null<item*> item, uint64_t val) {
    the_engine->item_set_cas(item, val);
}

void mock_engine::item_set_datatype(gsl::not_null<item*> item,
                                    protocol_binary_datatype_t datatype) {
    the_engine->item_set_datatype(item, datatype);
}

bool mock_engine::get_item_info(gsl::not_null<const item*> item,
                                gsl::not_null<item_info*> item_info) {
    return the_engine->get_item_info(item, item_info);
}

cb::engine_errc mock_engine::set_collection_manifest(
        gsl::not_null<const void*> cookie, cb::const_char_buffer json) {
    return the_engine->set_collection_manifest(cookie, json);
}

ENGINE_ERROR_CODE mock_engine::step(
        gsl::not_null<const void*> cookie,
        gsl::not_null<dcp_message_producers*> producers) {
    return the_engine_dcp->step(cookie, producers);
}

ENGINE_ERROR_CODE mock_engine::open(gsl::not_null<const void*> cookie,
                                    uint32_t opaque,
                                    uint32_t seqno,
                                    uint32_t flags,
                                    cb::const_char_buffer name,
                                    cb::const_char_buffer value) {
    return the_engine_dcp->open(cookie, opaque, seqno, flags, name, value);
}

ENGINE_ERROR_CODE mock_engine::add_stream(gsl::not_null<const void*> cookie,
                                          uint32_t opaque,
                                          Vbid vbucket,
                                          uint32_t flags) {
    auto* c = get_or_create_mock_connstruct(cookie);
    auto engine_fn = std::bind(&DcpIface::add_stream,
                               the_engine_dcp,
                               static_cast<const void*>(c),
                               opaque,
                               vbucket,
                               flags);

    ENGINE_ERROR_CODE ret = call_engine_and_handle_EWOULDBLOCK(c, engine_fn);

    check_and_destroy_mock_connstruct(c, cookie);
    return ret;
}

ENGINE_ERROR_CODE mock_engine::close_stream(gsl::not_null<const void*> cookie,
                                            uint32_t opaque,
                                            Vbid vbucket,
                                            cb::mcbp::DcpStreamId sid) {
    return the_engine_dcp->close_stream(cookie, opaque, vbucket, sid);
}

ENGINE_ERROR_CODE mock_engine::stream_req(
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

ENGINE_ERROR_CODE mock_engine::get_failover_log(
        gsl::not_null<const void*> cookie,
        uint32_t opaque,
        Vbid vbucket,
        dcp_add_failover_log cb) {
    return the_engine_dcp->get_failover_log(cookie, opaque, vbucket, cb);
}

ENGINE_ERROR_CODE mock_engine::stream_end(gsl::not_null<const void*> cookie,
                                          uint32_t opaque,
                                          Vbid vbucket,
                                          uint32_t flags) {
    return the_engine_dcp->stream_end(cookie, opaque, vbucket, flags);
}

ENGINE_ERROR_CODE mock_engine::snapshot_marker(
        gsl::not_null<const void*> cookie,
        uint32_t opaque,
        Vbid vbucket,
        uint64_t start_seqno,
        uint64_t end_seqno,
        uint32_t flags,
        boost::optional<uint64_t> high_completed_seqno) {
    return the_engine_dcp->snapshot_marker(cookie,
                                           opaque,
                                           vbucket,
                                           start_seqno,
                                           end_seqno,
                                           flags,
                                           high_completed_seqno);
}

ENGINE_ERROR_CODE mock_engine::mutation(gsl::not_null<const void*> cookie,
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
    auto* c = get_or_create_mock_connstruct(cookie);
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

    ENGINE_ERROR_CODE ret = call_engine_and_handle_EWOULDBLOCK(c, engine_fn);

    check_and_destroy_mock_connstruct(c, cookie);
    return ret;
}

ENGINE_ERROR_CODE mock_engine::deletion(gsl::not_null<const void*> cookie,
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
    auto* c = get_or_create_mock_connstruct(cookie);
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

    ENGINE_ERROR_CODE ret = call_engine_and_handle_EWOULDBLOCK(c, engine_fn);

    check_and_destroy_mock_connstruct(c, cookie);
    return ret;
}

ENGINE_ERROR_CODE mock_engine::expiration(gsl::not_null<const void*> cookie,
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
    auto* c = get_or_create_mock_connstruct(cookie);
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

    ENGINE_ERROR_CODE ret = call_engine_and_handle_EWOULDBLOCK(c, engine_fn);

    check_and_destroy_mock_connstruct(c, cookie);
    return ret;
}

ENGINE_ERROR_CODE mock_engine::set_vbucket_state(
        gsl::not_null<const void*> cookie,
        uint32_t opaque,
        Vbid vbucket,
        vbucket_state_t state) {
    return the_engine_dcp->set_vbucket_state(cookie, opaque, vbucket, state);
}

ENGINE_ERROR_CODE mock_engine::noop(gsl::not_null<const void*> cookie,
                                    uint32_t opaque) {
    return the_engine_dcp->noop(cookie, opaque);
}

ENGINE_ERROR_CODE mock_engine::control(gsl::not_null<const void*> cookie,
                                       uint32_t opaque,
                                       cb::const_char_buffer key,
                                       cb::const_char_buffer value) {
    return the_engine_dcp->control(cookie, opaque, key, value);
}

ENGINE_ERROR_CODE mock_engine::buffer_acknowledgement(
        gsl::not_null<const void*> cookie,
        uint32_t opaque,
        Vbid vbucket,
        uint32_t buffer_bytes) {
    return the_engine_dcp->buffer_acknowledgement(
            cookie, opaque, vbucket, buffer_bytes);
}

ENGINE_ERROR_CODE mock_engine::response_handler(
        gsl::not_null<const void*> cookie,
        const protocol_binary_response_header* response) {
    return the_engine_dcp->response_handler(cookie, response);
}

ENGINE_ERROR_CODE mock_engine::system_event(gsl::not_null<const void*> cookie,
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

ENGINE_ERROR_CODE mock_engine::prepare(gsl::not_null<const void*> cookie,
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

ENGINE_ERROR_CODE mock_engine::seqno_acknowledged(
        gsl::not_null<const void*> cookie,
        uint32_t opaque,
        Vbid vbucket,
        uint64_t prepared_seqno) {
    return the_engine_dcp->seqno_acknowledged(
            cookie, opaque, vbucket, prepared_seqno);
}

ENGINE_ERROR_CODE mock_engine::commit(gsl::not_null<const void*> cookie,
                                      uint32_t opaque,
                                      Vbid vbucket,
                                      const DocKey& key,
                                      uint64_t prepared_seqno,
                                      uint64_t commit_seqno) {
    return the_engine_dcp->commit(
            cookie, opaque, vbucket, key, prepared_seqno, commit_seqno);
}

ENGINE_ERROR_CODE mock_engine::abort(gsl::not_null<const void*> cookie,
                                     uint32_t opaque,
                                     Vbid vbucket,
                                     const DocKey& key,
                                     uint64_t prepared_seqno,
                                     uint64_t abort_seqno) {
    return the_engine_dcp->abort(
            cookie, opaque, vbucket, key, prepared_seqno, abort_seqno);
}

static void usage() {
    printf("\n");
    printf("engine_testapp -E <ep|mc>\n");
    printf("               [-e <engine_config>] [-h] [-X]\n");
    printf("\n");
    printf("-E <ep|mc>                   The engine to use.\n");
    printf("                               ep = ep-engine\n");
    printf("                               mc = default/memcache\n");
    printf("\n");
    printf("-a <attempts>                Maximum number of attempts for a test.\n");
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

static int report_test(const char* name,
                       std::chrono::steady_clock::duration duration,
                       enum test_result r,
                       bool quiet,
                       bool compact) {
    int rc = 0;
    const char* msg = nullptr;
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
    case PENDING:
        color = 33;
        msg = "PENDING";
        break;
    case SUCCESS_AFTER_RETRY:
        msg="OK AFTER RETRY";
        color = 33;
        break;
    case SKIPPED_UNDER_ROCKSDB:
        msg="SKIPPED_UNDER_ROCKSDB";
        color = 32;
        break;
    case SKIPPED_UNDER_MAGMA:
        msg = "SKIPPED_UNDER_MAGMA";
        color = 32;
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

    auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(duration);
    if (quiet) {
        if (r != SUCCESS) {
            printf("%s:  (%" PRIu64 " ms) %s%s%s\n", name, duration_ms.count(),
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
            printf("(%" PRIu64 " ms) %s%s%s\n", duration_ms.count(), color_str,
                   msg, reset_color);
        }
    }
    return rc;
}

class MockTestHarness : public test_harness {
public:
    const void* create_cookie() override {
        return create_mock_cookie();
    }

    void destroy_cookie(const void* cookie) override {
        destroy_mock_cookie(cookie);
    }

    void set_ewouldblock_handling(const void* cookie, bool enable) override {
        mock_set_ewouldblock_handling(cookie, enable);
    }

    void set_mutation_extras_handling(const void* cookie,
                                      bool enable) override {
        mock_set_mutation_extras_handling(cookie, enable);
    }

    void set_datatype_support(const void* cookie,
                              protocol_binary_datatype_t datatypes) override {
        mock_set_datatype_support(cookie, datatypes);
    }

    void set_collections_support(const void* cookie, bool enable) override {
        mock_set_collections_support(cookie, enable);
    }

    void lock_cookie(const void* cookie) override {
        lock_mock_cookie(cookie);
    }

    void unlock_cookie(const void* cookie) override {
        unlock_mock_cookie(cookie);
    }

    void waitfor_cookie(const void* cookie) override {
        waitfor_mock_cookie(cookie);
    }

    void store_engine_specific(const void* cookie, void* engine_data) override {
        get_mock_server_api()->cookie->store_engine_specific(cookie,
                                                             engine_data);
    }

    int get_number_of_mock_cookie_references(const void* cookie) override {
        return ::get_number_of_mock_cookie_references(cookie);
    }
    void set_pre_link_function(PreLinkFunction function) override {
        mock_set_pre_link_function(function);
    }

    void time_travel(int offset) override {
        mock_time_travel(offset);
    }

    void set_current_testcase(engine_test_t* testcase) {
        current_testcase = testcase;
    }

    const engine_test_t* get_current_testcase() override {
        return current_testcase;
    }

    void release_free_memory() override {
        get_mock_server_api()->alloc_hooks->release_free_memory();
    }

    EngineIface* create_bucket(bool initialize, const char* cfg) override {
        auto me = std::make_unique<mock_engine>();
        EngineIface* handle = new_engine_instance(
                bucketType, "engine_testapp", &get_mock_server_api);

        if (handle) {
            me->the_engine = (EngineIface*)handle;
            me->the_engine_dcp = dynamic_cast<DcpIface*>(handle);
            if (initialize) {
                const auto error = me->the_engine->initialize(cfg);
                if (error != ENGINE_SUCCESS) {
                    me->the_engine->destroy(false /*force*/);
                    cb::engine_error err{cb::engine_errc(error),
                                         "Failed to initialize instance"};
                    throw err;
                }
            }
        }
        return me.release();
    }

    void destroy_bucket(EngineIface* handle, bool force) override {
        handle->destroy(force);
        delete handle;
    }

    void reload_engine(EngineIface** h,
                       const char* cfg,
                       bool init,
                       bool force) override {
        disconnect_all_mock_connections();
        destroy_bucket(*h, force);
        destroy_mock_event_callbacks();
        currentEngineHandle = *h = create_bucket(init, cfg);
    }

    void notify_io_complete(const void* cookie,
                            ENGINE_ERROR_CODE status) override {
        get_mock_server_api()->cookie->notify_io_complete(cookie, status);
    }

private:
    engine_test_t* current_testcase = nullptr;
};

MockTestHarness harness;

static test_result execute_test(engine_test_t test,
                                const char* engine,
                                const char* default_cfg) {
    enum test_result ret = PENDING;
    cb_assert(test.tfun != nullptr || test.api_v2.tfun != nullptr);
    bool test_api_1 = test.tfun != nullptr;

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

            while (!cfg.empty() &&
                   (i = cfg.find(delimiter)) != std::string::npos) {
                std::string temp(cfg.substr(0, i));
                cfg.erase(0, i + 1);
                j = temp.find('=');
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
    // Necessary configuration to run tests under RocksDB.
    if (test.cfg != nullptr) {
        cfg.assign(test.cfg);
        if (std::string(test.cfg).find("backend=rocksdb") !=
            std::string::npos) {
            if (!cfg.empty() && cfg.back() != ';') {
                cfg.append(";");
            }
            // MB-26973: Disable RocksDB pre-allocation of disk space by
            // default. When 'allow_fallocate=true', RocksDB pre-allocates disk
            // space for the MANIFEST and WAL files (some tests showed up to
            // ~75MB per DB, ~7.5GB for 100 empty DBs created).
            cfg.append("rocksdb_options=allow_fallocate=false;");
            // BucketQuota is now used to calculate the MemtablesQuota at
            // runtime. The baseline value for BucketQuota is taken from the
            // 'max_size' default value in configuration.json. If that default
            // value is 0, then EPEngine sets the value to 'size_t::max()',
            // leading to a huge MemtablesQuota. Avoid that 'size_t::max()' is
            // used in the computation for MemtablesQuota.
            if (cfg.find("max_size") == std::string::npos) {
                cfg.append("max_size=1073741824;");
            }
            test.cfg = cfg.c_str();
        }
    }

    harness.set_current_testcase(&test);
    if (test.prepare != nullptr) {
        if ((ret = test.prepare(&test)) == SUCCESS) {
            ret = PENDING;
        }
    }

    if (ret == PENDING) {
        init_mock_server();

        const auto spd_log_level =
                verbose_logging ? spdlog::level::level_enum::debug
                                : spdlog::level::level_enum::critical;
        get_mock_server_api()->log->set_level(spd_log_level);
        get_mock_server_api()->log->get_spdlogger()->set_level(spd_log_level);

        if (test_api_1) {
            // all test (API1) get 1 bucket and they are welcome to ask for more.
            currentEngineHandle = harness.create_bucket(
                    true, test.cfg ? test.cfg : default_cfg);
            if (test.test_setup != nullptr &&
                !test.test_setup(currentEngineHandle)) {
                fprintf(stderr, "Failed to run setup for test %s\n", test.name);
                return FAIL;
            }

            ret = test.tfun(currentEngineHandle);

            if (test.test_teardown != nullptr &&
                !test.test_teardown(currentEngineHandle)) {
                fprintf(stderr, "WARNING: Failed to run teardown for test %s\n", test.name);
            }

        } else {
            if (test.api_v2.test_setup != nullptr &&
                !test.api_v2.test_setup(&test)) {
                fprintf(stderr, "Failed to run setup for test %s\n", test.name);
                return FAIL;
            }


            ret = test.api_v2.tfun(&test);

            if (test.api_v2.test_teardown != nullptr &&
                !test.api_v2.test_teardown(&test)) {
                fprintf(stderr, "WARNING: Failed to run teardown for test %s\n", test.name);
            }
        }

        if (currentEngineHandle) {
            harness.destroy_bucket(currentEngineHandle, false);
            currentEngineHandle = nullptr;
        }

        destroy_mock_event_callbacks();
        shutdown_all_engines();
        PHOSPHOR_INSTANCE.stop();

        if (test.cleanup) {
            test.cleanup(&test, ret);
        }
    }

    return ret;
}

int main(int argc, char **argv) {
    int c, exitcode = 0, num_cases = 0, loop_count = 0;
    bool verbose = false;
    bool quiet = false;
    bool dot = false;
    bool loop = false;
    bool terminate_on_error = false;
    std::string engine;
    const char* engine_args = nullptr;
    std::unique_ptr<std::regex> test_case_regex;
    engine_test_t* testcases = nullptr;
    int test_case_id = -1;

    /* If a testcase fails, retry up to 'attempts -1' times to allow it
       to pass - this is here to allow us to deal with intermittant
       test failures without having to manually retry the whole
       job. */
    int attempts = 1;

    cb::logger::createConsoleLogger();
    cb_initialize_sockets();

    AllocHooks::initialize();

    auto limit = cb::io::maximizeFileDescriptors(1024);
    if (limit < 1024) {
        std::cerr << "Error: The unit tests needs at least 1k file descriptors"
                  << std::endl;
        exit(EXIT_FAILURE);
    }

    color_enabled = getenv("TESTAPP_ENABLE_COLOR") != nullptr;

    /* Allow 'attempts' to also be set via env variable - this allows
       commit-validation scripts to enable retries for all
       engine_testapp-driven tests trivually. */
    const char* attempts_env;
    if ((attempts_env = getenv("TESTAPP_ATTEMPTS")) != nullptr) {
        attempts = std::stoi(attempts_env);
    }

    /* Use unbuffered stdio */
    setbuf(stdout, nullptr);
    setbuf(stderr, nullptr);

    install_backtrace_terminate_handler();

    /* process arguments */
    while ((c = getopt(
                    argc,
                    argv,
                    "a:" /* attempt tests N times before declaring them failed
                          */
                    "h" /* usage */
                    "E:" /* Engine to use */
                    "e:" /* Engine options */
                    "L" /* Loop until failure */
                    "q" /* Be more quiet (only report failures) */
                    "." /* dot mode. */
                    "n:" /* regex for test case(s) to run */
                    "v" /* verbose output */
                    "Z" /* Terminate on first error */
                    "C:" /* Test case id */
                    "s" /* spinlock the program */
                    "X" /* Use stderr logger */
                    "f:" /* output format. Valid values are: 'text' and 'xml' */
                    )) != -1) {
        switch (c) {
        case 'a':
            attempts = std::stoi(optarg);
            break;
        case 's' : {
            int spin = 1;
            while (spin) {

            }
            break;
        }
        case 'C' :
            test_case_id = std::stoi(optarg);
            break;
        case 'E':
            engine = optarg;
            break;
        case 'e':
            engine_args = optarg;
            break;
        case 'f':
            if (std::string(optarg) == "text") {
                harness.output_format = OutputFormat::Text;
            } else if (std::string(optarg) == "xml") {
                harness.output_format = OutputFormat::XML;
            } else {
                fprintf(stderr, "Invalid option for output format '%s'. Valid "
                    "options are 'text' and 'xml'.\n", optarg);
                return 1;
            }
            break;
        case 'h':
            usage();
            return 0;
        case 'L':
            loop = true;
            break;
        case 'n':
            test_case_regex = std::make_unique<std::regex>(optarg);
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
            verbose_logging = true;
            break;
        default:
            fprintf(stderr, "Illegal argument \"%c\"\n", c);
            return 1;
        }
    }

    /* validate args */
    if (engine.empty()) {
        fprintf(stderr, "-E <ep|mc> is a required parameter.\n");
        return 1;
    }

    if (engine == "ep") {
        harness.bucketType = BucketType::Couchstore;
    } else if (engine == "mc") {
        harness.bucketType = BucketType::Memcached;
    } else {
        fprintf(stderr, R"(Engine must be "ep" or "mc"\n)");
        return 1;
    }

    testcases = get_tests();

    /* set up the suite if needed */
    harness.default_engine_cfg = engine_args;

    /* Check to see whether the config string string sets the bucket type. */
    if (harness.default_engine_cfg != nullptr) {
        std::regex bucket_type("bucket_type=(\\w+)",
                               std::regex_constants::ECMAScript);
        std::cmatch matches;
        if (std::regex_search(
                    harness.default_engine_cfg, matches, bucket_type)) {
            harness.bucket_type = matches.str(1);
        }
    }

    for (num_cases = 0; testcases[num_cases].name; num_cases++) {
        /* Just counting */
    }

    if (!setup_suite(&harness)) {
        std::cerr << "Failed to set up test suite" << std::endl;
        return 1;
    }

    do {
        int i;
        bool need_newline = false;
        for (i = 0; testcases[i].name; i++) {
            // If a specific test was chosen, skip all other tests.
            if (test_case_id != -1 && i != test_case_id) {
                continue;
            }

            int error = 0;
            if (test_case_regex && !std::regex_search(testcases[i].name,
                                                      *test_case_regex)) {
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

            {
                enum test_result ecode = FAIL;

                for (int attempt = 0;
                     (attempt < attempts) && ((ecode != SUCCESS) &&
                                              (ecode != SUCCESS_AFTER_RETRY));
                     attempt++) {
                    auto start = std::chrono::steady_clock::now();
                    if (testcases[i].tfun || testcases[i].api_v2.tfun) {
                        // check there's a test to run, some modules need
                        // cleaning up of dead tests if all modules are fixed,
                        // this else if can be removed.
                        try {
                            ecode = execute_test(
                                    testcases[i], engine.c_str(), engine_args);
                        } catch (const TestExpectationFailed&) {
                            ecode = FAIL;
                        } catch (const std::exception& e) {
                            fprintf(stderr,
                                    "Uncaught std::exception. what():%s\n",
                                    e.what());
                            ecode = DIED;
                        } catch (...) {
                            // This is a non-test exception (i.e. not an
                            // explicit test check which failed) - mark as
                            // "died".
                            ecode = DIED;
                        }
                    } else {
                        ecode = PENDING; // ignored tests would always return
                                         // PENDING
                    }
                    auto stop = std::chrono::steady_clock::now();

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

    // tear down the suite if needed
    if (!teardown_suite()) {
        std::cerr << "Failed to teardown test suite" << std::endl;
    }

    printf("# Passed %d of %d tests\n", num_cases - exitcode, num_cases);

    return exitcode;
}
