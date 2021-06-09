/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "nobucket_public.h"

#include <memcached/dcp.h>
#include <memcached/durability_spec.h>
#include <memcached/engine.h>
#include <memcached/server_log_iface.h>

#include <cstdlib>
#include <stdexcept>
#include <string>

/**
 * The NoBucket is a bucket that just returns "cb::engine_errc::no_bucket" for
 * "all" operations. The main purpose for having this bucket is to reduce the
 * code complexity so we don't have to had checks all over the place to
 * see if a client is connected to a bucket or not (we may just associate
 * the connection to this bucket and it'll handle the appropriate
 * command.
 */
class NoBucket : public EngineIface, public DcpIface {
public:
    cb::engine_errc initialize(const char* config_str) override {
        return cb::engine_errc::success;
    }

    void destroy(bool) override {
        delete this;
    }

    std::pair<cb::unique_item_ptr, item_info> allocateItem(
            gsl::not_null<const CookieIface*> cookie,
            const DocKey& key,
            size_t nbytes,
            size_t priv_nbytes,
            int flags,
            rel_time_t exptime,
            uint8_t datatype,
            Vbid vbucket) override {
        throw cb::engine_error(cb::engine_errc::no_bucket, "no bucket");
    }

    cb::engine_errc remove(
            gsl::not_null<const CookieIface*>,
            const DocKey&,
            uint64_t&,
            Vbid,
            const std::optional<cb::durability::Requirements>& durability,
            mutation_descr_t&) override {
        return cb::engine_errc::no_bucket;
    }

    void release(gsl::not_null<ItemIface*>) override {
        throw std::logic_error(
                "NoBucket::item_release: no items should have"
                " been allocated from this engine");
    }

    cb::EngineErrorItemPair get(gsl::not_null<const CookieIface*>,
                                const DocKey&,
                                Vbid,
                                DocStateFilter) override {
        return cb::makeEngineErrorItemPair(cb::engine_errc::no_bucket);
    }

    cb::EngineErrorItemPair get_if(
            gsl::not_null<const CookieIface*>,
            const DocKey&,
            Vbid,
            std::function<bool(const item_info&)>) override {
        return cb::makeEngineErrorItemPair(cb::engine_errc::no_bucket);
    }

    cb::EngineErrorMetadataPair get_meta(
            gsl::not_null<const CookieIface*> cookie,
            const DocKey& key,
            Vbid vbucket) override {
        return cb::EngineErrorMetadataPair(cb::engine_errc::no_bucket, {});
    }

    cb::EngineErrorItemPair get_locked(gsl::not_null<const CookieIface*>,
                                       const DocKey&,
                                       Vbid,
                                       uint32_t) override {
        return cb::makeEngineErrorItemPair(cb::engine_errc::no_bucket);
    }

    cb::engine_errc unlock(gsl::not_null<const CookieIface*>,
                           const DocKey&,
                           Vbid,
                           uint64_t) override {
        return cb::engine_errc::no_bucket;
    }

    cb::EngineErrorItemPair get_and_touch(
            gsl::not_null<const CookieIface*> cookie,
            const DocKey&,
            Vbid,
            uint32_t,
            const std::optional<cb::durability::Requirements>&) override {
        return cb::makeEngineErrorItemPair(cb::engine_errc::no_bucket);
    }

    cb::engine_errc store(gsl::not_null<const CookieIface*>,
                          gsl::not_null<ItemIface*>,
                          uint64_t&,
                          StoreSemantics,
                          const std::optional<cb::durability::Requirements>&,
                          DocumentState,
                          bool) override {
        return cb::engine_errc::no_bucket;
    }

    cb::EngineErrorCasPair store_if(
            gsl::not_null<const CookieIface*>,
            gsl::not_null<ItemIface*>,
            uint64_t,
            StoreSemantics,
            const cb::StoreIfPredicate&,
            const std::optional<cb::durability::Requirements>&,
            DocumentState,
            bool) override {
        return {cb::engine_errc::no_bucket, 0};
    }

    cb::engine_errc flush(gsl::not_null<const CookieIface*>) override {
        return cb::engine_errc::no_bucket;
    }

    cb::engine_errc get_stats(gsl::not_null<const CookieIface*>,
                              std::string_view,
                              std::string_view,
                              const AddStatFn&) override {
        return cb::engine_errc::no_bucket;
    }

    void reset_stats(gsl::not_null<const CookieIface*> cookie) override {
    }

    cb::engine_errc unknown_command(const CookieIface*,
                                    const cb::mcbp::Request& request,
                                    const AddResponseFn&) override {
        return cb::engine_errc::no_bucket;
    }

    void item_set_cas(gsl::not_null<ItemIface*>, uint64_t) override {
        throw std::logic_error(
                "NoBucket::item_set_cas: no items should have"
                " been allocated from this engine");
    }

    void item_set_datatype(gsl::not_null<ItemIface*>,
                           protocol_binary_datatype_t) override {
        throw std::logic_error(
                "NoBucket::item_set_datatype: no items should have"
                " been allocated from this engine");
    }

    bool get_item_info(gsl::not_null<const ItemIface*>,
                       gsl::not_null<item_info*>) override {
        throw std::logic_error(
                "NoBucket::get_item_info: no items should have"
                " been allocated from this engine");
    }

    // DcpIface implementation ////////////////////////////////////////////////

    cb::engine_errc step(gsl::not_null<const CookieIface*>,
                         DcpMessageProducersIface&) override {
        return cb::engine_errc::no_bucket;
    }

    cb::engine_errc open(gsl::not_null<const CookieIface*>,
                         uint32_t,
                         uint32_t,
                         uint32_t,
                         std::string_view,
                         std::string_view) override {
        return cb::engine_errc::no_bucket;
    }

    cb::engine_errc add_stream(gsl::not_null<const CookieIface*>,
                               uint32_t,
                               Vbid,
                               uint32_t) override {
        return cb::engine_errc::no_bucket;
    }

    cb::engine_errc close_stream(gsl::not_null<const CookieIface*>,
                                 uint32_t,
                                 Vbid,
                                 cb::mcbp::DcpStreamId) override {
        return cb::engine_errc::no_bucket;
    }

    cb::engine_errc stream_req(gsl::not_null<const CookieIface*>,
                               uint32_t,
                               uint32_t,
                               Vbid,
                               uint64_t,
                               uint64_t,
                               uint64_t,
                               uint64_t,
                               uint64_t,
                               uint64_t*,
                               dcp_add_failover_log,
                               std::optional<std::string_view>) override {
        return cb::engine_errc::no_bucket;
    }

    cb::engine_errc get_failover_log(gsl::not_null<const CookieIface*>,
                                     uint32_t,
                                     Vbid,
                                     dcp_add_failover_log) override {
        return cb::engine_errc::no_bucket;
    }

    cb::engine_errc stream_end(gsl::not_null<const CookieIface*>,
                               uint32_t,
                               Vbid,
                               cb::mcbp::DcpStreamEndStatus) override {
        return cb::engine_errc::no_bucket;
    }

    cb::engine_errc snapshot_marker(gsl::not_null<const CookieIface*>,
                                    uint32_t,
                                    Vbid,
                                    uint64_t,
                                    uint64_t,
                                    uint32_t,
                                    std::optional<uint64_t>,
                                    std::optional<uint64_t>) override {
        return cb::engine_errc::no_bucket;
    }

    cb::engine_errc mutation(gsl::not_null<const CookieIface*>,
                             uint32_t,
                             const DocKey&,
                             cb::const_byte_buffer,
                             size_t,
                             uint8_t,
                             uint64_t,
                             Vbid,
                             uint32_t,
                             uint64_t,
                             uint64_t,
                             uint32_t,
                             uint32_t,
                             cb::const_byte_buffer,
                             uint8_t) override {
        return cb::engine_errc::no_bucket;
    }

    cb::engine_errc deletion(gsl::not_null<const CookieIface*>,
                             uint32_t,
                             const DocKey&,
                             cb::const_byte_buffer,
                             size_t,
                             uint8_t,
                             uint64_t,
                             Vbid,
                             uint64_t,
                             uint64_t,
                             cb::const_byte_buffer) override {
        return cb::engine_errc::no_bucket;
    }

    cb::engine_errc deletion_v2(gsl::not_null<const CookieIface*>,
                                uint32_t,
                                const DocKey&,
                                cb::const_byte_buffer,
                                size_t,
                                uint8_t,
                                uint64_t,
                                Vbid,
                                uint64_t,
                                uint64_t,
                                uint32_t) override {
        return cb::engine_errc::no_bucket;
    }

    cb::engine_errc expiration(gsl::not_null<const CookieIface*>,
                               uint32_t,
                               const DocKey&,
                               cb::const_byte_buffer,
                               size_t,
                               uint8_t,
                               uint64_t,
                               Vbid,
                               uint64_t,
                               uint64_t,
                               uint32_t) override {
        return cb::engine_errc::no_bucket;
    }

    cb::engine_errc set_vbucket_state(gsl::not_null<const CookieIface*>,
                                      uint32_t,
                                      Vbid,
                                      vbucket_state_t) override {
        return cb::engine_errc::no_bucket;
    }

    cb::engine_errc noop(gsl::not_null<const CookieIface*> cookie,
                         uint32_t opaque) override {
        return cb::engine_errc::no_bucket;
    }

    cb::engine_errc buffer_acknowledgement(gsl::not_null<const CookieIface*>,
                                           uint32_t,
                                           Vbid,
                                           uint32_t) override {
        return cb::engine_errc::no_bucket;
    }

    cb::engine_errc control(gsl::not_null<const CookieIface*>,
                            uint32_t,
                            std::string_view,
                            std::string_view) override {
        return cb::engine_errc::no_bucket;
    }

    cb::engine_errc response_handler(gsl::not_null<const CookieIface*>,
                                     const cb::mcbp::Response&) override {
        return cb::engine_errc::no_bucket;
    }

    cb::engine_errc system_event(gsl::not_null<const CookieIface*> cookie,
                                 uint32_t opaque,
                                 Vbid vbucket,
                                 mcbp::systemevent::id event,
                                 uint64_t bySeqno,
                                 mcbp::systemevent::version version,
                                 cb::const_byte_buffer key,
                                 cb::const_byte_buffer eventData) override {
        return cb::engine_errc::no_bucket;
    }

    cb::engine_errc prepare(gsl::not_null<const CookieIface*> cookie,
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
                            cb::durability::Level level) override {
        return cb::engine_errc::no_bucket;
    }
    cb::engine_errc seqno_acknowledged(gsl::not_null<const CookieIface*> cookie,
                                       uint32_t opaque,
                                       Vbid vbucket,
                                       uint64_t prepared_seqno) override {
        return cb::engine_errc::no_bucket;
    }
    cb::engine_errc commit(gsl::not_null<const CookieIface*> cookie,
                           uint32_t opaque,
                           Vbid vbucket,
                           const DocKey& key,
                           uint64_t prepared_seqno,
                           uint64_t commit_seqno) override {
        return cb::engine_errc::no_bucket;
    }
    cb::engine_errc abort(gsl::not_null<const CookieIface*> cookie,
                          uint32_t opaque,
                          Vbid vbucket,
                          const DocKey& key,
                          uint64_t prepared_seqno,
                          uint64_t abort_seqno) override {
        return cb::engine_errc::no_bucket;
    }

    cb::engine::FeatureSet getFeatures() override {
        cb::engine::FeatureSet ret;
        // To allow us to put collection connections in the NoBucket, we need to
        // pretend that it's supported.
        ret.emplace(cb::engine::Feature::Collections);
        return ret;
    }

    cb::HlcTime getVBucketHlcNow(Vbid vbucket) override {
        return {};
    }

    cb::engine_errc set_collection_manifest(
            gsl::not_null<const CookieIface*> cookie,
            std::string_view json) override {
        return cb::engine_errc::no_bucket;
    }

    cb::engine_errc get_collection_manifest(
            gsl::not_null<const CookieIface*> cookie,
            const AddResponseFn& response) override {
        return cb::engine_errc::no_bucket;
    }

    cb::EngineErrorGetCollectionIDResult get_collection_id(
            gsl::not_null<const CookieIface*> cookie,
            std::string_view path) override {
        return cb::EngineErrorGetCollectionIDResult{cb::engine_errc::no_bucket};
    }

    cb::EngineErrorGetScopeIDResult get_scope_id(
            gsl::not_null<const CookieIface*> cookie,
            std::string_view path) override {
        return cb::EngineErrorGetScopeIDResult{cb::engine_errc::no_bucket};
    }

    cb::EngineErrorGetScopeIDResult get_scope_id(
            gsl::not_null<const CookieIface*> cookie,
            const DocKey& key,
            std::optional<Vbid> vbid) const override {
        return cb::EngineErrorGetScopeIDResult(cb::engine_errc::no_bucket);
    }
};

unique_engine_ptr create_no_bucket_instance() {
    return unique_engine_ptr{new NoBucket()};
}
