/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <memcached/dcp.h>
#include <memcached/engine.h>

#pragma once
struct MockEngine : public EngineIface, public DcpIface {
    explicit MockEngine(unique_engine_ptr e)
        : the_engine(std::move(e)),
          the_engine_dcp(dynamic_cast<DcpIface*>(the_engine.get())) {
    }

    cb::engine_errc initialize(const char* config_str) override;
    void destroy(bool force) override;
    void disconnect(gsl::not_null<const CookieIface*> cookie) override;
    std::pair<cb::unique_item_ptr, item_info> allocateItem(
            gsl::not_null<const CookieIface*> cookie,
            const DocKey& key,
            size_t nbytes,
            size_t priv_nbytes,
            int flags,
            rel_time_t exptime,
            uint8_t datatype,
            Vbid vbucket) override;

    cb::engine_errc remove(
            gsl::not_null<const CookieIface*> cookie,
            const DocKey& key,
            uint64_t& cas,
            Vbid vbucket,
            const std::optional<cb::durability::Requirements>& durability,
            mutation_descr_t& mut_info) override;

    void release(gsl::not_null<ItemIface*> item) override;

    cb::EngineErrorItemPair get(gsl::not_null<const CookieIface*> cookie,
                                const DocKey& key,
                                Vbid vbucket,
                                DocStateFilter documentStateFilter) override;
    cb::EngineErrorItemPair get_if(
            gsl::not_null<const CookieIface*> cookie,
            const DocKey& key,
            Vbid vbucket,
            std::function<bool(const item_info&)> filter) override;

    cb::EngineErrorMetadataPair get_meta(
            gsl::not_null<const CookieIface*> cookie,
            const DocKey& key,
            Vbid vbucket) override;

    cb::EngineErrorItemPair get_locked(gsl::not_null<const CookieIface*> cookie,
                                       const DocKey& key,
                                       Vbid vbucket,
                                       uint32_t lock_timeout) override;

    cb::engine_errc unlock(gsl::not_null<const CookieIface*> cookie,
                           const DocKey& key,
                           Vbid vbucket,
                           uint64_t cas) override;

    cb::EngineErrorItemPair get_and_touch(
            gsl::not_null<const CookieIface*> cookie,
            const DocKey& key,
            Vbid vbucket,
            uint32_t expiryTime,
            const std::optional<cb::durability::Requirements>& durability)
            override;

    cb::engine_errc store(
            gsl::not_null<const CookieIface*> cookie,
            gsl::not_null<ItemIface*> item,
            uint64_t& cas,
            StoreSemantics operation,
            const std::optional<cb::durability::Requirements>& durability,
            DocumentState document_state,
            bool preserveTtl) override;

    cb::EngineErrorCasPair store_if(
            gsl::not_null<const CookieIface*> cookie,
            gsl::not_null<ItemIface*> item,
            uint64_t cas,
            StoreSemantics operation,
            const cb::StoreIfPredicate& predicate,
            const std::optional<cb::durability::Requirements>& durability,
            DocumentState document_state,
            bool preserveTtl) override;

    cb::engine_errc flush(gsl::not_null<const CookieIface*> cookie) override;

    cb::engine_errc get_stats(gsl::not_null<const CookieIface*> cookie,
                              std::string_view key,
                              std::string_view value,
                              const AddStatFn& add_stat) override;

    void reset_stats(gsl::not_null<const CookieIface*> cookie) override;

    cb::engine_errc unknown_command(const CookieIface* cookie,
                                    const cb::mcbp::Request& request,
                                    const AddResponseFn& response) override;

    void item_set_cas(gsl::not_null<ItemIface*> item, uint64_t val) override;

    void item_set_datatype(gsl::not_null<ItemIface*> item,
                           protocol_binary_datatype_t datatype) override;

    bool get_item_info(gsl::not_null<const ItemIface*> item,
                       gsl::not_null<item_info*> item_info) override;

    cb::engine_errc set_collection_manifest(
            gsl::not_null<const CookieIface*> cookie,
            std::string_view json) override;

    cb::engine_errc get_collection_manifest(
            gsl::not_null<const CookieIface*> cookie,
            const AddResponseFn& response) override;

    cb::EngineErrorGetCollectionIDResult get_collection_id(
            gsl::not_null<const CookieIface*> cookie,
            std::string_view path) override;

    cb::EngineErrorGetScopeIDResult get_scope_id(
            gsl::not_null<const CookieIface*> cookie,
            std::string_view path) override;

    cb::EngineErrorGetScopeIDResult get_scope_id(
            gsl::not_null<const CookieIface*> cookie,
            const DocKey& key,
            std::optional<Vbid> vbid) const override;

    bool isXattrEnabled() override {
        return the_engine->isXattrEnabled();
    }

    cb::HlcTime getVBucketHlcNow(Vbid vbucket) override {
        return the_engine->getVBucketHlcNow(vbucket);
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

    cb::engine_errc step(gsl::not_null<const CookieIface*> cookie,
                         DcpMessageProducersIface& producers) override;

    cb::engine_errc open(gsl::not_null<const CookieIface*> cookie,
                         uint32_t opaque,
                         uint32_t seqno,
                         uint32_t flags,
                         std::string_view name,
                         std::string_view value) override;

    cb::engine_errc add_stream(gsl::not_null<const CookieIface*> cookie,
                               uint32_t opaque,
                               Vbid vbucket,
                               uint32_t flags) override;

    cb::engine_errc close_stream(gsl::not_null<const CookieIface*> cookie,
                                 uint32_t opaque,
                                 Vbid vbucket,
                                 cb::mcbp::DcpStreamId sid) override;

    cb::engine_errc stream_req(gsl::not_null<const CookieIface*> cookie,
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

    cb::engine_errc get_failover_log(gsl::not_null<const CookieIface*> cookie,
                                     uint32_t opaque,
                                     Vbid vbucket,
                                     dcp_add_failover_log cb) override;

    cb::engine_errc stream_end(gsl::not_null<const CookieIface*> cookie,
                               uint32_t opaque,
                               Vbid vbucket,
                               cb::mcbp::DcpStreamEndStatus status) override;

    cb::engine_errc snapshot_marker(
            gsl::not_null<const CookieIface*> cookie,
            uint32_t opaque,
            Vbid vbucket,
            uint64_t start_seqno,
            uint64_t end_seqno,
            uint32_t flags,
            std::optional<uint64_t> high_completed_seqno,
            std::optional<uint64_t> max_visible_seqno) override;
    cb::engine_errc mutation(gsl::not_null<const CookieIface*> cookie,
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

    cb::engine_errc deletion(gsl::not_null<const CookieIface*> cookie,
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

    cb::engine_errc expiration(gsl::not_null<const CookieIface*> cookie,
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

    cb::engine_errc set_vbucket_state(gsl::not_null<const CookieIface*> cookie,
                                      uint32_t opaque,
                                      Vbid vbucket,
                                      vbucket_state_t state) override;

    cb::engine_errc noop(gsl::not_null<const CookieIface*> cookie,
                         uint32_t opaque) override;

    cb::engine_errc buffer_acknowledgement(
            gsl::not_null<const CookieIface*> cookie,
            uint32_t opaque,
            Vbid vbucket,
            uint32_t buffer_bytes) override;

    cb::engine_errc control(gsl::not_null<const CookieIface*> cookie,
                            uint32_t opaque,
                            std::string_view key,
                            std::string_view value) override;

    cb::engine_errc response_handler(
            gsl::not_null<const CookieIface*> cookie,
            const cb::mcbp::Response& response) override;

    cb::engine_errc system_event(gsl::not_null<const CookieIface*> cookie,
                                 uint32_t opaque,
                                 Vbid vbucket,
                                 mcbp::systemevent::id event,
                                 uint64_t bySeqno,
                                 mcbp::systemevent::version version,
                                 cb::const_byte_buffer key,
                                 cb::const_byte_buffer eventData) override;
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
                            cb::durability::Level level) override;
    cb::engine_errc seqno_acknowledged(gsl::not_null<const CookieIface*> cookie,
                                       uint32_t opaque,
                                       Vbid vbucket,
                                       uint64_t prepared_seqno) override;
    cb::engine_errc commit(gsl::not_null<const CookieIface*> cookie,
                           uint32_t opaque,
                           Vbid vbucket,
                           const DocKey& key,
                           uint64_t prepared_seqno,
                           uint64_t commit_seqno) override;
    cb::engine_errc abort(gsl::not_null<const CookieIface*> cookie,
                          uint32_t opaque,
                          Vbid vbucket,
                          const DocKey& key,
                          uint64_t prepared_seqno,
                          uint64_t abort_seqno) override;

    cb::engine_errc setParameter(gsl::not_null<const CookieIface*> cookie,
                                 EngineParamCategory category,
                                 std::string_view key,
                                 std::string_view value,
                                 Vbid vbucket) override;
    cb::engine_errc compactDatabase(gsl::not_null<const CookieIface*> cookie,
                                    Vbid vbid,
                                    uint64_t purge_before_ts,
                                    uint64_t purge_before_seq,
                                    bool drop_deletes) override;
    std::pair<cb::engine_errc, vbucket_state_t> getVBucket(
            gsl::not_null<const CookieIface*> cookie, Vbid vbid) override;
    cb::engine_errc setVBucket(gsl::not_null<const CookieIface*> cookie,
                               Vbid vbid,
                               uint64_t cas,
                               vbucket_state_t state,
                               nlohmann::json* meta) override;
    cb::engine_errc deleteVBucket(gsl::not_null<const CookieIface*> cookie,
                                  Vbid vbid,
                                  bool sync) override;
    unique_engine_ptr the_engine;

    // Pointer to DcpIface for the underlying engine we are proxying; or
    // nullptr if it doesn't implement DcpIface;
    DcpIface* the_engine_dcp = nullptr;
};
