/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "blob.h"
#include "configuration.h"
#include "encryption_key_provider.h"
#include "ep_engine_public.h"
#include "ep_engine_storage.h"
#include "ep_types.h"
#include "kvstore/kvstore_iface.h"
#include "memory_tracker.h"
#include "permitted_vb_states.h"
#include "stats.h"
#include "vbucket_fwd.h"

#include <cbcrypto/key_store.h>
#include <executor/taskable.h>
#include <memcached/cookie_iface.h>
#include <memcached/dcp.h>
#include <memcached/engine.h>
#include <memcached/rbac/privileges.h>
#include <platform/cb_arena_malloc_client.h>
#include <utilities/testing_hook.h>

#include <chrono>
#include <string>
#include <unordered_map>

namespace cb::mcbp {
class Request;
enum class ClientOpcode : uint8_t;
} // namespace cb::mcbp

class EPEngineGroup;
class CheckpointConfig;
struct CompactionConfig;
struct ConnCounter;
class ConnHandler;
enum class ConflictResolutionMode;
class DcpConnMap;
class DcpFlowControlManager;
class ItemMetaData;
class FileOpsTracker;
class KVBucket;
class PrometheusStatCollector;
class StatCollector;
class StoredValue;
class VBucketCountVisitor;

// Forward decl
class EventuallyPersistentEngine;

namespace cb::prometheus {
enum class MetricGroup;
} // namespace cb::prometheus

namespace cb::audit::document {
enum class Operation;
}

/**
    To allow Engines to run tasks.
**/
class EpEngineTaskable : public Taskable {
public:
    explicit EpEngineTaskable(EventuallyPersistentEngine* e) : myEngine(e) {
    }

    const std::string& getName() const override;

    task_gid_t getGID() const override;

    bucket_priority_t getWorkloadPriority() const override;

    void setWorkloadPriority(bucket_priority_t prio) override;

    WorkLoadPolicy& getWorkLoadPolicy() override;

    void logQTime(const GlobalTask& task,
                  std::string_view threadName,
                  cb::time::steady_clock::duration enqTime) override;

    void logRunTime(const GlobalTask& task,
                    std::string_view threadName,
                    cb::time::steady_clock::duration runTime) override;

    bool isShutdown() const override;

    void invokeViaTaskable(std::function<void()> fn) override;

private:
    EventuallyPersistentEngine* myEngine;
};

/**
 * An object responsible for quota sharing engines.
 */
class QuotaSharingManager {
public:
    /**
     * Returns a reference to the EPEngineGroup which provides access to the
     * active quota sharing engine instances.
     */
    virtual EPEngineGroup& getGroup() = 0;
    virtual ExTask getItemPager() = 0;
    virtual ~QuotaSharingManager() = default;
};

// Helper class which ensures the calling thread switches to the specified
// engine at the very start of EventuallyPersistentEngine's ctor; so
// all memory allocations with the ctor are correctly tracked.
//
// For this to work, EpEngineArenaHelper must be the first member variable in
// EventuallyPersistentEngine, this is statically asserted in the .cc.
//
// Given ObjectRegistry::switchToEngine requires access to the engine's
// memory arena, this class also owns the memory arena.
struct EpEngineArenaHelper {
    EpEngineArenaHelper(EventuallyPersistentEngine& engine,
                        cb::ArenaMallocClient arena);

    cb::ArenaMallocClient arena;
};

/**
 * memcached engine interface to the KVBucket.
 */
class EventuallyPersistentEngine : public EngineIface, public DcpIface {
    friend class LookupCallback;
public:
    EventuallyPersistentEngine(GET_SERVER_API get_server_api,
                               cb::ArenaMallocClient arena);
    ~EventuallyPersistentEngine() override;

    /////////////////////////////////////////////////////////////
    /// Engine interface ////////////////////////////////////////
    /////////////////////////////////////////////////////////////
    cb::engine_errc initialize(std::string_view config,
                               const nlohmann::json&,
                               std::string_view,
                               const nlohmann::json&) override;
    void destroy(bool force) override;
    void disconnect(CookieIface& cookie) override;
    cb::engine_errc set_traffic_control_mode(CookieIface& cookie,
                                             TrafficControlMode mode) override;
    void initiate_shutdown() override;
    void notify_num_writer_threads_changed() override;
    void notify_num_auxio_threads_changed() override;
    void set_num_storage_threads(
            ThreadPoolConfig::StorageThreadCount num) override;
    void cancel_all_operations_in_ewb_state() override;
    cb::unique_item_ptr allocateItem(CookieIface& cookie,
                                     const DocKeyView& key,
                                     size_t nbytes,
                                     size_t priv_nbytes,
                                     uint32_t flags,
                                     rel_time_t exptime,
                                     uint8_t datatype,
                                     Vbid vbucket) override;
    cb::engine_errc remove(
            CookieIface& cookie,
            const DocKeyView& key,
            uint64_t& cas,
            Vbid vbucket,
            const std::optional<cb::durability::Requirements>& durability,
            mutation_descr_t& mut_info) override;
    void release(ItemIface& itm) override;
    cb::EngineErrorItemPair get(CookieIface& cookie,
                                const DocKeyView& key,
                                Vbid vbucket,
                                DocStateFilter documentStateFilter) override;
    cb::EngineErrorItemPair get_replica(
            CookieIface& cookie,
            const DocKeyView& key,
            Vbid vbucket,
            DocStateFilter documentStateFilter) override;
    cb::EngineErrorItemPair get_random_document(CookieIface& cookie,
                                                CollectionID cid) override;
    cb::EngineErrorItemPair get_if(
            CookieIface& cookie,
            const DocKeyView& key,
            Vbid vbucket,
            const std::function<bool(const item_info&)>& filter) override;
    cb::EngineErrorMetadataPair get_meta(CookieIface& cookie,
                                         const DocKeyView& key,
                                         Vbid vbucket) override;
    cb::EngineErrorItemPair get_locked(
            CookieIface& cookie,
            const DocKeyView& key,
            Vbid vbucket,
            std::chrono::seconds lock_timeout) override;
    cb::engine_errc unlock(CookieIface& cookie,
                           const DocKeyView& key,
                           Vbid vbucket,
                           uint64_t cas) override;
    cb::EngineErrorItemPair get_and_touch(
            CookieIface& cookie,
            const DocKeyView& key,
            Vbid vbucket,
            uint32_t expirytime,
            const std::optional<cb::durability::Requirements>& durability)
            override;
    cb::engine_errc store(
            CookieIface& cookie,
            ItemIface& item,
            uint64_t& cas,
            StoreSemantics operation,
            const std::optional<cb::durability::Requirements>& durability,
            DocumentState document_state,
            bool preserveTtl) override;
    cb::EngineErrorCasPair store_if(
            CookieIface& cookie,
            ItemIface& item,
            uint64_t cas,
            StoreSemantics operation,
            const cb::StoreIfPredicate& predicate,
            const std::optional<cb::durability::Requirements>& durability,
            DocumentState document_state,
            bool preserveTtl) override;
    // Need to explicilty import EngineIface::flush to avoid warning about
    // DCPIface::flush hiding it.
    using EngineIface::flush;
    using EngineIface::get_stats;
    cb::engine_errc get_stats(CookieIface& cookie,
                              std::string_view key,
                              std::string_view value,
                              const AddStatFn& add_stat,
                              const CheckYieldFn& check_yield) override;
    cb::engine_errc get_prometheus_stats(
            const BucketStatCollector& collector,
            cb::prometheus::MetricGroup metricGroup) override;
    void reset_stats(CookieIface& cookie) override;
    cb::engine_errc unknown_command(CookieIface& cookie,
                                    const cb::mcbp::Request& request,
                                    const AddResponseFn& response) override;
    bool get_item_info(const ItemIface& item, item_info& item_info) override;
    cb::engine_errc set_collection_manifest(CookieIface& cookie,
                                            std::string_view json) override;
    cb::engine_errc get_collection_manifest(
            CookieIface& cookie, const AddResponseFn& response) override;
    cb::EngineErrorGetCollectionIDResult get_collection_id(
            CookieIface& cookie, std::string_view path) override;
    cb::EngineErrorGetScopeIDResult get_scope_id(
            CookieIface& cookie, std::string_view path) override;
    cb::EngineErrorGetCollectionMetaResult get_collection_meta(
            CookieIface& cookie,
            CollectionID cid,
            std::optional<Vbid> vbid) const override;
    cb::engine::FeatureSet getFeatures() override;
    bool isXattrEnabled() override;
    std::optional<cb::HlcTime> getVBucketHlcNow(Vbid vbucket) override;
    BucketCompressionMode getCompressionMode() override {
        return compressionMode;
    }
    size_t getMaxItemSize() override {
        return maxItemSize;
    }
    float getMinCompressionRatio() override {
        return minCompressionRatio;
    }
    cb::engine_errc setParameter(CookieIface& cookie,
                                 EngineParamCategory category,
                                 std::string_view key,
                                 std::string_view value,
                                 Vbid vbucket) override;
    cb::engine_errc compactDatabase(
            CookieIface& cookie,
            Vbid vbid,
            uint64_t purge_before_ts,
            uint64_t purge_before_seq,
            bool drop_deletes,
            const std::vector<std::string>& obsolete_keys) override;
    std::pair<cb::engine_errc, vbucket_state_t> getVBucket(CookieIface& cookie,
                                                           Vbid vbid) override;
    cb::engine_errc setVBucket(CookieIface& cookie,
                               Vbid vbid,
                               uint64_t cas,
                               vbucket_state_t state,
                               nlohmann::json* meta) override;
    cb::engine_errc deleteVBucket(CookieIface& cookie,
                                  Vbid vbid,
                                  bool sync) override;
    std::pair<cb::engine_errc, cb::rangescan::Id> createRangeScan(
            CookieIface& cookie,
            const cb::rangescan::CreateParameters& params) override;
    cb::engine_errc continueRangeScan(
            CookieIface& cookie,
            const cb::rangescan::ContinueParameters& params) override;
    cb::engine_errc cancelRangeScan(CookieIface& cookie,
                                    Vbid vbid,
                                    cb::rangescan::Id uuid) override;
    std::pair<cb::engine_errc, nlohmann::json> getFusionStorageSnapshot(
            Vbid vbid,
            std::string_view snapshotUuid,
            std::time_t validity) override;
    cb::engine_errc releaseFusionStorageSnapshot(
            Vbid vbid, std::string_view snapshotUuid) override;
    cb::engine_errc syncFusionLogstore(Vbid vbid) override;
    cb::engine_errc startFusionUploader(Vbid vbid, uint64_t term) override;
    cb::engine_errc stopFusionUploader(Vbid vbid) override;
    cb::engine_errc mountVBucket(
            CookieIface& cookie,
            Vbid vbid,
            const std::vector<std::string>& paths,
            const std::function<void(const nlohmann::json&)>& setResponse)
            override;
    cb::engine_errc unmountVBucket(Vbid vbid) override;

    cb::engine_errc setChronicleAuthToken(std::string_view token) override;

    cb::engine_errc pause(folly::CancellationToken cancellationToken) override;
    cb::engine_errc resume() override;
    cb::engine_errc start_persistence(CookieIface& cookie) override;
    cb::engine_errc stop_persistence(CookieIface& cookie) override;
    cb::engine_errc wait_for_seqno_persistence(CookieIface& cookie,
                                               uint64_t seqno,
                                               Vbid vbid) override;
    cb::engine_errc evict_key(CookieIface& cookie,
                              const DocKeyView& key,
                              Vbid vbucket) override;
    [[nodiscard]] cb::engine_errc set_active_encryption_keys(
            CookieIface& cookie, const nlohmann::json& json) override;
    [[nodiscard]] cb::engine_errc prepare_snapshot(
            CookieIface& cookie,
            Vbid vbid,
            const std::function<void(const nlohmann::json&)>& callback)
            override;
    [[nodiscard]] cb::engine_errc download_snapshot(
            CookieIface& cookie, Vbid vbid, std::string_view metadata) override;
    [[nodiscard]] cb::engine_errc get_snapshot_file_info(
            CookieIface& cookie,
            std::string_view uuid,
            std::size_t file_id,
            const std::function<void(const nlohmann::json&)>& callback)
            override;
    [[nodiscard]] cb::engine_errc release_snapshot(
            CookieIface& cookie,
            std::variant<Vbid, std::string_view> snap) override;

    /////////////////////////////////////////////////////////////
    // DcpIface implementation //////////////////////////////////
    /////////////////////////////////////////////////////////////
    cb::engine_errc step(CookieIface& cookie,
                         bool throttled,
                         DcpMessageProducersIface& producers) override;
    cb::engine_errc open(CookieIface& cookie,
                         uint32_t opaque,
                         uint32_t seqno,
                         cb::mcbp::DcpOpenFlag flags,
                         std::string_view name,
                         std::string_view value) override;
    cb::engine_errc add_stream(CookieIface& cookie,
                               uint32_t opaque,
                               Vbid vbucket,
                               cb::mcbp::DcpAddStreamFlag flags) override;
    cb::engine_errc close_stream(CookieIface& cookie,
                                 uint32_t opaque,
                                 Vbid vbucket,
                                 cb::mcbp::DcpStreamId sid) override;
    cb::engine_errc stream_req(CookieIface& cookie,
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
                               std::optional<std::string_view> json) override;
    cb::engine_errc get_failover_log(CookieIface& cookie,
                                     uint32_t opaque,
                                     Vbid vbucket,
                                     dcp_add_failover_log callback) override;
    cb::engine_errc stream_end(CookieIface& cookie,
                               uint32_t opaque,
                               Vbid vbucket,
                               cb::mcbp::DcpStreamEndStatus status) override;
    cb::engine_errc snapshot_marker(
            CookieIface& cookie,
            uint32_t opaque,
            Vbid vbucket,
            uint64_t start_seqno,
            uint64_t end_seqno,
            cb::mcbp::request::DcpSnapshotMarkerFlag flags,
            std::optional<uint64_t> high_completed_seqno,
            std::optional<uint64_t> high_prepared_seqno,
            std::optional<uint64_t> max_visible_seqno,
            std::optional<uint64_t> purge_seqno) override;
    cb::engine_errc mutation(CookieIface& cookie,
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
                             uint8_t nru) override;
    cb::engine_errc deletion(CookieIface& cookie,
                             uint32_t opaque,
                             const DocKeyView& key,
                             cb::const_byte_buffer value,
                             uint8_t datatype,
                             uint64_t cas,
                             Vbid vbucket,
                             uint64_t by_seqno,
                             uint64_t rev_seqno,
                             cb::const_byte_buffer meta) override;
    cb::engine_errc deletion_v2(CookieIface& cookie,
                                uint32_t opaque,
                                const DocKeyView& key,
                                cb::const_byte_buffer value,
                                uint8_t datatype,
                                uint64_t cas,
                                Vbid vbucket,
                                uint64_t by_seqno,
                                uint64_t rev_seqno,
                                uint32_t delete_time) override;
    cb::engine_errc expiration(CookieIface& cookie,
                               uint32_t opaque,
                               const DocKeyView& key,
                               cb::const_byte_buffer value,
                               uint8_t datatype,
                               uint64_t cas,
                               Vbid vbucket,
                               uint64_t by_seqno,
                               uint64_t rev_seqno,
                               uint32_t deleteTime) override;
    cb::engine_errc set_vbucket_state(CookieIface& cookie,
                                      uint32_t opaque,
                                      Vbid vbucket,
                                      vbucket_state_t state) override;
    cb::engine_errc noop(CookieIface& cookie, uint32_t opaque) override;
    cb::engine_errc buffer_acknowledgement(CookieIface& cookie,
                                           uint32_t opaque,
                                           uint32_t buffer_bytes) override;
    cb::engine_errc control(CookieIface& cookie,
                            uint32_t opaque,
                            std::string_view key,
                            std::string_view value) override;
    cb::engine_errc response_handler(
            CookieIface& cookie, const cb::mcbp::Response& response) override;
    cb::engine_errc system_event(CookieIface& cookie,
                                 uint32_t opaque,
                                 Vbid vbucket,
                                 mcbp::systemevent::id event,
                                 uint64_t bySeqno,
                                 mcbp::systemevent::version version,
                                 cb::const_byte_buffer key,
                                 cb::const_byte_buffer eventData) override;
    cb::engine_errc prepare(CookieIface& cookie,
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
                            cb::durability::Level level) override;
    cb::engine_errc seqno_acknowledged(CookieIface& cookie,
                                       uint32_t opaque,
                                       Vbid vbucket,
                                       uint64_t prepared_seqno) override;
    cb::engine_errc commit(CookieIface& cookie,
                           uint32_t opaque,
                           Vbid vbucket,
                           const DocKeyView& key,
                           uint64_t prepared_seqno,
                           uint64_t commit_seqno) override;
    cb::engine_errc abort(CookieIface& cookie,
                          uint32_t opaque,
                          Vbid vbucket,
                          const DocKeyView& key,
                          uint64_t prepared_seqno,
                          uint64_t abort_seqno) override;
    // End DcpIface ///////////////////////////////////////////////////////////

    /**
     * Delete a given key and value from the engine.
     *
     * @param cookie The cookie representing the connection
     * @param key The key that needs to be deleted from the engine
     * @param cas CAS value of the mutation that needs to be returned
     *            back to the client
     * @param vbucket vbucket id to which the deleted key corresponds to
     * @param durability Optional durability requirements for this deletion.
     * @param mut_info pointer to the mutation info that resulted from
     *                 the delete.
     *
     * @returns cb::engine_errc::success if the delete was successful or
     *          an error code indicating the error
     */
    cb::engine_errc removeInner(
            CookieIface& cookie,
            const DocKeyView& key,
            uint64_t& cas,
            Vbid vbucket,
            std::optional<cb::durability::Requirements> durability,
            mutation_descr_t& mut_info);

    void itemRelease(ItemIface* itm);

    cb::EngineErrorItemPair getInner(CookieIface& cookie,
                                     const DocKeyView& key,
                                     Vbid vbucket,
                                     get_options_t options);
    cb::EngineErrorItemPair getReplicaInner(CookieIface& cookie,
                                            const DocKeyView& key,
                                            Vbid vbucket,
                                            DocStateFilter documentStateFilter);
    cb::EngineErrorItemPair getRandomDocument(CookieIface& cookie,
                                              CollectionID cid);

    /**
     * Fetch an item only if the specified filter predicate returns true.
     *
     * Note: The implementation of this method is a performance tradeoff based
     * on the expected ratio of filter hit/misses under Full Eviction:
     * Currently get_if filter is used only for checking if a Document has
     * XATTRs, and so the assumption is such documents will be rare in general,
     * hence we first perform a meta bg fetch (instead of full meta+value) as
     * the value is not expected to be needed.
     * If however this assumption fails, and actually it is more common to have
     * documents which match the filter, then this tradeoff should be
     * re-visited, as we'll then need to go to disk a *second* time for the
     * value (whereas we could have just fetched the meta+value in the first
     * place).
     */
    cb::EngineErrorItemPair getIfInner(
            CookieIface& cookie,
            const DocKeyView& key,
            Vbid vbucket,
            const std::function<bool(const item_info&)>& filter);

    cb::EngineErrorItemPair getAndTouchInner(CookieIface& cookie,
                                             const DocKeyView& key,
                                             Vbid vbucket,
                                             uint32_t expiry_time);

    cb::EngineErrorItemPair getLockedInner(CookieIface& cookie,
                                           const DocKeyView& key,
                                           Vbid vbucket,
                                           std::chrono::seconds lock_timeout);

    cb::engine_errc unlockInner(CookieIface& cookie,
                                const DocKeyView& key,
                                Vbid vbucket,
                                uint64_t cas);

    const std::string& getName() const {
        return name;
    }

    cb::engine_errc getStats(CookieIface& cookie,
                             std::string_view key,
                             std::string_view value,
                             const AddStatFn& add_stat);

    cb::engine_errc getStats(CookieIface& cookie,
                             std::string_view key,
                             std::string_view value,
                             const AddStatFn& add_stat,
                             const CheckYieldFn& check_yield);

    void resetStats();

    cb::engine_errc storeInner(CookieIface& cookie,
                               Item& itm,
                               uint64_t& cas,
                               StoreSemantics operation,
                               bool preserveTtl);

    cb::EngineErrorCasPair storeIfInner(CookieIface& cookie,
                                        Item& itm,
                                        uint64_t cas,
                                        StoreSemantics operation,
                                        const cb::StoreIfPredicate& predicate,
                                        bool preserveTtl);

    cb::engine_errc dcpOpen(CookieIface& cookie,
                            uint32_t opaque,
                            uint32_t seqno,
                            cb::mcbp::DcpOpenFlag flags,
                            std::string_view stream_name,
                            std::string_view value);

    cb::engine_errc dcpAddStream(CookieIface& cookie,
                                 uint32_t opaque,
                                 Vbid vbucket,
                                 cb::mcbp::DcpAddStreamFlag flags);

    cb::EngineErrorMetadataPair getMetaInner(CookieIface& cookie,
                                             const DocKeyView& key,
                                             Vbid vbucket);

    cb::engine_errc setWithMeta(CookieIface& cookie,
                                const cb::mcbp::Request& request,
                                const AddResponseFn& response);

    cb::engine_errc deleteWithMeta(CookieIface& cookie,
                                   const cb::mcbp::Request& request,
                                   const AddResponseFn& response);

    cb::engine_errc returnMeta(CookieIface& cookie,
                               const cb::mcbp::Request& req,
                               const AddResponseFn& response);

    cb::engine_errc getAllKeys(CookieIface& cookie,
                               const cb::mcbp::Request& request,
                               const AddResponseFn& response);

    template <typename T>
    void notifyIOComplete(T cookies, cb::engine_errc status);
    void notifyIOComplete(CookieIface& cookie, cb::engine_errc status);
    void notifyIOComplete(CookieIface* cookie, cb::engine_errc status);
    void scheduleDcpStep(CookieIface& cookie);

    void reserveCookie(CookieIface& cookie);
    void releaseCookie(CookieIface& cookie);

    /**
     * Returns a copy of the stored value or nullopt, if there is no value
     * stored in the engine specific.
     * @throws std::bad_cast if the stored value is not of type T.
     */
    template <typename T>
    std::optional<T> getEngineSpecific(CookieIface& cookie) {
        auto* es = cookie.getEngineStorage();
        if (!es) {
            return {};
        }
        return dynamic_cast<const EPEngineStorage<T>&>(*es).get();
    }

    /**
     * Returns the contained value of type T from the engine storage and clears
     * the engine storage.
     * @throws std::bad_cast if the stored value is not of type T. The stored
     * value is lost in this case.
     */
    template <typename T>
    std::optional<T> takeEngineSpecific(CookieIface& cookie) {
        auto es = cookie.takeEngineStorage();
        if (!es) {
            return {};
        }

        return dynamic_cast<EPEngineStorage<T>&>(*es).take();
    }

    /// Stores the value in the engine specific storage.
    template <typename T>
    void storeEngineSpecific(CookieIface& cookie, T&& value) {
        // TODO: We should be able to assert that getCurrentEngine() == this
        // here, but that is not always true in many of our tests
        cb::unique_engine_storage_ptr p(
                new EPEngineStorage<std::decay_t<T>>(std::forward<T>(value)));
        cookie.setEngineStorage(std::move(p));
    }

    /// Clears the engine specific storage.
    void clearEngineSpecific(CookieIface& cookie) {
        cookie.setEngineStorage({});
    }

    void setErrorContext(CookieIface& cookie, std::string_view message);

    void setUnknownCollectionErrorContext(CookieIface& cookie,
                                          uint64_t manifestUid) const;

    void handleDisconnect(CookieIface& cookie);

    cb::engine_errc stopFlusher(CookieIface& cookie);

    cb::engine_errc startFlusher(CookieIface& cookie);

    /// Schedule compaction (used by unit tests)
    cb::engine_errc scheduleCompaction(Vbid vbid,
                                       const CompactionConfig& c,
                                       CookieIface* cookie);

    cb::engine_errc evictKey(CookieIface& cookie,
                             const DocKeyView& key,
                             Vbid vbucket);

    cb::engine_errc observe(
            CookieIface& cookie,
            const DocKeyView& key,
            Vbid vbucket,
            const std::function<void(uint8_t, uint64_t)>& key_handler,
            uint64_t& persist_time_hint) override;

    cb::engine_errc observe_seqno(CookieIface& cookie,
                                  const cb::mcbp::Request& request,
                                  const AddResponseFn& response);

    VBucketPtr getVBucket(Vbid vbucket) const;

    cb::engine_errc setVBucketState(CookieIface& cookie,
                                    Vbid vbid,
                                    vbucket_state_t to,
                                    const nlohmann::json* meta,
                                    TransferVB transfer,
                                    uint64_t cas);

    cb::engine_errc setParameterInner(CookieIface& cookie,
                                      EngineParamCategory category,
                                      std::string_view key,
                                      std::string_view value,
                                      Vbid vbucket);

    cb::engine_errc setFlushParam(std::string_view key,
                                  const std::string& val,
                                  std::string& msg);
    cb::engine_errc setFusionFlushParam(std::string_view key,
                                        const std::string& val,
                                        std::string& msg);
    cb::engine_errc setMagmaFlushParam(std::string_view key,
                                       const std::string& val,
                                       std::string& msg);
    cb::engine_errc setDefragmenterFlushParam(std::string_view key,
                                              const std::string& val,
                                              std::string& msg);

    cb::engine_errc setCheckpointParam(std::string_view key,
                                       const std::string& val,
                                       std::string& msg);

    cb::engine_errc setDcpParam(std::string_view key,
                                const std::string& val,
                                std::string& msg);

    cb::engine_errc setVbucketParam(Vbid vbucket,
                                    std::string_view key,
                                    const std::string& val,
                                    std::string& msg);

    EPStats& getEpStats() {
        return stats;
    }

    MemoryTracker& getMemoryTracker() {
        return *memoryTracker;
    }

    const EPStats& getEpStats() const {
        return stats;
    }

    KVBucket* getKVBucket() {
        return kvBucket.get();
    }

    const KVBucket* getKVBucket() const {
        return kvBucket.get();
    }

    DcpConnMap& getDcpConnMap() {
        return *dcpConnMap_;
    }

    DcpFlowControlManager& getDcpFlowControlManager() {
        return *dcpFlowControlManager;
    }

    CheckpointConfig& getCheckpointConfig() {
        return *checkpointConfig;
    }

    ServerApi* getServerApi() {
        return serverApi;
    }

    Configuration& getConfiguration() {
        return configuration;
    }

    const Configuration& getConfiguration() const {
        return configuration;
    }

    cb::engine_errc handleObserve(
            CookieIface& cookie,
            const DocKeyView& key,
            Vbid vbucket,
            const std::function<void(uint8_t, uint64_t)>& key_handler,
            uint64_t& persist_time_hint);

    cb::engine_errc handleSeqnoPersistence(CookieIface& cookie,
                                           uint64_t seqno,
                                           Vbid vbid);

    cb::engine_errc handleTrafficControlCmd(CookieIface& cookie,
                                            TrafficControlMode mode);

    std::chrono::seconds getGetlDefaultTimeout() const {
        return getlDefaultTimeout;
    }

    std::chrono::seconds getGetlMaxTimeout() const {
        return getlMaxTimeout;
    }

    size_t getMaxFailoverEntries() const {
        return maxFailoverEntries;
    }

    bool isDegradedMode() const;

    WorkLoadPolicy& getWorkLoadPolicy() {
        return *workload;
    }

    bucket_priority_t getWorkloadPriority() const {
        return workloadPriority;
    }

    void setWorkloadPriority(bucket_priority_t p) {
        workloadPriority = p;
    }

    void setCompressionMode(const std::string& compressModeStr);

    void setConflictResolutionMode(std::string_view mode);

    void setMinCompressionRatio(float minCompressRatio) {
        minCompressionRatio = minCompressRatio;
    }

    /**
     * Get the connection handler for the provided cookie
     *
     * @param cookie the cookie to look up
     * @return the shared_ptr to the connection handler (can be null)
     */
    static std::shared_ptr<ConnHandler> getSharedPtrConnHandler(
            CookieIface& cookie);

    /**
     * Get the connection handler for the provided cookie. This method differs
     * from the getSharedPtrConnHandler that it expects the conn handler to
     * exist and will throw an exception if there isn't.
     *
     * In most cases where we want to get the connection handlers we're called
     * as part of the command handler for DCP, and the core has already
     * checked that we're called on a DCP connection (and the handler should
     * be there) and return an error back to the caller if it isn't
     *
     * @param cookie the cookie to look up
     * @return the connection handler as shared_ptr, cannot be null.
     * @throws std::logic_error if the cookie don't have a connection handler
     */
    static std::shared_ptr<ConnHandler> getConnHandler(CookieIface& cookie);

    /*
     * Explicitly trigger the defragmenter task. Provided to facilitate
     * testing.
     */
    void runDefragmenterTask();

    /*
     * Explicitly trigger the AccessScanner task. Provided to facilitate
     * testing.
     */
    bool runAccessScannerTask();

    /*
     * Explicitly trigger the VbStatePersist task. Provided to facilitate
     * testing.
     */
    void runVbStatePersistTask(Vbid vbid);

    /**
     * Get a (sloppy) list of the sequence numbers for all of the vbuckets
     * on this server. It is not to be treated as a consistent set of seqence,
     * but rather a list of "at least" numbers. The way the list is generated
     * is that we're starting for vbucket 0 and record the current number,
     * then look at the next vbucket and record its number. That means that
     * at the time we get the number for vbucket X all of the previous
     * numbers could have been incremented. If the client just needs a list
     * of where we are for each vbucket this method may be more optimal than
     * requesting one by one.
     *
     * @param cookie The cookie representing the connection to requesting
     *               list
     * @param add_response The method used to format the output buffer
     * @return cb::engine_errc::success upon success
     */
    cb::engine_errc getAllVBucketSequenceNumbers(
            CookieIface& cookie,
            const cb::mcbp::Request& request,
            const AddResponseFn& response);

    void updateDcpMinCompressionRatio(float value);

    EpEngineTaskable& getTaskable() {
        return taskable;
    }

    /**
     * Return the item info as an item_info object.
     * @param item item whose data should be retrieved
     * @returns item_info created from item
     */
    item_info getItemInfo(const Item& item);

    void destroyInner(bool force);

    KVStoreIface::CreateItemCB getCreateItemCallback();

    [[nodiscard]] cb::engine_errc setActiveEncryptionKeys(
            CookieIface& cookie, const nlohmann::json& json);

    /**
     * Create an Item with the following parameters if the mutation watermark
     * will not be exceeded. If successful, the engine error code is set to
     * cb::engine_errc::success and a unique pointer to the created item is
     * returned. If memory allocation fails due to exceeding the mutation
     * watermark the engine error code is set to
     * cb::engine_errc::temporary_failure or cb::engine_errc::no_memory if above
     * memory quota, this is return along with a nullptr as the item.
     *
     * @param key The document key.
     * @param nbytes The size of the item body.
     * @param flags Flags associated with the item.
     * @param exptime Expiration time for the item.
     * @param body The value of the item.
     * @param datatype The datatype of the item.
     * @param theCas The CAS value for the item.
     * @param bySeq The sequence number associated with the item.
     * @param vbid The vBucket ID.
     * @param revSeq The revision sequence number.
     * @return A pair containing an engine error code and a unique pointer to
     * the created item.
     */
    virtual std::pair<cb::engine_errc, std::unique_ptr<Item>> createItem(
            const DocKeyView& key,
            size_t nbytes,
            uint32_t flags,
            rel_time_t exptime,
            const value_t& body,
            uint8_t datatype,
            uint64_t theCas,
            int64_t bySeq,
            Vbid vbid,
            int64_t revSeq);

    cb::EngineErrorItemPair itemAllocate(const DocKeyView& key,
                                         const size_t nbytes,
                                         const size_t priv_nbytes,
                                         const uint32_t flags,
                                         rel_time_t exptime,
                                         uint8_t datatype,
                                         Vbid vbucket);

    /**
     * Set the bucket's maximum data size (aka quota).
     * This method propagates the value to other components that need to
     * know the bucket's quota and is used on initialisation and dynamic
     * changes.
     *
     * @param size in bytes for the bucket
     */
    void setMaxDataSize(size_t size);

    /**
     * Set the low and high watermarks for the given quota
     *
     * @param size in bytes for the new quota
     */
    void configureMemWatermarksForQuota(size_t size);

    /**
     * Set the quota in the storage engine
     *
     * @param size in bytes for the new quota
     */
    void configureStorageMemoryForQuota(size_t size);

    /**
     * Update the arena threshold for the given quota
     *
     * @param size in bytes for the new quota
     */
    void updateArenaAllocThresholdForQuota(size_t size);

    cb::ArenaMallocClient& getArenaMallocClient() {
        return arenaHelper.arena;
    }

    cb::engine_errc checkForPrivilegeAtLeastInOneCollection(
            CookieIface& cookie, cb::rbac::Privilege privilege) const;

    /**
     * Check if the cookie holds the privilege for the requested collection
     * id. If the collection is a system collection the caller must also
     * hold the privilege specified in systemCollectionPrivilege
     */
    cb::engine_errc checkCollectionAccess(
            CookieIface& cookie,
            std::optional<Vbid> vbid,
            std::optional<cb::rbac::Privilege> systemCollectionPrivilege,
            cb::rbac::Privilege priv,
            CollectionID cid,
            bool logIfPrivilegeMissing = true) const;

    /**
     * Check if the cookie holds the privilege for the requested scope
     * id. If the scope is a system collection the caller must also
     * hold the privilege specified in systemScopePrivilege
     */
    cb::engine_errc testScopeAccess(
            CookieIface& cookie,
            std::optional<cb::rbac::Privilege> systemScopePrivilege,
            cb::rbac::Privilege priv,
            ScopeID sid,
            Collections::Visibility visibility) const;

    /**
     * Check if the cookie holds the privilege for the requested collection
     * id. If the collection is a system collection the caller must also
     * hold the privilege specified in systemCollectionPrivilege
     */
    cb::engine_errc testCollectionAccess(
            CookieIface& cookie,
            std::optional<cb::rbac::Privilege> systemCollectionPrivilege,
            cb::rbac::Privilege priv,
            CollectionID cid,
            ScopeID sid,
            Collections::Visibility visibility) const;

    /**
     * Test the access for the given privilege for the bucket.scope.collection
     * This differs from checkPrivilege in that the error case has no side
     * effect, such as setting error extras/logging
     */
    cb::engine_errc testPrivilege(CookieIface& cookie,
                                  cb::rbac::Privilege priv,
                                  std::optional<ScopeID> sid,
                                  std::optional<CollectionID> cid) const;

    /**
     * Checks if adding the specified memory size will keep memory usage below
     * the mutation watermark. If so, returns success; otherwise, invoke
     * eviction and return no_memory/temp_failure.
     *
     * @param pendingBytes The size of memory to be added.
     * @return cb::engine_errc::success if addition memory size is available,
     * otherwise no_memory / temp_failure.
     */
    cb::engine_errc checkMemoryForBGFetch(size_t pendingBytes);

    cb::engine_errc doRangeScanStats(const BucketStatCollector& collector,
                                     std::string_view statKey);

    /**
     * Set the ratio of the Bucket Quota that DCP Consumers on this node can
     * allocate for their DCP inbound buffer.
     * Note that Consumers buffer messages only in the case of OOM conditions on
     * the node.
     *
     * @param ratio
     */
    void setDcpConsumerBufferRatio(float ratio);

    float getDcpConsumerBufferRatio() const;

    /**
     * Returns the quota sharing manager.
     */
    virtual QuotaSharingManager& getQuotaSharingManager();

    /**
     * Create the item pager task for the engine's bucket. Scheduling of the
     * task is left up to the caller.
     */
    ExTask createItemPager();

    /**
     * Sets the limit (bytes) of data that a single DCP Producer connection can
     * push into its streams outbound queues (readyQ).
     *
     * @param bytes
     */
    void setDcpBackfillByteLimit(size_t bytes);

    /// Get the encryption key provider used by this instance of ep-engine
    [[nodiscard]] EncryptionKeyProvider* getEncryptionKeyProvider() {
        return &encryptionKeyProvider;
    }

    /**
     * Get the timeout for disconnecting stuck DCP Producers.
     *
     * @return the timeout
     */
    std::chrono::seconds getDcpDisconnectWhenStuckTimeout() const;

    /**
     * Get the regex for selecting which DCP producers can be disconnected.
     *
     * @return the regex as a base64 encoded string
     */
    std::string getDcpDisconnectWhenStuckNameRegex() const;

    std::string getCachedChronicleAuthToken() const;

    /**
     * Get the fusion namespace which is in the format
     * <service_prefix>/<bucket_name>/<bucket_uuid>.
     *
     * @return the fusion namespace
     */
    std::string getFusionNamespace() const;

    /**
     * Set the setting for returning tmpfail instead of not_locked when
     * unlocking an item that is not locked.
     *
     * @param value true if tmpfail should be returned, false otherwise.
     */
    void setNotLockedReturnsTmpfail(bool value);

    /**
     * Get the setting for returning tmpfail instead of not_locked when
     * unlocking an item that is not locked.
     *
     * @return true if tmpfail should be returned, false otherwise.
     */
    bool getNotLockedReturnsTmpfail() const;

    /**
     * @return the error code to return when attempting to unlock an item that
     * is not locked.
     */
    cb::engine_errc getNotLockedError() const;

    /**
     * @return the max_marker_version that a consumer will send to a producer.
     */
    double getDcpConsumerMaxMarkerVersion() const;

    /**
     * @return true if the HPS should be sent in Snapshot Marker.
     */
    bool isDcpSnapshotMarkerHPSEnabled() const;

    /**
     * @return true if the Purge Seqno should be sent in Snapshot Marker.
     */
    bool isDcpSnapshotMarkerPurgeSeqnoEnabled() const;

    /**
     * Whether blind write optimisation is enabled.
     */
    bool isMagmaBlindWriteOptimisationEnabled() const;

    /// Result of getValidVBucketFromString()
    struct StatusAndVBPtr {
        cb::engine_errc status;
        VBucketPtr vb;
    };
    /**
     * Helper method for stats calls - validates the specified vbucket number
     * and returns a VBucketPtr (if valid).
     */
    StatusAndVBPtr getValidVBucketFromString(std::string_view vbNum);

protected:
    friend class EpEngineValueChangeListener;

    cb::engine_errc compactDatabaseInner(
            CookieIface& cookie,
            Vbid vbid,
            uint64_t purge_before_ts,
            uint64_t purge_before_seq,
            bool drop_deletes,
            const std::vector<std::string>& obsolete_keys);

    std::pair<cb::engine_errc, vbucket_state_t> getVBucketInner(
            CookieIface& cookie, Vbid vbid);
    cb::engine_errc setVBucketInner(CookieIface& cookie,
                                    Vbid vbid,
                                    uint64_t cas,
                                    vbucket_state_t state,
                                    nlohmann::json* meta);
    cb::engine_errc deleteVBucketInner(CookieIface& cookie,
                                       Vbid vbid,
                                       bool sync);

    void auditDocumentAccess(CookieIface& cookie,
                             cb::audit::document::Operation operation) const;

    void setMaxItemSize(size_t value) {
        maxItemSize = value;
    }

    void setMaxItemPrivilegedBytes(size_t value) {
        maxItemPrivilegedBytes = value;
    }

    void setGetlDefaultTimeout(std::chrono::seconds value) {
        getlDefaultTimeout = value;
    }

    void setGetlMaxTimeout(std::chrono::seconds value) {
        getlMaxTimeout = value;
    }

    /**
     * Report the state of a memory condition when out of memory.
     *
     * @return ETMPFAIL if we think we can recover without interaction,
     *         else ENOMEM
     */
    cb::engine_errc memoryCondition();

    /**
     * Check if there is memory available to allocate an Item
     * instance with a given size.
     *
     * @param totalItemSize Total size requured by the item
     *
     * @return True if there is memory for the item; else False
     */
    bool hasMemoryForItemAllocation(size_t totalItemSize);

    friend class KVBucket;
    friend class EPBucket;

    bool enableTraffic(bool enable);

    /**
     * Collect all low and high cardinality Engine stats.
     *
     * Used to generate the cbstats "empty key" group.
     */
    cb::engine_errc doEngineStats(const BucketStatCollector& collector,
                                  CookieIface* cookie);
    /**
     * Collect a smaller number of engine stats which are of higher importance,
     * or specifically need to be collected frequently.
     */
    cb::engine_errc doEngineStatsLowCardinality(
            const BucketStatCollector& collector, CookieIface* cookie);
    /**
     * Collect engine stats which are any of:
     *  1) Numerous (e.g., per collection, per vbucket)
     *  2) Expensive to gather
     *  3) Not necessary to record with very fine temporal granularity
     *     (e.g., rarely changing, not of critical debugging value)
     */
    cb::engine_errc doEngineStatsHighCardinality(
            const BucketStatCollector& collector);

    cb::engine_errc doMemoryStats(CookieIface& cookie,
                                  const AddStatFn& add_stat);
    cb::engine_errc doVBucketStats(CookieIface& cookie,
                                   const AddStatFn& add_stat,
                                   std::string_view,
                                   VBucketStatsDetailLevel detail);
    cb::engine_errc doEncryptionKeyIdsStats(CookieIface& cookie,
                                            const AddStatFn& add_stat);
    cb::engine_errc doHashStats(CookieIface& cookie, const AddStatFn& add_stat);
    cb::engine_errc doHashDump(CookieIface& cookie,
                               const AddStatFn& addStat,
                               std::string_view keyArgs);
    cb::engine_errc doCheckpointStats(CookieIface& cookie,
                                      const AddStatFn& add_stat,
                                      std::string_view);
    cb::engine_errc doCheckpointDump(CookieIface& cookie,
                                     const AddStatFn& addStat,
                                     std::string_view keyArgs);
    cb::engine_errc doDurabilityMonitorStats(CookieIface& cookie,
                                             const AddStatFn& add_stat,
                                             std::string_view);
    cb::engine_errc doDurabilityMonitorDump(CookieIface& cookie,
                                            const AddStatFn& addStat,
                                            std::string_view keyArgs);
    cb::engine_errc doVBucketDump(CookieIface& cookie,
                                  const AddStatFn& addStat,
                                  std::string_view keyArgs);
    /**
     * Immediately collect DCP stats. This function yields control back to the
     * caller in places, to allow the caller to allow the caller to proccess
     * smaller chunks of data and provide backpressure.
     * @return throttled if the there are more stats to send; success when done.
     */
    cb::engine_errc doDcpStats(CookieIface& cookie,
                               const AddStatFn& add_stat,
                               const CheckYieldFn& check_yield,
                               std::string_view value);

    void addAggregatedProducerStats(const BucketStatCollector& collector,
                                    const ConnCounter& aggregator);
    cb::engine_errc doEvictionStats(CookieIface& cookie,
                                    const AddStatFn& add_stat);
    cb::engine_errc doConnAggStats(const BucketStatCollector& collector,
                                   std::string_view sep);
    void doTimingStats(const BucketStatCollector& collector);
    cb::engine_errc doFrequencyCountersStats(
            const BucketStatCollector& collector);
    cb::engine_errc doSchedulerStats(CookieIface& cookie,
                                     const AddStatFn& add_stat);
    cb::engine_errc doRunTimeStats(const BucketStatCollector& collector);
    cb::engine_errc doDispatcherStats(CookieIface& cookie,
                                      const AddStatFn& add_stat);
    cb::engine_errc doTasksStats(CookieIface& cookie,
                                 const AddStatFn& add_stat);
    cb::engine_errc doKeyStats(CookieIface& cookie,
                               const AddStatFn& add_stat,
                               Vbid vbid,
                               const DocKeyView& key,
                               bool validate = false);

    cb::engine_errc doDcpVbTakeoverStats(CookieIface& cookie,
                                         const AddStatFn& add_stat,
                                         std::string& key,
                                         Vbid vbid);
    cb::engine_errc doVbIdFailoverLogStats(CookieIface& cookie,
                                           const AddStatFn& add_stat,
                                           Vbid vbid);
    cb::engine_errc doAllFailoverLogStats(CookieIface& cookie,
                                          const AddStatFn& add_stat);
    cb::engine_errc doWorkloadStats(CookieIface& cookie,
                                    const AddStatFn& add_stat);
    cb::engine_errc doSeqnoStats(CookieIface& cookie,
                                 const AddStatFn& add_stat,
                                 std::string_view);
    void addSeqnoVbStats(CookieIface& cookie,
                         const AddStatFn& add_stat,
                         const VBucketPtr& vb);

    cb::engine_errc doCollectionStats(CookieIface& cookie,
                                      const AddStatFn& add_stat,
                                      std::string_view statKey);

    cb::engine_errc doScopeStats(CookieIface& cookie,
                                 const AddStatFn& add_stat,
                                 std::string_view statKey);

    cb::engine_errc doKeyStats(CookieIface& cookie,
                               const AddStatFn& add_stat,
                               std::string_view statKey);

    cb::engine_errc doVKeyStats(CookieIface& cookie,
                                const AddStatFn& add_stat,
                                std::string_view statKey);

    cb::engine_errc doDcpVbTakeoverStats(CookieIface& cookie,
                                         const AddStatFn& add_stat,
                                         std::string_view statKey);

    cb::engine_errc doFailoversStats(CookieIface& cookie,
                                     const AddStatFn& add_stat,
                                     std::string_view statKey);

    cb::engine_errc doDiskinfoStats(CookieIface& cookie,
                                    const AddStatFn& add_stat,
                                    std::string_view statKey);

    void doDiskFailureStats(const BucketStatCollector& collector);

    /// Collect disk slowness stats based on the provided threshold.
    cb::engine_errc doDiskSlownessStats(CookieIface& cookie,
                                        const AddStatFn& add_stat,
                                        std::string_view key);

    /// Collect disk slowness stats for Prometheus.
    void doDiskSlownessStats(const BucketStatCollector& collector);

    void doContinuousBackupStats(const BucketStatCollector& collector);

    cb::engine_errc doPrivilegedStats(CookieIface& cookie,
                                      const AddStatFn& add_stat,
                                      std::string_view statKey);

    cb::engine_errc doFusionStats(CookieIface& cookie,
                                  const AddStatFn& add_stat,
                                  std::string_view statKey);

    /**
     * Get metrics from this engine for the "High" (cardinality) group.
     *
     * These metrics will be gathered less frequently, and may be large in
     * number (e.g., per scope or collection).
     */
    cb::engine_errc doMetricGroupHigh(const BucketStatCollector& collector);

    /**
     * Get metrics from this engine for the "Low" (cardinality) group.
     *
     * These metrics will be gathered more frequently, and should be limited
     * .
     */
    cb::engine_errc doMetricGroupLow(const BucketStatCollector& collector);

    /**
     * Get metrics from this engine for the "Metering" group.
     *
     * These metrics will only be gathered if DeploymentModel::Serverless.
     * These are likely to be scraped frequently, so should not be too
     * numerous or expensive.
     */
    cb::engine_errc doMetricGroupMetering(
            const BucketStatCollector& collector);

    void addLookupResult(CookieIface& cookie, std::unique_ptr<Item> result);

    bool fetchLookupResult(CookieIface& cookie, std::unique_ptr<Item>& itm);

    /**
     * Private helper method for decoding the options on set/del_with_meta.
     * Tighly coupled to the logic of both those functions, it will
     * take a request pointer and locate and validate any options within.
     *
     * @param extras byte buffer containing the incoming meta extras
     * @param generateCas[out] set to Yes if CAS regeneration is enabled.
     * @param checkConflicts[out] set to No if conflict resolution should
     *        not be performed.
     * @param deleteSource Changes to TTL if IS_EXPIRATION flag (del_with_meta)
     * @return true if everything is OK, false for an invalid combination of
     *              options
     */
    bool decodeWithMetaOptions(cb::const_byte_buffer extras,
                               GenerateCas& generateCas,
                               CheckConflicts& checkConflicts,
                               DeleteSource& deleteSource);

    /**
     * Private wrapper method for decodeWithMetaOptions called from setWithMeta
     * to abstract out deleteSource, which is unused by setWithMeta.
     * @return true if everything is OK, false for an invalid combination of
     *              options
     */
    bool decodeSetWithMetaOptions(cb::const_byte_buffer extras,
                                  GenerateCas& generateCas,
                                  CheckConflicts& checkConflicts);

    /**
     * Sends error response, using the specified error and response callback
     * to the specified connection via it's cookie.
     *
     * @param response callback func to send the response
     * @param status error status to send
     * @param cas a cas value to send
     * @param cookie conn cookie
     *
     * @return status of sending response
     */
    static cb::engine_errc sendErrorResponse(const AddResponseFn& response,
                                             cb::mcbp::Status status,
                                             uint64_t cas,
                                             CookieIface& cookie);

    /**
     * Sends a response that includes the mutation extras, the VB uuid and
     * seqno of the mutation.
     *
     * @param response callback func to send the response
     * @param vbucket vbucket that was mutated
     * @param bySeqno the seqno to send
     * @param status a mcbp status code
     * @param cas cas assigned to the mutation
     * @param cookie conn cookie
     * @returns NMVB if VB can't be located, or the ADD_RESPONSE return code.
     */
    cb::engine_errc sendMutationExtras(const AddResponseFn& response,
                                       Vbid vbucket,
                                       uint64_t bySeqno,
                                       cb::mcbp::Status status,
                                       uint64_t cas,
                                       CookieIface& cookie);

    /**
     * Factory method for constructing the correct bucket type given the
     * configuration.
     * @param config Configuration to create bucket based on. Note this
     *               object may be modified to ensure the config is valid
     *               for the selected bucket type.
     */
    std::unique_ptr<KVBucket> makeBucket(Configuration& config);

    /**
     * helper method so that some commands can set the datatype of the document.
     *
     * @param cookie connection cookie (used for JSON validation)
     * @param datatype the current document datatype
     * @param body a buffer containing the document body
     * @returns a datatype which will now include JSON if the document is JSON
     */
    protocol_binary_datatype_t checkForDatatypeJson(
            CookieIface& cookie,
            protocol_binary_datatype_t datatype,
            std::string_view body);

    /**
     * Process the set_with_meta with the given buffers/values.
     *
     * @param vbucket VB to mutate
     * @param key DocKey initialised with key data
     * @param value buffer for the mutation's value
     * @param itemMeta mutation's cas/revseq/flags/expiration
     * @param isDeleted the Item is deleted (with value)
     * @param datatype datatype of the mutation
     * @param cas [in,out] CAS for the command (updated with new CAS)
     * @param seqno [out] optional - returns the seqno allocated to the mutation
     * @param cookie connection's cookie
     * @param permittedVBStates set of VB states that the target VB can be in
     * @param checkConflicts set to Yes if conflict resolution must be done
     * @param allowExisting true if the set can overwrite existing key
     * @param genBySeqno generate a new seqno? (yes/no)
     * @param genCas generate a new CAS? (yes/no)
     * @param emd buffer referencing ExtendedMetaData
     * @returns state of the operation as an cb::engine_errc
     */
    cb::engine_errc setWithMeta(Vbid vbucket,
                                DocKeyView key,
                                cb::const_byte_buffer value,
                                ItemMetaData itemMeta,
                                bool isDeleted,
                                protocol_binary_datatype_t datatype,
                                uint64_t& cas,
                                uint64_t* seqno,
                                CookieIface& cookie,
                                PermittedVBStates permittedVBStates,
                                CheckConflicts checkConflicts,
                                bool allowExisting,
                                GenerateBySeqno genBySeqno,
                                GenerateCas genCas,
                                cb::const_byte_buffer emd);

    /**
     * Process the del_with_meta with the given buffers/values.
     *
     * @param vbucket VB to mutate
     * @param key DocKey initialised with key data
     * @param itemMeta mutation's cas/revseq/flags/expiration
     * @param cas [in,out] CAS for the command (updated with new CAS)
     * @param seqno [out] optional - returns the seqno allocated to the mutation
     * @param cookie connection's cookie
     * @param permittedVBStates set of VB states that the target VB can be in
     * @param checkConflicts set to Yes if conflict resolution must be done
     * @param genBySeqno generate a new seqno? (yes/no)
     * @param genCas generate a new CAS? (yes/no)
     * @param emd buffer referencing ExtendedMetaData
     * @param deleteSource whether it should delete or expire
     * @returns state of the operation as an cb::engine_errc
     */
    cb::engine_errc deleteWithMeta(Vbid vbucket,
                                   DocKeyView key,
                                   ItemMetaData itemMeta,
                                   uint64_t& cas,
                                   uint64_t* seqno,
                                   CookieIface& cookie,
                                   PermittedVBStates permittedVBStates,
                                   CheckConflicts checkConflicts,
                                   GenerateBySeqno genBySeqno,
                                   GenerateCas genCas,
                                   cb::const_byte_buffer emd,
                                   DeleteSource deleteSource);

    /// Make a DocKey from the key buffer
    DocKeyView makeDocKey(CookieIface& cookie, cb::const_byte_buffer key) const;

    cb::engine_errc processUnknownCommandInner(CookieIface& cookie,
                                               const cb::mcbp::Request& request,
                                               const AddResponseFn& response);

    cb::engine_errc unmountVBucketInner(Vbid vbid);
    std::pair<cb::engine_errc, nlohmann::json> getFusionStorageSnapshotInner(
            Vbid vbid, std::string_view snapshotUuid, std::time_t validity);
    cb::engine_errc releaseFusionStorageSnapshotInner(
            Vbid vbid, std::string_view snapshotUuid);
    cb::engine_errc syncFusionLogstoreInner(Vbid vbid);
    cb::engine_errc startFusionUploaderInner(Vbid vbid, uint64_t term);
    cb::engine_errc stopFusionUploaderInner(Vbid vbid);

    /**
     * Get the configured shard count for the bucket
     * @return shard count
     */
    size_t getShardCount();

    /**
     * Attempt to read the shard count from disk
     * @return empty optional if it does not exist
     */
    std::optional<size_t> getShardCountFromDisk();

    /**
     * Save the shard count to disk if it does not exist
     * @param workload
     */
    void maybeSaveShardCount(WorkLoadPolicy& workload);

    /**
     * Do checks for get-all-vbseqnos, which can be executed based on
     * Read for specific cases.
     *
     * A collection aware client must have Read for the requested collection ID,
     * or at least Read for one collection/scope if requesting "bucket wide"
     * seqno.
     *
     * A client which isn't collection aware must have Read access to the
     * default collection.
     *
     * @param cookie connection cookie
     * @param collection optional collection for when the command operates with
     *        a collection
     * @return status success or no_access
     */
    cb::engine_errc doGetAllVbSeqnosPrivilegeCheck(
            CookieIface& cookie, std::optional<CollectionID> collection);

    /**
     * Return a status code for most READ operations. This remaps some status
     * codes to temporary_failure.
     *    status == not_my_vbucket && isDegradedMode => tmp_fail
     *    status == no_such_key && isDegradedMode && ValueEvict => tmp_fail
     * @return status remapped based on isDegradedMode and/or eviction policy
     */
    cb::engine_errc maybeRemapStatus(cb::engine_errc status);

    /**
     * Helper method for maybeRemapStatus in the NMVB case.
     *
     * @return true when a NMVB must be remapped to a temporary failure.
     */
    bool mustRemapNMVB() const;

    /**
     * Check the access for the given privilege for the
     * bucket.scope.collection
     *
     * NOTE: The caller must check the appropriate System collection
     * privilege if SID is represents a system collection.
     */
    static cb::engine_errc checkPrivilege(CookieIface& cookie,
                                          cb::rbac::Privilege priv,
                                          ScopeID sid,
                                          CollectionID cid);

private:
    void iteratePendingDiskOps(
            const std::function<
                    void(bool isDataWrite,
                         std::chrono::microseconds adjustedElapsed)>& callback);

    void doEngineStatsCouchDB(const StatCollector& collector,
                              const EPStats& epstats);
    void doEngineStatsMagma(const StatCollector& collector);
    void doEngineStatsFusion(const StatCollector& collector);

    cb::EngineErrorGetCollectionIDResult parseKeyStatCollection(
            std::string_view expectedStatPrefix,
            std::string_view statKeyArg,
            std::string_view collectionStr);

    std::tuple<cb::engine_errc,
               std::optional<Vbid>,
               std::optional<std::string>,
               std::optional<CollectionID>>
    parseStatKeyArg(CookieIface& cookie,
                    std::string_view statKeyPrefix,
                    std::string_view statKey);

    /**
     * Set the concurrency of range-scans (no-op if bucket is ephemeral)
     * @param rangeScanMaxContinueTasksValue The configuration value where 0
     *        means auto configure
     */
    void configureRangeScanConcurrency(size_t rangeScanMaxContinueTasksValue);

    /**
     * Push the new config value through to the range scan module
     * @param rangeScanMaxDuration The configuration value
     */
    void configureRangeScanMaxDuration(
            std::chrono::seconds rangeScanMaxDuration);

protected:
    // Responsible for owning engine's memory arena and switching current
    // thread to this engine asap on EpEngine creation. See EpEngineArenaHelper
    // for details.
    EpEngineArenaHelper arenaHelper;

    // Bucket name. Second member variable (after arenaHelper) as it is referred
    // to by other member variables dtors (e.g. DcpConnMap) and hence should
    // stay valid for the majority of EpEngine's ctor.
    std::string name;

public:
    // Testing hook for MB-45756, to allow a throw to be made during
    // destroyInner() to simulate a crash while waiting for the flusher to
    // finish
    TestingHook<> epDestroyFailureHook;

    // Testing hook for MB-45654, to allow for a sleep in between the backfill
    // context being created and backfill starting. To allow a test to
    // artificially increase backfill's duration.
    TestingHook<> visitWarmupHook;

protected:
    ServerApi* serverApi;
    FileOpsTracker* fileOpsTracker{nullptr};

    // Engine statistics. First concrete member as a number of other members
    // refer to it so needs to be constructed first (and destructed last).
    EPStats stats;
    /**
     * Engine configuration. Referred to by various other members
     * (e.g. dcpConnMap_) so needs to be constructed before (and destructed
     * after) them.
     */
    Configuration configuration;
    /**
     * Mutex to lock setting parameters, which serialises execution of all
     * setParameter calls. This also ensures all listeners are serialised,
     * simplifying the implementation of them.
     */
    std::mutex setParameterMutex;

    /**
     * Interface used for OOM conditions and triggering memory reclamation. For
     * quota-sharing engines, the memory usage of the entire quota-sharing
     * group will be considered.
     */
    std::unique_ptr<MemoryTracker> memoryTracker;
    std::unique_ptr<KVBucket> kvBucket;
    std::unique_ptr<WorkLoadPolicy> workload;
    bucket_priority_t workloadPriority;

    // The conflict resolution mode for this bucket (as used by XDCR via
    // SET/DEL_WITH_META operations).
    ConflictResolutionMode conflictResolutionMode;
    GET_SERVER_API getServerApiFunc;

    std::unique_ptr<DcpFlowControlManager> dcpFlowControlManager;

    std::unique_ptr<DcpConnMap> dcpConnMap_;
    std::unique_ptr<CheckpointConfig> checkpointConfig;
    size_t maxItemSize;
    size_t maxItemPrivilegedBytes;
    std::chrono::seconds getlDefaultTimeout;
    std::chrono::seconds getlMaxTimeout;
    size_t maxFailoverEntries;
    std::atomic<bool> trafficEnabled;
    // Indicates whether we are sharing quota with any other buckets.
    std::atomic_bool isCrossBucketHtQuotaSharing;

    // a unique system generated token initialized at each time
    // ep_engine starts up.
    std::atomic<time_t> startupTime;
    EpEngineTaskable taskable;
    std::atomic<BucketCompressionMode> compressionMode;
    std::atomic<float> minCompressionRatio;

    /**
     * Whether del-operations at EPE level (currently only DelWithMeta) should
     * just sanitize invalid payloads or fail the operation if an invalid
     * payload is detected.
     * Non-const as the related configuration param is dynamic.
     */
    std::atomic_bool allowSanitizeValueInDeletion;

    /**
     * If true, then the server will return tmpfail instead of a not_locked
     * error where possible. Note this is the bucket-level setting, but the
     * node-level setting should also be consulted.
     */
    cb::RelaxedAtomic<bool> notLockedReturnsTmpfail{false};

    /**
     * Should we validate that the key - vBucket mapping is correct?
     */
    std::atomic_bool sanityCheckVBucketMapping{false};

    /**
     * The method in which errors are handled should the key - vBucket mapping
     * be incorrect.
     */
    std::atomic<cb::ErrorHandlingMethod> vBucketMappingErrorHandlingMethod;

    /// The encryption key provider used to store keyAuthTs and notify others
    /// when the list of keys change
    EncryptionKeyProvider encryptionKeyProvider;

    // Chronicle auth token. Cached at bucket creation for passing down the
    // information to KVStore instantiation.
    folly::Synchronized<std::string> chronicleAuthToken;
};

/**
 * A configuration value changed listener that responds to ep-engine
 * parameter changes by invoking engine-specific methods on
 * configuration change events.
 */
class EpEngineValueChangeListener : public ValueChangedListener {
public:
    explicit EpEngineValueChangeListener(EventuallyPersistentEngine& e);

    void sizeValueChanged(std::string_view key, size_t value) override;
    void stringValueChanged(std::string_view key, const char* value) override;
    void floatValueChanged(std::string_view key, float value) override;
    void booleanValueChanged(std::string_view key, bool b) override;

private:
    EventuallyPersistentEngine& engine;
};
