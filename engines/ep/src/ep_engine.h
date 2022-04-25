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

#include "configuration.h"
#include "ep_engine_public.h"
#include "permitted_vb_states.h"
#include "stats.h"
#include "storeddockey.h"
#include "vbucket_fwd.h"

#include <executor/taskable.h>
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

class CheckpointConfig;
struct CompactionConfig;
struct ConnCounter;
class ConnHandler;
enum class ConflictResolutionMode;
class DcpConnMap;
class DcpFlowControlManager;
class ItemMetaData;
class KVBucket;
class PrometheusStatCollector;
class StatCollector;
class StoredValue;
class VBucketCountVisitor;

// Forward decl
class EventuallyPersistentEngine;
class ReplicationThrottle;

namespace cb::prometheus {
enum class Cardinality;
} // namespace cb::prometheus

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
                  std::chrono::steady_clock::duration enqTime) override;

    void logRunTime(const GlobalTask& task,
                    std::string_view threadName,
                    std::chrono::steady_clock::duration runTime) override;

    bool isShutdown() const override;

private:
    EventuallyPersistentEngine* myEngine;
};

/**
 * memcached engine interface to the KVBucket.
 */
class EventuallyPersistentEngine : public EngineIface, public DcpIface {
    friend class LookupCallback;
public:
    cb::engine_errc initialize(const std::string& config) override;
    void destroy(bool force) override;
    void disconnect(const CookieIface& cookie) override;

    void set_num_reader_threads(ThreadPoolConfig::ThreadCount num) override;
    void set_num_writer_threads(ThreadPoolConfig::ThreadCount num) override;
    void set_num_storage_threads(
            ThreadPoolConfig::StorageThreadCount num) override;

    std::pair<cb::unique_item_ptr, item_info> allocateItem(
            const CookieIface& cookie,
            const DocKey& key,
            size_t nbytes,
            size_t priv_nbytes,
            int flags,
            rel_time_t exptime,
            uint8_t datatype,
            Vbid vbucket) override;

    cb::engine_errc remove(
            const CookieIface& cookie,
            const DocKey& key,
            uint64_t& cas,
            Vbid vbucket,
            const std::optional<cb::durability::Requirements>& durability,
            mutation_descr_t& mut_info) override;

    void release(ItemIface& itm) override;

    cb::EngineErrorItemPair get(const CookieIface& cookie,
                                const DocKey& key,
                                Vbid vbucket,
                                DocStateFilter documentStateFilter) override;
    cb::EngineErrorItemPair get_if(
            const CookieIface& cookie,
            const DocKey& key,
            Vbid vbucket,
            std::function<bool(const item_info&)> filter) override;

    cb::EngineErrorMetadataPair get_meta(const CookieIface& cookie,
                                         const DocKey& key,
                                         Vbid vbucket) override;

    cb::EngineErrorItemPair get_locked(const CookieIface& cookie,
                                       const DocKey& key,
                                       Vbid vbucket,
                                       uint32_t lock_timeout) override;

    cb::engine_errc unlock(const CookieIface& cookie,
                           const DocKey& key,
                           Vbid vbucket,
                           uint64_t cas) override;

    cb::EngineErrorItemPair get_and_touch(
            const CookieIface& cookie,
            const DocKey& key,
            Vbid vbucket,
            uint32_t expirytime,
            const std::optional<cb::durability::Requirements>& durability)
            override;

    cb::engine_errc store(
            const CookieIface& cookie,
            ItemIface& item,
            uint64_t& cas,
            StoreSemantics operation,
            const std::optional<cb::durability::Requirements>& durability,
            DocumentState document_state,
            bool preserveTtl) override;

    cb::EngineErrorCasPair store_if(
            const CookieIface& cookie,
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

    cb::engine_errc get_stats(const CookieIface& cookie,
                              std::string_view key,
                              std::string_view value,
                              const AddStatFn& add_stat) override;

    cb::engine_errc get_prometheus_stats(
            const BucketStatCollector& collector,
            cb::prometheus::Cardinality cardinality) override;

    void reset_stats(const CookieIface& cookie) override;

    cb::engine_errc unknown_command(const CookieIface* cookie,
                                    const cb::mcbp::Request& request,
                                    const AddResponseFn& response) override;

    void item_set_cas(ItemIface& item, uint64_t cas) override;

    void item_set_datatype(ItemIface& item,
                           protocol_binary_datatype_t datatype) override;

    bool get_item_info(const ItemIface& item, item_info& item_info) override;

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

    bool isXattrEnabled() override;

    BucketCompressionMode getCompressionMode() override {
        return compressionMode;
    }

    size_t getMaxItemSize() override {
        return maxItemSize;
    }

    float getMinCompressionRatio() override {
        return minCompressionRatio;
    }

    // DcpIface implementation ////////////////////////////////////////////////

    cb::engine_errc step(const CookieIface& cookie,
                         DcpMessageProducersIface& producers) override;

    cb::engine_errc open(const CookieIface& cookie,
                         uint32_t opaque,
                         uint32_t seqno,
                         uint32_t flags,
                         std::string_view name,
                         std::string_view value = {}) override;

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
    cb::engine_errc itemDelete(
            const CookieIface* cookie,
            const DocKey& key,
            uint64_t& cas,
            Vbid vbucket,
            std::optional<cb::durability::Requirements> durability,
            mutation_descr_t& mut_info);

    void itemRelease(ItemIface* itm);

    cb::EngineErrorItemPair getInner(const CookieIface* cookie,
                                     const DocKey& key,
                                     Vbid vbucket,
                                     get_options_t options);

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
            const CookieIface* cookie,
            const DocKey& key,
            Vbid vbucket,
            std::function<bool(const item_info&)> filter);

    cb::EngineErrorItemPair getAndTouchInner(const CookieIface* cookie,
                                             const DocKey& key,
                                             Vbid vbucket,
                                             uint32_t expiry_time);

    cb::EngineErrorItemPair getLockedInner(const CookieIface* cookie,
                                           const DocKey& key,
                                           Vbid vbucket,
                                           uint32_t lock_timeout);

    cb::engine_errc unlockInner(const CookieIface* cookie,
                                const DocKey& key,
                                Vbid vbucket,
                                uint64_t cas);

    const std::string& getName() const {
        return name;
    }

    cb::engine_errc getStats(const void* cookie,
                             std::string_view key,
                             std::string_view value,
                             const AddStatFn& add_stat);

    void resetStats();

    cb::engine_errc storeInner(const CookieIface* cookie,
                               Item& itm,
                               uint64_t& cas,
                               StoreSemantics operation,
                               bool preserveTtl);

    cb::EngineErrorCasPair storeIfInner(const CookieIface* cookie,
                                        Item& itm,
                                        uint64_t cas,
                                        StoreSemantics operation,
                                        const cb::StoreIfPredicate& predicate,
                                        bool preserveTtl);

    cb::engine_errc dcpOpen(const CookieIface* cookie,
                            uint32_t opaque,
                            uint32_t seqno,
                            uint32_t flags,
                            std::string_view stream_name,
                            std::string_view value);

    cb::engine_errc dcpAddStream(const CookieIface* cookie,
                                 uint32_t opaque,
                                 Vbid vbucket,
                                 uint32_t flags);

    cb::EngineErrorMetadataPair getMetaInner(const CookieIface* cookie,
                                             const DocKey& key,
                                             Vbid vbucket);

    cb::engine_errc setWithMeta(const CookieIface* cookie,
                                const cb::mcbp::Request& request,
                                const AddResponseFn& response);

    cb::engine_errc deleteWithMeta(const CookieIface* cookie,
                                   const cb::mcbp::Request& request,
                                   const AddResponseFn& response);

    cb::engine_errc returnMeta(const CookieIface* cookie,
                               const cb::mcbp::Request& req,
                               const AddResponseFn& response);

    cb::engine_errc getAllKeys(const CookieIface* cookie,
                               const cb::mcbp::Request& request,
                               const AddResponseFn& response);

    ConnectionPriority getDCPPriority(const CookieIface* cookie);

    void setDCPPriority(const CookieIface* cookie, ConnectionPriority priority);

    void notifyIOComplete(const CookieIface* cookie, cb::engine_errc status);
    void scheduleDcpStep(const CookieIface& cookie);

    void reserveCookie(const CookieIface* cookie);
    void releaseCookie(const CookieIface* cookie);

    void storeEngineSpecific(const CookieIface* cookie, void* engine_data);

    void* getEngineSpecific(const CookieIface* cookie);

    bool isDatatypeSupported(const CookieIface* cookie,
                             protocol_binary_datatype_t datatype);

    bool isXattrEnabled(const CookieIface* cookie);

    void setErrorContext(const CookieIface* cookie, std::string_view message);

    void setUnknownCollectionErrorContext(const CookieIface* cookie,
                                          uint64_t manifestUid) const;

    template <typename T>
    void notifyIOComplete(T cookies, cb::engine_errc status);

    void handleDisconnect(const CookieIface* cookie);
    void initiate_shutdown() override;
    void cancel_all_operations_in_ewb_state() override;

    cb::mcbp::Status stopFlusher(const char** msg, size_t* msg_size);

    cb::mcbp::Status startFlusher(const char** msg, size_t* msg_size);

    /// Schedule compaction (used by unit tests)
    cb::engine_errc scheduleCompaction(Vbid vbid,
                                       const CompactionConfig& c,
                                       const CookieIface* cookie);

    cb::mcbp::Status evictKey(const CookieIface* cookie,
                              const cb::mcbp::Request& request,
                              const char** msg);

    cb::engine_errc observe(const CookieIface* cookie,
                            const cb::mcbp::Request& request,
                            const AddResponseFn& response);

    cb::engine_errc observe_seqno(const CookieIface* cookie,
                                  const cb::mcbp::Request& request,
                                  const AddResponseFn& response);

    VBucketPtr getVBucket(Vbid vbucket) const;

    cb::engine_errc setVBucketState(const CookieIface* cookie,
                                    Vbid vbid,
                                    vbucket_state_t to,
                                    const nlohmann::json* meta,
                                    TransferVB transfer,
                                    uint64_t cas);

    cb::engine_errc setParameter(const CookieIface& cookie,
                                 EngineParamCategory category,
                                 std::string_view key,
                                 std::string_view value,
                                 Vbid vbucket) override;

    cb::engine_errc setParameterInner(const CookieIface& cookie,
                                      EngineParamCategory category,
                                      std::string_view key,
                                      std::string_view value,
                                      Vbid vbucket);

    cb::engine_errc setFlushParam(const std::string& key,
                                  const std::string& val,
                                  std::string& msg);

    cb::engine_errc setReplicationParam(const std::string& key,
                                        const std::string& val,
                                        std::string& msg);

    cb::engine_errc setCheckpointParam(const std::string& key,
                                       const std::string& val,
                                       std::string& msg);

    cb::engine_errc setDcpParam(const std::string& key,
                                const std::string& val,
                                std::string& msg);

    cb::engine_errc setVbucketParam(Vbid vbucket,
                                    const std::string& key,
                                    const std::string& val,
                                    std::string& msg);

    cb::engine_errc getReplicaCmd(const cb::mcbp::Request& request,
                                  const AddResponseFn& response,
                                  const CookieIface* cookie);

    ~EventuallyPersistentEngine() override;

    EPStats& getEpStats() {
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
        return *dcpFlowControlManager_;
    }

    /**
     * Returns the replication throttle instance
     *
     * @return Ref to replication throttle
     */
    ReplicationThrottle& getReplicationThrottle();

    CheckpointConfig& getCheckpointConfig() {
        return *checkpointConfig;
    }

    ServerApi* getServerApi() {
        return serverApi;
    }

    Configuration& getConfiguration() {
        return configuration;
    }

    cb::engine_errc handleLastClosedCheckpoint(const CookieIface* cookie,
                                               const cb::mcbp::Request& request,
                                               const AddResponseFn& response);
    cb::engine_errc handleCreateCheckpoint(const CookieIface* cookie,
                                           const cb::mcbp::Request& request,
                                           const AddResponseFn& response);

    cb::engine_errc handleSeqnoPersistence(const CookieIface* cookie,
                                           const cb::mcbp::Request& req,
                                           const AddResponseFn& response);

    cb::engine_errc handleTrafficControlCmd(const CookieIface* cookie,
                                            const cb::mcbp::Request& request,
                                            const AddResponseFn& response);

    size_t getGetlDefaultTimeout() const {
        return getlDefaultTimeout;
    }

    size_t getGetlMaxTimeout() const {
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

    cb::engine_errc getRandomKey(const CookieIface* cookie,
                                 const cb::mcbp::Request& request,
                                 const AddResponseFn& response);

    void setCompressionMode(const std::string& compressModeStr);

    void setConflictResolutionMode(std::string_view mode);

    void setMinCompressionRatio(float minCompressRatio) {
        minCompressionRatio = minCompressRatio;
    }

    /**
     * Get the connection handler for the provided cookie
     *
     * @param cookie the cookie to look up
     * @return the pointer to the connection handler if found, nullptr otherwise
     */
    ConnHandler* tryGetConnHandler(const CookieIface* cookie);

    /**
     * Get the connection handler for the provided cookie. This method differs
     * from the tryGetConnHandler that it expects the conn handler to exist
     * and will throw an exception if there isn't.
     *
     * In most cases where we want to get the connection handlers we're called
     * as part of the command handler for DCP, and the core has already
     * checked that we're called on a DCP connection (and the handler should
     * be there) and return an error back to the caller if it isn't
     *
     * @param cookie the cookie to look up
     * @return the connection handler
     * @throws std::logic_error if the cookie don't have a connection handler
     */
    ConnHandler& getConnHandler(const CookieIface* cookie);

    /**
     * Method to add a cookie to allKeysLookups to store the result of the
     * getAllKeys() request.
     * @param cookie the cookie that the getAllKeys() was processed for
     * @param err Engine error code of the result of getAllKeys()
     */
    void addLookupAllKeys(const CookieIface* cookie, cb::engine_errc err);

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
            const CookieIface* cookie,
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

    cb::EngineErrorItemPair itemAllocate(const DocKey& key,
                                         const size_t nbytes,
                                         const size_t priv_nbytes,
                                         const int flags,
                                         rel_time_t exptime,
                                         uint8_t datatype,
                                         Vbid vbucket);

    cb::engine::FeatureSet getFeatures() override;

    /**
     * Set the bucket's maximum data size (aka quota).
     * This method propagates the value to other components that need to
     * know the bucket's quota and is used on initialisation and dynamic
     * changes.
     *
     * @param size in bytes for the bucket
     */
    void setMaxDataSize(size_t size);

    cb::ArenaMallocClient& getArenaMallocClient() {
        return arena;
    }

    cb::HlcTime getVBucketHlcNow(Vbid vbucket) override;

    cb::engine_errc checkForPrivilegeAtLeastInOneCollection(
            const CookieIface& cookie, cb::rbac::Privilege privilege) const;

    /**
     * Check the access for the given privilege for the bucket.scope.collection
     */
    cb::engine_errc checkPrivilege(const CookieIface* cookie,
                                   cb::rbac::Privilege priv,
                                   std::optional<ScopeID> sid,
                                   std::optional<CollectionID> cid) const;

    /**
     * Test the access for the given privilege for the bucket.scope.collection
     * This differs from checkPrivilege in that the error case has no side
     * effect, such as setting error extras/logging
     */
    cb::engine_errc testPrivilege(const CookieIface& cookie,
                                  cb::rbac::Privilege priv,
                                  std::optional<ScopeID> sid,
                                  std::optional<CollectionID> cid) const;

    /**
     * @return the privilege revision, which changes when privileges do.
     */
    uint32_t getPrivilegeRevision(const CookieIface* cookie) const;

    cb::engine_errc compactDatabase(const CookieIface& cookie,
                                    Vbid vbid,
                                    uint64_t purge_before_ts,
                                    uint64_t purge_before_seq,
                                    bool drop_deletes) override;

    std::pair<cb::engine_errc, vbucket_state_t> getVBucket(
            const CookieIface& cookie, Vbid vbid) override;
    cb::engine_errc setVBucket(const CookieIface& cookie,
                               Vbid vbid,
                               uint64_t cas,
                               vbucket_state_t state,
                               nlohmann::json* meta) override;
    cb::engine_errc deleteVBucket(const CookieIface& cookie,
                                  Vbid vbid,
                                  bool sync) override;

    std::pair<cb::engine_errc, cb::rangescan::Id> createRangeScan(
            const CookieIface& cookie,
            Vbid vbid,
            CollectionID cid,
            cb::rangescan::KeyView start,
            cb::rangescan::KeyView end,
            cb::rangescan::KeyOnly keyOnly,
            std::optional<cb::rangescan::SnapshotRequirements> snapshotReqs,
            std::optional<cb::rangescan::SamplingConfiguration> samplingConfig)
            override;
    cb::engine_errc continueRangeScan(
            const CookieIface& cookie,
            Vbid vbid,
            cb::rangescan::Id uuid,
            size_t itemLimit,
            std::chrono::milliseconds timeLimit) override;
    cb::engine_errc cancelRangeScan(const CookieIface& cookie,
                                    Vbid vbid,
                                    cb::rangescan::Id uuid) override;

protected:
    friend class EpEngineValueChangeListener;

    cb::engine_errc compactDatabaseInner(const CookieIface& cookie,
                                         Vbid vbid,
                                         uint64_t purge_before_ts,
                                         uint64_t purge_before_seq,
                                         bool drop_deletes);

    std::pair<cb::engine_errc, vbucket_state_t> getVBucketInner(
            const CookieIface& cookie, Vbid vbid);
    cb::engine_errc setVBucketInner(const CookieIface& cookie,
                                    Vbid vbid,
                                    uint64_t cas,
                                    vbucket_state_t state,
                                    nlohmann::json* meta);
    cb::engine_errc deleteVBucketInner(const CookieIface& cookie,
                                       Vbid vbid,
                                       bool sync);

    cb::engine_errc checkPrivilege(const CookieIface* cookie,
                                   cb::rbac::Privilege priv,
                                   CollectionID) const;

    /**
     * Check the access for the given privilege for the given key
     */
    cb::engine_errc checkPrivilege(const CookieIface* cookie,
                                   cb::rbac::Privilege priv,
                                   DocKey key) const;

    void setMaxItemSize(size_t value) {
        maxItemSize = value;
    }

    void setMaxItemPrivilegedBytes(size_t value) {
        maxItemPrivilegedBytes = value;
    }

    void setGetlDefaultTimeout(size_t value) {
        getlDefaultTimeout = value;
    }

    void setGetlMaxTimeout(size_t value) {
        getlMaxTimeout = value;
    }

    EventuallyPersistentEngine(GET_SERVER_API get_server_api,
                               cb::ArenaMallocClient arena);
    friend cb::engine_errc create_ep_engine_instance(
            GET_SERVER_API get_server_api, EngineIface** handle);
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
    bool hasMemoryForItemAllocation(uint32_t totalItemSize);

    friend class KVBucket;
    friend class EPBucket;

    bool enableTraffic(bool enable);

    /**
     * Collect all low and high cardinality Engine stats.
     *
     * Used to generate the cbstats "empty key" group.
     */
    cb::engine_errc doEngineStats(const BucketStatCollector& collector);
    /**
     * Collect a smaller number of engine stats which are of higher importance,
     * or specifically need to be collected frequently.
     */
    cb::engine_errc doEngineStatsLowCardinality(
            const BucketStatCollector& collector);
    /**
     * Collect engine stats which are any of:
     *  1) Numerous (e.g., per collection, per vbucket)
     *  2) Expensive to gather
     *  3) Not necessary to record with very fine temporal granularity
     *     (e.g., rarely changing, not of critical debugging value)
     */
    cb::engine_errc doEngineStatsHighCardinality(
            const BucketStatCollector& collector);

    cb::engine_errc doMemoryStats(const CookieIface* cookie,
                                  const AddStatFn& add_stat);
    cb::engine_errc doVBucketStats(const CookieIface* cookie,
                                   const AddStatFn& add_stat,
                                   const char* stat_key,
                                   int nkey,
                                   VBucketStatsDetailLevel detail);
    cb::engine_errc doHashStats(const CookieIface* cookie,
                                const AddStatFn& add_stat);
    cb::engine_errc doHashDump(const CookieIface* cookie,
                               const AddStatFn& addStat,
                               std::string_view keyArgs);
    cb::engine_errc doCheckpointStats(const CookieIface* cookie,
                                      const AddStatFn& add_stat,
                                      const char* stat_key,
                                      int nkey);
    cb::engine_errc doCheckpointDump(const CookieIface* cookie,
                                     const AddStatFn& addStat,
                                     std::string_view keyArgs);
    cb::engine_errc doDurabilityMonitorStats(const CookieIface* cookie,
                                             const AddStatFn& add_stat,
                                             const char* stat_key,
                                             int nkey);
    cb::engine_errc doDurabilityMonitorDump(const CookieIface* cookie,
                                            const AddStatFn& addStat,
                                            std::string_view keyArgs);
    cb::engine_errc doVBucketDump(const CookieIface* cookie,
                                  const AddStatFn& addStat,
                                  std::string_view keyArgs);
    /**
     * Immediately collect DCP stats, without scheduling a background task.
     */
    cb::engine_errc doDcpStats(const CookieIface* cookie,
                               const AddStatFn& add_stat,
                               std::string_view value);

    void addAggregatedProducerStats(const BucketStatCollector& collector,
                                    const ConnCounter& aggregator);
    cb::engine_errc doEvictionStats(const CookieIface* cookie,
                                    const AddStatFn& add_stat);
    cb::engine_errc doConnAggStats(const BucketStatCollector& collector,
                                   std::string_view sep);
    void doTimingStats(const BucketStatCollector& collector);
    cb::engine_errc doSchedulerStats(const CookieIface* cookie,
                                     const AddStatFn& add_stat);
    cb::engine_errc doRunTimeStats(const CookieIface* cookie,
                                   const AddStatFn& add_stat);
    cb::engine_errc doDispatcherStats(const CookieIface* cookie,
                                      const AddStatFn& add_stat);
    cb::engine_errc doTasksStats(const CookieIface* cookie,
                                 const AddStatFn& add_stat);
    cb::engine_errc doKeyStats(const CookieIface* cookie,
                               const AddStatFn& add_stat,
                               Vbid vbid,
                               const DocKey& key,
                               bool validate = false);

    cb::engine_errc doDcpVbTakeoverStats(const CookieIface* cookie,
                                         const AddStatFn& add_stat,
                                         std::string& key,
                                         Vbid vbid);
    cb::engine_errc doVbIdFailoverLogStats(const CookieIface* cookie,
                                           const AddStatFn& add_stat,
                                           Vbid vbid);
    cb::engine_errc doAllFailoverLogStats(const CookieIface* cookie,
                                          const AddStatFn& add_stat);
    cb::engine_errc doWorkloadStats(const CookieIface* cookie,
                                    const AddStatFn& add_stat);
    cb::engine_errc doSeqnoStats(const CookieIface* cookie,
                                 const AddStatFn& add_stat,
                                 const char* stat_key,
                                 int nkey);
    void addSeqnoVbStats(const CookieIface* cookie,
                         const AddStatFn& add_stat,
                         const VBucketPtr& vb);

    cb::engine_errc doCollectionStats(const CookieIface* cookie,
                                      const AddStatFn& add_stat,
                                      const std::string& statKey);

    cb::engine_errc doScopeStats(const CookieIface* cookie,
                                 const AddStatFn& add_stat,
                                 const std::string& statKey);

    cb::engine_errc doKeyStats(const CookieIface* cookie,
                               const AddStatFn& add_stat,
                               std::string_view statKey);

    cb::engine_errc doVKeyStats(const CookieIface* cookie,
                                const AddStatFn& add_stat,
                                std::string_view statKey);

    cb::engine_errc doDcpVbTakeoverStats(const CookieIface* cookie,
                                         const AddStatFn& add_stat,
                                         std::string_view statKey);

    cb::engine_errc doFailoversStats(const CookieIface* cookie,
                                     const AddStatFn& add_stat,
                                     std::string_view statKey);

    cb::engine_errc doDiskinfoStats(const CookieIface* cookie,
                                    const AddStatFn& add_stat,
                                    std::string_view statKey);

    void doDiskFailureStats(const BucketStatCollector& collector);

    cb::engine_errc doPrivilegedStats(const CookieIface* cookie,
                                      const AddStatFn& add_stat,
                                      std::string_view statKey);

    void addLookupResult(const CookieIface* cookie,
                         std::unique_ptr<Item> result);

    bool fetchLookupResult(const CookieIface* cookie,
                           std::unique_ptr<Item>& itm);

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

    /**
     * Private helper method for decoding the options on set/del_with_meta.
     * Tighly coupled to the logic of both those functions, it will
     * take a request pointer and locate and validate any options within.
     *
     * @param extras byte buffer containing the incoming meta extras
     * @param generateCas[out] set to Yes if CAS regeneration is enabled.
     * @param checkConflicts[out] set to No if conflict resolution should
     *        not be performed.
     * @param permittedVBStates[out] updates with replica and pending if the
     *        options contain force.
     * @param deleteSource Changes to TTL if IS_EXPIRATION flag (del_with_meta)
     * @return true if everything is OK, false for an invalid combination of
     *              options
     */
    bool decodeWithMetaOptions(cb::const_byte_buffer extras,
                               GenerateCas& generateCas,
                               CheckConflicts& checkConflicts,
                               PermittedVBStates& permittedVBStates,
                               DeleteSource& deleteSource);

    /**
     * Private wrapper method for decodeWithMetaOptions called from setWithMeta
     * to abstract out deleteSource, which is unused by setWithMeta.
     * @return true if everything is OK, false for an invalid combination of
     *              options
     */
    bool decodeSetWithMetaOptions(cb::const_byte_buffer extras,
                                  GenerateCas& generateCas,
                                  CheckConflicts& checkConflicts,
                                  PermittedVBStates& permittedVBStates);

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
    cb::engine_errc sendErrorResponse(const AddResponseFn& response,
                                      cb::mcbp::Status status,
                                      uint64_t cas,
                                      const CookieIface* cookie);

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
                                       const CookieIface* cookie);

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
     * @param cookie connection cookie
     * @param datatype the current document datatype
     * @param body a buffer containing the document body
     * @returns a datatype which will now include JSON if the document is JSON
     *          and the connection does not support datatype JSON.
     */
    protocol_binary_datatype_t checkForDatatypeJson(
            const CookieIface* cookie,
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
                                DocKey key,
                                cb::const_byte_buffer value,
                                ItemMetaData itemMeta,
                                bool isDeleted,
                                protocol_binary_datatype_t datatype,
                                uint64_t& cas,
                                uint64_t* seqno,
                                const CookieIface* cookie,
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
                                   DocKey key,
                                   ItemMetaData itemMeta,
                                   uint64_t& cas,
                                   uint64_t* seqno,
                                   const CookieIface* cookie,
                                   PermittedVBStates permittedVBStates,
                                   CheckConflicts checkConflicts,
                                   GenerateBySeqno genBySeqno,
                                   GenerateCas genCas,
                                   cb::const_byte_buffer emd,
                                   DeleteSource deleteSource);

    /**
     * Make a DocKey from the key buffer
     */
    DocKey makeDocKey(const CookieIface* cookie, cb::const_byte_buffer key);

    cb::engine_errc processUnknownCommandInner(const CookieIface* cookie,
                                               const cb::mcbp::Request& request,
                                               const AddResponseFn& response);

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
            const CookieIface* cookie, std::optional<CollectionID> collection);

private:
    void doEngineStatsCouchDB(const StatCollector& collector,
                              const EPStats& epstats);
    void doEngineStatsMagma(const StatCollector& collector);
    void doEngineStatsRocksDB(const StatCollector& collector);

    cb::EngineErrorGetCollectionIDResult parseKeyStatCollection(
            std::string_view expectedStatPrefix,
            std::string_view statKeyArg,
            std::string_view collectionStr);

    std::tuple<cb::engine_errc,
               std::optional<Vbid>,
               std::optional<std::string>,
               std::optional<CollectionID>>
    parseStatKeyArg(const CookieIface* cookie,
                    std::string_view statKeyPrefix,
                    std::string_view statKey);

public:
    // Testing hook for MB-45756, to allow a throw to be made during
    // destroyInner() to simulate a crash while waiting for the flusher to
    // finish
    TestingHook<> epDestroyFailureHook;

    // Testing hook for MB-45654, to allow for a sleep in between the backfill
    // context being created and backfill starting. To allow a test to
    // artificially increase backfill's duration.
    TestingHook<> hangWarmupHook;

protected:
    ServerApi* serverApi;

    // Engine statistics. First concrete member as a number of other members
    // refer to it so needs to be constructed first (and destructed last).
    EPStats stats;
    /**
     * Engine configuration. Referred to by various other members
     * (e.g. dcpConnMap_) so needs to be constructed before (and destructed
     * after) them.
     */
    Configuration configuration;
    std::unique_ptr<KVBucket> kvBucket;
    std::unique_ptr<WorkLoadPolicy> workload;
    bucket_priority_t workloadPriority;

    // The conflict resolution mode for this bucket (as used by XDCR via
    // SET/DEL_WITH_META operations).
    ConflictResolutionMode conflictResolutionMode;

    std::map<const CookieIface*, std::unique_ptr<Item>> lookups;
    std::unordered_map<const CookieIface*, cb::engine_errc> allKeysLookups;
    std::mutex lookupMutex;
    GET_SERVER_API getServerApiFunc;

    std::unique_ptr<DcpFlowControlManager> dcpFlowControlManager_;
    std::unique_ptr<DcpConnMap> dcpConnMap_;
    std::unique_ptr<CheckpointConfig> checkpointConfig;
    std::string name;
    size_t maxItemSize;
    size_t maxItemPrivilegedBytes;
    size_t getlDefaultTimeout;
    size_t getlMaxTimeout;
    size_t maxFailoverEntries;
    std::atomic<bool> trafficEnabled;

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
     * Should we validate that the key - vBucket mapping is correct?
     */
    std::atomic_bool sanityCheckVBucketMapping{false};

    /**
     * The method in which errors are handled should the key - vBucket mapping
     * be incorrect.
     */
    std::atomic<cb::ErrorHandlingMethod> vBucketMappingErrorHandlingMethod;

    cb::ArenaMallocClient arena;
};
