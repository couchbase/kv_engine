/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013 Couchbase, Inc
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

#pragma once

#include "config.h"

#include "configuration.h"
#include "connhandler.h"
#include "item.h"
#include "stats.h"
#include "storeddockey.h"
#include "taskable.h"
#include "vb_visitors.h"

#include <memcached/engine.h>
#include <memcached/protocol_binary.h>
#include <memcached/server_callback_iface.h>
#include <platform/processclock.h>

#include <string>
#include <unordered_map>

class CheckpointConfig;
struct CompactionConfig;
class DcpConnMap;
class DcpFlowControlManager;
class KVBucket;
class StoredValue;
class VBucketCountVisitor;

extern "C" {
    MEMCACHED_PUBLIC_API
    ENGINE_ERROR_CODE create_instance(GET_SERVER_API get_server_api,
                                      EngineIface** handle);

    MEMCACHED_PUBLIC_API
    void destroy_engine(void);

    void EvpNotifyPendingConns(void*arg);
}

// Forward decl
class EventuallyPersistentEngine;
class ReplicationThrottle;

/**
    To allow Engines to run tasks.
**/
class EpEngineTaskable : public Taskable {
public:
    EpEngineTaskable(EventuallyPersistentEngine* e) : myEngine(e) {

    }

    const std::string& getName() const;

    task_gid_t getGID() const;

    bucket_priority_t getWorkloadPriority() const;

    void setWorkloadPriority(bucket_priority_t prio);

    WorkLoadPolicy& getWorkLoadPolicy();

    void logQTime(TaskId id, const ProcessClock::duration enqTime);

    void logRunTime(TaskId id, const ProcessClock::duration runTime);

private:
    EventuallyPersistentEngine* myEngine;
};

/**
 * memcached engine interface to the KVBucket.
 */
class EventuallyPersistentEngine : public EngineIface, public DcpIface {
    friend class LookupCallback;
public:
    ENGINE_ERROR_CODE initialize(const char* config) override;
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

    ENGINE_ERROR_CODE remove(gsl::not_null<const void*> cookie,
                             const DocKey& key,
                             uint64_t& cas,
                             Vbid vbucket,
                             mutation_descr_t& mut_info) override;

    void release(gsl::not_null<item*> itm) override;

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

    cb::EngineErrorItemPair get_and_touch(gsl::not_null<const void*> cookie,
                                          const DocKey& key,
                                          Vbid vbucket,
                                          uint32_t expirytime) override;

    ENGINE_ERROR_CODE store(gsl::not_null<const void*> cookie,
                            gsl::not_null<item*> item,
                            uint64_t& cas,
                            ENGINE_STORE_OPERATION operation,
                            DocumentState document_state) override;
    cb::EngineErrorCasPair store_if(gsl::not_null<const void*> cookie,
                                    gsl::not_null<item*> item,
                                    uint64_t cas,
                                    ENGINE_STORE_OPERATION operation,
                                    cb::StoreIfPredicate predicate,
                                    DocumentState document_state) override;

    // Need to explicilty import EngineIface::flush to avoid warning about
    // DCPIface::flush hiding it.
    using EngineIface::flush;

    ENGINE_ERROR_CODE get_stats(gsl::not_null<const void*> cookie,
                                cb::const_char_buffer key,
                                ADD_STAT add_stat) override;

    void reset_stats(gsl::not_null<const void*> cookie) override;

    ENGINE_ERROR_CODE unknown_command(
            const void* cookie,
            gsl::not_null<protocol_binary_request_header*> request,
            ADD_RESPONSE response) override;

    void item_set_cas(gsl::not_null<item*> item, uint64_t cas) override;

    void item_set_datatype(gsl::not_null<item*> item,
                           protocol_binary_datatype_t datatype) override;

    bool get_item_info(gsl::not_null<const item*> item,
                       gsl::not_null<item_info*> item_info) override;

    void set_log_level(spdlog::level::level_enum level) override;

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

    ENGINE_ERROR_CODE step(
            gsl::not_null<const void*> cookie,
            gsl::not_null<dcp_message_producers*> producers) override;

    ENGINE_ERROR_CODE open(gsl::not_null<const void*> cookie,
                           uint32_t opaque,
                           uint32_t seqno,
                           uint32_t flags,
                           cb::const_char_buffer name) override;

    ENGINE_ERROR_CODE add_stream(gsl::not_null<const void*> cookie,
                                 uint32_t opaque,
                                 Vbid vbucket,
                                 uint32_t flags) override;

    ENGINE_ERROR_CODE close_stream(gsl::not_null<const void*> cookie,
                                   uint32_t opaque,
                                   Vbid vbucket) override;

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
                                       dcp_add_failover_log callback) override;

    ENGINE_ERROR_CODE stream_end(gsl::not_null<const void*> cookie,
                                 uint32_t opaque,
                                 Vbid vbucket,
                                 uint32_t flags) override;

    ENGINE_ERROR_CODE snapshot_marker(gsl::not_null<const void*> cookie,
                                      uint32_t opaque,
                                      Vbid vbucket,
                                      uint64_t start_seqno,
                                      uint64_t end_seqno,
                                      uint32_t flags) override;

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

    ENGINE_ERROR_CODE deletion_v2(gsl::not_null<const void*> cookie,
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
                                 cb::const_byte_buffer meta) override;

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
                              const void* key,
                              uint16_t nkey,
                              const void* value,
                              uint32_t nvalue) override;

    ENGINE_ERROR_CODE response_handler(
            gsl::not_null<const void*> cookie,
            const protocol_binary_response_header* response) override;

    ENGINE_ERROR_CODE system_event(gsl::not_null<const void*> cookie,
                                   uint32_t opaque,
                                   Vbid vbucket,
                                   mcbp::systemevent::id event,
                                   uint64_t bySeqno,
                                   cb::const_byte_buffer key,
                                   cb::const_byte_buffer eventData) override;

    // End DcpIface ///////////////////////////////////////////////////////////

    /**
     * Delete a given key and value from the engine.
     *
     * @param cookie The cookie representing the connection
     * @param key The key that needs to be deleted from the engine
     * @param cas CAS value of the mutation that needs to be returned
     *            back to the client
     * @param vbucket vbucket id to which the deleted key corresponds to
     * @param item_meta pointer to item meta data that needs to be
     *                  as a result the delete. A NULL pointer indicates
     *                  that no meta data needs to be returned.
     * @param mut_info pointer to the mutation info that resulted from
     *                 the delete.
     *
     * @returns ENGINE_SUCCESS if the delete was successful or
     *          an error code indicating the error
     */
    ENGINE_ERROR_CODE itemDelete(const void* cookie,
                                 const DocKey& key,
                                 uint64_t& cas,
                                 Vbid vbucket,
                                 ItemMetaData* item_meta,
                                 mutation_descr_t& mut_info);

    void itemRelease(item* itm);

    ENGINE_ERROR_CODE get(const void* cookie,
                          item** itm,
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
            const void* cookie,
            const DocKey& key,
            Vbid vbucket,
            std::function<bool(const item_info&)> filter);

    cb::EngineErrorItemPair getAndTouchInner(const void* cookie,
                                             const DocKey& key,
                                             Vbid vbucket,
                                             uint32_t expiry_time);

    ENGINE_ERROR_CODE getLockedInner(const void* cookie,
                                     item** itm,
                                     const DocKey& key,
                                     Vbid vbucket,
                                     uint32_t lock_timeout);

    ENGINE_ERROR_CODE unlockInner(const void* cookie,
                                  const DocKey& key,
                                  Vbid vbucket,
                                  uint64_t cas);

    const std::string& getName() const {
        return name;
    }

    ENGINE_ERROR_CODE getStats(const void* cookie,
                               const char* stat_key,
                               int nkey,
                               ADD_STAT add_stat);

    void resetStats();

    ENGINE_ERROR_CODE storeInner(const void* cookie,
                                 item* itm,
                                 uint64_t& cas,
                                 ENGINE_STORE_OPERATION operation);

    cb::EngineErrorCasPair storeIfInner(const void* cookie,
                                        Item& itm,
                                        uint64_t cas,
                                        ENGINE_STORE_OPERATION operation,
                                        cb::StoreIfPredicate predicate);

    ENGINE_ERROR_CODE dcpOpen(const void* cookie,
                              uint32_t opaque,
                              uint32_t seqno,
                              uint32_t flags,
                              cb::const_char_buffer stream_name);

    ENGINE_ERROR_CODE dcpAddStream(const void* cookie,
                                   uint32_t opaque,
                                   Vbid vbucket,
                                   uint32_t flags);

    cb::EngineErrorMetadataPair getMetaInner(const void* cookie,
                                             const DocKey& key,
                                             Vbid vbucket);

    ENGINE_ERROR_CODE setWithMeta(
            const void* cookie,
            protocol_binary_request_set_with_meta* request,
            ADD_RESPONSE response);

    ENGINE_ERROR_CODE deleteWithMeta(
            const void* cookie,
            protocol_binary_request_delete_with_meta* request,
            ADD_RESPONSE response);

    ENGINE_ERROR_CODE returnMeta(const void* cookie,
                                 protocol_binary_request_return_meta* request,
                                 ADD_RESPONSE response);

    ENGINE_ERROR_CODE getAllKeys(const void* cookie,
                                 protocol_binary_request_get_keys* request,
                                 ADD_RESPONSE response);

    CONN_PRIORITY getDCPPriority(const void* cookie);

    void setDCPPriority(const void* cookie, CONN_PRIORITY priority);

    void notifyIOComplete(const void* cookie, ENGINE_ERROR_CODE status);

    ENGINE_ERROR_CODE reserveCookie(const void *cookie);
    ENGINE_ERROR_CODE releaseCookie(const void *cookie);

    void storeEngineSpecific(const void* cookie, void* engine_data);

    void* getEngineSpecific(const void* cookie);

    bool isDatatypeSupported(const void* cookie,
                             protocol_binary_datatype_t datatype);

    bool isMutationExtrasSupported(const void* cookie);

    bool isXattrEnabled(const void* cookie);

    bool isCollectionsSupported(const void* cookie);

    uint8_t getOpcodeIfEwouldblockSet(const void* cookie);

    bool validateSessionCas(const uint64_t cas);

    void decrementSessionCtr();

    void setErrorContext(const void* cookie, cb::const_char_buffer message);

    void registerEngineCallback(ENGINE_EVENT_TYPE type,
                                EVENT_CALLBACK cb, const void *cb_data);

    template <typename T>
    void notifyIOComplete(T cookies, ENGINE_ERROR_CODE status);

    void handleDisconnect(const void *cookie);
    void handleDeleteBucket(const void *cookie);

    cb::mcbp::Status stopFlusher(const char** msg, size_t* msg_size);

    cb::mcbp::Status startFlusher(const char** msg, size_t* msg_size);

    ENGINE_ERROR_CODE deleteVBucket(Vbid vbid, const void* c = NULL);

    ENGINE_ERROR_CODE compactDB(Vbid vbid,
                                const CompactionConfig& c,
                                const void* cookie = NULL);

    bool resetVBucket(Vbid vbid);

    cb::mcbp::Status evictKey(const void* cookie,
                              protocol_binary_request_header* request,
                              const char** msg,
                              size_t* msg_size);

    ENGINE_ERROR_CODE observe(const void* cookie,
                              protocol_binary_request_header* request,
                              ADD_RESPONSE response);

    ENGINE_ERROR_CODE observe_seqno(const void* cookie,
                                    protocol_binary_request_header *request,
                                    ADD_RESPONSE response);

    VBucketPtr getVBucket(Vbid vbucket);

    ENGINE_ERROR_CODE setVBucketState(const void* cookie,
                                      ADD_RESPONSE response,
                                      Vbid vbid,
                                      vbucket_state_t to,
                                      bool transfer,
                                      uint64_t cas);

    cb::mcbp::Status setParam(protocol_binary_request_set_param* req,
                              std::string& msg);

    cb::mcbp::Status setFlushParam(const char* keyz,
                                   const char* valz,
                                   std::string& msg);

    cb::mcbp::Status setReplicationParam(const char* keyz,
                                         const char* valz,
                                         std::string& msg);

    cb::mcbp::Status setCheckpointParam(const char* keyz,
                                        const char* valz,
                                        std::string& msg);

    cb::mcbp::Status setDcpParam(const char* keyz,
                                 const char* valz,
                                 std::string& msg);

    cb::mcbp::Status setVbucketParam(Vbid vbucket,
                                     const char* keyz,
                                     const char* valz,
                                     std::string& msg);

    ENGINE_ERROR_CODE getReplicaCmd(protocol_binary_request_header* request,
                                    const void* cookie,
                                    Item** it,
                                    const char** msg,
                                    cb::mcbp::Status* res);

    ~EventuallyPersistentEngine();

    EPStats& getEpStats() {
        return stats;
    }

    KVBucket* getKVBucket() {
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

    SERVER_HANDLE_V1* getServerApi() {
        return serverApi;
    }

    Configuration& getConfiguration() {
        return configuration;
    }

    ENGINE_ERROR_CODE deregisterTapClient(const void* cookie,
                                          protocol_binary_request_header *request,
                                          ADD_RESPONSE response);

    ENGINE_ERROR_CODE handleCheckpointCmds(const void* cookie,
                                           protocol_binary_request_header *request,
                                           ADD_RESPONSE response);

    ENGINE_ERROR_CODE handleSeqnoCmds(const void* cookie,
                                      protocol_binary_request_header *request,
                                      ADD_RESPONSE response);

    ENGINE_ERROR_CODE resetReplicationChain(const void* cookie,
                                            protocol_binary_request_header *request,
                                            ADD_RESPONSE response);

    ENGINE_ERROR_CODE changeTapVBFilter(const void* cookie,
                                        protocol_binary_request_header *request,
                                        ADD_RESPONSE response);

    ENGINE_ERROR_CODE handleTrafficControlCmd(const void* cookie,
                                              protocol_binary_request_header *request,
                                              ADD_RESPONSE response);

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

    ENGINE_ERROR_CODE getRandomKey(const void *cookie,
                                   ADD_RESPONSE response);

    void setCompressionMode(const std::string& compressModeStr);

    void setMinCompressionRatio(float minCompressRatio) {
        minCompressionRatio = minCompressRatio;
    }

    ConnHandler* getConnHandler(const void *cookie);

    void addLookupAllKeys(const void *cookie, ENGINE_ERROR_CODE err);

    /*
     * Explicitly trigger the defragmenter task. Provided to facilitate
     * testing.
     */
    void runDefragmenterTask(void);

    /*
     * Explicitly trigger the AccessScanner task. Provided to facilitate
     * testing.
     */
    bool runAccessScannerTask(void);

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
     * @return ENGINE_SUCCESS upon success
     */
    ENGINE_ERROR_CODE getAllVBucketSequenceNumbers(
                                        const void *cookie,
                                        protocol_binary_request_header *request,
                                        ADD_RESPONSE response);

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

    ENGINE_ERROR_CODE itemAllocate(item** itm,
                                   const DocKey& key,
                                   const size_t nbytes,
                                   const size_t priv_nbytes,
                                   const int flags,
                                   rel_time_t exptime,
                                   uint8_t datatype,
                                   Vbid vbucket);

    /**
     * class-specific deallocation. Required to ensure engine is
     * deregisterd from TLS before freeing memory (and invoking delete
     * hooks).
     */
    static void operator delete(void* ptr);

protected:
    friend class EpEngineValueChangeListener;

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

    EventuallyPersistentEngine(GET_SERVER_API get_server_api);
    friend ENGINE_ERROR_CODE create_instance(GET_SERVER_API get_server_api,
                                             EngineIface** handle);

    /**
     * Report the state of a memory condition when out of memory.
     *
     * @return ETMPFAIL if we think we can recover without interaction,
     *         else ENOMEM
     */
    ENGINE_ERROR_CODE memoryCondition();

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

    ENGINE_ERROR_CODE doEngineStats(const void *cookie, ADD_STAT add_stat);
    ENGINE_ERROR_CODE doKlogStats(const void *cookie, ADD_STAT add_stat);
    ENGINE_ERROR_CODE doMemoryStats(const void *cookie, ADD_STAT add_stat);
    ENGINE_ERROR_CODE doVBucketStats(const void *cookie, ADD_STAT add_stat,
                                     const char* stat_key,
                                     int nkey,
                                     bool prevStateRequested,
                                     bool details);
    ENGINE_ERROR_CODE doHashStats(const void *cookie, ADD_STAT add_stat);
    ENGINE_ERROR_CODE doHashDump(const void* cookie,
                                 ADD_STAT addStat,
                                 cb::const_char_buffer keyArgs);
    ENGINE_ERROR_CODE doCheckpointStats(const void *cookie, ADD_STAT add_stat,
                                        const char* stat_key, int nkey);
    ENGINE_ERROR_CODE doCheckpointDump(const void* cookie,
                                       ADD_STAT addStat,
                                       cb::const_char_buffer keyArgs);
    ENGINE_ERROR_CODE doDcpStats(const void *cookie, ADD_STAT add_stat);
    ENGINE_ERROR_CODE doEvictionStats(const void* cookie, ADD_STAT add_stat);
    ENGINE_ERROR_CODE doConnAggStats(const void *cookie, ADD_STAT add_stat,
                                     const char *sep, size_t nsep);
    ENGINE_ERROR_CODE doTimingStats(const void *cookie, ADD_STAT add_stat);
    ENGINE_ERROR_CODE doSchedulerStats(const void *cookie, ADD_STAT add_stat);
    ENGINE_ERROR_CODE doRunTimeStats(const void *cookie, ADD_STAT add_stat);
    ENGINE_ERROR_CODE doDispatcherStats(const void *cookie, ADD_STAT add_stat);
    ENGINE_ERROR_CODE doTasksStats(const void* cookie, ADD_STAT add_stat);
    ENGINE_ERROR_CODE doKeyStats(const void* cookie,
                                 ADD_STAT add_stat,
                                 Vbid vbid,
                                 const DocKey& key,
                                 bool validate = false);

    ENGINE_ERROR_CODE doDcpVbTakeoverStats(const void* cookie,
                                           ADD_STAT add_stat,
                                           std::string& key,
                                           Vbid vbid);
    ENGINE_ERROR_CODE doVbIdFailoverLogStats(const void* cookie,
                                             ADD_STAT add_stat,
                                             Vbid vbid);
    ENGINE_ERROR_CODE doAllFailoverLogStats(const void *cookie, ADD_STAT add_stat);
    ENGINE_ERROR_CODE doWorkloadStats(const void *cookie, ADD_STAT add_stat);
    ENGINE_ERROR_CODE doSeqnoStats(const void *cookie, ADD_STAT add_stat,
                                   const char* stat_key, int nkey);
    void addSeqnoVbStats(const void *cookie, ADD_STAT add_stat,
                                  const VBucketPtr &vb);

    ENGINE_ERROR_CODE doCollectionsStats(const void* cookie,
                                         ADD_STAT add_stat,
                                         const std::string& statKey);

    void addLookupResult(const void* cookie, std::unique_ptr<Item> result);

    bool fetchLookupResult(const void* cookie, std::unique_ptr<Item>& itm);

    // Initialize all required callbacks of this engine with the underlying
    // server.
    void initializeEngineCallbacks();

    /*
     * Private helper method for decoding the options on set/del_with_meta.
     * Tighly coupled to the logic of both those functions, it will
     * take a request pointer and locate and validate any options within.
     * @param request byte buffer containing the incoming meta request.
     * @param extlen the extlen value from the incoming meta request.
     * @param generateCas[out] set to Yes if CAS regeneration is enabled.
     * @param checkConflicts[out] set to No if conflict resolution should
     *        not be performed.
     * @param permittedVBStates[out] updates with replica and pending if the
     *        options contain force.
     * @param keyOffset set to the number of bytes which are to be skipped to
     *        locate the key.
     */
    cb::mcbp::Status decodeWithMetaOptions(cb::const_byte_buffer request,
                                           uint8_t extlen,
                                           GenerateCas& generateCas,
                                           CheckConflicts& checkConflicts,
                                           PermittedVBStates& permittedVBStates,
                                           int& keyOffset);

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
    ENGINE_ERROR_CODE sendErrorResponse(ADD_RESPONSE response,
                                        cb::mcbp::Status status,
                                        uint64_t cas,
                                        const void* cookie);

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
    ENGINE_ERROR_CODE sendMutationExtras(ADD_RESPONSE response,
                                         Vbid vbucket,
                                         uint64_t bySeqno,
                                         cb::mcbp::Status status,
                                         uint64_t cas,
                                         const void* cookie);

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
            const void* cookie,
            protocol_binary_datatype_t datatype,
            cb::const_char_buffer body);

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
     * @returns state of the operation as an ENGINE_ERROR_CODE
     */
    ENGINE_ERROR_CODE setWithMeta(Vbid vbucket,
                                  DocKey key,
                                  cb::const_byte_buffer value,
                                  ItemMetaData itemMeta,
                                  bool isDeleted,
                                  protocol_binary_datatype_t datatype,
                                  uint64_t& cas,
                                  uint64_t* seqno,
                                  const void* cookie,
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
     * @returns state of the operation as an ENGINE_ERROR_CODE
     */
    ENGINE_ERROR_CODE deleteWithMeta(Vbid vbucket,
                                     DocKey key,
                                     ItemMetaData itemMeta,
                                     uint64_t& cas,
                                     uint64_t* seqno,
                                     const void* cookie,
                                     PermittedVBStates permittedVBStates,
                                     CheckConflicts checkConflicts,
                                     GenerateBySeqno genBySeqno,
                                     GenerateCas genCas,
                                     cb::const_byte_buffer emd);

    /**
     * Get parameters for expiry calculation
     * The function will return any limit which may be in-force (max_ttl is non
     * zero) and also calculate an expiry if exptime is 0 and max_ttl is in-use.
     * @param exptime for an incoming itemAllocate/GAT
     * @return a pair, the ExpiryLimit and the exptime to apply to the update
     */
    std::pair<cb::ExpiryLimit, rel_time_t>
    getExpiryParameters(rel_time_t exptime) const;

    /**
     * Process an expiry time to see if the maxTTL limit needs enforcing.
     * @param in a document's expiry time
     * @return the corrected expiry time (may return input)
     */
    time_t processExpiryTime(time_t in) const;

    /**
     * Make a DocKey from the key buffer
     */
    DocKey makeDocKey(const void* cookie, cb::const_byte_buffer key);

    SERVER_HANDLE_V1 *serverApi;

    // Engine statistics. First concrete member as a number of other members
    // refer to it so needs to be constructed first (and destructed last).
    EPStats stats;
    std::unique_ptr<KVBucket> kvBucket;
    WorkLoadPolicy *workload;
    bucket_priority_t workloadPriority;

    std::map<const void*, std::unique_ptr<Item>> lookups;
    std::unordered_map<const void*, ENGINE_ERROR_CODE> allKeysLookups;
    std::mutex lookupMutex;
    GET_SERVER_API getServerApiFunc;

    std::unique_ptr<DcpFlowControlManager> dcpFlowControlManager_;
    std::unique_ptr<DcpConnMap> dcpConnMap_;
    CheckpointConfig *checkpointConfig;
    std::string name;
    size_t maxItemSize;
    size_t maxItemPrivilegedBytes;
    size_t getlDefaultTimeout;
    size_t getlMaxTimeout;
    size_t maxFailoverEntries;
    Configuration configuration;
    std::atomic<bool> trafficEnabled;

    // a unique system generated token initialized at each time
    // ep_engine starts up.
    std::atomic<time_t> startupTime;
    EpEngineTaskable taskable;
    std::atomic<BucketCompressionMode> compressionMode;
    std::atomic<float> minCompressionRatio;
};
