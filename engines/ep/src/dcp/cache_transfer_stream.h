/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "collections/vbucket_filter.h"
#include "collections/vbucket_manifest_handles.h"
#include "dcp/producer_stream.h"
#include "dcp/stream_request_info.h"
#include <memory>

class StoredValue;
class EPStats;
class VBucket;

/**
 * A stream which is used to transfer cached items from the producer to the
 * consumer.
 *
 * A CacheTransferTask is used to visit the VBucket HashTable looking for
 * eligible items to transfer.
 *
 * To transfer the basic elgigiblity is:
 * 1. Not temp/deleted/pending/dropped-collection.
 * 2. Has a seqno <= maxSeqno.
 * 3. isResident when includeValue is IncludeValue::Yes.
 *
 */
class CacheTransferStream
    : public ProducerStream,
      public std::enable_shared_from_this<CacheTransferStream> {
public:
    CacheTransferStream(std::shared_ptr<DcpProducer> p,
                        const std::string& name,
                        uint32_t opaque,
                        const StreamRequestInfo& req,
                        Vbid vbid,
                        EventuallyPersistentEngine& engine,
                        Collections::VB::Filter filter);

    void setDead(cb::mcbp::DcpStreamEndStatus status) override;

    /**
     * Cancel the transfer. This will stop this transfer and begin the switch to
     * an ActiveStream.
     */
    void cancelTransfer();

    std::string getStreamTypeName() const override;

    std::string getStateName() const override;

    /// @returns true if Stream::state != Dead
    bool isActive() const override;

    /// ProducerStreamn overrides
    bool endIfRequiredPrivilegesLost(DcpProducer&) override;

    std::unique_ptr<DcpResponse> next(DcpProducer&) override;

    void setDeadWithLock(
            cb::mcbp::DcpStreamEndStatus status,
            std::unique_lock<folly::SharedMutex>& vbstateLock) override;

    size_t getItemsRemaining() override;

    void addTakeoverStats(const AddStatFn& add_stat,
                          CookieIface& c,
                          const VBucket& vb) override;

    void addStats(const AddStatFn& add_stat, CookieIface& c) override;

    enum class Status {
        /// maybeQueueItem() has queued an item. Caller should continue
        /// visiting.
        QueuedItem,

        /// maybeQueueItem() has not queued an item. Caller should continue
        /// visiting.
        KeepVisiting,

        /// maybeQueueItem() has not queued an item as there is not enough
        /// memory. Caller should yield visiting.
        OOM,

        /// maybeQueueItem() has detected that no more items will fit in the
        /// client's cache. Caller should stop visiting and evaluate next steps.
        ReachedClientMemoryLimit,

        /// maybeQueueItem() has not queued an item. Caller should stop
        /// visiting, e.g. the stream has been closed.
        Stop
    };

    static bool isFinished(Status status) {
        return status == Status::Stop ||
               status == Status::ReachedClientMemoryLimit;
    }

    Status maybeQueueItem(const StoredValue&, Collections::VB::ReadHandle&);

    /**
     * override setActive - this will schedule the CacheTransferTask to run.
     */
    void setActive() override;

    std::shared_ptr<DcpProducer> getProducer() const {
        return producerPtr.lock();
    }

    size_t getTotalBytesQueued() const {
        return totalBytesQueued;
    }

    /**
     * Required method for OBJ_LOG macros.
     * @param level The level to check.
     * @param msg The message to log.
     * @param ctx The context to log.
     */
    void logWithContext(spdlog::level::level_enum level,
                        std::string_view msg,
                        cb::logger::Json ctx) const override;

    /**
     * Required method for OBJ_LOG macros.
     * @param level The level to check.
     * @param msg The message to log.
     */
    void log(spdlog::level::level_enum level, std::string_view msg) const {
        logWithContext(level, msg, cb::logger::Json::object());
    }

    std::function<void(const StoredValue&)> preQueueCallback =
            [](const auto&) { /*nothing*/ };

    Collections::VB::Filter takeFilter() {
        return std::move(filter);
    }

    StreamRequestInfo getRequest() const {
        return request;
    }

    bool isAllKeys() const {
        return filter.isCacheTransferAllKeys();
    }

protected:
    uint64_t getMaxSeqno() const {
        // maxSeqno is the Stream start...
        return getStartSeqno();
    }

    // @return engine memory used (tests can override this to return a different
    // value)
    virtual size_t getMemoryUsed() const;

    struct MemoryUsage {
        /// engine mem_used
        size_t used{0};
        /// memory limit (mem_high_wat)
        size_t limit{0};
        /// @return true if memory is pressured (used > limit)
        bool isMemoryPressured() const {
            return used > limit;
        }
    };
    MemoryUsage getMemoryUsage() const;

    /**
     * Checks if the item should be skipped instead of queueing for transfer.
     * @param sv The StoredValue to check.
     * @param readHandle The read handle to use for the item.
     * @return true if the item should be skipped, false otherwise.
     */
    bool skip(const StoredValue& sv,
              Collections::VB::ReadHandle& readHandle) const;

    /**
     * Transitions are either:
     * Active -> Dead
     * Active -> SwitchingToActiveStream -> Dead
     */
    enum class State { Active, SwitchingToActiveStream, Dead };
    State state{State::Active};

    /// ID of the task generating data for the stream.
    size_t tid{0};

    /// Reference to the engine owning this producer/stream.
    EventuallyPersistentEngine& engine;

    /// As the stream iterates over the hash-table an all_keys stream can switch
    /// from value to key only based upon memory constraints provided by the
    /// client.
    IncludeValue includeValue{IncludeValue::Yes};

    /// Total bytes queued
    size_t totalBytesQueued{0};

    /// Optional track how much remaining memory is available (this is the
    /// target cache size provided by the client).
    std::optional<size_t> availableBytes{0};

    /// Filter to apply to the items in the stream.
    Collections::VB::Filter filter;

    /// The StreamRequestInfo which created this object
    StreamRequestInfo request;

    /// The last sequence number popped/sent from the stream.
    uint64_t lastSeqno{0};
};

std::string to_string(CacheTransferStream::Status status);
inline auto format_as(CacheTransferStream::Status status) {
    return to_string(status);
}