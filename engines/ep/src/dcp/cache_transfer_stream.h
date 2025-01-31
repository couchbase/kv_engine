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

#include "collections/vbucket_manifest_handles.h"
#include "dcp/producer_stream.h"

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
                        uint64_t maxSeqno,
                        uint64_t vbucketUuid,
                        Vbid vbid,
                        EventuallyPersistentEngine& engine,
                        IncludeValue includeValue);

    void setDead(cb::mcbp::DcpStreamEndStatus status) override;

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

        /// maybeQueueItem() has not queued an item. Caller should stop
        /// visiting, e.g. the stream has been closed.
        Stop
    };
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

    std::function<void(const StoredValue&)> preQueueCallback =
            [](const auto&) { /*nothing*/ };

protected:
    uint64_t getMaxSeqno() const {
        // maxSeqno is the Stream start...
        return getStartSeqno();
    }

    virtual size_t getMemoryUsed() const;

    /**
     * Can only transition from Active to Dead via setDead (later change will
     * add more states)
     */
    enum class State { Active, Dead };
    State state{State::Active};

    /// ID of the task generating data for the stream.
    size_t tid{0};

    /// Reference to the engine owning this producer/stream.
    EventuallyPersistentEngine& engine;

    /// Configuration: Stream will send keys or keys+value
    IncludeValue includeValue = IncludeValue::Yes;

    /// Total bytes queued
    size_t totalBytesQueued{0};
};
