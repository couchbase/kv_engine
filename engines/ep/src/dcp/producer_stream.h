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

#include "dcp/stream.h"
#include <platform/json_log.h>
#include <spdlog/common.h>

#include <memory>

class DcpProducer;
class DcpResponse;
struct StreamAggStats;
class VBucket;

/**
 * Abstract class for Stream types that will be created by a DcpProducer
 *
 * Streams that produce data will derive from this class and queue to
 * The Stream data from various sources with varying semantics.
 *
 * For example ActiveStream sources data from either checkpoints of disk
 * and provides sequence number ordered output for replication.
 */
class ProducerStream : public Stream {
public:
    ProducerStream(std::string name,
                   const std::shared_ptr<DcpProducer>& p,
                   cb::mcbp::DcpStreamId sid,
                   cb::mcbp::DcpAddStreamFlag flags,
                   uint32_t opaque,
                   Vbid vb,
                   uint64_t start_seqno,
                   uint64_t vb_uuid,
                   uint64_t snap_start_seqno = 0,
                   uint64_t snap_end_seqno = 0)
        : Stream(name,
                 flags,
                 opaque,
                 vb,
                 start_seqno,
                 vb_uuid,
                 snap_start_seqno,
                 snap_end_seqno),
          producerPtr(p),
          sid(sid) {
    }

    /**
     * A ProducerStream sub-class maybe configured with collection/scope/bucket
     * privileges. This method should recheck privileges and return true if the
     * stream should end for a loss of the required privileges.
     *
     * @param producer reference to the calling producer to avoid costs of
     * re-acquiring the calling DcpProducer
     *
     * @return true if the stream should end for a loss of the required
     * privileges
     */
    virtual bool endIfRequiredPrivilegesLost(DcpProducer&) = 0;

    /**
     * Get the next item.
     *
     * @param producer reference to the calling producer to avoid costs of
     * re-acquiring the calling DcpProducer
     *
     * @return a pointer to the next item or nullptr if no more items are
     * available
     */
    virtual std::unique_ptr<DcpResponse> next(DcpProducer&) = 0;

    /**
     * Ends the stream when the vbstate lock is already acquired.
     *
     * @param status The stream end status
     * @param vbstateLock Exclusive lock to vbstate
     */
    virtual void setDeadWithLock(
            cb::mcbp::DcpStreamEndStatus status,
            std::unique_lock<folly::SharedMutex>& vbstateLock) = 0;

    /**
     * @return a value representing how many items maybe left to process for
     * this stream
     */
    virtual size_t getItemsRemaining() = 0;

    /**
     * Update the aggregate stats for this stream
     *
     * @param stats the aggregate stats to update
     */
    virtual void updateAggStats(StreamAggStats& stats);

    /**
     * Provide a stub for notifySeqnoAvailable to be overridden by subclasses.
     * This stub means that DcpProducer can avoid calling dynamic_cast when
     * needing to call this hot function.
     *
     * @param producer reference to the calling producer to avoid costs of
     * re-acquiring the calling DcpProducer
     */
    virtual void notifySeqnoAvailable(DcpProducer& producer) {
    }

    /**
     * Generate takeover stats for the stream - sub-classes may need to provide
     * minimal information to ensure cluster manager continues a rebalance
     */
    virtual void addTakeoverStats(const AddStatFn& add_stat,
                                  CookieIface& c,
                                  const VBucket& vb) = 0;

    /**
     * Notifies the producer (which created and owns this stream) that the
     * stream has items ready for processing.
     *
     * @param force Indicates if the function should notify the connection
     *              irrespective of whether the connection already knows that
     *              the items are ready to be picked up. Default is 'false'
     * @param producer Optional pointer to the DcpProducer owning this stream.
     *                 Supplied in some cases to reduce the number of times that
     *                 we promote the weak_ptr to the DcpProducer (producerPtr).
     */
    void notifyStreamReady(bool force = false, DcpProducer* producer = nullptr);

    /**
     * @param reason The reason for the stream end
     * @return A DcpResponse representing a stream end for this stream
     */
    std::unique_ptr<DcpResponse> makeEndStreamResponse(
            cb::mcbp::DcpStreamEndStatus reason);

    /**
     * Log a message with a "standard" format for the stream/producer. The
     * message will be logged using the producer logWithContext if the producer
     * is available (adding in producer specific context). If the producer is
     * not available, the message will be logged using the global bucket logger.
     */
    void logWithContext(spdlog::level::level_enum severity,
                        std::string_view msg,
                        cb::logger::Json ctx) const;

    void logWithContext(spdlog::level::level_enum severity,
                        std::string_view msg) const;

protected:
    /// A weak pointer to the producer which created this stream.
    const std::weak_ptr<DcpProducer> producerPtr;

    /**
     * A stream-ID which is defined if the producer is using the many
     * streams-per-vbucket feature.
     */
    const cb::mcbp::DcpStreamId sid;
};