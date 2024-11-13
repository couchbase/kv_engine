/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "dcp/backfill_to_stream.h"
#include "ep_types.h"
#include "kvstore/kvstore.h"

#include <memory>

class ActiveStream;
class BySeqnoScanContext;
class KVBucket;
class KVStoreIface;

/**
 * Class holding common code (that itself is a subclass of DCPBackfill) but for
 * use in BySeqno/ByID sub-classes. This class was created to own the history
 * scanning code.
 */
class DCPBackfillDiskToStream : public DCPBackfillToStream {
public:
    DCPBackfillDiskToStream() = delete;

    DCPBackfillDiskToStream(KVBucket& bucket, std::shared_ptr<ActiveStream> s);

    ~DCPBackfillDiskToStream() override;

    /**
     * @return true if the current scan makes no progress (within a time check)
     */
    bool isSlow(const ActiveStream&) override;

protected:
    DCPBackfill::State getNextScanState(DCPBackfill::State current) override;

    /**
     * Indicates the completion to the stream and used by all scans that iterate
     * the seqno index.
     *
     * @param stream The stream associated with the scan
     * @param bytesRead how many bytes read in the scan
     * @param keysScanned how many keys scanned in the scan
     * @param startSeqno start of the scan range (used for logging)
     * @param endSeqno end of the scan range (used for logging)
     * @param maxSeqno This is the highest seqno seen by KVStore when performing
     *   a scan for the current snapshot we are backfilling for. This is
     *   regardless of collection or visibility. This used inform
     *   completeBackfill() of the last read seqno from disk, to help make a
     *   decision on if we should enqueue a SeqnoAdvanced op (see
     *   ::completeBackfill() for more info).
     */
    void seqnoScanComplete(ActiveStream& stream,
                           size_t bytesRead,
                           size_t keysScanned,
                           uint64_t startSeqno,
                           uint64_t endSeqno,
                           uint64_t maxSeqno);

    /**
     * Bespoke function for history completion, pulls required values from the
     * HistoryScan objects and calls seqnoScanComplete
     * @param stream The stream associated with the scan
     */
    void historyScanComplete(ActiveStream& stream);

    /**
     * This runs the "create" phase of the history scan, which includes creation
     * of the ScanContext and calling markDiskSnapshot on the ActiveStream
     *
     * This function should be called once only and the caller is in charge of
     * that.
     *
     * @param streamPtr The new ScanContext will reference this shared_ptr
     * @return true if the scan was created and the marker sent successfully.
     */
    bool scanHistoryCreate(KVBucket& bucket,
                           ScanContext& scanCtx,
                           const std::shared_ptr<ActiveStream>& streamPtr);

    /**
     * Run the scan but scan only the "history section"
     */
    backfill_status_t doHistoryScan(KVBucket& bucket, ScanContext& scanCtx);

    /**
     * To be called from ByID or BySeq setup paths to check if a history scan
     * must follow the initial scan or if the initial scan is to be skipped
     * when the scan range is wholly inside the history window.
     *
     * @param stream the ActiveStream associated with the backfill
     * @param scanCtx the context created for the first stage of the scan
     * @param startSeqno the startSeqno of the scan
     * @return true if only a history scan should be performed (backfill start
     *         is inside the history window). return false if a non history scan
     *         is required (which can still continue onto history).
     */
    bool setupForHistoryScan(ActiveStream& stream,
                             ScanContext& scanCtx,
                             uint64_t startSeqno);

    /**
     * Internal part of the setup phase of the history scan, on success this
     * will have initialised HistoryScanCtx::scanCtx
     *
     * @param bucket The bucket for the scan
     * @param scanCtx The ScanContext created in the Create phase
     * @return true if the ScanContext was created, false for failure.
     */
    bool createHistoryScanContext(KVBucket& bucket, ScanContext& scanCtx);

    /**
     * When shouldCancel is called, backfill progress tracking begins. This
     * works by keeping a copy of the ScanContext::Position and verifying that
     * it is changing with a configurable threshold.
     *
     * @param position The current position of the scan
     * @return true if no progress has been made within the threshold
     */
    bool isProgressStalled(const ScanContext::Position& position);

    /**
     * @param scan The current scan
     * @param stream The associated DCP stream
     * @return true if disk space is critical and closing the scan may release
     * disk space.
     */
    bool shouldEndStreamToReclaimDisk(const ScanContext& scan,
                                      const ActiveStream& stream);

    struct HistoryScanCtx {
        /**
         * Constructor records information for use in the history scan phase
         * The snapshot_info_t records the start seqno of the history scan and
         * the snapshot range of the disk snapshot being used by DCP backfill.
         */
        HistoryScanCtx(uint64_t historyStartSeqno,
                       snapshot_info_t snapshotInfo,
                       SnapshotType snapshotType);
        ~HistoryScanCtx();

        /**
         * Create a ScanContext (assigned to this->scanCtx) for scanning the
         * history-window
         *
         * @param kvs KVStore to call initBySeqnoContext on
         * @param ctx The ScanContext from the initial scan phase, this can be
         *            ByID or BySeq.
         * @return true if successfully created, false for a failure.
         */
        bool createScanContext(const KVStoreIface& kvs, ScanContext& ctx);

        /**
         * @return true if startSeqno >= historyStartSeqno
         */
        bool startSeqnoIsInsideHistoryWindow() const;

        /**
         * @return the type for the current history scan
         */
        SnapshotType getSnapshotType() const;

        // Record the historyStartSeqno
        uint64_t historyStartSeqno{0};

        /// records the start and snapshot range for the history scan
        snapshot_info_t snapshotInfo;

        // A BySeqnoScanContext which "drives" the history scan
        std::unique_ptr<BySeqnoScanContext> scanCtx;

        // type of the history scan (see the enum definition)
        const SnapshotType snapshotType;
    };

    KVBucket& bucket;

    // The non-history scan, which maybe ById or BySeqno
    std::unique_ptr<ScanContext> scanCtx;

    // If a history scan is required this optional will be initialised.
    std::unique_ptr<HistoryScanCtx> historyScan;

    // Once backfill reaches scanning, the scan Position is tracked for changes
    std::optional<ScanContext::Position> trackedPosition;
    // time when the Position last changed
    std::chrono::steady_clock::time_point lastPositionChangedTime;
    // the maximum duration that the scan is permitted no Position change. This
    // is optional, std::nullopt disables any slow scan detection
    std::optional<std::chrono::seconds> maxNoProgressDuration;
};
