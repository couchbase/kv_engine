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

#include <memory>

class ActiveStream;
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

    DCPBackfillDiskToStream(std::shared_ptr<ActiveStream> s)
        : DCPBackfillToStream(s) {
    }

protected:
    DCPBackfill::State getNextScanState(DCPBackfill::State current) override;

    /**
     * Indicates the completion to the stream and used by all scans that iterate
     * the seqno index.
     *
     * @param stream The stream associated with the scan
     * @param bytesRead how many bytes read in the scan
     * @param startSeqno start of the scan range (used for logging)
     * @param endSeqno end of the scan range (used for logging)
     */
    void seqnoScanComplete(ActiveStream& stream,
                           size_t bytesRead,
                           uint64_t startSeqno,
                           uint64_t endSeqno);

    /**
     * Bespoke function for history completion, pulls required values from the
     * HistoryScan objects and calls seqnoScanComplete
     * @param stream The stream associated with the scan
     */
    void historyScanComplete(ActiveStream& stream);

    bool scanHistoryCreate(KVBucket& bucket,
                           ScanContext& scanCtx,
                           ActiveStream& stream);

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

    struct HistoryScanCtx {
        /**
         * Constructor records information for use in the history scan phase
         * The snapshot_info_t records the start seqno of the history scan and
         * the snapshot range of the disk snapshot being used by DCP backfill.
         */
        HistoryScanCtx(uint64_t historyStartSeqno,
                       snapshot_info_t snapshotInfo);
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

        // Record the historyStartSeqno
        uint64_t historyStartSeqno{0};

        /// records the start and snapshot range for the history scan
        snapshot_info_t snapshotInfo;

        // A ScanContext which "drives" the history scan
        std::unique_ptr<ScanContext> scanCtx;
    };

    // If a history scan is required this optional will be initialised.
    std::unique_ptr<HistoryScanCtx> historyScan;
};