/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once
#include "callbacks.h"
#include "dcp/backfill.h"

#include <chrono>
#include <mutex>

class ActiveStream;
class KVBucket;
class KVStoreIface;
class VBucket;
enum class ValueFilter;

/* The possible states of the DCPBackfillDisk */
enum backfill_state_t {
    backfill_state_init,
    backfill_state_scanning,
    backfill_state_scanning_history_snapshot,
    backfill_state_completing,
    backfill_state_done
};

/* Callback to get the items that are found to be in the cache */
class CacheCallback : public StatusCallback<CacheLookup> {
public:
    CacheCallback(KVBucket& bucket, std::shared_ptr<ActiveStream> s);

    void callback(CacheLookup& lookup) override;

private:
    /**
     * Attempt to perform the get of lookup
     *
     * @return return a GetValue by performing a vb::get with lookup::getKey.
     */
    GetValue get(VBucket& vb, CacheLookup& lookup, ActiveStream& stream);

    KVBucket& bucket;
    std::weak_ptr<ActiveStream> streamPtr;
};

/* Callback to get the items that are found to be in the disk */
class DiskCallback : public StatusCallback<GetValue> {
public:
    explicit DiskCallback(std::shared_ptr<ActiveStream> s);

    void callback(GetValue& val) override;

protected:
    virtual bool skipItem(const Item& item) const {
        return false;
    }

    std::weak_ptr<ActiveStream> streamPtr;
};

// Callback to get the items that are found to be in the disk for a seqno scan
class BySeqnoDiskCallback : public DiskCallback {
public:
    explicit BySeqnoDiskCallback(std::shared_ptr<ActiveStream> s)
        : DiskCallback(std::move(s)) {
    }

    /**
     * The on disk "High Completed Seqno", used in the callback method to decide
     * if we need to send a prepare we find on disk or not (prepare seqnos <= to
     * persistedCompletedSeqno do not need to be sent over a DCP stream).
     */
    uint64_t persistedCompletedSeqno{0};

protected:
    bool skipItem(const Item& item) const override;
};

class DCPBackfillDisk : public virtual DCPBackfill {
public:
    explicit DCPBackfillDisk(KVBucket& bucket);

    ~DCPBackfillDisk() override;

    backfill_state_t getState() const {
        return state;
    }

protected:
    backfill_status_t run() override;
    void cancel() override;
    void transitionState(backfill_state_t newState);

    /**
     * Create the scan, intialising scanCtx from KVStore initScanContext
     */
    virtual backfill_status_t create() = 0;

    /**
     * Run the scan which will return items to the owning stream
     */
    virtual backfill_status_t scan() = 0;

    /**
     * Handles the completion of the backfill, e.g. notify completion status to
     * the stream.
     *
     * @param cancelled indicates the if backfill finished fully or was
     *                  cancelled in between; for debug
     */
    virtual void complete(bool cancelled) = 0;

    bool scanHistoryCreate(ActiveStream& stream);

    /**
     * Run the scan but scan only the "history section"
     */
    backfill_status_t scanHistory();

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
     * @return true if the ScanContext was created, false for failure.
     */
    bool createHistoryScanContext();

    std::mutex lock;
    backfill_state_t state = backfill_state_init;

    KVBucket& bucket;

    std::unique_ptr<ScanContext> scanCtx;

    uint64_t finalSeqno{0};

    // Class will be constructed when a history scan is required. This tracks
    // various seqnos which come from the non-history scan and the actual
    // ScanContext used for scanning the history range.
    struct HistoryScanCtx {
        HistoryScanCtx(uint64_t startSeqno, uint64_t snapshotMaxSeqno);
        ~HistoryScanCtx();

        /**
         * Create the ScanContext which will be used by the history phase of
         * a backfill.
         * @param startSeqno the start seqno for the history scan
         * @param kvs KVStore to call initBySeqnoContext on
         * @param ctx The ScanContext from the initial scan phase, this might be
         *            ByID or BySeq.
         * @return true if successfully created, false for a failure.
         */
        bool createScanContext(uint64_t startSeqno,
                               const KVStoreIface& kvs,
                               ScanContext& ctx);

        // Record the actual start seqno of the scan
        uint64_t startSeqno{0};
        // Record the snapshotMaxSeqno (which the history scan will reach)
        uint64_t snapshotMaxSeqno{0};

        // A ScanContext which "drives" the history scan
        std::unique_ptr<ScanContext> scanCtx;
    };

    // If a history scan is required this optional will be initialised.
    std::unique_ptr<HistoryScanCtx> historyScan;
};
