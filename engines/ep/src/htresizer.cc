/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "htresizer.h"
#include "ep_engine.h"
#include "kv_bucket_iface.h"
#include "vb_visitors.h"
#include "vbucket.h"

#include <phosphor/phosphor.h>

#include <memory>

/**
 * Look at all the hash tables and make sure they're sized appropriately.
 */
class ResizingVisitor : public CappedDurationVBucketVisitor {
public:
    ResizingVisitor(HashTable::ResizeAlgo algo) : resizeAlgoToUse(algo) {
    }

    void visitBucket(VBucket& vb) override {
        switch (vb.ht.getResizeInProgress()) {
        case HashTable::ResizeAlgo::None:
            if (resizeAlgoToUse == HashTable::ResizeAlgo::Incremental) {
                needsRevisit = vb.ht.beginIncrementalResize();
            } else {
                needsRevisit = vb.ht.resizeInOneStep();
            }
            break;
        case HashTable::ResizeAlgo::OneStep:
            needsRevisit = NeedsRevisit::No;
            break;
        case HashTable::ResizeAlgo::Incremental:
            needsRevisit = vb.ht.continueIncrementalResize();
            break;
        }
    }

    NeedsRevisit needsToRevisitLast() override {
        return needsRevisit;
    }

protected:
    NeedsRevisit needsRevisit = NeedsRevisit::No;

    const HashTable::ResizeAlgo resizeAlgoToUse;
};

HashtableResizerTask::HashtableResizerTask(KVBucketIface& s, double sleepTime)
    : EpTask(s.getEPEngine(), TaskId::HashtableResizerTask, sleepTime, false),
      store(s) {
}

bool HashtableResizerTask::run() {
    TRACE_EVENT0("ep-engine/task", "HashtableResizerTask");

    HashTable::ResizeAlgo resizeAlgoToUse;
    if (engine->getConfiguration().getHtResizeAlgo() == "incremental") {
        resizeAlgoToUse = HashTable::ResizeAlgo::Incremental;
    } else {
        resizeAlgoToUse = HashTable::ResizeAlgo::OneStep;
    }

    auto pv = std::make_unique<ResizingVisitor>(resizeAlgoToUse);

    // OneStep:
    // [per-VBucket Task] While a Hashtable is resizing no user
    // requests can be performed (the resizing process needs to
    // acquire all HT locks). As such we are sensitive to the duration
    // of this task - we want to log anything which has a
    // non-negligible impact on frontend operations.
    //
    // Incremental:
    // Most requests can be performed while resizing.
    // We keep the same as a sanity check.
    const auto maxExpectedDurationForVisitorTask =
            std::chrono::milliseconds(100);

    store.visitAsync(std::move(pv),
                     "Hashtable resizer",
                     TaskId::HashtableResizerVisitorTask,
                     maxExpectedDurationForVisitorTask);

    snooze(engine->getConfiguration().getHtResizeInterval());
    return !engine->getEpStats().isShutdown;
}
