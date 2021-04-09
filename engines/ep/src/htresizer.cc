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
    ResizingVisitor() { }

    void visitBucket(const VBucketPtr& vb) override {
        vb->ht.resize();
    }
};

HashtableResizerTask::HashtableResizerTask(KVBucketIface& s, double sleepTime)
    : GlobalTask(&s.getEPEngine(),
                 TaskId::HashtableResizerTask,
                 sleepTime,
                 false),
      store(s) {
}

bool HashtableResizerTask::run() {
    TRACE_EVENT0("ep-engine/task", "HashtableResizerTask");
    auto pv = std::make_unique<ResizingVisitor>();

    // [per-VBucket Task] While a Hashtable is resizing no user
    // requests can be performed (the resizing process needs to
    // acquire all HT locks). As such we are sensitive to the duration
    // of this task - we want to log anything which has a
    // non-negligible impact on frontend operations.
    const auto maxExpectedDurationForVisitorTask =
            std::chrono::milliseconds(100);

    store.visitAsync(std::move(pv),
                     "Hashtable resizer",
                     TaskId::HashtableResizerVisitorTask,
                     maxExpectedDurationForVisitorTask);

    snooze(engine->getConfiguration().getHtResizeInterval());
    return !engine->getEpStats().isShutdown;
}
