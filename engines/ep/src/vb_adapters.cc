/*
 *   Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "vb_adapters.h"

#include "ep_engine.h"
#include "kv_bucket.h"
#include "vb_visitors.h"

VBCBAdaptor::VBCBAdaptor(KVBucket* s,
                         TaskId id,
                         std::unique_ptr<InterruptableVBucketVisitor> v,
                         const char* l,
                         bool shutdown)
    : GlobalTask(s->getEPEngine(), id, 0 /*initialSleepTime*/, shutdown),
      store(s),
      visitor(std::move(v)),
      label(l),
      maxDuration(std::chrono::microseconds::max()) {
    // populate the list of vbuckets to visit, and order them as needed by
    // the visitor.
    const auto numVbs = store->getVBuckets().getSize();

    for (Vbid::id_type vbid = 0; vbid < numVbs; ++vbid) {
        if (visitor->getVBucketFilter()(Vbid(vbid))) {
            vbucketsToVisit.emplace_back(vbid);
        }
    }
    std::sort(vbucketsToVisit.begin(),
              vbucketsToVisit.end(),
              visitor->getVBucketComparator());
}

std::string VBCBAdaptor::getDescription() const {
    auto value = currentvb.load();
    if (value == None) {
        return std::string(label) + " no vbucket assigned";
    } else {
        return std::string(label) + " on " + Vbid(value).to_string();
    }
}

bool VBCBAdaptor::run() {
    visitor->begin();

    while (!vbucketsToVisit.empty()) {
        const auto vbid = vbucketsToVisit.front();
        VBucketPtr vb = store->getVBucket(vbid);
        if (vb) {
            currentvb = vbid.get();

            using State = InterruptableVBucketVisitor::ExecutionState;
            switch (visitor->shouldInterrupt()) {
            case State::Continue:
                break;
            case State::Pause:
                snooze(0);
                return true;
            case State::Stop:
                visitor->complete();
                return false;
            }

            visitor->visitBucket(*vb);
        }
        vbucketsToVisit.pop_front();
    }

    // Processed all vBuckets now, do not need to run again.
    visitor->complete();
    return false;
}