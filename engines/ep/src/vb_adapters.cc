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
#include "utilities/debug_variable.h"
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
    // It might be useful to have the visitor that is running recorded
    // in minidumps.
    cb::DebugVariable visitorName{cb::toCharArrayN<32>(label)};
    visitor->begin();

    while (!vbucketsToVisit.empty()) {
        const auto vbid = vbucketsToVisit.front();
        VBucketPtr vb = store->getVBucket(vbid);
        if (vb) {
            currentvb = vbid.get();
            // Also record the vbid.
            cb::DebugVariable debugVbid{vbid.get()};

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

CallbackAdapter::CallbackAdapter(ContinuationCallback continuation)
    : continuation(std::move(continuation)) {
    Expects(this->continuation);
}

void CallbackAdapter::callContinuation(bool runAgain) const {
    continuation(*this, runAgain);
}

SingleSteppingVisitorAdapter::SingleSteppingVisitorAdapter(
        KVBucket* store,
        TaskId id,
        std::unique_ptr<InterruptableVBucketVisitor> visitor,
        std::string_view label,
        ContinuationCallback continuation)
    : EpNotifiableTask(store->getEPEngine(),
                       id,
                       INT_MAX /*initialSleepTime*/,
                       true /*completeBeforeShutdown*/),
      CallbackAdapter(std::move(continuation)),
      store(store),
      visitor(std::move(visitor)),
      label(label),
      maxDuration(std::chrono::microseconds::max()) {
    // populate the list of vbuckets to visit, and order them as needed by
    // the visitor.
    const auto numVbs = store->getVBuckets().getSize();

    for (Vbid::id_type vbid = 0; vbid < numVbs; ++vbid) {
        if (this->visitor->getVBucketFilter()(Vbid(vbid))) {
            vbucketsToVisit.emplace_back(vbid);
        }
    }
    std::sort(vbucketsToVisit.begin(),
              vbucketsToVisit.end(),
              this->visitor->getVBucketComparator());
}

std::string SingleSteppingVisitorAdapter::getDescription() const {
    return label;
}

bool SingleSteppingVisitorAdapter::runInner(bool) {
    bool runAgain = [this] {
        if (engine->getEpStats().isShutdown) {
            return false;
        }

        visitor->begin();

        while (!vbucketsToVisit.empty()) {
            const auto vbid = vbucketsToVisit.front();
            vbucketsToVisit.pop_front();
            VBucketPtr vb = store->getVBucket(vbid);
            if (!vb) {
                continue;
            }

            using State = InterruptableVBucketVisitor::ExecutionState;
            switch (visitor->shouldInterrupt()) {
            case State::Continue:
            case State::Pause:
                break;
            case State::Stop:
                visitor->complete();
                return false;
            }

            visitor->visitBucket(*vb);
            break;
        }

        if (vbucketsToVisit.empty()) {
            // Processed all vBuckets now, do not need to run again.
            visitor->complete();
            return false;
        } else {
            return true;
        }
    }();

    callContinuation(runAgain);
    return runAgain;
}
