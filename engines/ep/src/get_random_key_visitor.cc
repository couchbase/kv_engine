/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "get_random_key_visitor.h"

#include <collections/vbucket_manifest_handles.h>
#include <ep_engine.h>
#include <vbucket.h>

GetRandomKeyObserver::~GetRandomKeyObserver() = default;
GetRandomKeyVisitor::GetRandomKeyVisitor(EventuallyPersistentEngine& engine,
                                         CookieIface& cookie,
                                         const CollectionID cid,
                                         GetRandomKeyObserver& observer)
    : engine(engine), cookie(cookie), cid(cid), observer(observer) {
    observer.start();
}
GetRandomKeyVisitor::~GetRandomKeyVisitor() = default;

void GetRandomKeyVisitor::visitBucket(VBucket& vb) {
    if (stop) {
        return;
    }

    if (vBucketFilter(vb.getId())) {
        folly::SharedMutex::ReadHolder rlh(vb.getStateLock());
        if (vb.getState() != vbucket_state_active) {
            return;
        }
        auto handle = vb.getManifest().lock(cid);
        if (!handle.valid()) {
            observer.error(cb::engine_errc::unknown_collection,
                           handle.getManifestUid());
            stop = true;
            return;
        }
        if (handle.getItemCount() == 0) {
            return;
        }
        auto item = vb.ht.getRandomKey(cid, std::rand(), [this] {
            return shouldInterrupt() != ExecutionState::Continue;
        });
        if (item) {
            observer.found(std::move(item));
            stop = true;
        }
    }
}

void GetRandomKeyVisitor::complete() {
    observer.finish();
    engine.notifyIOComplete(cookie, cb::engine_errc::success);
}

InterruptableVBucketVisitor::ExecutionState
GetRandomKeyVisitor::shouldInterrupt() {
    if (stop) {
        return ExecutionState::Stop;
    }
    return CappedDurationVBucketVisitor::shouldInterrupt();
}
