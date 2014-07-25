/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
#include "config.h"

#include "bgfetcher.h"
#include "ep_engine.h"
#include "flusher.h"
#include "tasks.h"
#include "warmup.h"

void GlobalTask::snooze(const double secs) {
    LockHolder lh(mutex);
    if (secs == INT_MAX) {
        set_max_tv(waketime);
        return;
    }

    gettimeofday(&waketime, NULL);

    if (secs) {
        advance_tv(waketime, secs);
    }
}

bool FlusherTask::run() {
    return flusher->step(this);
}

bool VBSnapshotTask::run() {
    engine->getEpStore()->snapshotVBuckets(priority, shardID);
    return false;
}

bool VBDeleteTask::run() {
    return !engine->getEpStore()->completeVBucketDeletion(vbucketId, cookie,
                                                          recreate);
}

bool CompactVBucketTask::run() {
    return engine->getEpStore()->compactVBucket(vbid, &compactCtx, cookie);
}

bool StatSnap::run() {
    engine->getEpStore()->snapshotStats();
    if (runOnce) {
        return false;
    }
    ExecutorPool::get()->snooze(taskId, 60);
    return true;
}

bool BgFetcherTask::run() {
    return bgfetcher->run(this);
}


bool VKeyStatBGFetchTask::run() {
    engine->getEpStore()->completeStatsVKey(cookie, key, vbucket, bySeqNum);
    return false;
}


bool BGFetchTask::run() {
    engine->getEpStore()->completeBGFetch(key, vbucket, seqNum, cookie, init,
                                          metaFetch);
    return false;
}
