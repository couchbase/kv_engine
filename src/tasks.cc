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

/**
 * It simulates timegm to convert the given GMT time to number of seconds
 * since epoch but it uses C standard time functions.
 */
static time_t do_timegm(struct tm *tmv)
{
  time_t epoch = 0;
  time_t offset = mktime(gmtime(&epoch));
  time_t gmt = mktime(tmv);
  return difftime(gmt, offset);
}

void GlobalTask::snooze(const double secs, bool first) {
    LockHolder lh(mutex);
    if (secs == INT_MAX) {
        set_max_tv(waketime);
        return;
    }
    gettimeofday(&waketime, NULL);
    // set scheduled task time for new task only
    if (first && (starttime == 0 || starttime <= 23) && secs >= 3600) {
        struct tm tim;
        struct timeval tmval = waketime;
        time_t seconds = tmval.tv_sec;
        tim = *(gmtime(&seconds));
        // change tm structure to the given start hour in GMT
        tim.tm_min = 0;
        tim.tm_sec = 0;
        if (tim.tm_hour >= (time_t)starttime) {
            tim.tm_hour = (time_t)starttime;
            tmval.tv_sec = do_timegm(&tim);
            // advance time until later than current time
            while (tmval.tv_sec < waketime.tv_sec) {
                advance_tv(tmval, secs);
            }
        } else if (tim.tm_hour < (time_t)starttime) {
            tim.tm_hour = starttime;
            tmval.tv_sec = do_timegm(&tim);
            // backtrack time until last time larger than current time
            time_t tsec;
            while ((tsec = tmval.tv_sec - (int)secs) > waketime.tv_sec) {
                tmval.tv_sec = tsec;
            }
        }
        waketime = tmval;
    } else {
        advance_tv(waketime, secs);
    }
}

bool FlusherTask::run() {
    return flusher->step(taskId);
}

bool VBSnapshotTask::run() {
    engine->getEpStore()->snapshotVBuckets(priority, shardID);
    return false;
}

bool VBDeleteTask::run() {
    return !engine->getEpStore()->completeVBucketDeletion(vbucket, cookie,
                                                          recreate);
}

bool CompactVBucketTask::run() {
    return engine->getEpStore()->compactVBucket(vbid, &compactCtx);
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
    return bgfetcher->run(taskId);
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
