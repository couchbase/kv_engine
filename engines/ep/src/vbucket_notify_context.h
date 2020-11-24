/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc
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

#pragma once

#include "callbacks.h"
#include "ep_types.h"

/* Structure that holds info needed for notification for an item being updated
   in the vbucket */
struct VBNotifyCtx {
    int64_t bySeqno = 0;
    bool notifyReplication = false;
    bool notifyFlusher = false;

    /**
     * It's only necessary to send prepares to SyncWrite enabled Producers. We
     * don't want to notify any non-SyncWrite enabled Producer of a prepare as
     * this will mean:
     *
     * 1) This (front end worker) thread spends more time notifying Producers
     *    (we attempt to schedule the ConnNotifier once per Producer although
     *    we only wake it if it has not yet been notified)
     *
     * 2) We would spend more time running NonIO threads for ConnNotifier tasks
     *    that will not result in us sending an item.
     *
     * 3) The front end worker servicing the non-SyncWrite enabled Producer will
     *    be notified and have to step (taking time away from other ops).
     *
     * 4) When there are no items in an ActiveStream's ready queue the front end
     *    worker stepping will schedule the
     *    ActiveStreamCheckpointProcessorTask. This will run on an NonIO thread
     *    and enqueue nothing into the ActiveStream's ready queue if the only
     *    item is a prepare. This will slow down other SyncWrites if NonIO
     *    threads are a bottleneck.
     *
     * 5) The ActiveStreamCheckpointProcessorTask would then notify the front
     *    end worker once more which would step (taking time away from other
     *    ops) and not send anything.
     *
     * We'll use this to determine if we can skip notifying a Producer of the
     * given seqno.
     */
    SyncWriteOperation syncWrite = SyncWriteOperation::No;

    // The number that should be added to the item count due to the performed
    // operation (+1 for new, -1 for delete, 0 for update of existing doc)
    int itemCountDifference = 0;
};

using NewSeqnoCallback =
        std::unique_ptr<Callback<const Vbid, const VBNotifyCtx&>>;
