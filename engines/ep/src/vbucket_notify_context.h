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
#include "ep_types.h"

/* Structure that holds info needed for notification for an item being updated
   in the vbucket */
struct VBNotifyCtx {
    bool isSyncWrite() const {
        return syncWrite == SyncWriteOperation::Yes;
    }

    int64_t bySeqno = 0;
    bool notifyReplication = false;
    bool notifyFlusher = false;

    /**
     * It's only necessary to send prepares to SyncWrite enabled Producers. We
     * don't want to notify any non-SyncWrite enabled Producer of a prepare as
     * this will mean:
     *
     * 1) This (front end worker) thread spends more time notifying Producers
     *
     * 2) The front end worker servicing the non-SyncWrite enabled Producer will
     *    be notified and have to step (taking time away from other ops).
     *
     * 3) When there are no items in an ActiveStream's ready queue the front end
     *    worker stepping will schedule the
     *    ActiveStreamCheckpointProcessorTask. This will run on an NonIO thread
     *    and enqueue nothing into the ActiveStream's ready queue if the only
     *    item is a prepare. This will slow down other SyncWrites if NonIO
     *    threads are a bottleneck.
     *
     * 4) The ActiveStreamCheckpointProcessorTask would then notify the front
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
