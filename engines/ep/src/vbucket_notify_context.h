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

#include <gsl/gsl-lite.hpp>

/**
 * Structure that holds info needed for notification for an item being updated
 * in the vbucket
 */
class VBNotifyCtx {
public:
    VBNotifyCtx() = default;
    VBNotifyCtx(int64_t seqno,
                bool notifyReplication,
                bool notifyFlusher,
                queue_op op)
        : seqno(seqno),
          notifyReplication(notifyReplication),
          notifyFlusher(notifyFlusher),
          op(op){};

    auto getSeqno() const {
        return seqno;
    }

    auto isNotifyReplication() const {
        return notifyReplication;
    }

    auto isNotifyFlusher() const {
        return this->notifyFlusher;
    }

    auto getOp() const {
        return op;
    }

    int8_t getItemCountDifference() const {
        return itemCountDifference;
    }

    void setItemCountDifference(int8_t diff) {
        Expects(std::abs(diff) <= 1);
        itemCountDifference = diff;
    }

private:
    int64_t seqno = 0;
    bool notifyReplication = false;
    bool notifyFlusher = false;

    /**
     * Not all DcpProducers will send every operation. For example any producer
     * that does not enable SyncReplication cannot send a prepare or abort.
     * This member records if the operation is a prepare or abort so that a we
     * can avoid pointlessly waking producers which will never send either a
     * prepare/abort or since 7.0 a seqno-advance in place of the prepare/abort.
     *
     * By avoiding a wakeup we can reduce/avoid the following:
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
    queue_op op = queue_op::empty;

    // The number that should be added to the item count due to the performed
    // operation (+1 for new, -1 for delete, 0 for update of existing doc)
    int8_t itemCountDifference = 0;
};
