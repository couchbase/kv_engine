/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <functional>

#include "vbucket.hh"
#include "ep_engine.h"

VBucketFilter VBucketFilter::filter_diff(const VBucketFilter &other) const {
    std::vector<uint16_t> tmp(acceptable.size() + other.size());
    std::vector<uint16_t>::iterator end;
    end = std::set_symmetric_difference(acceptable.begin(),
                                        acceptable.end(),
                                        other.acceptable.begin(),
                                        other.acceptable.end(),
                                        tmp.begin());
    return VBucketFilter(std::vector<uint16_t>(tmp.begin(), end));
}

VBucketFilter VBucketFilter::filter_intersection(const VBucketFilter &other) const {
    std::vector<uint16_t> tmp(acceptable.size() + other.size());
    std::vector<uint16_t>::iterator end;

    end = std::set_intersection(acceptable.begin(), acceptable.end(),
                                other.acceptable.begin(), other.acceptable.end(),
                                tmp.begin());
    return VBucketFilter(std::vector<uint16_t>(tmp.begin(), end));
}

static bool isRange(std::vector<uint16_t>::const_iterator it,
                    const std::vector<uint16_t>::const_iterator &end,
                    size_t &length)
{
    length = 0;
    for (uint16_t val = *it;
         it != end && (val + length) == *it;
         ++it, ++length) {
        // empty
    }

    --length;

    return length > 1;
}

std::ostream& operator <<(std::ostream &out, const VBucketFilter &filter)
{
    bool needcomma = false;
    std::vector<uint16_t>::const_iterator it;

    if (filter.acceptable.empty()) {
        out << "{ empty }";
    } else {
        out << "{ ";
        for (it = filter.acceptable.begin();
             it != filter.acceptable.end();
             ++it) {
            if (needcomma) {
                out << ", ";
            }

            size_t length;
            if (isRange(it, filter.acceptable.end(), length)) {
                out << "[" << *it << "," << *(it + length) << "]";
                it += length;
            } else {
                out << *it;
            }
            needcomma = true;
        }
        out << " }";
    }

    return out;
}

const vbucket_state_t VBucket::ACTIVE = static_cast<vbucket_state_t>(htonl(vbucket_state_active));
const vbucket_state_t VBucket::REPLICA = static_cast<vbucket_state_t>(htonl(vbucket_state_replica));
const vbucket_state_t VBucket::PENDING = static_cast<vbucket_state_t>(htonl(vbucket_state_pending));
const vbucket_state_t VBucket::DEAD = static_cast<vbucket_state_t>(htonl(vbucket_state_dead));

void VBucket::fireAllOps(EventuallyPersistentEngine &engine, ENGINE_ERROR_CODE code) {
    if (pendingOpsStart > 0) {
        hrtime_t now = gethrtime();
        if (now > pendingOpsStart) {
            hrtime_t d = (now - pendingOpsStart) / 1000;
            stats.pendingOpsHisto.add(d);
            stats.pendingOpsMaxDuration.setIfBigger(d);
        }
    }
    pendingOpsStart = 0;
    stats.pendingOps.decr(pendingOps.size());
    stats.pendingOpsMax.setIfBigger(pendingOps.size());

    engine.notifyIOComplete(pendingOps, code);
    pendingOps.clear();

    getLogger()->log(EXTENSION_LOG_INFO, NULL,
                     "Fired pendings ops for vbucket %d in state %s\n",
                     id, VBucket::toString(state));
}

void VBucket::fireAllOps(EventuallyPersistentEngine &engine) {
    LockHolder lh(pendingOpLock);

    if (state == vbucket_state_active) {
        fireAllOps(engine, ENGINE_SUCCESS);
    } else if (state == vbucket_state_pending) {
        // Nothing
    } else {
        fireAllOps(engine, ENGINE_NOT_MY_VBUCKET);
    }
}

void VBucket::setState(vbucket_state_t to, SERVER_HANDLE_V1 *sapi) {
    assert(sapi);
    vbucket_state_t oldstate(state);

    if (to == vbucket_state_active && checkpointManager.getOpenCheckpointId() == 0) {
        checkpointManager.setOpenCheckpointId(1);
    }

    if (oldstate == vbucket_state_dead || to == vbucket_state_dead) {
        resetStats();
    }

    getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                     "transitioning vbucket %d from %s to %s\n",
                     id, VBucket::toString(oldstate), VBucket::toString(to));

    state = to;
}

void VBucket::doStatsForQueueing(QueuedItem& qi, size_t itemBytes)
{
    ++dirtyQueueSize;
    dirtyQueueMem.incr(sizeof(QueuedItem));
    ++dirtyQueueFill;
    dirtyQueueAge.incr(qi.getQueuedTime());
    dirtyQueuePendingWrites.incr(itemBytes);
}


void VBucket::doStatsForFlushing(QueuedItem& qi, size_t itemBytes)
{
    if (dirtyQueueSize > 0) {
        --dirtyQueueSize;
    }
    if (dirtyQueueMem > sizeof(QueuedItem)) {
        dirtyQueueMem.decr(sizeof(QueuedItem));
    } else {
        dirtyQueueMem.set(0);
    }
    ++dirtyQueueDrain;

    if (dirtyQueueAge > qi.getQueuedTime()) {
        dirtyQueueAge.decr(qi.getQueuedTime());
    } else {
        dirtyQueueAge.set(0);
    }

    if (dirtyQueuePendingWrites > itemBytes) {
        dirtyQueuePendingWrites.decr(itemBytes);
    } else {
        dirtyQueuePendingWrites.set(0);
    }
}

void VBucket::resetStats() {
    opsCreate.set(0);
    opsUpdate.set(0);
    opsDelete.set(0);
    opsReject.set(0);

    dirtyQueueSize.set(0);
    dirtyQueueMem.set(0);
    dirtyQueueFill.set(0);
    dirtyQueueAge.set(0);
    dirtyQueuePendingWrites.set(0);
    dirtyQueueDrain.set(0);
}
