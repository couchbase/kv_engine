/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "vbucket.h"

#include "durability/durability_monitor.h"

/**
 * Friend class of VBucket. Used for accessing into VBucket for testing.
 */
class VBucketTestIntrospector {
public:
    static ActiveDurabilityMonitor& public_getActiveDM(VBucket& vb) {
        return vb.getActiveDM();
    }

    static PassiveDurabilityMonitor& public_getPassiveDM(VBucket& vb) {
        return vb.getPassiveDM();
    }

    static void setSeqnoAckCb(VBucket& vb, SeqnoAckCallback func) {
        vb.seqnoAckCb = func;
    }

    static const EPStats& getStats(VBucket& vb) {
        return vb.stats;
    }

    static void destroyDM(VBucket& vb) {
        vb.durabilityMonitor.reset();
    }

    static void setIsCalledHook(VBucket& vb, std::function<void()> hook) {
        vb.isCalledHook = hook;
    }

    static void setFetchValidValueHook(VBucket& vb,
                                       TestingHook<folly::SharedMutex&> hook) {
        vb.fetchValidValueHook = hook;
    }

    static void setSoftDeleteStoredValueHook(
            VBucket& vb, TestingHook<folly::SharedMutex&> hook) {
        vb.softDeleteStoredValueHook = hook;
    }
};
