/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc
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

#include "vbucket.h"

/**
 * Friend class of VBucket. Used for accessing into VBukcet for testing.
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
};
