/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "dcp/backfill-manager.h"
#include "dcp/dcpconnmap.h"
#include "ep_engine.h"

/*
 * Mock of the BackfillManager class.  Wraps the real BackfillManager, but
 * exposes normally protected methods publically for test purposes.
 */
class MockDcpBackfillManager : public BackfillManager {
public:
    explicit MockDcpBackfillManager(EventuallyPersistentEngine& theEngine)
        : BackfillManager(*theEngine.getKVBucket(),
                          theEngine.getDcpConnMap(),
                          "MockDcpBackfillManager",
                          theEngine.getConfiguration()) {
    }

    void setBackfillBufferSize(size_t newSize) {
        buffer.maxBytes = newSize;
    }

    void setBackfillBufferBytesRead(size_t newSize) {
        buffer.bytesRead = newSize;
    }

    bool getBackfillBufferFullStatus() {
        return buffer.full;
    }

    BackfillScanBuffer& public_getBackfillScanBuffer() {
        return scanBuffer;
    }

    UniqueDCPBackfillPtr public_dequeueNextBackfill() {
        std::unique_lock<std::mutex> lh(lock);

        return dequeueNextBackfill(lh).first;
    }

    using BackfillManager::getNumBackfills;
};
