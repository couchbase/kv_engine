/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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
                          theEngine.getConfiguration()) {
    }

    void setBackfillBufferSize(size_t newSize) {
        buffer.maxBytes = newSize;
    }

    bool getBackfillBufferFullStatus() {
        return buffer.full;
    }

    BackfillScanBuffer& public_getBackfillScanBuffer() {
        return scanBuffer;
    }

    using BackfillManager::getNumBackfills;
};
