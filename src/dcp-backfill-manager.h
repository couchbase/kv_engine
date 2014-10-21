/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013 Couchbase, Inc
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

#ifndef SRC_DCP_BACKFILL_MANAGER_H_
#define SRC_DCP_BACKFILL_MANAGER_H_ 1

#include "config.h"
#include "connmap.h"
#include "dcp-backfill.h"
#include "dcp-stream.h"
#include "mutex.h"

class EventuallyPersistentEngine;

class BackfillManager {
public:
    BackfillManager(EventuallyPersistentEngine* e, connection_t c);

    ~BackfillManager();

    void schedule(stream_t stream, uint64_t start, uint64_t end);

    bool bytesRead(uint32_t bytes);

    backfill_status_t backfill();

private:

    bool addIfLessThanMax(AtomicValue<uint32_t>& val, uint32_t incr,
                          uint32_t max);

    Mutex lock;
    std::queue<DCPBackfill*> backfills;
    EventuallyPersistentEngine* engine;
    connection_t conn;
    uint64_t taskId;

    struct {
        AtomicValue<uint32_t> bytesRead;
        AtomicValue<uint32_t> itemsRead;
        uint32_t maxBytes;
        uint32_t maxItems;
    } scanBuffer;
};

#endif  // SRC_DCP_BACKFILL_MANAGER_H_
