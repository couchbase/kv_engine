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
#include "dcp-producer.h"
#include "dcp-stream.h"
#include "mutex.h"

class EventuallyPersistentEngine;

class BackfillManager {
public:
    BackfillManager(EventuallyPersistentEngine* e, connection_t c);

    ~BackfillManager();

    void schedule(stream_t stream, uint64_t start, uint64_t end);

    bool bytesRead(uint32_t bytes);

    void bytesSent(uint32_t bytes);

    backfill_status_t backfill();

    void wakeUpTask();
    void wakeUpSnoozingBackfills(uint16_t vbid);

private:

    bool addIfLessThanMax(AtomicValue<uint32_t>& val, uint32_t incr,
                          uint32_t max);

    Mutex lock;
    std::queue<DCPBackfill*> activeBackfills;
    std::list<std::pair<rel_time_t, DCPBackfill*> > snoozingBackfills;
    //! When the number of (activeBackfills + snoozingBackfills) crosses a
    //!   threshold we use waitingBackfills
    std::queue<DCPBackfill*> pendingBackfills;
    EventuallyPersistentEngine* engine;
    connection_t conn;
    ExTask managerTask;

    //! The scan buffer is for the current stream being backfilled
    struct {
        uint32_t bytesRead;
        uint32_t itemsRead;
        uint32_t maxBytes;
        uint32_t maxItems;
    } scanBuffer;

    //! The buffer is the total bytes used by all backfills for this connection
    struct {
        uint32_t bytesRead;
        uint32_t maxBytes;
        uint32_t nextReadSize;
        bool full;
    } buffer;
};

class BackfillCallback: public Callback<uint64_t> {
public:
    BackfillCallback(uint64_t s, uint16_t vb, connection_t c)
        : seqno(s), vbid(vb), conn(c) {}

    void callback(uint64_t &curSeq) {
        if (curSeq >= seqno) {
            DcpProducer* producer = dynamic_cast<DcpProducer*> (conn.get());
            if (producer) {
                producer->getBackfillManager()->wakeUpSnoozingBackfills(vbid);
            }
            setStatus(ENGINE_SUCCESS);
        } else {
            setStatus(ENGINE_FAILED);
        }
    }

private:
    uint64_t seqno;
    uint16_t vbid;
    connection_t conn;
};

#endif  // SRC_DCP_BACKFILL_MANAGER_H_
