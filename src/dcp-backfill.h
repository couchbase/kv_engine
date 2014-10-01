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

#ifndef SRC_DCP_BACKFILL_H_
#define SRC_DCP_BACKFILL_H_ 1

#include "config.h"

#include "callbacks.h"
#include "dcp-stream.h"

class EventuallyPersistentEngine;

class CacheCallback : public Callback<CacheLookup> {
public:
    CacheCallback(EventuallyPersistentEngine* e, stream_t &s);

    void callback(CacheLookup &lookup);

private:
    EventuallyPersistentEngine* engine_;
    stream_t stream_;
};

class DiskCallback : public Callback<GetValue> {
public:
    DiskCallback(stream_t &s);

    void callback(GetValue &val);

private:
    stream_t stream_;
};

class DCPBackfill : public GlobalTask {
public:
    DCPBackfill(EventuallyPersistentEngine* e, stream_t s,
                uint64_t start_seqno, uint64_t end_seqno, const Priority &p,
                double sleeptime = 0, bool shutdown = false);

    bool run();

    std::string getDescription();

private:
    EventuallyPersistentEngine *engine;
    stream_t                    stream;
    uint64_t                    startSeqno;
    uint64_t                    endSeqno;
};

#endif  // SRC_DCP_BACKFILL_H_
