/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2012 Couchbase, Inc
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

#ifndef SRC_COUCH_KVSTORE_COUCH_FS_STATS_H_
#define SRC_COUCH_KVSTORE_COUCH_FS_STATS_H_ 1

#include "config.h"

#include "atomic.h"

#include <libcouchstore/couch_db.h>

#include <platform/histogram.h>

struct CouchstoreStats {
public:
    CouchstoreStats() :
        readSeekHisto(ExponentialGenerator<size_t>(1, 2), 50),
        readSizeHisto(ExponentialGenerator<size_t>(1, 2), 25),
        writeSizeHisto(ExponentialGenerator<size_t>(1, 2), 25) { }

    //Read time length
    Histogram<hrtime_t> readTimeHisto;
    //Distance from last read
    Histogram<size_t> readSeekHisto;
    //Size of read
    Histogram<size_t> readSizeHisto;
    //Write time length
    Histogram<hrtime_t> writeTimeHisto;
    //Write size
    Histogram<size_t> writeSizeHisto;
    //Time spent in sync
    Histogram<hrtime_t> syncTimeHisto;

    // total bytes read from disk.
    AtomicValue<size_t> totalBytesRead;
    // Total bytes written to disk.
    AtomicValue<size_t> totalBytesWritten;

    void reset() {
        readTimeHisto.reset();
        readSeekHisto.reset();
        readSizeHisto.reset();
        writeTimeHisto.reset();
        writeSizeHisto.reset();
        syncTimeHisto.reset();
    }
};

couch_file_ops getCouchstoreStatsOps(CouchstoreStats* stats);

#endif  // SRC_COUCH_KVSTORE_COUCH_FS_STATS_H_
