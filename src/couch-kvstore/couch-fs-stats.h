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

#include <memory>

#include <libcouchstore/couch_db.h>
#include <platform/histogram.h>

struct CouchstoreStats {
public:
    CouchstoreStats() :
        readSeekHisto(ExponentialGenerator<size_t>(1, 2), 50),
        readSizeHisto(ExponentialGenerator<size_t>(1, 2), 25),
        writeSizeHisto(ExponentialGenerator<size_t>(1, 2), 25),
        totalBytesRead(0),
        totalBytesWritten(0) { }

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
        totalBytesRead = 0;
        totalBytesWritten = 0;
    }
};

std::unique_ptr<FileOpsInterface> getCouchstoreStatsOps(CouchstoreStats& stats);

/**
 * FileOpsInterface implementation which records various statistics
 * about OS-level file operations performed by Couchstore.
 */
class StatsOps : public FileOpsInterface {
public:
    StatsOps(CouchstoreStats& _stats)
        : stats(_stats) {}

    couch_file_handle constructor(couchstore_error_info_t* errinfo) override ;
    couchstore_error_t open(couchstore_error_info_t* errinfo,
                            couch_file_handle* handle, const char* path,
                            int oflag) override;
    couchstore_error_t close(couchstore_error_info_t* errinfo,
                             couch_file_handle handle) override;
    ssize_t pread(couchstore_error_info_t* errinfo,
                  couch_file_handle handle, void* buf, size_t nbytes,
                  cs_off_t offset) override;
    ssize_t pwrite(couchstore_error_info_t* errinfo,
                   couch_file_handle handle, const void* buf,
                   size_t nbytes, cs_off_t offset) override;
    cs_off_t goto_eof(couchstore_error_info_t* errinfo,
                      couch_file_handle handle) override;
    couchstore_error_t sync(couchstore_error_info_t* errinfo,
                            couch_file_handle handle) override;
    couchstore_error_t advise(couchstore_error_info_t* errinfo,
                              couch_file_handle handle, cs_off_t offset,
                              cs_off_t len,
                              couchstore_file_advice_t advice) override;
    void destructor(couch_file_handle handle) override;

protected:
    CouchstoreStats& stats;
    struct StatFile {
        StatFile(FileOpsInterface* _orig_ops,
                 couch_file_handle _orig_handle,
                 cs_off_t _last_offs);

        FileOpsInterface* orig_ops;
        couch_file_handle orig_handle;
        cs_off_t last_offs;
    };

};

#endif  // SRC_COUCH_KVSTORE_COUCH_FS_STATS_H_
