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

#include <atomic>
#include <memory>

#include <libcouchstore/couch_db.h>
#include <platform/histogram.h>
#include "kvstore.h"

/**
 * Returns an instance of StatsOps from a FileStats reference and
 * a reference to a base FileOps implementation to wrap
 */
std::unique_ptr<FileOpsInterface> getCouchstoreStatsOps(
    FileStats& stats, FileOpsInterface& base_ops);

/**
 * FileOpsInterface implementation which records various statistics
 * about OS-level file operations performed by Couchstore.
 */
class StatsOps : public FileOpsInterface {
public:
    StatsOps(FileStats& _stats, FileOpsInterface& ops)
        : stats(_stats),
          wrapped_ops(ops) {}

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
    FileStats& stats;
    FileOpsInterface& wrapped_ops;

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
