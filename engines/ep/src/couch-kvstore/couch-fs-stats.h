/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2012-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <atomic>
#include <memory>

#include <libcouchstore/couch_db.h>

struct FileStats;

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
    couchstore_error_t set_periodic_sync(couch_file_handle handle,
                                         uint64_t period_bytes) override;
    couchstore_error_t set_tracing_enabled(couch_file_handle handle) override;
    couchstore_error_t set_write_validation_enabled(
            couch_file_handle handle) override;
    couchstore_error_t set_mprotect_enabled(couch_file_handle handle) override;

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
    FHStats* get_stats(couch_file_handle handle) override;
    void destructor(couch_file_handle handle) override;

protected:
    FileStats& stats;
    FileOpsInterface& wrapped_ops;

    struct StatFile : public FileOpsInterface::FHStats {
        StatFile(FileOpsInterface* _orig_ops,
                 couch_file_handle _orig_handle,
                 cs_off_t _last_offs);

        size_t getReadCount() override;
        size_t getWriteCount() override;
        size_t getWriteBytes() override;

        FileOpsInterface* orig_ops;
        couch_file_handle orig_handle;
        cs_off_t last_offs;

        /// Number of read() calls against this file since it was last opened.
        size_t read_count_since_open;
        /// Number of write() calls against this file since it was last opened.
        size_t write_count_since_open;
        /// Count of bytes written to this file since it was last opened.
        size_t write_bytes_since_open;
    };
};
