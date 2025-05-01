/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <libcouchstore/couch_db.h>

class CouchBasicFileOps : public FileOpsInterface {
public:
    couch_file_handle constructor(couchstore_error_info_t* errinfo) override {
        return defaultOps.constructor(errinfo);
    }

    couchstore_error_t open(couchstore_error_info_t* errinfo,
                            couch_file_handle* handle,
                            const char* path,
                            int oflag) override {
        return defaultOps.open(errinfo, handle, path, oflag);
    }

    couchstore_error_t close(couchstore_error_info_t* errinfo,
                             couch_file_handle handle) override {
        return defaultOps.close(errinfo, handle);
    }

    couchstore_error_t set_periodic_sync(couch_file_handle handle,
                                         uint64_t period_bytes) override {
        return defaultOps.set_periodic_sync(handle, period_bytes);
    }

    couchstore_error_t set_tracing_enabled(couch_file_handle handle) override {
        return defaultOps.set_tracing_enabled(handle);
    }

    couchstore_error_t set_mprotect_enabled(couch_file_handle handle) override {
        return defaultOps.set_mprotect_enabled(handle);
    }

    ssize_t pread(couchstore_error_info_t* errinfo,
                  couch_file_handle handle,
                  void* buf,
                  size_t nbytes,
                  cs_off_t offset) override {
        return defaultOps.pread(errinfo, handle, buf, nbytes, offset);
    }

    ssize_t pwrite(couchstore_error_info_t* errinfo,
                   couch_file_handle handle,
                   const void* buf,
                   size_t nbytes,
                   cs_off_t offset) override {
        return defaultOps.pwrite(errinfo, handle, buf, nbytes, offset);
    }

    cs_off_t goto_eof(couchstore_error_info_t* errinfo,
                      couch_file_handle handle) override {
        return defaultOps.goto_eof(errinfo, handle);
    }

    couchstore_error_t sync(couchstore_error_info_t* errinfo,
                            couch_file_handle handle) override {
        return defaultOps.sync(errinfo, handle);
    }

    couchstore_error_t advise(couchstore_error_info_t* errinfo,
                              couch_file_handle handle,
                              cs_off_t offset,
                              cs_off_t len,
                              couchstore_file_advice_t advice) override {
        return defaultOps.advise(errinfo, handle, offset, len, advice);
    }

    void tag(couch_file_handle handle, FileTag tag) override {
        defaultOps.tag(handle, tag);
    }

    FHStats* get_stats(couch_file_handle handle) override {
        return defaultOps.get_stats(handle);
    }

    void destructor(couch_file_handle handle) override {
        defaultOps.destructor(handle);
    }

private:
    FileOpsInterface& defaultOps = *couchstore_get_default_file_ops();
};
