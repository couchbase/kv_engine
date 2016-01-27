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

#include "config.h"

#include "common.h"
#include "couch-kvstore/couch-fs-stats.h"
#include <platform/histogram.h>

extern "C" {
static couch_file_handle cfs_construct(couchstore_error_info_t*, void* cookie);
static couchstore_error_t cfs_open(couchstore_error_info_t*,
                                   couch_file_handle*, const char*, int);
static void cfs_close(couchstore_error_info_t*, couch_file_handle);
static ssize_t cfs_pread(couchstore_error_info_t*, couch_file_handle,
                         void *, size_t, cs_off_t);
static ssize_t cfs_pwrite(couchstore_error_info_t*, couch_file_handle,
                          const void *, size_t, cs_off_t);
static cs_off_t cfs_goto_eof(couchstore_error_info_t*, couch_file_handle);
static couchstore_error_t cfs_sync(couchstore_error_info_t*, couch_file_handle);
static couchstore_error_t cfs_advise(couchstore_error_info_t*,
                                     couch_file_handle,
                                     cs_off_t, cs_off_t,
                                     couchstore_file_advice_t);
static void cfs_destroy(couchstore_error_info_t*,couch_file_handle);
}

couch_file_ops getCouchstoreStatsOps(CouchstoreStats* stats) {
    couch_file_ops ops = {
        5,
        cfs_construct,
        cfs_open,
        cfs_close,
        cfs_pread,
        cfs_pwrite,
        cfs_goto_eof,
        cfs_sync,
        cfs_advise,
        cfs_destroy,
        stats
    };
    return ops;
}

struct StatFile {
    const couch_file_ops* orig_ops;
    couch_file_handle orig_handle;
    CouchstoreStats* stats;
    cs_off_t last_offs;
};

extern "C" {
    static couch_file_handle cfs_construct(couchstore_error_info_t *errinfo,
                                           void* cookie) {
        StatFile* sf = new StatFile;
        sf->stats = static_cast<CouchstoreStats*>(cookie);
        sf->orig_ops = couchstore_get_default_file_ops();
        sf->orig_handle = sf->orig_ops->constructor(errinfo,
                                                    sf->orig_ops->cookie);
        sf->last_offs = 0;
        return reinterpret_cast<couch_file_handle>(sf);
    }

    static couchstore_error_t cfs_open(couchstore_error_info_t *errinfo,
                                       couch_file_handle* h,
                                       const char* path,
                                       int flags) {
        StatFile* sf = reinterpret_cast<StatFile*>(*h);
        return sf->orig_ops->open(errinfo, &sf->orig_handle, path, flags);
    }

    static void cfs_close(couchstore_error_info_t *errinfo,
                          couch_file_handle h) {
        StatFile* sf = reinterpret_cast<StatFile*>(h);
        sf->orig_ops->close(errinfo, sf->orig_handle);
    }

    static ssize_t cfs_pread(couchstore_error_info_t *errinfo,
                             couch_file_handle h,
                             void* buf,
                             size_t sz,
                             cs_off_t off) {
        StatFile* sf = reinterpret_cast<StatFile*>(h);
        sf->stats->readSizeHisto.add(sz);
        if(sf->last_offs) {
            sf->stats->readSeekHisto.add(std::abs(off - sf->last_offs));
        }
        sf->last_offs = off;
        BlockTimer bt(&sf->stats->readTimeHisto);
        ssize_t result = sf->orig_ops->pread(errinfo, sf->orig_handle, buf,
                                             sz, off);
        if (result > 0) {
            sf->stats->totalBytesRead += result;
        }
        return result;
    }

    static ssize_t cfs_pwrite(couchstore_error_info_t *errinfo,
                              couch_file_handle h,
                              const void* buf,
                              size_t sz,
                              cs_off_t off) {
        StatFile* sf = reinterpret_cast<StatFile*>(h);
        sf->stats->writeSizeHisto.add(sz);
        BlockTimer bt(&sf->stats->writeTimeHisto);
        ssize_t result = sf->orig_ops->pwrite(errinfo, sf->orig_handle, buf,
                                              sz, off);
        if (result > 0) {
            sf->stats->totalBytesWritten += result;
        }
        return result;
    }

    static cs_off_t cfs_goto_eof(couchstore_error_info_t *errinfo,
                                 couch_file_handle h) {
        StatFile* sf = reinterpret_cast<StatFile*>(h);
        return sf->orig_ops->goto_eof(errinfo, sf->orig_handle);
    }

    static couchstore_error_t cfs_sync(couchstore_error_info_t *errinfo,
                                       couch_file_handle h) {
        StatFile* sf = reinterpret_cast<StatFile*>(h);
        BlockTimer bt(&sf->stats->syncTimeHisto);
        return sf->orig_ops->sync(errinfo, sf->orig_handle);
    }

    static couchstore_error_t cfs_advise(couchstore_error_info_t *errinfo,
                                         couch_file_handle h,
                                         cs_off_t offs,
                                         cs_off_t len,
                                         couchstore_file_advice_t adv) {
        StatFile* sf = reinterpret_cast<StatFile*>(h);
        return sf->orig_ops->advise(errinfo, sf->orig_handle, offs, len, adv);
    }

    static void cfs_destroy(couchstore_error_info_t *errinfo,
                            couch_file_handle h) {
        StatFile* sf = reinterpret_cast<StatFile*>(h);
        sf->orig_ops->destructor(errinfo, sf->orig_handle);
        delete sf;
    }

}
