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

#ifndef _PLASMA_WRAPPER_H
#define _PLASMA_WRAPPER_H

#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    SuccessUpdate = 1,
    SuccessInsert = 0,
    Success = 0,
    SuccessSync = 1,
    ErrInitPlasmaNotCalled = -1,
    ErrDbOpen = -2,
    ErrDbNotOpen = -3,
    ErrHandleNotInUse = -4,
    ErrInsertValue = -5,
    ErrItemNotFound = -6,
    ErrInternal = -7,
    ErrValueBufTooSmall = -8,
    ErrBackfillQueryNotOpen = -9,
    ErrBackfillQueryEOF = -10
} wrapper_err_codes_t;

enum {
    Plasma_KVengine = 1,
    Plasma_LocalDb = 2
};

void init_plasma(
        const uint64_t memQuota,
        const bool dio,
        const bool kv,
        const int cleaner,
        const int cleanermax,
        const int delta,
        const int items,
        const int segments,
        const int sync,
        const bool upsert);
int shutdown_plasma();
int open_plasma(
        const char *dbPath,
        const int vbid);
int close_plasma(
        const int vbid,
        const int handle_id,
        uint64_t *ret_seq_num);
int insert_kv(
        const int db,
        const int vbid,
        const int handle_id,
        const void *key,
        const int keylen,
        const void *value,
        const int valuelen,
        const uint64_t seq_num);
int delete_kv(
        const int db,
        const int vbid,
        const int handle_id,
        const void *key,
        const int keylen);
int lookup_kv(
        const int db,
        const int vbid,
        const int handle_id,
        const void *key,
        const int keylen,
        void **value,
        int *valuelen);
int open_backfill_query(const int vbid, const uint64_t seq_num);
int close_backfill_query(const int vbid, const int handle_id);
int next_backfill_query(
    const int vbid,
    const int handle_id,
    void **retkey,
    int *retkeylen,
    void **retval,
    int *retvallen,
    uint64_t *ret_seq_num);
void get_stats(
        const int vbid,
        uint64_t *di_memsz,
        uint64_t *di_memszidx,
        uint64_t *di_numpages,
        uint64_t *di_itemscount,
        uint64_t *di_lssfrag,
        uint64_t *di_lssdatasize,
        uint64_t *di_lssusedspace,
        uint64_t *di_reclaimpending,
        uint64_t *st_memsz,
        uint64_t *st_memszidx,
        uint64_t *st_reclaimpending);

#ifdef __cplusplus
}
#endif
#endif /* _PLASMA_WRAPPER_H */
