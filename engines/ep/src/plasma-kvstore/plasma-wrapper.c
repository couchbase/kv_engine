#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>

#include "plasma-wrapper.h"
#include "libplasma-core.h"

#define DEFAULT_VALUE_SIZE (2048)

const bool debug = false;

void
init_plasma(const uint64_t memQuota,
        const bool dio,
        const bool kv,
        const int cleaner,
        const int cleanermax,
        const int delta,
        const int items,
        const int segments,
        const int sync,
        const bool upsert)
{
    GoUint64 mq = memQuota;
    GoUint8 di = dio;
    GoUint8 kvsep = kv;
    GoInt32 cl = cleaner;
    GoInt32 clmax = cleanermax;
    GoInt32 dl = delta;
    GoInt32 it = items;
    GoInt32 seg = segments;
    GoInt32 s = sync;
    GoUint8 u = upsert;

    InitPlasma(mq, di, kvsep, cl, clmax, dl, it, seg, s, u);
}

int
shutdown_plasma()
{
    GoInt perr;

    perr = ShutdownPlasma();
    return (int)perr;
}

int
open_plasma(const char *dbPath, const int vbid)
{
    GoString path;
    GoInt vBucketId;
    GoInt plasma_handle;

    path.p = dbPath;
    path.n = strlen(dbPath);
    vBucketId = (GoInt)vbid;

    plasma_handle = OpenPlasma(path, vBucketId);
    if (debug) {
        fprintf(stderr, "OpenPlasma(%s, %d) %d\n",
                (char *)path.p, (int)vBucketId, (int)plasma_handle);
    }
    return (int)plasma_handle;
}

int
close_plasma(const int vbid, const int handle_id, uint64_t *ret_seq_num)
{
    GoInt vBucketId = (GoInt)vbid;
    GoInt plasma_handle = (GoInt)handle_id;
    GoInt perr;
    GoUint64 retSeqNum;

    perr = ClosePlasma(vBucketId, plasma_handle, &retSeqNum);
    if (debug) {
        fprintf(stderr, "ClosePlasma(%d, %d) %d\n",
                (int)vBucketId, (int)plasma_handle, (int)perr);
    }

    *ret_seq_num = (uint64_t)retSeqNum;

    return (int)perr;
}

int
insert_kv(const int db,
        const int vbid,
        const int handle_id,
        const void *key,
        const int keylen,
        const void *value,
        const int valuelen,
        const uint64_t seq_num)
{
    GoInt plasmaDb = (GoInt)db;
    GoInt vBucketId = (GoInt)vbid;
    GoInt plasma_handle = (GoInt)handle_id;
    GoString gokey, govalue;
    GoUint64 goseq;
    GoInt perr;

    gokey.p = key;
    gokey.n = keylen;
    govalue.p = value;
    govalue.n = valuelen;
    goseq = (GoUint64)seq_num;

    perr = InsertKV(plasmaDb, vBucketId, plasma_handle, gokey, govalue, goseq);

    if (debug) {
        fprintf(stderr, "InsertKV(%d, %d, %d, %s, %d, %20.20s, %d, %lu) %d\n",
                db, vbid, handle_id, (char *)key,
                keylen, (char *)value, valuelen, seq_num, (int)perr);
    }
    return (int)perr;
}

int
delete_kv(const int db,
        const int vbid,
        const int handle_id,
        const void *key,
        const int keylen)
{
    GoInt plasmaDb = (GoInt)db;
    GoInt vBucketId = (GoInt)vbid;
    GoInt plasma_handle = (GoInt)handle_id;
    GoString gokey;
    GoInt perr;

    gokey.p = key;
    gokey.n = keylen;

    perr = DeleteKV(plasmaDb, vBucketId, plasma_handle, gokey);
    if (debug) {
        fprintf(stderr, "DeleteKV(%d, %d, %d, %s, %d) %d\n",
                db, vbid, handle_id, (char *)key,
                keylen, (int)perr);
    }
    return (int)perr;
}

int
lookup_kv(const int db,
        const int vbid,
        const int handle_id,
        const void *key,
        const int keylen,
        void **value,
        int *valuelen)
{
    GoInt plasmaDb = (GoInt)db;
    GoInt vBucketId = (GoInt)vbid;
    GoInt plasma_handle = (GoInt)handle_id;
    GoString gokey;
    GoInt ret;
    GoInt govaluelen;

    gokey.p = key;
    gokey.n = keylen;
    govaluelen = (GoInt)*valuelen;

    ret = LookupKV(plasmaDb, vBucketId, plasma_handle, gokey, value, &govaluelen);

    *valuelen = (int)govaluelen;

    if (debug) {
        fprintf(stderr, "LookupKV(%d, %d, %d, %s, %d) %30.30s %d %d\n",
                db, vbid, handle_id, (char *)key,
                keylen, (char *)*value, *valuelen, (int)ret);
    }

    return (int)ret;
}

void
get_stats(const int vbid,
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
    uint64_t *st_reclaimpending)
{
    struct PlasmaStats_return psr;
    GoInt vBucketId = (GoInt)vbid;

    psr = PlasmaStats(vBucketId);

    *di_memsz = (uint64_t)psr.r0;
    *di_memszidx = (uint64_t)psr.r1;
    *di_numpages = (uint64_t)psr.r2;
    *di_itemscount = (uint64_t)psr.r3;
    *di_lssfrag = (uint64_t)psr.r4;
    *di_lssdatasize = (uint64_t)psr.r5;
    *di_lssusedspace = (uint64_t)psr.r6;
    *di_reclaimpending = (uint64_t)psr.r7;
    *st_memsz = (uint64_t)psr.r8;
    *st_memszidx = (uint64_t)psr.r9;
    *st_reclaimpending = (uint64_t)psr.r10;

    return;
}

int
open_backfill_query(const int vbid, const uint64_t seq_num)
{
    GoInt vBucketId;
    GoInt plasma_handle;
    GoUint64 goseq;

    vBucketId = (GoInt)vbid;
    goseq = (GoUint64)seq_num;

    plasma_handle = OpenBackfillQuery(vBucketId, goseq);
    if (debug) {
        fprintf(stderr, "OpenPlasma(%d) %d\n", (int)vBucketId, (int)plasma_handle);
    }
    return (int)plasma_handle;
}

int
close_backfill_query(const int vbid, const int handle_id)
{
    GoInt vBucketId = (GoInt)vbid;
    GoInt plasma_handle = (GoInt)handle_id;
    GoInt perr;

    perr = CloseBackfillQuery(vBucketId, plasma_handle);
    if (debug) {
        fprintf(stderr, "CloseBackfillQuery(%d, %d) %d\n",
                (int)vBucketId, (int)plasma_handle, (int)perr);
    }
    return (int)perr;
}

int
next_backfill_query(
        const int vbid,
        const int handle_id,
        void **retkey,
        int *retkeylen,
        void **retval,
        int *retvallen,
        uint64_t *ret_seq_num)
{
    GoInt vBucketId = (GoInt)vbid;
    GoInt plasma_handle = (GoInt)handle_id;
    GoInt keyLen, valueLen;
    GoUint64 seqNum;
    GoInt ret;

    keyLen = (GoInt)*retkeylen;
    valueLen = (GoInt)*retvallen;

    ret = NextBackfillQuery(vBucketId, plasma_handle, retkey, &keyLen, retval, &valueLen, &seqNum);

    *retkeylen = (int)keyLen;
    *retvallen = (int)valueLen;
    *ret_seq_num = (uint64_t)seqNum;

    return (int)ret;
}
