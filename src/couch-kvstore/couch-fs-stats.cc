#include "config.h"
#include "common.hh"
#include "histo.hh"
#include "couch-kvstore/couch-fs-stats.hh"

extern "C" {
static couch_file_handle cfs_construct(void* cookie);
static couchstore_error_t cfs_open(couch_file_handle*, const char*, int);
static void cfs_close(couch_file_handle);
static ssize_t cfs_pread(couch_file_handle, void *, size_t, cs_off_t);
static ssize_t cfs_pwrite(couch_file_handle, const void *, size_t, cs_off_t);
static cs_off_t cfs_goto_eof(couch_file_handle);
static couchstore_error_t cfs_sync(couch_file_handle);
static void cfs_destroy(couch_file_handle);
}

couch_file_ops getCouchstoreStatsOps(CouchstoreStats* stats) {
    couch_file_ops ops = {
        3,
        cfs_construct,
        cfs_open,
        cfs_close,
        cfs_pread,
        cfs_pwrite,
        cfs_goto_eof,
        cfs_sync,
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
static couch_file_handle cfs_construct(void* cookie) {
    StatFile* sf = new StatFile;
    sf->stats = static_cast<CouchstoreStats*>(cookie);
    sf->orig_ops = couchstore_get_default_file_ops();
    sf->orig_handle = sf->orig_ops->constructor(sf->orig_ops->cookie);
    sf->last_offs = 0;
    return reinterpret_cast<couch_file_handle>(sf);
}

static couchstore_error_t cfs_open(couch_file_handle* h, const char* path, int flags) {
    StatFile* sf = reinterpret_cast<StatFile*>(*h);
    return sf->orig_ops->open(&sf->orig_handle, path, flags);
}

static void cfs_close(couch_file_handle h) {
    StatFile* sf = reinterpret_cast<StatFile*>(h);
    sf->orig_ops->close(sf->orig_handle);
}

static ssize_t cfs_pread(couch_file_handle h, void* buf, size_t sz, cs_off_t off) {
    StatFile* sf = reinterpret_cast<StatFile*>(h);
    sf->stats->readSizeHisto.add(sz);
    if(sf->last_offs) {
        sf->stats->readSeekHisto.add(abs(off - sf->last_offs));
    }
    sf->last_offs = off;
    BlockTimer bt(&sf->stats->readTimeHisto);
    return sf->orig_ops->pread(sf->orig_handle, buf, sz, off);
}

static ssize_t cfs_pwrite(couch_file_handle h, const void* buf, size_t sz, cs_off_t off) {
    StatFile* sf = reinterpret_cast<StatFile*>(h);
    sf->stats->writeSizeHisto.add(sz);
    BlockTimer bt(&sf->stats->writeTimeHisto);
    return sf->orig_ops->pwrite(sf->orig_handle, buf, sz, off);
}

static cs_off_t cfs_goto_eof(couch_file_handle h) {
    StatFile* sf = reinterpret_cast<StatFile*>(h);
    return sf->orig_ops->goto_eof(sf->orig_handle);
}

static couchstore_error_t cfs_sync(couch_file_handle h) {
    StatFile* sf = reinterpret_cast<StatFile*>(h);
    BlockTimer bt(&sf->stats->syncTimeHisto);
    return sf->orig_ops->sync(sf->orig_handle);
}

static void cfs_destroy(couch_file_handle h) {
    StatFile* sf = reinterpret_cast<StatFile*>(h);
    sf->orig_ops->destructor(sf->orig_handle);
    delete sf;
}

}
