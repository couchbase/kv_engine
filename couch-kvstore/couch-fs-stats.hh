#ifndef COUCH_FS_STATS_H
#define COUCH_FS_STATS_H 1
#include <libcouchstore/couch_db.h>

struct CouchstoreStats {
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
};

couch_file_ops getCouchstoreStatsOps(CouchstoreStats* stats);

#endif
