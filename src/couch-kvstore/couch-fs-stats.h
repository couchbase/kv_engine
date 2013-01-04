#ifndef SRC_COUCH_KVSTORE_COUCH_FS_STATS_H_
#define SRC_COUCH_KVSTORE_COUCH_FS_STATS_H_ 1

#include "config.h"

#include <libcouchstore/couch_db.h>

#include "histo.h"

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
