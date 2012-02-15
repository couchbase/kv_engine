/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef SQLITE_STATS_HH
#define SQLITE_STATS_HH 1

#include "common.hh"
#include "atomic.hh"
#include "histo.hh"

#ifndef DEFAULT_MAX_DATA_SIZE
/* Something something something ought to be enough for anybody */
#define DEFAULT_MAX_DATA_SIZE (std::numeric_limits<size_t>::max())
#endif

/**
 * What's going on with this storage engine and stuff?
 */
class SQLiteStats {
public:

    SQLiteStats() :
        readSeekHisto(ExponentialGenerator<size_t>(1, 2), 50),
        readSizeHisto(ExponentialGenerator<size_t>(1, 2), 25),
        writeSeekHisto(ExponentialGenerator<size_t>(1, 2), 50),
        writeSizeHisto(ExponentialGenerator<size_t>(1, 2), 25) {
    }

    //! Number of close() calls
    Atomic<size_t> numClose;
    //! Number of open() calls
    Atomic<size_t> numOpen;
    //! Sector size discovered by the VFS layer.
    Atomic<size_t> sectorSize;

    //! How long it takes us to complete a read
    Histogram<hrtime_t> readTimeHisto;
    //! How far we move the head on a read
    Histogram<size_t> readSeekHisto;
    //! How big are our reads?
    Histogram<size_t> readSizeHisto;

    //! How long it takes us to complete a write
    Histogram<hrtime_t> writeTimeHisto;
    //! How far we move the head on a write
    Histogram<size_t> writeSeekHisto;
    //! How big are our writes?
    Histogram<size_t> writeSizeHisto;

    //! Number of truncate() calls
    Atomic<size_t> numTruncates;

    //! Time spent in sync() calls
    Histogram<hrtime_t> syncTimeHisto;

    //! Tiem spent in delete() calls.
    Histogram<hrtime_t> deleteHisto;

    //! Number of locks acquired.
    Atomic<size_t> numLocks;
};

#endif /* SQLITE_STATS_HH */
