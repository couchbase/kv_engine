/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef KVSTORE_HH
#define KVSTORE_HH 1

/**
 * Properites of the storage layer.
 *
 * If concurrent filesystem access is possible, maxConcurrency() will
 * be greater than one.  One will need to determine whether more than
 * one writer is possible as well as whether more than one reader is
 * possible.
 */
class StorageProperties {
public:

    StorageProperties(size_t c, size_t r, size_t w)
        : maxc(c), maxr(r), maxw(w) {}

    //! The maximum number of active queries.
    size_t maxConcurrency() { return maxc; }
    //! Maximum number of active read-only connections.
    size_t maxReaders()     { return maxr; }
    //! Maximum number of active connections for read and write.
    size_t maxWriters()     { return maxw; }

private:
    size_t maxc;
    size_t maxr;
    size_t maxw;
};

#endif // KVSTORE_HH
