/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef SQLITE_BASE_H
#define SQLITE_BASE_H 1

#include <map>
#include <vector>

#ifdef USE_SYSTEM_LIBSQLITE3
#include <sqlite3.h>
#else
#include "embedded/sqlite3.h"
#endif

#include "sqlite-pst.hh"
#include "sqlite-strategies.hh"
#include "item.hh"
#include "stats.hh"

class EventuallyPersistentEngine;
class EPStats;

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

/**
 * Result of database mutation operations.
 *
 * This is a pair where .first is the number of rows affected, and
 * .second is the ID that was generated (if any).  .second will be 0
 * on updates (not generating an ID).
 *
 * .first will be -1 if there was an error performing the update.
 *
 * .first will be 0 if the update did not error, but did not occur.
 * This would generally be considered a fatal condition (in practice,
 * it requires you to be firing an update at a missing rowid).
 */
typedef std::pair<int, int64_t> mutation_result;

/**
 * A persistence store based on sqlite that uses a SqliteStrategy to
 * configure itself.
 */
class StrategicSqlite3 {
public:

    /**
     * Construct an instance of sqlite with the given database name.
     */
    StrategicSqlite3(EPStats &st, SqliteStrategy *s);

    /**
     * Copying opens a new underlying DB.
     */
    StrategicSqlite3(const StrategicSqlite3 &from);

    /**
     * Cleanup.
     */
    ~StrategicSqlite3() {
        close();
    }

    /**
     * Reset database to a clean state.
     */
    void reset();

    /**
     * Begin a transaction (if not already in one).
     */
    bool begin() {
        if(!intransaction) {
            if (execute("begin immediate") != -1) {
                intransaction = true;
            }
        }
        return intransaction;
    }

    /**
     * Commit a transaction (unless not currently in one).
     *
     * Returns false if the commit fails.
     */
    bool commit() {
        if(intransaction) {
            // If commit returns -1, we're still in a transaction.
            intransaction = (execute("commit") == -1);
        }
        // !intransaction == not in a transaction == committed
        return !intransaction;
    }

    /**
     * Rollback a transaction (unless not currently in one).
     */
    void rollback() {
        if(intransaction) {
            intransaction = false;
            execute("rollback");
        }
    }

    /**
     * Query the properties of the underlying storage.
     */
    StorageProperties getStorageProperties();

    /**
     * Overrides set().
     */
    void set(const Item &item, uint16_t vb_version, Callback<mutation_result> &cb);

    /**
     * Overrides get().
     */
    void get(const std::string &key, uint64_t rowid, Callback<GetValue> &cb);

    /**
     * Overrides del().
     */
    void del(const std::string &key, uint64_t rowid,
             Callback<int> &cb);

    void delInvalidItem(const std::string &key, uint64_t rowid);

    bool delVBucket(uint16_t vbucket, uint16_t vb_version,
                    std::pair<int64_t, int64_t> row_range);
    bool setVBState(uint16_t vbucket, uint16_t vb_version, const std::string &to);

    std::map<std::pair<uint16_t, uint16_t>, std::string> listPersistedVbuckets(void);

    /**
     * Take a snapshot of the stats in the main DB.
     */
    bool snapshotStats(const std::map<std::string, std::string> &m);
    /**
     * Take a snapshot of the vbucket states in the main DB.
     */
    bool snapshotVBuckets(const std::map<std::pair<uint16_t, uint16_t>, std::string> &m);

    /**
     * Overrides dump
     */
    void dump(Callback<GetValue> &cb);

private:
    /**
     * Shortcut to execute a simple query.
     *
     * @param query a simple query with no bindings to execute directly
     */
    int execute(const char *query) {
        PreparedStatement st(db, query);
        return st.execute();
    }

    template <typename T>
    bool storeMap(PreparedStatement *clearSt,
                  PreparedStatement *insSt,
                  const std::map<T, std::string> &m, bool pairKey = false);

    void insert(const Item &itm, uint16_t vb_version, Callback<mutation_result> &cb);
    void update(const Item &itm, uint16_t vb_version, Callback<mutation_result> &cb);
    int64_t lastRowId();

    EPStats &stats;

    /**
     * Direct access to the DB.
     */
    sqlite3 *db;

    void open() {
        assert(strategy);
        db = strategy->open();
        intransaction = false;
    }

    void close() {
        strategy->close();
        intransaction = false;
        db = NULL;
    }

    SqliteStrategy *strategy;

    bool intransaction;


    // Disallow assignment.
    void operator=(const StrategicSqlite3 &from);
};

#endif /* SQLITE_BASE_H */
