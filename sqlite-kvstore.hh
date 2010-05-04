/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef SQLITE_BASE_H
#define SQLITE_BASE_H 1

#include <vector>

#include <sqlite3.h>

#include "kvstore.hh"
#include "sqlite-pst.hh"
#include "sqlite-strategies.hh"

class StrategicSqlite3 : public KVStore {
public:

    /**
     * Construct an instance of sqlite with the given database name.
     */
    StrategicSqlite3(SqliteStrategy *s) : strategy(s), intransaction(false) {
        open();
    }

    /**
     * Cleanup.
     */
    ~StrategicSqlite3() {
        close();
    }

    /**
     * Reset database to a clean state.
     */
    void reset() {
        if (db) {
            rollback();
            close();
            open();
            strategy->destroyTables();
            strategy->initTables();
            execute("vacuum");
        }
    }

    /**
     * Begin a transaction (if not already in one).
     */
    void begin() {
        if(!intransaction) {
            execute("begin");
            intransaction = true;
        }
    }

    /**
     * Commit a transaction (unless not currently in one).
     */
    void commit() {
        if(intransaction) {
            execute("commit");
            intransaction = false;
        }
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
     * Overrides set().
     */
    void set(const Item &item, Callback<bool> &cb);

    /**
     * Overrides get().
     */
    void get(const std::string &key, Callback<GetValue> &cb);

    /**
     * Overrides del().
     */
    void del(const std::string &key, Callback<bool> &cb);

    /**
     * Overrides dump
     */
    virtual void dump(Callback<GetValue> &cb);

private:
    /**
     * Shortcut to execute a simple query.
     *
     * @param query a simple query with no bindings to execute directly
     */
    void execute(const char *query) {
        PreparedStatement st(db, query);
        st.execute();
    }

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
};

#endif /* SQLITE_BASE_H */
