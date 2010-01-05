/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef SQLITE_BASE_H
#define SQLITE_BASE_H 1

#include <sqlite3.h>

#include "kvstore.hh"

/**
 * A sqlite prepared statement.
 */
class PreparedStatement {
public:

    /**
     * Construct a prepared statement.
     *
     * @param d the DB where the prepared statement will execute
     * @param query the query to prepare
     */
    PreparedStatement(sqlite3 *d, const char *query);

    /**
     * Clean up.
     */
    ~PreparedStatement();

    /**
     * Bind a null-terminated string parameter to a binding in
     * this statement.
     *
     * @param pos the binding position (starting at 1)
     * @param s the value to bind
     */
    void bind(int pos, const char *s);

    /**
     * Bind a string parameter to a binding in this statement.
     *
     * @param pos the binding position (starting at 1)
     * @param s the value to bind
     * @param nbytes number of bytes in the string.
     */
    void bind(int pos, const char *s, size_t nbytes);

    /**
     * Execute a prepared statement that does not return results.
     *
     * @return how many rows were affected
     */
    int execute();

    /**
     * Execute a prepared statement that does return results
     * and/or return the next row.
     *
     * @return true if there are more rows after this one
     */
    bool fetch();

    /**
     * Reset the bindings.
     *
     * Call this before reusing a prepared statement.
     */
    void reset();

    /**
     * Get the value at a given column in the current row.
     *
     * Use this along with fetch.
     *
     * @param x the column number (starting at 1)
     * @return the value
     */
    const char *column(int x);

private:
    sqlite3      *db;
    sqlite3_stmt *st;
};

/**
 * The sqlite driver.
 */
class BaseSqlite3 : public KVStore {
public:

    /**
     * Construct an instance of sqlite with the given database name.
     */
    BaseSqlite3(const char *fn);

    /**
     * Cleanup.
     */
    ~BaseSqlite3();

    /**
     * Reset database to a clean state.
     */
    void reset();

    /**
     * Begin a transaction (if not already in one).
     */
    void begin();

    /**
     * Commit a transaction (unless not currently in one).
     */
    void commit();

    /**
     * Rollback a transaction (unless not currently in one).
     */
    void rollback();

protected:

    /**
     * Shortcut to execute a simple query.
     *
     * @param query a simple query with no bindings to execute directly
     */
    void execute(const char *query);

    /**
     * After setting up the DB, this is called to initialize our
     * prepared statements.
     */
    virtual void initStatements() {}

    /**
     * When tearing down, tear down the statements set up by
     * initStatements.
     */
    virtual void destroyStatements() {}

    /**
     * Set up the tables.
     */
    virtual void initTables() {}

    /**
     * Clean up the tables.
     */
    virtual void destroyTables() {}

protected:
    /**
     * Direct access to the DB.
     */
    sqlite3 *db;

    void open();
    void close();

private:

    const char *filename;
    bool intransaction;

};

class Sqlite3 : public BaseSqlite3 {
public:

    Sqlite3(const char *path, bool is_auditable=false) :
        BaseSqlite3(path), ins_stmt(NULL), sel_stmt(NULL), del_stmt(NULL),
        auditable(is_auditable)
    {
        open();
        initTables();
        initStatements();
    }

    /**
     * Overrides set() to call the char* variant.
     */
    void set(std::string &key, std::string &val, Callback<bool> &cb);

    /**
     * Overrides set().
     */
    void set(std::string &key, const char *val, size_t nbytes,
             Callback<bool> &cb);

    /**
     * Overrides get().
     */
    void get(std::string &key, Callback<GetValue> &cb);

    /**
     * Overrides del().
     */
    void del(std::string &key, Callback<bool> &cb);

    /**
     * Overrides dump
     */
    virtual void dump(Callback<KVPair> &cb);

protected:

    void initStatements();

    void destroyStatements();

    void initTables();

    void destroyTables();

private:
    bool               auditable;
    PreparedStatement *ins_stmt;
    PreparedStatement *sel_stmt;
    PreparedStatement *del_stmt;
};

#endif /* SQLITE_BASE_H */
