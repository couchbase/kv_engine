/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef SQLITE_PST_H
#define SQLITE_PST_H 1

#include <string>
#include <stdio.h>
#include <inttypes.h>
#ifdef USE_SYSTEM_LIBSQLITE3
#include <sqlite3.h>
#else
#include "embedded/sqlite3.h"
#endif

#include "common.hh"
#include "kvstore.hh"

/**
 * A sqlite prepared statement.
 *
 * Note that each bind method returns the number of columns it
 * consumed (allowing you to, for example, bind a std::pair over two
 * columns).
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
     *
     * @return 1
     */
    int bind(int pos, const char *s);

    /**
     * Bind a string parameter to a binding in this statement.
     *
     * @param pos the binding position (starting at 1)
     * @param s the value to bind
     * @param nbytes number of bytes in the string.
     *
     * @return 1
     */
    int bind(int pos, const char *s, size_t nbytes);

    /**
     * Bind a string parameter to a binding in this statement.
     *
     * @param pos the binding position (starting at 1)
     * @param s the string to bind
     *
     * @return 1
     */
    int bind(int pos, const std::string &s);

    /**
     * Bind a uint32 value.
     *
     * @param pos the binding position (starting at 1)
     * @param d the value to bind
     */
    int bind(int pos, int d);

    /**
     * Bind a pair of uint32 values.
     *
     * @param pos the binding start position (starting at 1)
     * @param pv the pair values to bind
     *
     * @return 2
     */
    int bind(int pos, std::pair<int, int> pv);

    /**
     * Bind a vbucket_state consisting of state and checkpointId.
     *
     * @param pos the binding start position (starting at 1)
     * @param vb_state the vbucket_state instance to bind
     *
     * @return 2
     */
    int bind(int pos, vbucket_state vb_state);

    /**
     * Bind a uint64 value.
     *
     * @param pos the binding position (starting at 1)
     * @param d the value to bind
     *
     * @return 1
     */
    int bind64(int pos, uint64_t d);

    /**
     * Return the number of parameters required by this statement.
     */
    size_t paramCount();

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
     * @param x the column number
     * @return the value
     */
    const char *column(int x);

    /**
     * Get the value as a blob from the given column in the current
     * row.
     *
     * Use this along with fetch and column_bytes().
     *
     * @param x the column number
     * @return the value
     */
    const void *column_blob(int x);

    /**
     * Get the number of bytes stored in the given column for the
     * current row.
     *
     * Use this along with fetch and column() or column_blob().
     *
     * @param x the column number
     * @return the number of bytes found
     */
    int column_bytes(int x);

    /**
     * Get the integer valueof the given column at the current row.
     *
     * Use this along with fetch.
     *
     * @param x the column number
     * @return the value
     */
    int column_int(int x);

    /**
     * Get the 64-bit integer value of the given column at the current
     * row.
     *
     * Use this along with fetch.
     *
     * @param x the column number
     * @return the value
     */
    uint64_t column_int64(int x);

private:
    sqlite3      *db;
    sqlite3_stmt *st;
};

/**
 * Builds PreparedStatement instances for Statements.
 */
class StatementFactory {
public:

    virtual ~StatementFactory() { }

    virtual PreparedStatement *mkInsert(sqlite3 *dbh,
                                        const std::string &table) const;
    virtual PreparedStatement *mkUpdate(sqlite3 *dbh,
                                        const std::string &table) const;
    virtual PreparedStatement *mkSelect(sqlite3 *dbh,
                                        const std::string &table) const;
    virtual PreparedStatement *mkSelectAll(sqlite3 *dbh,
                                           const std::string &table) const;
    virtual PreparedStatement *mkCountAll(sqlite3 *dbh,
                                          const std::string &table) const;
    virtual PreparedStatement *mkDelete(sqlite3 *dbh,
                                        const std::string &table) const;
    virtual PreparedStatement *mkDeleteVBucket(sqlite3 *dbh,
                                               const std::string &table) const;
};

/**
 * Contains the persistence statements used by various SqliteStrategy
 * implementations.
 */
class Statements {
public:
    Statements(sqlite3 *dbh, std::string tab, StatementFactory *sFact) {
        db = dbh;
        tableName = tab;
        initStatements(sFact);
    }

    ~Statements() {
        delete ins_stmt;
        delete upd_stmt;
        delete sel_stmt;
        delete del_stmt;
        delete del_vb_stmt;
        delete all_stmt;
        delete count_all_stmt;
        ins_stmt = upd_stmt = sel_stmt = del_stmt = del_vb_stmt = all_stmt =
            count_all_stmt = NULL;
    }

    PreparedStatement *ins() {
        return ins_stmt;
    }

    PreparedStatement *upd() {
        return upd_stmt;
    }

    PreparedStatement *sel() {
        return sel_stmt;
    }

    PreparedStatement *del() {
        return del_stmt;
    }

    PreparedStatement *del_vb() {
        return del_vb_stmt;
    }

    PreparedStatement *all() {
        return all_stmt;
    }

    PreparedStatement *count_all() {
        return count_all_stmt;
    }

private:

    void initStatements(const StatementFactory *sfact);

    sqlite3           *db;
    std::string        tableName;
    PreparedStatement *ins_stmt;
    PreparedStatement *upd_stmt;
    PreparedStatement *sel_stmt;
    PreparedStatement *del_stmt;
    PreparedStatement *del_vb_stmt;
    PreparedStatement *all_stmt;
    PreparedStatement *count_all_stmt;

    DISALLOW_COPY_AND_ASSIGN(Statements);
};

#endif /* SQLITE_PST_H */
