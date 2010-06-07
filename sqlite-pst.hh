/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef SQLITE_PST_H
#define SQLITE_PST_H 1

#include <string>
#include <stdio.h>
#include <inttypes.h>
#include "embedded/sqlite3.h"

#include "common.hh"

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
     * Bind a uint32 value.
     *
     * @param pos the binding position (starting at 1)
     * @param d the value to bind
     */
    void bind(int pos, int d);

    /**
     * Bind a uint64 value.
     *
     * @param pos the binding position (starting at 1)
     * @param d the value to bind
     */
    void bind64(int pos, uint64_t d);

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

// Container of multiple statements.

class Statements {
public:
    Statements(sqlite3 *dbh, std::string tab) {
        db = dbh;
        tableName = tab;
        initStatements();
    }

    ~Statements() {
        delete ins_stmt;
        delete upd_stmt;
        delete sel_stmt;
        delete del_stmt;
        delete all_stmt;
        ins_stmt = sel_stmt = del_stmt = NULL;
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

    PreparedStatement *all() {
        return all_stmt;
    }
private:

    void initStatements() {
        char buf[1024];
        snprintf(buf, sizeof(buf),
                 "insert into %s (k, v, flags, exptime, cas) "
                 "values(?, ?, ?, ?, ?)", tableName.c_str());
        ins_stmt = new PreparedStatement(db, buf);
        snprintf(buf, sizeof(buf),
                 "update %s set k=?, v=?, flags=?, exptime=?, cas=? "
                 " where rowid = ?", tableName.c_str());
        upd_stmt = new PreparedStatement(db, buf);
        snprintf(buf, sizeof(buf),
                 "select v, flags, exptime, cas "
                 "from %s where rowid = ?", tableName.c_str());
        sel_stmt = new PreparedStatement(db, buf);
        snprintf(buf, sizeof(buf),
                 "select k, v, flags, exptime, cas, rowid "
                 "from %s", tableName.c_str());
        all_stmt = new PreparedStatement(db, buf);
        snprintf(buf, sizeof(buf), "delete from %s where k = ?",
                 tableName.c_str());
        del_stmt = new PreparedStatement(db, buf);
    }

    sqlite3           *db;
    std::string        tableName;
    PreparedStatement *ins_stmt;
    PreparedStatement *upd_stmt;
    PreparedStatement *sel_stmt;
    PreparedStatement *del_stmt;
    PreparedStatement *all_stmt;

    DISALLOW_COPY_AND_ASSIGN(Statements);
};

#endif /* SQLITE_PST_H */
