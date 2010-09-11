/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <string.h>
#include <sstream>
#include <cstdlib>
#include <stdexcept>
#include <iostream>
#include <sstream>
#include <assert.h>

#include "sqlite-pst.hh"

#define MAX_STEPS 10000

PreparedStatement::PreparedStatement(sqlite3 *d, const char *query) {
    assert(d);
    assert(query);
    db = d;
    if(sqlite3_prepare_v2(db, query, (int)strlen(query), &st, NULL)
       != SQLITE_OK) {
        throw std::runtime_error(sqlite3_errmsg(db));
    }
}

PreparedStatement::~PreparedStatement() {
    sqlite3_finalize(st);
}

void PreparedStatement::bind(int pos, const char *s) {
    bind(pos, s, strlen(s));
}

void PreparedStatement::bind(int pos, const char *s, size_t nbytes) {
    sqlite3_bind_blob(st, pos, s, (int)nbytes, SQLITE_STATIC);
}

void PreparedStatement::bind(int pos, int v) {
    sqlite3_bind_int(st, pos, v);
}

void PreparedStatement::bind64(int pos, uint64_t v) {
    sqlite3_bind_int64(st, pos, v);
}

int PreparedStatement::execute() {
    int steps_run = 0, rc = 0;
    while ((rc = sqlite3_step(st)) != SQLITE_DONE) {
        if (++steps_run > MAX_STEPS) {
            return -1;
        }
        if (rc == SQLITE_ROW) {
            // This is rather normal
        } else if (rc == SQLITE_BUSY) {
            std::cerr << "SQLITE_BUSY (retrying)" << std::endl;
        } else {
            const char *msg = sqlite3_errmsg(db);
            std::cerr << "sqlite error:  " << msg << std::endl;
            return -1;
        }
    }
    return sqlite3_changes(db);
}

bool PreparedStatement::fetch() {
    bool rv = true;
    assert(st);
    int rc = sqlite3_step(st);
    switch(rc) {
    case SQLITE_ROW:
        break;
    case SQLITE_DONE:
        rv = false;
        break;
    default:
        std::stringstream ss;
        ss << "Unhandled case in sqlite-pst:  " << rc;
        const char *msg = sqlite3_errmsg(db);
        if (msg) {
            ss << " (" << msg << ")";
        }
        throw std::runtime_error(ss.str());
    }
    return rv;
}

const char *PreparedStatement::column(int x) {
    return (char*)sqlite3_column_text(st, x);
}

const void *PreparedStatement::column_blob(int x) {
    return (char*)sqlite3_column_text(st, x);
}

int PreparedStatement::column_bytes(int x) {
    return sqlite3_column_bytes(st, x);
}

int PreparedStatement::column_int(int x) {
    return sqlite3_column_int(st, x);
}

uint64_t PreparedStatement::column_int64(int x) {
    return sqlite3_column_int64(st, x);
}

void PreparedStatement::reset() {
    // The result of this is ignored as it indicates the last error
    // returned from step calls, not whether reset works.
    // http://www.sqlite.org/c3ref/reset.html
    sqlite3_reset(st);
}

void Statements::initStatements() {
    char buf[1024];
    snprintf(buf, sizeof(buf),
             "insert into %s (k, v, flags, exptime, cas, vbucket) "
             "values(?, ?, ?, ?, ?, ?)", tableName.c_str());
    ins_stmt = new PreparedStatement(db, buf);

    // Note that vbucket IDs don't change here.
    snprintf(buf, sizeof(buf),
             "update %s set k=?, v=?, flags=?, exptime=?, cas=? "
             " where rowid = ?", tableName.c_str());
    upd_stmt = new PreparedStatement(db, buf);

    // v=0, flags=1, exptime=2, cas=3, rowid=4, vbucket=5
    snprintf(buf, sizeof(buf),
             "select v, flags, exptime, cas, rowid, vbucket "
             "from %s where rowid = ?", tableName.c_str());
    sel_stmt = new PreparedStatement(db, buf);

    // k=0, v=1, flags=2, exptime=3, cas=4, vbucket=5, rowid=6
    snprintf(buf, sizeof(buf),
             "select k, v, flags, exptime, cas, vbucket, rowid "
             "from %s", tableName.c_str());
    all_stmt = new PreparedStatement(db, buf);

    snprintf(buf, sizeof(buf),
             "delete from %s where k = ? and vbucket = ?",
             tableName.c_str());
    del_stmt = new PreparedStatement(db, buf);

    snprintf(buf, sizeof(buf),
             "delete from %s where vbucket = ?", tableName.c_str());
    del_vb_stmt = new PreparedStatement(db, buf);
}
