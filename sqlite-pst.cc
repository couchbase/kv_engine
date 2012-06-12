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
        std::stringstream ss;
        ss << sqlite3_errmsg(db) << " while building query: ``" << query << "''";
        throw std::runtime_error(ss.str());
    }
}

PreparedStatement::~PreparedStatement() {
    sqlite3_finalize(st);
}

int PreparedStatement::bind(int pos, const char *s) {
    return bind(pos, s, strlen(s));
}

int PreparedStatement::bind(int pos, const char *s, size_t nbytes) {
    sqlite3_bind_blob(st, pos, s, (int)nbytes, SQLITE_STATIC);
    return 1;
}

int PreparedStatement::bind(int pos, const std::string &s) {
    sqlite3_bind_blob(st, pos, s.data(), s.size(), SQLITE_STATIC);
    return 1;
}

int PreparedStatement::bind(int pos, int v) {
    sqlite3_bind_int(st, pos, v);
    return 1;
}

int PreparedStatement::bind(int pos, std::pair<int, int> pv) {
    bind(pos, pv.first);
    bind(++pos, pv.second);
    return 2;
}

int PreparedStatement::bind(int pos, vbucket_state vb_state) {
    bind(pos, vb_state.state);
    bind64(++pos, vb_state.checkpointId);
    return 2;
}

int PreparedStatement::bind64(int pos, uint64_t v) {
    sqlite3_bind_int64(st, pos, v);
    return 1;
}

size_t PreparedStatement::paramCount() {
    return static_cast<size_t>(sqlite3_bind_parameter_count(st));
}

int PreparedStatement::execute() {
    int steps_run = 0, rc = 0, busy = 0;
    while ((rc = sqlite3_step(st)) != SQLITE_DONE) {
        if (++steps_run > MAX_STEPS) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Too many db steps, erroring (busy=%d)\n", busy);
            return -1;
        }
        if (rc == SQLITE_ROW) {
            // This is rather normal
        } else if (rc == SQLITE_BUSY) {
            ++busy;
        } else {
            const char *msg = sqlite3_errmsg(db);
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "sqlite error:  %s\n", msg);
            return -1;
        }
    }
    return sqlite3_changes(db);
}

bool PreparedStatement::fetch() {
    bool rv = true;
    int steps_run = 0;
    assert(st);
    while (true) {
        int rc = sqlite3_step(st);
        if (rc == SQLITE_ROW) {
            break;
        } else if (rc == SQLITE_DONE) {
            rv = false;
            break;
        } else if (rc == SQLITE_SCHEMA) {
            if (++steps_run > MAX_STEPS) {
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "Too many db steps!!! Terminate fetch() by force...\n");
                rv = false;
                break;
            }
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "There were schema changes in sqlite while fetching a record!!! "
                             "Fetching it again...\n");
        } else {
            std::stringstream ss;
            ss << "Unhandled case in sqlite-pst:  " << rc;
            const char *msg = sqlite3_errmsg(db);
            if (msg) {
                ss << " (" << msg << ")";
            }
            throw std::runtime_error(ss.str());
        }
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

void Statements::initStatements(const StatementFactory *sfact) {
    assert(sfact);
    ins_stmt = sfact->mkInsert(db, tableName);
    assert(ins_stmt);
    upd_stmt = sfact->mkUpdate(db, tableName);
    assert(upd_stmt);
    set_meta_stmt = sfact->mkSetMeta(db, tableName);
    assert(set_meta_stmt);
    sel_stmt = sfact->mkSelect(db, tableName);
    assert(sel_stmt);
    all_stmt = sfact->mkSelectAll(db, tableName);
    assert(all_stmt);
    del_stmt = sfact->mkDelete(db, tableName);
    assert(del_stmt);
    del_vb_stmt = sfact->mkDeleteVBucket(db, tableName);
    assert(del_vb_stmt);
}

PreparedStatement *StatementFactory::mkInsert(sqlite3 *db,
                                              const std::string &table) const {
    char buf[1024];
    snprintf(buf, sizeof(buf),
             "insert into %s (k, v, flags, exptime, cas, vbucket, vb_version) "
             "values(?, ?, ?, ?, ?, ?, ?)", table.c_str());
    return new PreparedStatement(db, buf);
}

PreparedStatement *StatementFactory::mkUpdate(sqlite3 *db,
                                              const std::string &table) const {
    char buf[1024];
    // Note that vbucket IDs don't change here.
    snprintf(buf, sizeof(buf),
             "update %s set k=?, v=?, flags=?, exptime=?, cas=?, vb_version=? "
             " where rowid = ?", table.c_str());
    return new PreparedStatement(db, buf);
}

PreparedStatement *StatementFactory::mkSetMeta(sqlite3 *db,
                                               const std::string &table)
                                               const {
    char buf[1024];
    // Note that vbucket IDs don't change here.
    snprintf(buf, sizeof(buf),
             "update %s set k=?, flags=?, exptime=?, cas=?, vb_version=? "
             " where rowid = ?", table.c_str());
    return new PreparedStatement(db, buf);
}

PreparedStatement *StatementFactory::mkSelect(sqlite3 *db,
                                              const std::string &table) const {
    char buf[1024];
    // v=0, flags=1, exptime=2, cas=3, rowid=4, vbucket=5
    snprintf(buf, sizeof(buf),
             "select v, flags, exptime, cas, rowid, vbucket "
             "from %s where rowid = ?", table.c_str());
    return new PreparedStatement(db, buf);
}

PreparedStatement *StatementFactory::mkSelectAll(sqlite3 *db,
                                                 const std::string &table) const {
    char buf[1024];
    // k=0, v=1, flags=2, exptime=3, cas=4, vbucket=5, rowid=6
    snprintf(buf, sizeof(buf),
             "select k, v, flags, exptime, cas, vbucket, vb_version, rowid "
             "from %s", table.c_str());
    return new PreparedStatement(db, buf);
}

PreparedStatement *StatementFactory::mkDelete(sqlite3 *db,
                                              const std::string &table) const {
    char buf[1024];
    snprintf(buf, sizeof(buf), "delete from %s where rowid = ?",
             table.c_str());
    return new PreparedStatement(db, buf);
}

PreparedStatement *StatementFactory::mkDeleteVBucket(sqlite3 *db,
                                                     const std::string &table) const {
    char buf[1024];
    snprintf(buf, sizeof(buf),
             "delete from %s where vbucket = ? and vb_version <= ? and "
             "rowid between ? and ?", table.c_str());
    return new PreparedStatement(db, buf);
}
