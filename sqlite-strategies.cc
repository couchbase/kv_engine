/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <cassert>
#include <iostream>
#include <stdexcept>

#include "sqlite-strategies.hh"
#include "sqlite-eval.hh"
#include "ep.hh"

sqlite3 *SqliteStrategy::open(void) {
    if(!db) {
        if(sqlite3_open(filename, &db) !=  SQLITE_OK) {
            throw std::runtime_error("Error initializing sqlite3");
        }

        if(sqlite3_extended_result_codes(db, 1) != SQLITE_OK) {
            throw std::runtime_error("Error enabling extended RCs");
        }

        doFile(initFile);

        PreparedStatement uv_get(db, "PRAGMA user_version");
        uv_get.fetch();
        schema_version = uv_get.column_int(0);

        initMetaTables();
        initTables();
        initStatements();
        doFile(postInitFile);
        if (schema_version == 0) {
            execute("PRAGMA user_version=1");
            schema_version = 1;
        }
    }
    return db;
}

void SqliteStrategy::close(void) {
    if(db) {
        destroyStatements();
        sqlite3_close(db);
        db = NULL;
    }
}

void SqliteStrategy::destroyStatements() {
    while (!statements.empty()) {
        Statements *st = statements.back();
        delete st;
        statements.pop_back();
    }
    destroyMetaStatements();
}

void SqliteStrategy::destroyMetaStatements(void) {
    delete ins_vb_stmt;
    delete clear_vb_stmt;
    delete sel_vb_stmt;
    delete clear_stats_stmt;
    delete ins_stat_stmt;
}

void SqliteStrategy::initMetaTables() {
    assert(db);
    PreparedStatement st(db, "select name from sqlite_master where name='vbucket_states'");
    if (schema_version == 0 && st.fetch()) {
        execute("alter table vbucket_states add column"
                " vb_version integer default 0");
    } else {
        execute("create table if not exists vbucket_states"
                " (vbid integer primary key on conflict replace,"
                "  vb_version interger,"
                "  state varchar(16),"
                "  last_change datetime)");
    }

    execute("create table if not exists stats_snap"
            " (name varchar(16),"
            "  value varchar(24),"
            "  last_change datetime)");
}

void SqliteStrategy::initTables(void) {
    assert(db);
    PreparedStatement st(db, "select name from sqlite_master where name='kv'");
    if (schema_version == 0 && st.fetch()) {
        execute("alter table kv add column"
                " vb_version integer default 0");
    } else {
        execute("create table if not exists kv"
                " (vbucket integer,"
                "  vb_version integer,"
                "  k varchar(250), "
                "  flags integer,"
                "  exptime integer,"
                "  cas integer,"
                "  v text)");
    }
}

void SqliteStrategy::initMetaStatements(void) {
    const char *ins_query = "insert into vbucket_states"
        " (vbid, vb_version, state, last_change) values (?, ?, ?, current_timestamp)";
    ins_vb_stmt = new PreparedStatement(db, ins_query);

    const char *del_query = "delete from vbucket_states";
    clear_vb_stmt = new PreparedStatement(db, del_query);

    const char *sel_query = "select vbid, vb_version, state from vbucket_states";
    sel_vb_stmt = new PreparedStatement(db, sel_query);

    const char *clear_stats_query = "delete from stats_snap";
    clear_stats_stmt = new PreparedStatement(db, clear_stats_query);

    const char *ins_stat_query = "insert into stats_snap "
        "(name, value, last_change) values (?, ?, current_timestamp)";
    ins_stat_stmt = new PreparedStatement(db, ins_stat_query);
}

void SqliteStrategy::initStatements(void) {
    assert(db);
    initMetaStatements();
    Statements *st = new Statements(db, "kv");
    statements.push_back(st);
}

void SqliteStrategy::destroyTables(void) {
    execute("drop table if exists kv");
}

void SqliteStrategy::doFile(const char * const fn) {
    if (fn) {
        SqliteEvaluator eval(db);
        getLogger()->log(EXTENSION_LOG_INFO, NULL,
                         "Running db script: %s\n", fn);
        eval.eval(fn);
    }
}

void SqliteStrategy::execute(const char * const query) {
    PreparedStatement st(db, query);
    st.execute();
}

//
// ----------------------------------------------------------------------
// Multi DB strategy
// ----------------------------------------------------------------------
//

void MultiDBSqliteStrategy::initTables() {
    char buf[1024];

    for (int i = 0; i < numTables; i++) {
        snprintf(buf, sizeof(buf), "attach database \"%s-%d.sqlite\" as kv_%d",
                 filename, i, i);
        execute(buf);
        snprintf(buf, sizeof(buf), "select name from kv_%d.sqlite_master where name='kv'", i);
        PreparedStatement st(db, buf);
        if (schema_version == 0 && st.fetch()) {
            snprintf(buf, sizeof(buf),
                     "alter table kv_%d.kv add column"
                     " vb_version integer default 0", i);
            execute(buf);
        } else {
            snprintf(buf, sizeof(buf),
                     "create table if not exists kv_%d.kv"
                     " (vbucket integer,"
                     "  vb_version integer,"
                     "  k varchar(250),"
                     "  flags integer,"
                     "  exptime integer,"
                     "  cas integer,"
                     "  v text)", i);
            execute(buf);
        }
    }
}

void MultiDBSqliteStrategy::initStatements() {
    initMetaStatements();
    char buf[64];
    for (int i = 0; i < numTables; i++) {
        snprintf(buf, sizeof(buf), "kv_%d.kv", i);
        statements.push_back(new Statements(db, std::string(buf)));
    }
}

void MultiDBSqliteStrategy::destroyTables() {
    char buf[1024];
    for (int i = 0; i < numTables; i++) {
        snprintf(buf, sizeof(buf), "drop table if exists kv_%d.kv", i);
        execute(buf);
    }
}
