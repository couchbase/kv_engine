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
        initTables();
        initStatements();
        doFile(postInitFile);
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
    delete set_vb_stmt;
    delete del_vb_stmt;
    delete sel_vb_stmt;
}

void SqliteStrategy::initTables(void) {
    assert(db);
    execute("create table if not exists kv"
            " (k varchar(250), "
            "  v text,"
            "  flags integer,"
            "  exptime integer,"
            "  cas integer,"
            "  vbucket integer)");
}

void SqliteStrategy::initMetaStatements(void) {
    const char *ins_query = "insert into vbucket_states"
        " (vbid, state, last_change) values (?, ?, current_timestamp)";
    set_vb_stmt = new PreparedStatement(db, ins_query);

    const char *del_query = "delete from vbucket_states where vbid = ?";
    del_vb_stmt = new PreparedStatement(db, del_query);

    const char *sel_query = "select vbid, state from vbucket_states";
    sel_vb_stmt = new PreparedStatement(db, sel_query);
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
    execute("create table if not exists vbucket_states"
            " (vbid integer primary key on conflict replace,"
            "  state varchar(16),"
            "  last_change datetime)");

    for (int i = 0; i < numTables; i++) {
        snprintf(buf, sizeof(buf), "attach database \"%s-%d.sqlite\" as kv_%d",
                 filename, i, i);
        execute(buf);
        snprintf(buf, sizeof(buf),
                 "create table if not exists kv_%d.kv"
                 " (k varchar(250),"
                 "  v text,"
                 "  flags integer,"
                 "  exptime integer,"
                 "  cas integer,"
                 "  vbucket integer)", i);
        execute(buf);
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
