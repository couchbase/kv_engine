/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

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

        initPragmas();
        initTables();
        initStatements();
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
}

void SqliteStrategy::initTables(void) {
    assert(db);
    execute("create table if not exists kv"
            " (k varchar(250), "
            "  v text,"
            "  flags integer,"
            "  exptime integer,"
            "  cas integer)");
}

void SqliteStrategy::initStatements(void) {
    assert(db);
    Statements *st = new Statements(db, "kv");
    statements.push_back(st);
}

void SqliteStrategy::destroyTables(void) {
    execute("drop table if exists kv");
}

void SqliteStrategy::initPragmas(void) {
    if (initFile) {
        SqliteEvaluator eval(db);
        getLogger()->log(EXTENSION_LOG_INFO, NULL,
                         "Initializing DB session from %s\n", initFile);
        eval.eval(initFile);
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
        snprintf(buf, sizeof(buf),
                 "create table if not exists kv_%d.kv"
                 " (k varchar(250),"
                 "  v text,"
                 "  flags integer,"
                 "  exptime integer,"
                 "  cas integer)", i);
        execute(buf);
    }
}

void MultiDBSqliteStrategy::initStatements() {
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
