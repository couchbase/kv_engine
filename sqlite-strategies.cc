/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include <cassert>
#include <iostream>
#include <stdexcept>

#include "sqlite-strategies.hh"

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
            " (k varchar(250) primary key on conflict replace,"
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
    execute("pragma page_size = 8192");
    execute("pragma cache_size = 8192");
    execute("pragma journal_mode = TRUNCATE");
    execute("pragma locking_mode = EXCLUSIVE");
    execute("pragma synchronous = NORMAL");
}

void SqliteStrategy::execute(const char * const query) {
    PreparedStatement st(db, query);
    st.execute();
}
