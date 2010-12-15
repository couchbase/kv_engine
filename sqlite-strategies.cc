/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <cassert>
#include <iostream>
#include <stdexcept>

#include "sqlite-strategies.hh"
#include "sqlite-eval.hh"
#include "ep.hh"
#include "pathexpand.hh"

static const int CURRENT_SCHEMA_VERSION(2);

SqliteStrategy::SqliteStrategy(const char * const fn,
                               const char * const finit,
                               const char * const pfinit,
                               size_t shards) : db(NULL),
    filename(fn),
    initFile(finit),
    postInitFile(pfinit),
    shardCount(shards),
    ins_vb_stmt(NULL), clear_vb_stmt(NULL), sel_vb_stmt(NULL),
    clear_stats_stmt(NULL), ins_stat_stmt(NULL),
    schema_version(0) {

    assert(filename);
    assert(shardCount > 0);
}

SqliteStrategy::~SqliteStrategy() {
    destroyMetaStatements();
    close();
}

void SqliteStrategy::destroyMetaStatements() {
    delete ins_vb_stmt;
    delete clear_vb_stmt;
    delete sel_vb_stmt;
    delete clear_stats_stmt;
    delete ins_stat_stmt;
}

void SqliteStrategy::initMetaTables() {
    assert(db);
    PreparedStatement st(db, "select name from sqlite_master where name='vbucket_states'");
    if (schema_version == 0 && !st.fetch()) {
        std::stringstream ss;
        ss << "PRAGMA user_version=" << CURRENT_SCHEMA_VERSION;
        execute(ss.str().c_str());
        schema_version = CURRENT_SCHEMA_VERSION;
    }

    execute("create table if not exists vbucket_states"
            " (vbid integer primary key on conflict replace,"
            "  vb_version interger,"
            "  state varchar(16),"
            "  last_change datetime)");

    execute("create table if not exists stats_snap"
            " (name varchar(16),"
            "  value varchar(24),"
            "  last_change datetime)");
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

sqlite3 *SqliteStrategy::open(void) {
    if(!db) {
        int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
            | SQLITE_OPEN_PRIVATECACHE;

        if(sqlite3_open_v2(filename, &db, flags, NULL) !=  SQLITE_OK) {
            throw std::runtime_error("Error initializing sqlite3");
        }

        if(sqlite3_extended_result_codes(db, 1) != SQLITE_OK) {
            throw std::runtime_error("Error enabling extended RCs");
        }

        initDB();

        PreparedStatement uv_get(db, "PRAGMA user_version");
        uv_get.fetch();
        schema_version = uv_get.column_int(0);

        initMetaTables();
        initTables();
        initMetaStatements();
        initStatements();
        doFile(postInitFile);
        if (schema_version < CURRENT_SCHEMA_VERSION) {
            std::stringstream ss;
            ss << "Schema version " << schema_version << " is not supported anymore.\n"
               << "Run the script to upgrade the schema to version "
               << CURRENT_SCHEMA_VERSION;
            close();
            throw std::runtime_error(ss.str().c_str());
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

// ----------------------------------------------------------------------

void SingleTableSqliteStrategy::destroyStatements() {
    while (!statements.empty()) {
        Statements *st = statements.back();
        delete st;
        statements.pop_back();
    }
}

void SingleTableSqliteStrategy::initTables(void) {
    assert(db);
    execute("create table if not exists kv"
            " (vbucket integer,"
            "  vb_version integer,"
            "  k varchar(250), "
            "  flags integer,"
            "  exptime integer,"
            "  cas integer,"
            "  v text)");
}

void SingleTableSqliteStrategy::initStatements(void) {
    assert(db);
    Statements *st = new Statements(db, "kv");
    statements.push_back(st);
}

void SingleTableSqliteStrategy::destroyTables(void) {
    execute("drop table if exists kv");
}

//
// ----------------------------------------------------------------------
// Multi DB strategy
// ----------------------------------------------------------------------
//

void MultiDBSingleTableSqliteStrategy::initDB() {
    char buf[1024];
    PathExpander p(filename);

    for (int i = 0; i < numTables; i++) {
        std::string shardname(p.expand(shardpattern, i));
        snprintf(buf, sizeof(buf), "attach database \"%s\" as kv_%d",
                 shardname.c_str(), i);
        execute(buf);
    }
    doFile(initFile);
}

void MultiDBSingleTableSqliteStrategy::initTables() {
    char buf[1024];
    PathExpander p(filename);

    for (int i = 0; i < numTables; i++) {
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

void MultiDBSingleTableSqliteStrategy::initStatements() {
    char buf[64];
    for (int i = 0; i < numTables; i++) {
        snprintf(buf, sizeof(buf), "kv_%d.kv", i);
        statements.push_back(new Statements(db, std::string(buf)));
    }
}

void MultiDBSingleTableSqliteStrategy::destroyTables() {
    char buf[1024];
    for (int i = 0; i < numTables; i++) {
        snprintf(buf, sizeof(buf), "drop table if exists kv_%d.kv", i);
        execute(buf);
    }
}
