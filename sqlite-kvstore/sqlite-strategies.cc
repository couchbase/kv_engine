/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <cassert>
#include <iostream>
#include <stdexcept>

#include "sqlite-strategies.hh"
#include "sqlite-eval.hh"
#include "ep.hh"
#include "pathexpand.hh"

#include "sqlite-vfs.h"

static const int CURRENT_SCHEMA_VERSION(2);

bool SqliteStrategy::shouldCheckSchemaVersion = true;

extern "C" {

static void traceGotSectorSize(int s, void *arg) {
    static_cast<SQLiteStats*>(arg)->sectorSize = static_cast<size_t>(s);
}

static void traceOpen(int, void *arg) {
    ++static_cast<SQLiteStats*>(arg)->numOpen;
}

static void traceClose(int, void *arg) {
    ++static_cast<SQLiteStats*>(arg)->numClose;
}

static void traceLock(int, void *arg) {
    ++static_cast<SQLiteStats*>(arg)->numLocks;
}

static void traceTrunc(int, void *arg) {
    ++static_cast<SQLiteStats*>(arg)->numTruncates;
}

static void traceDelete(int, hrtime_t elapsed, void *arg) {
    static_cast<SQLiteStats*>(arg)->deleteHisto.add(elapsed / 1000);
}

static void traceSync(int, int, hrtime_t elapsed, void *arg) {
    static_cast<SQLiteStats*>(arg)->syncTimeHisto.add(elapsed / 1000);
}

static void traceRead(int, size_t rs, ssize_t dist, hrtime_t elapsed, void *arg) {
    static_cast<SQLiteStats*>(arg)->readTimeHisto.add(elapsed / 1000);
    static_cast<SQLiteStats*>(arg)->readSizeHisto.add(rs);
    static_cast<SQLiteStats*>(arg)->readSeekHisto.add(abs(dist));
}

static void traceWrite(int, size_t rs, ssize_t dist, hrtime_t elapsed, void *arg) {
    static_cast<SQLiteStats*>(arg)->writeTimeHisto.add(elapsed / 1000);
    static_cast<SQLiteStats*>(arg)->writeSizeHisto.add(rs);
    static_cast<SQLiteStats*>(arg)->writeSeekHisto.add(abs(dist));
}

}

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

    if (sqlite3_vfs_find(filename) == NULL) {

        sqlite3_vfs *default_vfs(sqlite3_vfs_find(NULL));
        assert(default_vfs);

        static struct vfs_callbacks sqlite_vfs_callbacks = {
            traceGotSectorSize,
            traceOpen,
            traceClose,
            traceLock,
            traceTrunc,
            traceDelete,
            traceSync,
            traceRead,
            traceWrite
        };

        vfsepstat_register(filename, default_vfs->zName,
                           sqlite_vfs_callbacks, &sqliteStats);

        assert(sqlite3_vfs_find(filename) != NULL);
    }
}

SqliteStrategy::~SqliteStrategy() {
    close();
    sqlite3_vfs *toDestroy(sqlite3_vfs_find(filename));
    if (toDestroy) {
        sqlite3_vfs_unregister(toDestroy);
    }
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
    execute("create table if not exists vbucket_states"
            " (vbid integer primary key on conflict replace,"
            "  vb_version integer,"
            "  state varchar(16),"
            "  checkpoint_id integer,"
            "  last_change datetime)");

    execute("create table if not exists stats_snap"
            " (name varchar(16),"
            "  value varchar(24),"
            "  last_change datetime)");
}

void SqliteStrategy::initMetaStatements(void) {
    const char *ins_query = "insert into vbucket_states"
        " (vbid, vb_version, state, checkpoint_id, last_change)"
        " values (?, ?, ?, ?, current_timestamp)";
    ins_vb_stmt = new PreparedStatement(db, ins_query);

    const char *del_query = "delete from vbucket_states";
    clear_vb_stmt = new PreparedStatement(db, del_query);

    const char *sel_query = "select vbid, vb_version, state, checkpoint_id from vbucket_states";
    sel_vb_stmt = new PreparedStatement(db, sel_query);

    const char *clear_stats_query = "delete from stats_snap";
    clear_stats_stmt = new PreparedStatement(db, clear_stats_query);

    const char *ins_stat_query = "insert into stats_snap "
        "(name, value, last_change) values (?, ?, current_timestamp)";
    ins_stat_stmt = new PreparedStatement(db, ins_stat_query);
}

void SqliteStrategy::checkSchemaVersion(void) {
    assert(db);
    PreparedStatement uv_get(db, "PRAGMA user_version");
    uv_get.fetch();
    schema_version = uv_get.column_int(0);

    PreparedStatement st(db, "select name from sqlite_master where name='vbucket_states'");
    if (schema_version == 0 && !st.fetch()) {
        std::stringstream ss;
        ss << "PRAGMA user_version=" << CURRENT_SCHEMA_VERSION;
        execute(ss.str().c_str());
        schema_version = CURRENT_SCHEMA_VERSION;
    }

    if (shouldCheckSchemaVersion && (schema_version < CURRENT_SCHEMA_VERSION)) {
        std::stringstream ss;
        ss << "Schema version " << schema_version << " is not supported anymore.\n"
           << "Run the script to upgrade the schema to version "
           << CURRENT_SCHEMA_VERSION;
        close();
        throw std::runtime_error(ss.str().c_str());
    }
}

sqlite3 *SqliteStrategy::open(void) {
    if(!db) {
        int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
            | SQLITE_OPEN_PRIVATECACHE;

        if(sqlite3_open_v2(filename, &db, flags, filename) !=  SQLITE_OK) {
            std::stringstream ss;
            ss << "Error initializing sqlite3: " << sqlite3_errcode(db) << std::endl;
            throw std::runtime_error(ss.str());
        }

        if(sqlite3_extended_result_codes(db, 1) != SQLITE_OK) {
            throw std::runtime_error("Error enabling extended RCs");
        }

        initDB();
        checkSchemaVersion();

        execute("begin immediate");
        initMetaTables();
        initTables();
        execute("commit");
        initMetaStatements();
        initStatements();
        doFile(postInitFile);
    }
    return db;
}

void SqliteStrategy::close(void) {
    if(db) {
        destroyMetaStatements();
        destroyStatements();
        if (sqlite3_close(db) != SQLITE_OK) {
            std::stringstream ss;
            ss << "Error closing sqlite3: " << sqlite3_errcode(db) << std::endl;
            throw std::runtime_error(ss.str());
        }
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
    StatementFactory sfact;
    Statements *st = new Statements(db, "kv", &sfact);
    statements.push_back(st);
}

std::string SingleTableSqliteStrategy::getKvTableName(const std::string &,
                                                      uint16_t)
{
    return std::string("kv");
}

void SingleTableSqliteStrategy::destroyTables(void) {
    execute("drop table if exists kv");
}

void SingleTableSqliteStrategy::destroyInvalidTables(bool destroyOnlyOne) {
    assert(db);
    char buf[1024];
    PreparedStatement st(db, "select name from sqlite_master where name like 'invalid_kv_%'");
    while (st.fetch()) {
        snprintf(buf, sizeof(buf), "drop table if exists %s",
                 st.column(0));
        execute(buf);
        getLogger()->log(EXTENSION_LOG_INFO, NULL,
                         "Removed the temp table %s\n", st.column(0));
        if (destroyOnlyOne) {
            break;
        }
    }
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
    StatementFactory sfact;
    for (int i = 0; i < numTables; i++) {
        snprintf(buf, sizeof(buf), "kv_%d.kv", i);
        statements.push_back(new Statements(db, std::string(buf), &sfact));
    }
}

std::string MultiDBSingleTableSqliteStrategy::getKvTableName(const std::string &key,
                                                             uint16_t)
{
    char buf[64];
    snprintf(buf, sizeof(buf), "kv_%d.kv", static_cast<int>(getDbShardIdForKey(key)));
    return std::string(buf);
}

void MultiDBSingleTableSqliteStrategy::destroyTables() {
    char buf[1024];
    for (int i = 0; i < numTables; i++) {
        snprintf(buf, sizeof(buf), "drop table if exists kv_%d.kv", i);
        execute(buf);
    }
}

void MultiDBSingleTableSqliteStrategy::destroyInvalidTables(bool destroyOnlyOne) {
    assert(db);
    char buf[1024];
    for (int i = 0; i < numTables; ++i) {
        snprintf(buf, sizeof(buf),
                 "select name from kv_%d.sqlite_master where name like 'invalid_kv_%%'", i);
        PreparedStatement st(db, buf);
        while (st.fetch()) {
            snprintf(buf, sizeof(buf), "drop table if exists kv_%d.%s",
                     i, st.column(0));
            execute(buf);
            getLogger()->log(EXTENSION_LOG_INFO, NULL,
                             "Removed the temp table kv_%d.%s\n", i, st.column(0));
            if (destroyOnlyOne) {
                return;
            }
        }
    }
}

//
// ----------------------------------------------------------------------
// Table per vbucket
// ----------------------------------------------------------------------
//


/**
 * StatementFactory for table-per-vbucket strategies.
 */
class FastVBDelStatementFactory : public StatementFactory {
public:

    PreparedStatement *mkDeleteVBucket(sqlite3 *db,
                                       const std::string &table) const {
        char buf[1024];
        snprintf(buf, sizeof(buf), "delete from %s", table.c_str());
        return new PreparedStatement(db, buf);
    }
};

void MultiTableSqliteStrategy::initTables() {
    char buf[1024];

    for (size_t i = 0; i < nvbuckets; ++i) {
        snprintf(buf, sizeof(buf),
                 "create table if not exists kv_%d"
                 " (vbucket integer,"
                 "  vb_version integer,"
                 "  k varchar(250),"
                 "  flags integer,"
                 "  exptime integer,"
                 "  cas integer,"
                 "  v text)", static_cast<int>(i));
        execute(buf);
    }
}

void MultiTableSqliteStrategy::initStatements() {
    char buf[64];
    FastVBDelStatementFactory sfact;
    for (size_t i = 0; i < nvbuckets; ++i) {
        snprintf(buf, sizeof(buf), "kv_%d", static_cast<int>(i));
        statements.push_back(new Statements(db, std::string(buf), &sfact));
    }
}

std::string MultiTableSqliteStrategy::getKvTableName(const std::string &,
                                                     uint16_t vbid)
{
    char buf[64];
    snprintf(buf, sizeof(buf), "kv_%d",
             static_cast<int>(vbid));
    return std::string(buf);
}

void MultiTableSqliteStrategy::destroyTables() {
    char buf[1024];
    for (size_t i = 0; i < nvbuckets; ++i) {
        snprintf(buf, sizeof(buf), "drop table if exists kv_%d",
                 static_cast<int>(i));
        execute(buf);
    }
}

void MultiTableSqliteStrategy::destroyInvalidTables(bool destroyOnlyOne) {
    assert(db);
    char buf[1024];
    std::vector<std::string> tables;

    PreparedStatement st(db, "select name from sqlite_master where name like 'invalid_kv_%'");
    while (st.fetch()) {
        tables.push_back(std::string(st.column(0)));
        if (destroyOnlyOne) {
            break;
        }
    }
    st.reset();
    std::vector<std::string>::iterator it = tables.begin();
    for (; it != tables.end(); ++it) {
        snprintf(buf, sizeof(buf), "drop table if exists %s",
                 it->c_str());
        execute(buf);
        getLogger()->log(EXTENSION_LOG_INFO, NULL,
                         "Removed the temp table %s\n", it->c_str());
        if (destroyOnlyOne) {
            break;
        }
    }
}

void MultiTableSqliteStrategy::renameVBTable(uint16_t vbucket, const std::string &newName) {
    assert(db);
    char buf[1024];
    snprintf(buf, sizeof(buf),
             "alter table kv_%d rename to %s", static_cast<int>(vbucket), newName.c_str());
    execute(buf);
}

void MultiTableSqliteStrategy::createVBTable(uint16_t vbucket) {
    assert(db);
    char buf[1024];
    snprintf(buf, sizeof(buf),
             "create table if not exists kv_%d"
             " (vbucket integer,"
             "  vb_version integer,"
             "  k varchar(250),"
             "  flags integer,"
             "  exptime integer,"
             "  cas integer,"
             "  v text)", static_cast<int>(vbucket));
    execute(buf);
}

void MultiTableSqliteStrategy::destroyStatements() {
    while (!statements.empty()) {
        Statements *st = statements.back();
        delete st;
        statements.pop_back();
    }
}

//
// ----------------------------------------------------------------------
// Multiple Shards, Table Per Vbucket
// ----------------------------------------------------------------------
//

Statements *ShardedMultiTableSqliteStrategy::getStatements(uint16_t vbid, uint16_t,
                                                           const std::string &key) {
    size_t shard(getDbShardIdForKey(key));
    assert(static_cast<size_t>(shard) < statementsPerShard.size());
    assert(static_cast<size_t>(vbid) < statementsPerShard[shard].size());
    return statementsPerShard[shard][vbid];
}


void ShardedMultiTableSqliteStrategy::destroyStatements() {
    MultiTableSqliteStrategy::destroyStatements();
    statementsPerShard.clear();
}

void ShardedMultiTableSqliteStrategy::destroyTables() {
    char buf[1024];
    for (size_t i = 0; i < shardCount; ++i) {
        for (size_t j = 0; j < nvbuckets; ++j) {
            snprintf(buf, sizeof(buf), "drop table if exists kv_%d.kv_%d",
                     static_cast<int>(i), static_cast<int>(j));
            execute(buf);
        }
    }
}

void ShardedMultiTableSqliteStrategy::destroyInvalidTables(bool destroyOnlyOne) {
    assert(db);
    char buf[1024];
    for (size_t i = 0; i < shardCount; ++i) {
        std::vector<std::string> tables;
        snprintf(buf, sizeof(buf),
                 "select name from kv_%d.sqlite_master where name like 'invalid_kv_%%'",
                 static_cast<int>(i));
        PreparedStatement st(db, buf);
        while (st.fetch()) {
            tables.push_back(std::string(st.column(0)));
            if (destroyOnlyOne) {
                break;
            }
        }
        st.reset();
        std::vector<std::string>::iterator it = tables.begin();
        for (; it != tables.end(); ++it) {
            snprintf(buf, sizeof(buf), "drop table if exists kv_%d.%s",
                     static_cast<int>(i), it->c_str());
            execute(buf);
            getLogger()->log(EXTENSION_LOG_INFO, NULL,
                             "Removed the temp table kv_%ld.%s\n", i, it->c_str());
            if (destroyOnlyOne) {
                return;
            }
        }
    }
}

void ShardedMultiTableSqliteStrategy::renameVBTable(uint16_t vbucket, const std::string &newName) {
    assert(db);
    char buf[1024];
    for (size_t i = 0; i < shardCount; ++i) {
        snprintf(buf, sizeof(buf),
                 "alter table kv_%d.kv_%d rename to %s",
                 static_cast<int>(i), static_cast<int>(vbucket), newName.c_str());
        execute(buf);
    }
}

void ShardedMultiTableSqliteStrategy::createVBTable(uint16_t vbucket) {
    assert(db);
    char buf[1024];
    for (size_t i = 0; i < shardCount; ++i) {
        snprintf(buf, sizeof(buf),
                 "create table if not exists kv_%d.kv_%d"
                 " (vbucket integer,"
                 "  vb_version integer,"
                 "  k varchar(250),"
                 "  flags integer,"
                 "  exptime integer,"
                 "  cas integer,"
                 "  v text)", static_cast<int>(i), static_cast<int>(vbucket));
        execute(buf);
    }
}

std::vector<PreparedStatement*> ShardedMultiTableSqliteStrategy::getVBStatements(uint16_t vb,
                                                                      vb_statement_type vbst) {
    std::vector<PreparedStatement*> rv;
    for (size_t i = 0; i < shardCount; ++i) {
        std::vector<Statements*> st = statementsPerShard[i];
        assert(static_cast<size_t>(vb) < st.size());
        switch (vbst) {
        case select_all:
            rv.push_back(st.at(vb)->all());
            break;
        case delete_vbucket:
            rv.push_back(st.at(vb)->del_vb());
            break;
        default:
            break;
        }
    }
    return rv;
}

void ShardedMultiTableSqliteStrategy::initDB() {
    char buf[1024];
    PathExpander p(filename);

    for (size_t i = 0; i < shardCount; ++i) {
        std::string shardname(p.expand(shardpattern, static_cast<int>(i)));
        snprintf(buf, sizeof(buf), "attach database \"%s\" as kv_%d",
                 shardname.c_str(), static_cast<int>(i));
        execute(buf);
    }
    doFile(initFile);
}

void ShardedMultiTableSqliteStrategy::initTables() {
    char buf[1024];

    for (size_t i = 0; i < shardCount; ++i) {
        for (size_t j = 0; j < nvbuckets; ++j) {
            snprintf(buf, sizeof(buf),
                     "create table if not exists kv_%d.kv_%d"
                     " (vbucket integer,"
                     "  vb_version integer,"
                     "  k varchar(250),"
                     "  flags integer,"
                     "  exptime integer,"
                     "  cas integer,"
                     "  v text)",
                     static_cast<int>(i), static_cast<int>(j));
            execute(buf);
        }
    }
}

void ShardedMultiTableSqliteStrategy::initStatements() {
    char buf[64];
    statementsPerShard.resize(shardCount);
    FastVBDelStatementFactory sfact;
    for (size_t i = 0; i < shardCount; ++i) {
        statementsPerShard[i].resize(nvbuckets);
        for (size_t j = 0; j < nvbuckets; ++j) {
            snprintf(buf, sizeof(buf), "kv_%d.kv_%d",
                     static_cast<int>(i), static_cast<int>(j));
            Statements *s = new Statements(db, std::string(buf), &sfact);
            statements.push_back(s);
            statementsPerShard[i][j] = s;
        }
    }
}

std::string ShardedMultiTableSqliteStrategy::getKvTableName(const std::string &key,
                                                            uint16_t vbid)
{
    char buf[64];
    snprintf(buf, sizeof(buf), "kv_%d.kv_%d",
             static_cast<int>(getDbShardIdForKey(key)),
             static_cast<int>(vbid));
    return std::string(buf);
}

//
// ----------------------------------------------------------------------
// Sharded by VBucket
// ----------------------------------------------------------------------
//

void ShardedByVBucketSqliteStrategy::destroyTables() {
    char buf[1024];
    for (size_t j = 0; j < nvbuckets; ++j) {
        snprintf(buf, sizeof(buf), "drop table if exists kv_%d.kv_%d",
                 static_cast<int>(getShardForVBucket(static_cast<uint16_t>(j))),
                 static_cast<int>(j));
        execute(buf);
    }
}

void ShardedByVBucketSqliteStrategy::destroyInvalidTables(bool destroyOnlyOne) {
    assert(db);
    char buf[1024];
    for (size_t i = 0; i < shardCount; ++i) {
        std::vector<std::string> tables;
        snprintf(buf, sizeof(buf),
                 "select name from kv_%d.sqlite_master where name like 'invalid_kv_%%'",
                 static_cast<int>(i));
        PreparedStatement st(db, buf);
        while (st.fetch()) {
            tables.push_back(std::string(st.column(0)));
            if (destroyOnlyOne) {
                break;
            }
        }
        st.reset();
        std::vector<std::string>::iterator it = tables.begin();
        for (; it != tables.end(); ++it) {
            snprintf(buf, sizeof(buf), "drop table if exists kv_%d.%s",
                     static_cast<int>(i), it->c_str());
            execute(buf);
            getLogger()->log(EXTENSION_LOG_INFO, NULL,
                             "Removed the temp table kv_%ld.%s\n", i, it->c_str());
            if (destroyOnlyOne) {
                return;
            }
        }
    }
}

void ShardedByVBucketSqliteStrategy::renameVBTable(uint16_t vbucket, const std::string &newName) {
    assert(db);
    char buf[1024];
    snprintf(buf, sizeof(buf),
             "alter table kv_%d.kv_%d rename to %s",
             static_cast<int>(getShardForVBucket(static_cast<uint16_t>(vbucket))),
             static_cast<int>(vbucket),
             newName.c_str());
    execute(buf);
}

void ShardedByVBucketSqliteStrategy::createVBTable(uint16_t vbucket) {
    assert(db);
    char buf[1024];
    snprintf(buf, sizeof(buf),
             "create table if not exists kv_%d.kv_%d"
             " (vbucket integer,"
             "  vb_version integer,"
             "  k varchar(250),"
             "  flags integer,"
             "  exptime integer,"
             "  cas integer,"
             "  v text)",
             static_cast<int>(getShardForVBucket(static_cast<uint16_t>(vbucket))),
             static_cast<int>(vbucket));
    execute(buf);
}

void ShardedByVBucketSqliteStrategy::initDB() {
    char buf[1024];
    PathExpander p(filename);

    for (size_t i = 0; i < shardCount; ++i) {
        std::string shardname(p.expand(shardpattern, static_cast<int>(i)));
        snprintf(buf, sizeof(buf), "attach database \"%s\" as kv_%d",
                 shardname.c_str(), static_cast<int>(i));
        execute(buf);
    }
    doFile(initFile);
}

void ShardedByVBucketSqliteStrategy::initTables() {
    char buf[1024];

    for (size_t j = 0; j < nvbuckets; ++j) {
        snprintf(buf, sizeof(buf),
                 "create table if not exists kv_%d.kv_%d"
                 " (vbucket integer,"
                 "  vb_version integer,"
                 "  k varchar(250),"
                 "  flags integer,"
                 "  exptime integer,"
                 "  cas integer,"
                 "  v text)",
                 static_cast<int>(getShardForVBucket(static_cast<uint16_t>(j))),
                 static_cast<int>(j));
        execute(buf);
    }
}

void ShardedByVBucketSqliteStrategy::initStatements() {
    char buf[64];
    statements.resize(nvbuckets);
    FastVBDelStatementFactory sfact;
    for (size_t j = 0; j < nvbuckets; ++j) {
        snprintf(buf, sizeof(buf), "kv_%d.kv_%d",
                 static_cast<int>(getShardForVBucket(static_cast<uint16_t>(j))),
                 static_cast<int>(j));
        statements[j] = new Statements(db, std::string(buf), &sfact);
    }
}

std::string ShardedByVBucketSqliteStrategy::getKvTableName(const std::string &,
                                                           uint16_t vbid)
{
    char buf[64];
    snprintf(buf, sizeof(buf), "kv_%d.kv_%d",
             static_cast<int>(getShardForVBucket(vbid)),
             static_cast<int>(vbid));
    return std::string(buf);
}
