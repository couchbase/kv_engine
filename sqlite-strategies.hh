/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef SQLITE_STRATEGIES_H
#define SQLITE_STRATEGIES_H 1

#include <cstdlib>
#include <vector>

#include "common.hh"
#include "sqlite-pst.hh"

class EventuallyPersistentEngine;

/**
 * Base class for all Sqlite strategies.
 */
class SqliteStrategy {
public:

    SqliteStrategy(const char * const fn,
                   const char * const finit,
                   const char * const pfinit,
                   size_t shards);

    virtual ~SqliteStrategy();

    sqlite3 *open();
    void close();

    size_t getNumOfDbShards() {
        return shardCount;
    }

    uint16_t getDbShardIdForKey(const std::string &key) {
        assert(shardCount > 0);
        int h=5381;
        int i=0;
        const char *str = key.c_str();

        for(i=0; str[i] != 0x00; i++) {
            h = ((h << 5) + h) ^ str[i];
        }
        return std::abs(h) % (int)shardCount;
    }

    virtual const std::vector<Statements *> &allStatements() = 0;

    virtual Statements *getStatements(uint16_t vbid, uint16_t vbver,
                                      const std::string &key) = 0;

    virtual PreparedStatement *getInsVBucketStateST() = 0;

    virtual PreparedStatement *getClearVBucketStateST() = 0;

    virtual PreparedStatement *getGetVBucketStateST() = 0;

    virtual PreparedStatement *getClearStatsST() = 0;
    virtual PreparedStatement *getInsStatST() = 0;

    virtual void destroyTables() = 0;

    void execute(const char * const query);

protected:

    void doFile(const char * const filename);

    virtual void initDB() {
        doFile(initFile);
    }

    virtual void initMetaTables() = 0;
    virtual void initTables() = 0;
    virtual void initStatements() = 0;
    virtual void destroyStatements() {};

    sqlite3            *db;
    const char * const  filename;
    const char * const  initFile;
    const char * const  postInitFile;
    size_t              shardCount;
    uint16_t            schema_version;

private:
    DISALLOW_COPY_AND_ASSIGN(SqliteStrategy);
};

//
// ----------------------------------------------------------------------
// Concrete Strategies
// ----------------------------------------------------------------------
//

/**
 * Strategy for a single table kv store in a single DB.
 */
class SingleTableSqliteStrategy : public SqliteStrategy {
public:

    /**
     * Constructor.
     *
     * @param fn the filename of the DB
     * @param finit an init script to run as soon as the DB opens
     * @param pfinit an init script to run after initializing all schema
     */
    SingleTableSqliteStrategy(const char * const fn,
                              const char * const finit = NULL,
                              const char * const pfinit = NULL,
                              size_t shards = 1) :
        SqliteStrategy(fn, finit, pfinit, shards),
        statements(),
        ins_vb_stmt(NULL), clear_vb_stmt(NULL), sel_vb_stmt(NULL),
        clear_stats_stmt(NULL), ins_stat_stmt(NULL) {
        assert(filename);
    }

    virtual ~SingleTableSqliteStrategy() { }

    const std::vector<Statements *> &allStatements() {
        return statements;
    }

    Statements *getStatements(uint16_t vbid, uint16_t vbver,
                              const std::string &key) {
        (void)vbid;
        (void)vbver;
        return statements.at(getDbShardIdForKey(key));
    }

    PreparedStatement *getInsVBucketStateST() {
        return ins_vb_stmt;
    }

    PreparedStatement *getClearVBucketStateST() {
        return clear_vb_stmt;
    }

    PreparedStatement *getGetVBucketStateST() {
        return sel_vb_stmt;
    }


    PreparedStatement *getClearStatsST() {
        return clear_stats_stmt;
    }

    PreparedStatement *getInsStatST() {
        return ins_stat_stmt;
    }

    void destroyStatements();
    virtual void destroyTables();

    virtual void initMetaStatements();
    virtual void destroyMetaStatements();

protected:
    std::vector<Statements *> statements;

    virtual void initMetaTables();
    virtual void initTables();
    virtual void initStatements();

    PreparedStatement *ins_vb_stmt;
    PreparedStatement *clear_vb_stmt;
    PreparedStatement *sel_vb_stmt;

    PreparedStatement *clear_stats_stmt;
    PreparedStatement *ins_stat_stmt;

private:
    DISALLOW_COPY_AND_ASSIGN(SingleTableSqliteStrategy);
};

//
// ----------------------------------------------------------------------
// Multi DB strategy
// ----------------------------------------------------------------------
//

/**
 * A specialization of SqliteStrategy that allows multiple data
 * shards with a single kv table each.
 */
class MultiDBSingleTableSqliteStrategy : public SingleTableSqliteStrategy {
public:

    /**
     * Constructor.
     *
     * @param fn same as SqliteStrategy
     * @param sp the shard pattern
     * @param finit same as SqliteStrategy
     * @param pfinit same as SqliteStrategy
     * @param n number of DB shards to create
     */
    MultiDBSingleTableSqliteStrategy(const char * const fn,
                                     const char * const sp,
                                     const char * const finit = NULL,
                                     const char * const pfinit = NULL,
                                     int n=4):
        SingleTableSqliteStrategy(fn, finit, pfinit, n),
        shardpattern(sp), numTables(n) {
        assert(shardpattern);
    }

    void initDB(void);
    void initTables(void);
    void initStatements(void);
    void destroyTables(void);

private:
    const char * const shardpattern;
    int numTables;
};

#endif /* SQLITE_STRATEGIES_H */
