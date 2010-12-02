/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef SQLITE_STRATEGIES_H
#define SQLITE_STRATEGIES_H 1

#include <cstdlib>
#include <vector>

#include "common.hh"
#include "sqlite-pst.hh"

class EventuallyPersistentEngine;

/**
 * Base strategy for persisting data in SQLite.
 */
class SqliteStrategy {
public:

    /**
     * Constructor.
     *
     * @param fn the filename of the DB
     * @param finit an init script to run as soon as the DB opens
     * @param pfinit an init script to run after initializing all schema
     */
    SqliteStrategy(const char * const fn,
                   const char * const finit = NULL,
                   const char * const pfinit = NULL) :
        filename(fn),
        initFile(finit),
        postInitFile(pfinit),
        schema_version(0),
        db(NULL),
        statements(),
        ins_vb_stmt(NULL), clear_vb_stmt(NULL), sel_vb_stmt(NULL),
        clear_stats_stmt(NULL), ins_stat_stmt(NULL)
    { }

    virtual ~SqliteStrategy() {
        close();
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

    size_t getNumOfDbShards(void) {
        assert(shardCount > 0);
        return shardCount;
    }

    const std::vector<Statements *> &allStatements() {
        return statements;
    }

    Statements *forKey(const std::string &key) {
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


    virtual void initMetaTables();

    PreparedStatement *getClearStatsST() {
        return clear_stats_stmt;
    }

    PreparedStatement *getInsStatST() {
        return ins_stat_stmt;
    }

    virtual void initTables(void);
    virtual void initStatements(void);
    virtual void destroyTables(void);
    void destroyStatements(void);

    virtual void initMetaStatements(void);
    virtual void destroyMetaStatements(void);

    void execute(const char * const query);

    sqlite3 *open(void);
    void close(void);

protected:
    const char * const filename;
    const char * const initFile;
    const char * const postInitFile;
    uint16_t schema_version;
    size_t shardCount;
    sqlite3 *db;
    std::vector<Statements *> statements;

    PreparedStatement *ins_vb_stmt;
    PreparedStatement *clear_vb_stmt;
    PreparedStatement *sel_vb_stmt;

    void doFile(const char * const filename);

    PreparedStatement *clear_stats_stmt;
    PreparedStatement *ins_stat_stmt;

private:
    DISALLOW_COPY_AND_ASSIGN(SqliteStrategy);
};

//
// ----------------------------------------------------------------------
// Multi DB strategy
// ----------------------------------------------------------------------
//

/**
 * A specialization of SqliteStrategy that allows multiple data
 * shards.
 */
class MultiDBSqliteStrategy : public SqliteStrategy {
public:

    /**
     * Constructor.
     *
     * @param fn same as SqliteStrategy
     * @param finit same as SqliteStrategy
     * @param pfinit same as SqliteStrategy
     * @param n number of DB shards to create
     */
    MultiDBSqliteStrategy(const char * const fn,
                          const char * const finit = NULL,
                          const char * const pfinit = NULL,
                          int n=4):
        SqliteStrategy(fn, finit, pfinit),
        numTables(n)
    {}

    void initTables(void);
    void initStatements(void);
    void destroyTables(void);

private:
    int numTables;
};

#endif /* SQLITE_STRATEGIES_H */
