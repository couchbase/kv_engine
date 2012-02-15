/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef SQLITE_STRATEGIES_H
#define SQLITE_STRATEGIES_H 1

#include <cstdlib>
#include <vector>
#include <algorithm>

#include "common.hh"
#include "queueditem.hh"
#include "sqlite-pst.hh"
#include "sqlite-stats.hh"

class EventuallyPersistentEngine;

typedef enum {
    select_all,
    delete_vbucket
} vb_statement_type;

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

    virtual uint16_t getDbShardId(const QueuedItem &qi) {
        return getDbShardIdForKey(qi.getKey());
    }

    virtual const std::vector<Statements *> &allStatements() = 0;

    virtual Statements *getStatements(uint16_t vbid, uint16_t vbver,
                                      const std::string &key) = 0;

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

    virtual bool hasEfficientVBLoad() { return false; }

    virtual bool hasEfficientVBDeletion() { return false; }

    virtual std::vector<PreparedStatement*> getVBStatements(uint16_t vb, vb_statement_type vbst) {
        (void)vb;
        (void)vbst;
        std::vector<PreparedStatement*> rv;
        return rv;
    }

    virtual void closeVBStatements(std::vector<PreparedStatement*> &psts) {
        (void)psts;
    }

    virtual void destroyTables() = 0;
    virtual void destroyInvalidTables(bool destroyOnlyOne = false) = 0;

    virtual void renameVBTable(uint16_t vbucket, const std::string &newName) {
        (void)vbucket;
        (void)newName;
    }

    virtual void createVBTable(uint16_t vbucket) {
        (void)vbucket;
    }

    virtual void optimizeWrites(std::vector<queued_item> &items) {
        (void)items;
    }

    void execute(const char * const query);

    static void disableSchemaCheck() {
        shouldCheckSchemaVersion = false;
    }

    SQLiteStats         sqliteStats;

protected:

    void doFile(const char * const filename);

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

    virtual void initDB() {
        doFile(initFile);
    }

    void checkSchemaVersion();
    void initMetaTables();

    virtual void initTables() = 0;
    virtual void initStatements() = 0;
    virtual void initMetaStatements();
    virtual void destroyStatements() {};

    void destroyMetaStatements();

    static bool shouldCheckSchemaVersion;

    sqlite3            *db;
    const char * const  filename;
    const char * const  initFile;
    const char * const  postInitFile;
    size_t              shardCount;

    PreparedStatement *ins_vb_stmt;
    PreparedStatement *clear_vb_stmt;
    PreparedStatement *sel_vb_stmt;

    PreparedStatement *clear_stats_stmt;
    PreparedStatement *ins_stat_stmt;

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
     * @param shards the number of shards
     */
    SingleTableSqliteStrategy(const char * const fn,
                              const char * const finit,
                              const char * const pfinit,
                              size_t shards = 1) :
        SqliteStrategy(fn, finit, pfinit, shards), statements() {
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

    void destroyStatements();
    virtual void destroyTables();
    virtual void destroyInvalidTables(bool destroyOnlyOne = false);

    void optimizeWrites(std::vector<queued_item> &items) {
        // Sort all the queued items for each db shard by their row ids
        CompareQueuedItemsByRowId cq;
        std::sort(items.begin(), items.end(), cq);
    }

    virtual std::vector<PreparedStatement*> getVBStatements(uint16_t vb, vb_statement_type vbst) {
        (void)vb;
        std::vector<PreparedStatement*> rv;
        std::vector<Statements*>::iterator it;
        for (it = statements.begin(); it != statements.end(); ++it) {
            switch (vbst) {
            case select_all:
                rv.push_back((*it)->all());
                break;
            case delete_vbucket:
                rv.push_back((*it)->del_vb());
                break;
            default:
                break;
            }
        }

        return rv;
    }

    virtual void closeVBStatements(std::vector<PreparedStatement*> &psts) {
        std::for_each(psts.begin(), psts.end(),
                      std::mem_fun(&PreparedStatement::reset));
    }

protected:
    std::vector<Statements *> statements;

    virtual void initTables();
    virtual void initStatements();

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
                                     const char * const finit,
                                     const char * const pfinit,
                                     int n):
        SingleTableSqliteStrategy(fn, finit, pfinit, n),
        shardpattern(sp), numTables(n) {
        assert(shardpattern);
    }

    void initDB(void);
    void initTables(void);
    void initStatements(void);
    void destroyTables(void);
    void destroyInvalidTables(bool destroyOnlyOne = false);

private:
    const char * const shardpattern;
    int numTables;
};

//
// ----------------------------------------------------------------------
// Table Per Vbucket
// ----------------------------------------------------------------------
//

/**
 * Strategy for a table per vbucket store in a single DB.
 */
class MultiTableSqliteStrategy : public SqliteStrategy {
public:

    /**
     * Constructor.
     *
     * @param fn the filename of the DB
     * @param finit an init script to run as soon as the DB opens
     * @param pfinit an init script to run after initializing all schema
     * @param nv the maxinum number of vbuckets
     * @param shards the number of data shards
     */
    MultiTableSqliteStrategy(const char * const fn,
                             const char * const finit,
                             const char * const pfinit,
                             int nv,
                             int shards=1)
        : SqliteStrategy(fn, finit, pfinit, shards),
          nvbuckets(nv), statements() {

        assert(filename);
    }

    virtual ~MultiTableSqliteStrategy() { }

    const std::vector<Statements *> &allStatements() {
        return statements;
    }

    virtual Statements *getStatements(uint16_t vbid, uint16_t vbver,
                                      const std::string &key) {
        (void)vbver;
        (void)key;
        assert(static_cast<size_t>(vbid) < statements.size());
        return statements.at(vbid);
    }

    virtual void destroyStatements();
    virtual void destroyTables();
    virtual void destroyInvalidTables(bool destroyOnlyOne = false);

    virtual void renameVBTable(uint16_t vbucket, const std::string &newName);
    virtual void createVBTable(uint16_t vbucket);

    bool hasEfficientVBLoad() { return true; }

    bool hasEfficientVBDeletion() { return true; }

    virtual std::vector<PreparedStatement*> getVBStatements(uint16_t vb, vb_statement_type vbst) {
        std::vector<PreparedStatement*> rv;
        assert(static_cast<size_t>(vb) < statements.size());
        switch (vbst) {
        case select_all:
            rv.push_back(statements.at(vb)->all());
            break;
        case delete_vbucket:
            rv.push_back(statements.at(vb)->del_vb());
            break;
        default:
            break;
        }
        return rv;
    }

    void closeVBStatements(std::vector<PreparedStatement*> &psts) {
        std::for_each(psts.begin(), psts.end(),
                      std::mem_fun(&PreparedStatement::reset));
    }

    void optimizeWrites(std::vector<queued_item> &items) {
        // Sort all the queued items for each db shard by its vbucket
        // ID and then its row ids
        CompareQueuedItemsByVBAndRowId cq;
        std::sort(items.begin(), items.end(), cq);
    }

protected:
    size_t nvbuckets;
    std::vector<Statements *> statements;

    virtual void initTables();
    virtual void initStatements();

private:
    DISALLOW_COPY_AND_ASSIGN(MultiTableSqliteStrategy);
};

//
// ----------------------------------------------------------------------
// Multiple Shards, Table Per Vbucket
// ----------------------------------------------------------------------
//

/**
 * Strategy for a table per vbucket store in multiple shards.
 */
class ShardedMultiTableSqliteStrategy : public MultiTableSqliteStrategy {
public:

    /**
     * Constructor.
     *
     * @param fn the filename of the DB
     * @param sp the shard pattern
     * @param finit an init script to run as soon as the DB opens
     * @param pfinit an init script to run after initializing all schema
     * @param nv the maxinum number of vbuckets
     * @param n the number of data shards
     */
    ShardedMultiTableSqliteStrategy(const char * const fn,
                                    const char * const sp,
                                    const char * const finit,
                                    const char * const pfinit,
                                    int nv,
                                    int n)
        : MultiTableSqliteStrategy(fn, finit, pfinit, nv, n),
          shardpattern(sp),
          statementsPerShard() {

        assert(filename);
    }

    virtual ~ShardedMultiTableSqliteStrategy() { }

    Statements *getStatements(uint16_t vbid, uint16_t vbver,
                              const std::string &key);

    void destroyStatements();
    void destroyTables();
    void destroyInvalidTables(bool destroyOnlyOne = false);

    void renameVBTable(uint16_t vbucket, const std::string &newName);
    void createVBTable(uint16_t vbucket);

    std::vector<PreparedStatement*> getVBStatements(uint16_t vb, vb_statement_type vbst);

protected:
    const char * const        shardpattern;
    // statementsPerShard[vbucket][shard]
    std::vector<std::vector<Statements*> > statementsPerShard;

    void initDB();
    void initTables();
    void initStatements();

private:
    DISALLOW_COPY_AND_ASSIGN(ShardedMultiTableSqliteStrategy);
};

//
// ----------------------------------------------------------------------
// Sharded by VBucket
// ----------------------------------------------------------------------
//

/**
 * Strategy for a table per vbucket where each vbucket exists in only
 * one shard.
 */
class ShardedByVBucketSqliteStrategy : public MultiTableSqliteStrategy {
public:

    /**
     * Constructor.
     *
     * @param fn the filename of the DB
     * @param sp the shard pattern for generating shard files
     * @param finit an init script to run as soon as the DB opens
     * @param pfinit an init script to run after initializing all schema
     * @param nv the maxinum number of vbuckets
     * @param n the number of data shards
     */
    ShardedByVBucketSqliteStrategy(const char * const fn,
                                   const char * const sp,
                                   const char * const finit,
                                   const char * const pfinit,
                                   int nv,
                                   int n)
        : MultiTableSqliteStrategy(fn, finit, pfinit, nv, n),
          shardpattern(sp) {

        assert(filename);
    }

    uint16_t getDbShardId(const QueuedItem &qi) {
        return getShardForVBucket(qi.getVBucketId());
    }

    virtual ~ShardedByVBucketSqliteStrategy() { }

    void destroyTables();
    void destroyInvalidTables(bool destroyOnlyOne = false);

    void renameVBTable(uint16_t vbucket, const std::string &newName);
    void createVBTable(uint16_t vbucket);

protected:
    const char * const shardpattern;

    size_t getShardForVBucket(uint16_t vb) {
        size_t rv = (static_cast<size_t>(vb) % shardCount);
        assert(rv < shardCount);
        return rv;
    }

    void initDB();
    void initTables();
    void initStatements();

private:
    DISALLOW_COPY_AND_ASSIGN(ShardedByVBucketSqliteStrategy);
};

#endif /* SQLITE_STRATEGIES_H */
