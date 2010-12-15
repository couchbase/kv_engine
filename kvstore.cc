/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"

#include <string>
#include <map>

#include "common.hh"
#include "stats.hh"
#include "kvstore.hh"
#include "sqlite-kvstore.hh"

KVStore *KVStore::create(db_type type, EPStats &stats,
                         const KVStoreConfig &conf) {
    SqliteStrategy *sqliteInstance = NULL;
    switch (type) {
    case multi_db:
        sqliteInstance = new MultiDBSingleTableSqliteStrategy(conf.location,
                                                              conf.shardPattern,
                                                              conf.initFile,
                                                              conf.postInitFile,
                                                              conf.shards);
        break;
    case single_db:
        sqliteInstance = new SingleTableSqliteStrategy(conf.location,
                                                       conf.initFile,
                                                       conf.postInitFile);
        break;
    case single_mt_db:
        sqliteInstance = new MultiTableSqliteStrategy(conf.location,
                                                      conf.initFile,
                                                      conf.postInitFile);
        break;
    }
    return new StrategicSqlite3(stats,
                                shared_ptr<SqliteStrategy>(sqliteInstance));
}
