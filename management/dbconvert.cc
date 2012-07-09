/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "config.h"

#include <cassert>
#include <iostream>
#include <utility>
#include <cstdlib>

#ifdef HAVE_SYSEXITS_H
#include <sysexits.h>
#endif

#include <memcached/engine.h>

#include <getopt.h>
#include <stats.hh>
#include <kvstore.hh>
#include <item.hh>
#include <callbacks.hh>
#include "sqlite-kvstore/sqlite-strategies.hh"
#include "sqlite-kvstore/sqlite-kvstore.hh"

#ifndef EX_USAGE
#define EX_USAGE 64
#endif

/* getopt.h on Solaris defines the name member as "char*" and not
 * "const char*". This cause a compile error when you try to assign
 * it to a constant string. To aviod compile errors let's create
 * a macro to cast it to a char pointer.
 */
#ifdef __sun
#define OPTNAME(a) (char*)(a)
#else
#define OPTNAME(a) (const char*)(a)
#endif

using namespace std;

static KVStore *getStore(EPStats &st,
                         const char *path,
                         const char *strategyName,
                         const char *shardPattern,
                         const char *initFile = NULL) {
    db_type dbStrategy = multi_db;

    if (!SqliteKVStoreFactory::stringToType(strategyName, dbStrategy)) {
        cerr << "Unable to parse strategy type:  " << strategyName << endl;
        exit(EX_USAGE);
    }

    size_t nVBuckets(1024);
    size_t dbShards(4);
    SqliteStrategy *sqliteInstance = NULL;

    switch (dbStrategy) {
    case multi_db:
        sqliteInstance = new MultiDBSingleTableSqliteStrategy(path,
                                                              shardPattern,
                                                              initFile,
                                                              NULL, dbShards);
        break;
    case single_db:
        sqliteInstance = new SingleTableSqliteStrategy(path,
                                                       initFile,
                                                       NULL);
        break;
    case single_mt_db:
        sqliteInstance = new MultiTableSqliteStrategy(path,
                                                      initFile,
                                                      NULL, nVBuckets);
        break;
    case multi_mt_db:
        sqliteInstance = new ShardedMultiTableSqliteStrategy(path,
                                                             shardPattern,
                                                             initFile,
                                                             NULL, nVBuckets,
                                                             dbShards);
        break;
    case multi_mt_vb_db:
        sqliteInstance = new ShardedByVBucketSqliteStrategy(path,
                                                            shardPattern,
                                                            initFile,
                                                            NULL, nVBuckets,
                                                            dbShards);
        break;
    }

    return new StrategicSqlite3(st,
                                shared_ptr<SqliteStrategy>(sqliteInstance));
}

class MutationVerifier : public Callback<mutation_result> {
public:
    void callback(mutation_result &mutation) {
        assert(mutation.first == 1);
    }
};

class Mover : public Callback<GetValue> {
public:

    Mover(KVStore *d, size_t re, bool kc, size_t ts) : dest(d),
                                                       transferred(0),
                                                       txnSize(ts),
                                                       reportEvery(re),
                                                       killCrlf(kc),
                                                       inTxn(false) {
        assert(dest);
        assert(txnSize != 0);
        assert(reportEvery != 0);

        if (!enterTransaction()) {
           cout << "Failed to start a transaction" << endl;
           abort();
        }

        startTime = gethrtime();
    }

    ~Mover() {
        nextTransaction(false);
        cout << "Elapsed time=" << (gethrtime() - startTime)/1000000000 << " seconds."<< std::endl;
    }

    void callback(GetValue &gv) {
        Item *i = gv.getValue();
        adjust(&i);
        dest->set(*i, mv);
        delete i;

        if ((++transferred % txnSize) == 0) {
            nextTransaction(true);
        }
        if ((transferred % reportEvery) == 0) {
            cout << "." << flush;
        }
    }

    size_t getTransferred() {
        return transferred;
    }

    bool enterTransaction()  {
        if (!inTxn) {
           inTxn = dest->begin();
        }
        return inTxn;
    }

    bool nextTransaction(bool next)  {
        if (inTxn) {
            while (!dest->commit()) {
                cout << "Failed to commit a transaction. Sleep a while." << endl;
                sleep(1);
            }
            inTxn = false;
        }
        return next ? enterTransaction() : true;
    }

private:

    void adjust(Item **i) {
        Item *input(*i);
        if (killCrlf) {
            const char *data = input->getData();
            assert(data[input->getNBytes() - 2] == '\r');
            assert(data[input->getNBytes() - 1] == '\n');
            *i = new Item(input->getKey(), input->getFlags(),
                          input->getExptime(),
                          data, input->getNBytes() - 2,
                          0, -1, input->getVBucketId());
            delete input;
        } else {
            input->setId(-1);
        }
    }

    MutationVerifier  mv;
    KVStore          *dest;
    size_t            transferred;
    size_t            txnSize;
    size_t            reportEvery;
    bool              killCrlf;
    bool              inTxn;
    hrtime_t          startTime;
};

static void usage(const char *cmd) {
    cerr << "Usage:  " << cmd << " [args] srcPath destPath" << endl
         << endl
         << "Optional arguments:" << endl
         << "  --src-strategy=someStrategy (default=multiDB)" << endl
         << "  --src-pattern=shardPattern (default=%d/%b-%i.sqlite)" << endl
         << "  --dest-strategy=someStrategy (default=multiMTVBDB)" << endl
         << "  --dest-pattern=somePattern (default=%d/%b-%i.mb)" << endl
         << "  --remove-crlf" << endl
         << "  --txn-size=someNumber (default=10000)" << endl
         << "  --report-every=someNumber (default=10000)" << endl
         << "  --init-file=filepath (default=NULL)" << endl;
    exit(EX_USAGE);
}

int main(int argc, char **argv) {
    putenv(strdup("ALLOW_NO_STATS_UPDATE=yeah"));
    const char *cmd(argv[0]);
    const char *srcPath(NULL), *srcStrategy("multiDB");
    const char *destPath(NULL), *destStrategy("multiMTVBDB");
    const char *srcShardPattern("%d/%b-%i.sqlite");
    const char *destShardPattern("%d/%b-%i.mb");
    const char *initFile(NULL);
    size_t txnSize(10000), reportEvery(10000);
    int killCrlf(0);

    /* options descriptor */
    static struct option longopts[] = {
        { OPTNAME("src-strategy"),  required_argument, NULL,      's' },
        { OPTNAME("src-pattern"),   required_argument, NULL,      'p' },
        { OPTNAME("dest-strategy"), required_argument, NULL,      'S' },
        { OPTNAME("dest-pattern"),  required_argument, NULL,      'P' },
        { OPTNAME("remove-crlf"),   no_argument,       &killCrlf, 'x' },
        { OPTNAME("txn-size"),      required_argument, NULL,      't' },
        { OPTNAME("report-every"),  required_argument, NULL,      'r' },
        { OPTNAME("init-file"),     required_argument, NULL,      'i' },
        { NULL,            0,                 NULL,      0 }
    };

    int ch(0);
    while ((ch = getopt_long(argc, argv, "s:S:x", longopts, NULL)) != -1) {
        switch (ch) {
        case 's':
            srcStrategy = optarg;
            break;
        case 'p':
            srcShardPattern = optarg;
            break;
        case 'S':
            destStrategy = optarg;
            break;
        case 'P':
            destShardPattern = optarg;
            break;
        case 'i':
            initFile = optarg;
            break;
        case 't':
            txnSize = static_cast<size_t>(atoi(optarg));
            break;
        case 'r':
            reportEvery = static_cast<size_t>(atoi(optarg));
            break;
        case 0: // Path for automatically handled cases (e.g. remove-crlf)
            break;
        default:
            usage(cmd);
        }
    }
    argc -= optind;
    argv += optind;

    if (argc != 2) {
        usage(cmd);
    }
    srcPath = argv[0];
    destPath = argv[1];

    cout << "src = " << srcStrategy << "@" << srcPath << endl;
    cout << "dest = " << destStrategy << "@" << destPath << endl;
    EPStats srcStats, destStats;

    SqliteStrategy::disableSchemaCheck();

    KVStore *src(getStore(srcStats, srcPath,
                          srcStrategy, srcShardPattern));
    KVStore *dest(getStore(destStats, destPath,
                           destStrategy, destShardPattern, initFile));

    shared_ptr<Mover> mover(new Mover(dest, txnSize, static_cast<bool>(killCrlf), reportEvery));
    cout << "Each . represents " << reportEvery << " items moved." << endl;
    src->dump(mover);
    cout << endl << "Moved " << mover->getTransferred() << " items." << endl;

    return 0;
}
