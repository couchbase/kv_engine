/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <string.h>
#include <cstdlib>
#include <cctype>
#include <algorithm>

#include "sqlite-kvstore.hh"
#include "sqlite-pst.hh"

#define STATWRITER_NAMESPACE sqlite_engine
#include "statwriter.hh"
#undef STATWRITER_NAMESPACE

StrategicSqlite3::StrategicSqlite3(EPStats &st, shared_ptr<SqliteStrategy> s) : KVStore(),
    stats(st), strategy(s),
    intransaction(false) {
    open();
}

StrategicSqlite3::StrategicSqlite3(const StrategicSqlite3 &from) : KVStore(from),
    stats(from.stats), strategy(from.strategy),
    intransaction(false) {
    open();
}

int64_t StrategicSqlite3::lastRowId() {
    assert(db);
    return static_cast<int64_t>(sqlite3_last_insert_rowid(db));
}

void StrategicSqlite3::insert(const Item &itm, uint16_t vb_version,
                              Callback<mutation_result> &cb) {
    assert(itm.getId() <= 0);

    PreparedStatement *ins_stmt = strategy->getStatements(itm.getVBucketId(),
                                                          vb_version,
                                                          itm.getKey())->ins();
    ins_stmt->bind(1, itm.getKey());
    ins_stmt->bind(2, const_cast<Item&>(itm).getData(), itm.getNBytes());
    ins_stmt->bind(3, itm.getFlags());
    ins_stmt->bind(4, itm.getExptime());
    ins_stmt->bind64(5, itm.getCas());
    ins_stmt->bind(6, itm.getVBucketId());
    ins_stmt->bind(7, vb_version);

    ++stats.io_num_write;
    stats.io_write_bytes += itm.getKey().length() + itm.getNBytes();

    int rv = ins_stmt->execute();
    if (rv == 1) {
        stats.totalPersisted++;
    }

    int64_t newId = lastRowId();

    std::pair<int, int64_t> p(rv, newId);
    cb.callback(p);
    ins_stmt->reset();
}

void StrategicSqlite3::update(const Item &itm, uint16_t vb_version,
                              Callback<mutation_result> &cb) {
    assert(itm.getId() > 0);

    PreparedStatement *upd_stmt = strategy->getStatements(itm.getVBucketId(),
                                                          vb_version,
                                                          itm.getKey())->upd();

    upd_stmt->bind(1, itm.getKey());
    upd_stmt->bind(2, const_cast<Item&>(itm).getData(), itm.getNBytes());
    upd_stmt->bind(3, itm.getFlags());
    upd_stmt->bind(4, itm.getExptime());
    upd_stmt->bind64(5, itm.getCas());
    upd_stmt->bind(6, vb_version);
    upd_stmt->bind64(7, itm.getId());

    int rv = upd_stmt->execute();
    if (rv == 1) {
        stats.totalPersisted++;
    }
    ++stats.io_num_write;
    stats.io_write_bytes += itm.getKey().length() + itm.getNBytes();

    std::pair<int, int64_t> p(rv, 0);
    cb.callback(p);
    upd_stmt->reset();
}

vbucket_map_t StrategicSqlite3::listPersistedVbuckets() {
    std::map<std::pair<uint16_t, uint16_t>, vbucket_state> rv;

    PreparedStatement *st = strategy->getGetVBucketStateST();

    while (st->fetch()) {
        ++stats.io_num_read;
        std::pair<uint16_t, uint16_t> vb(st->column_int(0), st->column_int(1));
        vbucket_state vb_state;
        vb_state.state = st->column(2);
        vb_state.checkpointId = st->column_int64(3);
        rv[vb] = vb_state;
    }

    st->reset();

    return rv;
}

void StrategicSqlite3::set(const Item &itm, uint16_t vb_version,
                           Callback<mutation_result> &cb) {
    if (itm.getId() <= 0) {
        insert(itm, vb_version, cb);
    } else {
        update(itm, vb_version, cb);
    }
}

void StrategicSqlite3::get(const std::string &key, uint64_t rowid,
                           uint16_t vb, uint16_t vbver, Callback<GetValue> &cb) {
    PreparedStatement *sel_stmt = strategy->getStatements(vb, vbver, key)->sel();
    sel_stmt->bind64(1, rowid);

    ++stats.io_num_read;

    if(sel_stmt->fetch()) {
        GetValue rv(new Item(key.data(),
                             static_cast<uint16_t>(key.length()),
                             sel_stmt->column_int(1),
                             sel_stmt->column_int(2),
                             sel_stmt->column_blob(0),
                             sel_stmt->column_bytes(0),
                             sel_stmt->column_int64(3),
                             sel_stmt->column_int64(4),
                             static_cast<uint16_t>(sel_stmt->column_int(5))));
        stats.io_read_bytes += key.length() + rv.getValue()->getNBytes();
        cb.callback(rv);
    } else {
        GetValue rv;
        cb.callback(rv);
    }
    sel_stmt->reset();
}

void StrategicSqlite3::reset() {
    if (db) {
        rollback();
        close();
        open();
        strategy->destroyTables();
        close();
        open();
        execute("vacuum");
    }
}

void StrategicSqlite3::del(const std::string &key, uint64_t rowid,
                           uint16_t vb, uint16_t vbver,
                           Callback<int> &cb) {
    PreparedStatement *del_stmt = strategy->getStatements(vb, vbver, key)->del();
    del_stmt->bind64(1, rowid);
    int rv = del_stmt->execute();
    if (rv > 0) {
        stats.totalPersisted++;
    }
    cb.callback(rv);
    del_stmt->reset();
}

bool StrategicSqlite3::delVBucket(uint16_t vbucket, uint16_t vb_version,
                                  std::pair<int64_t, int64_t> row_range) {
    bool rv = true;
    std::vector<PreparedStatement*> vb_del(strategy->getVBStatements(vbucket, delete_vbucket));
    std::vector<PreparedStatement*>::iterator it;
    for (it = vb_del.begin(); it != vb_del.end(); ++it) {
        PreparedStatement *del_stmt = *it;
        if (del_stmt->paramCount() > 0) {
            del_stmt->bind(1, vbucket);
            del_stmt->bind(2, vb_version);
            del_stmt->bind64(3, row_range.first);
            del_stmt->bind64(4, row_range.second);
        }
        rv &= del_stmt->execute() >= 0;
    }
    strategy->closeVBStatements(vb_del);
    ++stats.io_num_write;

    return rv;
}

bool StrategicSqlite3::delVBucket(uint16_t vbucket, uint16_t vb_version) {
    (void) vb_version;
    assert(strategy->hasEfficientVBDeletion());
    bool rv = true;
    std::stringstream tmp_table_name;
    tmp_table_name << "invalid_kv_" << vbucket << "_" << gethrtime();
    rv = begin();
    if (rv) {
        strategy->renameVBTable(vbucket, tmp_table_name.str());
        strategy->createVBTable(vbucket);
        rv = commit();
    }
    return rv;
}

bool StrategicSqlite3::snapshotVBuckets(const vbucket_map_t &m) {
    return storeMap(strategy->getClearVBucketStateST(),
                    strategy->getInsVBucketStateST(), m);
}

bool StrategicSqlite3::snapshotStats(const std::map<std::string, std::string> &m) {
    return storeMap(strategy->getClearStatsST(), strategy->getInsStatST(), m);
}

/**
 * Function object to set a series of k,v pairs into a PreparedStatement.
 */
template <typename T1, typename T2>
struct map_setter {

    /**
     * Constructor.
     *
     * @param i the prepared statement to operate on
     * @param o the location of the return value - will set to false upon failure
     */
    map_setter(PreparedStatement *i, bool &o) : insSt(i), output(o) {}

    PreparedStatement *insSt;
    bool &output;

    void operator() (const std::pair<T1, T2> &p) {
        int pos = 1;
        pos += insSt->bind(pos, p.first);
        insSt->bind(pos, p.second);

        bool inserted = insSt->execute() == 1;
        insSt->reset();
        output &= inserted;
    }
};

template <typename T1, typename T2>
bool StrategicSqlite3::storeMap(PreparedStatement *clearSt,
                                PreparedStatement *insSt,
                                const std::map<T1, T2> &m) {
    bool rv(false);
    if (!begin()) {
        return false;
    }
    try {
        bool deleted = clearSt->execute() >= 0;
        rv &= deleted;
        clearSt->reset();

        map_setter<T1, T2> ms(insSt, rv);
        std::for_each(m.begin(), m.end(), ms);

        commit();
        rv = true;
    } catch(...) {
        rollback();
    }
    return rv;
}

static void processDumpRow(EPStats &stats,
                           PreparedStatement *st, Callback<GetValue> &cb) {
    ++stats.io_num_read;
    GetValue rv(new Item(st->column_blob(0),
                         static_cast<uint16_t>(st->column_bytes(0)),
                         st->column_int(2),
                         st->column_int(3),
                         st->column_blob(1),
                         st->column_bytes(1),
                         0,
                         st->column_int64(7),
                         static_cast<uint16_t>(st->column_int(5))),
                ENGINE_SUCCESS,
                -1,
                static_cast<uint16_t>(st->column_int(6)));
    stats.io_read_bytes += rv.getValue()->getKey().length() + rv.getValue()->getNBytes();
    cb.callback(rv);
}

void StrategicSqlite3::dump(Callback<GetValue> &cb) {
    const std::vector<Statements*> statements = strategy->allStatements();
    std::vector<Statements*>::const_iterator it;
    for (it = statements.begin(); it != statements.end(); ++it) {
        PreparedStatement *st = (*it)->all();
        st->reset();
        while (st->fetch()) {
            processDumpRow(stats, st, cb);
        }

        st->reset();
    }
}

void StrategicSqlite3::dump(uint16_t vb, Callback<GetValue> &cb) {
    assert(strategy->hasEfficientVBLoad());
    std::vector<PreparedStatement*> loaders(strategy->getVBStatements(vb, select_all));

    std::vector<PreparedStatement*>::iterator it;
    for (it = loaders.begin(); it != loaders.end(); ++it) {
        PreparedStatement *st = *it;
        while (st->fetch()) {
            processDumpRow(stats, st, cb);
        }
    }

    strategy->closeVBStatements(loaders);
}


static char lc(const char i) {
    return std::tolower(i);
}

StorageProperties StrategicSqlite3::getStorageProperties() {
    // Verify we at least compiled in mutexes.
    assert(sqlite3_threadsafe());
    bool allows_concurrency(false);
    {
        PreparedStatement st(db, "pragma journal_mode");
        static const std::string wal_str("wal");
        if (st.fetch()) {
            std::string s(st.column(0));
            std::transform(s.begin(), s.end(), s.begin(), lc);
            getLogger()->log(EXTENSION_LOG_INFO, NULL,
                             "journal-mode:  %s\n", s.c_str());
            allows_concurrency = s == wal_str;
        }
    }
    if (allows_concurrency) {
        PreparedStatement st(db, "pragma read_uncommitted");
        if (st.fetch()) {
            allows_concurrency = st.column_int(0) == 1;
            getLogger()->log(EXTENSION_LOG_INFO, NULL,
                             "read_uncommitted:  %s\n",
                             allows_concurrency ? "yes" : "no");
        }
    }
    size_t concurrency(allows_concurrency ? 10 : 1);
    StorageProperties rv(concurrency, concurrency - 1, 1,
                         strategy->hasEfficientVBLoad(),
                         strategy->hasEfficientVBDeletion());
    return rv;
}

void StrategicSqlite3::addStats(const std::string &prefix,
                             ADD_STAT add_stat, const void *c) {
    if (prefix != "rw") {
        return;
    }

    SQLiteStats &st(strategy->sqliteStats);
    add_casted_stat("sector_size", st.sectorSize, add_stat, c);
    add_casted_stat("open", st.numOpen, add_stat, c);
    add_casted_stat("close", st.numClose, add_stat, c);
    add_casted_stat("lock", st.numLocks, add_stat, c);
    add_casted_stat("truncate", st.numTruncates, add_stat, c);
}


void StrategicSqlite3::addTimingStats(const std::string &prefix,
                                    ADD_STAT add_stat, const void *c) {
    if (prefix != "rw") {
        return;
    }

    SQLiteStats &st(strategy->sqliteStats);
    add_casted_stat("delete", st.deleteHisto, add_stat, c);
    add_casted_stat("sync", st.syncTimeHisto, add_stat, c);
    add_casted_stat("readTime", st.readTimeHisto, add_stat, c);
    add_casted_stat("readSeek", st.readSeekHisto, add_stat, c);
    add_casted_stat("readSize", st.readSizeHisto, add_stat, c);
    add_casted_stat("writeTime", st.writeTimeHisto, add_stat, c);
    add_casted_stat("writeSeek", st.writeSeekHisto, add_stat, c);
    add_casted_stat("writeSize", st.writeSizeHisto, add_stat, c);
}
