/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <string.h>
#include <cstdlib>

#include "sqlite-kvstore.hh"
#include "sqlite-pst.hh"
#include "ep_engine.h"

StrategicSqlite3::StrategicSqlite3(EventuallyPersistentEngine &theEngine, SqliteStrategy *s) :
    engine(theEngine), stats(engine.getEpStats()), strategy(s),
    intransaction(false) {
    open();
}


int64_t StrategicSqlite3::lastRowId() {
    assert(db);
    return static_cast<int64_t>(sqlite3_last_insert_rowid(db));
}

void StrategicSqlite3::insert(const Item &itm, Callback<std::pair<bool, int64_t> > &cb) {
    assert(itm.getId() <= 0);

    PreparedStatement *ins_stmt = strategy->forKey(itm.getKey())->ins();
    ins_stmt->bind(1, itm.getKey().c_str());
    ins_stmt->bind(2, const_cast<Item&>(itm).getData(), itm.getNBytes());
    ins_stmt->bind(3, itm.getFlags());
    ins_stmt->bind(4, itm.getExptime());
    ins_stmt->bind64(5, itm.getCas());
    ins_stmt->bind(6, itm.getVBucketId());

    ++stats.io_num_write;
    stats.io_write_bytes += itm.getKey().length() + itm.getNBytes();

    bool rv = ins_stmt->execute() == 1;
    if (rv) {
        stats.totalPersisted++;
    }

    int64_t newId = lastRowId();

    std::pair<bool, int64_t> p(rv, newId);
    cb.callback(p);
    ins_stmt->reset();
}

void StrategicSqlite3::update(const Item &itm, Callback<std::pair<bool, int64_t> > &cb) {
    assert(itm.getId() > 0);

    PreparedStatement *upd_stmt = strategy->forKey(itm.getKey())->upd();

    upd_stmt->bind(1, itm.getKey().c_str());
    upd_stmt->bind(2, const_cast<Item&>(itm).getData(), itm.getNBytes());
    upd_stmt->bind(3, itm.getFlags());
    upd_stmt->bind(4, itm.getExptime());
    upd_stmt->bind64(5, itm.getCas());
    upd_stmt->bind64(6, itm.getId());

    bool rv = upd_stmt->execute() == 1;
    if (rv) {
        stats.totalPersisted++;
    }
    ++stats.io_num_write;
    stats.io_write_bytes += itm.getKey().length() + itm.getNBytes();

    std::pair<bool, int64_t> p(rv, 0);
    cb.callback(p);
    upd_stmt->reset();
}

std::map<uint16_t, std::string> StrategicSqlite3::listPersistedVbuckets() {
    std::map<uint16_t, std::string> rv;

    PreparedStatement *st = strategy->getGetVBucketStateST();

    while (st->fetch()) {
        ++stats.io_num_read;
        rv[st->column_int(0)] = st->column(1);
    }

    st->reset();

    return rv;
}

void StrategicSqlite3::set(const Item &itm, Callback<std::pair<bool, int64_t> > &cb) {
    if (itm.getId() <= 0) {
        insert(itm, cb);
    } else {
        update(itm, cb);
    }
}

void StrategicSqlite3::get(const std::string &key,
                           uint64_t rowid, Callback<GetValue> &cb) {
    PreparedStatement *sel_stmt = strategy->forKey(key)->sel();
    sel_stmt->bind64(1, rowid);

    ++stats.io_num_read;

    if(sel_stmt->fetch()) {
        GetValue rv(new Item(key.c_str(),
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
                           Callback<bool> &cb) {
    PreparedStatement *del_stmt = strategy->forKey(key)->del();
    del_stmt->bind64(1, rowid);
    bool rv = del_stmt->execute() >= 0;
    if (rv) {
        stats.totalPersisted++;
    }
    cb.callback(rv);
    del_stmt->reset();
}

bool StrategicSqlite3::delVBucket(uint16_t vbucket) {
    bool rv = true;
    const std::vector<Statements*> statements = strategy->allStatements();
    std::vector<Statements*>::const_iterator it;
    for (it = statements.begin(); it != statements.end(); ++it) {
        PreparedStatement *del_stmt = (*it)->del_vb();
        del_stmt->bind(1, vbucket);
        rv &= del_stmt->execute() >= 0;
        del_stmt->reset();
    }
    PreparedStatement *dst = strategy->getDelVBucketStateST();
    dst->bind(1, vbucket);
    ++stats.io_num_write;
    rv &= dst->execute() >= 0;
    dst->reset();

    return rv;
}

bool StrategicSqlite3::setVBState(uint16_t vbucket, const std::string& state_str) {
    PreparedStatement *st = strategy->getSetVBucketStateST();
    st->bind(1, vbucket);
    st->bind(2, state_str.data(), state_str.length());
    ++stats.io_num_write;
    bool rv = st->execute() >= 0;
    st->reset();
    return rv;
}

void StrategicSqlite3::dump(Callback<GetValue> &cb) {

    const std::vector<Statements*> statements = strategy->allStatements();
    std::vector<Statements*>::const_iterator it;
    for (it = statements.begin(); it != statements.end(); ++it) {
        PreparedStatement *st = (*it)->all();
        st->reset();
        while (st->fetch()) {
            ++stats.io_num_read;
            GetValue rv(new Item(st->column_blob(0),
                                 static_cast<uint16_t>(st->column_bytes(0)),
                                 st->column_int(2),
                                 st->column_int(3),
                                 st->column_blob(1),
                                 st->column_bytes(1),
                                 0,
                                 st->column_int64(6),
                                 static_cast<uint16_t>(st->column_int(5))));
            stats.io_read_bytes += rv.getValue()->getKey().length() + rv.getValue()->getNBytes();
            cb.callback(rv);
        }

        st->reset();
    }
}
