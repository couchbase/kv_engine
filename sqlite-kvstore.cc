/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include <string.h>
#include <cstdlib>

#include "sqlite-kvstore.hh"
#include "sqlite-pst.hh"

int64_t StrategicSqlite3::lastRowId() {
    assert(db);
    return static_cast<int64_t>(sqlite3_last_insert_rowid(db));
}

void StrategicSqlite3::insert(const Item &itm,
                              Callback<std::pair<bool, int64_t> > &cb) {
    assert(itm.getId() <= 0);

    PreparedStatement *ins_stmt = strategy->forKey(itm.getKey())->ins();
    ins_stmt->bind(1, itm.getKey().c_str());
    ins_stmt->bind(2, const_cast<Item&>(itm).getData(), itm.getNBytes());
    ins_stmt->bind(3, itm.getFlags());
    ins_stmt->bind(4, itm.getExptime());
    ins_stmt->bind64(5, itm.getCas());
    bool rv = ins_stmt->execute() == 1;

    int64_t newId = lastRowId();

    std::pair<bool, int64_t> p(rv, newId);
    cb.callback(p);
    ins_stmt->reset();
}

void StrategicSqlite3::update(const Item &itm,
                              Callback<std::pair<bool, int64_t> > &cb) {
    assert(itm.getId() > 0);

    PreparedStatement *upd_stmt = strategy->forKey(itm.getKey())->upd();

    upd_stmt->bind(1, itm.getKey().c_str());
    upd_stmt->bind(2, const_cast<Item&>(itm).getData(), itm.getNBytes());
    upd_stmt->bind(3, itm.getFlags());
    upd_stmt->bind(4, itm.getExptime());
    upd_stmt->bind64(5, itm.getCas());
    upd_stmt->bind64(6, itm.getId());
    bool rv = upd_stmt->execute() == 1;

    std::pair<bool, int64_t> p(rv, 0);
    cb.callback(p);
    upd_stmt->reset();
}

void StrategicSqlite3::set(const Item &itm,
                           Callback<std::pair<bool, int64_t> > &cb) {
    if (itm.getId() <= 0) {
        insert(itm, cb);
    } else {
        update(itm, cb);
    }
}

void StrategicSqlite3::get(const std::string &key, Callback<GetValue> &cb) {
    PreparedStatement *sel_stmt = strategy->forKey(key)->sel();
    sel_stmt->bind(1, key.c_str());

    if(sel_stmt->fetch()) {
        GetValue rv(new Item(key.c_str(),
                             static_cast<uint16_t>(key.length()),
                             sel_stmt->column_int(1),
                             sel_stmt->column_int(2),
                             sel_stmt->column_blob(0),
                             sel_stmt->column_bytes(0)));
        cb.callback(rv);
    } else {
        GetValue rv(false);
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

void StrategicSqlite3::del(const std::string &key, Callback<bool> &cb) {
    PreparedStatement *del_stmt = strategy->forKey(key)->del();
    del_stmt->bind(1, key.c_str());
    bool rv = del_stmt->execute() >= 0;
    cb.callback(rv);
    del_stmt->reset();
}

void StrategicSqlite3::dump(Callback<GetValue> &cb) {

    const std::vector<Statements*> statements = strategy->allStatements();
    std::vector<Statements*>::const_iterator it;
    for (it = statements.begin(); it != statements.end(); ++it) {
        PreparedStatement *st = (*it)->all();
        st->reset();
        while (st->fetch()) {
            GetValue rv(new Item(st->column_blob(0),
                                 static_cast<uint16_t>(st->column_bytes(0)),
                                 st->column_int(2),
                                 st->column_int(3),
                                 st->column_blob(1),
                                 st->column_bytes(1),
                                 0,
                                 st->column_int64(5)));
            cb.callback(rv);
        }

        st->reset();
    }
}
