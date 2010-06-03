/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include <string.h>
#include <cstdlib>

#include "sqlite-kvstore.hh"
#include "sqlite-pst.hh"

void StrategicSqlite3::set(const Item &itm, Callback<bool> &cb) {
    PreparedStatement *ins_stmt = strategy->forKey(itm.getKey())->ins();
    ins_stmt->bind(1, itm.getKey().c_str());
    ins_stmt->bind(2, const_cast<Item&>(itm).getData(), itm.getNBytes());
    ins_stmt->bind(3, itm.getFlags());
    ins_stmt->bind(4, itm.getExptime());
    ins_stmt->bind64(5, itm.getCas());
    bool rv = ins_stmt->execute() == 1;
    cb.callback(rv);
    ins_stmt->reset();
}

void StrategicSqlite3::get(const std::string &key, uint16_t vbucket,
                           Callback<GetValue> &cb) {
    // TODO:  Use vbucket.
    (void)vbucket;
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

void StrategicSqlite3::del(const std::string &key, uint16_t vbucket,
                           Callback<bool> &cb) {
    // TODO:  Use vbucket
    (void)vbucket;

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
            // TODO:  Grab vbucket
            GetValue rv(new Item(st->column_blob(0),
                                 static_cast<uint16_t>(st->column_bytes(0)),
                                 st->column_int(2),
                                 st->column_int(3),
                                 st->column_blob(1),
                                 st->column_bytes(1)));
            cb.callback(rv);
        }

        st->reset();
    }
}
