/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include <string.h>
#include <cstdlib>

#include "sqlite-kvstore.hh"

#define MAX_STEPS 10000

PreparedStatement::PreparedStatement(sqlite3 *d, const char *query) {
    db = d;
    if(sqlite3_prepare_v2(db, query, (int)strlen(query), &st, NULL)
       != SQLITE_OK) {
        throw std::runtime_error(sqlite3_errmsg(db));
    }
}

PreparedStatement::~PreparedStatement() {
    sqlite3_finalize(st);
}

void PreparedStatement::bind(int pos, const char *s) {
    bind(pos, s, strlen(s));
}

void PreparedStatement::bind(int pos, const char *s, size_t nbytes) {
    sqlite3_bind_blob(st, pos, s, (int)nbytes, SQLITE_STATIC);
}

void PreparedStatement::bind(int pos, int v) {
    sqlite3_bind_int(st, pos, v);
}

void PreparedStatement::bind64(int pos, uint64_t v) {
    sqlite3_bind_int64(st, pos, v);
}

int PreparedStatement::execute() {
    int steps_run = 0, rc = 0;
    while ((rc = sqlite3_step(st)) != SQLITE_DONE) {
        steps_run++;
        assert(steps_run < MAX_STEPS);
        if (rc == SQLITE_ROW) {
            // This is rather normal
        } else if (rc == SQLITE_BUSY) {
            std::cerr << "SQLITE_BUSY (retrying)" << std::endl;
        } else {
            const char *msg = sqlite3_errmsg(db);
            std::cerr << "sqlite error:  " << msg << std::endl;
            assert(false);
        }
    }
    return sqlite3_changes(db);
}

bool PreparedStatement::fetch() {
    bool rv = true;
    assert(st);
    switch(sqlite3_step(st)) {
    case SQLITE_BUSY:
        throw std::runtime_error("DB was busy.");
        break;
    case SQLITE_ROW:
        break;
    case SQLITE_DONE:
        rv = false;
        break;
    default:
        throw std::runtime_error("Unhandled case.");
    }
    return rv;
}

const char *PreparedStatement::column(int x) {
    return (char*)sqlite3_column_text(st, x);
}

const void *PreparedStatement::column_blob(int x) {
    return (char*)sqlite3_column_text(st, x);
}

int PreparedStatement::column_bytes(int x) {
    return sqlite3_column_bytes(st, x);
}

int PreparedStatement::column_int(int x) {
    return sqlite3_column_int(st, x);
}

uint64_t PreparedStatement::column_int64(int x) {
    return sqlite3_column_int64(st, x);
}

void PreparedStatement::reset() {
    if(sqlite3_reset(st) != SQLITE_OK) {
        throw std::runtime_error("Error resetting statement.");
    }
}

BaseSqlite3::BaseSqlite3(const char *fn) {
    filename = fn;
    db = NULL;
    open();
}

BaseSqlite3::~BaseSqlite3() {
    close();
}

void BaseSqlite3::open() {
    if(!db) {
        if(sqlite3_open(filename, &db) !=  SQLITE_OK) {
            throw std::runtime_error("Error initializing sqlite3");
        }

        if(sqlite3_extended_result_codes(db, 1) != SQLITE_OK) {
            throw std::runtime_error("Error enabling extended RCs");
        }

        intransaction = false;
        initTables();
        initStatements();
    }
}

void BaseSqlite3::close() {
    if(db) {
        intransaction = false;
        destroyStatements();
        sqlite3_close(db);
        db = NULL;
    }
}

void BaseSqlite3::reset() {
    if(db) {
        rollback();
        close();
        open();
        destroyTables();
        initTables();
        execute("vacuum");
    }
}

void BaseSqlite3::begin() {
    if(!intransaction) {
        execute("begin");
        intransaction = true;
    }
}

void BaseSqlite3::commit() {
    if(intransaction) {
        intransaction = false;
        execute("commit");
    }
}

void BaseSqlite3::rollback() {
    if(intransaction) {
        intransaction = false;
        execute("rollback");
    }
}

void BaseSqlite3::execute(const char *query) {
    PreparedStatement st(db, query);
    st.execute();
}


// Sqlite3 naive class.


void Sqlite3::initStatements() {
    ins_stmt = new PreparedStatement(db,
                                     "insert into kv(k, v, flags, exptime, cas) "
                                     "values(?, ?, ?, ?, ?)");
    sel_stmt = new PreparedStatement(db,
                                     "select v, flags, exptime, cas "
                                     "from kv where k = ?");
    del_stmt = new PreparedStatement(db, "delete from kv where k = ?");
}

void Sqlite3::destroyStatements() {
    delete ins_stmt;
    delete sel_stmt;
    delete del_stmt;
    ins_stmt = sel_stmt = del_stmt = NULL;
}

void Sqlite3::initPragmas() {
    execute("pragma page_size = 8192");
    execute("pragma cache_size = 8192");
    execute("pragma journal_mode = TRUNCATE");
    execute("pragma locking_mode = EXCLUSIVE");
    execute("pragma synchronous = NORMAL");
}

void Sqlite3::initTables() {
    execute("create table if not exists kv"
            " (k varchar(250) primary key on conflict replace,"
            "  v text,"
            "  flags integer,"
            "  exptime integer,"
            "  cas integer)");
    if(auditable) {
        execute("create table if not exists history ("
                " id integer primary key autoincrement,"
                " op char(1) not null,"
                " key varchar(250) not null,"
                " value text null)");
        execute("create trigger if not exists on_audit_insert"
                " before insert on kv for each row begin"
                "  insert into history (op,key,value)"
                "         values ('s', new.k, new.v);"
                " end");
        execute("create trigger if not exists on_audit_delete"
                " before delete on kv for each row begin"
                "  insert into history (op,key)"
                "         values ('d', old.k);"
                " end");
    }
}

void Sqlite3::destroyTables() {
    execute("drop table if exists kv");
    execute("drop table if exists history");
    execute("drop trigger if exists on_audit_insert");
    execute("drop trigger if exists on_audit_delete");
}

void Sqlite3::set(const Item &itm, Callback<bool> &cb) {
    ins_stmt->bind(1, itm.getKey().c_str());
    ins_stmt->bind(2, const_cast<Item&>(itm).getData(), itm.nbytes);
    ins_stmt->bind(3, itm.flags);
    ins_stmt->bind(4, itm.exptime);
    ins_stmt->bind64(5, itm.getCas());
    bool rv = ins_stmt->execute() == 1;
    cb.callback(rv);
    ins_stmt->reset();
}

// XXX:  This needs to die.  It's incorrect and not the way forward.
void Sqlite3::get(const std::string &key, Callback<GetValue> &cb) {
    sel_stmt->bind(1, key.c_str());

    if(sel_stmt->fetch()) {
        std::string str(sel_stmt->column(0));
        GetValue rv(new Item(key,
                             sel_stmt->column_int(1),
                             sel_stmt->column_int(2),
                             str));
        cb.callback(rv);
    } else {
        GetValue rv(false);
        cb.callback(rv);
    }
    sel_stmt->reset();
}

void Sqlite3::del(const std::string &key, Callback<bool> &cb) {
    del_stmt->bind(1, key.c_str());
    bool rv = del_stmt->execute() == 1;
    cb.callback(rv);
    del_stmt->reset();
}

void Sqlite3::dump(Callback<GetValue> &cb) {

    PreparedStatement st(db, "select k,v,flags,exptime,cas from kv");
    while (st.fetch()) {
        std::string key(st.column(0));
        std::string value(st.column(1));
        GetValue rv(new Item(key,
                             st.column_int(2),
                             st.column_int(3),
                             value,
                             st.column_int64(4)));
        cb.callback(rv);
    }

    st.reset();
}

// ----------------------------------------------------------------------
// MultiDB class
// ----------------------------------------------------------------------

void MultiTableSqlite3::initStatements() {
    char buf[64];
    for (int i = 0; i < numTables; i++) {
        snprintf(buf, sizeof(buf), "kv_%d", i);
        stmts.push_back(new Statements(db, std::string(buf)));
    }
}

void BaseMultiSqlite3::destroyStatements() {
    std::vector<Statements*>::iterator iter;
    for (iter = stmts.begin(); iter != stmts.end(); iter++) {
        Statements *st = *iter;
        delete st;
    }
}

void MultiTableSqlite3::initTables() {
    char buf[1024];
    for (int i = 0; i < numTables; i++) {
        snprintf(buf, sizeof(buf),
                 "create table if not exists kv_%d"
                 " (k varchar(250) primary key on conflict replace,"
                 "  v text,"
                 "  flags integer,"
                 "  exptime integer,"
                 "  cas integer)", i);
        execute(buf);
    }
}

void MultiTableSqlite3::destroyTables() {
    char buf[64];
    for (int i = 0; i < numTables; i++) {
        snprintf(buf, sizeof(buf), "drop table if exists kv_%d", i);
        execute(buf);
    }
}

Statements* BaseMultiSqlite3::forKey(const std::string &key) {
    int h=5381;
    int i=0;
    const char *str = key.c_str();

    for(i=0; str[i] != 0x00; i++) {
        h = ((h << 5) + h) ^ str[i];
    }

    return stmts[std::abs(h) % (int)stmts.size()];
}

void BaseMultiSqlite3::set(const Item &itm, Callback<bool> &cb) {
    PreparedStatement *ins_stmt = forKey(itm.getKey())->ins();
    ins_stmt->bind(1, itm.getKey().c_str());
    ins_stmt->bind(2, const_cast<Item&>(itm).getData(), itm.nbytes);
    ins_stmt->bind(3, itm.flags);
    ins_stmt->bind(4, itm.exptime);
    ins_stmt->bind64(5, itm.getCas());
    bool rv = ins_stmt->execute() == 1;
    cb.callback(rv);
    ins_stmt->reset();
}

// XXX:  This needs to die.  It's incorrect and not the way forward.
void BaseMultiSqlite3::get(const std::string &key, Callback<GetValue> &cb) {
    PreparedStatement *sel_stmt = forKey(key)->sel();
    sel_stmt->bind(1, key.c_str());

    if(sel_stmt->fetch()) {
        std::string str(sel_stmt->column(0));
        GetValue rv(new Item(key,
                             sel_stmt->column_int(1),
                             sel_stmt->column_int(2),
                             str));
        cb.callback(rv);
    } else {
        GetValue rv(false);
        cb.callback(rv);
    }
    sel_stmt->reset();
}

void BaseMultiSqlite3::del(const std::string &key, Callback<bool> &cb) {
    PreparedStatement *del_stmt = forKey(key)->del();
    del_stmt->bind(1, key.c_str());
    bool rv = del_stmt->execute() == 1;
    cb.callback(rv);
    del_stmt->reset();
}

void MultiTableSqlite3::dump(Callback<GetValue> &cb) {

    char buf[128];
    for (int i = 0; i < numTables; i++) {
        snprintf(buf, sizeof(buf), "select k,v,flags,exptime,cas from kv_%d", i);

        PreparedStatement st(db, buf);
        while (st.fetch()) {
            std::string key(st.column(0));
            std::string value(st.column(1));
            GetValue rv(new Item(key,
                                 st.column_int(2),
                                 st.column_int(3),
                                 value,
                                 st.column_int64(4)));
            cb.callback(rv);
        }

        st.reset();
    }
}

// ----------------------------------------------------------------------
// Multi database sqlite3 implementation
// ----------------------------------------------------------------------

void MultiDBSqlite3::initStatements() {
    char buf[64];
    for (int i = 0; i < numTables; i++) {
        snprintf(buf, sizeof(buf), "kv_%d.kv", i);
        stmts.push_back(new Statements(db, std::string(buf)));
    }
}

void MultiDBSqlite3::initTables() {
    char buf[1024];
    for (int i = 0; i < numTables; i++) {
        snprintf(buf, sizeof(buf), "attach database \"%s-%d.sqlite\" as kv_%d",
                 filename, i, i);
        execute(buf);
        snprintf(buf, sizeof(buf),
                 "create table if not exists kv_%d.kv"
                 " (k varchar(250) primary key on conflict replace,"
                 "  v text,"
                 "  flags integer,"
                 "  exptime integer,"
                 "  cas integer)", i);
        execute(buf);
    }
}

void MultiDBSqlite3::destroyTables() {
    char buf[64];
    for (int i = 0; i < numTables; i++) {
        snprintf(buf, sizeof(buf), "drop table if exists kv_%d.kv", i);
        execute(buf);
    }
}

void MultiDBSqlite3::dump(Callback<GetValue> &cb) {
    char buf[128];
    for (int i = 0; i < numTables; i++) {
        snprintf(buf, sizeof(buf), "select k,v,flags,exptime,cas from kv_%d.kv", i);

        PreparedStatement st(db, buf);
        while (st.fetch()) {
            GetValue rv(new Item(st.column_blob(0),
                                 (uint16_t)st.column_bytes(0),
                                 st.column_int(2),
                                 st.column_int(3),
                                 st.column_blob(1),
                                 st.column_bytes(1),
                                 st.column_int64(4)));
            cb.callback(rv);
        }

        st.reset();
    }
}
