/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef SQLITE_EVAL_HH
#define SQLITE_EVAL_HH 1

#include <cassert>

#include "embedded/sqlite3.h"

class SqliteEvaluator {
public:

    SqliteEvaluator(sqlite3 *d) : db(d) {
        assert(db);
    }

    void eval(const std::string &filename);

private:

    void execute(std::string &query);
    void trim(std::string &str);

    sqlite3 *db;
};

#endif /* SQLITE_EVAL_HH */
