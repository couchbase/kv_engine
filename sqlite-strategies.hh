/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef SQLITE_STRATEGIES_H
#define SQLITE_STRATEGIES_H 1

#include <vector>

#include "common.hh"
#include "sqlite-pst.hh"

class SqliteStrategy {
public:

    SqliteStrategy(const char * const fn) : filename(fn),
                                            db(NULL),
                                            statements(NULL)
    { }

    virtual ~SqliteStrategy() {
        close();
    }

    const std::vector<Statements *> &allStatements() {
        return statements;
    }

    Statements *forKey(const std::string &key) {
        assert(statements.size() > 0);
        int h=5381;
        int i=0;
        const char *str = key.c_str();

        for(i=0; str[i] != 0x00; i++) {
            h = ((h << 5) + h) ^ str[i];
        }

        return statements.at(std::abs(h) % (int)statements.size());
    }

    virtual void initTables(void);
    virtual void initStatements(void);
    virtual void destroyTables(void);
    virtual void initPragmas(void);
    void destroyStatements(void);

    void execute(const char * const query);

    sqlite3 *open(void);
    void close(void);

private:

    const char * const filename;
    sqlite3 *db;
    std::vector<Statements *> statements;

    DISALLOW_COPY_AND_ASSIGN(SqliteStrategy);
};

#endif /* SQLITE_STRATEGIES_H */
