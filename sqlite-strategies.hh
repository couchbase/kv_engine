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

protected:
    const char * const filename;
    sqlite3 *db;
    std::vector<Statements *> statements;

private:
    DISALLOW_COPY_AND_ASSIGN(SqliteStrategy);
};

//
// ----------------------------------------------------------------------
// Multi DB strategy
// ----------------------------------------------------------------------
//

class MultiDBSqliteStrategy : public SqliteStrategy {
public:
    MultiDBSqliteStrategy(const char * const fn, int n=4):
        SqliteStrategy(fn),
        numTables(n)
    {}

    void initTables(void);
    void initStatements(void);
    void destroyTables(void);

private:
    int numTables;
};

#endif /* SQLITE_STRATEGIES_H */
