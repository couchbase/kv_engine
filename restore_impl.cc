/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2011 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
#include "config.h"
#include "ep_engine.h"
#include "restore.hh"

#ifdef USE_SYSTEM_LIBSQLITE3
#include <sqlite3.h>
#else
#include "embedded/sqlite3.h"
#endif

static const char *query =
    "select cpoint_op.vbucket_id,op,key,flg,exp,cas,val,cpoint_op.cpoint_id "
    "from cpoint_state "
    "  join cpoint_op on (cpoint_op.vbucket_id = cpoint_state.vbucket_id and"
    "                     cpoint_op.cpoint_id = cpoint_state.cpoint_id) "
    "where cpoint_state.state = \"closed\" "
    "order by cpoint_op.cpoint_id desc";

static const int vbucket_id_idx = 0;
static const int op_idx = 1;
static const int key_idx = 2;
static const int flag_idx = 3;
static const int exp_idx = 4;
static const int cas_idx = 5;
static const int val_idx = 6;
static const int cpoint_idx = 7;

extern "C" {
    static void *restoreThreadMain(void *arg);
}

/**
 * A helper class to track the state of the restore manager
 */
class State {
public:
    State(const char *s) : name(s) {}
    const std::string &toString() const {
        return name;
    }

    static const State Uninitialized;
    static const State Initialized;
    static const State Starting;
    static const State Running;
    static const State Zombie;

private:
    const std::string name;
};

const State State::Uninitialized("uninitialized");
const State State::Initialized("initialized");
const State State::Starting("starting");
const State State::Running("running");
const State State::Zombie("zombie");

/**
 * Hehe.. Since we're applying all of the incremental backups in the
 * let's let the name reflect that ;-)
 *
 * The DecrementalRestorer is responsible for processing a single
 * incremental restore file and add all of it's content to epengine.
 *
 */
class DecrementalRestorer {
public:
    /**
     * Create a new instance of DecrementalRestorer and initialize
     * its member variable.
     * @param theEngine where to restore the data
     * @param dbname the name of the incremental database to restore
     */
    DecrementalRestorer(EventuallyPersistentEngine &theEngine,
                        const std::string &dbname) :
        db(NULL), statement(NULL), engine(theEngine),
        store(*engine.getEpStore()), file(dbname),
        expired(0), wrongVBucket(0), restored(0), skipped(0), busy(0), restore_cpoint(0)
    {
        // None needed
    }

    /**
     * Release all allocated resources. We might have the database in an
     * open state if an exception is thrown during restore.
     */
    ~DecrementalRestorer() {
        if (db != NULL) {
            (void)sqlite3_finalize(statement);
            (void)sqlite3_close(db);
        }
    }

    const std::string &getDbFile() const
    {
        return file;
    }

    uint32_t getNumBusy() const {
        return busy;
    }

    uint32_t getRestoreCheckpoint() const {
        return restore_cpoint;
    }

    uint32_t getNumRestored() const {
        return restored;
    }

    uint32_t getNumSkipped() const {
        return skipped;
    }

    uint32_t getNumWrongVBucket() const {
        return wrongVBucket;
    }

    uint32_t getNumExpired() const {
        return expired;
    }

    /**
     * Process this database file
     * @throw a string describing why an error occured
     */
    void process() throw (std::string) {
        if (sqlite3_open(file.c_str(), &db) !=  SQLITE_OK) {
            db = NULL;
            throw std::string("Failed to open database");
        }

        if (sqlite3_prepare_v2(db, query,
                               strlen(query),
                               &statement, NULL) != SQLITE_OK) {
            (void)sqlite3_finalize(statement);
            (void)sqlite3_close(db);
            db = NULL;
            throw std::string("Failed to prepare statement");
        }

        int rc;
        while ((rc = sqlite3_step(statement)) != SQLITE_DONE) {
            if (rc == SQLITE_ROW) {
                processEntry();
            } else if (rc == SQLITE_BUSY) {
                ++busy;
            } else {
                std::stringstream ss;
                ss << "sqlite error: " << sqlite3_errmsg(db);
                throw std::string(ss.str());
            }
        }

        (void)sqlite3_finalize(statement);
        (void)sqlite3_close(db);
        db = NULL;
    }

private:

    /**
     * callback to process the current item
     */
    void processEntry() throw(std::string) {
        uint32_t exptime = sqlite3_column_int(statement, exp_idx);
        if (exptime != 0 && exptime <  static_cast<int64_t>(ep_real_time())) {
            ++expired;
            return ;
        }
        std::string key((const char*)sqlite3_column_text(statement, key_idx),
                        sqlite3_column_bytes(statement, key_idx));

        enum queue_operation op = queue_op_set;
        if ((sqlite3_column_bytes(statement, op_idx) > 0) &&
            sqlite3_column_text(statement, op_idx)[0] == 'd') {
            op = queue_op_del;
        }

        value_t value(Blob::New((const char*)sqlite3_column_text(statement,
                                                                 val_idx),
                                sqlite3_column_bytes(statement, val_idx)));

        uint16_t vbid =  (uint16_t)sqlite3_column_int(statement,
                                                      vbucket_id_idx);
        if (!restore_cpoint) {
            restore_cpoint = (uint32_t)sqlite3_column_int(statement,
                                                            cpoint_idx);
        }

        int r = store.addUnlessThere(key, vbid, op, value,
                                     sqlite3_column_int(statement, flag_idx),
                                     sqlite3_column_int(statement, exp_idx),
                                     sqlite3_column_int(statement, cas_idx));
        if (r == 0) {
            ++restored;
        } else if (r == 1) {
            ++skipped;
        } else {
            ++wrongVBucket;
        }
    }

    sqlite3 *db;
    sqlite3_stmt *statement;
    EventuallyPersistentEngine &engine;
    EventuallyPersistentStore &store;
    const std::string file;
    uint32_t expired;
    uint32_t wrongVBucket;
    uint32_t restored;
    uint32_t skipped;
    uint32_t busy;
    uint32_t restore_cpoint;
};

class RestoreManagerImpl : public RestoreManager {
public:
    RestoreManagerImpl(EventuallyPersistentEngine &theEngine) :
        RestoreManager(theEngine),
        instance(NULL),
        expired(0),
        wrongVBucket(0),
        restored(0),
        skipped(0),
        busy(0),
        restore_cpoint(0),
        state(&State::Uninitialized)
    {
        // None needed
    }

    virtual void initialize(const std::string &config) throw(std::string)
    {
        if (access(config.c_str(), F_OK) != 0) {
            throw std::string("File not found");
        }

        LockHolder lh(mutex);
        errorMsg.resize(0);
        if (state != &State::Uninitialized) {
            // Only allow the zombie state, because then we can just reap it..
            if (state != &State::Zombie) {
                throw std::string("restorer isn't idle!");
            }
            // reap the zombie!
            reap_UNLOCKED();
        }

        assert(instance == NULL);
        instance = new DecrementalRestorer(engine, config);
        setState_UNLOCKED(State::Initialized);
    }

    virtual void start() throw (std::string)
    {
        LockHolder lh(mutex);
        if (instance == NULL) {
            lh.unlock();
            throw std::string("you need to call initialize before start");
        }

        if (state != &State::Initialized) {
            lh.unlock();
            throw std::string("Restore already running");
        }

        state = &State::Starting;
        int ret = pthread_create(&thread, NULL, restoreThreadMain, this);
        if (ret != 0) {
            state = &State::Uninitialized;
            collectResults();
            delete instance;
            instance = NULL;
            lh.unlock();
            std::stringstream ss;
            ss << "Failed to create restore thread: " << strerror(ret);
            throw ss.str();
        }
    }

    virtual void abort() throw (std::string)
    {
        LockHolder lh(mutex);
        terminate = true;
    }

    virtual void wait() throw (std::string)
    {
        LockHolder lh(mutex);
        if (state != &State::Initialized && state != &State::Uninitialized) {
            reap_UNLOCKED();
        }
    }

    virtual void stats(const void* cookie, ADD_STAT add_stat)
    {
        RestoreManager::stats(cookie, add_stat);
        addStat(cookie, "engine", "RestoreManagerImpl", add_stat);

        LockHolder lh(mutex);
        addStat(cookie, "state", state->toString(), add_stat);

        if (errorMsg.length() > 0) {
            addStat(cookie, "last_error", errorMsg, add_stat);
        }

        if (instance == NULL) {
            addStat(cookie, "restore_checkpoint", restore_cpoint, add_stat);
            addStat(cookie, "number_busy", busy, add_stat);
            addStat(cookie, "number_skipped", skipped, add_stat);
            addStat(cookie, "number_restored", restored, add_stat);
            addStat(cookie, "number_expired", expired, add_stat);
            addStat(cookie, "number_wrong_vbucket", wrongVBucket, add_stat);
        } else {
            addStat(cookie, "restore_checkpoint", restore_cpoint ? restore_cpoint :
                                        instance->getRestoreCheckpoint(), add_stat);
            addStat(cookie, "file", instance->getDbFile(), add_stat);
            addStat(cookie, "number_busy", instance->getNumBusy() + busy,
                    add_stat);
            addStat(cookie, "number_skipped", instance->getNumSkipped() + skipped,
                    add_stat);
            addStat(cookie, "number_restored", instance->getNumRestored() + restored,
                    add_stat);
            addStat(cookie, "number_expired",
                    instance->getNumExpired() + expired, add_stat);
            addStat(cookie, "number_wrong_vbucket",
                    instance->getNumWrongVBucket() + wrongVBucket, add_stat);
            addStat(cookie, "terminate", terminate, add_stat);
        }
    }

    virtual bool isRunning() {
        LockHolder lh(mutex);
        return (state == &State::Starting ||
                state == &State::Running);
    }

    virtual ~RestoreManagerImpl() {
        wait();
    }

    void *run(void) {
        ObjectRegistry::onSwitchThread(&engine);
        setState(State::Running);
        try {
            instance->process();
        } catch (std::string message) {
            LockHolder lh(mutex);
            errorMsg.assign(message);
        }
        setState(State::Zombie);
        return NULL;
    }

private:
    void collectResults() {
        skipped += instance->getNumSkipped();
        busy += instance->getNumBusy();
        restored += instance->getNumRestored();
        if (!restore_cpoint) {
            restore_cpoint = instance->getRestoreCheckpoint();
        }
    }

    void reap_UNLOCKED() throw (std::string) {
        if (instance != NULL) {
            void *rcode;
            int ret = pthread_join(thread, &rcode);
            if (ret != 0 && ret != ESRCH) {
                std::stringstream ss;
                ss << "Failed to join restore thread: " << strerror(ret);
                throw ss.str();
            }
            collectResults();
            delete instance;
            instance = NULL;
            setState_UNLOCKED(State::Uninitialized);
        }
    }

    void setState_UNLOCKED(const State &s) {
        state = &s;
    }

    void setState(const State &s) {
        LockHolder lh(mutex);
        setState_UNLOCKED(s);
    }

    // Access to the variables in here are all protected by a single mutex.
    // I know this doesn't scale much, but if you're having performance
    // problems you should stop calling stats all of the times ;-)
    Mutex mutex;
    DecrementalRestorer *instance;
    std::string errorMsg;
    uint32_t expired;
    uint32_t wrongVBucket;
    uint32_t restored;
    uint32_t skipped;
    uint32_t busy;
    uint32_t restore_cpoint;

    // should we abort or not?
    Atomic<bool> terminate;

    const State *state;
    // The thread running the backup
    pthread_t thread;
};

RestoreManager* create_restore_manager(EventuallyPersistentEngine &engine)
{
    return new RestoreManagerImpl(engine);
}

void destroy_restore_manager(RestoreManager *manager)
{
    delete manager;
}

static void *restoreThreadMain(void *arg)
{
    RestoreManagerImpl *instance;
    instance = reinterpret_cast<RestoreManagerImpl*>(arg);
    return instance->run();
}

