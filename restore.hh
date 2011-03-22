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
#ifndef RESTORE_HH
#define RESTORE_HH 1

#include <string>
#include <sstream>
#include <memcached/engine.h>

class EventuallyPersistentEngine;

/**
 * The restore manager is responsible for running the restore of a set of
 * incremental backup files.
 */
class RestoreManager {
public:
    /**
     * Initialize a new instance of the Restore Manager.
     * @param theEngine the engine to restore the data into
     */
    RestoreManager(EventuallyPersistentEngine &theEngine) : engine(theEngine) {}

    /**
     * Parse the configuration and intialize the instance.
     * An exception is thrown if there is something wrong with the configuration
     * @param config what to initialize the restore manager with (this could be
     *               a pointer to a file or whatever.. it's up to the implementation
     *               to define)
     * @throws a string describing the error if it failed to intitalize itself
     */
    virtual void initialize(const std::string &config) throw(std::string) = 0;

    /**
     * Start the restore. The restore may use its own threads or utilize the
     * scheduler. that's up to the implementation to decide. This method
     * should start the restore and return immediately (not wait for the
     * restore to complete).
     *
     * @throws a string describing why we failed to start the restore
     */
    virtual void start() throw (std::string) = 0;

    /**
     * Terminate the restore process as soon as possible. This method should
     * <b>not</b> wait for the abortion to complete, but return to the caller
     * immediately.
     *
     * @throws a string describing why we failed to abort the restore
     */
    virtual void abort() throw (std::string) = 0;

    /**
     * Wait for the restore process to stop (after a successfull completion or
     * an abort).
     *
     * @throws a string describing why we aren't waiting for the restore to
     *         shut down
     */
    virtual void wait() throw (std::string) = 0;

    /**
     * Get statistics from the restore
     * @param cookie provided by the core
     * @param add_stat callback to the core
     */
    virtual void stats(const void* /* cookie */, ADD_STAT /* add_stat */) {}

    virtual bool isRunning() = 0;


    /**
     * Release all allocated resources.
     */
    virtual ~RestoreManager() {}

protected:

    template <typename T>
    void addStat(const void *cookie, const char *nm, T val, ADD_STAT add_stat) {
        std::stringstream name;
        name << "ep_restore:" << nm;
        std::stringstream value;
        value << val;
        std::string n = name.str();
        add_stat(n.data(), static_cast<uint16_t>(n.length()),
                 value.str().data(), static_cast<uint32_t>(value.str().length()),
                 cookie);
    }

    void addStat(const char *nm, bool val, ADD_STAT add_stat, const void *c) {
        addStat(nm, val ? "true" : "false", add_stat, c);
    }

    EventuallyPersistentEngine &engine;
};

extern "C" {
    /**
     * Create a new restore manager to restore data into the specified
     * engine. The restore manager should be returned in an uninitialized
     * state, so the only thing that may make the method call is an out
     * of memory error. The user should call initialize() on the returning
     * object.
     *
     * The primary reason for creating this the hard way with an extern "C"
     * linkage and not throw an exception, is that I want to be able to
     * put it into a separate loadable module.
     *
     * @param engine the engine to restore the data into
     * @return a pointer to the restore manager
     */
    RestoreManager* create_restore_manager(EventuallyPersistentEngine &engine);

    /**
     * Destroy the instance of the restore manager.
     * @param restoreManager the manager to destroy.
     */
    void destroy_restore_manager(RestoreManager *restoreManager);
}

#endif
