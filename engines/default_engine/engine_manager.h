/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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

/**
 * Engine manager provides a C API for the managment of default_engine
 * 'handles'. Creation / Deletion and the item scrubber thread are all
 * managed by this module.
 */

#ifdef __cplusplus
#include <condition_variable>
#include <mutex>
#include <unordered_set>

#include "scrubber_task.h"

class EngineManager {
public:

    EngineManager();
    ~EngineManager();

    /**
     * Create a new instance of the default_engine
     *
     * @return the newly created instance or nullptr if we ran out of
     *         resources (memory _or_ bucket id's)
     */
    struct default_engine* createEngine();

    /**
     * Request that the scrubber destroy's this engine.
     * Scrubber will delete the object.
     */
    void requestDestroyEngine(struct default_engine* engine);

    /**
     *  Request that the engine is scrubbed.
     */
    void scrubEngine(struct default_engine* engine);

    /**
     * Set the shutdown flag so that we can clean up
     *    1) no new engine's can be created.
     *    2) the scrubber can be notified to exit and joined.
     */
    void shutdown();

    /**
     * When the scrubber is done running the requested scrub task it
     * will call this callback notifying that it is done.
     *
     * @param engine the requested engine
     * @param destroy set to true if the engine should be destroyed
     */
    void notifyScrubComplete(struct default_engine* engine, bool destroy);

protected:
    /**
     * Wait for the scrubber task to be idle. You <b>must</b> hold the
     * mutex while calling this function.
     */
    void waitForScrubberToBeIdle(std::unique_lock<std::mutex>& lck);

private:
    /** single mutex protects all members */
    std::mutex lock;

    /** condition variable used from the scrubber to notify the engine */
    std::condition_variable cond;

    /** Handle to the scrubber task being used to preform the operations */
    ScrubberTask scrubberTask;

    /** Are we currently shutting down? (Note: We should refactor the clients
     * using the class to ensure that this isn't a problem. Given that we can't
     * restart the task it doesn't really make any sense if we have a race
     * with one client trying to delete a bucket, and another one trying to
     * shut down the object...
     */
    bool shuttingdown;

    /** Handle of all of the instances created of default engine */
    std::unordered_set<struct default_engine*> engines;
};

extern "C" {
#endif

/*
 * Create a new engine instance.
 * Returns NULL for failure.
 */
struct default_engine* engine_manager_create_engine();

/*
 * Delete the engine instance.
 * Deletion is performed by a background thread.
 * On return from this method the caller must not use the pointer as
 * it will be deleted at any time.
 */
void engine_manager_delete_engine(struct default_engine* engine);

/*
 * Request that a scrub of the engine is performed.
 * Scrub is perfromed by a background thread.
 */
void engine_manager_scrub_engine(struct default_engine* engine);

/*
 * Perform global shutdown in prepration for unloading of the shared object.
 * This method will block until background threads are joined.
 */
void engine_manager_shutdown();

#ifdef __cplusplus
}
#endif
