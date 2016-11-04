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

/*
 * Engine manager provides methods for creating and deleting of engine
 * handles/structs and the creation and safe teardown of the scrubber thread.
 *
 *  Note: A single scrubber exists for the purposes of running a user requested
 *  scrub and for background deletion of bucket items when a bucket is
 *  destroyed.
 */

#include "engine_manager.h"
#include "default_engine_internal.h"

#include <chrono>
#include <memory>

static std::unique_ptr<EngineManager> engineManager;

EngineManager::EngineManager()
  : scrubberTask(*this),
    shuttingdown(false) {}

EngineManager::~EngineManager() {
    shutdown();
}

struct default_engine* EngineManager::createEngine() {
    std::lock_guard<std::mutex> lck(lock);
    if (shuttingdown) {
        return nullptr;
    }

    try {
        static bucket_id_t bucket_id;

        struct default_engine* newEngine = new struct default_engine();
        if (bucket_id + 1 == 0) {
            // We've used all of the available id's
            delete newEngine;
            return nullptr;
        }
        default_engine_constructor(newEngine, bucket_id++);
        engines.insert(newEngine);

        return newEngine;
    } catch (const std::bad_alloc&) {
        return nullptr;
    }
}

void EngineManager::requestDestroyEngine(struct default_engine* engine) {
    std::lock_guard<std::mutex> lck(lock);
    if (!shuttingdown) {
        scrubberTask.placeOnWorkQueue(engine, true);
    }
}

void EngineManager::scrubEngine(struct default_engine* engine) {
    std::lock_guard<std::mutex> lck(lock);
    if (!shuttingdown) {
        scrubberTask.placeOnWorkQueue(engine, false);
    }
}

void EngineManager::waitForScrubberToBeIdle(std::unique_lock<std::mutex>& lck) {
    if (!lck.owns_lock()) {
        throw std::logic_error("EngineManager::waitForScrubberToBeIdle: Lock must be held");
    }

    while (!scrubberTask.isIdle()) {
        auto& task = scrubberTask;
        // There is a race for the isIdle call, and I don't want to solve it
        // by using a mutex as that would result in the use of trying to
        // acquire multiple locks (which is a highway to deadlocks ;-)
        //
        // The scrubber does *not* hold the for the scrubber while calling
        // notify on this condition variable.. And the state is then Scrubbing
        // That means that this thread will wake, grab the mutex and check
        // the state which is still Scrubbing and go back to sleep (depending
        // on the scheduling order)..
        cond.wait_for(lck,
                      std::chrono::milliseconds(10),
                      [&task] {
            return task.isIdle();
        });
    }
}

/*
 * Join the scrubber and delete any data which wasn't cleaned by clients
 */
void EngineManager::shutdown() {
    std::unique_lock<std::mutex> lck(lock);
    if (!shuttingdown) {
        shuttingdown = true;

        // Wait until the scrubber is done with all of its tasks
        waitForScrubberToBeIdle(lck);

        // Do we have any engines defined?
        if (!engines.empty()) {
            // Tell it to go ahead and scrub all engines
            for (auto engine : engines) {
                scrubberTask.placeOnWorkQueue(engine, true);
            }

            // Wait for all of the engines to be deleted
            auto& set = engines;
            cond.wait(lck, [&set] {
                return set.empty();
            });

            // wait for the scrubber to become idle again
            waitForScrubberToBeIdle(lck);
        }

        scrubberTask.shutdown();
        scrubberTask.joinThread();
    }
}

void EngineManager::notifyScrubComplete(struct default_engine* engine,
                                        bool destroy) {
    if (destroy) {
        destroy_engine_instance(engine);
    }

    std::lock_guard<std::mutex> lck(lock);
    if (destroy) {
        engines.erase(engine);
        delete engine;
    }

    cond.notify_one();
}

EngineManager& getEngineManager() {
    static std::mutex createLock;
    if (engineManager.get() == nullptr) {
        std::lock_guard<std::mutex> lg(createLock);
        if (engineManager.get() == nullptr) {
            engineManager.reset(new EngineManager());
        }
    }
    return *engineManager.get();
}

// C API methods follow.
struct default_engine* engine_manager_create_engine() {
    return getEngineManager().createEngine();
}

void engine_manager_delete_engine(struct default_engine* engine) {
    getEngineManager().requestDestroyEngine(engine);
}

void engine_manager_scrub_engine(struct default_engine* engine) {
    getEngineManager().scrubEngine(engine);
}

void engine_manager_shutdown() {
    // will block waiting for scrubber to finish
    // Note that it would be tempting to just call reset on the unique_ptr,
    // but then we could recreate the object on accident by calling
    // one of the other functions which in turn call getEngineManager()
    getEngineManager().shutdown();
}
