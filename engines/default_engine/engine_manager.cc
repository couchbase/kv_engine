/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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
    Engine manager provides methods for creating and deleting of engine handles/structs
    and the creation and safe teardown of the scrubber thread.

    Note: A single scrubber exists for the purposes of running a user requested scrub
    and for background deletion of bucket items when a bucket is destroyed.
*/

#include "engine_manager.h"
#include "default_engine_internal.h"
#include "items.h"


#include <atomic>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <string>
#include <unordered_set>
#include <memory>

static std::unique_ptr<EngineManager> engineManager;

EngineManager::EngineManager()
  : scrubberTask(this),
    shuttingdown(false) {}

EngineManager::~EngineManager() {
    shutdown();
}

struct default_engine* EngineManager::createEngine() {
    struct default_engine* newEngine = NULL;

    if (!shuttingdown) {
        newEngine = new struct default_engine();
    }

    if (newEngine) {
        std::lock_guard<std::mutex> lck(lock);
        static bucket_id_t bucket_id;
        if (bucket_id + 1 == 0) {
            // We've used all of the available id's
            delete newEngine;
            return nullptr;
        }
        default_engine_constructor(newEngine, bucket_id++);
        engines.insert(newEngine);
    }

    return newEngine;
}

void EngineManager::deleteEngine(struct default_engine* engine) {
    std::lock_guard<std::mutex> lck(lock);
    engines.erase(engine);
    delete engine;
}

void EngineManager::requestDestroyEngine(struct default_engine* engine) {
    if (!shuttingdown) {
        scrubberTask.placeOnWorkQueue(engine, true);
    }
}

void EngineManager::scrubEngine(struct default_engine* engine) {
    if (!shuttingdown) {
        scrubberTask.placeOnWorkQueue(engine, false);
    }
}

/*
 * Join the scrubber and delete any data which wasn't cleaned by clients
 */
void EngineManager::shutdown() {
    shuttingdown = true;
    scrubberTask.shutdown();
    scrubberTask.joinThread();
    std::lock_guard<std::mutex> lck(lock);
    for (auto engine : engines) {
        delete engine;
    }
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
    getEngineManager().shutdown();
}
