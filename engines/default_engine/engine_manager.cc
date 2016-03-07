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

/**
    The scrubber task is charged with
     1. removing items from memory
     2. deleting engine structs

    The common use-case is for bucket deletion performing tasks 1 and 2.
    The start_scrub command only performs 1.

    Global destruction can safely join the task and allow the engine to
    safely unload the shared object.
**/
class EngineManager;

class ScrubberTask {
public:
    ScrubberTask(EngineManager* manager);

    /**
        Shutdown the task
    **/
    void shutdown();

    /**
        Join the thread running the scrubber (to be called after shutdown).
    **/
    void joinThread();

    /**
        Place the engine on the threads work queue for item scrubbing.
        bool destroy indicates if the engine should be deleted once scrubbed.
    **/
    void placeOnWorkQueue(struct default_engine* engine, bool destroy);

    /**
        Task's run loop method.
    **/
    void run();

private:
    /*
       A queue of engine's to work on.
       The second bool indicates if the engine is to be deleted when done.
    */
    std::deque<std::pair<struct default_engine*, bool> > workQueue;
    std::atomic<bool> shuttingdown;
    EngineManager* engineManager;
    std::mutex lock;
    std::condition_variable cvar;
    cb_thread_t scrubberThread;
};

/**
    Create/Delete of engines from one location.
    Manages the scrubber task and handles global shutdown
**/
class EngineManager {
public:

    EngineManager();
    ~EngineManager();

    struct default_engine* createEngine();

    /**
        Delete engine struct
    **/
    void deleteEngine(struct default_engine* engine);

    /**
        Request that the scrubber destroy's this engine.
        Scrubber will delete the object.
    **/
    void requestDestroyEngine(struct default_engine* engine);

    /**
        Request that the engine is scrubbed.
    **/
    void scrubEngine(struct default_engine* engine);

    /**
        Set the shutdown flag so that we can clean up
        1) no new engine's can be created.
        2) the scrubber can be notified to exit and joined.
    **/
    void shutdown();

private:
    ScrubberTask scrubberTask;
    std::atomic<bool> shuttingdown;
    std::mutex lock;
    std::unordered_set<struct default_engine*> engines;

};

static std::unique_ptr<EngineManager> engineManager;

static void scrubber_task_main(void* arg) {
    ScrubberTask* task = reinterpret_cast<ScrubberTask*>(arg);
    task->run();
}

ScrubberTask::ScrubberTask(EngineManager* manager)
  : shuttingdown(false),
    engineManager(manager) {
    if (cb_create_named_thread(&scrubberThread, &scrubber_task_main, this, 0,
                               "mc:item_scrub") != 0) {
        throw std::runtime_error("Error creating 'mc:item_scrub' thread");
    }
}

void ScrubberTask::shutdown() {
    shuttingdown = true;
    // Serialize with ::run
    std::lock_guard<std::mutex> lck(lock);
    cvar.notify_one();
}

void ScrubberTask::joinThread() {
    cb_join_thread(scrubberThread);
}

void ScrubberTask::placeOnWorkQueue(struct default_engine* engine, bool destroy) {
    if (!shuttingdown) {
        std::lock_guard<std::mutex> lck(lock);
        engine->scrubber.force_delete = destroy;
        workQueue.push_back(std::make_pair(engine, destroy));
        cvar.notify_one();
    }
}

void ScrubberTask::run() {
    while (true) {
        std::unique_lock<std::mutex> lck(lock);
        if (!workQueue.empty()) {
            auto engine = workQueue.front();
            workQueue.pop_front();
            lck.unlock();

            item_scrubber_main(engine.first);

            if (engine.second) {
                destroy_engine_instance(engine.first);
                engineManager->deleteEngine(engine.first);
            }

            lck.lock(); // relock so lck can safely unlock when destroyed at loop end.
        } else if (!shuttingdown) {
            cvar.wait(lck);
        } else {
            return;
        }
    }
}

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
