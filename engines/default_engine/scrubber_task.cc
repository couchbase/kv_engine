/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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
#include "scrubber_task.h"

#include "default_engine_internal.h"
#include "engine_manager.h"

static void scrubber_task_main(void* arg) {
    ScrubberTask* task = reinterpret_cast<ScrubberTask*>(arg);
    task->run();
}

ScrubberTask::ScrubberTask(EngineManager& manager)
    : state(State::Idle),
      shuttingdown(false),
      engineManager(manager) {
    std::unique_lock<std::mutex> lck(lock);
    if (cb_create_named_thread(&scrubberThread, &scrubber_task_main, this, 0,
                               "mc:item scrub") != 0) {
        throw std::runtime_error("Error creating 'mc:item scrub' thread");
    }
}

void ScrubberTask::shutdown() {
    std::unique_lock<std::mutex> lck(lock);
    shuttingdown = true;
    // Serialize with ::run
    cvar.notify_one();
}

void ScrubberTask::joinThread() {
    cb_join_thread(scrubberThread);
}

void ScrubberTask::placeOnWorkQueue(struct default_engine* engine,
                                    bool destroy) {
    std::lock_guard<std::mutex> lck(lock);
    if (!shuttingdown) {
        engine->scrubber.force_delete = destroy;
        workQueue.push_back(std::make_pair(engine, destroy));
        cvar.notify_one();
    }
}

void ScrubberTask::run() {
    std::unique_lock<std::mutex> lck(lock);
    while (!shuttingdown) {
        if (!workQueue.empty()) {
            auto engine = workQueue.front();
            workQueue.pop_front();
            state = State::Scrubbing;
            lck.unlock();
            // Run the task without holding the lock
            item_scrubber_main(engine.first);
            engineManager.notifyScrubComplete(engine.first, engine.second);

            // relock so lck can safely unlock when destroyed at loop end.
            lck.lock();
        } else {
            state = State::Idle;
            cvar.wait(lck);
        }
    }
    state = State::Stopped;
}
