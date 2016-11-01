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
#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <platform/platform.h>

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
