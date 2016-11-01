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

#include <condition_variable>
#include <deque>
#include <mutex>
#include <platform/platform.h>

class EngineManager;

/**
 * The scrubber task is charged with
 *   1. removing items from memory
 *   2. deleting engine structs
 *
 * The common use-case is for bucket deletion performing tasks 1 and 2.
 * The start_scrub command only performs 1.
 *
 * Global destruction can safely join the task and allow the engine to
 * safely unload the shared object.
 */
class ScrubberTask {
public:
    ScrubberTask(EngineManager& manager);

    /**
     *  Shutdown the task
     */
    void shutdown();

    /**
     *  Join the thread running the scrubber (to be called after shutdown).
     */
    void joinThread();

    /**
     *  Place the engine on the threads work queue for item scrubbing.
     *  bool destroy indicates if the engine should be deleted once scrubbed.
     */
    void placeOnWorkQueue(struct default_engine* engine, bool destroy);

    /**
     * Task's run loop method. This is not a public function and should only
     * be called from the tasks constructor.
     */
    void run();

    /**
     * Is the scrubber idle and have no pending work scheduled
     *
     * @return true if no work has been scheduled and its work queue is empty
     */
    bool isIdle() {
        std::lock_guard<std::mutex> guard(lock);
        return (workQueue.empty() && state == State::Idle);
    }

private:
    enum class State {
        /// The scrubber is currently in the waiting state
        Idle,
        /// The scrubber is currently scrubbing a list
        Scrubbing,
        /// The scrubber task is stopped (returning from main)
        Stopped
    };

    /** What is the scrubber currently doing */
    State state;

    /*
     * A queue of engine's to work on.
     * The second bool indicates if the engine is to be deleted when done.
     */
    std::deque<std::pair<struct default_engine*, bool> > workQueue;

    /** Is the task being requested to shut down? */
    bool shuttingdown;

    /** The manager owning us */
    EngineManager& engineManager;

    /** All internal state is protected by this mutex */
    std::mutex lock;

    /**
     * The condition variable used to notify the task that it has work
     * to do.
     */
    std::condition_variable cvar;

    /**
     * The identifier to the thread handle
     */
    cb_thread_t scrubberThread;
};
