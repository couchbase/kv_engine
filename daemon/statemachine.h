/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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

class Connection;

typedef bool (* TaskFunction)(Connection* connection);

/**
 * The state machine is a virtual class representing the various states
 * a connection object may be in. Currently we only have a single Connection
 * type (and all of them is using the Memcached Binary Protocol (MCBP).
 *
 * At some point the Connection class will get refactored to multiple classes
 * (Server, MCBP and Greenstack) which will use their own state subclass
 * of the StateMachine that enforce the legal state transitions).
 *
 * @todo refactor: rename to ConnectionStateMachine
 */
class StateMachine {
public:
    StateMachine(TaskFunction task)
        : currentTask(task) {

    }

    /**
     * Execute the current task function
     *
     * @param connection the associated connection
     * @return the return value from the task function (true to continue
     *         execution, false otherwise)
     */
    bool execute(Connection& connection) {
        return currentTask(&connection);
    }

    /**
     * Get the current task function (for debug purposes)
     */
    TaskFunction getCurrentTask() const {
        return currentTask;
    }

    /**
     * Get the name of the current task (for debug purposes)
     */
    const char* getCurrentTaskName() const {
        return getTaskName(currentTask);
    }

    /**
     * Set the current task function
     *
     * This validates the requested state transition is valid.
     *
     * @param connection the associated connection
     * @param task the new task function
     * @throws std::logical_error for illegal state transitions
     */
    virtual void setCurrentTask(Connection& connection, TaskFunction task) = 0;

    /**
     * Get the name of a given task
     */
    virtual const char* getTaskName(TaskFunction task) const = 0;

protected:
    TaskFunction currentTask;
};

