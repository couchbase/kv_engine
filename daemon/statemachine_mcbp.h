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

class McbpConnection;

typedef bool (* TaskFunction)(McbpConnection* connection);

/**
 * The state machinery for the Memcached Binary Protocol.
 */
class McbpStateMachine {
public:
    McbpStateMachine() = delete;

    McbpStateMachine(TaskFunction task)
        : currentTask(task) {

    }

    /**
     * Execute the current task function
     *
     * @param connection the associated connection
     * @return the return value from the task function (true to continue
     *         execution, false otherwise)
     */
    bool execute(McbpConnection& connection) {
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
    void setCurrentTask(McbpConnection& connection, TaskFunction task);

    /**
     * Get the name of a given task
     */
    const char* getTaskName(TaskFunction task) const;


protected:
    TaskFunction currentTask;
};

bool conn_new_cmd(McbpConnection* c);
bool conn_waiting(McbpConnection* c);
bool conn_read(McbpConnection* c);
bool conn_parse_cmd(McbpConnection* c);
bool conn_write(McbpConnection* c);
bool conn_nread(McbpConnection* c);
bool conn_pending_close(McbpConnection* c);
bool conn_immediate_close(McbpConnection* c);
bool conn_closing(McbpConnection* c);
bool conn_destroyed(McbpConnection* c);
bool conn_mwrite(McbpConnection* c);
bool conn_ship_log(McbpConnection* c);
bool conn_setup_tap_stream(McbpConnection* c);
bool conn_refresh_cbsasl(McbpConnection* c);
bool conn_refresh_ssl_certs(McbpConnection* c);
bool conn_flush(McbpConnection* c);
bool conn_audit_configuring(McbpConnection* c);
bool conn_create_bucket(McbpConnection* c);
bool conn_delete_bucket(McbpConnection* c);
