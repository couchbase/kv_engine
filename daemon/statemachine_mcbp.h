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

/**
 * conn_new_cmd is the initial state for a connection object, and the
 * initial state for the processing a new command. It is used to prepare
 * the connection object for handling the next command. It is also the state
 * where the connection object would back off the CPU after servicing n number
 * of requests (to avoid starvation of other connections bound to the same
 * worker thread).
 *
 * possible next state:
 *   * conn_closing - if the bucket is currently being deleted
 *   * conn_parse_cmd - if the input buffer contains the next header
 *   * conn_waiting - we need more data
 *   * conn_ship_log - (for DCP connections)
 *
 * @param c the connection object
 * @return true if the state machinery should continue to process events
 *              for this connection, false if we're done
 */
bool conn_new_cmd(McbpConnection* c);

/**
 * Set up a read event for the connection
 *
 * possible next state:
 *   * conn_closing - if the bucket is currently being deleted
 *   * conn_read_packet_header - the bucket isn't being deleted
 *
 * @param c the connection object
 * @return true if the next state is conn_closing, false otherwise (blocked
 *              waiting for a socket event)
 */
bool conn_waiting(McbpConnection* c);

/**
 * conn_read_packet_header tries to read from the network and fill the
 * input buffer.
 *
 * possible next state:
 *   * conn_closing - if the bucket is currently being deleted
 *   * conn_waiting - if we failed to read more data from the network
 *                    (DCP connections will enter conn_ship_log)
 *   * conn_parse_cmd - if the entire packet header is available
 *
 * @param c the connection object
 * @return true always
 */
bool conn_read_packet_header(McbpConnection* c);

/**
 * conn_parse_cmd parse the command header, and make a host-local copy of
 * the packet header in c->binary_header for convenience
 *
 * possible next state:
 *   * conn_closing - if the bucket is currently being deleted (or protocol
 *                    error)
 *   * conn_read_packet_body - to fetch the rest of the data in the packet
 *   * conn_send_data - if an error occurs and we want to tell the user
 *                      about the error before disconnecting.
 *
 * @param c the connection object
 * @return true if the state machinery should continue to process events
 *              for this connection, false if we're done
 */
bool conn_parse_cmd(McbpConnection* c);

/**
 * Make sure that the entire packet is available
 *
 * possible next state:
 *   * conn_closing - if the bucket is currently being deleted (or protocol
 *                    error)
 *   * conn_execute - the entire packet is available in memory
 *
 * @param c the connection object
 * @return true if the state machinery should continue to process events
 *              for this connection, false if we're done
 */
bool conn_read_packet_body(McbpConnection* c);

/**
 * start closing a connection
 *
 * possible next state:
 *   * conn_immediate_close - no one else is holding a reference to the
 *                            connection
 *   * conn_pending_close - someone is holding a reference to the connection
 *
 * @param c the connection object
 * @return true always
 */
bool conn_closing(McbpConnection* c);

/**
 * Wait until all references to the connection is released
 *
 * possible next state:
 *   * conn_immediate_close - no one else is holding a reference to the
 *                            connection
 *
 * @param c the connection object
 * @return true if all references is gone. false otherwise
 */
bool conn_pending_close(McbpConnection* c);

/**
 * Close sockets and notify everyone with ON_DISCONNECT
 * disassociate the bucket and release all engine allocations
 *
 * next state:
 *    * conn_destroyed
 *
 * @param c the connection object
 * @return false - always
 */
bool conn_immediate_close(McbpConnection* c);

/**
 * The sentinel state for a connection object
 *
 * @param c not used
 * @return false - always
 */
bool conn_destroyed(McbpConnection* c);

/**
 * Execute the current command
 *
 * possible next state:
 *   * conn_closing - if the bucket is currently being deleted (or protocol
 *                    error)
 *   * conn_send_data - If there is data to send
 *   * conn_new_cmd - for "no-reply" commands
 *
 * @param c the connection object
 * @return true if the state machinery should continue to process events
 *              for this connection, false if we're blocked
 */
bool conn_execute(McbpConnection *c);

/**
 * Send data to the client
 *
 * possible next state:
 *   * conn_closing - if the bucket is currently being deleted (or network
 *                    errors)
 *   * conn_new_cmd - All data sent and we should start at the next command
 *   * conn_ship_log - DCP connections
 *
 * @param c the connection object
 * @return true if the state machinery should continue to process events
 *              for this connection, false if we're done
 */
bool conn_send_data(McbpConnection* c);

/**
 * conn_ship_log is the state where the DCP connection end up in the "idle"
 * state to allow it to either process an incomming packet from the other end,
 * or start filling in the data from the underlying engine to ship to the
 * other end
 *
 * possible next state:
 *   * conn_closing - if the bucket is currently being deleted
 *   * conn_read_packet_header - to process input packet
 *   * conn_read_packet_body - to process input packet
 *   * conn_send_data - send the data from the engine
 *
 * @param c the connection object
 * @return true if the state machinery should continue to process events
 *              for this connection, false if we're done
 */
bool conn_ship_log(McbpConnection* c);

/**
 * Special state to park a connection which waiting for the SASL subsystem
 * to perform a task
 *
 * possible next state:
 *   * conn_closing - failing to update the libevent setting
 *   * conn_send_data - sending the response packet
 *
 * @param c the connection object
 * @return true - always
 */
bool conn_sasl_auth(McbpConnection* c);
