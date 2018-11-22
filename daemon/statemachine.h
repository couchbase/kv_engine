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

/**
 * The state machinery for connections in the daemon
 */
class StateMachine {
public:
    enum class State {
        /**
         * new_cmd is the initial state for a connection object, and the
         * initial state for the processing a new command. It is used to
         * prepare the connection object for handling the next command. It
         * is also the state where the connection object would back off the
         * CPU after servicing n number of requests (to avoid starvation of
         * other connections bound to the same worker thread).
         *
         * possible next state:
         *   * closing - if the bucket is currently being deleted
         *   * parse_cmd - if the input buffer contains the next header
         *   * waiting - we need more data
         *   * ship_log - (for DCP connections)
         */
        new_cmd,

        /**
         * Set up a read event for the connection
         *
         * possible next state:
         *   * closing - if the bucket is currently being deleted
         *   * read_packet_header - the bucket isn't being deleted
         */
        waiting,

        /**
         * read_packet_header tries to read from the network and fill the
         * input buffer.
         *
         * possible next state:
         *   * closing - if the bucket is currently being deleted
         *   * waiting - if we failed to read more data from the network
         *               (DCP connections will enter ship_log)
         *   * parse_cmd - if the entire packet header is available
         */
        read_packet_header,

        /**
         * parse_cmd parse the command header
         *
         * possible next state:
         *   * closing - if the bucket is currently being deleted (or protocol
         *               error)
         *   * read_packet_body - to fetch the rest of the data in the packet
         *   * send_data - if an error occurs and we want to tell the user
         *                 about the error before disconnecting.
         */
        parse_cmd,

        /**
         * Make sure that the entire packet is available
         *
         * possible next state:
         *   * closing - if the bucket is currently being deleted (or protocol
         *               error)
         *   * execute - the entire packet is available in memory
         */
        read_packet_body,

        /**
         * Validate the packet
         *
         * Possible next state
         *   * closing - If the magic is incorrect or we can't send an error
         *               message (validate failed on response for instance)
         (               of if the bucket is being deleted...
         *   * send_data - If we want to send an error message
         *   * execute - start executing the packet
         */
        validate,

        /**
         * Execute the current command
         *
         * possible next state:
         *   * closing - if the bucket is currently being deleted (or protocol
         *               error)
         *   * send_data - If there is data to send
         *   * new_cmd - for "no-reply" commands
         */
        execute,

        /**
         * Send data to the client
         *
         * possible next state:
         *   * closing - if the bucket is currently being deleted (or network
         *               errors)
         *   * new_cmd - All data sent and we should start at the next command
         *   * ship_log - DCP connections
         */
        send_data,

        /**
         * ship_log is the state where the DCP connection end up in the "idle"
         * state to allow it to either process an incomming packet from the
         * other end, or start filling in the data from the underlying engine
         * to ship to the other end
         *
         * possible next state:
         *   * closing - if the bucket is currently being deleted
         *   * read_packet_header - to process input packet
         *   * read_packet_body - to process input packet
         *   * send_data - send the data from the engine
         */
        ship_log,

        /**
         * start closing a connection
         *
         * possible next state:
         *   * immediate_close - no one else is holding a reference to the
         *                       connection
         *   * pending_close - someone is holding a reference to the connection
         */
        closing,

        /**
         * Wait until all references to the connection is released
         *
         * possible next state:
         *   * immediate_close - no one else is holding a reference to the
         *                       connection
         */
        pending_close,

        /**
         * Close sockets and notify everyone with ON_DISCONNECT
         * disassociate the bucket and release all engine allocations
         *
         * next state:
         *    * destroyed
         */
        immediate_close,

        /**
         * The sentinel state for a connection object
         */
        destroyed
    };

    StateMachine() = delete;

    explicit StateMachine(Connection& connection_)
        : currentState(State::new_cmd), connection(connection_) {
    }

    bool isIdleState() const;

    /**
     * Execute the current task function
     *
     * @return the return value from the task function (true to continue
     *         execution, false otherwise)
     */
    bool execute();

    /**
     * Get the current state (for debug purposes)
     */
    State getCurrentState() const {
        return currentState;
    }

    /**
     * Get the name of the current state (for debug purposes)
     */
    const char* getCurrentStateName() const {
        return getStateName(currentState);
    }

    /**
     * Set the current state
     *
     * This validates the requested state transition is valid.
     *
     * @param task the new task function
     * @throws std::logical_error for illegal state transitions
     */
    void setCurrentState(State state);

    /**
     * Get the name of a given state
     */
    const char* getStateName(State state) const;

protected:
    // The various methods implementing the logic for that state
    bool conn_new_cmd();
    bool conn_waiting();
    bool conn_read_packet_header();
    bool conn_parse_cmd();
    bool conn_read_packet_body();
    bool conn_closing();
    bool conn_pending_close();
    bool conn_immediate_close();
    bool conn_destroyed();
    bool conn_validate();
    bool conn_execute();
    bool conn_send_data();
    bool conn_ship_log();

    State currentState;
    Connection& connection;
};
