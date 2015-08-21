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
#include "config.h"
#include "statemachine_mcbp.h"

#include "memcached.h"

void McbpStateMachine::setCurrentTask(Connection& connection, TaskFunction task) {
    // Moving to the same state is legal
    if (task == currentTask) {
        return;
    }


    if (connection.isTAP() || connection.isDCP()) {
        /*
         * TAP and DCP connections behaves differently than normal
         * connections because they operate in a full duplex mode.
         * New messages may appear from both sides, so we can't block on
         * read from the nework / engine
         */

        if (task == conn_waiting) {
            connection.setCurrentEvent(EV_WRITE);
            task = conn_ship_log;
        }
    }

    if (settings.verbose > 2 || task == conn_closing
        || task == conn_setup_tap_stream) {
        settings.extensions.logger->log(EXTENSION_LOG_DETAIL, this,
                                        "%u: going from %s to %s\n",
                                        connection.getId(),
                                        getTaskName(currentTask),
                                        getTaskName(task));
    }

    if (task == conn_write || task == conn_mwrite) {
        if (connection.getStart() != 0) {
            collect_timings(&connection);
            connection.setStart(0);
        }
        MEMCACHED_PROCESS_COMMAND_END(connection.getId(),
                                      connection.write.buf,
                                      connection.write.bytes);
    }
    currentTask = task;
}

const char* McbpStateMachine::getTaskName(TaskFunction task) const {
    if (task == conn_listening) {
        return "conn_listening";
    } else if (task == conn_new_cmd) {
        return "conn_new_cmd";
    } else if (task == conn_waiting) {
        return "conn_waiting";
    } else if (task == conn_read) {
        return "conn_read";
    } else if (task == conn_parse_cmd) {
        return "conn_parse_cmd";
    } else if (task == conn_write) {
        return "conn_write";
    } else if (task == conn_nread) {
        return "conn_nread";
    } else if (task == conn_closing) {
        return "conn_closing";
    } else if (task == conn_mwrite) {
        return "conn_mwrite";
    } else if (task == conn_ship_log) {
        return "conn_ship_log";
    } else if (task == conn_setup_tap_stream) {
        return "conn_setup_tap_stream";
    } else if (task == conn_pending_close) {
        return "conn_pending_close";
    } else if (task == conn_immediate_close) {
        return "conn_immediate_close";
    } else if (task == conn_refresh_cbsasl) {
        return "conn_refresh_cbsasl";
    } else if (task == conn_refresh_ssl_certs) {
        return "conn_refresh_ssl_cert";
    } else if (task == conn_flush) {
        return "conn_flush";
    } else if (task == conn_audit_configuring) {
        return "conn_audit_configuring";
    } else if (task == conn_create_bucket) {
        return "conn_create_bucket";
    } else if (task == conn_delete_bucket) {
        return "conn_delete_bucket";
    } else {
        throw std::invalid_argument("Unknown task");
    }
}