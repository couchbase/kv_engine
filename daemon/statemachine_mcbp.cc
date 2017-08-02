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

#include "protocol/mcbp/engine_wrapper.h"
#include "memcached.h"
#include "mcbp.h"
#include "mcbp_executors.h"
#include "connections.h"
#include "sasl_tasks.h"
#include "runtime.h"
#include "mcaudit.h"

#include <platform/strerror.h>

void McbpStateMachine::setCurrentTask(McbpConnection& connection, TaskFunction task) {
    // Moving to the same state is legal
    if (task == currentTask) {
        return;
    }

    if (connection.isDCP()) {
        /*
         * DCP connections behaves differently than normal
         * connections because they operate in a full duplex mode.
         * New messages may appear from both sides, so we can't block on
         * read from the network / engine
         */

        if (task == conn_waiting) {
            connection.setCurrentEvent(EV_WRITE);
            task = conn_ship_log;
        }

        if (task == conn_read_packet_header) {
            // If we're starting to read data, reset any running timers
            connection.setStart(0);
        }
    }

    if (settings.getVerbose() > 2 || task == conn_closing) {
        LOG_DETAIL(this,
                   "%u: going from %s to %s\n",
                   connection.getId(),
                   getTaskName(currentTask),
                   getTaskName(task));
    }

    if (task == conn_send_data) {
        if (connection.getStart() != 0) {
            mcbp_collect_timings(&connection);
            connection.setStart(0);
        }
        MEMCACHED_PROCESS_COMMAND_END(connection.getId(), nullptr, 0);
    }
    currentTask = task;
}

const char* McbpStateMachine::getTaskName(TaskFunction task) const {
    if (task == conn_new_cmd) {
        return "conn_new_cmd";
    } else if (task == conn_waiting) {
        return "conn_waiting";
    } else if (task == conn_read_packet_header) {
        return "conn_read_packet_header";
    } else if (task == conn_parse_cmd) {
        return "conn_parse_cmd";
    } else if (task == conn_read_packet_body) {
        return "conn_read_packet_body";
    } else if (task == conn_execute) {
        return "conn_execute";
    } else if (task == conn_closing) {
        return "conn_closing";
    } else if (task == conn_send_data) {
        return "conn_send_data";
    } else if (task == conn_ship_log) {
        return "conn_ship_log";
    } else if (task == conn_pending_close) {
        return "conn_pending_close";
    } else if (task == conn_immediate_close) {
        return "conn_immediate_close";
    } else if (task == conn_destroyed) {
        return "conn_destroyed";
    } else {
        throw std::invalid_argument(
                "McbpStateMachine::getTaskName: Unknown task");
    }
}

static void reset_cmd_handler(McbpConnection *c) {
    c->setCmd(-1);

    c->getCookieObject().reset();
    c->resetCommandContext();

    if (c->read.bytes == 0) {
        /* Make the whole read buffer available. */
        c->read.curr = c->read.buf;
    }

    c->shrinkBuffers();
    if (c->read.bytes >= sizeof(c->binary_header)) {
        c->setState(conn_parse_cmd);
    } else {
        c->setState(conn_waiting);
    }
}

/**
 * Ship tap log to the other end. This state differs with all other states
 * in the way that it support full duplex dialog. We're listening to both read
 * and write events from libevent most of the time. If a read event occurs we
 * switch to the conn_read state to read and execute the input message (that would
 * be an ack message from the other side). If a write event occurs we continue to
 * send tap log to the other end.
 * @param c the tap connection to drive
 * @return true if we should continue to process work for this connection, false
 *              if we should start processing events for other connections.
 */
bool conn_ship_log(McbpConnection *c) {
    if (is_bucket_dying(c)) {
        return true;
    }

    bool cont = false;
    short mask = EV_READ | EV_PERSIST | EV_WRITE;

    if (c->isSocketClosed()) {
        return false;
    }

    if (c->isReadEvent() || c->read.bytes > 0) {
        if (c->read.bytes >= sizeof(c->binary_header)) {
            try_read_mcbp_command(c);
        } else {
            c->setState(conn_read_packet_header);
        }

        /* we're going to process something.. let's proceed */
        cont = true;

        /* We have a finite number of messages in the input queue */
        /* so let's process all of them instead of backing off after */
        /* reading a subset of them. */
        /* Why? Because we've got every time we're calling ship_tap_log */
        /* we try to send a chunk of items.. This means that if we end */
        /* up in a situation where we're receiving a burst of nack messages */
        /* we'll only process a subset of messages in our input queue, */
        /* and it will slowly grow.. */
        c->setNumEvents(c->getMaxReqsPerEvent());
    } else if (c->isWriteEvent()) {
        if (c->decrementNumEvents() >= 0) {
            c->setEwouldblock(false);
            ship_mcbp_dcp_log(c);
            if (c->isEwouldblock()) {
                mask = EV_READ | EV_PERSIST;
            } else {
                cont = true;
            }
        }
    }

    if (!c->updateEvent(mask)) {
        LOG_WARNING(c, "%u: conn_ship_log - Unable to update libevent "
                    "settings, closing connection (%p) %s", c->getId(),
                    c->getCookie(), c->getDescription().c_str());
        c->setState(conn_closing);
    }

    return cont;
}

bool conn_waiting(McbpConnection *c) {
    if (is_bucket_dying(c)) {
        return true;
    }

    if (!c->updateEvent(EV_READ | EV_PERSIST)) {
        LOG_WARNING(c, "%u: conn_waiting - Unable to update libevent "
                    "settings with (EV_READ | EV_PERSIST), closing connection "
                    "(%p) %s",
                    c->getId(), c->getCookie(), c->getDescription().c_str());
        c->setState(conn_closing);
        return true;
    }
    c->setState(conn_read_packet_header);
    return false;
}

bool conn_read_packet_header(McbpConnection* c) {
    if (is_bucket_dying(c)) {
        return true;
    }

    switch (c->tryReadNetwork()) {
    case McbpConnection::TryReadResult::NoDataReceived:
        if (settings.isExitOnConnectionClose()) {
            // No more data, proceed to close which will exit the process
            c->setState(conn_closing);
        } else {
            c->setState(conn_waiting);
        }
        break;
    case McbpConnection::TryReadResult::DataReceived:
        if (c->read.bytes >= sizeof(c->binary_header)) {
            c->setState(conn_parse_cmd);
        } else {
            c->setState(conn_waiting);
        }
        break;
    case McbpConnection::TryReadResult::SocketClosed:
    case McbpConnection::TryReadResult::SocketError:
        c->setState(conn_closing);
        break;
    case McbpConnection::TryReadResult::MemoryError: /* Failed to allocate more memory */
        /* State already set by try_read_network */
        break;
    }

    return true;
}

bool conn_parse_cmd(McbpConnection *c) {
    try_read_mcbp_command(c);
    return !c->isEwouldblock();
}

bool conn_new_cmd(McbpConnection *c) {
    if (is_bucket_dying(c)) {
        return true;
    }

    c->setStart(0);

    if (!c->write->empty()) {
        LOG_WARNING(c,
                    "%u: Expected write buffer to be empty.. It's not! (%"
                    PRIu64 ")", c->getId(), c->write->rsize());
    }

    /*
     * In order to ensure that all clients will be served each
     * connection will only process a certain number of operations
     * before they will back off.
     */
    if (c->decrementNumEvents() >= 0) {
        reset_cmd_handler(c);
    } else {
        get_thread_stats(c)->conn_yields++;

        /*
         * If we've got data in the input buffer we might get "stuck"
         * if we're waiting for a read event. Why? because we might
         * already have all of the data for the next command in the
         * userspace buffer so the client is idle waiting for the
         * response to arrive. Lets set up a _write_ notification,
         * since that'll most likely be true really soon.
         *
         * DCP and TAP connections is different from normal
         * connections in the way that they may not even get data from
         * the other end so that they'll _have_ to wait for a write event.
         */
        if (c->havePendingInputData() || c->isDCP()) {
            short flags = EV_WRITE | EV_PERSIST;
            // pipe requires EV_READ forcing to ensure we can read until EOF
            if (c->isPipeConnection()) {
                flags |= EV_READ;
            }
            if (!c->updateEvent(flags)) {
                LOG_WARNING(c, "%u: conn_new_cmd - Unable to update "
                            "libevent settings, closing connection (%p) %s",
                            c->getId(), c->getCookie(),
                            c->getDescription().c_str());
                c->setState(conn_closing);
                return true;
            }
        }
        return false;
    }

    return true;
}

bool conn_execute(McbpConnection *c) {
    if (is_bucket_dying(c)) {
        return true;
    }

    if (c->getRlbytes() != 0) {
        throw std::logic_error("conn_execute: Expecting more data (" +
                               std::to_string(c->getRlbytes()) + ")");
    }

    c->setEwouldblock(false);
    bool block = false;

    mcbp_complete_packet(c);

    if (c->isEwouldblock()) {
        c->unregisterEvent();
        block = true;
    }
    return !block;
}

bool conn_read_packet_body(McbpConnection* c) {
    if (is_bucket_dying(c)) {
        return true;
    }

    if (c->getRlbytes() == 0) {
        c->setState(conn_execute);
        return true;
    }

    /* first check if we have the bytes present in our input buffer */
    if (c->read.bytes > 0) {
        auto tocopy = std::min(c->getRlbytes(), c->read.bytes);
        c->setRlbytes(c->getRlbytes() - tocopy);
        c->read.curr += tocopy;
        c->read.bytes -= tocopy;

        if (c->getRlbytes() == 0) {
            // We've got all we need... go execute the command
            c->setState(conn_execute);
            return true;
        }
    }

    /*  now try reading from the socket */
    auto res = c->recv(c->read.curr, c->getRlbytes());
    auto error = GetLastNetworkError();
    if (res > 0) {
        get_thread_stats(c)->bytes_read += res;
        c->read.curr += res;
        c->setRlbytes(c->getRlbytes() - res);
        return true;
    }
    if (res == 0) { /* end of stream */
        c->setState(conn_closing);
        return true;
    }

    if (res == -1 && is_blocking(error)) {
        if (!c->updateEvent(EV_READ | EV_PERSIST)) {
            LOG_WARNING(c,
                        "%u: conn_read_packet_body - Unable to update libevent "
                        "settings with (EV_READ | EV_PERSIST), closing "
                        "connection (%p) %s",
                        c->getId(),
                        c->getCookie(),
                        c->getDescription().c_str());
            c->setState(conn_closing);
            return true;
        }
        return false;
    }

    /* otherwise we have a real error, on which we close the connection */
    if (!is_closed_conn(error)) {
        LOG_WARNING(c,
                    "%u Failed to read, and not due to blocking:\n"
                        "errno: %d %s \n"
                        "rcurr=%lx rbuf=%lx rlbytes=%d rsize=%d\n",
                    c->getId(), errno, strerror(errno),
                    (long)c->read.curr, (long)c->read.buf,
                    (int)c->getRlbytes(), (int)c->read.size);
    }
    c->setState(conn_closing);
    return true;
}

bool conn_send_data(McbpConnection* c) {
    bool ret = true;

    switch (c->transmit()) {
    case McbpConnection::TransmitResult::Complete:
        // Release all allocated resources
        c->releaseTempAlloc();
        c->releaseReservedItems();

        // We're done sending the response to the client. Enter the next
        // state in the state machine
        c->setState(c->getWriteAndGo());
        break;

    case McbpConnection::TransmitResult::Incomplete:
        LOG_INFO(c, "%d - Incomplete transfer. Will retry", c->getId());
        break;

    case McbpConnection::TransmitResult::HardError:
        LOG_NOTICE(c, "%d - Hard error, closing connection", c->getId());
        break;

    case McbpConnection::TransmitResult::SoftError:
        ret = false;
        break;
    }

    if (is_bucket_dying(c)) {
        return true;
    }

    return ret;
}

bool conn_pending_close(McbpConnection *c) {
    if (!c->isSocketClosed()) {
        throw std::logic_error("conn_pending_close: socketDescriptor must be closed");
    }
    LOG_DEBUG(c,
              "Awaiting clients to release the cookie (pending close for %p)",
              (void*)c);
    /*
     * tell the tap connection that we're disconnecting it now,
     * but give it a grace period
     */
    perform_callbacks(ON_DISCONNECT, NULL, c->getCookie());

    if (c->getRefcount() > 1) {
        return false;
    }

    c->setState(conn_immediate_close);
    return true;
}

bool conn_immediate_close(McbpConnection *c) {
    ListeningPort *port_instance;
    if (!c->isSocketClosed()) {
        throw std::logic_error("conn_immediate_close: socketDescriptor must be closed");
    }
    LOG_DETAIL(c, "Releasing connection %p", c);

    {
        std::lock_guard<std::mutex> guard(stats_mutex);
        port_instance = get_listening_port_instance(c->getParentPort());
        if (port_instance) {
            --port_instance->curr_conns;
        } else if(!c->isPipeConnection()) {
            throw std::logic_error("null port_instance and connection "
                                       "is not a pipe");
        }
    }

    perform_callbacks(ON_DISCONNECT, NULL, c->getCookie());
    disassociate_bucket(c);
    conn_close(c);

    return false;
}

bool conn_closing(McbpConnection *c) {
    // Delete any attached command context
    c->resetCommandContext();

    /* We don't want any network notifications anymore.. */
    c->unregisterEvent();
    safe_close(c->getSocketDescriptor());
    c->setSocketDescriptor(INVALID_SOCKET);

    /* engine::release any allocated state */
    conn_cleanup_engine_allocations(c);

    if (c->getRefcount() > 1 || c->isEwouldblock()) {
        c->setState(conn_pending_close);
    } else {
        c->setState(conn_immediate_close);
    }
    return true;
}

/** sentinal state used to represent a 'destroyed' connection which will
 *  actually be freed at the end of the event loop. Always returns false.
 */
bool conn_destroyed(McbpConnection*) {
    return false;
}
