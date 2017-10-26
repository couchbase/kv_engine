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

#include "connections.h"
#include "mcaudit.h"
#include "mcbp.h"
#include "mcbp_executors.h"
#include "protocol/mcbp/ship_dcp_log.h"
#include "sasl_tasks.h"

#include <platform/strerror.h>

static bool conn_new_cmd(McbpConnection& connection);
static bool conn_waiting(McbpConnection& connection);
static bool conn_read_packet_header(McbpConnection& connection);
static bool conn_parse_cmd(McbpConnection& connection);
static bool conn_read_packet_body(McbpConnection& connection);
static bool conn_closing(McbpConnection& connection);
static bool conn_pending_close(McbpConnection& connection);
static bool conn_immediate_close(McbpConnection& connection);
static bool conn_destroyed(McbpConnection& connection);
static bool conn_execute(McbpConnection& connection);
static bool conn_send_data(McbpConnection& connection);
static bool conn_ship_log(McbpConnection& connection);

typedef bool (*TaskFunction)(McbpConnection& connection);

static TaskFunction stateToFunc(McbpStateMachine::State state) {
    switch (state) {
    case McbpStateMachine::State::new_cmd:
        return conn_new_cmd;
    case McbpStateMachine::State::waiting:
        return conn_waiting;
    case McbpStateMachine::State::read_packet_header:
        return conn_read_packet_header;
    case McbpStateMachine::State::parse_cmd:
        return conn_parse_cmd;
    case McbpStateMachine::State::read_packet_body:
        return conn_read_packet_body;
    case McbpStateMachine::State::closing:
        return conn_closing;
    case McbpStateMachine::State::pending_close:
        return conn_pending_close;
    case McbpStateMachine::State::immediate_close:
        return conn_immediate_close;
    case McbpStateMachine::State::destroyed:
        return conn_destroyed;
    case McbpStateMachine::State::execute:
        return conn_execute;
    case McbpStateMachine::State::send_data:
        return conn_send_data;
    case McbpStateMachine::State::ship_log:
        return conn_ship_log;
    }
    throw std::invalid_argument("stateToFunc(): invalid state");
}

void McbpStateMachine::setCurrentState(State task) {
    // Moving to the same state is legal
    if (task == currentState) {
        return;
    }

    if (connection.isDCP()) {
        /*
         * DCP connections behaves differently than normal
         * connections because they operate in a full duplex mode.
         * New messages may appear from both sides, so we can't block on
         * read from the network / engine
         */

        if (task == State::waiting) {
            connection.setCurrentEvent(EV_WRITE);
            task = State::ship_log;
        }

        if (task == State::read_packet_header) {
            // If we're starting to read data, reset any running timers
            connection.setStart(ProcessClock::time_point());
        }
    }

    if (settings.getVerbose() > 2 || task == State::closing) {
        LOG_DETAIL(this,
                   "%u: going from %s to %s",
                   connection.getId(),
                   getStateName(currentState),
                   getStateName(task));
    }

    if (task == State::send_data) {
        if (connection.getStart() != ProcessClock::time_point()) {
            mcbp_collect_timings(&connection);
            connection.setStart(ProcessClock::time_point());
        }
        MEMCACHED_PROCESS_COMMAND_END(connection.getId(), nullptr, 0);
    }
    currentState = task;
}

const char* McbpStateMachine::getStateName(State state) const {
    switch (state) {
    case McbpStateMachine::State::new_cmd:
        return "new_cmd";
    case McbpStateMachine::State::waiting:
        return "waiting";
    case McbpStateMachine::State::read_packet_header:
        return "read_packet_header";
    case McbpStateMachine::State::parse_cmd:
        return "parse_cmd";
    case McbpStateMachine::State::read_packet_body:
        return "read_packet_body";
    case McbpStateMachine::State::closing:
        return "closing";
    case McbpStateMachine::State::pending_close:
        return "pending_close";
    case McbpStateMachine::State::immediate_close:
        return "immediate_close";
    case McbpStateMachine::State::destroyed:
        return "destroyed";
    case McbpStateMachine::State::execute:
        return "execute";
    case McbpStateMachine::State::send_data:
        return "send_data";
    case McbpStateMachine::State::ship_log:
        return "ship_log";
    }

    return "McbpStateMachine::getStateName: Invalid state";
}

bool McbpStateMachine::isIdleState() const {
    switch (currentState) {
    case State::read_packet_header:
    case State::read_packet_body:
    case State::waiting:
    case State::new_cmd:
    case State::ship_log:
    case State::send_data:
        return true;
    case State::parse_cmd:
    case State::closing:
    case State::pending_close:
    case State::immediate_close:
    case State::destroyed:
    case State::execute:
        return false;
    }
    throw std::logic_error("McbpStateMachine::isIdleState: Invalid state");
}

bool McbpStateMachine::execute() {
    return stateToFunc(currentState)(connection);
}

static void reset_cmd_handler(McbpConnection *c) {
    c->setCmd(-1);

    c->getCookieObject().reset();

    c->shrinkBuffers();
    if (c->read->rsize() >= sizeof(c->binary_header)) {
        c->setState(McbpStateMachine::State::parse_cmd);
    } else if (c->isSslEnabled()) {
        c->setState(McbpStateMachine::State::read_packet_header);
    } else {
        c->setState(McbpStateMachine::State::waiting);
    }
}

/**
 * Ship DCP log to the other end. This state differs with all other states
 * in the way that it support full duplex dialog. We're listening to both read
 * and write events from libevent most of the time. If a read event occurs we
 * switch to the conn_read state to read and execute the input message (that would
 * be an ack message from the other side). If a write event occurs we continue to
 * send DCP log to the other end.
 * @param c the DCP connection to drive
 * @return true if we should continue to process work for this connection, false
 *              if we should start processing events for other connections.
 */
bool conn_ship_log(McbpConnection& connection) {
    auto* c = &connection;
    if (is_bucket_dying(c)) {
        return true;
    }

    bool cont = false;
    short mask = EV_READ | EV_PERSIST | EV_WRITE;

    if (c->isSocketClosed()) {
        return false;
    }

    if (c->isReadEvent() || !c->read->empty()) {
        if (c->read->rsize() >= sizeof(c->binary_header)) {
            try_read_mcbp_command(c);
        } else {
            c->setState(McbpStateMachine::State::read_packet_header);
        }

        /* we're going to process something.. let's proceed */
        cont = true;

        /* We have a finite number of messages in the input queue */
        /* so let's process all of them instead of backing off after */
        /* reading a subset of them. */
        /* Why? Because we've got every time we're calling ship_mcbp_dcp_log */
        /* we try to send a chunk of items.. This means that if we end */
        /* up in a situation where we're receiving a burst of nack messages */
        /* we'll only process a subset of messages in our input queue, */
        /* and it will slowly grow.. */
        c->setNumEvents(c->getMaxReqsPerEvent());
    } else if (c->isWriteEvent()) {
        if (c->decrementNumEvents() >= 0) {
            c->setEwouldblock(false);
            ship_dcp_log(connection);
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
        c->setState(McbpStateMachine::State::closing);
    }

    return cont;
}

bool conn_waiting(McbpConnection& connection) {
    auto* c = &connection;
    if (is_bucket_dying(c) || c->processServerEvents()) {
        return true;
    }

    if (!c->updateEvent(EV_READ | EV_PERSIST)) {
        LOG_WARNING(c, "%u: conn_waiting - Unable to update libevent "
                    "settings with (EV_READ | EV_PERSIST), closing connection "
                    "(%p) %s",
                    c->getId(), c->getCookie(), c->getDescription().c_str());
        c->setState(McbpStateMachine::State::closing);
        return true;
    }
    c->setState(McbpStateMachine::State::read_packet_header);
    return false;
}

bool conn_read_packet_header(McbpConnection& connection) {
    auto* c = &connection;
    if (is_bucket_dying(c) || c->processServerEvents()) {
        return true;
    }

    switch (c->tryReadNetwork()) {
    case McbpConnection::TryReadResult::NoDataReceived:
        c->setState(McbpStateMachine::State::waiting);
        break;
    case McbpConnection::TryReadResult::DataReceived:
        if (c->read->rsize() >= sizeof(c->binary_header)) {
            c->setState(McbpStateMachine::State::parse_cmd);
        } else {
            c->setState(McbpStateMachine::State::waiting);
        }
        break;
    case McbpConnection::TryReadResult::SocketClosed:
    case McbpConnection::TryReadResult::SocketError:
        c->setState(McbpStateMachine::State::closing);
        break;
    case McbpConnection::TryReadResult::MemoryError: /* Failed to allocate more memory */
        /* State already set by try_read_network */
        break;
    }

    return true;
}

bool conn_parse_cmd(McbpConnection& connection) {
    auto* c = &connection;
    try_read_mcbp_command(c);
    return !c->isEwouldblock();
}

bool conn_new_cmd(McbpConnection& connection) {
    auto* c = &connection;
    if (is_bucket_dying(c)) {
        return true;
    }

    c->setStart(ProcessClock::time_point());

    if (!c->write->empty()) {
        LOG_WARNING(
                c,
                "%u: Expected write buffer to be empty.. It's not! (%" PRIu64
                ")",
                c->getId(),
                c->write->rsize());
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
         * DCP connections are different from normal
         * connections in the way that they may not even get data from
         * the other end so that they'll _have_ to wait for a write event.
         */
        if (c->havePendingInputData() || c->isDCP()) {
            short flags = EV_WRITE | EV_PERSIST;
            if (!c->updateEvent(flags)) {
                LOG_WARNING(c, "%u: conn_new_cmd - Unable to update "
                            "libevent settings, closing connection (%p) %s",
                            c->getId(), c->getCookie(),
                            c->getDescription().c_str());
                c->setState(McbpStateMachine::State::closing);
                return true;
            }
        }
        return false;
    }

    return true;
}

bool conn_execute(McbpConnection& connection) {
    auto* c = &connection;
    if (is_bucket_dying(c)) {
        return true;
    }

    if (!c->isPacketAvailable()) {
        throw std::logic_error(
                "conn_execute: Internal error.. the input packet is not "
                "completely in memory");
    }

    c->setEwouldblock(false);

    mcbp_execute_packet(c);

    if (c->isEwouldblock()) {
        c->unregisterEvent();
        return false;
    }

    // We've executed the packet, and given that we're not blocking we
    // we should move over to the next state. Just do a sanity check
    // for that.
    if (c->getState() == McbpStateMachine::State::execute) {
        throw std::logic_error(
                "conn_execute: Should leave conn_execute for !EWOULDBLOCK");
    }

    // Consume the packet we just executed from the input buffer
    c->read->consume([c](cb::const_byte_buffer buffer) -> ssize_t {
        size_t size =
                sizeof(c->binary_header) + c->binary_header.request.bodylen;
        if (size > buffer.size()) {
            throw std::logic_error(
                    "conn_execute: Not enough data in input buffer");
        }
        return size;
    });

    return true;
}

bool conn_read_packet_body(McbpConnection& connection) {
    auto* c = &connection;
    if (is_bucket_dying(c)) {
        return true;
    }

    if (c->isPacketAvailable()) {
        throw std::logic_error(
                "conn_read_packet_body: should not be called with the complete "
                "packet available");
    }

    // We need to get more data!!!
    auto res = c->read->produce([c](cb::byte_buffer buffer) -> ssize_t {
        return c->recv(reinterpret_cast<char*>(buffer.data()), buffer.size());
    });

    if (res > 0) {
        get_thread_stats(c)->bytes_read += res;

        if (c->isPacketAvailable()) {
            auto& cookie = connection.getCookieObject();
            auto input = c->read->rdata();
            const auto* req =
                    reinterpret_cast<const cb::mcbp::Request*>(input.data());
            cookie.setPacket(Cookie::PacketContent::Full,
                             cb::const_byte_buffer{input.data(),
                                                   sizeof(cb::mcbp::Request) +
                                                           req->getBodylen()});
            c->setState(McbpStateMachine::State::execute);
        }

        return true;
    }

    if (res == 0) { /* end of stream */
        c->setState(McbpStateMachine::State::closing);
        return true;
    }

    auto error = GetLastNetworkError();
    if (is_blocking(error)) {
        if (!c->updateEvent(EV_READ | EV_PERSIST)) {
            LOG_WARNING(c,
                        "%u: conn_read_packet_body - Unable to update libevent "
                        "settings with (EV_READ | EV_PERSIST), closing "
                        "connection (%p) %s",
                        c->getId(),
                        c->getCookie(),
                        c->getDescription().c_str());
            c->setState(McbpStateMachine::State::closing);
            return true;
        }

        // We need to wait for more data to be available on the socket
        // before we may proceed. Return false to stop the state machinery
        return false;
    }

    // We have a "real" error on the socket.
    std::string errormsg = cb_strerror(error);
    LOG_WARNING(c,
                "%u Closing connection (%p) %s due to read error: %s",
                c->getId(),
                c->getCookie(),
                c->getDescription().c_str(),
                errormsg.c_str());

    c->setState(McbpStateMachine::State::closing);
    return true;
}

bool conn_send_data(McbpConnection& connection) {
    auto* c = &connection;
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

bool conn_pending_close(McbpConnection& connection) {
    auto* c = &connection;
    if (!c->isSocketClosed()) {
        throw std::logic_error("conn_pending_close: socketDescriptor must be closed");
    }
    LOG_DEBUG(c,
              "Awaiting clients to release the cookie (pending close for %p)",
              (void*)c);
    /*
     * tell the DCP connection that we're disconnecting it now,
     * but give it a grace period
     */
    perform_callbacks(ON_DISCONNECT, NULL, c->getCookie());

    if (c->getRefcount() > 1) {
        return false;
    }

    c->setState(McbpStateMachine::State::immediate_close);
    return true;
}

bool conn_immediate_close(McbpConnection& connection) {
    auto* c = &connection;
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
        } else {
            throw std::logic_error("null port_instance");
        }
    }

    perform_callbacks(ON_DISCONNECT, NULL, c->getCookie());
    disassociate_bucket(c);
    conn_close(c);

    return false;
}

bool conn_closing(McbpConnection& connection) {
    auto* c = &connection;
    // Delete any attached command context
    c->getCookieObject().reset();

    /* We don't want any network notifications anymore.. */
    c->unregisterEvent();
    safe_close(c->getSocketDescriptor());
    c->setSocketDescriptor(INVALID_SOCKET);

    /* engine::release any allocated state */
    conn_cleanup_engine_allocations(c);

    if (c->getRefcount() > 1 || c->isEwouldblock()) {
        c->setState(McbpStateMachine::State::pending_close);
    } else {
        c->setState(McbpStateMachine::State::immediate_close);
    }
    return true;
}

/** sentinal state used to represent a 'destroyed' connection which will
 *  actually be freed at the end of the event loop. Always returns false.
 */
bool conn_destroyed(McbpConnection&) {
    return false;
}
