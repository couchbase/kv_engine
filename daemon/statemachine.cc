/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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
#include "statemachine.h"

#include "buckets.h"
#include "connection.h"
#include "connections.h"
#include "cookie.h"
#include "external_auth_manager_thread.h"
#include "front_end_thread.h"
#include "mcaudit.h"
#include "mcbp_executors.h"
#include "runtime.h"
#include "sasl_tasks.h"
#include "settings.h"

#include <event2/bufferevent_ssl.h>
#include <logger/logger.h>
#include <mcbp/mcbp.h>
#include <nlohmann/json.hpp>
#include <platform/strerror.h>
#include <platform/string_hex.h>
#include <gsl/gsl>

void StateMachine::setCurrentState(State task) {
    // Moving to the same state is legal
    if (task == currentState) {
        return;
    }

    currentState = task;
}

const char* StateMachine::getStateName(State state) const {
    switch (state) {
    case StateMachine::State::ssl_init:
        return "ssl_init";
    case StateMachine::State::new_cmd:
        return "new_cmd";
    case StateMachine::State::read_packet:
        return "read_packet";
    case StateMachine::State::closing:
        return "closing";
    case StateMachine::State::pending_close:
        return "pending_close";
    case StateMachine::State::immediate_close:
        return "immediate_close";
    case StateMachine::State::destroyed:
        return "destroyed";
    case StateMachine::State::execute:
        return "execute";
    case StateMachine::State::send_data:
        return "send_data";
    case StateMachine::State::drain_send_buffer:
        return "drain_send_buffer";
    case StateMachine::State::ship_log:
        return "ship_log";
    }

    return "StateMachine::getStateName: Invalid state";
}

bool StateMachine::isIdleState() const {
    switch (currentState) {
    case State::read_packet:
    case State::new_cmd:
    case State::ship_log:
    case State::send_data:
    case State::pending_close:
    case State::drain_send_buffer:
    case State::ssl_init:
        return true;
    case State::closing:
    case State::immediate_close:
    case State::destroyed:
    case State::execute:
        return false;
    }
    throw std::logic_error("StateMachine::isIdleState: Invalid state");
}

bool StateMachine::execute() {
    switch (currentState) {
    case StateMachine::State::ssl_init:
        return conn_ssl_init();
    case StateMachine::State::new_cmd:
        return conn_new_cmd();
    case StateMachine::State::read_packet:
        return conn_read_packet();
    case StateMachine::State::closing:
        return conn_closing();
    case StateMachine::State::pending_close:
        return conn_pending_close();
    case StateMachine::State::immediate_close:
        return conn_immediate_close();
    case StateMachine::State::destroyed:
        return conn_destroyed();
    case StateMachine::State::execute:
        return conn_execute();
    case StateMachine::State::send_data:
        return conn_send_data();
    case StateMachine::State::drain_send_buffer:
        return conn_drain_send_buffer();
    case StateMachine::State::ship_log:
        return conn_ship_log();
    }
    throw std::invalid_argument("execute(): invalid state");
}

static std::pair<cb::x509::Status, std::string> getCertUserName(Connection& c) {
    auto* ssl_st = bufferevent_openssl_get_ssl(c.bev.get());
    cb::openssl::unique_x509_ptr cert(SSL_get_peer_certificate(ssl_st));
    return Settings::instance().lookupUser(cert.get());
}

bool StateMachine::conn_ssl_init() {
    connection.setState(StateMachine::State::new_cmd);
    auto certResult = getCertUserName(connection);
    bool disconnect = false;
    switch (certResult.first) {
    case cb::x509::Status::NoMatch:
    case cb::x509::Status::Error:
        disconnect = true;
        break;
    case cb::x509::Status::NotPresent:
        if (Settings::instance().getClientCertMode() ==
            cb::x509::Mode::Mandatory) {
            disconnect = true;
        } else if (is_default_bucket_enabled()) {
            associate_bucket(connection, "default");
        }
        break;
    case cb::x509::Status::Success:
        if (!connection.tryAuthFromSslCert(certResult.second)) {
            disconnect = true;
            // Don't print an error message... already logged
            certResult.second.resize(0);
        }
    }

    if (disconnect) {
        if (certResult.first == cb::x509::Status::NotPresent) {
            audit_auth_failure(connection,
                               "Client did not provide an X.509 certificate");
        } else {
            audit_auth_failure(
                    connection,
                    "Failed to use client provided X.509 certificate");
        }
        connection.setState(StateMachine::State::closing);
        if (!certResult.second.empty()) {
            LOG_WARNING(
                    "{}: conn_ssl_init: disconnection client due to"
                    " error [{}]",
                    connection.getId(),
                    certResult.second);
        }
    } else {
        auto* ssl_st = bufferevent_openssl_get_ssl(connection.bev.get());
        LOG_INFO("{}: Using SSL cipher:{}",
                 connection.getId(),
                 SSL_get_cipher_name(ssl_st));
    }

    return true;
}

/**
 * Ship DCP log to the other end. This state differs with all other states
 * in the way that it support full duplex dialog. We're listening to both read
 * and write events from libevent most of the time. If a read event occurs we
 * switch to the conn_read state to read and execute the input message (that
 * would be an ack message from the other side). If a write event occurs we
 * continue to send DCP log to the other end.
 * @param c the DCP connection to drive
 * @return true if we should continue to process work for this connection, false
 *              if we should start processing events for other connections.
 */
bool StateMachine::conn_ship_log() {
    if (is_bucket_dying(connection)) {
        return true;
    }

    auto& cookie = connection.getCookieObject();
    cookie.setEwouldblock(false);

    if (connection.isPacketAvailable()) {
        cookie.initialize(connection.getPacket(),
                          connection.isTracingEnabled());
        return validate_input_packet();
    }

    const auto ret = connection.getBucket().getDcpIface()->step(
            static_cast<const void*>(&cookie), &connection);

    switch (connection.remapErrorCode(ret)) {
    case ENGINE_SUCCESS:
        /* The engine got more data it wants to send */
        connection.setState(StateMachine::State::send_data);
        break;
    case ENGINE_EWOULDBLOCK:
        // the engine don't have more data to send at this moment
        return false;
    default:
        LOG_WARNING(
                R"({}: ship_dcp_log - step returned {} - closing connection {})",
                connection.getId(),
                std::to_string(ret),
                connection.getDescription());
        connection.getCookieObject().setEwouldblock(false);
        setCurrentState(State::closing);
    }

    return true;
}

bool StateMachine::conn_read_packet() {
    if (is_bucket_dying(connection) || connection.processServerEvents()) {
        return true;
    }

    if (connection.isPacketAvailable()) {
        connection.getCookieObject().initialize(connection.getPacket(),
                                                connection.isTracingEnabled());
        return validate_input_packet();
    }

    return false;
}

bool StateMachine::conn_new_cmd() {
    if (is_bucket_dying(connection)) {
        return true;
    }

    connection.getCookieObject().reset();
    if (connection.isDCP()) {
        setCurrentState(State::ship_log);
    } else {
        setCurrentState(State::read_packet);
    }

    // In order to ensure that all clients will be served each
    // connection will only process a certain number of operations
    // before they will back off.
    return !connection.maybeYield();
}

bool StateMachine::validate_input_packet() {
    static McbpValidator packetValidator;

    if (is_bucket_dying(connection)) {
        return true;
    }

    auto& cookie = connection.getCookieObject();
    const auto& header = cookie.getHeader();
    // We validated the basics of the header in try_read_mcbp_command
    // (we needed that in order to know if we could use the length field...

    if (header.isRequest()) {
        const auto& request = header.getRequest();
        if (cb::mcbp::is_client_magic(request.getMagic())) {
            auto opcode = request.getClientOpcode();
            if (!cb::mcbp::is_valid_opcode(opcode)) {
                // We don't know about this command so we can stop
                // processing it. We know that the header adds
                cookie.sendResponse(cb::mcbp::Status::UnknownCommand);
                return true;
            }

            auto result = packetValidator.validate(opcode, cookie);
            if (result != cb::mcbp::Status::Success) {
                LOG_WARNING(
                        R"({}: Invalid format specified for "{}" - Status: "{}" - Closing connection. Packet:[{}] Reason:"{}")",
                        connection.getId(),
                        to_string(opcode),
                        to_string(result),
                        request.toJSON(false).dump(),
                        cookie.getErrorContext());
                audit_invalid_packet(cookie.getConnection(),
                                     cookie.getPacket());
                cookie.sendResponse(result);
                setCurrentState(State::closing);
                return true;
            }
            cookie.setValidated(true);
        } else {
            // We should not be receiving a server command.
            // Audit and log
            audit_invalid_packet(connection, cookie.getPacket());
            LOG_WARNING("{}: Received a server command. Closing connection",
                        connection.getId());
            setCurrentState(State::closing);
            return true;
        }
    } // We don't currently have any validators for response packets

    setCurrentState(State::execute);
    return true;
}

bool StateMachine::conn_execute() {
    if (is_bucket_dying(connection)) {
        return true;
    }

    auto& cookie = connection.getCookieObject();
    cookie.setEwouldblock(false);
    connection.enableReadEvent();

    if (!cookie.execute()) {
        connection.disableReadEvent();
        return false;
    }

    // We've executed the packet, and given that we're not blocking we
    // we should move over to the next state. Just do a sanity check
    // for that.
    if (currentState == StateMachine::State::execute) {
        throw std::logic_error(
                "conn_execute: Should leave conn_execute for !EWOULDBLOCK");
    }

    mcbp_collect_timings(cookie);

    // We've cleared the memory for this packet so we need to mark it
    // as cleared in the cookie to avoid having it dumped in toJSON and
    // using freed memory. We cannot call reset on the cookie as we
    // want to preserve the error context and id.
    cookie.clearPacket();
    return true;
}

bool StateMachine::conn_send_data() {
    if (connection.getSendQueueSize() >
        Settings::instance().getMaxPacketSize()) {
        // We don't want the connection to allocate too much resources
        // so lets drain the send buffer before proceeding
        // (but we need to enter the drain
        setCurrentState(State::drain_send_buffer);
        return false;
    }

    setCurrentState(State::new_cmd);
    return true;
}

bool StateMachine::conn_drain_send_buffer() {
    if (connection.havePendingData()) {
        return false;
    }

    setCurrentState(State::new_cmd);
    return true;
}

bool StateMachine::conn_immediate_close() {
    disassociate_bucket(connection);

    // Do the final cleanup of the connection:
    auto& thread = connection.getThread();
    thread.notification.remove(&connection);
    // remove from pending-io list
    std::lock_guard<std::mutex> lock(thread.pending_io.mutex);
    thread.pending_io.map.erase(&connection);

    connection.bev.reset();

    // Set the connection to the sentinal state destroyed and return
    // false to break out of the event loop (and have the the framework
    // delete the connection object).
    setCurrentState(State::destroyed);

    return false;
}

bool StateMachine::conn_pending_close() {
    return connection.close();
}

bool StateMachine::conn_closing() {
    externalAuthManager->remove(connection);
    return connection.close();
}

/** sentinal state used to represent a 'destroyed' connection which will
 *  actually be freed at the end of the event loop. Always returns false.
 */
bool StateMachine::conn_destroyed() {
    return false;
}
