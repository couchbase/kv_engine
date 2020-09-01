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
#include "connection.h"

#include "buckets.h"
#include "connections.h"
#include "cookie.h"
#include "external_auth_manager_thread.h"
#include "front_end_thread.h"
#include "listening_port.h"
#include "mc_time.h"
#include "mcaudit.h"
#include "memcached.h"
#include "protocol/mcbp/dcp_snapshot_marker_codec.h"
#include "protocol/mcbp/engine_wrapper.h"
#include "runtime.h"
#include "server_event.h"
#include "settings.h"

#include <logger/logger.h>
#include <mcbp/mcbp.h>
#include <mcbp/protocol/framebuilder.h>
#include <mcbp/protocol/header.h>
#include <memcached/durability_spec.h>
#include <nlohmann/json.hpp>
#include <phosphor/phosphor.h>
#include <platform/cbassert.h>
#include <platform/checked_snprintf.h>
#include <platform/socket.h>
#include <platform/strerror.h>
#include <platform/string_hex.h>
#include <platform/timeutils.h>
#include <utilities/logtags.h>
#include <gsl/gsl>

#include <cctype>
#include <exception>
#ifndef WIN32
#include <netinet/tcp.h> // For TCP_NODELAY etc
#endif

/// The TLS packet is using the following format:
/// Byte 0 - Content type
/// Byte 1 and 2 - Version
/// Byte 3 and 4 - length
///   n bytes of user payload
///   m bytes of MAC
///   o bytes of padding for block ciphers
/// Create a constant which represents the maxium amount of data we
/// may put in a single TLS frame
static constexpr std::size_t TlsFrameSize = 16 * 1024;

std::string to_string(Connection::Priority priority) {
    switch (priority) {
    case Connection::Priority::High:
        return "High";
    case Connection::Priority::Medium:
        return "Medium";
    case Connection::Priority::Low:
        return "Low";
    }
    throw std::invalid_argument("No such priority: " +
                                std::to_string(int(priority)));
}

bool Connection::setTcpNoDelay(bool enable) {
    if (socketDescriptor == INVALID_SOCKET) {
        // Our unit test run without a connected socket (and there is
        // no point of running setsockopt on an invalid socket and
        // get the error message from there).. But we don't want them
        // (the unit tests) to flood the console with error messages
        // that setsockopt failed
        return false;
    }

    const int flags = enable ? 1 : 0;
    int error = cb::net::setsockopt(socketDescriptor,
                                    IPPROTO_TCP,
                                    TCP_NODELAY,
                                    reinterpret_cast<const void*>(&flags),
                                    sizeof(flags));

    if (error != 0) {
        std::string errmsg = cb_strerror(cb::net::get_socket_error());
        LOG_WARNING("setsockopt(TCP_NODELAY): {}", errmsg);
        nodelay = false;
        return false;
    } else {
        nodelay = enable;
    }

    return true;
}

/**
 * Get a JSON representation of an event mask
 *
 * @param mask the mask to convert to JSON
 * @return the json representation.
 */
static nlohmann::json event_mask_to_json(const short mask) {
    nlohmann::json ret;
    nlohmann::json array = nlohmann::json::array();

    ret["raw"] = cb::to_hex(uint16_t(mask));

    if (mask & EV_READ) {
        array.push_back("read");
    }
    if (mask & EV_WRITE) {
        array.push_back("write");
    }
    if (mask & EV_PERSIST) {
        array.push_back("persist");
    }
    if (mask & EV_TIMEOUT) {
        array.push_back("timeout");
    }

    ret["decoded"] = array;
    return ret;
}

nlohmann::json Connection::toJSON() const {
    nlohmann::json ret;

    ret["connection"] = cb::to_hex(uint64_t(this));

    if (socketDescriptor == INVALID_SOCKET) {
        ret["socket"] = "disconnected";
        return ret;
    }

    ret["socket"] = socketDescriptor;
    ret["yields"] = yields.load();
    ret["protocol"] = "memcached";
    ret["peername"] = getPeername().c_str();
    ret["sockname"] = getSockname().c_str();
    ret["parent_port"] = parent_port;
    ret["bucket_index"] = getBucketIndex();
    ret["internal"] = isInternal();

    if (authenticated) {
        if (internal) {
            // We want to be able to map these connections, and given
            // that it is internal we don't reveal any user data
            ret["username"] = username;
        } else {
            ret["username"] = cb::tagUserData(username);
        }
    }

    ret["refcount"] = refcount;

    nlohmann::json features = nlohmann::json::array();
    if (isSupportsMutationExtras()) {
        features.push_back("mutation extras");
    }
    if (isXerrorSupport()) {
        features.push_back("xerror");
    }
    if (nodelay) {
        features.push_back("tcp nodelay");
    }
    if (allowUnorderedExecution()) {
        features.push_back("unordered execution");
    }
    if (tracingEnabled) {
        features.push_back("tracing");
    }

    if (isCollectionsSupported()) {
        features.push_back("collections");
    }

    if (isDuplexSupported()) {
        features.push_back("duplex");
    }

    if (isClustermapChangeNotificationSupported()) {
        features.push_back("CCN");
    }

    ret["features"] = features;

    ret["thread"] = getThread().index;
    ret["priority"] = to_string(priority);

    if (clustermap_revno == -2) {
        ret["clustermap_revno"] = "unknown";
    } else {
        ret["clustermap_revno"] = clustermap_revno;
    }

    ret["total_cpu_time"] = std::to_string(total_cpu_time.count());
    ret["min_sched_time"] = std::to_string(min_sched_time.count());
    ret["max_sched_time"] = std::to_string(max_sched_time.count());

    nlohmann::json arr = nlohmann::json::array();
    for (const auto& c : cookies) {
        arr.push_back(c->toJSON());
    }
    ret["cookies"] = arr;

    if (agentName.front() != '\0') {
        ret["agent_name"] = std::string(agentName.data());
    }
    if (connectionId.front() != '\0') {
        ret["connection_id"] = std::string(connectionId.data());
    }

    ret["sasl_enabled"] = saslAuthEnabled;
    ret["dcp"] = isDCP();
    ret["dcp_xattr_aware"] = isDcpXattrAware();
    ret["dcp_deleted_user_xattr"] = isDcpDeletedUserXattr();
    ret["dcp_no_value"] = isDcpNoValue();
    ret["max_reqs_per_event"] = max_reqs_per_event;
    ret["nevents"] = numEvents;
    ret["state"] = getStateName();

    nlohmann::json libevt;
    libevt["registered"] = isRegisteredInLibevent();
    libevt["ev_flags"] = event_mask_to_json(ev_flags);
    libevt["which"] = event_mask_to_json(currentEvent);
    ret["libevent"] = libevt;

    if (read) {
        ret["read"] = read->to_json();
    }

    if (write) {
        ret["write"] = write->to_json();
    }

    ret["write_and_go"] = std::string(stateMachine.getStateName(write_and_go));

    nlohmann::json iovobj;
    iovobj["size"] = iov.size();
    iovobj["used"] = iovused;
    ret["iov"] = iovobj;

    nlohmann::json msg;
    msg["used"] = msglist.size();
    msg["curr"] = msgcurr;
    msg["bytes"] = msgbytes;
    ret["msglist"] = msg;

    nlohmann::json ilist;
    ilist["size"] = reservedItems.size();
    ret["itemlist"] = ilist;

    nlohmann::json talloc;
    talloc["size"] = temp_alloc.size();
    ret["temp_alloc_list"] = talloc;

    ret["ssl"] = ssl.toJSON();
    ret["total_recv"] = totalRecv;
    ret["total_send"] = totalSend;

    ret["datatype"] = mcbp::datatype::to_string(datatype.getRaw()).c_str();

    return ret;
}

void Connection::setDCP(bool dcp) {
    Connection::dcp = dcp;

    if (isSslEnabled()) {
        try {
            // Make sure that we have space for up to a single TLS frame
            // in our send buffer (so that we can stick all of the mutations
            // in that buffer)
            write->ensureCapacity(TlsFrameSize);
        } catch (const std::bad_alloc&) {
        }
    }
}

void Connection::restartAuthentication() {
    if (authenticated && domain == cb::sasl::Domain::External) {
        externalAuthManager->logoff(username);
    }
    sasl_conn.reset();
    setInternal(false);
    authenticated = false;
    username = "";
}

cb::engine_errc Connection::dropPrivilege(cb::rbac::Privilege privilege) {
    if (privilegeContext.dropPrivilege(privilege)) {
        return cb::engine_errc::success;
    }

    return cb::engine_errc::no_access;
}

cb::rbac::PrivilegeAccess Connection::checkPrivilege(
        cb::rbac::Privilege privilege, Cookie& cookie) {
    cb::rbac::PrivilegeAccess ret;
    unsigned int retries = 0;
    const unsigned int max_retries = 100;

    while ((ret = privilegeContext.check(privilege)) ==
                   cb::rbac::PrivilegeAccess::Stale &&
           retries < max_retries) {
        ++retries;
        const auto opcode = cookie.getRequest(Cookie::PacketContent::Header)
                                    .getClientOpcode();
        const std::string command(to_string(opcode));

        // The privilege context we had could have been a dummy entry
        // (created when the client connected, and used until the
        // connection authenticates). Let's try to automatically update it,
        // but let the client deal with whatever happens after
        // a single update.
        try {
            privilegeContext = cb::rbac::createContext(
                    getUsername(), getDomain(), all_buckets[bucketIndex].name);
        } catch (const cb::rbac::NoSuchBucketException&) {
            // Remove all access to the bucket
            privilegeContext =
                    cb::rbac::createContext(getUsername(), getDomain(), "");
            LOG_INFO(
                    "{}: RBAC: Connection::checkPrivilege({}) {} No access "
                    "to "
                    "bucket [{}]. command: [{}] new privilege set: {}",
                    getId(),
                    to_string(privilege),
                    getDescription(),
                    all_buckets[bucketIndex].name,
                    command,
                    privilegeContext.to_string());
        } catch (const cb::rbac::Exception& error) {
            LOG_WARNING(
                    "{}: RBAC: Connection::checkPrivilege({}) {}: An "
                    "exception occurred. command: [{}] bucket: [{}] UUID:"
                    "[{}] message: {}",
                    getId(),
                    to_string(privilege),
                    getDescription(),
                    command,
                    all_buckets[bucketIndex].name,
                    cookie.getEventId(),
                    error.what());
            // Add a textual error as well
            cookie.setErrorContext("An exception occurred. command: [" +
                                   command + "]");
            return cb::rbac::PrivilegeAccess::Fail;
        }
    }

    if (retries == max_retries) {
        LOG_INFO(
                "{}: RBAC: Gave up rebuilding privilege context after {} "
                "times. Let the client handle the stale authentication "
                "context",
                getId(),
                retries);

    } else if (retries > 1) {
        LOG_INFO("{}: RBAC: Had to rebuild privilege context {} times",
                 getId(),
                 retries);
    }

    if (ret == cb::rbac::PrivilegeAccess::Fail) {
        const auto opcode = cookie.getRequest(Cookie::PacketContent::Header)
                                    .getClientOpcode();
        const std::string command(to_string(opcode));
        const std::string privilege_string = cb::rbac::to_string(privilege);
        const std::string context = privilegeContext.to_string();

        if (Settings::instance().isPrivilegeDebug()) {
            audit_privilege_debug(*this,
                                  command,
                                  all_buckets[bucketIndex].name,
                                  privilege_string,
                                  context);

            LOG_INFO(
                    "{}: RBAC privilege debug:{} command:[{}] bucket:[{}] "
                    "privilege:[{}] context:{}",
                    getId(),
                    getDescription(),
                    command,
                    all_buckets[bucketIndex].name,
                    privilege_string,
                    context);

            return cb::rbac::PrivilegeAccess::Ok;
        } else {
            LOG_INFO(
                    "{} RBAC {} missing privilege {} for {} in bucket:[{}] "
                    "with context: "
                    "{} UUID:[{}]",
                    getId(),
                    getDescription(),
                    privilege_string,
                    command,
                    all_buckets[bucketIndex].name,
                    context,
                    cookie.getEventId());
            // Add a textual error as well
            cookie.setErrorContext("Authorization failure: can't execute " +
                                   command + " operation without the " +
                                   privilege_string + " privilege");
        }
    }

    return ret;
}

Bucket& Connection::getBucket() const {
    return all_buckets[getBucketIndex()];
}

EngineIface* Connection::getBucketEngine() const {
    return getBucket().getEngine();
}

ENGINE_ERROR_CODE Connection::remapErrorCode(ENGINE_ERROR_CODE code) {
    if (xerror_support) {
        return code;
    }

    // Check our whitelist
    switch (code) {
    case ENGINE_SUCCESS: // FALLTHROUGH
    case ENGINE_KEY_ENOENT: // FALLTHROUGH
    case ENGINE_KEY_EEXISTS: // FALLTHROUGH
    case ENGINE_ENOMEM: // FALLTHROUGH
    case ENGINE_NOT_STORED: // FALLTHROUGH
    case ENGINE_EINVAL: // FALLTHROUGH
    case ENGINE_ENOTSUP: // FALLTHROUGH
    case ENGINE_EWOULDBLOCK: // FALLTHROUGH
    case ENGINE_E2BIG: // FALLTHROUGH
    case ENGINE_DISCONNECT: // FALLTHROUGH
    case ENGINE_NOT_MY_VBUCKET: // FALLTHROUGH
    case ENGINE_TMPFAIL: // FALLTHROUGH
    case ENGINE_ERANGE: // FALLTHROUGH
    case ENGINE_ROLLBACK: // FALLTHROUGH
    case ENGINE_EBUSY: // FALLTHROUGH
    case ENGINE_DELTA_BADVAL: // FALLTHROUGH
    case ENGINE_PREDICATE_FAILED:
    case ENGINE_FAILED:
        return code;

    case ENGINE_LOCKED:
        return ENGINE_KEY_EEXISTS;
    case ENGINE_LOCKED_TMPFAIL:
        return ENGINE_TMPFAIL;
    case ENGINE_UNKNOWN_COLLECTION:
    case ENGINE_COLLECTIONS_MANIFEST_IS_AHEAD:
        return isCollectionsSupported() ? code : ENGINE_EINVAL;

    case ENGINE_EACCESS:break;
    case ENGINE_NO_BUCKET:break;
    case ENGINE_AUTH_STALE:break;
    case ENGINE_DURABILITY_INVALID_LEVEL:
    case ENGINE_DURABILITY_IMPOSSIBLE:
    case ENGINE_SYNC_WRITE_PENDING:
        break;
    case ENGINE_SYNC_WRITE_IN_PROGRESS:
    case ENGINE_SYNC_WRITE_RECOMMIT_IN_PROGRESS:
        // we can return tmpfail to old clients and have them retry the
        // operation
        return ENGINE_TMPFAIL;
    case ENGINE_SYNC_WRITE_AMBIGUOUS:
    case ENGINE_DCP_STREAMID_INVALID:
        break;
    }

    // Seems like the rest of the components in our system isn't
    // prepared to receive access denied or authentincation stale.
    // For now we should just disconnect them
    auto errc = cb::make_error_condition(cb::engine_errc(code));
    LOG_WARNING(
            "{} - Client {} not aware of extended error code ({}). "
            "Disconnecting",
            getId(),
            getDescription().c_str(),
            errc.message().c_str());
    setTerminationReason("XError not enabled on client");

    return ENGINE_DISCONNECT;
}

void Connection::resetUsernameCache() {
    if (sasl_conn.isInitialized()) {
        username = sasl_conn.getUsername();
        domain = sasl_conn.getDomain();
    } else {
        username = "unknown";
        domain = cb::sasl::Domain::Local;
    }

    updateDescription();
}

void Connection::updateDescription() {
    description.assign("[ " + getPeername() + " - " + getSockname());
    if (authenticated) {
        description += " (";
        if (isInternal()) {
            description += "System, ";
        }
        description += cb::tagUserData(getUsername());

        if (domain == cb::sasl::Domain::External) {
            description += " (LDAP)";
        }
        description += ")";
    } else {
        description += " (not authenticated)";
    }
    description += " ]";
}

void Connection::setBucketIndex(int bucketIndex) {
    Connection::bucketIndex.store(bucketIndex, std::memory_order_relaxed);

    // Update the privilege context. If a problem occurs within the RBAC
    // module we'll assign an empty privilege context to the connection.
    try {
        if (authenticated) {
            // The user have logged in, so we should create a context
            // representing the users context in the desired bucket.
            privilegeContext = cb::rbac::createContext(
                    username, getDomain(), all_buckets[bucketIndex].name);
        } else if (is_default_bucket_enabled() &&
                   strcmp("default", all_buckets[bucketIndex].name) == 0) {
            // We've just connected to the _default_ bucket, _AND_ the client
            // is unknown.
            // Personally I think the "default bucket" concept is a really
            // really bad idea, but we need to be backwards compatible for
            // a while... lets look up a profile named "default" and
            // assign that. It should only contain access to the default
            // bucket.
            privilegeContext = cb::rbac::createContext(
                    "default", getDomain(), all_buckets[bucketIndex].name);
        } else {
            // The user has not authenticated, and this isn't for the
            // "default bucket". Assign an empty profile which won't give
            // you any privileges.
            privilegeContext = cb::rbac::PrivilegeContext{getDomain()};
        }
    } catch (const cb::rbac::Exception&) {
        privilegeContext = cb::rbac::PrivilegeContext{getDomain()};
    }

    if (bucketIndex == 0) {
        // If we're connected to the no bucket we should return
        // no bucket instead of EACCESS. Lets give the connection all
        // possible bucket privileges
        privilegeContext.setBucketPrivileges();
    }
}

void Connection::addCpuTime(std::chrono::nanoseconds ns) {
    total_cpu_time += ns;
    min_sched_time = std::min(min_sched_time, ns);
    max_sched_time = std::max(min_sched_time, ns);
}

void Connection::enqueueServerEvent(std::unique_ptr<ServerEvent> event) {
    server_events.push(std::move(event));
}

bool Connection::unregisterEvent() {
    if (!registered_in_libevent) {
        LOG_WARNING(
                "Connection::unregisterEvent: Not registered in libevent - "
                "ignoring unregister attempt");
        return false;
    }

    cb_assert(socketDescriptor != INVALID_SOCKET);

    if (event_del(event.get()) == -1) {
        LOG_WARNING("Failed to remove connection to libevent: {}",
                    cb_strerror());
        return false;
    }

    registered_in_libevent = false;
    return true;
}

bool Connection::registerEvent() {
    if (registered_in_libevent) {
        LOG_WARNING(
                "Connection::registerEvent: Already registered in"
                " libevent - ignoring register attempt");
        return false;
    }

    if (event_add(event.get(), nullptr) == -1) {
        LOG_WARNING("Failed to add connection to libevent: {}", cb_strerror());
        return false;
    }

    registered_in_libevent = true;
    return true;
}

bool Connection::updateEvent(const short new_flags) {
    if (ssl.isEnabled() && ssl.isConnected() && (new_flags & EV_READ)) {
        /*
         * If we want more data and we have SSL, that data might be inside
         * SSL's internal buffers rather than inside the socket buffer. In
         * that case signal an EV_READ event without actually polling the
         * socket.
         */
        if (ssl.havePendingInputData()) {
            // signal a call to the handler
            event_active(event.get(), EV_READ, 0);
            return true;
        }
    }

    if (ev_flags == new_flags) {
        // We do "cache" the current libevent state (using EV_PERSIST) to avoid
        // having to re-register it when it doesn't change (which it mostly
        // don't).
        return true;
    }

    if (!unregisterEvent()) {
        LOG_WARNING(
                "{}: Failed to remove connection from event notification "
                "library. Shutting down connection {}",
                getId(),
                getDescription());
        return false;
    }

    if (event_assign(event.get(),
                     base,
                     socketDescriptor,
                     new_flags,
                     event_handler,
                     reinterpret_cast<void*>(this)) == -1) {
        LOG_WARNING(
                "{}: Failed to set up event notification. "
                "Shutting down connection {}",
                getId(),
                getDescription());
        return false;
    }
    ev_flags = new_flags;

    if (!registerEvent()) {
        LOG_WARNING(
                "{}: Failed to add connection to the event notification "
                "library. Shutting down connection {}",
                getId(),
                getDescription());
        return false;
    }

    return true;
}

bool Connection::initializeEvent() {
    short event_flags = (EV_READ | EV_PERSIST);

    event.reset(event_new(base,
                          socketDescriptor,
                          event_flags,
                          event_handler,
                          reinterpret_cast<void*>(this)));

    if (!event) {
        throw std::bad_alloc();
    }
    ev_flags = event_flags;

    return registerEvent();
}

void Connection::shrinkBuffers() {
    // We share the buffers with the thread, so we don't need to worry
    // about the read and write buffer.

    if (msglist.size() > MSG_LIST_HIGHWAT) {
        try {
            msglist.resize(MSG_LIST_INITIAL);
            msglist.shrink_to_fit();
        } catch (const std::bad_alloc&) {
            LOG_WARNING("{}: Failed to shrink msglist down to {} elements.",
                        getId(),
                        MSG_LIST_INITIAL);
        }
    }

    if (iov.size() > IOV_LIST_HIGHWAT) {
        try {
            iov.resize(IOV_LIST_INITIAL);
            iov.shrink_to_fit();
        } catch (const std::bad_alloc&) {
            LOG_WARNING("{}: Failed to shrink iov down to {} elements.",
                        getId(),
                        IOV_LIST_INITIAL);
        }
    }
}

void Connection::setAuthenticated(bool authenticated) {
    Connection::authenticated = authenticated;
    if (authenticated) {
        updateDescription();
        privilegeContext = cb::rbac::createContext(username, getDomain(), "");
    } else {
        resetUsernameCache();
        privilegeContext = cb::rbac::PrivilegeContext{getDomain()};
    }
}

bool Connection::tryAuthFromSslCert(const std::string& userName) {
    username.assign(userName);
    domain = cb::sasl::Domain::Local;

    try {
        auto context =
                cb::rbac::createInitialContext(getUsername(), getDomain());
        setAuthenticated(true);
        setInternal(context.second);
        audit_auth_success(*this);
        LOG_INFO(
                "{}: Client {} authenticated as '{}' via X509 "
                "certificate",
                getId(),
                getPeername(),
                cb::UserDataView(getUsername()));
        // Connections authenticated by using X.509 certificates should not
        // be able to use SASL to change it's identity.
        saslAuthEnabled = false;
    } catch (const cb::rbac::NoSuchUserException& e) {
        setAuthenticated(false);
        LOG_WARNING("{}: User [{}] is not defined as a user in Couchbase",
                    getId(),
                    cb::UserDataView(e.what()));
        return false;
    }
    return true;
}

void Connection::logSslErrorInfo(const std::string& method, int rval) {
    const int error = ssl.getError(rval);
    unsigned long code = ERR_peek_error();
    if (code == 0) {
        LOG_WARNING("{}: ERROR: {} returned {} with error {}",
                    getId(),
                    method,
                    rval,
                    error);
        return;
    }

    try {
        std::string errmsg(method + "() returned " +
                           std::to_string(rval) + " with error " +
                           std::to_string(error));
        while ((code = ERR_get_error()) != 0) {
            std::vector<char> ssl_err(1024);
            ERR_error_string_n(
                               code, ssl_err.data(), ssl_err.size());
            LOG_WARNING("{}: {}: {}", getId(), errmsg, ssl_err.data());
        }
    } catch (const std::bad_alloc&) {
        // unable to print error message; continue.
        LOG_WARNING("{}: {}() returned {} with error {}",
                    getId(),
                    method,
                    rval,
                    error);
    }
}

int Connection::sslPreConnection() {
    int r = ssl.accept();
    if (r == 1) {
        ssl.drainBioSendPipe(socketDescriptor);
        ssl.setConnected();
        auto certResult = ssl.getCertUserName();
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
                associate_bucket(*this, "default");
            }
            break;
        case cb::x509::Status::Success:
            if (!tryAuthFromSslCert(certResult.second)) {
                disconnect = true;
                // Don't print an error message... already logged
                certResult.second.resize(0);
            }
        }
        if (disconnect) {
            // Set the username to "[unknown]" if we failed to pick
            // out a username from the certificate to avoid the
            // audit event being "empty"
            if (certResult.first == cb::x509::Status::NotPresent) {
                audit_auth_failure(
                        *this, "Client did not provide an X.509 certificate");
            } else {
                audit_auth_failure(
                        *this,
                        "Failed to use client provided X.509 certificate");
            }
            cb::net::set_econnreset();
            if (!certResult.second.empty()) {
                LOG_WARNING(
                        "{}: SslPreConnection: disconnection client due to"
                        " error [{}]",
                        getId(),
                        certResult.second);
            }
            return -1;
        }

         LOG_INFO("{}: Using SSL cipher:{}",
                 getId(),
                 ssl.getCurrentCipherName());
    } else {
        if (ssl.getError(r) == SSL_ERROR_WANT_READ) {
            ssl.drainBioSendPipe(socketDescriptor);
            cb::net::set_ewouldblock();
            return -1;
        } else {
            logSslErrorInfo("SSL_accept", r);
            cb::net::set_econnreset();
            return -1;
        }
    }

    return 0;
}

int Connection::recv(char* dest, size_t nbytes) {
    if (nbytes == 0) {
        throw std::logic_error("Connection::recv: Can't read 0 bytes");
    }

    int res = -1;
    if (ssl.isEnabled()) {
        ssl.drainBioRecvPipe(socketDescriptor);

        if (ssl.hasError()) {
            cb::net::set_econnreset();
            return -1;
        }

        if (!ssl.isConnected()) {
            res = sslPreConnection();
            if (res == -1) {
                return -1;
            }
        }

        /* The SSL negotiation might be complete at this time */
        if (ssl.isConnected()) {
            res = sslRead(dest, nbytes);
        }
    } else {
        res = (int)::cb::net::recv(socketDescriptor, dest, nbytes, 0);
        if (res > 0) {
            totalRecv += res;
        }
    }

    return res;
}

ssize_t Connection::sendmsg(struct msghdr* m) {
    ssize_t res = 0;
    if (ssl.isEnabled()) {
        for (int ii = 0; ii < int(m->msg_iovlen); ++ii) {
            int n = sslWrite(reinterpret_cast<char*>(m->msg_iov[ii].iov_base),
                             m->msg_iov[ii].iov_len);
            if (n > 0) {
                res += n;
                if (n != int(m->msg_iov[ii].iov_len)) {
                    // We didnt' send the entire chunk. return the number
                    // of bytes sent so far to the caller and let them
                    // deal with adjusting the pointers and retry
                    return res;
                }
            } else {
                // We failed to send the data over ssl. it might be
                // because the underlying socket buffer is full, or
                // if there is a real error with the socket or inside
                // OpenSSL. If the error is because we the network
                // is full we'll return the number of bytes we've sent
                // so far (so that it may adjust the iov_base and iov_len
                // fields before it'll try to call us again and the
                // send will most likely fail again, but this we'll
                // return -1 and when the caller checks it'll see it is
                // because the network buffer is full).
                auto error = cb::net::get_socket_error();
                if (cb::net::is_blocking(error) && res > 0) {
                    return res;
                }
                return -1;
            }
        }
        ssl.drainBioSendPipe(socketDescriptor);
        return res;
    } else {
        res = cb::net::sendmsg(socketDescriptor, m, 0);
        if (res > 0) {
            totalSend += res;
        }
    }

    return res;
}

/**
 * Adjust the msghdr by "removing" n bytes of data from it.
 *
 * @param m the msgheader to update
 * @param nbytes
 * @return the number of bytes left in the current iov entry
 */
size_t adjust_msghdr(cb::Pipe& pipe, struct msghdr* m, ssize_t nbytes) {
    auto rbuf = pipe.rdata();

    // We've written some of the data. Remove the completed
    // iovec entries from the list of pending writes.
    while (m->msg_iovlen > 0 && nbytes >= ssize_t(m->msg_iov->iov_len)) {
        if (rbuf.data() == static_cast<const uint8_t*>(m->msg_iov->iov_base)) {
            pipe.consumed(m->msg_iov->iov_len);
            rbuf = pipe.rdata();
        }
        nbytes -= (ssize_t)m->msg_iov->iov_len;
        m->msg_iovlen--;
        m->msg_iov++;
    }

    // Might have written just part of the last iovec entry;
    // adjust it so the next write will do the rest.
    if (nbytes > 0) {
        if (rbuf.data() == static_cast<const uint8_t*>(m->msg_iov->iov_base)) {
            pipe.consumed(nbytes);
        }
        m->msg_iov->iov_base =
                (void*)((unsigned char*)m->msg_iov->iov_base + nbytes);
        m->msg_iov->iov_len -= nbytes;
    }

    return m->msg_iov->iov_len;
}

Connection::TransmitResult Connection::transmit() {
    if (ssl.isEnabled()) {
        // We use OpenSSL to write data into a buffer before we send it
        // over the wire... Lets go ahead and drain that BIO pipe before
        // we may do anything else.
        ssl.drainBioSendPipe(socketDescriptor);
        if (ssl.morePendingOutput()) {
            if (ssl.hasError() || !updateEvent(EV_WRITE | EV_PERSIST)) {
                setTerminationReason("Failed to send data to client");
                setState(StateMachine::State::closing);
                return TransmitResult::HardError;
            }
            return TransmitResult::SoftError;
        }

        // The output buffer is completely drained (well, put in the kernel
        // buffer to send to the client). Go ahead and send more data
    }

    while (msgcurr < msglist.size() && msglist[msgcurr].msg_iovlen == 0) {
        /* Finished writing the current msg; advance to the next. */
        msgcurr++;
    }

    if (msgcurr < msglist.size()) {
        ssize_t res;
        struct msghdr* m = &msglist[msgcurr];

        res = sendmsg(m);
        auto error = cb::net::get_socket_error();
        if (res > 0) {
            get_thread_stats(this)->bytes_written += res;

            if (adjust_msghdr(*write, m, res) == 0) {
                msgcurr++;
                if (msgcurr == msglist.size()) {
                    // We sent the final chunk of data.. In our SSL connections
                    // we might however have data spooled in the SSL buffers
                    // which needs to be drained before we may consider the
                    // transmission complete (note that our sendmsg tried
                    // to drain the buffers before returning).
                    if (ssl.isEnabled() && ssl.morePendingOutput()) {
                        if (ssl.hasError() ||
                            !updateEvent(EV_WRITE | EV_PERSIST)) {
                            setTerminationReason(
                                    "Failed to send data to client");
                            setState(StateMachine::State::closing);
                            return TransmitResult::HardError;
                        }
                        return TransmitResult::SoftError;
                    }
                    return TransmitResult::Complete;
                }
            }

            return TransmitResult::Incomplete;
        }

        if (res == -1 && cb::net::is_blocking(error)) {
            if (!updateEvent(EV_WRITE | EV_PERSIST)) {
                setTerminationReason("Failed to send data to client");
                setState(StateMachine::State::closing);
                return TransmitResult::HardError;
            }
            return TransmitResult::SoftError;
        }

        // if res == 0 or res == -1 and error is not EAGAIN or EWOULDBLOCK,
        // we have a real error, on which we close the connection
        if (res == -1) {
            if (cb::net::is_closed_conn(error)) {
                LOG_INFO("{}: Failed to send data; peer closed the connection",
                         getId());
            } else {
                LOG_WARNING("Failed to write, and not due to blocking: {}",
                            cb_strerror(error));
            }
        } else {
            // sendmsg should return the number of bytes written, but we
            // sent 0 bytes. That shouldn't be possible unless we
            // requested to write 0 bytes (otherwise we should have gotten
            // -1 with EWOULDBLOCK)
            // Log the request buffer so that we can look into this
            LOG_WARNING("{} - sendmsg returned 0", socketDescriptor);
            for (int ii = 0; ii < int(m->msg_iovlen); ++ii) {
                LOG_WARNING(
                        "\t{} - {}", socketDescriptor, m->msg_iov[ii].iov_len);
            }
        }

        setTerminationReason("Failed to send data to client");
        setState(StateMachine::State::closing);
        return TransmitResult::HardError;
    } else {
        return TransmitResult::Complete;
    }
}

/**
 * To protect us from someone flooding a connection with bogus data causing
 * the connection to eat up all available memory, break out and start
 * looking at the data I've got after a number of reallocs...
 */
Connection::TryReadResult Connection::tryReadNetwork() {
    // When we get here we've either got an empty buffer, or we've got
    // a buffer with less than a packet header filled in.
    //
    // Verify that assumption!!!
    if (read->rsize() >= sizeof(cb::mcbp::Request)) {
        // The above don't hold true ;)
        throw std::logic_error(
                "tryReadNetwork: Expected the input buffer to be empty or "
                "contain a partial header");
    }

    // Make sure we can fit the header into the input buffer
    try {
        read->ensureCapacity(sizeof(cb::mcbp::Request) - read->rsize());
    } catch (const std::bad_alloc&) {
        LOG_WARNING(
                "{}: Failed to allocate memory for package header. Closing "
                "connection {}",
                getId(),
                getDescription());
        return TryReadResult::MemoryError;
    }

    Connection* c = this;
    const auto res = read->produce([c](cb::byte_buffer buffer) -> ssize_t {
        return c->recv(reinterpret_cast<char*>(buffer.data()), buffer.size());
    });

    if (res > 0) {
        get_thread_stats(this)->bytes_read += res;
        return TryReadResult::DataReceived;
    }

    if (res == 0) {
        LOG_DEBUG(
                "{} Closing connection as the other side closed the "
                "connection {}",
                getId(),
                getDescription());
        return TryReadResult::SocketClosed;
    }

    const auto error = cb::net::get_socket_error();
    if (cb::net::is_blocking(error)) {
        return TryReadResult::NoDataReceived;
    }

    // There was an error reading from the socket. There isn't much we
    // can do about that apart from logging it and close the connection.
    // Keep this as INFO as it isn't a problem with the memcached server,
    // it is a network issue (or a bad client not closing the connection
    // cleanly)
    LOG_INFO("{} Closing connection {} due to read error: {}",
             getId(),
             getDescription(),
             cb_strerror(error));
    return TryReadResult::SocketError;
}

int Connection::sslRead(char* dest, size_t nbytes) {
    int ret = 0;

    while (ret < int(nbytes)) {
        int n;
        ssl.drainBioRecvPipe(socketDescriptor);
        if (ssl.hasError()) {
            cb::net::set_econnreset();
            return -1;
        }
        n = ssl.read(dest + ret, (int)(nbytes - ret));
        if (n > 0) {
            ret += n;
        } else {
            /* n < 0 and n == 0 require a check of SSL error*/
            const int error = ssl.getError(n);

            switch (error) {
            case SSL_ERROR_WANT_READ:
                /*
                 * Drain the buffers and retry if we've got data in
                 * our input buffers
                 */
                if (ssl.moreInputAvailable()) {
                    /* our recv buf has data feed the BIO */
                    ssl.drainBioRecvPipe(socketDescriptor);
                } else if (ret > 0) {
                    /* nothing in our recv buf, return what we have */
                    return ret;
                } else {
                    cb::net::set_ewouldblock();
                    return -1;
                }
                break;

            case SSL_ERROR_ZERO_RETURN:
                /* The TLS/SSL connection has been closed (cleanly). */
                return 0;

            default:
                logSslErrorInfo("SSL_read", n);
                cb::net::set_econnreset();
                return -1;
            }
        }
    }

    return ret;
}

int Connection::sslWrite(const char* src, size_t nbytes) {
    // Start by trying to send everything we've already got
    // buffered in our bio
    ssl.drainBioSendPipe(socketDescriptor);
    if (ssl.hasError()) {
        cb::net::set_econnreset();
        return -1;
    }

    // If the network socket is full there isn't much point
    // of trying to add more to SSL
    if (ssl.morePendingOutput()) {
        cb::net::set_ewouldblock();
        return -1;
    }

    // We've got an empty buffer for SSL to operate on,
    // so lets get to it.
    do {
        const auto n = ssl.write(src, int(nbytes));
        if (n > 0) {
            // we've successfully sent some bytes to
            // SSL. Lets try to flush it to the network
            // socket buffers.
            ssl.drainBioSendPipe(socketDescriptor);
            if (ssl.hasError()) {
                cb::net::set_econnreset();
                return -1;
            }
            // Return the number of bytes submitted to SSL
            return n;
        } else if (n == 0) {
            // Closed connection.
            cb::net::set_econnreset();
            return -1;
        } else {
            const int error = ssl.getError(n);
            if (error == SSL_ERROR_WANT_WRITE) {
                ssl.drainBioSendPipe(socketDescriptor);
                if (ssl.morePendingOutput()) {
                    cb::net::set_ewouldblock();
                    return -1;
                }
                // We've got space in the network buffer.
                // retry the operation (and openssl will
                // try to fill the network buffer again)
            } else {
                // We enountered an error we don't have code
                // to handle. Reset the connection
                logSslErrorInfo("SSL_write", n);
                cb::net::set_econnreset();
                return -1;
            }
        }
    } while (true);
}

bool Connection::useCookieSendResponse(std::size_t size) const {
    // The limit for when to copy the data into a separate response
    // buffer instead of using the IO vector to send the data. The limit
    // is currently hardcoded, but it should probably be possible to tune
    // the value. (when the cost of copying exceeds the cost of creating
    // two (or 3) extra TLS frames. Start by keep it fixed to see if it
    // makes any difference in showfast.
    static constexpr std::size_t SslCopyLimit = 4096;
    return isSslEnabled() && size < SslCopyLimit;
}

bool Connection::dcpUseWriteBuffer(size_t size) const {
    return isSslEnabled() && size < write->wsize();
}

void Connection::addMsgHdr(bool reset) {
    if (reset) {
        msgcurr = 0;
        msglist.clear();
        iovused = 0;
    }

    msglist.emplace_back();

    struct msghdr& msg = msglist.back();

    /* this wipes msg_iovlen, msg_control, msg_controllen, and
       msg_flags, the last 3 of which aren't defined on solaris: */
    memset(&msg, 0, sizeof(struct msghdr));

    msg.msg_iov = &iov.data()[iovused];

    msgbytes = 0;
    STATS_MAX(this, msgused_high_watermark, gsl::narrow<int>(msglist.size()));
}

void Connection::addIov(const void* buf, size_t len) {
    if (len == 0) {
        return;
    }

    struct msghdr* m = &msglist.back();

    /* We may need to start a new msghdr if this one is full. */
    if (m->msg_iovlen == IOV_MAX) {
        addMsgHdr(false);
    }

    ensureIovSpace();

    // Update 'm' as we may have added an additional msghdr
    m = &msglist.back();
    // If this entry is right after the previous one we can just
    // extend the previous entry instead of adding a new one
    bool addNewEntry = true;
    if (m->msg_iovlen > 0) {
        auto& prev = m->msg_iov[m->msg_iovlen - 1];
        const auto* p = static_cast<const char*>(prev.iov_base) + prev.iov_len;
        if (buf == p) {
            prev.iov_len += len;
            addNewEntry = false;
        }
    }

    if (addNewEntry) {
        m->msg_iov[m->msg_iovlen].iov_base = (void*)buf;
        m->msg_iov[m->msg_iovlen].iov_len = len;
        ++iovused;
        STATS_MAX(this, iovused_high_watermark, gsl::narrow<int>(getIovUsed()));
        m->msg_iovlen++;
    }

    msgbytes += len;
}

void Connection::releaseReservedItems() {
    auto* bucketEngine = getBucket().getEngine();
    for (auto* it : reservedItems) {
        bucketEngine->release(it);
    }
    reservedItems.clear();
}

void Connection::ensureIovSpace() {
    if (iovused < iov.size()) {
        // There is still size in the list
        return;
    }

    // Try to double the size of the array
    iov.resize(iov.size() * 2);

    /* Point all the msghdr structures at the new list. */
    size_t ii;
    int iovnum;
    for (ii = 0, iovnum = 0; ii < msglist.size(); ii++) {
        msglist[ii].msg_iov = &iov[iovnum];
        iovnum += msglist[ii].msg_iovlen;
    }
}

bool Connection::enableSSL(const std::string& cert, const std::string& pkey) {
    if (ssl.enable(cert, pkey)) {
        if (Settings::instance().getVerbose() > 1) {
            ssl.dumpCipherList(getId());
        }

        return true;
    }

    return false;
}

Connection::Connection(FrontEndThread& thr)
    : socketDescriptor(INVALID_SOCKET),
      connectedToSystemPort(false),
      base(nullptr),
      thread(thr),
      peername(R"({"ip":"unknown","port":0})"),
      sockname(R"({"ip":"unknown","port":0})"),
      stateMachine(*this),
      max_reqs_per_event(Settings::instance().getRequestsPerEventNotification(
              EventPriority::Default)) {
    updateDescription();
    cookies.emplace_back(std::unique_ptr<Cookie>{new Cookie(*this)});
    setConnectionId(peername.c_str());
    msglist.reserve(MSG_LIST_INITIAL);
    iov.resize(IOV_LIST_INITIAL);
}

Connection::Connection(SOCKET sfd,
                       event_base* b,
                       const ListeningPort& ifc,
                       FrontEndThread& thr)
    : socketDescriptor(sfd),
      connectedToSystemPort(ifc.system),
      base(b),
      thread(thr),
      parent_port(ifc.port),
      peername(cb::net::getPeerNameAsJson(socketDescriptor).dump()),
      sockname(cb::net::getSockNameAsJson(socketDescriptor).dump()),
      stateMachine(*this),
      max_reqs_per_event(Settings::instance().getRequestsPerEventNotification(
              EventPriority::Default)) {
    setTcpNoDelay(true);
    updateDescription();
    cookies.emplace_back(std::unique_ptr<Cookie>{new Cookie(*this)});
    msglist.reserve(MSG_LIST_INITIAL);
    iov.resize(IOV_LIST_INITIAL);

    if (ifc.isSslPort()) {
        if (!enableSSL(ifc.sslCert, ifc.sslKey)) {
            throw std::runtime_error(std::to_string(getId()) +
                                     " Failed to enable SSL");
        }
    }

    if (!initializeEvent()) {
        throw std::runtime_error("Failed to initialize event structure");
    }
    setConnectionId(peername.c_str());
}

Connection::~Connection() {
    cb::audit::addSessionTerminated(*this);

    if (connectedToSystemPort) {
        --stats.system_conns;
    }
    if (authenticated && domain == cb::sasl::Domain::External) {
        externalAuthManager->logoff(username);
    }

    releaseReservedItems();
    for (auto* ptr : temp_alloc) {
        cb_free(ptr);
    }
    if (socketDescriptor != INVALID_SOCKET) {
        LOG_DEBUG("{} - Closing socket descriptor", getId());
        safe_close(socketDescriptor);
    }
}

void Connection::setTerminationReason(std::string reason) {
    if (terminationReason.empty()) {
        terminationReason = std::move(reason);
    } else {
        terminationReason.append(";");
        terminationReason.append(reason);
    }
}

void Connection::setState(StateMachine::State next_state) {
    stateMachine.setCurrentState(next_state);
}

void Connection::runStateMachinery() {
    if (Settings::instance().getVerbose() > 1) {
        do {
            LOG_DEBUG("{} - Running task: {}",
                      getId(),
                      stateMachine.getCurrentStateName());
        } while (stateMachine.execute());
    } else {
        while (stateMachine.execute()) {
            // empty
        }
    }
}

void Connection::setAgentName(cb::const_char_buffer name) {
    auto size = std::min(name.size(), agentName.size() - 1);
    std::copy(name.begin(), name.begin() + size, agentName.begin());
    agentName[size] = '\0';
}

void Connection::setConnectionId(cb::const_char_buffer uuid) {
    auto size = std::min(uuid.size(), connectionId.size() - 1);
    std::copy(uuid.begin(), uuid.begin() + size, connectionId.begin());
    // the uuid string shall always be zero terminated
    connectionId[size] = '\0';
}

bool Connection::shouldDelete() {
    return getState() == StateMachine::State ::destroyed;
}

void Connection::setInternal(bool internal) {
    Connection::internal = internal;
}

size_t Connection::getNumberOfCookies() const {
    size_t ret = 0;
    for (const auto& cookie : cookies) {
        if (cookie) {
            ++ret;
        }
    }

    return ret;
}

bool Connection::isPacketAvailable() const {
    auto buffer = read->rdata();

    if (buffer.size() < sizeof(cb::mcbp::Request)) {
        // we don't have the header, so we can't even look at the body
        // length
        return false;
    }

    const auto* req = reinterpret_cast<const cb::mcbp::Request*>(buffer.data());
    return buffer.size() >= sizeof(cb::mcbp::Request) + req->getBodylen();
}

bool Connection::processServerEvents() {
    if (server_events.empty()) {
        return false;
    }

    const auto before = getState();

    // We're waiting for the next command to arrive from the client
    // and we've got a server event to process. Let's start
    // processing the server events (which might toggle our state)
    if (server_events.front()->execute(*this)) {
        server_events.pop();
    }

    return getState() != before;
}

void Connection::runEventLoop(short which) {
    conn_loan_buffers(this);
    currentEvent = which;
    numEvents = max_reqs_per_event;

    try {
        runStateMachinery();
    } catch (const std::exception& e) {
        setTerminationReason(std::string("Received exception: ") + e.what());
        bool logged = false;
        if (getState() == StateMachine::State::execute ||
            getState() == StateMachine::State::validate) {
            try {
                // Converting the cookie to json -> string could probably
                // cause too much memory allcation. We don't want that to
                // cause us to crash..
                std::stringstream ss;
                nlohmann::json array = nlohmann::json::array();
                for (const auto& cookie : cookies) {
                    if (cookie) {
                        try {
                            array.push_back(cookie->toJSON());
                        } catch (const std::exception&) {
                            // ignore
                        }
                    }
                }
                LOG_ERROR(
                        R"({}: exception occurred in runloop during packet execution. Cookie info: {} - closing connection ({}): {})",
                        getId(),
                        array.dump(),
                        getDescription(),
                        e.what());
                logged = true;
            } catch (const std::bad_alloc&) {
                // none
            }
        }

        if (!logged) {
            try {
                LOG_ERROR(
                        R"({}: exception occurred in runloop (state: "{}") - closing connection ({}): {})",
                        getId(),
                        getStateName(),
                        getDescription(),
                        e.what());
            } catch (const std::exception&) {
                // Ditch logging.. just shut down the connection
            }
        }

        setState(StateMachine::State::closing);
        /*
         * In addition to setting the state to conn_closing
         * we need to move execution foward by executing
         * conn_closing() and the subsequent functions
         * i.e. conn_pending_close() or conn_immediate_close()
         */
        try {
            runStateMachinery();
        } catch (const std::exception& e) {
            try {
                LOG_ERROR(
                        R"({}: exception occurred in runloop whilst attempting to close connection ({}): {})",
                        getId(),
                        getDescription(),
                        e.what());
            } catch (const std::exception&) {
                // Drop logging
            }
        }
    }

    conn_return_buffers(this);
}

bool Connection::close() {
    bool ewb = false;
    uint32_t rc = refcount;

    for (auto& cookie : cookies) {
        if (cookie) {
            rc += cookie->getRefcount();
            if (cookie->isEwouldblock()) {
                ewb = true;
            } else {
                cookie->reset();
            }
        }
    }

    if (getState() == StateMachine::State::closing) {
        // We don't want any network notifications anymore..
        if (registered_in_libevent) {
            unregisterEvent();
        }

        // Shut down the read end of the socket to avoid more data
        // to arrive
        shutdown(socketDescriptor, SHUT_RD);

        // Release all reserved items!
        releaseReservedItems();
    }

    // Notify interested parties that the connection is currently being
    // disconnected
    propagateDisconnect();

    if (isDCP()) {
        // DCP channels work a bit different.. they use the refcount
        // to track if it has a reference in the engine
        ewb = false;
    }

    if (rc > 1 || ewb) {
        setState(StateMachine::State::pending_close);
        return false;
    }
    setState(StateMachine::State::immediate_close);
    return true;
}

void Connection::propagateDisconnect() const {
    for (auto& cookie : cookies) {
        if (cookie) {
            perform_callbacks(ON_DISCONNECT, nullptr, cookie.get());
        }
    }
}

bool Connection::signalIfIdle() {
    for (const auto& c : cookies) {
        if (c->isEwouldblock()) {
            return false;
        }
    }

    if (stateMachine.isIdleState()) {
        thread.notification.push(this);
        notify_thread(thread);
        return true;
    }

    return false;
}

void Connection::setPriority(Connection::Priority priority) {
    Connection::priority.store(priority);
    switch (priority) {
    case Priority::High:
        max_reqs_per_event =
                Settings::instance().getRequestsPerEventNotification(
                        EventPriority::High);
        return;
    case Priority::Medium:
        max_reqs_per_event =
                Settings::instance().getRequestsPerEventNotification(
                        EventPriority::Medium);
        return;
    case Priority::Low:
        max_reqs_per_event =
                Settings::instance().getRequestsPerEventNotification(
                        EventPriority::Low);
        return;
    }
    throw std::invalid_argument("Unkown priority: " +
                                std::to_string(int(priority)));
}

bool Connection::selectedBucketIsXattrEnabled() const {
    auto* bucketEngine = getBucketEngine();
    if (bucketEngine) {
        return Settings::instance().isXattrEnabled() &&
               bucketEngine->isXattrEnabled();
    }
    return Settings::instance().isXattrEnabled();
}

ENGINE_ERROR_CODE Connection::add_packet_to_send_pipe(
        cb::const_byte_buffer packet) {
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    write->produce([this, packet, &ret](cb::byte_buffer buffer) -> size_t {
        if (buffer.size() < packet.size()) {
            ret = ENGINE_E2BIG;
            return 0;
        }

        std::copy(packet.begin(), packet.end(), buffer.begin());
        addIov(buffer.data(), packet.size());
        return packet.size();
    });

    return ret;
}

////////////////////////////////////////////////////////////////////////////
//                                                                        //
//                   DCP Message producer interface                       //
//                                                                        //
////////////////////////////////////////////////////////////////////////////

ENGINE_ERROR_CODE Connection::get_failover_log(uint32_t opaque, Vbid vbucket) {
    cb::mcbp::Request req = {};
    req.setMagic(cb::mcbp::Magic::ClientRequest);
    req.setOpcode(cb::mcbp::ClientOpcode::DcpGetFailoverLog);
    req.setOpaque(opaque);
    req.setVBucket(vbucket);

    return add_packet_to_send_pipe(req.getFrame());
}

ENGINE_ERROR_CODE Connection::stream_req(uint32_t opaque,
                                         Vbid vbucket,
                                         uint32_t flags,
                                         uint64_t start_seqno,
                                         uint64_t end_seqno,
                                         uint64_t vbucket_uuid,
                                         uint64_t snap_start_seqno,
                                         uint64_t snap_end_seqno,
                                         const std::string& request_value) {
    using Framebuilder = cb::mcbp::FrameBuilder<cb::mcbp::Request>;
    using cb::mcbp::Request;
    using cb::mcbp::request::DcpStreamReqPayload;

    auto size = sizeof(Request) + sizeof(DcpStreamReqPayload) +
                request_value.size();

    std::vector<uint8_t> buffer(size);

    Framebuilder builder({buffer.data(), buffer.size()});
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::DcpStreamReq);
    builder.setOpaque(opaque);
    builder.setVBucket(vbucket);

    DcpStreamReqPayload payload;
    payload.setFlags(flags);
    payload.setStartSeqno(start_seqno);
    payload.setEndSeqno(end_seqno);
    payload.setVbucketUuid(vbucket_uuid);
    payload.setSnapStartSeqno(snap_start_seqno);
    payload.setSnapEndSeqno(snap_end_seqno);

    builder.setExtras(
            {reinterpret_cast<const uint8_t*>(&payload), sizeof(payload)});

    if (request_value.empty()) {
        builder.setValue(request_value);
    }

    return add_packet_to_send_pipe(builder.getFrame()->getFrame());
}

ENGINE_ERROR_CODE Connection::add_stream_rsp(uint32_t opaque,
                                             uint32_t dialogopaque,
                                             cb::mcbp::Status status) {
    cb::mcbp::response::DcpAddStreamPayload extras;
    extras.setOpaque(dialogopaque);
    uint8_t buffer[sizeof(cb::mcbp::Response) + sizeof(extras)];
    cb::mcbp::ResponseBuilder builder({buffer, sizeof(buffer)});
    builder.setMagic(cb::mcbp::Magic::ClientResponse);
    builder.setOpcode(cb::mcbp::ClientOpcode::DcpAddStream);
    builder.setStatus(status);
    builder.setOpaque(opaque);
    builder.setExtras(extras.getBuffer());

    return add_packet_to_send_pipe(builder.getFrame()->getFrame());
}

ENGINE_ERROR_CODE Connection::marker_rsp(uint32_t opaque,
                                         cb::mcbp::Status status) {
    cb::mcbp::Response response{};
    response.setMagic(cb::mcbp::Magic::ClientResponse);
    response.setOpcode(cb::mcbp::ClientOpcode::DcpSnapshotMarker);
    response.setExtlen(0);
    response.setStatus(status);
    response.setBodylen(0);
    response.setOpaque(opaque);

    return add_packet_to_send_pipe(
            {reinterpret_cast<const uint8_t*>(&response), sizeof(response)});
}

ENGINE_ERROR_CODE Connection::set_vbucket_state_rsp(uint32_t opaque,
                                                    cb::mcbp::Status status) {
    uint8_t buffer[sizeof(cb::mcbp::Response)];
    cb::mcbp::ResponseBuilder builder({buffer, sizeof(buffer)});
    builder.setMagic(cb::mcbp::Magic::ClientResponse);
    builder.setOpcode(cb::mcbp::ClientOpcode::DcpSetVbucketState);
    builder.setStatus(status);
    builder.setOpaque(opaque);

    return add_packet_to_send_pipe(builder.getFrame()->getFrame());
}

ENGINE_ERROR_CODE Connection::stream_end(uint32_t opaque,
                                         Vbid vbucket,
                                         uint32_t flags,
                                         cb::mcbp::DcpStreamId sid) {
    using Framebuilder = cb::mcbp::FrameBuilder<cb::mcbp::Request>;
    using cb::mcbp::Request;
    using cb::mcbp::request::DcpStreamEndPayload;
    uint8_t buffer[sizeof(Request) + sizeof(DcpStreamEndPayload) +
                   sizeof(cb::mcbp::DcpStreamIdFrameInfo)];

    Framebuilder builder({buffer, sizeof(buffer)});
    builder.setMagic(sid ? cb::mcbp::Magic::AltClientRequest
                         : cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::DcpStreamEnd);
    builder.setOpaque(opaque);
    builder.setVBucket(vbucket);

    DcpStreamEndPayload payload;
    payload.setFlags(flags);

    builder.setExtras(
            {reinterpret_cast<const uint8_t*>(&payload), sizeof(payload)});

    if (sid) {
        cb::mcbp::DcpStreamIdFrameInfo framedSid(sid);
        builder.setFramingExtras(framedSid.getBuf());
    }

    return add_packet_to_send_pipe(builder.getFrame()->getFrame());
}

ENGINE_ERROR_CODE Connection::marker(uint32_t opaque,
                                     Vbid vbucket,
                                     uint64_t start_seqno,
                                     uint64_t end_seqno,
                                     uint32_t flags,
                                     boost::optional<uint64_t> hcs,
                                     boost::optional<uint64_t> mvs,
                                     cb::mcbp::DcpStreamId sid) {
    using Framebuilder = cb::mcbp::FrameBuilder<cb::mcbp::Request>;
    using cb::mcbp::Request;
    using cb::mcbp::request::DcpSnapshotMarkerV1Payload;
    using cb::mcbp::request::DcpSnapshotMarkerV2_0Value;
    using cb::mcbp::request::DcpSnapshotMarkerV2xPayload;

    // Allocate the buffer to be big enough for all cases, which will be the
    // v2.0 packet
    const auto size = sizeof(Request) + sizeof(cb::mcbp::DcpStreamIdFrameInfo) +
                      sizeof(DcpSnapshotMarkerV2xPayload) +
                      sizeof(DcpSnapshotMarkerV2_0Value);
    std::vector<uint8_t> buffer(size);

    Framebuilder builder({buffer.data(), buffer.size()});
    builder.setMagic(sid ? cb::mcbp::Magic::AltClientRequest
                         : cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::DcpSnapshotMarker);
    builder.setOpaque(opaque);
    builder.setVBucket(vbucket);

    if (sid) {
        cb::mcbp::DcpStreamIdFrameInfo framedSid(sid);
        builder.setFramingExtras(framedSid.getBuf());
    }

    cb::mcbp::encodeDcpSnapshotMarker(
            builder, start_seqno, end_seqno, flags, hcs, mvs);

    return add_packet_to_send_pipe(builder.getFrame()->getFrame());
}

ENGINE_ERROR_CODE Connection::mutation(uint32_t opaque,
                                       cb::unique_item_ptr it,
                                       Vbid vbucket,
                                       uint64_t by_seqno,
                                       uint64_t rev_seqno,
                                       uint32_t lock_time,
                                       uint8_t nru,
                                       cb::mcbp::DcpStreamId sid) {
    item_info info;
    if (!bucket_get_item_info(*this, it.get(), &info)) {
        LOG_WARNING("{}: Failed to get item info", getId());
        return ENGINE_FAILED;
    }

    char* root = reinterpret_cast<char*>(info.value[0].iov_base);
    cb::char_buffer value{root, info.value[0].iov_len};

    auto key = info.key;
    // The client doesn't support collections, so must not send an encoded key
    if (!isCollectionsSupported()) {
        key = key.makeDocKeyWithoutCollectionID();
    }

    cb::mcbp::request::DcpMutationPayload extras(
            by_seqno,
            rev_seqno,
            info.flags,
            gsl::narrow<uint32_t>(info.exptime),
            lock_time,
            nru);

    cb::mcbp::DcpStreamIdFrameInfo frameExtras(sid);

    const auto total = sizeof(extras) + key.size() + value.size() +
                       (sid ? sizeof(cb::mcbp::DcpStreamIdFrameInfo) : 0) +
                       sizeof(cb::mcbp::Request);
    if (dcpUseWriteBuffer(total)) {
        cb::mcbp::RequestBuilder builder(write->wdata());
        builder.setMagic(sid ? cb::mcbp::Magic::AltClientRequest
                             : cb::mcbp::Magic::ClientRequest);
        builder.setOpcode(cb::mcbp::ClientOpcode::DcpMutation);
        if (sid) {
            builder.setFramingExtras(frameExtras.getBuf());
        }
        builder.setExtras(extras.getBuffer());
        builder.setKey({key.data(), key.size()});
        builder.setValue(value);
        builder.setOpaque(opaque);
        builder.setVBucket(vbucket);
        builder.setCas(info.cas);
        builder.setDatatype(cb::mcbp::Datatype(info.datatype));

        auto packet = builder.getFrame()->getFrame();
        addIov(packet.data(), packet.size());
        write->produced(packet.size());
        return ENGINE_SUCCESS;
    }

    if (!reserveItem(it.get())) {
        LOG_WARNING("{}: Failed to grow item array", getId());
        return ENGINE_FAILED;
    }

    // we've reserved the item, and it'll be released when we're done sending
    // the item.
    it.release();

    cb::mcbp::Request req = {};
    req.setMagic(sid ? cb::mcbp::Magic::AltClientRequest
                     : cb::mcbp::Magic::ClientRequest);
    req.setOpcode(cb::mcbp::ClientOpcode::DcpMutation);
    req.setExtlen(gsl::narrow<uint8_t>(sizeof(extras)));
    req.setKeylen(gsl::narrow<uint16_t>(key.size()));
    req.setBodylen(gsl::narrow<uint32_t>(
            sizeof(extras) + key.size() + value.size() +
            (sid ? sizeof(cb::mcbp::DcpStreamIdFrameInfo) : 0)));
    req.setOpaque(opaque);
    req.setVBucket(vbucket);
    req.setCas(info.cas);
    req.setDatatype(cb::mcbp::Datatype(info.datatype));

    if (sid) {
        req.setFramingExtraslen(sizeof(cb::mcbp::DcpStreamIdFrameInfo));
    }

    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    write->produce([this, &req, &frameExtras, &extras, &value, &ret, &key, sid](
                           cb::byte_buffer wbuf) -> size_t {
        size_t headerSize = sizeof(extras) + sizeof(req);
        if (sid) {
            headerSize += sizeof(frameExtras);
        }
        if (wbuf.size() < headerSize) {
            ret = ENGINE_E2BIG;
            return 0;
        }

        auto nextWbuf = std::copy_n(reinterpret_cast<const uint8_t*>(&req),
                                    sizeof(req),
                                    wbuf.begin());

        if (sid) {
            // Add the optional stream-ID
            nextWbuf =
                    std::copy_n(reinterpret_cast<const uint8_t*>(&frameExtras),
                                sizeof(frameExtras),
                                nextWbuf);
        }

        nextWbuf = std::copy_n(reinterpret_cast<const uint8_t*>(&extras),
                               sizeof(extras),
                               nextWbuf);

        // Add the header (which includes extras, optional frame-extra and
        // optional nmeta)
        addIov(wbuf.data(), headerSize);

        // Add the key
        addIov(key.data(), key.size());

        // Add the value
        addIov(value.data(), value.size());

        return headerSize;
    });

    return ret;
}

ENGINE_ERROR_CODE Connection::deletionInner(const item_info& info,
                                            cb::const_byte_buffer packet,
                                            const DocKey& key) {
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    write->produce([this, &packet, &info, &ret, &key](
                           cb::byte_buffer buffer) -> size_t {
        if (buffer.size() <
            (packet.size() +
             cb::mcbp::unsigned_leb128<CollectionIDType>::getMaxSize())) {
            ret = ENGINE_E2BIG;
            return 0;
        }

        std::copy(packet.begin(), packet.end(), buffer.begin());

        // Add the header + collection-ID (stored in buffer)
        addIov(buffer.data(), packet.size());

        // Add the key
        addIov(key.data(), key.size());

        // Add the optional payload (xattr)
        if (info.nbytes > 0) {
            addIov(info.value[0].iov_base, info.nbytes);
        }

        return packet.size();
    });

    return ret;
}

ENGINE_ERROR_CODE Connection::deletion(uint32_t opaque,
                                       cb::unique_item_ptr it,
                                       Vbid vbucket,
                                       uint64_t by_seqno,
                                       uint64_t rev_seqno,
                                       cb::mcbp::DcpStreamId sid) {
    // Should be using the V2 callback
    if (isCollectionsSupported()) {
        LOG_WARNING("{}: Connection::deletion: called when collections-enabled",
                    getId());
        return ENGINE_FAILED;
    }

    item_info info;
    if (!bucket_get_item_info(*this, it.get(), &info)) {
        LOG_WARNING("{}: Connection::deletion: Failed to get item info",
                    getId());
        return ENGINE_FAILED;
    }

    auto key = info.key;
    if (!isCollectionsSupported()) {
        key = info.key.makeDocKeyWithoutCollectionID();
    }
    char* root = reinterpret_cast<char*>(info.value[0].iov_base);
    cb::char_buffer value{root, info.value[0].iov_len};

    cb::mcbp::DcpStreamIdFrameInfo frameInfo(sid);
    cb::mcbp::request::DcpDeletionV1Payload extdata(by_seqno, rev_seqno);

    const auto total = sizeof(extdata) + key.size() + value.size() +
                       (sid ? sizeof(cb::mcbp::DcpStreamIdFrameInfo) : 0) +
                       sizeof(cb::mcbp::Request);

    if (dcpUseWriteBuffer(total)) {
        cb::mcbp::RequestBuilder builder(write->wdata());

        builder.setMagic(sid ? cb::mcbp::Magic::AltClientRequest
                             : cb::mcbp::Magic::ClientRequest);
        builder.setOpcode(cb::mcbp::ClientOpcode::DcpDeletion);
        if (sid) {
            builder.setFramingExtras(frameInfo.getBuf());
        }
        builder.setExtras(extdata.getBuffer());
        builder.setKey({key.data(), key.size()});
        builder.setValue(value);
        builder.setOpaque(opaque);
        builder.setVBucket(vbucket);
        builder.setCas(info.cas);
        builder.setDatatype(cb::mcbp::Datatype(info.datatype));

        auto packet = builder.getFrame()->getFrame();
        addIov(packet.data(), packet.size());
        write->produced(packet.size());
        return ENGINE_SUCCESS;
    }

    if (!reserveItem(it.get())) {
        LOG_WARNING("{}: Connection::deletion: Failed to grow item array",
                    getId());
        return ENGINE_FAILED;
    }

    // we've reserved the item, and it'll be released when we're done sending
    // the item.
    it.release();

    using cb::mcbp::Request;
    using cb::mcbp::request::DcpDeletionV1Payload;
    uint8_t blob[sizeof(Request) + sizeof(DcpDeletionV1Payload) +
                 sizeof(cb::mcbp::DcpStreamIdFrameInfo)];
    auto& req = *reinterpret_cast<Request*>(blob);
    req.setMagic(sid ? cb::mcbp::Magic::AltClientRequest
                     : cb::mcbp::Magic::ClientRequest);
    req.setOpcode(cb::mcbp::ClientOpcode::DcpDeletion);
    req.setExtlen(gsl::narrow<uint8_t>(sizeof(DcpDeletionV1Payload)));
    req.setKeylen(gsl::narrow<uint16_t>(key.size()));
    req.setBodylen(gsl::narrow<uint32_t>(
            sizeof(DcpDeletionV1Payload) + key.size() + info.nbytes +
            (sid ? sizeof(cb::mcbp::DcpStreamIdFrameInfo) : 0)));
    req.setOpaque(opaque);
    req.setVBucket(vbucket);
    req.setCas(info.cas);
    req.setDatatype(cb::mcbp::Datatype(info.datatype));

    auto* ptr = blob + sizeof(Request);
    if (sid) {
        auto buf = frameInfo.getBuf();
        std::copy(buf.begin(), buf.end(), ptr);
        ptr += buf.size();
        req.setFramingExtraslen(buf.size());
    }

    std::copy(extdata.getBuffer().begin(), extdata.getBuffer().end(), ptr);
    cb::const_byte_buffer packetBuffer{
            blob,
            sizeof(Request) + sizeof(DcpDeletionV1Payload) +
                    (sid ? sizeof(cb::mcbp::DcpStreamIdFrameInfo) : 0)};

    return deletionInner(info, packetBuffer, key);
}

ENGINE_ERROR_CODE Connection::deletion_v2(uint32_t opaque,
                                          cb::unique_item_ptr it,
                                          Vbid vbucket,
                                          uint64_t by_seqno,
                                          uint64_t rev_seqno,
                                          uint32_t delete_time,
                                          cb::mcbp::DcpStreamId sid) {
    item_info info;
    if (!bucket_get_item_info(*this, it.get(), &info)) {
        LOG_WARNING("{}: Connection::deletion_v2: Failed to get item info",
                    getId());
        return ENGINE_FAILED;
    }

    auto key = info.key;
    if (!isCollectionsSupported()) {
        key = info.key.makeDocKeyWithoutCollectionID();
    }

    cb::mcbp::request::DcpDeletionV2Payload extras(
            by_seqno, rev_seqno, delete_time);
    cb::mcbp::DcpStreamIdFrameInfo frameInfo(sid);
    char* root = reinterpret_cast<char*>(info.value[0].iov_base);
    cb::char_buffer value{root, info.value[0].iov_len};

    const auto total = sizeof(extras) + key.size() + value.size() +
                       (sid ? sizeof(cb::mcbp::DcpStreamIdFrameInfo) : 0) +
                       sizeof(cb::mcbp::Request);

    if (dcpUseWriteBuffer(total)) {
        cb::mcbp::RequestBuilder builder(write->wdata());
        builder.setMagic(sid ? cb::mcbp::Magic::AltClientRequest
                             : cb::mcbp::Magic::ClientRequest);
        builder.setOpcode(cb::mcbp::ClientOpcode::DcpDeletion);
        if (sid) {
            builder.setFramingExtras(frameInfo.getBuf());
        }
        builder.setExtras(extras.getBuffer());
        builder.setKey({key.data(), key.size()});
        builder.setValue(value);
        builder.setOpaque(opaque);
        builder.setVBucket(vbucket);
        builder.setCas(info.cas);
        builder.setDatatype(cb::mcbp::Datatype(info.datatype));

        auto packet = builder.getFrame()->getFrame();
        addIov(packet.data(), packet.size());
        write->produced(packet.size());
        return ENGINE_SUCCESS;
    }

    if (!reserveItem(it.get())) {
        LOG_WARNING("{}: Connection::deletion_v2: Failed to grow item array",
                    getId());
        return ENGINE_FAILED;
    }

    // we've reserved the item, and it'll be released when we're done sending
    // the item.
    it.release();

    // Make blob big enough for either delete or expiry
    uint8_t blob[sizeof(cb::mcbp::Request) + sizeof(extras) +
                 sizeof(frameInfo)] = {};
    const size_t payloadLen = sizeof(extras);
    const size_t frameInfoLen = sid ? sizeof(frameInfo) : 0;

    auto& req = *reinterpret_cast<cb::mcbp::Request*>(blob);
    req.setMagic(sid ? cb::mcbp::Magic::AltClientRequest
                     : cb::mcbp::Magic::ClientRequest);

    req.setOpcode(cb::mcbp::ClientOpcode::DcpDeletion);
    req.setExtlen(gsl::narrow<uint8_t>(payloadLen));
    req.setKeylen(gsl::narrow<uint16_t>(key.size()));
    req.setBodylen(gsl::narrow<uint32_t>(payloadLen +
                                         gsl::narrow<uint16_t>(key.size()) +
                                         info.nbytes + frameInfoLen));
    req.setOpaque(opaque);
    req.setVBucket(vbucket);
    req.setCas(info.cas);
    req.setDatatype(cb::mcbp::Datatype(info.datatype));
    auto size = sizeof(cb::mcbp::Request);
    auto* ptr = blob + size;
    if (sid) {
        auto buf = frameInfo.getBuf();
        std::copy(buf.begin(), buf.end(), ptr);
        ptr += buf.size();
        size += buf.size();
    }

    auto buffer = extras.getBuffer();
    std::copy(buffer.begin(), buffer.end(), ptr);
    size += buffer.size();

    return deletionInner(info, {blob, size}, key);
}

ENGINE_ERROR_CODE Connection::expiration(uint32_t opaque,
                                         cb::unique_item_ptr it,
                                         Vbid vbucket,
                                         uint64_t by_seqno,
                                         uint64_t rev_seqno,
                                         uint32_t delete_time,
                                         cb::mcbp::DcpStreamId sid) {
    item_info info;
    if (!bucket_get_item_info(*this, it.get(), &info)) {
        LOG_WARNING("{}: Connection::expiration: Failed to get item info",
                    getId());
        return ENGINE_FAILED;
    }

    auto key = info.key;
    if (!isCollectionsSupported()) {
        key = info.key.makeDocKeyWithoutCollectionID();
    }

    cb::mcbp::request::DcpExpirationPayload extras(
            by_seqno, rev_seqno, delete_time);
    cb::mcbp::DcpStreamIdFrameInfo frameInfo(sid);
    char* root = reinterpret_cast<char*>(info.value[0].iov_base);
    cb::char_buffer value{root, info.value[0].iov_len};

    const auto total = sizeof(extras) + key.size() + value.size() +
                       (sid ? sizeof(cb::mcbp::DcpStreamIdFrameInfo) : 0) +
                       sizeof(cb::mcbp::Request);

    if (dcpUseWriteBuffer(total)) {
        cb::mcbp::RequestBuilder builder(write->wdata());
        builder.setMagic(sid ? cb::mcbp::Magic::AltClientRequest
                             : cb::mcbp::Magic::ClientRequest);
        builder.setOpcode(cb::mcbp::ClientOpcode::DcpExpiration);
        if (sid) {
            builder.setFramingExtras(frameInfo.getBuf());
        }
        builder.setExtras(extras.getBuffer());
        builder.setKey({key.data(), key.size()});
        builder.setValue(value);
        builder.setOpaque(opaque);
        builder.setVBucket(vbucket);
        builder.setCas(info.cas);
        builder.setDatatype(cb::mcbp::Datatype(info.datatype));

        auto packet = builder.getFrame()->getFrame();
        addIov(packet.data(), packet.size());
        write->produced(packet.size());
        return ENGINE_SUCCESS;
    }

    if (!reserveItem(it.get())) {
        LOG_WARNING("{}: Connection::expiration: Failed to grow item array",
                    getId());
        return ENGINE_FAILED;
    }

    // we've reserved the item, and it'll be released when we're done sending
    // the item.
    it.release();

    // Make blob big enough for either delete or expiry
    uint8_t blob[sizeof(cb::mcbp::Request) + sizeof(extras) +
                 sizeof(frameInfo)] = {};
    const size_t payloadLen = sizeof(extras);
    const size_t frameInfoLen = sid ? sizeof(frameInfo) : 0;

    auto& req = *reinterpret_cast<cb::mcbp::Request*>(blob);
    req.setMagic(sid ? cb::mcbp::Magic::AltClientRequest
                     : cb::mcbp::Magic::ClientRequest);

    req.setOpcode(cb::mcbp::ClientOpcode::DcpExpiration);
    req.setExtlen(gsl::narrow<uint8_t>(payloadLen));
    req.setKeylen(gsl::narrow<uint16_t>(key.size()));
    req.setBodylen(gsl::narrow<uint32_t>(payloadLen +
                                         gsl::narrow<uint16_t>(key.size()) +
                                         info.nbytes + frameInfoLen));
    req.setOpaque(opaque);
    req.setVBucket(vbucket);
    req.setCas(info.cas);
    req.setDatatype(cb::mcbp::Datatype(info.datatype));
    auto size = sizeof(cb::mcbp::Request);
    auto* ptr = blob + size;
    if (sid) {
        auto buf = frameInfo.getBuf();
        std::copy(buf.begin(), buf.end(), ptr);
        ptr += buf.size();
        size += buf.size();
    }

    auto buffer = extras.getBuffer();
    std::copy(buffer.begin(), buffer.end(), ptr);
    size += buffer.size();

    return deletionInner(info, {blob, size}, key);
}

ENGINE_ERROR_CODE Connection::set_vbucket_state(uint32_t opaque,
                                                Vbid vbucket,
                                                vbucket_state_t state) {
    if (!is_valid_vbucket_state_t(state)) {
        return ENGINE_EINVAL;
    }

    cb::mcbp::request::DcpSetVBucketState extras;
    extras.setState(static_cast<uint8_t>(state));
    uint8_t buffer[sizeof(cb::mcbp::Request) + sizeof(extras)];
    cb::mcbp::RequestBuilder builder({buffer, sizeof(buffer)});
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::DcpSetVbucketState);
    builder.setOpaque(opaque);
    builder.setVBucket(vbucket);
    builder.setExtras(extras.getBuffer());

    return add_packet_to_send_pipe(builder.getFrame()->getFrame());
}

ENGINE_ERROR_CODE Connection::noop(uint32_t opaque) {
    uint8_t buffer[sizeof(cb::mcbp::Request)];
    cb::mcbp::RequestBuilder builder({buffer, sizeof(buffer)});
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::DcpNoop);
    builder.setOpaque(opaque);

    return add_packet_to_send_pipe(builder.getFrame()->getFrame());
}

ENGINE_ERROR_CODE Connection::buffer_acknowledgement(uint32_t opaque,
                                                     Vbid vbucket,
                                                     uint32_t buffer_bytes) {
    cb::mcbp::request::DcpBufferAckPayload extras;
    extras.setBufferBytes(buffer_bytes);
    uint8_t buffer[sizeof(cb::mcbp::Request) + sizeof(extras)];
    cb::mcbp::RequestBuilder builder({buffer, sizeof(buffer)});
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::DcpBufferAcknowledgement);
    builder.setOpaque(opaque);
    builder.setVBucket(vbucket);
    builder.setExtras(extras.getBuffer());

    return add_packet_to_send_pipe(builder.getFrame()->getFrame());
}

ENGINE_ERROR_CODE Connection::control(uint32_t opaque,
                                      cb::const_char_buffer key,
                                      cb::const_char_buffer value) {
    std::vector<uint8_t> buffer;
    buffer.resize(sizeof(cb::mcbp::Request) + key.size() + value.size());
    cb::mcbp::RequestBuilder builder({buffer.data(), buffer.size()});

    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::DcpControl);
    builder.setOpaque(opaque);
    builder.setKey({reinterpret_cast<const uint8_t*>(key.data()), key.size()});
    builder.setValue(
            {reinterpret_cast<const uint8_t*>(value.data()), value.size()});
    return add_packet_to_send_pipe(builder.getFrame()->getFrame());
}

ENGINE_ERROR_CODE Connection::system_event(uint32_t opaque,
                                           Vbid vbucket,
                                           mcbp::systemevent::id event,
                                           uint64_t bySeqno,
                                           mcbp::systemevent::version version,
                                           cb::const_byte_buffer key,
                                           cb::const_byte_buffer eventData,
                                           cb::mcbp::DcpStreamId sid) {
    cb::mcbp::request::DcpSystemEventPayload extras(bySeqno, event, version);
    std::vector<uint8_t> buffer;
    buffer.resize(sizeof(cb::mcbp::Request) + sizeof(extras) + key.size() +
                  eventData.size() + sizeof(cb::mcbp::DcpStreamIdFrameInfo));
    cb::mcbp::RequestBuilder builder({buffer.data(), buffer.size()});

    builder.setMagic(sid ? cb::mcbp::Magic::AltClientRequest
                         : cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::DcpSystemEvent);
    builder.setOpaque(opaque);
    builder.setVBucket(vbucket);
    builder.setDatatype(cb::mcbp::Datatype::Raw);
    builder.setExtras(extras.getBuffer());
    if (sid) {
        cb::mcbp::DcpStreamIdFrameInfo framedSid(sid);
        builder.setFramingExtras(framedSid.getBuf());
    }
    builder.setKey(key);
    builder.setValue(eventData);

    return add_packet_to_send_pipe(builder.getFrame()->getFrame());
}

ENGINE_ERROR_CODE Connection::get_error_map(uint32_t opaque, uint16_t version) {
    cb::mcbp::request::GetErrmapPayload body;
    body.setVersion(version);
    uint8_t buffer[sizeof(cb::mcbp::Request) + sizeof(body)];
    cb::mcbp::RequestBuilder builder({buffer, sizeof(buffer)});
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::GetErrorMap);
    builder.setOpaque(opaque);
    builder.setValue(body.getBuffer());

    return add_packet_to_send_pipe(builder.getFrame()->getFrame());
}

ENGINE_ERROR_CODE Connection::prepare(uint32_t opaque,
                                      cb::unique_item_ptr it,
                                      Vbid vbucket,
                                      uint64_t by_seqno,
                                      uint64_t rev_seqno,
                                      uint32_t lock_time,
                                      uint8_t nru,
                                      DocumentState document_state,
                                      cb::durability::Level level) {
    item_info info;
    if (!bucket_get_item_info(*this, it.get(), &info)) {
        LOG_WARNING("{}: Connection::prepare: Failed to get item info",
                    getId());
        return ENGINE_FAILED;
    }

    char* root = reinterpret_cast<char*>(info.value[0].iov_base);
    cb::char_buffer buffer{root, info.value[0].iov_len};

    auto key = info.key;

    // The client doesn't support collections, so must not send an encoded key
    if (!isCollectionsSupported()) {
        key = key.makeDocKeyWithoutCollectionID();
    }

    cb::mcbp::request::DcpPreparePayload extras(
            by_seqno,
            rev_seqno,
            info.flags,
            gsl::narrow<uint32_t>(info.exptime),
            lock_time,
            nru);
    if (document_state == DocumentState::Deleted) {
        extras.setDeleted(uint8_t(1));
    }
    extras.setDurabilityLevel(level);

    size_t total = sizeof(extras) + key.size() + buffer.size() +
                   sizeof(cb::mcbp::Request);
    if (dcpUseWriteBuffer(total)) {
        // Format a local copy and send
        cb::mcbp::RequestBuilder builder(write->wdata());
        builder.setMagic(cb::mcbp::Magic::ClientRequest);
        builder.setOpcode(cb::mcbp::ClientOpcode::DcpPrepare);
        builder.setExtras(extras.getBuffer());
        builder.setKey({key.data(), key.size()});
        builder.setOpaque(opaque);
        builder.setVBucket(vbucket);
        builder.setCas(info.cas);
        builder.setDatatype(cb::mcbp::Datatype(info.datatype));
        builder.setValue(buffer);

        auto packet = builder.getFrame()->getFrame();
        addIov(packet.data(), packet.size());
        write->produced(packet.size());
        return ENGINE_SUCCESS;
    }

    // Use an IO vector instead
    if (!reserveItem(it.get())) {
        LOG_WARNING("{}: Connection::prepare: Failed to grow item array",
                    getId());
        return ENGINE_FAILED;
    }

    // we've reserved the item, and it'll be released when we're done sending
    // the item.
    it.release();

    cb::mcbp::Request req = {};
    req.setMagic(cb::mcbp::Magic::ClientRequest);
    req.setOpcode(cb::mcbp::ClientOpcode::DcpPrepare);
    req.setExtlen(gsl::narrow<uint8_t>(sizeof(extras)));
    req.setKeylen(gsl::narrow<uint16_t>(key.size()));
    req.setBodylen(
            gsl::narrow<uint32_t>(sizeof(extras) + key.size() + buffer.size()));
    req.setOpaque(opaque);
    req.setVBucket(vbucket);
    req.setCas(info.cas);
    req.setDatatype(cb::mcbp::Datatype(info.datatype));

    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    write->produce([this, &req, &extras, &buffer, &ret, &key](
                           cb::byte_buffer wbuf) -> size_t {
        const size_t total = sizeof(extras) + sizeof(req);
        if (wbuf.size() < total) {
            ret = ENGINE_E2BIG;
            return 0;
        }

        std::copy_n(reinterpret_cast<const uint8_t*>(&req),
                    sizeof(req),
                    wbuf.begin());
        std::copy_n(reinterpret_cast<const uint8_t*>(&extras),
                    sizeof(extras),
                    wbuf.begin() + sizeof(req));

        // Add the header
        addIov(wbuf.data(), sizeof(req) + sizeof(extras));

        // Add the key
        addIov(key.data(), key.size());

        // Add the value
        addIov(buffer.data(), buffer.size());
        return total;
    });

    return ret;
}

ENGINE_ERROR_CODE Connection::seqno_acknowledged(uint32_t opaque,
                                                 Vbid vbucket,
                                                 uint64_t prepared_seqno) {
    cb::mcbp::request::DcpSeqnoAcknowledgedPayload extras(prepared_seqno);
    uint8_t buffer[sizeof(cb::mcbp::Request) + sizeof(extras)];
    cb::mcbp::RequestBuilder builder({buffer, sizeof(buffer)});
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::DcpSeqnoAcknowledged);
    builder.setOpaque(opaque);
    builder.setVBucket(vbucket);
    builder.setExtras(extras.getBuffer());
    return add_packet_to_send_pipe(builder.getFrame()->getFrame());
}

ENGINE_ERROR_CODE Connection::commit(uint32_t opaque,
                                     Vbid vbucket,
                                     const DocKey& key_,
                                     uint64_t prepare_seqno,
                                     uint64_t commit_seqno) {
    cb::mcbp::request::DcpCommitPayload extras(prepare_seqno, commit_seqno);
    auto key = key_;
    if (!isCollectionsSupported()) {
        // The client doesn't support collections, don't send an encoded key
        key = key.makeDocKeyWithoutCollectionID();
    }
    const size_t totalBytes =
            sizeof(cb::mcbp::Request) + sizeof(extras) + key.size();
    std::vector<uint8_t> buffer(totalBytes);
    cb::mcbp::RequestBuilder builder({buffer.data(), buffer.size()});
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::DcpCommit);
    builder.setOpaque(opaque);
    builder.setVBucket(vbucket);
    builder.setExtras(extras.getBuffer());
    builder.setKey(cb::const_char_buffer(key));
    return add_packet_to_send_pipe(builder.getFrame()->getFrame());
}

ENGINE_ERROR_CODE Connection::abort(uint32_t opaque,
                                    Vbid vbucket,
                                    const DocKey& key_,
                                    uint64_t prepared_seqno,
                                    uint64_t abort_seqno) {
    cb::mcbp::request::DcpAbortPayload extras(prepared_seqno, abort_seqno);
    auto key = key_;
    if (!isCollectionsSupported()) {
        // The client doesn't support collections, don't send an encoded key
        key = key.makeDocKeyWithoutCollectionID();
    }
    const size_t totalBytes =
            sizeof(cb::mcbp::Request) + sizeof(extras) + key.size();
    std::vector<uint8_t> buffer(totalBytes);
    cb::mcbp::RequestBuilder builder({buffer.data(), buffer.size()});
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::DcpAbort);
    builder.setOpaque(opaque);
    builder.setVBucket(vbucket);
    builder.setExtras(extras.getBuffer());
    builder.setKey(cb::const_char_buffer(key));
    return add_packet_to_send_pipe(builder.getFrame()->getFrame());
}

////////////////////////////////////////////////////////////////////////////
//                                                                        //
//               End DCP Message producer interface                       //
//                                                                        //
////////////////////////////////////////////////////////////////////////////
