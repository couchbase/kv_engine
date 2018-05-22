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
#include "connection.h"

#include "buckets.h"
#include "connections.h"
#include "mc_time.h"
#include "mcaudit.h"
#include "memcached.h"
#include "runtime.h"
#include "server_event.h"
#include "statemachine_mcbp.h"
#include "trace.h"

#include <mcbp/mcbp.h>
#include <mcbp/protocol/header.h>
#include <phosphor/phosphor.h>
#include <platform/cb_malloc.h>
#include <platform/checked_snprintf.h>
#include <platform/socket.h>
#include <platform/strerror.h>
#include <platform/timeutils.h>
#include <utilities/logtags.h>
#include <utilities/protocol2text.h>
#include <cctype>
#include <exception>
#include <gsl/gsl>

const char* to_string(const Connection::Priority& priority) {
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

void Connection::resolveConnectionName() {
    if (socketDescriptor == INVALID_SOCKET) {
        // Our unit tests run without a socket connected, and we don't
        // want them to flood the console with error messages
        peername = "[invalid]";
        sockname = "[invalid]";
        return;
    }

    try {
        peername = cb::net::getpeername(socketDescriptor);
        sockname = cb::net::getsockname(socketDescriptor);
        updateDescription();
    } catch (const std::bad_alloc& e) {
        LOG_WARNING(
                "Connection::resolveConnectionName: failed to allocate "
                "memory: "
                "{}",
                e.what());
    }
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
 * @return the json representation. Caller is responsible for calling
 *         cJSON_Delete()
 */
static cJSON* event_mask_to_json(const short mask) {
    cJSON* ret = cJSON_CreateObject();
    cJSON* array = cJSON_CreateArray();

    cJSON_AddUintPtrToObject(ret, "raw", mask);
    if (mask & EV_READ) {
        cJSON_AddItemToArray(array, cJSON_CreateString("read"));
    }
    if (mask & EV_WRITE) {
        cJSON_AddItemToArray(array, cJSON_CreateString("write"));
    }
    if (mask & EV_PERSIST) {
        cJSON_AddItemToArray(array, cJSON_CreateString("persist"));
    }
    if (mask & EV_TIMEOUT) {
        cJSON_AddItemToArray(array, cJSON_CreateString("timeout"));
    }

    cJSON_AddItemToObject(ret, "decoded", array);
    return ret;
}

unique_cJSON_ptr Connection::toJSON() const {
    unique_cJSON_ptr ret(cJSON_CreateObject());
    cJSON* obj = ret.get();
    cJSON_AddUintPtrToObject(obj, "connection", (uintptr_t)this);
    if (socketDescriptor == INVALID_SOCKET) {
        cJSON_AddStringToObject(obj, "socket", "disconnected");
        return ret;
    }

    cJSON_AddNumberToObject(obj, "socket", socketDescriptor);
    cJSON_AddStringToObject(obj, "protocol", "memcached");
    cJSON_AddStringToObject(obj, "peername", getPeername().c_str());
    cJSON_AddStringToObject(obj, "sockname", getSockname().c_str());
    cJSON_AddNumberToObject(obj, "parent_port", parent_port);
    cJSON_AddNumberToObject(obj, "bucket_index", getBucketIndex());
    cJSON_AddBoolToObject(obj, "internal", isInternal());
    if (authenticated) {
        cJSON_AddStringToObject(obj, "username", username.c_str());
        }
        cJSON_AddBoolToObject(obj, "nodelay", nodelay);
        cJSON_AddNumberToObject(obj, "refcount", refcount);

        cJSON* features = cJSON_CreateObject();
        cJSON_AddBoolToObject(features, "mutation_extras",
                                isSupportsMutationExtras());
        cJSON_AddBoolToObject(features, "xerror", isXerrorSupport());

        cJSON_AddItemToObject(obj, "features", features);

        cJSON_AddUintPtrToObject(obj, "engine_storage",
                                   (uintptr_t)engine_storage);
        cJSON_AddUintPtrToObject(obj, "thread", (uintptr_t)thread.load(
            std::memory_order::memory_order_relaxed));
        cJSON_AddStringToObject(obj, "priority", to_string(priority));

        if (clustermap_revno == -2) {
            cJSON_AddStringToObject(obj, "clustermap_revno", "unknown");
        } else {
            cJSON_AddNumberToObject(obj, "clustermap_revno", clustermap_revno);
        }

        cJSON_AddStringToObject(obj,
                                "total_cpu_time",
                                std::to_string(total_cpu_time.count()).c_str());
        cJSON_AddStringToObject(obj,
                                "min_sched_time",
                                std::to_string(min_sched_time.count()).c_str());
        cJSON_AddStringToObject(obj,
                                "max_sched_time",
                                std::to_string(max_sched_time.count()).c_str());

        unique_cJSON_ptr arr(cJSON_CreateArray());
        for (const auto& c : cookies) {
            cJSON_AddItemToArray(arr.get(), c->toJSON().release());
        }
        cJSON_AddItemToObject(obj, "cookies", arr.release());

        if (agentName.front() != '\0') {
            cJSON_AddStringToObject(obj, "agent_name", agentName.data());
        }
        if (connectionId.front() != '\0') {
            cJSON_AddStringToObject(obj, "connection_id", connectionId.data());
        }

        cJSON_AddBoolToObject(obj, "tracing", tracingEnabled);
        cJSON_AddBoolToObject(obj, "sasl_enabled", saslAuthEnabled);
        cJSON_AddBoolToObject(obj, "dcp", isDCP());
        cJSON_AddBoolToObject(obj, "dcp_xattr_aware", isDcpXattrAware());
        cJSON_AddBoolToObject(obj, "dcp_no_value", isDcpNoValue());
        cJSON_AddNumberToObject(obj, "max_reqs_per_event", max_reqs_per_event);
        cJSON_AddNumberToObject(obj, "nevents", numEvents);
        cJSON_AddStringToObject(obj, "state", getStateName());

        {
            cJSON* o = cJSON_CreateObject();
            cJSON_AddBoolToObject(o, "registered", isRegisteredInLibevent());
            cJSON_AddItemToObject(o, "ev_flags", event_mask_to_json(ev_flags));
            cJSON_AddItemToObject(o, "which", event_mask_to_json(currentEvent));
            cJSON_AddItemToObject(obj, "libevent", o);
        }

        if (read) {
            cJSON_AddItemToObject(obj, "read", read->to_json().release());
        }

        if (write) {
            cJSON_AddItemToObject(obj, "write", write->to_json().release());
        }

        cJSON_AddStringToObject(
                obj, "write_and_go", stateMachine.getStateName(write_and_go));

        {
            cJSON* iovobj = cJSON_CreateObject();
            cJSON_AddNumberToObject(iovobj, "size", iov.size());
            cJSON_AddNumberToObject(iovobj, "used", iovused);
            cJSON_AddItemToObject(obj, "iov", iovobj);
        }

        {
            cJSON* msg = cJSON_CreateObject();
            cJSON_AddNumberToObject(msg, "used", msglist.size());
            cJSON_AddNumberToObject(msg, "curr", msgcurr);
            cJSON_AddNumberToObject(msg, "bytes", msgbytes);
            cJSON_AddItemToObject(obj, "msglist", msg);
        }
        {
            cJSON* ilist = cJSON_CreateObject();
            cJSON_AddNumberToObject(ilist, "size", reservedItems.size());
            cJSON_AddItemToObject(obj, "itemlist", ilist);
        }
        {
            cJSON* talloc = cJSON_CreateObject();
            cJSON_AddNumberToObject(talloc, "size", temp_alloc.size());
            cJSON_AddItemToObject(obj, "temp_alloc_list", talloc);
        }

        /* @todo we should decode the binary header */
        cJSON_AddNumberToObject(obj, "aiostat", aiostat);
        cJSON_AddBoolToObject(obj, "ewouldblock", ewouldblock);
        cJSON_AddItemToObject(obj, "ssl", ssl.toJSON());
        cJSON_AddNumberToObject(obj, "total_recv", totalRecv);
        cJSON_AddNumberToObject(obj, "total_send", totalSend);
        cJSON_AddStringToObject(
                obj,
                "datatype",
                mcbp::datatype::to_string(datatype.getRaw()).c_str());
        return ret;
}

void Connection::restartAuthentication() {
    sasl_conn.reset();
    internal = false;
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
        const auto opcode = cookie.getHeader().getOpcode();
        const std::string command(memcached_opcode_2_text(opcode));

        // The privilege context we had could have been a dummy entry
        // (created when the client connected, and used until the
        // connection authenticates). Let's try to automatically update it,
        // but let the client deal with whatever happens after
        // a single update.
        try {
            privilegeContext = cb::rbac::createContext(getUsername(),
                                                       all_buckets[bucketIndex].name);
        } catch (const cb::rbac::NoSuchBucketException&) {
            // Remove all access to the bucket
            privilegeContext = cb::rbac::createContext(getUsername(), "");
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
        const auto opcode = cookie.getHeader().getOpcode();
        const std::string command(memcached_opcode_2_text(opcode));
        const std::string privilege_string = cb::rbac::to_string(privilege);
        const std::string context = privilegeContext.to_string();

        if (settings.isPrivilegeDebug()) {
            audit_privilege_debug(this,
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

ENGINE_ERROR_CODE Connection::remapErrorCode(ENGINE_ERROR_CODE code) const {
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
    case ENGINE_WANT_MORE: // FALLTHROUGH
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
        return isCollectionsSupported() ? code : ENGINE_EINVAL;

    case ENGINE_EACCESS:break;
    case ENGINE_NO_BUCKET:break;
    case ENGINE_AUTH_STALE:break;
    }

    // Seems like the rest of the components in our system isn't
    // prepared to receive access denied or authentincation stale.
    // For now we should just disconnect them
    auto errc = cb::make_error_condition(cb::engine_errc(code));
    LOG_INFO(
            "{} - Client {} not aware of extended error code ({}). "
            "Disconnecting",
            getId(),
            getDescription().c_str(),
            errc.message().c_str());

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
        description += cb::logtags::tagUserData(getUsername());

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

    if (bucketIndex < 0) {
        // The connection objects which listens to the ports to accept
        // use a bucketIndex of -1. Those connection objects should
        // don't need an entry
        return;
    }

    // Update the privilege context. If a problem occurs within the RBAC
    // module we'll assign an empty privilege context to the connection.
    try {
        if (authenticated) {
            // The user have logged in, so we should create a context
            // representing the users context in the desired bucket.
            privilegeContext = cb::rbac::createContext(username,
                                                       all_buckets[bucketIndex].name);
        } else if (is_default_bucket_enabled() &&
                   strcmp("default", all_buckets[bucketIndex].name) == 0) {
            // We've just connected to the _default_ bucket, _AND_ the client
            // is unknown.
            // Personally I think the "default bucket" concept is a really
            // really bad idea, but we need to be backwards compatible for
            // a while... lets look up a profile named "default" and
            // assign that. It should only contain access to the default
            // bucket.
            privilegeContext = cb::rbac::createContext("default",
                                                       all_buckets[bucketIndex].name);
        } else {
            // The user has not authenticated, and this isn't for the
            // "default bucket". Assign an empty profile which won't give
            // you any privileges.
            privilegeContext = cb::rbac::PrivilegeContext{};
        }
    } catch (const cb::rbac::Exception&) {
        privilegeContext = cb::rbac::PrivilegeContext{};
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

bool Connection::reapplyEventmask() {
    return updateEvent(ev_flags);
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

bool Connection::tryAuthFromSslCert(const std::string& userName) {
    username.assign(userName);
    domain = cb::sasl::Domain::Local;

    try {
        auto context =
                cb::rbac::createInitialContext(getUsername(), getDomain());
        setAuthenticated(true);
        setInternal(context.second);
        LOG_INFO(
                "{}: Client {} authenticated as '{}' via X509 "
                "certificate",
                getId(),
                getPeername(),
                cb::logtags::tagUserData(getUsername()));
        // Connections authenticated by using X.509 certificates should not
        // be able to use SASL to change it's identity.
        saslAuthEnabled = false;
    } catch (const cb::rbac::NoSuchUserException& e) {
        setAuthenticated(false);
        LOG_WARNING("{}: User [{}] is not defined as a user in Couchbase",
                    getId(),
                    cb::logtags::tagUserData(e.what()));
        return false;
    }
    return true;
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
            if (settings.getClientCertMode() == cb::x509::Mode::Mandatory) {
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
    } else {
        if (ssl.getError(r) == SSL_ERROR_WANT_READ) {
            ssl.drainBioSendPipe(socketDescriptor);
            cb::net::set_ewouldblock();
            return -1;
        } else {
            try {
                std::string errmsg("SSL_accept() returned " +
                                   std::to_string(r) + " with error " +
                                   std::to_string(ssl.getError(r)));

                std::vector<char> ssl_err(1024);
                ERR_error_string_n(
                        ERR_get_error(), ssl_err.data(), ssl_err.size());

                LOG_WARNING("{}: {}: {}", getId(), errmsg, ssl_err.data());
            } catch (const std::bad_alloc&) {
                // unable to print error message; continue.
            }

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
            } else {
                return res > 0 ? res : -1;
            }
        }

        /* @todo figure out how to drain the rest of the data if we
         * failed to send all of it...
         */
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
                setState(McbpStateMachine::State::closing);
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
                            setState(McbpStateMachine::State::closing);
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
                setState(McbpStateMachine::State::closing);
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

        setState(McbpStateMachine::State::closing);
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
    LOG_INFO(
            "{} Closing connection {} due to read "
            "error: {}",
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
            int error = ssl.getError(n);

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
                /*
                 * @todo I don't know how to gracefully recover from this
                 * let's just shut down the connection
                 */
                LOG_WARNING("{}: ERROR: SSL_read returned -1 with error {}",
                            getId(),
                            error);
                cb::net::set_econnreset();
                return -1;
            }
        }
    }

    return ret;
}

int Connection::sslWrite(const char* src, size_t nbytes) {
    int ret = 0;

    int chunksize = settings.getBioDrainBufferSize();

    while (ret < int(nbytes)) {
        int n;
        int chunk;

        ssl.drainBioSendPipe(socketDescriptor);
        if (ssl.hasError()) {
            cb::net::set_econnreset();
            return -1;
        }

        chunk = (int)(nbytes - ret);
        if (chunk > chunksize) {
            chunk = chunksize;
        }

        n = ssl.write(src + ret, chunk);
        if (n > 0) {
            ret += n;
        } else {
            if (ret > 0) {
                /* We've sent some data.. let the caller have them */
                return ret;
            }

            if (n < 0) {
                int error = ssl.getError(n);
                switch (error) {
                case SSL_ERROR_WANT_WRITE:
                    cb::net::set_ewouldblock();
                    return -1;

                default:
                    /*
                     * @todo I don't know how to gracefully recover from this
                     * let's just shut down the connection
                     */
                    LOG_WARNING(
                            "{}: ERROR: SSL_write returned -1 with error {}",
                            getId(),
                            error);
                    cb::net::set_econnreset();
                    return -1;
                }
            }
        }
    }

    return ret;
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

    m->msg_iov[m->msg_iovlen].iov_base = (void*)buf;
    m->msg_iov[m->msg_iovlen].iov_len = len;

    msgbytes += len;
    ++iovused;
    STATS_MAX(this, iovused_high_watermark, gsl::narrow<int>(getIovUsed()));
    m->msg_iovlen++;
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

Connection::Connection()
    : socketDescriptor(INVALID_SOCKET),
      base(nullptr),
      stateMachine(*this) {
    MEMCACHED_CONN_CREATE(this);
    updateDescription();
    cookies.emplace_back(std::unique_ptr<Cookie>{new Cookie(*this)});
    setConnectionId(peername.c_str());
}

Connection::Connection(SOCKET sfd, event_base* b, const ListeningPort& ifc)
    : socketDescriptor(sfd),
      base(b),
      parent_port(ifc.port),
      stateMachine(*this) {
    MEMCACHED_CONN_CREATE(this);
    resolveConnectionName();
    setTcpNoDelay(ifc.tcp_nodelay);
    updateDescription();
    cookies.emplace_back(std::unique_ptr<Cookie>{new Cookie(*this)});
    msglist.reserve(MSG_LIST_INITIAL);
    iov.resize(IOV_LIST_INITIAL);

    if (ifc.ssl.enabled) {
        if (!enableSSL(ifc.ssl.cert, ifc.ssl.key)) {
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
    MEMCACHED_CONN_DESTROY(this);
    releaseReservedItems();
    for (auto* ptr : temp_alloc) {
        cb_free(ptr);
    }
    if (socketDescriptor != INVALID_SOCKET) {
        LOG_DEBUG("{} - Closing socket descriptor", getId());
        safe_close(socketDescriptor);
    }
}

void Connection::setState(McbpStateMachine::State next_state) {
    stateMachine.setCurrentState(next_state);
}

void Connection::runStateMachinery() {
    if (settings.getVerbose() > 1) {
        do {
            LOG_INFO("{} - Running task: {}",
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
    return getState() == McbpStateMachine::State ::destroyed;
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
        bool logged = false;
        if (getState() == McbpStateMachine::State::execute) {
            try {
                // Converting the cookie to json -> string could probably
                // cause too much memory allcation. We don't want that to
                // cause us to crash..
                LOG_WARNING(
                        "{}: exception occurred in runloop during packet "
                        "execution. Cookie info: {} - closing connection: {}",
                        getId(),
                        to_string(getCookieObject().toJSON(), false),
                        e.what());
                logged = true;
            } catch (const std::bad_alloc&) {
                // none
            }
        }

        if (!logged) {
            try {
                LOG_WARNING(
                        "{}: exception occurred in runloop (state: \"{}\") - "
                        "closing connection: {}",
                        getId(),
                        getStateName(),
                        e.what());
            } catch (const std::bad_alloc&) {
                // Ditch logging.. just shut down the connection
            }
        }

        setState(McbpStateMachine::State::closing);
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
                LOG_WARNING(
                        "{}: exception occurred in runloop whilst"
                        " attempting to close connection: {}",
                        getId(),
                        e.what());
            } catch (const std::bad_alloc&) {
                // Drop logging
            }
        }
    }

    conn_return_buffers(this);
}

void Connection::initiateShutdown() {
    setState(McbpStateMachine::State::closing);
}

void Connection::close() {
    bool ewb = false;
    for (auto& cookie : cookies) {
        if (cookie) {
            if (cookie->isEwouldblock()) {
                ewb = true;
            }
            cookie->reset();
        }
    }

    // We don't want any network notifications anymore..
    unregisterEvent();
    safe_close(socketDescriptor);
    socketDescriptor = INVALID_SOCKET;

    // Release all reserved items!
    releaseReservedItems();

    if (refcount > 1 || ewb) {
        setState(McbpStateMachine::State::pending_close);
    } else {
        setState(McbpStateMachine::State::immediate_close);
    }
}

void Connection::propagateDisconnect() const {
    for (auto& cookie : cookies) {
        if (cookie) {
            perform_callbacks(ON_DISCONNECT, nullptr, cookie.get());
        }
    }
}

void Connection::signalIfIdle(bool logbusy, size_t workerthread) {
    if (!isEwouldblock() && stateMachine.isIdleState()) {
        // Raise a 'fake' write event to ensure the connection has an
        // event delivered (for example if its sendQ is full).
        if (!registered_in_libevent) {
            ev_flags = EV_READ | EV_WRITE | EV_PERSIST;
            if (!registerEvent()) {
                LOG_WARNING(
                        "{}: Connection::signalIfIdle: Unable to "
                        "registerEvent.  Setting state to conn_closing",
                        getId());
                setState(McbpStateMachine::State::closing);
            }
        } else if (!updateEvent(EV_READ | EV_WRITE | EV_PERSIST)) {
            LOG_WARNING(
                    "{}: Connection::signalIfIdle: Unable to "
                    "updateEvent.  Setting state to conn_closing",
                    getId());
            setState(McbpStateMachine::State::closing);
        }
        event_active(event.get(), EV_WRITE, 0);
    } else if (logbusy) {
        unique_cJSON_ptr json(toJSON());
        auto details = to_string(json, false);
        LOG_INFO("Worker thread {}: {}", workerthread, details);
    }
}

void Connection::setPriority(const Connection::Priority& priority) {
    Connection::priority = priority;
    switch (priority) {
    case Priority::High:
        max_reqs_per_event =
                settings.getRequestsPerEventNotification(EventPriority::High);
        return;
    case Priority::Medium:
        max_reqs_per_event =
                settings.getRequestsPerEventNotification(EventPriority::Medium);
        return;
    case Priority::Low:
        max_reqs_per_event =
                settings.getRequestsPerEventNotification(EventPriority::Low);
        return;
    }
    throw std::invalid_argument("Unkown priority: " +
                                std::to_string(int(priority)));
}

bool Connection::selectedBucketIsXattrEnabled() const {
    if (bucketEngine) {
        return settings.isXattrEnabled() &&
               bucketEngine->isXattrEnabled(getBucketEngineAsV0());
    }
    return settings.isXattrEnabled();
}
