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
#include "sendbuffer.h"
#include "server_event.h"
#include "settings.h"
#include "ssl_utils.h"
#include "tracing.h"

#include <event2/bufferevent.h>
#include <event2/bufferevent_ssl.h>
#include <logger/logger.h>
#include <mcbp/mcbp.h>
#include <mcbp/protocol/framebuilder.h>
#include <mcbp/protocol/header.h>
#include <memcached/durability_spec.h>
#include <nlohmann/json.hpp>
#include <phosphor/phosphor.h>
#include <platform/checked_snprintf.h>
#include <platform/socket.h>
#include <platform/strerror.h>
#include <platform/string_hex.h>
#include <platform/timeutils.h>
#include <utilities/logtags.h>
#include <gsl/gsl>

#include <exception>
#ifndef WIN32
#include <netinet/tcp.h> // For TCP_NODELAY etc
#endif

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

void Connection::shutdown() {
    state = State::closing;
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
            ret["user"]["name"] = user.name;
        } else {
            ret["user"]["name"] = cb::tagUserData(user.name);
        }
        ret["user"]["domain"] = to_string(user.domain);
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
    ret["dcp_no_value"] = isDcpNoValue();
    ret["max_reqs_per_event"] = max_reqs_per_event;
    ret["nevents"] = numEvents;

    switch (state) {
    case State::running:
        ret["state"] = "running";
    case State::closing:
        ret["state"] = "closing";
    case State::pending_close:
        ret["state"] = "pending close";
    case State::immediate_close:
        ret["state"] = "immediate close";
    }

    ret["ssl"] = ssl;
    ret["total_recv"] = totalRecv;
    ret["total_send"] = totalSend;

    ret["datatype"] = mcbp::datatype::to_string(datatypeFilter.getRaw());

    ret["sendqueue"]["size"] = sendQueueInfo.size;
    ret["sendqueue"]["last"] = sendQueueInfo.last.time_since_epoch().count();
    ret["sendqueue"]["term"] = sendQueueInfo.term;

    return ret;
}

void Connection::setDCP(bool enable) {
    dcp = enable;
}

void Connection::restartAuthentication() {
    if (authenticated && user.domain == cb::sasl::Domain::External) {
        externalAuthManager->logoff(user.name);
    }
    sasl_conn.reset();
    setInternal(false);
    authenticated = false;
    user = cb::rbac::UserIdent{"", cb::rbac::Domain::Local};
}

cb::engine_errc Connection::dropPrivilege(cb::rbac::Privilege privilege) {
    privilegeContext.dropPrivilege(privilege);
    return cb::engine_errc::success;
}

cb::rbac::PrivilegeContext Connection::getPrivilegeContext() {
    if (privilegeContext.isStale()) {
        try {
            privilegeContext = cb::rbac::createContext(user, getBucket().name);
        } catch (const cb::rbac::NoSuchBucketException&) {
            // Remove all access to the bucket
            privilegeContext = cb::rbac::createContext(user, "");
            LOG_INFO(
                    "{}: RBAC: Connection::refreshPrivilegeContext() {} No "
                    "access "
                    "to bucket [{}]. new privilege set: {}",
                    getId(),
                    getDescription(),
                    getBucket().name,
                    privilegeContext.to_string());

        } catch (const cb::rbac::Exception& error) {
            privilegeContext = cb::rbac::PrivilegeContext{user.domain};
            LOG_WARNING(
                    "{}: RBAC: Connection::refreshPrivilegeContext() {}: An "
                    "exception occurred. bucket:[{}] message: {}",
                    getId(),
                    getDescription(),
                    getBucket().name,
                    error.what());
        }
    }

    return privilegeContext;
}

Bucket& Connection::getBucket() const {
    return all_buckets[getBucketIndex()];
}

EngineIface* Connection::getBucketEngine() const {
    return getBucket().getEngine();
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

    return ENGINE_DISCONNECT;
}

void Connection::resetUsernameCache() {
    if (sasl_conn.isInitialized()) {
        user = {sasl_conn.getUsername(), sasl_conn.getDomain()};
    } else {
        user = {"unknown", cb::sasl::Domain::Local};
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
        description += cb::tagUserData(user.name);

        if (user.domain == cb::sasl::Domain::External) {
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
                    user, all_buckets[bucketIndex].name);
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
                    {"default", user.domain}, all_buckets[bucketIndex].name);
        } else {
            // The user has not authenticated, and this isn't for the
            // "default bucket". Assign an empty profile which won't give
            // you any privileges.
            privilegeContext = cb::rbac::PrivilegeContext{user.domain};
        }
    } catch (const cb::rbac::Exception&) {
        privilegeContext = cb::rbac::PrivilegeContext{user.domain};
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

void Connection::shutdownIfSendQueueStuck(
        std::chrono::steady_clock::time_point now) {
    auto currentSendBufferSize = getSendQueueSize();
    if (currentSendBufferSize == 0) {
        // The current buffer is empty!
        sendQueueInfo.size = currentSendBufferSize;
        return;
    }

    if (sendQueueInfo.size != currentSendBufferSize) {
        // The current buffer have a different size than the last
        // time we checked.. record the new size
        sendQueueInfo.size = currentSendBufferSize;
        sendQueueInfo.last = now;
        return;
    }

    // We've seen that some clients isn't draining their socket fast
    // enough causing data to back up in the send pipe. We don't want
    // to disconnect those clients too fast as that may generate more
    // work on the server if they just reconnect and start a large
    // task which consume a lot of resources on the server to fill the
    // pipe again. During bucket deletion we want to disconnect the
    // clients relatively fast.
    const auto limit = (getBucket().state == Bucket::State::Ready)
                               ? std::chrono::seconds(29)
                               : std::chrono::seconds(1);
    if ((now - sendQueueInfo.last) > limit) {
        LOG_WARNING(
                "{}: send buffer stuck at {} for ~{} seconds. Shutting "
                "down connection {}",
                getId(),
                sendQueueInfo.size,
                std::chrono::duration_cast<std::chrono::seconds>(
                        now - sendQueueInfo.last)
                        .count(),
                getDescription());

        // We've not had any progress on the socket for "n" secs
        // Forcibly shut down the connection!
        sendQueueInfo.term = true;
        state = State::closing;
    }
}

bool Connection::processAllReadyCookies() {
    // Look at the existing commands and check possibly execute them
    bool active = false;
    auto iter = cookies.begin();
    // Iterate over all of the cookies and try to execute them
    // (and nuke the entries as they complete so that we may start
    // new ones)
    while (iter != cookies.end()) {
        auto& cookie = *iter;

        if (cookie->empty()) {
            ++iter;
            continue;
        }

        if (cookie->isEwouldblock()) {
            // This cookie is waiting for an engine notification.
            // Look at the next one
            ++iter;
            active = true;
            continue;
        }

        if (active && !cookie->mayReorder()) {
            // we've got active commands, and this command can't be
            // reordered... stop executing!
            break;
        }

        if (cookie->execute()) {
            // The command executed successfully
            if (iter == cookies.begin()) {
                cookie->reset();
                ++iter;
            } else {
                iter = cookies.erase(iter);
            }
        } else {
            ++iter;
            active = true;
        }

        if (--numEvents == 0) {
            // We've used out time slice
            break;
        }
    }

    return active;
}

void Connection::executeCommandPipeline() {
    numEvents = max_reqs_per_event;
    const auto maxActiveCommands =
            Settings::instance().getMaxConcurrentCommandsPerConnection();

    bool active = processAllReadyCookies();

    // We might add more commands to the queue
    if (is_bucket_dying(*this)) {
        // we need to shut down the bucket
        return;
    }

    if (!active || cookies.back()->mayReorder()) {
        // Only look at new commands if we don't have any active commands
        // or the active command allows for reordering.
        auto input = bufferevent_get_input(bev.get());
        bool stop = false;
        while (!stop && cookies.size() < maxActiveCommands &&
               isPacketAvailable() && numEvents > 0) {
            std::unique_ptr<Cookie> cookie;
            if (cookies.back()->empty()) {
                // we want to reuse the cookie
                cookie = std::move(cookies.back());
                cookies.pop_back();
            } else {
                cookie = std::make_unique<Cookie>(*this);
            }

            cookie->initialize(getPacket(), isTracingEnabled());
            auto drainSize = cookie->getPacket().size();

            const auto status = cookie->validate();
            if (status != cb::mcbp::Status::Success) {
                cookie->sendResponse(status);
                if (status != cb::mcbp::Status::UnknownCommand) {
                    state = State::closing;
                    return;
                }
            } else {
                // We may only start execute the packet if:
                //  * We don't have any ongoing commands
                //  * We have an ongoing command and this command allows
                //    for reorder
                if ((!active || cookie->mayReorder()) && cookie->execute()) {
                    if (cookies.empty()) {
                        // Add back the empty slot
                        cookie->reset();
                        cookies.push_back(std::move(cookie));
                    }
                } else {
                    active = true;
                    // We need to block so we need to preserve the request
                    // as we'll drain the data from the buffer)
                    cookie->preserveRequest();
                    cookies.push_back(std::move(cookie));
                    if (!cookies.back()->mayReorder()) {
                        // Don't add commands as we need the last one to
                        // complete
                        stop = true;
                    }
                }
                --numEvents;
            }

            if (evbuffer_drain(input, drainSize) == -1) {
                throw std::runtime_error(
                        "Connection::executeCommandsCallback(): Failed to "
                        "drain buffer");
            }
        }
    }

    // Do we have any server-side messages to send?
    processServerEvents();

    if (numEvents == 0) {
        yields++;
        // Update the aggregated stat
        get_thread_stats(this)->conn_yields++;
    }

    // We have to make sure that we drain the send queue back to a "normal"
    // size if it grows too big. At the same time we don't want to signal
    // the thread to be run again if we've got a pending notification for
    // the thread (an active command running which is waiting for the engine)
    // If the last command in the pipeline may be reordered we can add more
    if ((getSendQueueSize() < Settings::instance().getMaxPacketSize()) &&
        (!active || (cookies.back()->mayReorder() &&
                     cookies.size() < maxActiveCommands))) {
        enableReadEvent();
        if ((!active || numEvents == 0) && isPacketAvailable()) {
            triggerCallback();
        }
    } else {
        disableReadEvent();
    }
}

bool Connection::executeCommandsCallback() {
    using std::chrono::duration_cast;
    using std::chrono::microseconds;
    using std::chrono::nanoseconds;

    const auto start = std::chrono::steady_clock::now();

    shutdownIfSendQueueStuck(start);
    if (state == State::running) {
        try {
            // continue to run the state machine
            executeCommandPipeline();
        } catch (const std::exception& e) {
            state = State::closing;

            try {
                auto array = nlohmann::json::array();
                for (const auto& c : cookies) {
                    if (c && !c->empty()) {
                        array.push_back(c->toJSON());
                    }
                }
                LOG_WARNING(
                        "{}: exception occurred in runloop during packet "
                        "execution. Closing connection: {}. Cookies: {}",
                        getId(),
                        e.what(),
                        array.dump());
            } catch (const std::bad_alloc&) {
                LOG_WARNING(
                        "{}: exception occurred in runloop during packet "
                        "execution. Closing connection: {}",
                        getId(),
                        e.what());
            }
        }
    }

    if (isDCP() && state == State::running) {
        if (cookies.empty()) {
            throw std::runtime_error(
                    "Connection::executeCommandsCallback(): no cookies "
                    "available!");
        }
        if (cookies.front()->empty()) {
            // make sure we reset the privilege context
            cookies.front()->reset();
            bool more = true;
            do {
                const auto ret = getBucket().getDcpIface()->step(
                        static_cast<const void*>(cookies.front().get()), this);
                switch (remapErrorCode(ret)) {
                case ENGINE_SUCCESS:
                    more = (getSendQueueSize() <
                            Settings::instance().getMaxPacketSize());
                    break;
                case ENGINE_EWOULDBLOCK:
                    more = false;
                    break;
                default:
                    LOG_WARNING(
                            R"({}: step returned {} - closing connection {})",
                            getId(),
                            std::to_string(ret),
                            getDescription());

                    state = State::closing;
                    more = false;
                }
            } while (more);
        }
    }

    const auto stop = std::chrono::steady_clock::now();
    const auto ns = duration_cast<nanoseconds>(stop - start);
    scheduler_info[getThread().index].add(duration_cast<microseconds>(ns));
    addCpuTime(ns);

    if (state != State::running) {
        if (state == State::closing) {
            externalAuthManager->remove(*this);
            close();
        }

        if (state == State::pending_close) {
            close();
        }

        if (state == State::immediate_close) {
            disassociate_bucket(*this);

            // Do the final cleanup of the connection:
            thread.notification.remove(this);
            // remove from pending-io list
            {
                std::lock_guard<std::mutex> lock(thread.pending_io.mutex);
                thread.pending_io.map.erase(this);
            }

            // delete the object
            return false;
        }
    }
    return true;
}

void Connection::rw_callback(bufferevent*, void* ctx) {
    auto& instance = *reinterpret_cast<Connection*>(ctx);
    auto& thread = instance.getThread();

    TRACE_LOCKGUARD_TIMED(thread.mutex,
                          "mutex",
                          "Connection::rw_callback::threadLock",
                          SlowMutexThreshold);
    // Remove the connection from the list of pending io's (in case the
    // object was scheduled to run in the dispatcher before the
    // callback for the worker thread is executed.
    //
    {
        std::lock_guard<std::mutex> lock(thread.pending_io.mutex);
        auto iter = thread.pending_io.map.find(&instance);
        if (iter != thread.pending_io.map.end()) {
            for (const auto& pair : iter->second) {
                if (pair.first) {
                    pair.first->setAiostat(pair.second);
                    pair.first->setEwouldblock(false);
                }
            }
            thread.pending_io.map.erase(iter);
        }
    }

    // Remove the connection from the notification list if it's there
    thread.notification.remove(&instance);
    if (!instance.executeCommandsCallback()) {
        conn_destroy(&instance);
    }
}

void Connection::event_callback(bufferevent*, short event, void* ctx) {
    auto& instance = *reinterpret_cast<Connection*>(ctx);
    bool term = false;

    if ((event & BEV_EVENT_EOF) == BEV_EVENT_EOF) {
        LOG_DEBUG("{}: Connection::on_event: Socket EOF", instance.getId());
        term = true;
    }

    if ((event & BEV_EVENT_ERROR) == BEV_EVENT_ERROR) {
        LOG_INFO("{}: Connection::on_event: Socket error: {}",
                 instance.getId(),
                 evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR()));
        term = true;
    }

    if (term) {
        auto& thread = instance.getThread();
        TRACE_LOCKGUARD_TIMED(thread.mutex,
                              "mutex",
                              "Connection::event_callback::threadLock",
                              SlowMutexThreshold);

        // Remove the connection from the list of pending io's (in case the
        // object was scheduled to run in the dispatcher before the
        // callback for the worker thread is executed.
        //
        {
            std::lock_guard<std::mutex> lock(thread.pending_io.mutex);
            auto iter = thread.pending_io.map.find(&instance);
            if (iter != thread.pending_io.map.end()) {
                for (const auto& pair : iter->second) {
                    if (pair.first) {
                        pair.first->setAiostat(pair.second);
                        pair.first->setEwouldblock(false);
                    }
                }
                thread.pending_io.map.erase(iter);
            }
        }

        // Remove the connection from the notification list if it's there
        thread.notification.remove(&instance);

        if (instance.state == State::running) {
            instance.state = State::closing;
        }

        if (!instance.executeCommandsCallback()) {
            conn_destroy(&instance);
        }
    }
}

void Connection::ssl_read_callback(bufferevent* bev, void* ctx) {
    auto& instance = *reinterpret_cast<Connection*>(ctx);
    // Lets inspect the certificate before we'll do anything further
    auto* ssl_st = bufferevent_openssl_get_ssl(bev);
    cb::openssl::unique_x509_ptr cert(SSL_get_peer_certificate(ssl_st));
    auto certResult = Settings::instance().lookupUser(cert.get());
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
            associate_bucket(instance, "default");
        }
        break;
    case cb::x509::Status::Success:
        if (!instance.tryAuthFromSslCert(certResult.second)) {
            disconnect = true;
            // Don't print an error message... already logged
            certResult.second.clear();
        }
    }

    if (disconnect) {
        if (certResult.first == cb::x509::Status::NotPresent) {
            audit_auth_failure(instance,
                               "Client did not provide an X.509 certificate");
        } else {
            audit_auth_failure(
                    instance,
                    "Failed to use client provided X.509 certificate");
        }
        instance.shutdown();
        if (!certResult.second.empty()) {
            LOG_WARNING(
                    "{}: conn_ssl_init: disconnection client due to error [{}]",
                    instance.getId(),
                    certResult.second);
        }
    } else {
        LOG_INFO("{}: Using SSL cipher:{}",
                 instance.getId(),
                 SSL_get_cipher_name(ssl_st));
    }

    // update the callback to call the normal read callback
    bufferevent_setcb(bev,
                      Connection::rw_callback,
                      Connection::rw_callback,
                      Connection::event_callback,
                      ctx);

    // and let's call it to make sure we step through the state machinery
    Connection::rw_callback(bev, ctx);
}

void Connection::setAuthenticated(bool authenticated) {
    Connection::authenticated = authenticated;
    if (authenticated) {
        updateDescription();
        privilegeContext = cb::rbac::createContext(user, "");
    } else {
        resetUsernameCache();
        privilegeContext = cb::rbac::PrivilegeContext{user.domain};
    }
}

bool Connection::tryAuthFromSslCert(const std::string& userName) {
    user.name.assign(userName);
    user.domain = cb::sasl::Domain::Local;

    try {
        auto context = cb::rbac::createInitialContext(user);
        setAuthenticated(true);
        setInternal(context.second);
        audit_auth_success(*this);
        LOG_INFO(
                "{}: Client {} authenticated as '{}' via X509 "
                "certificate",
                getId(),
                getPeername(),
                cb::UserDataView(user.name));
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

void Connection::triggerCallback() {
    const auto opt = BEV_TRIG_IGNORE_WATERMARKS | BEV_TRIG_DEFER_CALLBACKS;
    bufferevent_trigger(bev.get(), EV_READ, opt);
}

bool Connection::dcpUseWriteBuffer(size_t size) const {
    return isSslEnabled() && size < thread.scratch_buffer.size();
}

void Connection::copyToOutputStream(cb::const_char_buffer data) {
    if (data.empty()) {
        return;
    }

    if (bufferevent_write(bev.get(), data.data(), data.size()) == -1) {
        throw std::bad_alloc();
    }

    totalSend += data.size();
}

static void sendbuffer_cleanup_cb(const void*, size_t, void* extra) {
    delete reinterpret_cast<SendBuffer*>(extra);
}

void Connection::chainDataToOutputStream(std::unique_ptr<SendBuffer> buffer) {
    if (!buffer || buffer->getPayload().empty()) {
        throw std::logic_error(
                "McbpConnection::chainDataToOutputStream: buffer must be set");
    }

    auto data = buffer->getPayload();
    if (evbuffer_add_reference(bufferevent_get_output(bev.get()),
                               data.data(),
                               data.size(),
                               sendbuffer_cleanup_cb,
                               buffer.get()) == -1) {
        throw std::bad_alloc();
    }

    // Buffer successfully added to libevent and the callback
    // (sendbuffer_cleanup_cb) will free the memory.
    // Move the ownership of the buffer!
    (void)buffer.release();
    totalSend += data.size();
}

Connection::Connection(FrontEndThread& thr)
    : socketDescriptor(INVALID_SOCKET),
      connectedToSystemPort(false),
      base(nullptr),
      thread(thr),
      peername("unknown"),
      sockname("unknown"),
      max_reqs_per_event(Settings::instance().getRequestsPerEventNotification(
              EventPriority::Default)),
      ssl(false) {
    updateDescription();
    cookies.emplace_back(std::unique_ptr<Cookie>{new Cookie(*this)});
    setConnectionId(peername.c_str());
    stats.conn_structs++;
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
      peername(cb::net::getpeername(socketDescriptor)),
      sockname(cb::net::getsockname(socketDescriptor)),
      max_reqs_per_event(Settings::instance().getRequestsPerEventNotification(
              EventPriority::Default)),
      ssl(ifc.isSslPort()) {
    setTcpNoDelay(true);
    updateDescription();
    cookies.emplace_back(std::unique_ptr<Cookie>{new Cookie(*this)});
    setConnectionId(peername.c_str());

    const auto options = BEV_OPT_THREADSAFE | BEV_OPT_UNLOCK_CALLBACKS |
                         BEV_OPT_CLOSE_ON_FREE | BEV_OPT_DEFER_CALLBACKS;
    if (ssl) {
        bev.reset(bufferevent_openssl_socket_new(
                base,
                sfd,
                createSslStructure(ifc).release(),
                BUFFEREVENT_SSL_ACCEPTING,
                options));
        bufferevent_setcb(bev.get(),
                          Connection::ssl_read_callback,
                          Connection::rw_callback,
                          Connection::event_callback,
                          static_cast<void*>(this));
    } else {
        bev.reset(bufferevent_socket_new(base, sfd, options));
        bufferevent_setcb(bev.get(),
                          Connection::rw_callback,
                          Connection::rw_callback,
                          Connection::event_callback,
                          static_cast<void*>(this));
    }

    bufferevent_enable(bev.get(), EV_READ);
    stats.conn_structs++;
}

Connection::~Connection() {
    if (connectedToSystemPort) {
        --stats.system_conns;
    }
    if (authenticated && user.domain == cb::sasl::Domain::External) {
        externalAuthManager->logoff(user.name);
    }

    if (bev) {
        bev.reset();
        stats.curr_conns.fetch_sub(1, std::memory_order_relaxed);
    }

    --stats.conn_structs;
}

void Connection::EventDeleter::operator()(bufferevent* ev) {
    bufferevent_free(ev);
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
    auto* event = bev.get();
    auto* input = bufferevent_get_input(event);
    auto size = evbuffer_get_length(input);
    if (size < sizeof(cb::mcbp::Header)) {
        return false;
    }

    const auto* header = reinterpret_cast<const cb::mcbp::Header*>(
            evbuffer_pullup(input, sizeof(cb::mcbp::Header)));
    if (header == nullptr) {
        throw std::runtime_error(
                "Connection::isPacketAvailable(): Failed to reallocate event "
                "input buffer: " +
                std::to_string(sizeof(cb::mcbp::Header)));
    }

    if (!header->isValid()) {
        audit_invalid_packet(*this, getAvailableBytes());
        throw std::runtime_error(
                "Connection::isPacketAvailable(): Invalid packet header "
                "detected");
    }

    const auto framesize = sizeof(*header) + header->getBodylen();
    if (size >= framesize) {
        // We've got the entire buffer available.. make sure it is continuous
        if (evbuffer_pullup(input, framesize) == nullptr) {
            throw std::runtime_error(
                    "Connection::isPacketAvailable(): Failed to reallocate "
                    "event input buffer: " +
                    std::to_string(framesize));
        }
        return true;
    }

    // We don't have the entire frame available.. Are we receiving an
    // incredible big packet so that we want to disconnect the client?
    if (framesize > Settings::instance().getMaxPacketSize()) {
        throw std::runtime_error(
                "Connection::isPacketAvailable(): The packet size " +
                std::to_string(framesize) +
                " exceeds the max allowed packet size " +
                std::to_string(Settings::instance().getMaxPacketSize()));
    }

    return false;
}

const cb::mcbp::Header& Connection::getPacket() const {
    // Drain all of the data available in bufferevent into the
    // socket read buffer
    auto* event = bev.get();
    auto* input = bufferevent_get_input(event);
    auto nb = evbuffer_get_length(input);
    if (nb < sizeof(cb::mcbp::Header)) {
        throw std::runtime_error(
                "Connection::getPacket(): packet not available");
    }

    return *reinterpret_cast<const cb::mcbp::Header*>(
            evbuffer_pullup(input, sizeof(cb::mcbp::Header)));
}

cb::const_byte_buffer Connection::getAvailableBytes(size_t max) const {
    auto* input = bufferevent_get_input(bev.get());
    auto nb = std::min(evbuffer_get_length(input), max);
    return {evbuffer_pullup(input, nb), nb};
}

bool Connection::processServerEvents() {
    if (server_events.empty()) {
        return false;
    }

    const auto before = state;

    // We're waiting for the next command to arrive from the client
    // and we've got a server event to process. Let's start
    // processing the server events (which might toggle our state)
    if (server_events.front()->execute(*this)) {
        server_events.pop();
    }

    return state != before;
}

void Connection::close() {
    bool ewb = false;
    uint32_t rc = refcount;

    for (auto& cookie : cookies) {
        if (cookie) {
            rc += cookie->getRefcount();
            if (cookie->isEwouldblock()) {
                ewb = true;
                break;
            } else {
                cookie->reset();
            }
        }
    }

    if (state == State::closing) {
        // We don't want any network notifications anymore. Start by disabling
        // all read notifications (We may have data in the write buffers we
        // want to send. It seems like we don't immediately send the data over
        // the socket when writing to a bufferevent. it is scheduled to be sent
        // once we return from the dispatch function for the read event. If
        // we nuke the connection now, the error message we tried to send back
        // to the client won't be sent).
        disableReadEvent();
        cb::net::shutdown(socketDescriptor, SHUT_RD);
    }

    // Notify interested parties that the connection is currently being
    // disconnected
    propagateDisconnect();

    if (isDCP()) {
        // DCP channels work a bit different.. they use the refcount
        // to track if it has a reference in the engine
        ewb = false;
    }

    if (rc > 1 || ewb || havePendingData()) {
        state = State::pending_close;
    } else {
        state = State::immediate_close;
    }
}

void Connection::propagateDisconnect() const {
    for (auto& cookie : cookies) {
        if (cookie) {
            getBucket().getEngine()->disconnect(cookie.get());
        }
    }
}

bool Connection::signalIfIdle() {
    for (const auto& c : cookies) {
        if (c && !c->empty() && c->isEwouldblock()) {
            return false;
        }
    }

    if (state != State::immediate_close) {
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
    throw std::invalid_argument("Unknown priority: " +
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

void Connection::disableReadEvent() {
    if ((bufferevent_get_enabled(bev.get()) & EV_READ) == EV_READ) {
        if (bufferevent_disable(bev.get(), EV_READ) == -1) {
            throw std::runtime_error(
                    "McbpConnection::disableReadEvent: Failed to disable read "
                    "events");
        }
    }
}

void Connection::enableReadEvent() {
    if ((bufferevent_get_enabled(bev.get()) & EV_READ) == 0) {
        if (bufferevent_enable(bev.get(), EV_READ) == -1) {
            throw std::runtime_error(
                    "McbpConnection::enableReadEvent: Failed to enable read "
                    "events");
        }
    }
}

bool Connection::havePendingData() const {
    if (sendQueueInfo.term) {
        return false;
    }

    return getSendQueueSize() != 0;
}

size_t Connection::getSendQueueSize() const {
    return evbuffer_get_length(bufferevent_get_output(bev.get()));
}

void Connection::sendResponseHeaders(Cookie& cookie,
                                     cb::mcbp::Status status,
                                     cb::const_char_buffer extras,
                                     cb::const_char_buffer key,
                                     std::size_t value_len,
                                     uint8_t datatype) {
    static_assert(sizeof(FrontEndThread::scratch_buffer) >
                          (sizeof(cb::mcbp::Response) + 3),
                  "scratch buffer too small");
    const auto& request = cookie.getRequest();
    auto wbuf = cb::char_buffer{thread.scratch_buffer.data(),
                                thread.scratch_buffer.size()};
    auto& response = *reinterpret_cast<cb::mcbp::Response*>(wbuf.data());

    response.setOpcode(request.getClientOpcode());
    response.setExtlen(gsl::narrow_cast<uint8_t>(extras.size()));
    response.setDatatype(cb::mcbp::Datatype(datatype));
    response.setStatus(status);
    response.setOpaque(request.getOpaque());
    response.setCas(cookie.getCas());

    if (cookie.isTracingEnabled()) {
        // When tracing is enabled we'll be using the alternative
        // response header where we inject the framing header.
        // For now we'll just hard-code the adding of the bytes
        // for the tracing info.
        //
        // Moving forward we should get a builder for encoding the
        // framing header (but do that the next time we need to add
        // something so that we have a better understanding on how
        // we need to do that (it could be that we need to modify
        // an already existing section etc).
        response.setMagic(cb::mcbp::Magic::AltClientResponse);
        // The framing extras when we just include the tracing information
        // is 3 bytes. 1 byte with id and length, then the 2 bytes
        // containing the actual data.
        const uint8_t framing_extras_size = MCBP_TRACING_RESPONSE_SIZE;
        const uint8_t tracing_framing_id = 0x02;

        wbuf.data()[2] = framing_extras_size; // framing header extras 3 bytes
        wbuf.data()[3] = gsl::narrow_cast<uint8_t>(key.size());
        response.setBodylen(value_len + extras.size() + key.size() +
                            framing_extras_size);

        auto& tracer = cookie.getTracer();
        const auto val = htons(tracer.getEncodedMicros());
        auto* ptr = wbuf.data() + sizeof(cb::mcbp::Response);
        *ptr = tracing_framing_id;
        ptr++;
        memcpy(ptr, &val, sizeof(val));
        wbuf = {wbuf.data(), sizeof(cb::mcbp::Response) + framing_extras_size};
    } else {
        response.setMagic(cb::mcbp::Magic::ClientResponse);
        response.setKeylen(gsl::narrow_cast<uint16_t>(key.size()));
        response.setFramingExtraslen(0);
        response.setBodylen(value_len + extras.size() + key.size());
        wbuf = {wbuf.data(), sizeof(cb::mcbp::Response)};
    }

    if (Settings::instance().getVerbose() > 1) {
        auto* header = reinterpret_cast<const cb::mcbp::Header*>(wbuf.data());
        try {
            LOG_TRACE("<{} Sending: {}", getId(), header->toJSON(true).dump());
        } catch (const std::exception&) {
            // Failed.. do a raw dump instead
            LOG_TRACE("<{} Sending: {}",
                      getId(),
                      cb::to_hex({reinterpret_cast<const uint8_t*>(wbuf.data()),
                                  sizeof(cb::mcbp::Header)}));
        }
    }

    // if we can fit the key and extras in the scratch buffer lets copy them
    // in to avoid the extra mutex lock
    if ((wbuf.size() + extras.size() + key.size()) <
        thread.scratch_buffer.size()) {
        std::copy(extras.begin(), extras.end(), wbuf.end());
        wbuf = {wbuf.data(), wbuf.size() + extras.size()};
        std::copy(key.begin(), key.end(), wbuf.end());
        wbuf = {wbuf.data(), wbuf.size() + key.size()};
        copyToOutputStream(wbuf);
    } else {
        // Copy the data to the output stream
        copyToOutputStream(wbuf);
        copyToOutputStream(extras);
        copyToOutputStream(key);
    }
    ++getBucket().responseCounters[uint16_t(status)];
}

void Connection::sendResponse(Cookie& cookie,
                              cb::mcbp::Status status,
                              cb::const_char_buffer extras,
                              cb::const_char_buffer key,
                              cb::const_char_buffer value,
                              uint8_t datatype,
                              std::unique_ptr<SendBuffer> sendbuffer) {
    sendResponseHeaders(cookie, status, extras, key, value.size(), datatype);
    if (sendbuffer) {
        if (sendbuffer->getPayload().size() != value.size()) {
            throw std::runtime_error(
                    "Connection::sendResponse: The sendbuffers payload must "
                    "match the value encoded in the response");
        }
        chainDataToOutputStream(std::move(sendbuffer));
    } else {
        cookie.getConnection().copyToOutputStream(value);
    }
}

ENGINE_ERROR_CODE Connection::add_packet_to_send_pipe(
        cb::const_byte_buffer packet) {
    try {
        copyToOutputStream(packet);
    } catch (const std::bad_alloc&) {
        return ENGINE_E2BIG;
    }

    return ENGINE_SUCCESS;
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
        cb::mcbp::RequestBuilder builder(thread.getScratchBuffer());
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

        copyToOutputStream(builder.getFrame()->getFrame());
        return ENGINE_SUCCESS;
    }

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

    try {
        // Add the header
        copyToOutputStream(
                {reinterpret_cast<const uint8_t*>(&req), sizeof(req)});
        if (sid) {
            copyToOutputStream(frameExtras.getBuf());
        }
        copyToOutputStream(extras.getBuffer());

        // Add the key
        copyToOutputStream({key.data(), key.size()});

        // Add the value
        if (!value.empty()) {
            if (value.size() > SendBuffer::MinimumDataSize) {
                auto sendbuffer = std::make_unique<ItemSendBuffer>(
                        std::move(it), value, getBucket());
                chainDataToOutputStream(std::move(sendbuffer));
            } else {
                copyToOutputStream(value);
            }
        }
    } catch (const std::bad_alloc&) {
        /// We might have written a partial message into the buffer so
        /// we need to disconnect the client
        return ENGINE_DISCONNECT;
    }

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE Connection::deletionInner(const item_info& info,
                                            cb::const_byte_buffer packet,
                                            const DocKey& key) {
    try {
        copyToOutputStream(packet);
        copyToOutputStream({key.data(), key.size()});
        copyToOutputStream(
                {reinterpret_cast<const char*>(info.value[0].iov_base),
                 info.nbytes});
    } catch (const std::bad_alloc&) {
        // We might have written a partial message into the buffer so
        // we need to disconnect the client
        return ENGINE_DISCONNECT;
    }

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE Connection::deletion(uint32_t opaque,
                                       cb::unique_item_ptr it,
                                       Vbid vbucket,
                                       uint64_t by_seqno,
                                       uint64_t rev_seqno,
                                       cb::mcbp::DcpStreamId sid) {
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
        cb::mcbp::RequestBuilder builder(thread.getScratchBuffer());

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

        copyToOutputStream(builder.getFrame()->getFrame());
        return ENGINE_SUCCESS;
    }

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
        cb::mcbp::RequestBuilder builder(thread.getScratchBuffer());
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
        copyToOutputStream(builder.getFrame()->getFrame());
        return ENGINE_SUCCESS;
    }

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
        cb::mcbp::RequestBuilder builder(thread.getScratchBuffer());
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
        copyToOutputStream(builder.getFrame()->getFrame());
        return ENGINE_SUCCESS;
    }

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
        cb::mcbp::RequestBuilder builder(thread.getScratchBuffer());
        builder.setMagic(cb::mcbp::Magic::ClientRequest);
        builder.setOpcode(cb::mcbp::ClientOpcode::DcpPrepare);
        builder.setExtras(extras.getBuffer());
        builder.setKey({key.data(), key.size()});
        builder.setOpaque(opaque);
        builder.setVBucket(vbucket);
        builder.setCas(info.cas);
        builder.setDatatype(cb::mcbp::Datatype(info.datatype));
        builder.setValue(buffer);
        copyToOutputStream(builder.getFrame()->getFrame());
        return ENGINE_SUCCESS;
    }

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

    try {
        // Add the header
        copyToOutputStream(
                {reinterpret_cast<const uint8_t*>(&req), sizeof(req)});
        copyToOutputStream(
                {reinterpret_cast<const uint8_t*>(&extras), sizeof(extras)});

        // Add the key
        copyToOutputStream({key.data(), key.size()});

        // Add the value
        if (!buffer.empty()) {
            if (buffer.size() > SendBuffer::MinimumDataSize) {
                auto sendbuffer = std::make_unique<ItemSendBuffer>(
                        std::move(it), buffer, getBucket());
                chainDataToOutputStream(std::move(sendbuffer));
            } else {
                copyToOutputStream(buffer);
            }
        }
    } catch (const std::bad_alloc&) {
        /// We might have written a partial message into the buffer so
        /// we need to disconnect the client
        return ENGINE_DISCONNECT;
    }

    return ENGINE_SUCCESS;
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
