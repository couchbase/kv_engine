/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "connection.h"

#include "buckets.h"
#include "client_cert_config.h"
#include "connections.h"
#include "cookie.h"
#include "external_auth_manager_thread.h"
#include "front_end_thread.h"
#include "listening_port.h"
#include "mc_time.h"
#include "mcaudit.h"
#include "memcached.h"
#include "protocol/mcbp/engine_wrapper.h"
#include "sendbuffer.h"
#include "settings.h"
#include "ssl_utils.h"
#include "tracing.h"

#include <event2/bufferevent.h>
#include <event2/bufferevent_ssl.h>
#include <gsl/gsl-lite.hpp>
#include <logger/logger.h>
#include <mcbp/codec/dcp_snapshot_marker.h>
#include <mcbp/mcbp.h>
#include <mcbp/protocol/framebuilder.h>
#include <mcbp/protocol/header.h>
#include <memcached/durability_spec.h>
#include <nlohmann/json.hpp>
#include <phosphor/phosphor.h>
#include <platform/backtrace.h>
#include <platform/checked_snprintf.h>
#include <platform/exceptions.h>
#include <platform/scope_timer.h>
#include <platform/socket.h>
#include <platform/strerror.h>
#include <platform/string_hex.h>
#include <platform/timeutils.h>
#include <serverless/config.h>
#include <utilities/logtags.h>

#include <exception>

#ifdef __linux__
#include <linux/sockios.h>
#include <netinet/in.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#endif

#ifndef WIN32
#include <netinet/tcp.h> // For TCP_NODELAY etc
#endif

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

    if (isNonBlockingThrottlingMode()) {
        features.push_back("NonBlockingThrottlingMode");
    }

    ret["features"] = features;

    ret["thread"] = getThread().index;
    ret["priority"] = to_string(priority);
    ret["clustermap"] = pushed_clustermap.to_json();

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
    switch (type) {
    case Type::Normal:
        ret["type"] = "normal";
        break;
    case Type::Producer:
        ret["type"] = "producer";
        ret["flow_ctrl_buffer_size"] = dcpFlowControlBufferSize;
        break;
    case Type::Consumer:
        ret["type"] = "consumer";
        break;
    }
    ret["dcp_xattr_aware"] = isDcpXattrAware();
    ret["dcp_deleted_user_xattr"] = isDcpDeletedUserXattr();
    ret["dcp_no_value"] = isDcpNoValue();
    ret["max_reqs_per_event"] = max_reqs_per_event;
    ret["nevents"] = numEvents;

    switch (state) {
    case State::running:
        ret["state"] = "running";
        break;
    case State::closing:
        ret["state"] = "closing";
        break;
    case State::pending_close:
        ret["state"] = "pending close";
        break;
    case State::immediate_close:
        ret["state"] = "immediate close";
        break;
    }

    ret["ssl"] = ssl;
    ret["total_recv"] = totalRecv;
    ret["total_queued_send"] = totalSend;
    ret["total_send"] = totalSend - getSendQueueSize();

    ret["datatype"] = mcbp::datatype::to_string(datatypeFilter.getRaw());

    ret["sendqueue"]["size"] = sendQueueInfo.size;
    ret["sendqueue"]["last"] = sendQueueInfo.last.time_since_epoch().count();
    ret["sendqueue"]["term"] = sendQueueInfo.term;

#ifdef __linux__
    int value;
    if (ioctl(socketDescriptor, SIOCINQ, &value) == 0) {
        ret["SIOCINQ"] = value;
    }
    if (ioctl(socketDescriptor, SIOCOUTQ, &value) == 0) {
        ret["SIOCOUTQ"] = value;
    }
    socklen_t intsize = sizeof(int);
    int iobufsize;
    if (cb::net::getsockopt(socketDescriptor,
                            SOL_SOCKET,
                            SO_SNDBUF,
                            reinterpret_cast<void*>(&iobufsize),
                            &intsize) == 0) {
        ret["SNDBUF"] = iobufsize;
    }
    if (cb::net::getsockopt(socketDescriptor,
                            SOL_SOCKET,
                            SO_RCVBUF,
                            reinterpret_cast<void*>(&iobufsize),
                            &intsize) == 0) {
        ret["RCVBUF"] = iobufsize;
    }
#endif

    return ret;
}

void Connection::restartAuthentication() {
    if (authenticated) {
        if (user.domain == cb::sasl::Domain::External) {
            externalAuthManager->logoff(user.name);
        }
    }
    internal = false;
    authenticated = false;
    user = cb::rbac::UserIdent{"unknown", cb::rbac::Domain::Local};
}

void Connection::updatePrivilegeContext() {
    for (std::size_t ii = 0; ii < droppedPrivileges.size(); ++ii) {
        if (droppedPrivileges.test(ii)) {
            privilegeContext.dropPrivilege(cb::rbac::Privilege(ii));
        }
    }
    subject_to_metering.store(
            privilegeContext.check(cb::rbac::Privilege::Unmetered, {}, {})
                    .failed(),
            std::memory_order::memory_order_release);
    subject_to_throttling.store(
            privilegeContext.check(cb::rbac::Privilege::Unthrottled, {}, {})
                    .failed(),
            std::memory_order::memory_order_release);
}

cb::engine_errc Connection::dropPrivilege(cb::rbac::Privilege privilege) {
    if (isDCP() && privilege == cb::rbac::Privilege::Unthrottled) {
        // DCP connections set up as unmetered can't drop the privilege
        // as we only registered throttled DCP connections
        return cb::engine_errc::failed;
    }
    droppedPrivileges.set(int(privilege), true);
    updatePrivilegeContext();
    return cb::engine_errc::success;
}

in_port_t Connection::getParentPort() const {
    return listening_port->port;
}

cb::rbac::PrivilegeContext Connection::getPrivilegeContext() {
    if (privilegeContext.isStale()) {
        try {
            privilegeContext = cb::rbac::createContext(user, getBucket().name);
        } catch (const cb::rbac::NoSuchBucketException&) {
            // Remove all access to the bucket
            privilegeContext = cb::rbac::createContext(user, "");
            LOG_INFO(
                    "{}: RBAC: {} No access to bucket [{}]. "
                    "New privilege set: {}",
                    getId(),
                    getDescription(),
                    getBucket().name,
                    privilegeContext.to_string());
        } catch (const cb::rbac::NoSuchUserException&) {
            // Remove all access to the bucket
            privilegeContext = cb::rbac::PrivilegeContext{user.domain};
            if (isAuthenticated()) {
                LOG_INFO("{}: RBAC: {} No RBAC definition for the user.",
                         getId(),
                         getDescription());
            }
        }
        updatePrivilegeContext();
    }

    return privilegeContext;
}

Bucket& Connection::getBucket() const {
    return all_buckets[getBucketIndex()];
}

EngineIface& Connection::getBucketEngine() const {
    return getBucket().getEngine();
}

cb::engine_errc Connection::remapErrorCode(cb::engine_errc code) {
    if (xerror_support) {
        return code;
    }

    // Check our whitelist
    switch (code) {
    case cb::engine_errc::success: // FALLTHROUGH
    case cb::engine_errc::no_such_key: // FALLTHROUGH
    case cb::engine_errc::key_already_exists: // FALLTHROUGH
    case cb::engine_errc::no_memory: // FALLTHROUGH
    case cb::engine_errc::not_stored: // FALLTHROUGH
    case cb::engine_errc::invalid_arguments: // FALLTHROUGH
    case cb::engine_errc::not_supported: // FALLTHROUGH
    case cb::engine_errc::would_block: // FALLTHROUGH
    case cb::engine_errc::too_big: // FALLTHROUGH
    case cb::engine_errc::disconnect: // FALLTHROUGH
    case cb::engine_errc::not_my_vbucket: // FALLTHROUGH
    case cb::engine_errc::temporary_failure: // FALLTHROUGH
    case cb::engine_errc::out_of_range: // FALLTHROUGH
    case cb::engine_errc::rollback: // FALLTHROUGH
    case cb::engine_errc::too_busy: // FALLTHROUGH
    case cb::engine_errc::delta_badval: // FALLTHROUGH
    case cb::engine_errc::predicate_failed:
    case cb::engine_errc::failed:
        /**
         * For cb::engine_errc::stream_not_found and
         * cb::engine_errc::opaque_no_match, fallthrough as these will only ever
         * be used if the DcpConsumer has successfully enabled them using the
         * DcpControl msg with key=v7_dcp_status_codes value=true
         */
    case cb::engine_errc::stream_not_found:
    case cb::engine_errc::opaque_no_match:
    case cb::engine_errc::throttled:
        return code;

    case cb::engine_errc::too_many_connections:
    case cb::engine_errc::scope_size_limit_exceeded:
        return cb::engine_errc::too_big;

    case cb::engine_errc::locked:
        return cb::engine_errc::key_already_exists;
    case cb::engine_errc::locked_tmpfail:
        return cb::engine_errc::temporary_failure;
    case cb::engine_errc::unknown_collection:
    case cb::engine_errc::unknown_scope:
        return isCollectionsSupported() ? code
                                        : cb::engine_errc::invalid_arguments;
    case cb::engine_errc::sync_write_in_progress:
    case cb::engine_errc::sync_write_re_commit_in_progress:
        // we can return tmpfail to old clients and have them retry the
        // operation
        return cb::engine_errc::temporary_failure;
    case cb::engine_errc::cannot_apply_collections_manifest:
        // Don't disconnect for this error, just return failed. This keeps
        // ns_server connected.
        return cb::engine_errc::failed;
    case cb::engine_errc::no_access:
    case cb::engine_errc::no_bucket:
    case cb::engine_errc::authentication_stale:
    case cb::engine_errc::durability_invalid_level:
    case cb::engine_errc::durability_impossible:
    case cb::engine_errc::sync_write_pending:
    case cb::engine_errc::sync_write_ambiguous:
    case cb::engine_errc::dcp_streamid_invalid:
    case cb::engine_errc::range_scan_cancelled:
    case cb::engine_errc::range_scan_more:
    case cb::engine_errc::range_scan_complete:
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

    return cb::engine_errc::disconnect;
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

void Connection::setBucketIndex(int index, Cookie* cookie) {
    bucketIndex.store(index, std::memory_order_release);

    using cb::tracing::Code;
    using cb::tracing::SpanStopwatch;
    ScopeTimer<SpanStopwatch> timer(
            std::forward_as_tuple(cookie, Code::UpdatePrivilegeContext));

    // Update the privilege context. If a problem occurs within the RBAC
    // module we'll assign an empty privilege context to the connection.
    try {
        if (authenticated) {
            // The user have logged in, so we should create a context
            // representing the users context in the desired bucket.
            privilegeContext =
                    cb::rbac::createContext(user, all_buckets[index].name);
        } else {
            // The user has not authenticated. Assign an empty profile which
            // won't give you any privileges.
            privilegeContext = cb::rbac::PrivilegeContext{user.domain};
        }
    } catch (const cb::rbac::Exception&) {
        privilegeContext = cb::rbac::PrivilegeContext{user.domain};
    }

    if (index == 0) {
        // If we're connected to the no bucket we should return
        // no bucket instead of EACCESS. Lets give the connection all
        // possible bucket privileges
        privilegeContext.setBucketPrivileges();
    }
    updatePrivilegeContext();
}

void Connection::addCpuTime(std::chrono::nanoseconds ns) {
    total_cpu_time += ns;
    min_sched_time = std::min(min_sched_time, ns);
    max_sched_time = std::max(min_sched_time, ns);
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
    const auto limit = is_memcached_shutting_down()
                               ? std::chrono::seconds(0)
                               : (getBucket().state == Bucket::State::Ready)
                                         ? std::chrono::seconds(360)
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
        setTerminationReason("Failed to send data to client");
        shutdown();
    }
}

bool Connection::reEvaluateThrottledCookies() {
    bool throttled = false;
    for (auto& c : cookies) {
        if (c->isThrottled()) {
            if (getBucket().shouldThrottle(*c, false)) {
                throttled = true;
            } else {
                c->setThrottled(false);
                notifyIoComplete(*c, cb::engine_errc::success);
            }
        }
    }

    return throttled;
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
            if (iter == cookies.begin() || cookie->getRefcount()) {
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

    const auto maxSendQueueSize = Settings::instance().getMaxSendQueueSize();
    if (!active || cookies.back()->mayReorder()) {
        // Only look at new commands if we don't have any active commands
        // or the active command allows for reordering.
        auto input = bufferevent_get_input(bev.get());
        bool stop = !isDCP() && (getSendQueueSize() >= maxSendQueueSize);
        while (!stop && cookies.size() < maxActiveCommands &&
               isPacketAvailable() && numEvents > 0 &&
               state == State::running) {
            if (!cookies.back()->empty()) {
                // Create a new entry if we can't reuse the last entry
                cookies.emplace_back(std::make_unique<Cookie>(*this));
            }

            auto& cookie = *cookies.back();
            cookie.initialize(getPacket(), isTracingEnabled());
            auto drainSize = cookie.getPacket().size();

            updateRecvBytes(drainSize);

            const auto status = cookie.validate();
            if (status == cb::mcbp::Status::Success) {
                // We may only start execute the packet if:
                //  * We shouldn't be throttled
                //  * We don't have any ongoing commands
                //  * We have an ongoing command and this command allows
                //    for reorder
                if (getBucket().shouldThrottle(cookie, true)) {
                    if (isNonBlockingThrottlingMode()) {
                        cookie.sendResponse(cb::mcbp::Status::EWouldThrottle);
                        cookie.reset();
                    } else {
                        cookie.setThrottled(true);
                        // Set the cookie to true to block the destruction
                        // of the command (and the connection)
                        cookie.setEwouldblock(true);
                        cookie.preserveRequest();
                        if (!cookie.mayReorder()) {
                            // Don't add commands as we need the last one to
                            // complete
                            stop = true;
                        }
                    }
                } else if ((!active || cookie.mayReorder()) &&
                           cookie.execute(true)) {
                    // Command executed successfully, reset the cookie to
                    // allow it to be reused
                    cookie.reset();
                    // Check that we're not reserving too much memory for
                    // this client...
                    stop = !isDCP() && (getSendQueueSize() >= maxSendQueueSize);
                } else {
                    active = true;
                    // We need to block so we need to preserve the request
                    // as we'll drain the data from the buffer)
                    cookie.preserveRequest();
                    if (!cookie.mayReorder()) {
                        // Don't add commands as we need the last one to
                        // complete
                        stop = true;
                    }
                }
                --numEvents;
            } else {
                cookie.getConnection().getBucket().rejectCommand(cookie);
                // Packet validation failed
                cookie.sendResponse(status);
                cookie.reset();
            }

            if (evbuffer_drain(input, drainSize) == -1) {
                throw std::runtime_error(
                        "Connection::executeCommandPipeline(): Failed to "
                        "drain buffer");
            }
        }
    }

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
    if ((isDCP() || (getSendQueueSize() < maxSendQueueSize)) &&
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

void Connection::resumeThrottledDcpStream() {
    dcpStreamThrottled = false;
}

void Connection::tryToProgressDcpStream() {
    if (cookies.empty()) {
        throw std::runtime_error(
                "Connection::executeCommandsCallback(): no cookies "
                "available!");
    }

    if (dcpStreamThrottled) {
        return;
    }

    // Currently working on an incomming packet
    if (!cookies.front()->empty()) {
        return;
    }

    // make sure we reset the privilege context
    cookies.front()->reset();

    // MB-38007: We see an increase in rebalance time for
    // "in memory" workloads when allowing DCP to fill up to
    // 40MB (thats the default) batch size into the output buffer.
    // We've not been able to figure out exactly _why_ this is
    // happening and have assumptions that it may be caused
    // that it doesn't align too much with the flow control being
    // used. Before moving to bufferevent we would copy the entire
    // message into kernel space before trying to read (and process)
    // any input messages before trying to send the next one.
    // It could be that it would be better at processing the
    // incoming flow control messages instead of the current
    // model where the input socket gets drained and put in
    // userspace buffers, the send queue is tried to be drained
    // before we do the callback and process the already queued
    // input and generate more output before returning to the
    // layer doing the actual IO.
    std::size_t dcpMaxQSize =
            (dcpFlowControlBufferSize == 0)
                    ? Settings::instance().getMaxSendQueueSize()
                    : 1024 * 1024;
    bool more = (getSendQueueSize() < dcpMaxQSize);
    if (type == Type::Consumer) {
        // We want the consumer to perform some steps because
        // it could be pending bufferAcks
        numEvents = max_reqs_per_event;
    }
    while (more && numEvents > 0) {
        const auto ret = getBucket().getDcpIface()->step(
                *cookies.front().get(),
                getBucket().shouldThrottleDcp(*this),
                *this);
        switch (remapErrorCode(ret)) {
        case cb::engine_errc::success:
            more = (getSendQueueSize() < dcpMaxQSize);
            --numEvents;
            break;
        case cb::engine_errc::throttled:
            dcpStreamThrottled = true;
            // fallthrough
        case cb::engine_errc::would_block:
            more = false;
            break;
        default:
            LOG_WARNING(R"({}: step returned {} - closing connection {})",
                        getId(),
                        cb::to_string(ret),
                        getDescription());
            if (ret == cb::engine_errc::disconnect) {
                setTerminationReason("Engine forced disconnect");
            }
            shutdown();
            more = false;
        }
    }
    if (more && numEvents == 0) {
        // We used the entire timeslice... schedule a new one
        triggerCallback();
    }
}

void Connection::processNotifiedCookie(Cookie& cookie, cb::engine_errc status) {
    using std::chrono::duration_cast;
    using std::chrono::microseconds;
    using std::chrono::nanoseconds;

    const auto start = std::chrono::steady_clock::now();
    try {
        Expects(cookie.isEwouldblock());
        cookie.setAiostat(status);
        cookie.setEwouldblock(false);
        if (cookie.execute()) {
            // completed!!! time to clean up after it and process the
            // command pipeline? / schedule more?
            if (cookies.front().get() == &cookie) {
                cookies.front()->reset();
            } else {
                cookies.erase(
                        std::remove_if(cookies.begin(),
                                       cookies.end(),
                                       [ptr = &cookie](const auto& cookie) {
                                           return ptr == cookie.get();
                                       }),
                        cookies.end());
            }
            triggerCallback();
        }
    } catch (const std::exception& e) {
        logExecutionException("processNotifiedCookie", e);
    }

    const auto stop = std::chrono::steady_clock::now();
    const auto ns = duration_cast<nanoseconds>(stop - start);
    scheduler_info[getThread().index].add(duration_cast<microseconds>(ns));
    addCpuTime(ns);
}

void Connection::commandExecuted(Cookie& cookie) {
    getBucket().commandExecuted(cookie);
}

void Connection::logExecutionException(const std::string_view where,
                                       const std::exception& e) {
    setTerminationReason(std::string("Received exception: ") + e.what());
    shutdown();

    try {
        auto array = nlohmann::json::array();
        for (const auto& c : cookies) {
            if (c && !c->empty()) {
                array.push_back(c->toJSON());
            }
        }
        auto callstack = nlohmann::json::array();
        if (const auto* backtrace = cb::getBacktrace(e)) {
            print_backtrace_frames(*backtrace, [&callstack](const char* frame) {
                callstack.emplace_back(frame);
            });
            LOG_ERROR(
                    "{}: Exception occurred during {}. Closing connection: "
                    "{}. Cookies: {} Exception thrown from: {}",
                    getId(),
                    where,
                    e.what(),
                    array.dump(),
                    callstack.dump());
        } else {
            LOG_ERROR(
                    "{}: Exception occurred during {}. Closing connection: "
                    "{}. Cookies: {}",
                    getId(),
                    where,
                    e.what(),
                    array.dump());
        }
    } catch (const std::exception& exception2) {
        try {
            LOG_ERROR(
                    "{}: Second exception occurred during {}. Closing "
                    "connection: e:{} exception2:{}",
                    getId(),
                    where,
                    e.what(),
                    exception2.what());
            if (const auto* backtrace = cb::getBacktrace(e)) {
                LOG_ERROR("{}: Exception thrown from:", getId());
                print_backtrace_frames(*backtrace, [this](const char* frame) {
                    LOG_ERROR("{} -    {}", getId(), frame);
                });
            }
        } catch (const std::bad_alloc&) {
            // Logging failed.
        }
    } catch (...) {
        // catch all, defensive as possible
    }
}

void Connection::reEvaluateParentPort() {
    if (listening_port->valid) {
        return;
    }

    bool localhost = false;
    if (Settings::instance().isLocalhostInterfaceWhitelisted()) {
        // Make sure we don't tear down localhost connections
        if (listening_port->family == AF_INET) {
            localhost =
                    peername.find(R"("ip":"127.0.0.1")") != std::string::npos;
        } else {
            localhost = peername.find(R"("ip":"::1")") != std::string::npos;
        }
    }

    if (localhost) {
        LOG_INFO(
                "{} Keeping connection alive even if server port was removed: "
                "{}",
                getId(),
                getDescription());
    } else {
        LOG_INFO("{} Shutting down; server port was removed: {}",
                 getId(),
                 getDescription());
        setTerminationReason("Server port shut down");
        shutdown();
        signalIfIdle();
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
            logExecutionException("packet execution", e);
        }
    }

    if (isDCP() && state == State::running) {
        try {
            tryToProgressDcpStream();
        } catch (const std::exception& e) {
            logExecutionException("DCP step()", e);
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
            if (isDCP()) {
                thread.removeThrottleableDcpConnection(*this);
            }
            disassociate_bucket(*this);
            // delete the object
            return false;
        }
    }
    return true;
}

std::string Connection::getOpenSSLErrors() {
    unsigned long err;
    auto buffer = thread.getScratchBuffer();
    std::vector<std::string> messages;
    while ((err = bufferevent_get_openssl_error(bev.get())) != 0) {
        std::stringstream ss;
        ERR_error_string_n(err, buffer.data(), buffer.size());
        ss << "{" << buffer.data() << "},";
        messages.emplace_back(ss.str());
    }

    if (messages.empty()) {
        return {};
    }

    if (messages.size() == 1) {
        // remove trailing ,
        messages.front().pop_back();
        return messages.front();
    }

    std::reverse(messages.begin(), messages.end());
    std::string ret = "[";
    for (const auto& a : messages) {
        ret.append(a);
    }
    ret.back() = ']';
    return ret;
}

void Connection::read_callback() {
    if (isSslEnabled()) {
        const auto ssl_errors = getOpenSSLErrors();
        if (!ssl_errors.empty()) {
            LOG_INFO("{} - OpenSSL errors reported: {}",
                     this->getId(),
                     ssl_errors);
        }
    }

    TRACE_LOCKGUARD_TIMED(thread.mutex,
                          "mutex",
                          "Connection::read_callback::threadLock",
                          SlowMutexThreshold);

    if (!executeCommandsCallback()) {
        conn_destroy(this);
    }
}

void Connection::read_callback(bufferevent*, void* ctx) {
    reinterpret_cast<Connection*>(ctx)->read_callback();
}

void Connection::write_callback() {
    if (isSslEnabled()) {
        const auto ssl_errors = getOpenSSLErrors();
        if (!ssl_errors.empty()) {
            LOG_INFO("{} - OpenSSL errors reported: {}", getId(), ssl_errors);
        }
    }

    TRACE_LOCKGUARD_TIMED(thread.mutex,
                          "mutex",
                          "Connection::rw_callback::threadLock",
                          SlowMutexThreshold);

    if (!executeCommandsCallback()) {
        conn_destroy(this);
    }
}

void Connection::write_callback(bufferevent*, void* ctx) {
    reinterpret_cast<Connection*>(ctx)->write_callback();
}

static nlohmann::json BevEvent2Json(short event) {
    if (!event) {
        return {};
    }
    nlohmann::json err = nlohmann::json::array();

    if ((event & BEV_EVENT_READING) == BEV_EVENT_READING) {
        err.push_back("reading");
    }
    if ((event & BEV_EVENT_WRITING) == BEV_EVENT_WRITING) {
        err.push_back("writing");
    }
    if ((event & BEV_EVENT_EOF) == BEV_EVENT_EOF) {
        err.push_back("EOF");
    }
    if ((event & BEV_EVENT_ERROR) == BEV_EVENT_ERROR) {
        err.push_back("error");
    }
    if ((event & BEV_EVENT_TIMEOUT) == BEV_EVENT_TIMEOUT) {
        err.push_back("timeout");
    }
    if ((event & BEV_EVENT_CONNECTED) == BEV_EVENT_CONNECTED) {
        err.push_back("connected");
    }

    const short known = BEV_EVENT_READING | BEV_EVENT_WRITING | BEV_EVENT_EOF |
                        BEV_EVENT_ERROR | BEV_EVENT_TIMEOUT |
                        BEV_EVENT_CONNECTED;

    if (event & ~known) {
        err.push_back(cb::to_hex(uint16_t(event)));
    }

    return err;
}

void Connection::event_callback(bufferevent* bev, short event, void* ctx) {
    auto& instance = *reinterpret_cast<Connection*>(ctx);
    bool term = false;

    std::string ssl_errors;
    if (instance.isSslEnabled()) {
        ssl_errors = instance.getOpenSSLErrors();
    }

    if ((event & BEV_EVENT_EOF) == BEV_EVENT_EOF) {
        LOG_DEBUG("{}: Socket EOF", instance.getId());
        instance.setTerminationReason("Client closed connection");
        term = true;
    } else if ((event & BEV_EVENT_ERROR) == BEV_EVENT_ERROR) {
        // Note: SSL connections may fail for reasons different than socket
        // error, so we avoid to dump errno:0 (ie, socket operation success).
        const auto sockErr = EVUTIL_SOCKET_ERROR();
        if (sockErr != 0) {
            const auto errStr = evutil_socket_error_to_string(sockErr);
            if (sockErr == ECONNRESET) {
                LOG_INFO(
                        "{}: Unrecoverable error encountered: {}, "
                        "socket_error: {}:{}, shutting down connection",
                        instance.getId(),
                        BevEvent2Json(event).dump(),
                        sockErr,
                        errStr);
            } else {
                LOG_WARNING(
                        "{}: Unrecoverable error encountered: {}, "
                        "socket_error: {}:{}, shutting down connection",
                        instance.getId(),
                        BevEvent2Json(event).dump(),
                        sockErr,
                        errStr);
            }
            instance.setTerminationReason(
                    "socket_error: " + std::to_string(sockErr) + ":" + errStr);
        } else if (!ssl_errors.empty()) {
            LOG_WARNING(
                    "{}: Unrecoverable error encountered: {}, ssl_error: "
                    "{}, shutting down connection",
                    instance.getId(),
                    BevEvent2Json(event).dump(),
                    ssl_errors);
            instance.setTerminationReason("ssl_error: " + ssl_errors);
        } else {
            LOG_WARNING(
                    "{}: Unrecoverable error encountered: {}, shutting down "
                    "connection",
                    instance.getId(),
                    BevEvent2Json(event).dump());
            instance.setTerminationReason("Network error");
        }

        term = true;
    }

    if (term) {
        auto& thread = instance.getThread();
        TRACE_LOCKGUARD_TIMED(thread.mutex,
                              "mutex",
                              "Connection::event_callback::threadLock",
                              SlowMutexThreshold);
        // MB-44460: If a connection disconnects before all of the data
        //           was moved to the kernels send buffer we would still
        //           wait for the send buffer to be drained before closing
        //           the connection. Given that the other side hung up that
        //           will never happen so we should just terminate the
        //           send queue immediately.
        //           note: This extra complexity was added so that we could
        //           send error messages back to the client and then
        //           disconnect the socket once all data was sent to the
        //           client (and bufferevent performs the actual send/recv
        //           on the socket after the callback returned)
        instance.sendQueueInfo.term = true;

        if (instance.state == State::running) {
            instance.shutdown();
        }

        if (!instance.executeCommandsCallback()) {
            conn_destroy(&instance);
        }
    }
}

void Connection::ssl_read_callback(bufferevent* bev, void* ctx) {
    auto& instance = *reinterpret_cast<Connection*>(ctx);

    const auto ssl_errors = instance.getOpenSSLErrors();
    if (!ssl_errors.empty()) {
        LOG_INFO("{} - OpenSSL errors reported: {}",
                 instance.getId(),
                 ssl_errors);
    }

    // Let's inspect the certificate before we'll do anything further
    auto* ssl_st = bufferevent_openssl_get_ssl(bev);
    const auto verifyMode = SSL_get_verify_mode(ssl_st);
    const auto enabled = ((verifyMode & SSL_VERIFY_PEER) == SSL_VERIFY_PEER);

    bool disconnect = false;
    cb::openssl::unique_x509_ptr cert(SSL_get_peer_certificate(ssl_st));
    if (enabled) {
        const auto mandatory =
                ((verifyMode & SSL_VERIFY_FAIL_IF_NO_PEER_CERT) ==
                 SSL_VERIFY_FAIL_IF_NO_PEER_CERT);
        // Check certificate
        if (cert) {
            class ServerAuthMapper {
            public:
                static std::pair<cb::x509::Status, std::string> lookup(
                        X509* cert) {
                    static ServerAuthMapper inst;
                    return inst.mapper->lookupUser(cert);
                }

            protected:
                ServerAuthMapper() {
                    mapper = cb::x509::ClientCertConfig::create(R"({
"prefixes": [
    {
        "path": "san.email",
        "prefix": "",
        "delimiter": "",
        "suffix":"@internal.couchbase.com"
    }
]
})"_json);
                }

                std::unique_ptr<cb::x509::ClientCertConfig> mapper;
            };

            auto [status, name] = ServerAuthMapper::lookup(cert.get());
            if (status == cb::x509::Status::Success) {
                if (name == "internal") {
                    name = "@internal";
                } else {
                    status = cb::x509::Status::NoMatch;
                }
            } else {
                auto pair = Settings::instance().lookupUser(cert.get());
                status = pair.first;
                name = std::move(pair.second);
            }

            switch (status) {
            case cb::x509::Status::NoMatch:
                audit_auth_failure(instance,
                                   {"unknown", cb::sasl::Domain::Local},
                                   "Failed to map a user from the client "
                                   "provided X.509 certificate");
                instance.setTerminationReason(
                        "Failed to map a user from the client provided X.509 "
                        "certificate");
                LOG_WARNING(
                        "{}: Failed to map a user from the "
                        "client provided X.509 certificate: [{}]",
                        instance.getId(),
                        name);
                disconnect = true;
                break;
            case cb::x509::Status::Error:
                audit_auth_failure(
                        instance,
                        {"unknown", cb::sasl::Domain::Local},
                        "Failed to use client provided X.509 certificate");
                instance.setTerminationReason(
                        "Failed to use client provided X.509 certificate");
                LOG_WARNING(
                        "{}: Disconnection client due to error with the X.509 "
                        "certificate [{}]",
                        instance.getId(),
                        name);
                disconnect = true;
                break;
            case cb::x509::Status::NotPresent:
                // Note: NotPresent in this context is that there is no
                //       mapper present in the _configuration_ which is
                //       allowed in "Enabled" mode as it just means that we'll
                //       try to verify the peer.
                if (mandatory) {
                    const char* reason =
                            "The server does not have any mapping rules "
                            "configured for certificate authentication";
                    audit_auth_failure(instance,
                                       {"unknown", cb::sasl::Domain::Local},
                                       reason);
                    instance.setTerminationReason(reason);
                    disconnect = true;
                    LOG_WARNING("{}: Disconnecting client: {}",
                                instance.getId(),
                                reason);
                }
                break;
            case cb::x509::Status::Success:
                if (!instance.tryAuthFromSslCert(name,
                                                 SSL_get_cipher_name(ssl_st))) {
                    // Already logged
                    const std::string reason =
                            "User [" + name + "] not defined in Couchbase";
                    audit_auth_failure(instance,
                                       {name, cb::sasl::Domain::Local},
                                       reason.c_str());
                    instance.setTerminationReason(reason.c_str());
                    disconnect = true;
                }
            }
        }
    }

    if (disconnect) {
        instance.shutdown();
    } else if (!instance.authenticated) {
        // tryAuthFromSslCertificate logged the cipher
        LOG_INFO("{}: Using cipher '{}', peer certificate {}provided",
                 instance.getId(),
                 SSL_get_cipher_name(ssl_st),
                 cert ? "" : "not ");
    }

    // update the callback to call the normal read callback
    bufferevent_setcb(bev,
                      Connection::read_callback,
                      Connection::write_callback,
                      Connection::event_callback,
                      ctx);

    // and let's call it to make sure we step through the state machinery
    Connection::read_callback(bev, ctx);
}

void Connection::setAuthenticated(bool authenticated_,
                                  bool internal_,
                                  cb::rbac::UserIdent ui) {
    authenticated = authenticated_;
    internal = internal_;
    user = std::move(ui);
    if (authenticated_) {
        updateDescription();
        droppedPrivileges.reset();
        privilegeContext = cb::rbac::createContext(user, "");
    } else {
        updateDescription();
        privilegeContext = cb::rbac::PrivilegeContext{user.domain};
    }
    updatePrivilegeContext();
}

bool Connection::tryAuthFromSslCert(const std::string& userName,
                                    std::string_view cipherName) {
    try {
        auto context = cb::rbac::createInitialContext(
                {userName, cb::sasl::Domain::Local});
        setAuthenticated(
                true, context.second, {userName, cb::sasl::Domain::Local});
        audit_auth_success(*this);
        LOG_INFO(
                "{}: Client {} using cipher '{}' authenticated as '{}' via "
                "X.509 certificate",
                getId(),
                getPeername(),
                cipherName,
                cb::UserDataView(user.name));
        // External users authenticated by using X.509 certificates should not
        // be able to use SASL to change it's identity.
        saslAuthEnabled = internal;
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

void Connection::updateSendBytes(size_t nbytes) {
    totalSend += nbytes;
    get_thread_stats(this)->bytes_written += nbytes;
}

void Connection::updateRecvBytes(size_t nbytes) {
    totalRecv += nbytes;
    get_thread_stats(this)->bytes_read += nbytes;
}

void Connection::copyToOutputStream(std::string_view data) {
    if (data.empty()) {
        return;
    }

    if (bufferevent_write(bev.get(), data.data(), data.size()) == -1) {
        throw std::bad_alloc();
    }

    updateSendBytes(data.size());
}

void Connection::copyToOutputStream(gsl::span<std::string_view> data) {
    size_t nb = 0;
    for (const auto& d : data) {
        if (bufferevent_write(bev.get(), d.data(), d.size()) == -1) {
            throw std::bad_alloc();
        }
        nb += d.size();
    }
    updateSendBytes(nb);
}

static void sendbuffer_cleanup_cb(const void*, size_t, void* extra) {
    delete reinterpret_cast<SendBuffer*>(extra);
}

void Connection::chainDataToOutputStream(std::unique_ptr<SendBuffer> buffer) {
    if (!buffer || buffer->getPayload().empty()) {
        throw std::logic_error(
                "Connection::chainDataToOutputStream: buffer must be set");
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
    updateSendBytes(data.size());
}

Connection::Connection(FrontEndThread& thr)
    : peername(R"({"ip":"unknown","port":0})"),
      sockname(R"({"ip":"unknown","port":0})"),
      thread(thr),
      max_reqs_per_event(Settings::instance().getRequestsPerEventNotification(
              EventPriority::Default)),
      socketDescriptor(INVALID_SOCKET),
      connectedToSystemPort(false),
      ssl(false) {
    updateDescription();
    cookies.emplace_back(std::make_unique<Cookie>(*this));
    setConnectionId("unknown:0");
    stats.conn_structs++;
}

Connection::Connection(SOCKET sfd,
                       FrontEndThread& thr,
                       std::shared_ptr<ListeningPort> descr,
                       uniqueSslPtr sslStructure)
    : peername(cb::net::getPeerNameAsJson(sfd).dump()),
      sockname(cb::net::getSockNameAsJson(sfd).dump()),
      thread(thr),
      listening_port(std::move(descr)),
      max_reqs_per_event(Settings::instance().getRequestsPerEventNotification(
              EventPriority::Default)),
      socketDescriptor(sfd),
      parent_port(listening_port->port),
      connectedToSystemPort(listening_port->system),
      ssl(sslStructure) {
    setTcpNoDelay(true);
    updateDescription();
    cookies.emplace_back(std::make_unique<Cookie>(*this));
    setConnectionId(cb::net::getpeername(socketDescriptor).c_str());

    // We need to use BEV_OPT_UNLOCK_CALLBACKS (which again require
    // BEV_OPT_DEFER_CALLBACKS) to avoid lock ordering problem (and potential
    // deadlock) because otherwise we'll hold the internal mutex in libevent
    // as part of the callback and later on we acquire the worker threads
    // mutex, but when we try to signal another cookie we hold
    // the worker thread mutex when we try to acquire the mutex inside
    // libevent.
    const auto options = BEV_OPT_THREADSAFE | BEV_OPT_UNLOCK_CALLBACKS |
                         BEV_OPT_CLOSE_ON_FREE | BEV_OPT_DEFER_CALLBACKS;
    if (ssl) {
        bev.reset(
                bufferevent_openssl_socket_new(thr.eventBase.getLibeventBase(),
                                               sfd,
                                               sslStructure.release(),
                                               BUFFEREVENT_SSL_ACCEPTING,
                                               options));
        bufferevent_setcb(bev.get(),
                          Connection::ssl_read_callback,
                          Connection::write_callback,
                          Connection::event_callback,
                          static_cast<void*>(this));
    } else {
        bev.reset(bufferevent_socket_new(
                thr.eventBase.getLibeventBase(), sfd, options));
        bufferevent_setcb(bev.get(),
                          Connection::read_callback,
                          Connection::write_callback,
                          Connection::event_callback,
                          static_cast<void*>(this));
    }

    bufferevent_enable(bev.get(), EV_READ);
    stats.conn_structs++;
}

Connection::~Connection() {
    cb::audit::addSessionTerminated(*this);

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

void Connection::setTerminationReason(std::string reason) {
    if (terminationReason.empty()) {
        terminationReason = std::move(reason);
    } else {
        terminationReason.append(";");
        terminationReason.append(reason);
    }
}

void Connection::setAgentName(std::string_view name) {
    auto size = std::min(name.size(), agentName.size() - 1);
    std::copy(name.begin(), name.begin() + size, agentName.begin());
    agentName[size] = '\0';
}

void Connection::setConnectionId(std::string_view uuid) {
    auto size = std::min(uuid.size(), connectionId.size() - 1);
    std::copy(uuid.begin(), uuid.begin() + size, connectionId.begin());
    // the uuid string shall always be zero terminated
    connectionId[size] = '\0';
    // Remove any occurrences of " so that the client won't be allowed
    // to mess up the output where we log the cid
    std::replace(connectionId.begin(), connectionId.end(), '"', ' ');
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
            getBucket().getEngine().disconnect(*cookie);
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
        triggerCallback();
        return true;
    }
    return false;
}

void Connection::setPriority(ConnectionPriority priority_) {
    priority.store(priority_);
    switch (priority_) {
    case ConnectionPriority::High:
        max_reqs_per_event =
                Settings::instance().getRequestsPerEventNotification(
                        EventPriority::High);
        return;
    case ConnectionPriority::Medium:
        max_reqs_per_event =
                Settings::instance().getRequestsPerEventNotification(
                        EventPriority::Medium);
        return;
    case ConnectionPriority::Low:
        max_reqs_per_event =
                Settings::instance().getRequestsPerEventNotification(
                        EventPriority::Low);
        return;
    }
    throw std::invalid_argument("Unknown priority: " +
                                std::to_string(int(priority_)));
}

bool Connection::selectedBucketIsXattrEnabled() const {
    // The unit tests call this method with no bucket
    if (bucketIndex == 0) {
        return Settings::instance().isXattrEnabled();
    }
    return Settings::instance().isXattrEnabled() &&
           getBucketEngine().isXattrEnabled();
}

void Connection::disableReadEvent() {
    if ((bufferevent_get_enabled(bev.get()) & EV_READ) == EV_READ) {
        if (bufferevent_disable(bev.get(), EV_READ) == -1) {
            throw std::runtime_error(
                    "Connection::disableReadEvent: Failed to disable read "
                    "events");
        }
    }
}

void Connection::enableReadEvent() {
    if ((bufferevent_get_enabled(bev.get()) & EV_READ) == 0) {
        if (bufferevent_enable(bev.get(), EV_READ) == -1) {
            throw std::runtime_error(
                    "Connection::enableReadEvent: Failed to enable read "
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

void Connection::setDcpFlowControlBufferSize(std::size_t size) {
    if (type == Type::Producer) {
        LOG_INFO("{} - using DCP buffer size of {}", getId(), size);
        dcpFlowControlBufferSize = size;
    } else {
        throw std::logic_error(
                "Connection::setDcpFlowControlBufferSize should only be called "
                "on DCP Producers");
    }
}

static constexpr size_t MaxFrameInfoSize =
        cb::mcbp::response::ServerRecvSendDurationFrameInfoSize +
        cb::mcbp::response::ReadUnitsFrameInfoSize +
        cb::mcbp::response::WriteUnitsFrameInfoSize;

std::string_view Connection::formatResponseHeaders(Cookie& cookie,
                                                   cb::char_buffer dest,
                                                   cb::mcbp::Status status,
                                                   std::size_t extras_len,
                                                   std::size_t key_len,
                                                   std::size_t value_len,
                                                   uint8_t datatype) {
    if (dest.size() < sizeof(cb::mcbp::Response) + MaxFrameInfoSize) {
        throw std::runtime_error(
                "Connection::formatResponseHeaders: The provided buffer must "
                "be big enough to hold header and ALL possible response "
                "frame infos");
    }

    const auto& request = cookie.getRequest();
    auto wbuf = dest;
    auto& response = *reinterpret_cast<cb::mcbp::Response*>(wbuf.data());

    response.setOpcode(request.getClientOpcode());
    response.setKeylen(gsl::narrow_cast<uint16_t>(key_len));
    response.setExtlen(gsl::narrow_cast<uint8_t>(extras_len));
    response.setDatatype(cb::mcbp::Datatype(datatype));
    response.setStatus(status);
    response.setOpaque(request.getOpaque());
    response.setCas(cookie.getCas());

    const auto tracing = isTracingEnabled() && cookie.isTracingEnabled();
    auto cutracing = isReportUnitUsage() && isSubjectToMetering();
    size_t ru = 0;
    size_t wu = 0;
    if (cutracing) {
        auto [read, write] = cookie.getDocumentRWBytes();
        if (!read && !write) {
            cutracing = false;
        } else {
            auto& inst = cb::serverless::Config::instance();
            ru = inst.to_ru(read);
            wu = inst.to_wu(write);
            if (!ru && !wu) {
                cutracing = false;
            }
        }
    }

    if (tracing || cutracing) {
        using namespace cb::mcbp::response;
        response.setMagic(cb::mcbp::Magic::AltClientResponse);
        // We can't use a 16 bits key length when using the alternative
        // response header.. Verify that the key fits in a single byte.
        if (key_len > 255) {
            throw std::runtime_error(
                    "formatResponseHeaders: The provided key can't be put in "
                    "an AltClientResponse (" +
                    std::to_string(key_len) + " > 255)");
        }
        uint8_t framing_extras_size = 0;
        if (tracing) {
            framing_extras_size += ServerRecvSendDurationFrameInfoSize;
        }
        if (ru) {
            framing_extras_size += ReadUnitsFrameInfoSize;
        }
        if (wu) {
            framing_extras_size += WriteUnitsFrameInfoSize;
        }
        response.setFramingExtraslen(framing_extras_size);
        response.setBodylen(value_len + extras_len + key_len +
                            framing_extras_size);
        auto* ptr = wbuf.data() + sizeof(cb::mcbp::Response);

        auto add_frame_info = [&ptr](auto id, uint16_t val) {
            *ptr = id;
            ++ptr;
            val = htons(val);
            memcpy(ptr, &val, sizeof(val));
            ptr += sizeof(val);
        };

        if (tracing) {
            auto& tracer = cookie.getTracer();
            add_frame_info(ServerRecvSendDurationFrameInfoMagic,
                           tracer.getEncodedMicros());
        }

        if (ru) {
            add_frame_info(ReadUnitsFrameInfoMagic,
                           gsl::narrow_cast<uint16_t>(ru));
        }

        if (wu) {
            add_frame_info(WriteUnitsFrameInfoMagic,
                           gsl::narrow_cast<uint16_t>(wu));
        }

        wbuf = {wbuf.data(), sizeof(cb::mcbp::Response) + framing_extras_size};
    } else {
        response.setMagic(cb::mcbp::Magic::ClientResponse);
        response.setFramingExtraslen(0);
        response.setBodylen(value_len + extras_len + key_len);
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
    ++getBucket().responseCounters[uint16_t(status)];

    return {wbuf.data(), wbuf.size()};
}

void Connection::sendResponseHeaders(Cookie& cookie,
                                     cb::mcbp::Status status,
                                     std::string_view extras,
                                     std::string_view key,
                                     std::size_t value_len,
                                     uint8_t datatype) {
    std::array<char, sizeof(cb::mcbp::Response) + MaxFrameInfoSize> buffer;

    auto wbuf = formatResponseHeaders(cookie,
                                      {buffer.data(), buffer.size()},
                                      status,
                                      extras.size(),
                                      key.size(),
                                      value_len,
                                      datatype);
    copyToOutputStream(wbuf, extras, key);
}

void Connection::sendResponse(Cookie& cookie,
                              cb::mcbp::Status status,
                              std::string_view extras,
                              std::string_view key,
                              std::string_view value,
                              uint8_t datatype,
                              std::unique_ptr<SendBuffer> sendbuffer) {
    cookie.setResponseStatus(status);
    if (sendbuffer) {
        if (sendbuffer->getPayload().size() != value.size()) {
            throw std::runtime_error(
                    "Connection::sendResponse: The sendbuffers payload must "
                    "match the value encoded in the response");
        }
        sendResponseHeaders(
                cookie, status, extras, key, value.size(), datatype);
        chainDataToOutputStream(std::move(sendbuffer));
    } else {
        std::array<char, sizeof(cb::mcbp::Response) + MaxFrameInfoSize> buffer;
        auto wbuf = formatResponseHeaders(cookie,
                                          {buffer.data(), buffer.size()},
                                          status,
                                          extras.size(),
                                          key.size(),
                                          value.size(),
                                          datatype);
        copyToOutputStream(wbuf, extras, key, value);
    }
}

cb::engine_errc Connection::add_packet_to_send_pipe(
        cb::const_byte_buffer packet) {
    try {
        copyToOutputStream(packet);
    } catch (const std::bad_alloc&) {
        return cb::engine_errc::too_big;
    }

    return cb::engine_errc::success;
}

////////////////////////////////////////////////////////////////////////////
//                                                                        //
//                   DCP Message producer interface                       //
//                                                                        //
////////////////////////////////////////////////////////////////////////////

cb::engine_errc Connection::get_failover_log(uint32_t opaque, Vbid vbucket) {
    cb::mcbp::Request req = {};
    req.setMagic(cb::mcbp::Magic::ClientRequest);
    req.setOpcode(cb::mcbp::ClientOpcode::DcpGetFailoverLog);
    req.setOpaque(opaque);
    req.setVBucket(vbucket);

    return add_packet_to_send_pipe(req.getFrame());
}

cb::engine_errc Connection::stream_req(uint32_t opaque,
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

cb::engine_errc Connection::add_stream_rsp(uint32_t opaque,
                                           uint32_t dialogopaque,
                                           cb::mcbp::Status status) {
    cb::mcbp::response::DcpAddStreamPayload extras;
    extras.setOpaque(dialogopaque);
    cb::mcbp::ResponseBuilder builder(thread.getScratchBuffer());
    builder.setMagic(cb::mcbp::Magic::ClientResponse);
    builder.setOpcode(cb::mcbp::ClientOpcode::DcpAddStream);
    builder.setStatus(status);
    builder.setOpaque(opaque);
    builder.setExtras(extras.getBuffer());

    return add_packet_to_send_pipe(builder.getFrame()->getFrame());
}

cb::engine_errc Connection::marker_rsp(uint32_t opaque,
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

cb::engine_errc Connection::set_vbucket_state_rsp(uint32_t opaque,
                                                  cb::mcbp::Status status) {
    cb::mcbp::ResponseBuilder builder(thread.getScratchBuffer());
    builder.setMagic(cb::mcbp::Magic::ClientResponse);
    builder.setOpcode(cb::mcbp::ClientOpcode::DcpSetVbucketState);
    builder.setStatus(status);
    builder.setOpaque(opaque);

    return add_packet_to_send_pipe(builder.getFrame()->getFrame());
}

cb::engine_errc Connection::stream_end(uint32_t opaque,
                                       Vbid vbucket,
                                       cb::mcbp::DcpStreamEndStatus status,
                                       cb::mcbp::DcpStreamId sid) {
    using Framebuilder = cb::mcbp::FrameBuilder<cb::mcbp::Request>;
    Framebuilder builder(thread.getScratchBuffer());
    builder.setMagic(sid ? cb::mcbp::Magic::AltClientRequest
                         : cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::DcpStreamEnd);
    builder.setOpaque(opaque);
    builder.setVBucket(vbucket);

    cb::mcbp::request::DcpStreamEndPayload payload;
    payload.setStatus(status);

    builder.setExtras(
            {reinterpret_cast<const uint8_t*>(&payload), sizeof(payload)});

    if (sid) {
        cb::mcbp::DcpStreamIdFrameInfo framedSid(sid);
        builder.setFramingExtras(framedSid.getBuf());
    }

    return add_packet_to_send_pipe(builder.getFrame()->getFrame());
}

cb::engine_errc Connection::marker(uint32_t opaque,
                                   Vbid vbucket,
                                   uint64_t start_seqno,
                                   uint64_t end_seqno,
                                   uint32_t flags,
                                   std::optional<uint64_t> hcs,
                                   std::optional<uint64_t> mvs,
                                   std::optional<uint64_t> timestamp,
                                   cb::mcbp::DcpStreamId sid) {
    using Framebuilder = cb::mcbp::FrameBuilder<cb::mcbp::Request>;
    using cb::mcbp::Request;
    using cb::mcbp::request::DcpSnapshotMarkerV1Payload;
    using cb::mcbp::request::DcpSnapshotMarkerV2_1Value;
    using cb::mcbp::request::DcpSnapshotMarkerV2xPayload;

    // Allocate the buffer to be big enough for all cases, which will be the
    // v2.0 packet
    const auto size = sizeof(Request) + sizeof(cb::mcbp::DcpStreamIdFrameInfo) +
                      sizeof(DcpSnapshotMarkerV2xPayload) +
                      sizeof(DcpSnapshotMarkerV2_1Value);
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

    cb::mcbp::DcpSnapshotMarker marker(
            start_seqno, end_seqno, flags, hcs, mvs, timestamp);
    marker.encode(builder);

    return add_packet_to_send_pipe(builder.getFrame()->getFrame());
}

cb::engine_errc Connection::mutation(uint32_t opaque,
                                     cb::unique_item_ptr it,
                                     Vbid vbucket,
                                     uint64_t by_seqno,
                                     uint64_t rev_seqno,
                                     uint32_t lock_time,
                                     uint8_t nru,
                                     cb::mcbp::DcpStreamId sid) {
    auto key = it->getDocKey();

    const auto doc_read_bytes = key.size() + it->getValueView().size();

    // The client doesn't support collections, so must not send an encoded key
    if (!isCollectionsSupported()) {
        key = key.makeDocKeyWithoutCollectionID();
    }

    cb::mcbp::request::DcpMutationPayload extras(
            by_seqno,
            rev_seqno,
            it->getFlags(),
            gsl::narrow<uint32_t>(it->getExptime()),
            lock_time,
            nru);

    cb::mcbp::DcpStreamIdFrameInfo frameExtras(sid);

    const auto value = it->getValueView();
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
        builder.setCas(it->getCas());
        builder.setDatatype(cb::mcbp::Datatype(it->getDataType()));
        const auto ret =
                add_packet_to_send_pipe(builder.getFrame()->getFrame());
        if (ret == cb::engine_errc::success) {
            getBucket().recordMeteringReadBytes(*this, doc_read_bytes);
        }
        return ret;
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
    req.setCas(it->getCas());
    req.setDatatype(cb::mcbp::Datatype(it->getDataType()));

    if (sid) {
        req.setFramingExtraslen(sizeof(cb::mcbp::DcpStreamIdFrameInfo));
    }

    try {
        std::string_view sidbuffer;
        if (sid) {
            sidbuffer = frameExtras.getBuffer();
        }

        if (value.size() > SendBuffer::MinimumDataSize) {
            copyToOutputStream(
                    {reinterpret_cast<const char*>(&req), sizeof(req)},
                    sidbuffer,
                    extras.getBuffer(),
                    key.getBuffer());
            chainDataToOutputStream(std::make_unique<ItemSendBuffer>(
                    std::move(it), value, getBucket()));
        } else {
            copyToOutputStream(
                    {reinterpret_cast<const char*>(&req), sizeof(req)},
                    sidbuffer,
                    extras.getBuffer(),
                    key.getBuffer(),
                    value);
        }
    } catch (const std::bad_alloc&) {
        /// We might have written a partial message into the buffer so
        /// we need to disconnect the client
        return cb::engine_errc::disconnect;
    }

    getBucket().recordMeteringReadBytes(*this, doc_read_bytes);
    return cb::engine_errc::success;
}

cb::engine_errc Connection::deletionInner(const ItemIface& item,
                                          cb::const_byte_buffer packet,
                                          const DocKey& key) {
    try {
        copyToOutputStream(
                {reinterpret_cast<const char*>(packet.data()), packet.size()},
                key.getBuffer(),
                item.getValueView());
    } catch (const std::bad_alloc&) {
        // We might have written a partial message into the buffer so
        // we need to disconnect the client
        return cb::engine_errc::disconnect;
    }

    return cb::engine_errc::success;
}

cb::engine_errc Connection::deletion(uint32_t opaque,
                                     cb::unique_item_ptr it,
                                     Vbid vbucket,
                                     uint64_t by_seqno,
                                     uint64_t rev_seqno,
                                     cb::mcbp::DcpStreamId sid) {
    auto key = it->getDocKey();
    const auto doc_read_bytes = key.size() + it->getValueView().size();

    if (!isCollectionsSupported()) {
        key = key.makeDocKeyWithoutCollectionID();
    }
    auto value = it->getValueView();

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
        builder.setCas(it->getCas());
        builder.setDatatype(cb::mcbp::Datatype(it->getDataType()));

        const auto ret =
                add_packet_to_send_pipe(builder.getFrame()->getFrame());
        if (ret == cb::engine_errc::success) {
            getBucket().recordMeteringReadBytes(*this, doc_read_bytes);
        }
        return ret;
    }

    using cb::mcbp::Request;
    using cb::mcbp::request::DcpDeletionV1Payload;
    std::array<uint8_t,
               sizeof(Request) + sizeof(DcpDeletionV1Payload) +
                       sizeof(cb::mcbp::DcpStreamIdFrameInfo)>
            blob;
    auto& req = *reinterpret_cast<Request*>(blob.data());
    req.setMagic(sid ? cb::mcbp::Magic::AltClientRequest
                     : cb::mcbp::Magic::ClientRequest);
    req.setOpcode(cb::mcbp::ClientOpcode::DcpDeletion);
    req.setExtlen(gsl::narrow<uint8_t>(sizeof(DcpDeletionV1Payload)));
    req.setKeylen(gsl::narrow<uint16_t>(key.size()));
    req.setBodylen(gsl::narrow<uint32_t>(
            sizeof(DcpDeletionV1Payload) + key.size() + value.size() +
            (sid ? sizeof(cb::mcbp::DcpStreamIdFrameInfo) : 0)));
    req.setOpaque(opaque);
    req.setVBucket(vbucket);
    req.setCas(it->getCas());
    req.setDatatype(cb::mcbp::Datatype(it->getDataType()));

    auto* ptr = blob.data() + sizeof(Request);
    if (sid) {
        auto buf = frameInfo.getBuf();
        std::copy(buf.begin(), buf.end(), ptr);
        ptr += buf.size();
        req.setFramingExtraslen(buf.size());
    }

    std::copy(extdata.getBuffer().begin(), extdata.getBuffer().end(), ptr);
    cb::const_byte_buffer packetBuffer{
            blob.data(),
            sizeof(Request) + sizeof(DcpDeletionV1Payload) +
                    (sid ? sizeof(cb::mcbp::DcpStreamIdFrameInfo) : 0)};

    const auto ret = deletionInner(*it, packetBuffer, key);
    if (ret == cb::engine_errc::success) {
        getBucket().recordMeteringReadBytes(*this, doc_read_bytes);
    }
    return ret;
}

cb::engine_errc Connection::deletion_v2(uint32_t opaque,
                                        cb::unique_item_ptr it,
                                        Vbid vbucket,
                                        uint64_t by_seqno,
                                        uint64_t rev_seqno,
                                        uint32_t delete_time,
                                        cb::mcbp::DcpStreamId sid) {
    auto key = it->getDocKey();
    const auto doc_read_bytes = key.size() + it->getValueView().size();

    if (!isCollectionsSupported()) {
        key = key.makeDocKeyWithoutCollectionID();
    }

    cb::mcbp::request::DcpDeletionV2Payload extras(
            by_seqno, rev_seqno, delete_time);
    cb::mcbp::DcpStreamIdFrameInfo frameInfo(sid);
    auto value = it->getValueView();

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
        builder.setCas(it->getCas());
        builder.setDatatype(cb::mcbp::Datatype(it->getDataType()));
        const auto ret =
                add_packet_to_send_pipe(builder.getFrame()->getFrame());
        if (ret == cb::engine_errc::success) {
            getBucket().recordMeteringReadBytes(*this, doc_read_bytes);
        }
        return ret;
    }

    // Make blob big enough for either delete or expiry
    std::array<uint8_t,
               sizeof(cb::mcbp::Request) + sizeof(extras) + sizeof(frameInfo)>
            blob = {};
    const size_t payloadLen = sizeof(extras);
    const size_t frameInfoLen = sid ? sizeof(frameInfo) : 0;

    auto& req = *reinterpret_cast<cb::mcbp::Request*>(blob.data());
    req.setMagic(sid ? cb::mcbp::Magic::AltClientRequest
                     : cb::mcbp::Magic::ClientRequest);

    req.setOpcode(cb::mcbp::ClientOpcode::DcpDeletion);
    req.setExtlen(gsl::narrow<uint8_t>(payloadLen));
    req.setKeylen(gsl::narrow<uint16_t>(key.size()));
    req.setBodylen(gsl::narrow<uint32_t>(payloadLen +
                                         gsl::narrow<uint16_t>(key.size()) +
                                         value.size() + frameInfoLen));
    req.setOpaque(opaque);
    req.setVBucket(vbucket);
    req.setCas(it->getCas());
    req.setDatatype(cb::mcbp::Datatype(it->getDataType()));
    auto size = sizeof(cb::mcbp::Request);
    auto* ptr = blob.data() + size;
    if (sid) {
        auto buf = frameInfo.getBuf();
        std::copy(buf.begin(), buf.end(), ptr);
        ptr += buf.size();
        size += buf.size();
    }

    auto buffer = extras.getBuffer();
    std::copy(buffer.begin(), buffer.end(), ptr);
    size += buffer.size();

    const auto ret = deletionInner(*it, {blob.data(), size}, key);
    if (ret == cb::engine_errc::success) {
        getBucket().recordMeteringReadBytes(*this, doc_read_bytes);
    }
    return ret;
}

cb::engine_errc Connection::expiration(uint32_t opaque,
                                       cb::unique_item_ptr it,
                                       Vbid vbucket,
                                       uint64_t by_seqno,
                                       uint64_t rev_seqno,
                                       uint32_t delete_time,
                                       cb::mcbp::DcpStreamId sid) {
    auto key = it->getDocKey();
    const auto doc_read_bytes = key.size() + it->getValueView().size();

    if (!isCollectionsSupported()) {
        key = key.makeDocKeyWithoutCollectionID();
    }

    cb::mcbp::request::DcpExpirationPayload extras(
            by_seqno, rev_seqno, delete_time);
    cb::mcbp::DcpStreamIdFrameInfo frameInfo(sid);
    auto value = it->getValueView();

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
        builder.setCas(it->getCas());
        builder.setDatatype(cb::mcbp::Datatype(it->getDataType()));
        const auto ret =
                add_packet_to_send_pipe(builder.getFrame()->getFrame());
        if (ret == cb::engine_errc::success) {
            getBucket().recordMeteringReadBytes(*this, doc_read_bytes);
        }
        return ret;
    }

    // Make blob big enough for either delete or expiry
    std::array<uint8_t,
               sizeof(cb::mcbp::Request) + sizeof(extras) + sizeof(frameInfo)>
            blob = {};
    const size_t payloadLen = sizeof(extras);
    const size_t frameInfoLen = sid ? sizeof(frameInfo) : 0;

    auto& req = *reinterpret_cast<cb::mcbp::Request*>(blob.data());
    req.setMagic(sid ? cb::mcbp::Magic::AltClientRequest
                     : cb::mcbp::Magic::ClientRequest);

    req.setOpcode(cb::mcbp::ClientOpcode::DcpExpiration);
    req.setExtlen(gsl::narrow<uint8_t>(payloadLen));
    req.setKeylen(gsl::narrow<uint16_t>(key.size()));
    req.setBodylen(gsl::narrow<uint32_t>(payloadLen +
                                         gsl::narrow<uint16_t>(key.size()) +
                                         value.size() + frameInfoLen));
    req.setOpaque(opaque);
    req.setVBucket(vbucket);
    req.setCas(it->getCas());
    req.setDatatype(cb::mcbp::Datatype(it->getDataType()));
    auto size = sizeof(cb::mcbp::Request);
    auto* ptr = blob.data() + size;
    if (sid) {
        auto buf = frameInfo.getBuf();
        std::copy(buf.begin(), buf.end(), ptr);
        ptr += buf.size();
        size += buf.size();
    }

    auto buffer = extras.getBuffer();
    std::copy(buffer.begin(), buffer.end(), ptr);
    size += buffer.size();

    const auto ret = deletionInner(*it, {blob.data(), size}, key);
    if (ret == cb::engine_errc::success) {
        getBucket().recordMeteringReadBytes(*this, doc_read_bytes);
    }
    return ret;
}

cb::engine_errc Connection::set_vbucket_state(uint32_t opaque,
                                              Vbid vbucket,
                                              vbucket_state_t st) {
    if (!is_valid_vbucket_state_t(st)) {
        return cb::engine_errc::invalid_arguments;
    }

    cb::mcbp::request::DcpSetVBucketState extras;
    extras.setState(static_cast<uint8_t>(st));
    cb::mcbp::RequestBuilder builder(thread.getScratchBuffer());
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::DcpSetVbucketState);
    builder.setOpaque(opaque);
    builder.setVBucket(vbucket);
    builder.setExtras(extras.getBuffer());

    return add_packet_to_send_pipe(builder.getFrame()->getFrame());
}

cb::engine_errc Connection::noop(uint32_t opaque) {
    cb::mcbp::RequestBuilder builder(thread.getScratchBuffer());
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::DcpNoop);
    builder.setOpaque(opaque);

    return add_packet_to_send_pipe(builder.getFrame()->getFrame());
}

cb::engine_errc Connection::buffer_acknowledgement(uint32_t opaque,
                                                   Vbid vbucket,
                                                   uint32_t buffer_bytes) {
    cb::mcbp::request::DcpBufferAckPayload extras;
    extras.setBufferBytes(buffer_bytes);
    cb::mcbp::RequestBuilder builder(thread.getScratchBuffer());
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::DcpBufferAcknowledgement);
    builder.setOpaque(opaque);
    builder.setVBucket(vbucket);
    builder.setExtras(extras.getBuffer());

    return add_packet_to_send_pipe(builder.getFrame()->getFrame());
}

cb::engine_errc Connection::control(uint32_t opaque,
                                    std::string_view key,
                                    std::string_view value) {
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

cb::engine_errc Connection::system_event(uint32_t opaque,
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

cb::engine_errc Connection::get_error_map(uint32_t opaque, uint16_t version) {
    cb::mcbp::request::GetErrmapPayload body;
    body.setVersion(version);
    cb::mcbp::RequestBuilder builder(thread.getScratchBuffer());
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::GetErrorMap);
    builder.setOpaque(opaque);
    builder.setValue(body.getBuffer());

    return add_packet_to_send_pipe(builder.getFrame()->getFrame());
}

cb::engine_errc Connection::prepare(uint32_t opaque,
                                    cb::unique_item_ptr it,
                                    Vbid vbucket,
                                    uint64_t by_seqno,
                                    uint64_t rev_seqno,
                                    uint32_t lock_time,
                                    uint8_t nru,
                                    DocumentState document_state,
                                    cb::durability::Level level) {
    auto buffer = it->getValueView();

    auto key = it->getDocKey();

    // The client doesn't support collections, so must not send an encoded key
    if (!isCollectionsSupported()) {
        key = key.makeDocKeyWithoutCollectionID();
    }

    cb::mcbp::request::DcpPreparePayload extras(
            by_seqno,
            rev_seqno,
            it->getFlags(),
            gsl::narrow<uint32_t>(it->getExptime()),
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
        builder.setCas(it->getCas());
        builder.setDatatype(cb::mcbp::Datatype(it->getDataType()));
        builder.setValue(buffer);
        return add_packet_to_send_pipe(builder.getFrame()->getFrame());
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
    req.setCas(it->getCas());
    req.setDatatype(cb::mcbp::Datatype(it->getDataType()));

    try {
        if (buffer.size() > SendBuffer::MinimumDataSize) {
            copyToOutputStream(
                    {reinterpret_cast<const char*>(&req), sizeof(req)},
                    {reinterpret_cast<const char*>(&extras), sizeof(extras)},
                    key.getBuffer());
            auto sendbuffer = std::make_unique<ItemSendBuffer>(
                    std::move(it), buffer, getBucket());
            chainDataToOutputStream(std::move(sendbuffer));
        } else {
            copyToOutputStream(
                    {reinterpret_cast<const char*>(&req), sizeof(req)},
                    {reinterpret_cast<const char*>(&extras), sizeof(extras)},
                    key.getBuffer(),
                    buffer);
        }
    } catch (const std::bad_alloc&) {
        /// We might have written a partial message into the buffer so
        /// we need to disconnect the client
        return cb::engine_errc::disconnect;
    }

    return cb::engine_errc::success;
}

cb::engine_errc Connection::seqno_acknowledged(uint32_t opaque,
                                               Vbid vbucket,
                                               uint64_t prepared_seqno) {
    cb::mcbp::request::DcpSeqnoAcknowledgedPayload extras(prepared_seqno);
    cb::mcbp::RequestBuilder builder(thread.getScratchBuffer());
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::DcpSeqnoAcknowledged);
    builder.setOpaque(opaque);
    builder.setVBucket(vbucket);
    builder.setExtras(extras.getBuffer());
    return add_packet_to_send_pipe(builder.getFrame()->getFrame());
}

cb::engine_errc Connection::commit(uint32_t opaque,
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
    builder.setKey(std::string_view(key));
    return add_packet_to_send_pipe(builder.getFrame()->getFrame());
}

cb::engine_errc Connection::abort(uint32_t opaque,
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
    builder.setKey(std::string_view(key));
    return add_packet_to_send_pipe(builder.getFrame()->getFrame());
}

cb::engine_errc Connection::oso_snapshot(uint32_t opaque,
                                         Vbid vbucket,
                                         uint32_t flags,
                                         cb::mcbp::DcpStreamId sid) {
    cb::mcbp::request::DcpOsoSnapshotPayload extras(flags);
    const size_t totalBytes = sizeof(cb::mcbp::Request) + sizeof(extras) +
                              sizeof(cb::mcbp::DcpStreamIdFrameInfo);
    std::vector<uint8_t> buffer(totalBytes);
    cb::mcbp::RequestBuilder builder({buffer.data(), buffer.size()});
    builder.setMagic(sid ? cb::mcbp::Magic::AltClientRequest
                         : cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::DcpOsoSnapshot);
    builder.setOpaque(opaque);
    builder.setVBucket(vbucket);
    builder.setExtras(extras.getBuffer());
    if (sid) {
        cb::mcbp::DcpStreamIdFrameInfo framedSid(sid);
        builder.setFramingExtras(framedSid.getBuf());
    }
    return add_packet_to_send_pipe(builder.getFrame()->getFrame());
}

cb::engine_errc Connection::seqno_advanced(uint32_t opaque,
                                           Vbid vbucket,
                                           uint64_t seqno,
                                           cb::mcbp::DcpStreamId sid) {
    cb::mcbp::request::DcpSeqnoAdvancedPayload extras(seqno);
    const size_t totalBytes = sizeof(cb::mcbp::Request) + sizeof(extras) +
                              sizeof(cb::mcbp::DcpStreamIdFrameInfo);
    std::vector<uint8_t> buffer(totalBytes);
    cb::mcbp::RequestBuilder builder({buffer.data(), buffer.size()});
    builder.setMagic(sid ? cb::mcbp::Magic::AltClientRequest
                         : cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::DcpSeqnoAdvanced);
    builder.setOpaque(opaque);
    builder.setVbucket(vbucket);
    builder.setExtras(extras.getBuffer());
    if (sid) {
        cb::mcbp::DcpStreamIdFrameInfo frameSid(sid);
        builder.setFramingExtras(frameSid.getBuf());
    }

    return add_packet_to_send_pipe(builder.getFrame()->getFrame());
}
////////////////////////////////////////////////////////////////////////////
//                                                                        //
//               End DCP Message producer interface                       //
//                                                                        //
////////////////////////////////////////////////////////////////////////////
