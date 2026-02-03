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
#include "connection_libevent.h"
#include "cookie.h"
#include "external_auth_manager_thread.h"
#include "front_end_thread.h"
#include "listening_port.h"
#include "mc_time.h"
#include "mcaudit.h"
#include "memcached.h"
#include "network_interface_manager.h"
#include "sdk_connection_manager.h"
#include "sendbuffer.h"
#include "settings.h"
#include "ssl_utils.h"
#include "tracing.h"
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
#include <platform/exceptions.h>
#include <platform/json_log_conversions.h>
#include <platform/scope_timer.h>
#include <platform/socket.h>
#include <platform/strerror.h>
#include <platform/string_hex.h>
#include <platform/timeutils.h>
#include <serverless/config.h>
#include <utilities/debug_variable.h>
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
    // Shutdown may be called in various contexts but the legal
    // state transitions are:
    //   running -> closing
    //   closing -> pending_close
    //   closing -> immediate_close
    //   pending_close -> immediate_close
    if (state == State::running) {
        ++stats.curr_conn_closing;
        shutdown_initiated = std::chrono::steady_clock::now();
        pending_close_next_log = *shutdown_initiated + std::chrono::seconds(10);
        thread.onInitiateShutdown(*this);
        state = State::closing;
    }
}

bool Connection::isConnectedToSystemPort() const {
    return listening_port && listening_port->system;
}

nlohmann::json Connection::to_json() const {
    nlohmann::json ret = to_json_tcp();

    ret["socket"] = socketDescriptor;
    ret["peername"] = peername;
    ret["sockname"] = sockname;

    ret["connection"] = cb::to_hex(uint64_t(this));

    ret["yields"] = yields.load();
    ret["protocol"] = "memcached";
    ret["bucket_index"] = getBucketIndex();
    ret["internal"] = isInternal();

    if (isAuthenticated()) {
        const auto& ui = getUser();
        ret["user"]["name"] = ui.getSanitizedName();
        ret["user"]["domain"] = ui.domain;
    }

    ret["refcount"] = refcount;

    nlohmann::json features = nlohmann::json::array();
    if (isSupportsMutationExtras()) {
        features.push_back("mutation extras");
    }
    if (isXerrorSupport()) {
        features.push_back("xerror");
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

    switch (cccp) {
    case ClustermapChangeNotification::None:
        break;
    case ClustermapChangeNotification::Brief:
        features.push_back("ClustermapChangeNotificationBrief");
        break;
    case ClustermapChangeNotification::Full:
        features.push_back("ClustermapChangeNotification");
        break;
    }

    if (isNonBlockingThrottlingMode()) {
        features.push_back("NonBlockingThrottlingMode");
    }

    if (dedupe_nmvb_maps) {
        features.push_back("DedupeNotMyVbucketClustermap");
    }

    if (snappy_everywhere) {
        features.push_back("SnappyEverywhere");
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
        if (c && (!c->empty() || isDCP())) {
            arr.push_back(c->to_json());
        }
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

    ret["ssl"] = isTlsEnabled();
    ret["datatype"] = cb::mcbp::datatype::to_string(datatypeFilter.getRaw());
    ret["last_used"] =
            cb::time2text(std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now() - last_used_timestamp));
    return ret;
}

nlohmann::json Connection::to_json_tcp() const {
    nlohmann::json ret;

    ret["total_recv"] = totalRecv;
    ret["total_send"] = totalSend;

    ret["sendqueue"]["actual"] = getSendQueueSize();
    ret["sendqueue"]["size"] = sendQueueInfo.size;
    ret["sendqueue"]["last"] = sendQueueInfo.last.time_since_epoch().count();
    ret["sendqueue"]["term"] = sendQueueInfo.term;

    ret["blocked_send_queue_duration"] =
            cb::time2text(blockedOnFullSendQueueDuration);
    ret["socket_options"] = cb::net::getSocketOptions(socketDescriptor);

#ifdef __linux__
    int value;
    if (ioctl(socketDescriptor, SIOCINQ, &value) == 0) {
        ret["SIOCINQ"] = value;
    }
    if (ioctl(socketDescriptor, SIOCOUTQ, &value) == 0) {
        ret["SIOCOUTQ"] = value;
    }
#endif

    return ret;
}

void Connection::restartAuthentication() {
    if (getUser().domain == cb::sasl::Domain::External) {
        externalAuthManager->logoff(getUser().name);
    }
    user.reset();
    updateDescription();
    privilegeContext = std::make_shared<cb::rbac::PrivilegeContext>(
            cb::rbac::PrivilegeContext{getUser().domain});
    updatePrivilegeContext();
}

void Connection::updatePrivilegeContext() {
    for (std::size_t ii = 0; ii < droppedPrivileges.size(); ++ii) {
        if (droppedPrivileges.test(ii)) {
            privilegeContext->dropPrivilege(cb::rbac::Privilege(ii));
        }
    }
    subject_to_metering.store(
            privilegeContext->check(cb::rbac::Privilege::Unmetered, {}, {})
                    .failed(),
            std::memory_order_release);
    subject_to_throttling.store(
            privilegeContext->check(cb::rbac::Privilege::Unthrottled, {}, {})
                    .failed(),
            std::memory_order_release);
    node_supervisor.store(
            privilegeContext->check(cb::rbac::Privilege::NodeSupervisor, {}, {})
                    .success(),
            std::memory_order_release);
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

std::shared_ptr<cb::rbac::PrivilegeContext> Connection::getPrivilegeContext() {
    if (!privilegeContext || privilegeContext->isStale()) {
        if (!isAuthenticated()) {
            // We're not authenticated, so there isn't any point of trying
            // to make this more complex than it is...
            privilegeContext = std::make_shared<cb::rbac::PrivilegeContext>(
                    cb::rbac::PrivilegeContext{getUser().domain});

            if (bucketIndex.load() == 0) {
                // If we're connected to the no bucket we should return
                // no bucket instead of EACCESS. Lets give the connection all
                // possible bucket privileges
                privilegeContext->setBucketPrivileges();
            }
            return privilegeContext;
        }

        try {
            privilegeContext = std::make_shared<cb::rbac::PrivilegeContext>(
                    createContext(getBucket().name));
        } catch (const cb::rbac::NoSuchBucketException&) {
            // Remove all access to the bucket
            privilegeContext = std::make_shared<cb::rbac::PrivilegeContext>(
                    createContext({}));
            LOG_INFO_CTX("RBAC: No access to bucket",
                         {"conn_id", getId()},
                         {"description", getDescription()},
                         {"bucket", getBucket().name},
                         {"privileges", privilegeContext->to_string()});
        } catch (const cb::rbac::NoSuchUserException&) {
            // Remove all access to the bucket
            privilegeContext = std::make_shared<cb::rbac::PrivilegeContext>(
                    cb::rbac::PrivilegeContext{getUser().domain});
            if (isAuthenticated()) {
                LOG_INFO_CTX("RBAC: No RBAC definition for the user",
                             {"conn_id", getId()},
                             {"description", getDescription()});
            }
        }

        if (bucketIndex.load() == 0) {
            // If we're connected to the no bucket we should return
            // no bucket instead of EACCESS. Lets give the connection all
            // possible bucket privileges
            privilegeContext->setBucketPrivileges();
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

    switch (code) {
    case cb::engine_errc::success: // FALLTHROUGH
    case cb::engine_errc::no_such_key: // FALLTHROUGH
    case cb::engine_errc::key_already_exists: // FALLTHROUGH
    case cb::engine_errc::no_memory: // FALLTHROUGH
    case cb::engine_errc::not_stored: // FALLTHROUGH
    case cb::engine_errc::invalid_arguments: // FALLTHROUGH
    case cb::engine_errc::not_supported: // FALLTHROUGH
    case cb::engine_errc::too_much_data_in_output_buffer: // FALLTHROUGH
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
    case cb::engine_errc::bucket_paused:
    case cb::engine_errc::encryption_key_not_available:
        return code;

    case cb::engine_errc::too_many_connections:
        return cb::engine_errc::too_big;

    case cb::engine_errc::locked:
    case cb::engine_errc::cas_value_invalid:
        return cb::engine_errc::key_already_exists;
    case cb::engine_errc::locked_tmpfail:
    case cb::engine_errc::not_locked:
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
    case cb::engine_errc::vbuuid_not_equal:
    case cb::engine_errc::cancelled:
    case cb::engine_errc::checksum_mismatch:
        break;
    }

    // Seems like the rest of the components in our system isn't
    // prepared to receive access denied or authentincation stale.
    // For now we should just disconnect them
    auto errc = cb::make_error_condition(code);
    LOG_WARNING_CTX("Disconnecting client not aware of extended error code",
                    {"conn_id", getId()},
                    {"description", getDescription()},
                    {"error", errc.message()});
    setTerminationReason("XError not enabled on client");

    return cb::engine_errc::disconnect;
}

void Connection::updateDescription() {
    description = {
            {"peer", peername},
            {"socket", sockname},
    };
    if (isAuthenticated()) {
        nlohmann::json user{{"name", getUser().getSanitizedName()}};
        if (isInternal()) {
            user["system"] = true;
        }
        if (getUser().domain == cb::sasl::Domain::External) {
            user["ldap"] = true;
        }
        description["user"] = std::move(user);
    } else {
        description["user"] = nullptr;
    }
}

void Connection::setBucketIndex(int index, Cookie* cookie) {
    bucketIndex.store(index, std::memory_order_release);

    using cb::tracing::Code;
    using cb::tracing::SpanStopwatch;
    ScopeTimer1<SpanStopwatch<cb::tracing::Code>> timer(
            cookie, Code::UpdatePrivilegeContext);

    // Update the privilege context. If a problem occurs within the RBAC
    // module we'll assign an empty privilege context to the connection.
    try {
        if (isAuthenticated()) {
            // The user have logged in, so we should create a context
            // representing the users context in the desired bucket.
            privilegeContext = std::make_shared<cb::rbac::PrivilegeContext>(
                    createContext(all_buckets[index].name));
        } else {
            // The user has not authenticated. Assign an empty profile which
            // won't give you any privileges.
            privilegeContext = std::make_shared<cb::rbac::PrivilegeContext>(
                    cb::rbac::PrivilegeContext{getUser().domain});
        }
    } catch (const cb::rbac::Exception&) {
        privilegeContext = std::make_shared<cb::rbac::PrivilegeContext>(
                cb::rbac::PrivilegeContext{getUser().domain});
    }

    if (index == 0) {
        // If we're connected to the no bucket we should return
        // no bucket instead of EACCESS. Lets give the connection all
        // possible bucket privileges
        privilegeContext->setBucketPrivileges();
    }
    updatePrivilegeContext();
}

void Connection::addCpuTime(std::chrono::nanoseconds ns) {
    total_cpu_time += ns;
    min_sched_time = std::min(min_sched_time, ns);
    max_sched_time = std::max(max_sched_time, ns);
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
    const auto limit = is_memcached_shutting_down() ? std::chrono::seconds(0)
                       : (getBucket().state == Bucket::State::Ready)
                               ? std::chrono::seconds(360)
                               : std::chrono::seconds(1);
    if ((now - sendQueueInfo.last) > limit) {
        LOG_WARNING_CTX(
                "Send buffer stuck for too long. Shutting "
                "down connection",
                {"conn_id", getId()},
                {"send_queue_size", sendQueueInfo.size},
                {"duration", (now - sendQueueInfo.last)},
                {"description", getDescription()});

        // We've not had any progress on the socket for "n" secs
        // Forcibly shut down the connection!
        cb::net::shutdown(socketDescriptor, SHUT_WR);
        sendQueueInfo.term = true;
        setTerminationReason("Failed to send data to client");
        shutdown();
    }
}

bool Connection::reEvaluateThrottledCookies() {
    bool throttled = false;
    for (auto& c : cookies) {
        if (c->isThrottled()) {
            if (getBucket().shouldThrottle(*c, false, 0)) {
                throttled = true;
            } else {
                c->setThrottled(false);
                c->notifyIoComplete(cb::engine_errc::success);
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
    while (iter != cookies.end() &&
           std::chrono::steady_clock::now() < current_timeslice_end) {
        auto& cookie = *iter;

        if (cookie->empty()) {
            ++iter;
            continue;
        }

        if (cookie->isEwouldblock() ||
            (cookie->isOutputbufferFull() && havePendingData())) {
            // This cookie is waiting for an engine notification or
            // that the network buffer needs to drain
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

static bool isReAuthenticate(const cb::mcbp::Header& header) {
    if (!header.isRequest()) {
        return false;
    }
    const auto& req = header.getRequest();
    if (!cb::mcbp::is_client_magic(req.getMagic())) {
        return false;
    }

    const auto opcode = req.getClientOpcode();
    return opcode == cb::mcbp::ClientOpcode::SaslAuth ||
           opcode == cb::mcbp::ClientOpcode::SaslStep;
}

inline bool Connection::handleThrottleCommand(Cookie& cookie) const {
    if (isNonBlockingThrottlingMode()) {
        using namespace std::chrono;
        const auto now = steady_clock::now();
        const auto sec = duration_cast<seconds>(now.time_since_epoch());
        const auto usec =
                duration_cast<microseconds>(now.time_since_epoch()) - sec;
        const auto delta = duration_cast<microseconds>(seconds{1}) - usec;
        cookie.setErrorJsonExtras(
                nlohmann::json{{"next_tick_us", delta.count()}});
        cookie.sendResponse(cb::mcbp::Status::EWouldThrottle);
        cookie.reset();
        return false;
    }
    cookie.setThrottled(true);
    // Set the cookie to true to block the destruction
    // of the command (and the connection)
    cookie.setEwouldblock();
    cookie.preserveRequest();
    // Given that we're blocked on throttling, we should
    // stop accepting new commands and wait for throttling
    // to unblock the execution pipeline
    return true;
}

inline void Connection::handleRejectCommand(Cookie& cookie,
                                            const cb::mcbp::Status status,
                                            const bool auth_stale) {
    cookie.getConnection().getBucket().rejectCommand(cookie);
    if (status == cb::mcbp::Status::Success && auth_stale) {
        // The packet was correctly encoded, but the
        // authentication expired
        cookie.sendResponse(cb::mcbp::Status::AuthStale);
    } else {
        // Packet validation failed
        cookie.sendResponse(status);
    }
    cookie.reset();
}

void Connection::executeCommandPipeline() {
    const auto maxSendQueueSize = Settings::instance().getMaxSendQueueSize();
    const auto tooMuchData = [this, maxSendQueueSize]() {
        return getSendQueueSize() >= maxSendQueueSize;
    };

    using cb::mcbp::Status;
    numEvents = max_reqs_per_event;
    const auto maxActiveCommands =
            Settings::instance().getMaxConcurrentCommandsPerConnection();

    bool active = processAllReadyCookies();

    // We might add more commands to the queue
    if (is_bucket_dying(*this)) {
        // we need to shut down the bucket
        return;
    }

    std::chrono::steady_clock::time_point now;
    bool allow_more_commands = (!active || cookies.back()->mayReorder()) &&
                               cookies.size() < maxActiveCommands;
    if (allow_more_commands) {
        // Only look at new commands if we don't have any active commands
        // or the active command allows for reordering.

        bool stop = tooMuchData();
        while ((now = std::chrono::steady_clock::now()) <
                       current_timeslice_end &&
               !stop && cookies.size() < maxActiveCommands &&
               isPacketAvailable() && numEvents > 0 &&
               state == State::running) {
            const auto auth_stale = authContextLifetime.isStale(now);
            if (!cookies.back()->empty()) {
                // Create a new entry if we can't reuse the last entry
                cookies.emplace_back(std::make_unique<Cookie>(*this));
            }

            auto& cookie = *cookies.back();
            // We always want to collect trace information if we're running
            // for a serverless configuration
            cookie.initialize(now, getPacket());
            updateRecvBytes(cookie.getPacket().size());

            const auto status = cookie.validate();
            if (status == Status::Success &&
                (!auth_stale || isReAuthenticate(cookie.getHeader()))) {
                // We may only start execute the packet if:
                //  * We shouldn't be throttled
                //  * We don't have any ongoing commands
                //  * We have an ongoing command and this command allows
                //    for reorder
                if (getBucket().shouldThrottle(cookie, true, 0)) {
                    stop = handleThrottleCommand(cookie) || tooMuchData();
                } else if ((!active || cookie.mayReorder()) &&
                           cookie.execute(true)) {
                    // Command executed successfully, reset the cookie to
                    // allow it to be reused
                    cookie.reset();
                    // Check that we're not reserving too much memory for
                    // this client...
                    stop = tooMuchData();
                } else {
                    active = true;
                    // We need to block, so we need to preserve the request
                    // as we'll drain the data from the buffer.
                    cookie.preserveRequest();
                    if (!cookie.mayReorder()) {
                        // Don't add commands as we need the last one to
                        // complete
                        stop = true;
                    }
                }
            } else {
                handleRejectCommand(cookie, status, auth_stale);
            }

            nextPacket();
            --numEvents;
        }
        allow_more_commands = (!active || cookies.back()->mayReorder()) &&
                              cookies.size() < maxActiveCommands;
    } else {
        now = std::chrono::steady_clock::now();
    }

    if (numEvents == 0) {
        // We've used all the iterations we're allowed to do for each
        // time we're scheduled to run (max_reqs_per_event)
        yields++;
        get_thread_stats(this)->conn_yields++;
    } else if (now > current_timeslice_end) {
        // We've used more time than we're allowed to do for each
        // time we're scheduled to run
        get_thread_stats(this)->conn_timeslice_yields++;
    }

    if (tooMuchData()) {
        // The client have too much data spooled. Disable read event
        // to avoid the client from getting rescheduled just to back
        // out again due to too much data in the queue to start executing
        // a new command
        disableReadEvent();
    } else if (allow_more_commands) {
        if (isPacketAvailable()) {
            // We have the entire packet spooled up in the input buffer
            // and may continue processing it at the next time slice
            disableReadEvent();
            triggerCallback();
        } else {
            enableReadEvent();
        }
    } else {
        enableReadEvent();
    }
}

void Connection::resumeThrottledDcpStream() {
    dcpStreamThrottled = false;
}

void Connection::tryToProgressDcpStream() {
    Expects(!cookies.empty() &&
            "Connection::tryToProgressDcpStream(): no cookies available!");

    if (dcpStreamThrottled || !cookies.front()->empty()) {
        // Currently throttled or working on an incomming packet
        return;
    }

    // Reset the privilegeContext in the cookie (this will also ensure
    // that we update the privilege context in the connection to the
    // current revision)
    cookies.front()->reset();

    // Verify that we still have access to DCP
    const auto required_privilege = type == Type::Consumer
                                            ? cb::rbac::Privilege::DcpConsumer
                                            : cb::rbac::Privilege::DcpProducer;

    if (privilegeContext
                ->checkForPrivilegeAtLeastInOneCollection(required_privilege)
                .failed()) {
        setTerminationReason(fmt::format("{} privilege no longer available",
                                         required_privilege));
        LOG_WARNING_CTX("Shutting down connection due to lost privilege",
                        {"conn_id", getId()},
                        {"description", getDescription()},
                        {"priviledge", required_privilege});
        shutdown();
        return;
    }

    std::size_t dcpMaxQSize = Settings::instance().getMaxSendQueueSize();
    bool more = (getSendQueueSize() < dcpMaxQSize);
    if (type == Type::Consumer) {
        // We want the consumer to perform some steps because
        // it could be pending bufferAcks
        numEvents = max_reqs_per_event;
    }
    bool exceededTimeslice = false;
    while (more && numEvents > 0) {
        exceededTimeslice =
                std::chrono::steady_clock::now() >= current_timeslice_end;
        if (exceededTimeslice) {
            break;
        }
        const auto [throttle, allocDomain] =
                getBucket().shouldThrottleDcp(*this);
        dcpResourceAllocationDomain = allocDomain;

        const auto ret = getBucket().getDcpIface()->step(
                *cookies.front().get(), throttle, *this);
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
            LOG_WARNING_CTX("step returned - closing connection",
                            {"conn_id", getId()},
                            {"status", cb::to_string(ret)},
                            {"description", getDescription()});
            if (ret == cb::engine_errc::disconnect) {
                setTerminationReason("Engine forced disconnect");
            }
            shutdown();
            more = false;
        }
    }
    // There is no need to try to trigger a callback as we should
    // have data in the output queue if we have more data to send (otherwise
    // we would have tried to put it in the output queue).
}

void Connection::processBlockedSendQueue(
        const std::chrono::steady_clock::time_point& now) {
    if (blockedOnFullSendQueue.has_value()) {
        blockedOnFullSendQueueDuration +=
                (now - blockedOnFullSendQueue.value());
        blockedOnFullSendQueue.reset();
    }
}

void Connection::updateBlockedSendQueue(
        const std::chrono::steady_clock::time_point& now) {
    const std::size_t maxQSize = Settings::instance().getMaxSendQueueSize();

    // We may have copied data to the stream
    if (getSendQueueSize() >= maxQSize) {
        blockedOnFullSendQueue = now;
    }
}

void Connection::processNotifiedCookie(
        Cookie& cookie,
        cb::engine_errc status,
        std::chrono::steady_clock::time_point scheduled) {
    using namespace std::chrono;
    const auto now = steady_clock::now();
    cookie_notification_histogram[thread.index].add(
            duration_cast<microseconds>(now - scheduled));
    Expects(cookie.isEwouldblock());
    cookie.setAiostat(status);
    cookie.clearEwouldblock();
    triggerCallback();
    cookie.getTracer().record(cb::tracing::Code::Notified, scheduled, now);
}

void Connection::commandExecuted(Cookie& cookie) {
    getBucket().commandExecuted(cookie);
}

static cb::logger::Json createBacktraceJson(
        const boost::stacktrace::stacktrace& frames) {
    auto backtrace = cb::logger::Json::array();
    print_backtrace_frames(frames, [&backtrace](const char* frame) {
        backtrace.emplace_back(frame);
    });
    return backtrace;
}

void Connection::logExecutionException(const std::string_view where,
                                       const std::exception& e) {
    setTerminationReason(std::string("Received exception: ") + e.what());
    shutdown();

    try {
        auto array = nlohmann::json::array();
        for (const auto& c : cookies) {
            if (c && (!c->empty() || isDCP() || c->getRefcount() > 0)) {
                array.push_back(c->to_json());
            }
        }
        if (const auto* backtrace = cb::getBacktrace(e)) {
            auto callstack = createBacktraceJson(*backtrace);
            LOG_ERROR_CTX("Exception occurred. Closing connection",
                          {"conn_id", getId()},
                          {"details", where},
                          {"description", getDescription()},
                          {"error", e.what()},
                          {"cookies", std::move(array)},
                          {"backtrace", std::move(callstack)});
        } else {
            LOG_ERROR_CTX("Exception occurred. Closing connection",
                          {"conn_id", getId()},
                          {"details", where},
                          {"description", getDescription()},
                          {"error", e.what()},
                          {"cookies", std::move(array)});
        }
    } catch (const std::exception& exception2) {
        try {
            nlohmann::json callstack{nullptr};
            if (const auto* backtrace = cb::getBacktrace(e)) {
                callstack = createBacktraceJson(*backtrace);
            }
            LOG_ERROR_CTX(
                    "Second exception occurred. Closing "
                    "connection",
                    {"conn_id", getId()},
                    {"details", where},
                    {"description", getDescription()},
                    {"error", e.what()},
                    {"secondary_error", exception2.what()},
                    {"backtrace", std::move(callstack)});
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
    if (Settings::instance().isLocalhostInterfaceAllowed()) {
        using namespace std::string_view_literals;
        // Make sure we don't tear down localhost connections
        if (listening_port->family == AF_INET) {
            localhost = peername["ip"].get<std::string_view>() == "127.0.0.1"sv;
        } else {
            localhost = peername["ip"].get<std::string_view>() == "::1"sv;
        }
    }

    if (localhost) {
        LOG_INFO_CTX("Keeping connection alive even if server port was removed",
                     {"conn_id", getId()},
                     {"description", getDescription()});
    } else {
        LOG_INFO_CTX("Shutting down; server port was removed",
                     {"conn_id", getId()},
                     {"description", getDescription()});
        setTerminationReason("Server port shut down");
        shutdown();
        signalIfIdle();
    }
}

bool Connection::executeCommandsCallback() {
    using std::chrono::duration_cast;
    using std::chrono::microseconds;
    using std::chrono::nanoseconds;

    // Make sure any core dumps from this code contain the bucket name.
    cb::DebugVariable bucketName(cb::toCharArrayN<32>(getBucket().name));
    const auto start = last_used_timestamp = std::chrono::steady_clock::now();
    current_timeslice_end = start + Settings::instance().getCommandTimeSlice();

    processBlockedSendQueue(start);
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

    bool ret = true;
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

            --stats.curr_conn_closing;
            using namespace std::chrono;
            const auto now = steady_clock::now();
            // Only log if shutdown took more than 10 seconds
            if ((now - *shutdown_initiated) > seconds(10)) {
                LOG_INFO_CTX("Slow client disconnect",
                             {"conn_id", getId()},
                             {"duration", now - *shutdown_initiated});
            }

            // delete the object
            ret = false;
        }
    }

    // Update the scheduler information and the per-connection cpu usage
    const auto stop = std::chrono::steady_clock::now();
    const auto ns = duration_cast<nanoseconds>(stop - start);
    scheduler_info[getThread().index].add(duration_cast<microseconds>(ns));
    addCpuTime(ns);

    updateBlockedSendQueue(stop);
    return ret;
}

/*
 * Sets a socket's send buffer size to the maximum allowed by the system.
 */
static void maximize_sndbuf(const SOCKET sfd) {
    static cb::RelaxedAtomic<int> hint{0};

    if (hint != 0) {
        int value = hint;
        if (cb::net::setsockopt(
                    sfd, SOL_SOCKET, SO_SNDBUF, &value, sizeof(value)) == -1) {
            LOG_WARNING_CTX(
                    "Failed to set socket send buffer",
                    {"conn_id", sfd},
                    {"to", value},
                    {"error", cb_strerror(cb::net::get_socket_error())});
        }

        return;
    }

    int last_good = 0;
    int old_size;

    try {
        old_size = cb::net::getSocketOption<int>(sfd, SOL_SOCKET, SO_SNDBUF);
    } catch (const std::exception& e) {
        LOG_WARNING_CTX("Failed to get socket send buffer",
                        {"conn_id", sfd},
                        {"error", e.what()});
        return;
    }

    /* Binary-search for the real maximum. */
    int min = old_size;
    int max = Settings::instance().getMaxSoSndbufSize();

    while (min <= max) {
        int avg = ((unsigned int)(min + max)) / 2;
        if (cb::net::setSocketOption(sfd, SOL_SOCKET, SO_SNDBUF, avg)) {
            last_good = avg;
            min = avg + 1;
        } else {
            max = avg - 1;
        }
    }

    hint = last_good;
}

void Connection::setAuthenticated(cb::rbac::UserIdent ui) {
    auto reauthentication = isAuthenticated();
    user = std::move(ui);

    if (!reauthentication) {
        maximize_sndbuf(socketDescriptor);

#ifdef __linux__
        if (!listening_port->system) {
            auto timeout = Settings::instance().getTcpUserTimeout();
            if (!cb::net::setSocketOption<uint32_t>(
                        socketDescriptor,
                        IPPROTO_TCP,
                        TCP_USER_TIMEOUT,
                        gsl::narrow_cast<uint32_t>(timeout.count()))) {
                LOG_WARNING_CTX(
                        "Failed to set TCP_USER_TIMEOUT",
                        {"conn_id", getId()},
                        {"timeout", timeout},
                        {"error", cb_strerror(cb::net::get_socket_error())});
            }
        }
#endif
    }

    updateDescription();
    droppedPrivileges.reset();
    try {
        privilegeContext = std::make_shared<cb::rbac::PrivilegeContext>(
                createContext(getBucket().name));
    } catch (const std::exception&) {
        privilegeContext =
                std::make_shared<cb::rbac::PrivilegeContext>(createContext({}));
    }
    updatePrivilegeContext();
    thread.onConnectionAuthenticated(*this);
    if (!isInternal() && !registeredSdk && agentName.front() != '\0') {
        SdkConnectionManager::instance().registerSdk(
                std::string_view(agentName.data()));
        registeredSdk = true;
    }
}

const cb::rbac::UserIdent& Connection::getUser() const {
    static const cb::rbac::UserIdent unknown{"unknown",
                                             cb::sasl::Domain::Local};
    if (user.has_value()) {
        return user.value();
    }
    return unknown;
}

bool Connection::isTlsEnabled() const {
    return listening_port->tls;
}

bool Connection::tryAuthUserFromX509Cert(std::string_view userName,
                                         std::string_view cipherName) {
    try {
        cb::rbac::UserIdent ident{std::string{userName.data(), userName.size()},
                                  cb::sasl::Domain::Local};
        // This isn't using JWT and shouldn't have a privilege context
        // provided in the request
        auto context = cb::rbac::createContext(ident, {});
        setAuthenticated(ident);
        audit_auth_success(*this);
        LOG_INFO_CTX("Client authenticated via X.509 certificate",
                     {"conn_id", getId()},
                     {"peer", getPeername()},
                     {"cipher", cipherName},
                     {"user", ident.getSanitizedName()});
        // External users authenticated by using X.509 certificates should not
        // be able to use SASL to change its identity.
        saslAuthEnabled = getUser().is_internal();
    } catch (const cb::rbac::NoSuchUserException&) {
        return false;
    }
    return true;
}

void Connection::updateSendBytes(size_t nbytes) {
    totalSend += nbytes;
    get_thread_stats(this)->bytes_written += nbytes;
}

void Connection::updateRecvBytes(size_t nbytes) {
    totalRecv += nbytes;
    get_thread_stats(this)->bytes_read += nbytes;
}

Connection::Connection(FrontEndThread& thr)
    : ConnectionIface({{"ip", "unknown"}, {"port", 0}},
                      {{"ip", "unknown"}, {"port", 0}}),
      thread(thr),
      listening_port(std::make_shared<ListeningPort>(
              "dummy", "127.0.0.1", 11210, AF_INET, false, false)),
      max_reqs_per_event(Settings::instance().getRequestsPerEventNotification(
              EventPriority::Default)),
      socketDescriptor(INVALID_SOCKET) {
    updateDescription();
    cookies.emplace_back(std::make_unique<Cookie>(*this));
    setConnectionId("unknown:0");
    stats.conn_structs++;
    thread.onConnectionCreate(*this);
}

std::unique_ptr<Connection> Connection::create(
        SOCKET sfd, FrontEndThread& thr, std::shared_ptr<ListeningPort> descr) {
    uniqueSslPtr context;
    if (descr->tls) {
        context = networkInterfaceManager->createClientSslHandle();
        if (!context) {
            throw std::runtime_error(
                    "Connection::create: Failed to create TLS context");
        }
    }

    return std::make_unique<LibeventConnection>(
            sfd, thr, std::move(descr), std::move(context));
}

void Connection::setAuthContextLifetime(
        std::optional<std::chrono::system_clock::time_point> begin,
        std::optional<std::chrono::system_clock::time_point> end) {
    using namespace std::chrono;
    const auto system_now = system_clock::now();
    const auto steady_now = steady_clock::now();
    auto setValue = [&system_now, &steady_now](auto& field, auto& tp) {
        if (tp.has_value()) {
            if (system_now < *tp) {
                // Convert the system clock to offset from the steady clock as
                // that's cheaper to read
                field = steady_now + duration_cast<seconds>(*tp - system_now);
            } else {
                field = steady_now;
            }
        } else {
            field.reset();
        }
    };
    setValue(authContextLifetime.begin, begin);
    setValue(authContextLifetime.end, end);
}

Connection::Connection(SOCKET sfd,
                       FrontEndThread& thr,
                       std::shared_ptr<ListeningPort> descr)
    : ConnectionIface(cb::net::getPeerNameAsJson(sfd),
                      cb::net::getSockNameAsJson(sfd)),
      thread(thr),
      listening_port(std::move(descr)),
      max_reqs_per_event(Settings::instance().getRequestsPerEventNotification(
              EventPriority::Default)),
      socketDescriptor(sfd) {
    updateDescription();
    cookies.emplace_back(std::make_unique<Cookie>(*this));
    setConnectionId(cb::net::getpeername(socketDescriptor));
    stats.conn_structs++;
    thread.onConnectionCreate(*this);
}

bool Connection::maybeInitiateShutdown(const std::string_view reason,
                                       bool log) {
    if (state != State::running) {
        return false;
    }

    for (const auto& c : cookies) {
        if (c && !c->empty() && c->isEwouldblock()) {
            return false;
        }
    }

    thread.onConnectionForcedDisconnect(*this);
    auto message = fmt::format("Initiate shutdown of connection from '{}': {}",
                               peername.dump(),
                               reason);
    if (log) {
        LOG_INFO_CTX("Initiate shutdown of connection",
                     {"conn_id", getId()},
                     {"peer", peername},
                     {"reason", reason});
    }
    setTerminationReason(std::move(message));
    shutdown();
    triggerCallback(true);
    return true;
}

void Connection::reportIfStuckInShutdown(
        const std::chrono::steady_clock::time_point& tp) {
    Expects(state == State::closing || state == State::pending_close ||
            state == State::immediate_close);

    using namespace std::chrono_literals;
    if (pending_close_next_log <= tp) {
        LOG_WARNING_CTX("Stuck in shutdown",
                        {"conn_id", getId()},
                        {"duration", tp - *shutdown_initiated},
                        {"details", to_json()});
        pending_close_next_log = tp + 10s;
    }
}

Connection::~Connection() {
    thread.onConnectionDestroy(*this);
    cb::audit::addSessionTerminated(*this);

    if (listening_port->system) {
        --stats.system_conns;
    }
    if (user && user->domain == cb::sasl::Domain::External) {
        externalAuthManager->logoff(user->name);
    }

    --stats.conn_structs;
    --stats.curr_conns;
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
    std::copy_n(name.begin(), size, agentName.begin());
    agentName[size] = '\0';

    if (isAuthenticated() && !isInternal() && !registeredSdk &&
        agentName.front() != '\0') {
        SdkConnectionManager::instance().registerSdk(
                std::string_view(agentName.data()));
        registeredSdk = true;
    }
}

std::string_view Connection::getAgentName() const {
    return agentName.data();
}

void Connection::setConnectionId(std::string_view uuid) {
    auto size = std::min(uuid.size(), connectionId.size() - 1);
    std::copy_n(uuid.begin(), size, connectionId.begin());
    // the uuid string shall always be zero terminated
    connectionId[size] = '\0';
    // Remove any occurrences of " so that the client won't be allowed
    // to mess up the output where we log the cid
    std::ranges::replace(connectionId, '"', ' ');
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

void Connection::close() {
    bool ewb = false;
    uint32_t rc = refcount;

    for (auto& cookie : cookies) {
        if (cookie) {
            rc += cookie->getRefcount();
            if (cookie->isEwouldblock()) {
                ewb = true;
                break;
            }
            cookie->reset();
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

    if (rc > 1 || ewb || havePendingData()) {
        state = State::pending_close;
    } else {
        state = State::immediate_close;
    }

    // Notify interested parties that the connection is currently being
    // disconnected
    propagateDisconnect();
}

void Connection::propagateDisconnect() const {
    auto& bucket = getBucket();

    if (bucket.type == BucketType::ClusterConfigOnly) {
        // We don't have an engine
        return;
    }
    auto& engine = bucket.getEngine();
    for (auto& cookie : cookies) {
        if (cookie) {
            engine.disconnect(*cookie);
        }
    }
    engine.disconnect(*this);
}

void Connection::resetThrottledCookies() {
    for (auto& c : cookies) {
        if (c->isThrottled()) {
            c->clearEwouldblock();
            c->reset();
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
        triggerCallback(true);
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

bool Connection::havePendingData() const {
    if (sendQueueInfo.term) {
        return false;
    }

    return getSendQueueSize() != 0;
}

void Connection::setDcpFlowControlBufferSize(std::size_t size) {
    if (type == Type::Producer) {
        LOG_INFO_CTX("Using DCP buffer", {"conn_id", getId()}, {"size", size});
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
    response.setDatatype(datatype);
    response.setStatus(status);
    response.setOpaque(request.getOpaque());
    response.setCas(cookie.getCas());

    const auto tracing = isTracingEnabled();
    auto cutracing = isReportUnitUsage();
    size_t ru = 0;
    size_t wu = 0;
    uint16_t throttled = 0;

    if (cutracing) {
        auto [nru, nwu] = cookie.getDocumentMeteringRWUnits();
        throttled = cb::tracing::Tracer::encodeMicros(
                cookie.getTotalThrottleTime());
        if (!nru && !nwu && !throttled) {
            // no values to report
            cutracing = false;
        } else {
            ru = nru;
            wu = nwu;
        }
    }

    if (tracing || cutracing) {
        using namespace cb::mcbp::response;
        response.setMagic(cb::mcbp::Magic::AltClientResponse);
        // We can't use a 16 bits key length when using the alternative
        // response header. Verify that the key fits in a single byte.
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
        if (throttled) {
            framing_extras_size += ThrottleDurationFrameInfoSize;
        }
        response.setFramingExtraslen(framing_extras_size);
        response.setBodylen(gsl::narrow_cast<uint32_t>(
                value_len + extras_len + key_len + framing_extras_size));
        auto* ptr = wbuf.data() + sizeof(cb::mcbp::Response);

        auto add_frame_info = [&ptr](auto id, uint16_t val) {
            *ptr = id;
            ++ptr;
            val = htons(val);
            memcpy(ptr, &val, sizeof(val));
            ptr += sizeof(val);
        };

        if (tracing) {
            auto duration =
                    std::chrono::duration_cast<cb::tracing::Span::Duration>(
                            std::chrono::steady_clock::now() -
                            cookie.getStartTime());
            add_frame_info(ServerRecvSendDurationFrameInfoMagic,
                           cb::tracing::Tracer::encodeMicros(duration.count()));
        }

        if (ru) {
            add_frame_info(ReadUnitsFrameInfoMagic,
                           gsl::narrow_cast<uint16_t>(ru));
        }

        if (wu) {
            add_frame_info(WriteUnitsFrameInfoMagic,
                           gsl::narrow_cast<uint16_t>(wu));
        }

        if (throttled) {
            add_frame_info(ThrottleDurationFrameInfoMagic, throttled);
        }

        wbuf = {wbuf.data(), sizeof(cb::mcbp::Response) + framing_extras_size};
    } else {
        response.setMagic(cb::mcbp::Magic::ClientResponse);
        response.setFramingExtraslen(0);
        response.setBodylen(
                gsl::narrow_cast<uint32_t>(value_len + extras_len + key_len));
        wbuf = {wbuf.data(), sizeof(cb::mcbp::Response)};
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
    cookie.setResponseStatus(status);
    auto wbuf = formatResponseHeaders(cookie,
                                      {buffer.data(), buffer.size()},
                                      status,
                                      extras.size(),
                                      key.size(),
                                      value_len,
                                      datatype);
    copyToOutputStream(wbuf, extras, key);
}

bool Connection::sendResponse(Cookie& cookie,
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
    return !sendQueueInfo.term;
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
                                       cb::mcbp::DcpAddStreamFlag flags,
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

cb::engine_errc Connection::marker(
        uint32_t opaque,
        Vbid vbucket,
        uint64_t start_seqno,
        uint64_t end_seqno,
        cb::mcbp::request::DcpSnapshotMarkerFlag flags,
        std::optional<uint64_t> hcs,
        std::optional<uint64_t> hps,
        std::optional<uint64_t> mvs,
        std::optional<uint64_t> purge_seqno,
        cb::mcbp::DcpStreamId sid) {
    using Framebuilder = cb::mcbp::FrameBuilder<cb::mcbp::Request>;
    using cb::mcbp::Request;
    using cb::mcbp::request::DcpSnapshotMarkerV1Payload;
    using cb::mcbp::request::DcpSnapshotMarkerV2_2Value;
    using cb::mcbp::request::DcpSnapshotMarkerV2xPayload;

    // Allocate the buffer to be big enough for all cases, which will be the
    // v2.2 packet
    constexpr auto size = sizeof(Request) +
                          sizeof(cb::mcbp::DcpStreamIdFrameInfo) +
                          sizeof(DcpSnapshotMarkerV2xPayload) +
                          sizeof(DcpSnapshotMarkerV2_2Value);
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
            start_seqno, end_seqno, flags, hcs, hps, mvs, purge_seqno);
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
    req.setDatatype(it->getDataType());

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

    getBucket().recordDcpMeteringReadBytes(
            *this, doc_read_bytes, dcpResourceAllocationDomain);
    return cb::engine_errc::success;
}

cb::engine_errc Connection::deletionInner(const ItemIface& item,
                                          cb::const_byte_buffer packet,
                                          const DocKeyView& key) {
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
    req.setDatatype(it->getDataType());

    auto* ptr = blob.data() + sizeof(Request);
    if (sid) {
        auto buf = frameInfo.getBuf();
        std::ranges::copy(buf, ptr);
        ptr += buf.size();
        req.setFramingExtraslen(gsl::narrow_cast<uint8_t>(buf.size()));
    }

    std::copy(extdata.getBuffer().begin(), extdata.getBuffer().end(), ptr);
    cb::const_byte_buffer packetBuffer{
            blob.data(),
            sizeof(Request) + sizeof(DcpDeletionV1Payload) +
                    (sid ? sizeof(cb::mcbp::DcpStreamIdFrameInfo) : 0)};

    const auto ret = deletionInner(*it, packetBuffer, key);
    if (ret == cb::engine_errc::success) {
        getBucket().recordDcpMeteringReadBytes(
                *this, doc_read_bytes, dcpResourceAllocationDomain);
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

    // Make blob big enough for either delete or expiry
    std::array<uint8_t,
               sizeof(cb::mcbp::Request) + sizeof(extras) + sizeof(frameInfo)>
            blob = {};
    constexpr size_t payloadLen = sizeof(extras);
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
    req.setDatatype(it->getDataType());
    auto size = sizeof(cb::mcbp::Request);
    auto* ptr = blob.data() + size;
    if (sid) {
        auto buf = frameInfo.getBuf();
        std::ranges::copy(buf, ptr);
        ptr += buf.size();
        size += buf.size();
    }

    auto buffer = extras.getBuffer();
    std::ranges::copy(buffer, ptr);
    size += buffer.size();

    const auto ret = deletionInner(*it, {blob.data(), size}, key);
    if (ret == cb::engine_errc::success) {
        getBucket().recordDcpMeteringReadBytes(
                *this, doc_read_bytes, dcpResourceAllocationDomain);
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

    // Make blob big enough for either delete or expiry
    std::array<uint8_t,
               sizeof(cb::mcbp::Request) + sizeof(extras) + sizeof(frameInfo)>
            blob = {};
    constexpr size_t payloadLen = sizeof(extras);
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
    req.setDatatype(it->getDataType());
    auto size = sizeof(cb::mcbp::Request);
    auto* ptr = blob.data() + size;
    if (sid) {
        auto buf = frameInfo.getBuf();
        std::ranges::copy(buf, ptr);
        ptr += buf.size();
        size += buf.size();
    }

    auto buffer = extras.getBuffer();
    std::ranges::copy(buffer, ptr);
    size += buffer.size();

    const auto ret = deletionInner(*it, {blob.data(), size}, key);
    if (ret == cb::engine_errc::success) {
        getBucket().recordDcpMeteringReadBytes(
                *this, doc_read_bytes, dcpResourceAllocationDomain);
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
                                                   uint32_t buffer_bytes) {
    cb::mcbp::request::DcpBufferAckPayload extras;
    extras.setBufferBytes(buffer_bytes);
    cb::mcbp::RequestBuilder builder(thread.getScratchBuffer());
    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(cb::mcbp::ClientOpcode::DcpBufferAcknowledgement);
    builder.setOpaque(opaque);
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
    req.setDatatype(it->getDataType());

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
                                   const DocKeyView& key_,
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
                                  const DocKeyView& key_,
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
    constexpr size_t totalBytes = sizeof(cb::mcbp::Request) + sizeof(extras) +
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
    constexpr size_t totalBytes = sizeof(cb::mcbp::Request) + sizeof(extras) +
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

void Connection::onTlsConnect(const SSL* ssl_st) {
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
                    return inst.mapper->lookupUser(cert, [](const auto& name) {
                        return name == "internal";
                    });
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
                    tryAuthUserFromX509Cert(name, SSL_get_cipher_name(ssl_st));
                } else {
                    status = cb::x509::Status::NoMatch;
                }
            } else {
                auto cipher = SSL_get_cipher_name(ssl_st);
                auto pair = Settings::instance().lookupUser(
                        cert.get(), [this, &cipher](const auto& name) -> bool {
                            try {
                                return tryAuthUserFromX509Cert(name, cipher);
                            } catch (const std::exception& e) {
                                restartAuthentication();
                                return false;
                            }
                        });
                status = pair.first;
                name = std::move(pair.second);
            }

            switch (status) {
            case cb::x509::Status::NoMatch:
                audit_auth_failure(*this,
                                   {"unknown", cb::sasl::Domain::Local},
                                   "Failed to map a user from the client "
                                   "provided X.509 certificate");
                setTerminationReason(
                        "Failed to map a user from the client provided X.509 "
                        "certificate");
                LOG_WARNING_CTX(
                        "Failed to map a user from the client provided X.509 "
                        "certificate",
                        {"conn_id", getId()},
                        {"user", name});
                disconnect = true;
                break;
            case cb::x509::Status::Error:
                audit_auth_failure(
                        *this,
                        {"unknown", cb::sasl::Domain::Local},
                        "Failed to use client provided X.509 certificate");
                setTerminationReason(
                        "Failed to use client provided X.509 certificate");
                LOG_WARNING_CTX(
                        "Disconnection client due to error with the X.509 "
                        "certificate",
                        {"conn_id", getId()},
                        {"user", name});
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
                    audit_auth_failure(*this,
                                       {"unknown", cb::sasl::Domain::Local},
                                       reason);
                    setTerminationReason(reason);
                    disconnect = true;
                    LOG_WARNING_CTX("Disconnecting client",
                                    {"conn_id", getId()},
                                    {"reason", reason});
                }
                break;
            case cb::x509::Status::Success:
                // authenticated as part of the lookup method
                break;
            }
        }
    }

    if (disconnect) {
        shutdown();
    } else if (!isAuthenticated()) {
        // tryAuthFromSslCertificate logged the cipher
        LOG_INFO_CTX("Using cipher",
                     {"conn_id", getId()},
                     {"cipher", SSL_get_cipher_name(ssl_st)},
                     {"certificate", static_cast<bool>(cert)});
    }
}

std::string Connection::getSaslMechanisms() const {
    if (!saslAuthEnabled) {
        return {};
    }
    const auto tls = isTlsEnabled();
    if (tls && Settings::instance().has.ssl_sasl_mechanisms) {
        return Settings::instance().getSslSaslMechanisms();
    }

    if (!tls && Settings::instance().has.sasl_mechanisms) {
        return Settings::instance().getSaslMechanisms();
    }

    // None configured, return the full list
    return cb::sasl::server::listmech(tls);
}

void Connection::scheduleDcpStep() {
    if (!isDCP()) {
        LOG_ERROR_CTX(
                "Connection::scheduleDcpStep: Must only be called with a DCP "
                "connection",
                {"conn", to_json()});
        throw std::logic_error(
                "Connection::scheduleDcpStep(): Provided cookie is not bound "
                "to a connection set up for DCP");
    }

    // @todo we've not switched the backed off the logic with the first
    //       cookie
    if (cookies.front()->getRefcount() == 0) {
        LOG_ERROR_CTX(
                "scheduleDcpStep: DCP connection did not reserve the "
                "cookie",
                {"conn", to_json()});
        throw std::logic_error("scheduleDcpStep: cookie must be reserved!");
    }

    getThread().eventBase.runInEventBaseThreadAlwaysEnqueue([this]() {
        TRACE_LOCKGUARD_TIMED(getThread().mutex,
                              "mutex",
                              "scheduleDcpStep",
                              SlowMutexThreshold);
        triggerCallback();
    });
}

bool Connection::mayAccessBucket(std::string_view bucket) const {
    if (!isAuthenticated()) {
        // Fast path. unauthenticated users don't have access
        return false;
    }

    if (tokenProvidedUserEntry) {
        try {
            (void)createContext(bucket);
            return true;
        } catch (const std::exception&) {
            return false;
        }
    }
    return cb::rbac::mayAccessBucket(getUser(), std::string{bucket});
}

cb::rbac::PrivilegeContext Connection::createContext(
        std::string_view bucket) const {
    Expects(isAuthenticated());
    if (tokenProvidedUserEntry) {
        std::string name(bucket);
        if (bucket.empty()) {
            return {0,
                    user->domain,
                    tokenProvidedUserEntry->getPrivileges(),
                    {},
                    true};
        }
        // Add the bucket specific privileges
        auto iter = tokenProvidedUserEntry->getBuckets().find(name);
        if (iter == tokenProvidedUserEntry->getBuckets().end()) {
            // No explicit match... Is there a wildcard entry
            iter = tokenProvidedUserEntry->getBuckets().find("*");
            if (iter == tokenProvidedUserEntry->getBuckets().cend()) {
                throw cb::rbac::NoSuchBucketException(name.c_str());
            }
        }

        return {0,
                user->domain,
                tokenProvidedUserEntry->getPrivileges(),
                iter->second,
                true};
    }

    return cb::rbac::createContext(getUser(), std::string{bucket});
}

void to_json(nlohmann::json& json, const Connection& connection) {
    json = connection.to_json();
}
