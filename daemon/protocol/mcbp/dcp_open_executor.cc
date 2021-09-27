/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "engine_wrapper.h"
#include "executors.h"
#include "utilities.h"
#include <daemon/mcaudit.h>
#include <logger/logger.h>
#include <memcached/protocol_binary.h>
#include <string>

void dcp_open_executor(Cookie& cookie) {
    using cb::mcbp::request::DcpOpenPayload;

    auto& request = cookie.getHeader().getRequest();
    const auto& payload = request.getCommandSpecifics<DcpOpenPayload>();
    const uint32_t flags = payload.getFlags();

    auto ret = cookie.swapAiostat(cb::engine_errc::success);

    auto& connection = cookie.getConnection();
    connection.enableDatatype(cb::mcbp::Feature::JSON);

    const bool dcpProducer =
            (flags & DcpOpenPayload::Producer) == DcpOpenPayload::Producer;

    if (ret == cb::engine_errc::success) {
        const auto privilege = dcpProducer ? cb::rbac::Privilege::DcpProducer
                                           : cb::rbac::Privilege::DcpConsumer;

        ret = mcbp::checkPrivilege(cookie, privilege);

        if (ret == cb::engine_errc::success) {
            auto key = request.getKey();
            auto value = request.getValue();

            // MB-43622 There is a race condition in the creation and
            //          notification of the DCP connections. Initially
            //          I tried to reserve the cookie from the constructor
            //          of the ConnHandler, but that caused a ton of problems
            //          in the unit tests as they didn't explicitly release
            //          the reference (and trying to clean up all of that was
            //          a lot of work). In addition to that we could end up
            //          with a memory allocation failure trying to insert
            //          the new cookie in the connection array which would
            //          also make it hard to figure out when to release
            //          the reference (and the engine is not allowed to call
            //          release from a workerthread as it tries to reschedule
            //          the cookie). The workaround for used is to bump
            //          the refcount before calling DCP Open so that the
            //          the checks in scheduleDcpStep can see that the ref
            //          count is correct if we get a notification before
            //          this thread call reserve.
            cookie.incrementRefcount();
            try {
                ret = dcpOpen(
                        cookie,
                        request.getOpaque(),
                        payload.getSeqno(),
                        flags,
                        {reinterpret_cast<const char*>(key.data()), key.size()},
                        {reinterpret_cast<const char*>(value.data()),
                         value.size()});
            } catch (const std::exception& e) {
                LOG_WARNING(
                        "{}: Received an exception as part DCP Open: {}, "
                        "disconnect client",
                        connection.getId(),
                        e.what());
                ret = cb::engine_errc::disconnect;
            }
            cookie.decrementRefcount();
        }
    }

    if (ret == cb::engine_errc::success) {
        const bool dcpXattrAware =
                (flags & DcpOpenPayload::IncludeXattrs) != 0 &&
                connection.selectedBucketIsXattrEnabled();
        const bool dcpDeletedUserXattr =
                (flags & DcpOpenPayload::IncludeDeletedUserXattrs) != 0 &&
                connection.selectedBucketIsXattrEnabled();
        const bool dcpNoValue = (flags & DcpOpenPayload::NoValue) != 0;
        const bool dcpDeleteTimes =
                (flags & DcpOpenPayload::IncludeDeleteTimes) != 0;
        connection.setDcpXattrAware(dcpXattrAware);
        connection.setDcpDeletedUserXattr(dcpDeletedUserXattr);
        connection.setDcpNoValue(dcpNoValue);
        connection.setDcpDeleteTimeEnabled(dcpDeleteTimes);
        connection.disableSaslAuth();
        connection.setType(dcpProducer ? Connection::Type::Producer
                                       : Connection::Type::Consumer);

        if (!connection.getDcpConnHandlerIface()) {
            throw std::logic_error(
                    "dcp_open_executor(): The underlying engine returned "
                    "success but did not set up a DCP connection handler "
                    "interface");
        }

        // String buffer with max length = total length of all possible contents
        std::string logBuffer;

        if (dcpProducer) {
            logBuffer.append("PRODUCER, ");
        } else {
            logBuffer.append("CONSUMER, ");
        }
        if (dcpXattrAware) {
            logBuffer.append("INCLUDE_XATTRS, ");
        }
        if (dcpNoValue) {
            logBuffer.append("NO_VALUE, ");
        }
        if (dcpDeleteTimes) {
            logBuffer.append("DELETE_TIMES, ");
        }
        if (connection.isDcpDeletedUserXattr()) {
            logBuffer.append("INCLUDE_DELETED_USER_XATTRS, ");
        }

        // Remove trailing whitespace and comma
        if (!logBuffer.empty()) {
            logBuffer.resize(logBuffer.size() - 2);
        }

        LOG_INFO("{}: DCP connection opened successfully. {} {}",
                 connection.getId(),
                 logBuffer,
                 connection.getDescription().c_str());

        audit_dcp_open(cookie);
        cookie.sendResponse(cb::mcbp::Status::Success);
    } else {
        handle_executor_status(cookie, ret);
    }
}
