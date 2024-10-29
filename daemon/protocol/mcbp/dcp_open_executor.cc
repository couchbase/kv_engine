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
#include <daemon/front_end_thread.h>
#include <daemon/mcaudit.h>
#include <logger/logger.h>
#include <memcached/protocol_binary.h>
#include <string>

void dcp_open_executor(Cookie& cookie) {
    using cb::mcbp::DcpOpenFlag;
    using cb::mcbp::request::DcpOpenPayload;

    auto& request = cookie.getHeader().getRequest();
    const auto& payload = request.getCommandSpecifics<DcpOpenPayload>();
    const auto flags = payload.getFlags();

    auto ret = cookie.swapAiostat(cb::engine_errc::success);

    auto& connection = cookie.getConnection();
    connection.enableDatatype(cb::mcbp::Feature::JSON);

    const bool dcpProducer =
            (flags & DcpOpenFlag::Producer) == DcpOpenFlag::Producer;

    if (ret == cb::engine_errc::success) {
        const auto privilege = dcpProducer ? cb::rbac::Privilege::DcpProducer
                                           : cb::rbac::Privilege::DcpConsumer;

        ret = mcbp::checkPrivilege(cookie, privilege);

        if (ret == cb::engine_errc::success) {
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
                ret = dcpOpen(cookie,
                              request.getOpaque(),
                              payload.getSeqno(),
                              flags,
                              request.getKeyString(),
                              request.getValueString());
            } catch (const std::exception& e) {
                LOG_WARNING_CTX(
                        "Received an exception as part DCP Open, disconnect "
                        "client",
                        {"conn_id", connection.getId()},
                        {"error", e.what()});
                ret = cb::engine_errc::disconnect;
            }
            cookie.decrementRefcount();
        }
    }

    if (ret == cb::engine_errc::success) {
        const bool dcpXattrAware = (flags & DcpOpenFlag::IncludeXattrs) ==
                                           DcpOpenFlag::IncludeXattrs &&
                                   connection.selectedBucketIsXattrEnabled();
        const bool dcpDeletedUserXattr =
                (flags & DcpOpenFlag::IncludeDeletedUserXattrs) ==
                        DcpOpenFlag::IncludeDeletedUserXattrs &&
                connection.selectedBucketIsXattrEnabled();
        const bool dcpNoValue =
                (flags & DcpOpenFlag::NoValue) == DcpOpenFlag::NoValue;
        const bool dcpDeleteTimes = (flags & DcpOpenFlag::IncludeDeleteTimes) ==
                                    DcpOpenFlag::IncludeDeleteTimes;
        connection.setDcpXattrAware(dcpXattrAware);
        connection.setDcpDeletedUserXattr(dcpDeletedUserXattr);
        connection.setDcpNoValue(dcpNoValue);
        connection.setDcpDeleteTimeEnabled(dcpDeleteTimes);
        connection.disableSaslAuth();
        connection.setType(dcpProducer ? Connection::Type::Producer
                                       : Connection::Type::Consumer);

        if (!connection.getDcpConnHandler()) {
            throw std::logic_error(
                    "dcp_open_executor(): The underlying engine returned "
                    "success but did not set up a DCP connection handler "
                    "interface");
        }

        auto flags = cb::logger::Json::array();

        if (dcpProducer) {
            flags.push_back("PRODUCER");
        } else {
            flags.push_back("CONSUMER");
        }
        if (dcpXattrAware) {
            flags.push_back("INCLUDE_XATTRS");
        }
        if (dcpNoValue) {
            flags.push_back("NO_VALUE");
        }
        if (dcpDeleteTimes) {
            flags.push_back("DELETE_TIMES");
        }
        if (connection.isDcpDeletedUserXattr()) {
            flags.push_back("INCLUDE_DELETED_USER_XATTRS");
        }

        LOG_INFO_CTX("DCP connection opened successfully",
                     {"conn_id", connection.getId()},
                     {"flags", std::move(flags)},
                     {"description", connection.getDescription()});

        connection.getThread().maybeRegisterThrottleableDcpConnection(
                connection);
        audit_dcp_open(cookie);
        cookie.sendResponse(cb::mcbp::Status::Success);
    } else {
        handle_executor_status(cookie, ret);
    }
}
