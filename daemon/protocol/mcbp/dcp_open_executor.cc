/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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
    auto ext = request.getExtdata();
    const auto* payload = reinterpret_cast<const DcpOpenPayload*>(ext.data());
    const uint32_t flags = payload->getFlags();

    auto ret = cookie.swapAiostat(ENGINE_SUCCESS);

    auto& connection = cookie.getConnection();
    connection.enableDatatype(cb::mcbp::Feature::JSON);

    const bool dcpNotifier =
            (flags & DcpOpenPayload::Notifier) == DcpOpenPayload::Notifier;

    if (ret == ENGINE_SUCCESS) {
        cb::rbac::Privilege privilege = cb::rbac::Privilege::DcpProducer;
        if (dcpNotifier) {
            privilege = cb::rbac::Privilege::DcpConsumer;
        }

        ret = mcbp::checkPrivilege(cookie, privilege);

        if (ret == ENGINE_SUCCESS) {
            auto key = request.getKey();
            auto value = request.getValue();

            ret = dcpOpen(
                    cookie,
                    request.getOpaque(),
                    payload->getSeqno(),
                    flags,
                    {reinterpret_cast<const char*>(key.data()), key.size()},
                    {reinterpret_cast<const char*>(value.data()),
                     value.size()});
        }
    }

    if (ret == ENGINE_SUCCESS) {
        const bool dcpXattrAware =
                (flags & DcpOpenPayload::IncludeXattrs) != 0 &&
                connection.selectedBucketIsXattrEnabled();
        const bool dcpNoValue = (flags & DcpOpenPayload::NoValue) != 0;
        const bool dcpDeleteTimes =
                (flags & DcpOpenPayload::IncludeDeleteTimes) != 0;
        connection.setDcpXattrAware(dcpXattrAware);
        connection.setDcpNoValue(dcpNoValue);
        connection.setDcpDeleteTimeEnabled(dcpDeleteTimes);
        connection.setDCP(true);
        connection.disableSaslAuth();

        // String buffer with max length = total length of all possible contents
        std::string logBuffer;

        const bool dcpProducer =
                (flags & DcpOpenPayload::Producer) == DcpOpenPayload::Producer;
        if (dcpProducer) {
            logBuffer.append("PRODUCER, ");
        }
        if (dcpNotifier) {
            logBuffer.append("NOTIFIER, ");
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

        // Remove trailing whitespace and comma
        if (!logBuffer.empty()) {
            logBuffer.resize(logBuffer.size() - 2);
        }

        LOG_INFO("{}: DCP connection opened successfully. {} {}",
                 connection.getId(),
                 logBuffer,
                 connection.getDescription().c_str());

        audit_dcp_open(connection);
        cookie.sendResponse(cb::mcbp::Status::Success);
    } else {
        handle_executor_status(cookie, ret);
    }
}
