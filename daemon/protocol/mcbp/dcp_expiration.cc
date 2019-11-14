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
#include "dcp_expiration.h"
#include "engine_wrapper.h"
#include "utilities.h"
#include <memcached/limits.h>
#include <memcached/protocol_binary.h>

void dcp_expiration_executor(Cookie& cookie) {
    auto ret = cookie.swapAiostat(ENGINE_SUCCESS);

    auto& connection = cookie.getConnection();
    if (ret == ENGINE_SUCCESS) {
        const auto& req = cookie.getRequest();
        const auto datatype = uint8_t(req.getDatatype());
        const auto extdata = req.getExtdata();
        using cb::mcbp::request::DcpExpirationPayload;
        const auto& extras =
                *reinterpret_cast<const DcpExpirationPayload*>(extdata.data());

        auto value = req.getValue();

        uint32_t priv_bytes = 0;
        if (mcbp::datatype::is_xattr(datatype)) {
            priv_bytes = gsl::narrow<uint32_t>(value.size());
        }

        if (priv_bytes > cb::limits::PrivilegedBytes) {
            ret = ENGINE_E2BIG;
        } else {
            ret = dcpExpiration(cookie,
                                req.getOpaque(),
                                connection.makeDocKey(req.getKey()),
                                value,
                                priv_bytes,
                                uint8_t(req.getDatatype()),
                                req.getCas(),
                                req.getVBucket(),
                                extras.getBySeqno(),
                                extras.getRevSeqno(),
                                extras.getDeleteTime());
        }
    }

    ret = connection.remapErrorCode(ret);
    switch (ret) {
    case ENGINE_SUCCESS:
        connection.setState(StateMachine::State::new_cmd);
        break;

    case ENGINE_DISCONNECT:
        connection.shutdown();
        break;

    case ENGINE_EWOULDBLOCK:
        cookie.setEwouldblock(true);
        break;

    default:
        cookie.sendResponse(cb::engine_errc(ret));
    }
}
