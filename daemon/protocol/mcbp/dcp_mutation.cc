/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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

#include "dcp_mutation.h"
#include "engine_wrapper.h"
#include "utilities.h"

#include <memcached/limits.h>
#include <memcached/protocol_binary.h>
#include <xattr/blob.h>
#include <xattr/utils.h>

static inline ENGINE_ERROR_CODE do_dcp_mutation(Cookie& cookie) {
    const auto& req = cookie.getRequest();
    const auto extdata = req.getExtdata();
    const auto& extras =
            *reinterpret_cast<const cb::mcbp::request::DcpMutationPayload*>(
                    extdata.data());
    const auto datatype = uint8_t(req.getDatatype());
    const auto nmeta = extras.getNmeta();
    auto value = req.getValue();
    const cb::const_byte_buffer meta = {value.data() + value.size() - nmeta,
                                        nmeta};
    value = {value.data(), value.size() - nmeta};

    uint32_t priv_bytes = 0;
    if (mcbp::datatype::is_xattr(datatype)) {
        const char* payload = reinterpret_cast<const char*>(value.buf);
        cb::xattr::Blob blob({const_cast<char*>(payload), value.len},
                             mcbp::datatype::is_snappy(datatype));
        priv_bytes = uint32_t(blob.get_system_size());
        if (priv_bytes > cb::limits::PrivilegedBytes) {
            return ENGINE_E2BIG;
        }
    }

    return dcpMutation(cookie,
                       req.getOpaque(),
                       cookie.getConnection().makeDocKey(req.getKey()),
                       value,
                       priv_bytes,
                       datatype,
                       req.getCas(),
                       req.getVBucket(),
                       extras.getFlags(),
                       extras.getBySeqno(),
                       extras.getRevSeqno(),
                       extras.getExpiration(),
                       extras.getLockTime(),
                       meta,
                       extras.getNru());
}

void dcp_mutation_executor(Cookie& cookie) {
    auto ret = cookie.swapAiostat(ENGINE_SUCCESS);

    auto& connection = cookie.getConnection();
    if (ret == ENGINE_SUCCESS) {
        ret = do_dcp_mutation(cookie);
    }

    ret = connection.remapErrorCode(ret);
    switch (ret) {
    case ENGINE_SUCCESS:
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
