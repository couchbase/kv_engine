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

#include <mcbp/mcbp.h>

#include <stdexcept>

namespace cb {
namespace mcbp {

unique_cJSON_ptr Header::toJSON() const {
    if (!isRequest() && !isResponse()) {
        throw std::logic_error("Header::toJSON(): Invalid packet");
    }
    unique_cJSON_ptr ret(cJSON_CreateObject());
    auto m = cb::mcbp::Magic(magic);
    cJSON_AddStringToObject(ret.get(), "magic", ::to_string(m).c_str());
    switch (m) {
    case Magic::ClientRequest: {
        const auto& req = getRequest();
        cJSON_AddStringToObject(ret.get(),
                                "opcode",
                                ::to_string(req.getClientOpcode()).c_str());
        cJSON_AddNumberToObject(ret.get(), "vbucket", req.getVBucket());
    } break;
    case Magic::ClientResponse:
    case Magic::AltClientResponse: {
        const auto& res = getResponse();
        cJSON_AddStringToObject(ret.get(),
                                "opcode",
                                ::to_string(res.getClientOpcode()).c_str());
        cJSON_AddStringToObject(
                ret.get(),
                "status",
                ::to_string(cb::mcbp::Status(res.getStatus())).c_str());
    } break;

    case Magic::ServerRequest: {
        const auto& req = getRequest();
        cJSON_AddStringToObject(ret.get(),
                                "opcode",
                                ::to_string(req.getServerOpcode()).c_str());
        cJSON_AddNumberToObject(ret.get(), "vbucket", req.getVBucket());
    } break;
    case Magic::ServerResponse: {
        const auto& res = getResponse();
        cJSON_AddStringToObject(ret.get(),
                                "opcode",
                                ::to_string(res.getServerOpcode()).c_str());
        cJSON_AddStringToObject(
                ret.get(),
                "status",
                ::to_string(cb::mcbp::Status(res.getStatus())).c_str());
    } break;
    }

    return ret;
}

} // namespace mcbp
} // namespace cb
