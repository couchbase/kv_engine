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

#include <mcbp/protocol/response.h>
#include <mcbp/protocol/status.h>

unique_cJSON_ptr cb::mcbp::Response::toJSON() const {
    if (!isValid()) {
        throw std::logic_error("Response::toJSON(): Invalid packet");
    }
    unique_cJSON_ptr ret(cJSON_CreateObject());
    auto m = cb::mcbp::Magic(magic);
    cJSON_AddStringToObject(ret.get(), "magic", ::to_string(m));

    if (m == Magic::ClientResponse || m == Magic::AltClientResponse) {
        cJSON_AddStringToObject(
                ret.get(), "opcode", ::to_string(getClientOpcode()));

    } else {
        cJSON_AddStringToObject(
                ret.get(), "opcode", ::to_string(getServerOpcode()));
    }

    cJSON_AddNumberToObject(ret.get(), "keylen", getKeylen());
    cJSON_AddNumberToObject(ret.get(), "extlen", getExtlen());

    if (m == Magic::AltClientResponse) {
        cJSON_AddNumberToObject(
                ret.get(), "framingextra", getFramingExtraslen());
    }

    cJSON_AddItemToObject(
            ret.get(), "datatype", ::toJSON(getDatatype()).release());
    cJSON_AddStringToObject(
            ret.get(), "status", ::to_string(Status(getStatus())));
    cJSON_AddNumberToObject(ret.get(), "bodylen", getBodylen());
    cJSON_AddUintPtrToObject(ret.get(), "opaque", getOpaque());
    cJSON_AddNumberToObject(ret.get(), "cas", getCas());

    return ret;
}
