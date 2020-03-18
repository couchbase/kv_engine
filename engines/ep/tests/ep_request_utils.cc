/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc
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

#include "ep_request_utils.h"

void RequestDeleter::operator()(cb::mcbp::Request* request) {
    delete[] reinterpret_cast<uint8_t*>(request);
}

unique_request_ptr createPacket(cb::mcbp::ClientOpcode opcode,
                                Vbid vbid,
                                uint64_t cas,
                                cb::const_char_buffer ext,
                                cb::const_char_buffer key,
                                cb::const_char_buffer val,
                                uint8_t datatype,
                                cb::const_char_buffer meta) {
    using namespace cb::mcbp;

    const auto total = sizeof(cb::mcbp::Request) + ext.size() + key.size() +
                       val.size() + meta.size();
    std::unique_ptr<uint8_t[]> memory(new uint8_t[total]);
    RequestBuilder builder({memory.get(), total});

    builder.setMagic(cb::mcbp::Magic::ClientRequest);
    builder.setOpcode(opcode);
    builder.setVBucket(vbid);
    builder.setCas(cas);
    builder.setDatatype(cb::mcbp::Datatype(datatype));
    builder.setExtras(
            {reinterpret_cast<const uint8_t*>(ext.data()), ext.size()});
    builder.setKey({reinterpret_cast<const uint8_t*>(key.data()), key.size()});
    if (meta.empty()) {
        builder.setValue(
                {reinterpret_cast<const uint8_t*>(val.data()), val.size()});
    } else {
        std::vector<uint8_t> backing;
        std::copy(val.begin(), val.end(), std::back_inserter(backing));
        std::copy(meta.begin(), meta.end(), std::back_inserter(backing));
        builder.setValue({backing.data(), backing.size()});
    }
    memory.release();
    return unique_request_ptr(builder.getFrame());
}