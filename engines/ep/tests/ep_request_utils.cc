/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "ep_request_utils.h"

void RequestDeleter::operator()(cb::mcbp::Request* request) {
    delete[] reinterpret_cast<uint8_t*>(request);
}

unique_request_ptr createPacket(cb::mcbp::ClientOpcode opcode,
                                Vbid vbid,
                                uint64_t cas,
                                std::string_view ext,
                                std::string_view key,
                                std::string_view val,
                                uint8_t datatype,
                                std::string_view meta) {
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
