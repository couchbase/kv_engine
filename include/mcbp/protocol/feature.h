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
#pragma once

#include <cstdint>
#include <string>

namespace cb {
namespace mcbp {
/**
 * Definition of hello's features.
 * Note regarding JSON:0x1. Previously this was named DATATYPE and
 * implied that when supported all bits of the protocol datatype byte would
 * be valid. DATATYPE was never enabled and has been renamed as
 * JSON. Clients are now required negotiate individual datatypes
 * with the server using the feature DataType_* feature codes. Note XATTR
 * is linked with general xattr support and the ability to set the xattr
 * datatype bit using set_with_meta.
 */
enum class Feature : uint16_t {
    Invalid = 0x01, // Previously DATATYPE, now retired
    TLS = 0x2,
    TCPNODELAY = 0x03,
    MUTATION_SEQNO = 0x04,
    TCPDELAY = 0x05,
    XATTR = 0x06, // enables xattr support and set_with_meta.datatype == xattr
    XERROR = 0x07,
    SELECT_BUCKET = 0x08,
    COLLECTIONS = 0x09,
    SNAPPY = 0x0a,
    JSON = 0x0b,
    Duplex = 0x0c,
    /**
     * Request the server to push any cluster maps stored by ns_server into
     * one of the buckets the client have access to.
     */
    ClustermapChangeNotification = 0x0d
};

} // namespace mcbp
} // namespace cb

std::string to_string(cb::mcbp::Feature feature);
