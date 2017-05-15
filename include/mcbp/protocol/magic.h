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

namespace cb {
namespace mcbp {
/**
 * Definition of the legal "magic" values used in a packet.
 * See section 3.1 Magic byte
 */
enum class Magic : uint8_t {
    /// Request packet from client to server
    ClientRequest = 0x80,
    /// Response packet from server to client
    ClientResponse = 0x81,
    /// Request packet from server to client
    ServerRequest = 0x82,
    /// Response packet from client to server
    ServerResponse = 0x83
};
} // namespace mcbp
} // namespace cb
