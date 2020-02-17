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
#pragma once
#include <mcbp/protocol/framebuilder.h>

/**
 * The unique_request_ptr returns a pointer to a Request, but the underlying
 * allocation is an array of bytes so we need a special deleter to make
 * sure that we release the correct amount of bytes
 */
struct RequestDeleter {
    void operator()(cb::mcbp::Request* request);
};
using unique_request_ptr = std::unique_ptr<cb::mcbp::Request, RequestDeleter>;

unique_request_ptr createPacket(cb::mcbp::ClientOpcode opcode,
                                Vbid vbid = Vbid(0),
                                uint64_t cas = 0,
                                std::string_view ext = {},
                                std::string_view key = {},
                                std::string_view val = {},
                                uint8_t datatype = 0x00,
                                std::string_view meta = {});
