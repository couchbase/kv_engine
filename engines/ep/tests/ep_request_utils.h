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
