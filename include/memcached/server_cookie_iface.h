/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "engine_error.h"
#include "protocol_binary.h"
#include "rbac.h"
#include "types.h"

#include <gsl/gsl-lite.hpp>
#include <mcbp/protocol/opcode.h>
#include <nlohmann/json_fwd.hpp>
#include <string>

namespace cb::mcbp {
class Request;
} // namespace cb::mcbp

class CookieIface;
class DcpConnHandlerIface;

/**
 * Commands to operate on a specific cookie.
 */
struct ServerCookieIface {
    virtual ~ServerCookieIface() = default;

    /**
     * Set the size of the DCP flow control buffer size used by this
     * DCP producer
     *
     * @param cookie the cookie representing the DCP connection
     * @param size The new buffer size
     */
    virtual void setDcpFlowControlBufferSize(CookieIface& cookie,
                                             std::size_t size) = 0;
};
