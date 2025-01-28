/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "unknown_packet_command_context.h"

#include "engine_wrapper.h"

#include <daemon/mcbp.h>

UnknownPacketCommandContext::UnknownPacketCommandContext(Cookie& cookie)
    : SteppableCommandContext(cookie) {
}

cb::engine_errc UnknownPacketCommandContext::step() {
    // The underlying engine sends the response back to the client
    // if success is returned so we don't need to deal with anything
    return bucket_unknown_command(cookie, mcbpResponseHandlerFn);
}
