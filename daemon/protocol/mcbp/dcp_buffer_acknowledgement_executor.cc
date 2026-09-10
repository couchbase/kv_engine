/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "engine_wrapper.h"
#include "executors.h"
#include "no_success_response_steppable_context.h"
#include <daemon/cookie.h>
#include <memcached/protocol_binary.h>

void dcp_buffer_acknowledgement_executor(Cookie& cookie) {
    cookie.obtainContext<NoSuccessResponseCommandContext>(
                  cookie,
                  [](Cookie& c) {
                      auto& req = c.getRequest();
                      using cb::mcbp::request::DcpBufferAckPayload;
                      const auto& payload =
                              req.getCommandSpecifics<DcpBufferAckPayload>();
                      return dcpBufferAcknowledgement(
                              c, req.getOpaque(), payload.getBufferBytes());
                  })
            .drive();
}
