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
#include "dcp_system_event_executor.h"
#include "engine_wrapper.h"
#include "no_success_response_steppable_context.h"

#include <memcached/protocol_binary.h>

void dcp_system_event_executor(Cookie& cookie) {
    cookie.obtainContext<
                  NoSuccessResponseCommandContext>(cookie, [](Cookie& c) {
              using cb::mcbp::request::DcpSystemEventPayload;
              const auto& request = c.getRequest();
              const auto& payload =
                      request.getCommandSpecifics<DcpSystemEventPayload>();
              return dcpSystemEvent(
                      c,
                      request.getOpaque(),
                      request.getVBucket(),
                      mcbp::systemevent::id(payload.getEvent()),
                      payload.getBySeqno(),
                      mcbp::systemevent::version(payload.getVersion()),
                      request.getKey(),
                      request.getValue());
          }).drive();
}
