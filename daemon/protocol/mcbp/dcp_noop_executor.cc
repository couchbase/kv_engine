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
#include "single_state_steppable_context.h"
#include <daemon/cookie.h>
#include <mcbp/protocol/header.h>

void dcp_noop_executor(Cookie& cookie) {
    cookie.obtainContext<SingleStateCommandContext>(cookie, [](Cookie& c) {
              return SingleStateCommandContext::noPayload(
                      dcpNoop(c, c.getHeader().getOpaque()));
          }).drive();
}
