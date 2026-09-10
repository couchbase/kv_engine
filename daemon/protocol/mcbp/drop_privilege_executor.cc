/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "executors.h"
#include "single_state_steppable_context.h"

#include <daemon/connection.h>
#include <daemon/cookie.h>
#include <daemon/sendbuffer.h>
#include <mcbp/protocol/request.h>

void drop_privilege_executor(Cookie& cookie) {
    cookie.obtainContext<SingleStateCommandContext>(cookie, [](Cookie& c) {
              const auto& request = c.getRequest();
              try {
                  auto privilege =
                          cb::rbac::to_privilege(request.getPrintableKey());
                  return SingleStateCommandContext::noPayload(
                          c.getConnection().dropPrivilege(privilege));
              } catch (const std::invalid_argument&) {
                  // Invalid name of privilege
                  return SingleStateCommandContext::noPayload(
                          cb::engine_errc::no_such_key);
              }
          }).drive();
}
