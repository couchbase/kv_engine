/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "executors.h"
#include "single_state_steppable_context.h"

#include <daemon/cookie.h>
#include <memcached/collections.h>

void collections_get_collection_id_executor(Cookie& cookie) {
    cookie.obtainContext<SingleStateCommandContext>(
                  cookie,
                  [](Cookie& c) -> std::expected<std::string, cb::engine_errc> {
                      auto& req = c.getRequest();
                      auto path = req.getKeyString();
                      if (path.empty()) {
                          path = req.getValueString();
                      }
                      auto rv = c.getConnection()
                                        .getBucketEngine()
                                        .get_collection_id(c, path);
                      if (rv.result != cb::engine_errc::success) {
                          return std::unexpected(rv.result);
                      }
                      return std::string{rv.getPayload().getBuffer()};
                  },
                  cb::mcbp::Datatype::Raw,
                  SingleStateCommandContext::PayloadLocation::Extras)
            .drive();
}
