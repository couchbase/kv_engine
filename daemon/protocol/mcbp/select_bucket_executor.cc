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

#include <cblogger/logger.h>
#include <daemon/bucket_manager.h>
#include <daemon/buckets.h>
#include <daemon/cookie.h>
#include <mcbp/protocol/request.h>
#include <platform/scope_timer.h>
#include <serverless/config.h>

static cb::engine_errc select_bucket(Cookie& cookie,
                                     const std::string_view bucketname) {
    auto& connection = cookie.getConnection();
    auto oldIndex = connection.getBucketIndex();

    if (!cookie.mayAccessBucket(bucketname)) {
        return cb::engine_errc::no_access;
    }

    auto& bm = BucketManager::instance();
    if (bm.associateBucket(cookie, bucketname)) {
        if (cb::serverless::isEnabled() && !connection.isInternal()) {
            using cb::serverless::Config;
            if (connection.getBucket().references >
                Config::instance().maxConnectionsPerBucket.load(
                        std::memory_order_acquire)) {
                if (oldIndex != connection.getBucketIndex()) {
                    bm.associateBucket(cookie, bm.at(oldIndex).name);
                }
                cookie.setErrorContext("Too many bucket connections");
                return cb::engine_errc::too_many_connections;
            }
        }

        connection.setPushedClustermapRevno({});
        return cb::engine_errc::success;
    }

    if (oldIndex != connection.getBucketIndex()) {
        // try to jump back to the bucket we used to be associated
        // with..
        bm.associateBucket(cookie, bm.at(oldIndex).name);
    }
    return cb::engine_errc::no_such_key;
}

void select_bucket_executor(Cookie& cookie) {
    cookie.obtainContext<SingleStateCommandContext>(cookie, [](Cookie& c) {
              using namespace std::string_view_literals;
              using cb::tracing::Code;
              using cb::tracing::SpanStopwatch;
              ScopeTimer1<SpanStopwatch<cb::tracing::Code>> timer(
                      c, Code::SelectBucket);

              const auto bucketname{c.getRequest().getKeyString()};
              auto& connection = c.getConnection();
              cb::engine_errc code = cb::engine_errc::success;
              if (connection.isDCP()) {
                  c.setErrorContext("DCP connections cannot change bucket");
                  code = cb::engine_errc::not_supported;
              } else if (bucketname == "@no bucket@"sv) {
                  // unselect bucket!
                  BucketManager::instance().associateBucket(c, {});
              } else {
                  code = select_bucket(c, bucketname);
              }

              return SingleStateCommandContext::noPayload(code);
          }).drive();
}
