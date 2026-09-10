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
#include <daemon/bucket_manager.h>
#include <daemon/cookie.h>

static std::expected<std::string, cb::engine_errc> list_bucket(
        Connection& connection) {
    std::string blob;
    // The blob string will contain all of the buckets, and to
    // avoid too many reallocations we should probably just reserve
    // a chunk
    blob.reserve(100);
    BucketManager::instance().forEach([&connection, &blob](auto& bucket) {
        if (bucket.type != BucketType::NoBucket &&
            connection.mayAccessBucket(bucket.name)) {
            blob.append(bucket.name);
            blob.push_back(' ');
        }
        return true;
    });

    if (!blob.empty()) {
        /* remove trailing " " */
        blob.pop_back();
    }

    return blob;
}

void list_bucket_executor(Cookie& cookie) {
    cookie.obtainContext<SingleStateCommandContext>(cookie, [](Cookie& c) {
              return list_bucket(c.getConnection());
          }).drive();
}
