/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "dockey_validator.h"

#include "bucket_logger.h"
#include "error_handler.h"

#include <utilities/logtags.h>

extern "C" {
#include "crc32.h"
}

void validateKeyMapping(std::string_view caller,
                        cb::ErrorHandlingMethod errorHandlingMethod,
                        DocKey key,
                        Vbid vbid,
                        size_t maxVbuckets) {
    auto crc = crc32buf((uint8_t*)key.data(), key.size());
    auto hashedVb = Vbid(((crc >> 16) & 0x7fff) % maxVbuckets);
    if (hashedVb != vbid) {
        cb::handleError(
                *getGlobalBucketLogger(),
                spdlog::level::warn,
                fmt::format("{}: Key {} supplied with vBucket {}. Correct "
                            "vBucket is {}",
                            caller,
                            cb::UserData(key.to_string()),
                            vbid,
                            hashedVb),
                errorHandlingMethod);
    }
}
