/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "ep_types.h"

#include <memcached/dockey.h>
#include <memcached/vbucket.h>

/**
 * Validate the mapping of key to vBucket
 *
 * @param caller caller name logged to identify what caused the error
 * @param errorHandlingMethod How to deal with errors
 * @param key Key
 * @param vbid vBucket supplied
 * @param maxVbuckets Max vBuckets
 */
void validateKeyMapping(std::string_view caller,
                        cb::ErrorHandlingMethod errorHandlingMethod,
                        DocKey key,
                        Vbid vbid,
                        size_t maxVbuckets);
