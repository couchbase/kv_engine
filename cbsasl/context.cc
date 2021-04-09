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

#include <cbsasl/context.h>
#include <platform/uuid.h>

namespace cb::sasl {

std::string Context::getUuid() {
    if (uuid.empty()) {
        uuid = ::to_string(cb::uuid::random());
    }

    return uuid;
}
} // namespace cb::sasl
