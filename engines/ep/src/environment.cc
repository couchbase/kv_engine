/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "environment.h"

#include <gsl/gsl-lite.hpp>
#include <cstdint>

Environment& Environment::get() {
    static Environment env;
    return env;
}

size_t Environment::getMaxBackendFileDescriptors() const {
    // Better to crash here than return a bad number if we haven't set up our
    // env yet.
    Expects(engineFileDescriptors > 0);

    // We always support Couchstore as a backend
    uint8_t backends = 1;

    // Add 1 for each backend this build supports.
#ifdef EP_USE_MAGMA
    backends++;
#endif

#ifdef EP_USE_ROCKSDB
    backends++;
#endif

    auto backendFileDescriptors =
            engineFileDescriptors - reservedFileDescriptors;

    // For now we will share our file descriptors equally between all backends.
    // @TODO We might want to make this configurable so a user could choose how
    // to share FDs if they have both magma and couchstore buckets.
    return backendFileDescriptors / backends;
}
