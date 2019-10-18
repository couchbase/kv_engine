/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include "environment.h"

#include <gsl.h>
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
