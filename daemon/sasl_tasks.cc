/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "sasl_tasks.h"
#include "memcached.h"
#include <memcached/engine_error.h>

SaslAuthTask::SaslAuthTask(Cookie& cookie_,
                           cb::sasl::server::ServerContext& serverContext_,
                           std::string mechanism_,
                           std::string challenge_)
    : cookie(cookie_),
      serverContext(serverContext_),
      mechanism(std::move(mechanism_)),
      challenge(std::move(challenge_)) {
    // no more init needed
}

void SaslAuthTask::notifyExecutionComplete() {
    notifyIoComplete(cookie, cb::engine_errc::success);
}
