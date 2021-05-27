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

#include "sasl_auth_command_context.h"

#include <cbsasl/error.h>
#include <daemon/cookie.h>

class StartSaslAuthTask;

/**
 * SaslStartCommandContext is responsible for handling the
 * SASL auth command. Offload the work to another thread to avoid blocking
 * the front end thread while generating iterative hashes etc.
 */
class SaslStartCommandContext : public SaslAuthCommandContext {
public:
    explicit SaslStartCommandContext(Cookie& cookie);

protected:
    cb::engine_errc initial() override;
    cb::engine_errc handleSaslAuthTaskResult() override;
    std::shared_ptr<StartSaslAuthTask> task;

private:
    /// Run the SASL START (run in another thread from the folly executor
    /// and update Error and Payload
    void doSaslStart();

    cb::sasl::Error error = cb::sasl::Error::FAIL;
    std::string payload;
};
