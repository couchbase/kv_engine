/*
 *     Copyright 2017-Present Couchbase, Inc.
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

/**
 * SaslStepCommandContext is responsible for handling the
 * SASL continue command.
 */
class SaslStepCommandContext : public SaslAuthCommandContext {
public:
    explicit SaslStepCommandContext(Cookie& cookie);

protected:
    cb::engine_errc initial() override;
    cb::engine_errc handleSaslAuthTaskResult() override;

private:
    void doSaslStep();

    cb::sasl::Error error = cb::sasl::Error::FAIL;
    std::string payload;
};
