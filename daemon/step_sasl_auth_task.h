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

#pragma once

#include "sasl_tasks.h"

/**
 * The StepSaslAuthTask is used to handle the next SASL
 * authentication messages
 */
class StepSaslAuthTask : public SaslAuthTask {
public:
    StepSaslAuthTask() = delete;

    StepSaslAuthTask(const StepSaslAuthTask&) = delete;

    StepSaslAuthTask(Cookie& cookie_,
                     cb::sasl::server::ServerContext& serverContext_,
                     const std::string& mechanism_,
                     const std::string& challenge_);

    Status execute() override;
    void externalResponse(cb::mcbp::Status status,
                          const std::string& payload) override;
};
