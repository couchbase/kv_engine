/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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
