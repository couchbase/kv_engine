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

#include <mcbp/protocol/status.h>

/**
 * The StartSaslAuthTask is used to handle the initial SASL
 * authentication message
 */
class StartSaslAuthTask : public SaslAuthTask {
public:
    StartSaslAuthTask() = delete;

    StartSaslAuthTask(const StartSaslAuthTask&) = delete;

    StartSaslAuthTask(Cookie& cookie_,
                      Connection& connection_,
                      const std::string& mechanism_,
                      const std::string& challenge_);

    Status execute() override;

    void externalResponse(cb::mcbp::Status status,
                          const std::string& payload) override;

    std::string getUsername() const;

protected:
    Status internal_auth();
    Status external_auth();

    void successfull_external_auth();
    void unsuccessfull_external_auth(cb::mcbp::Status status,
                                     const std::string& payload);

    // Is this phase for internal or external auth
    bool internal = true;
};
