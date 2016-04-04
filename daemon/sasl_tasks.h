/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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

#include "task.h"
#include <cbsasl/cbsasl.h>
#include <string>


class Connection;
class Cookie;

/**
 * The SaslAuthTask is the abstract base class used during SASL
 * authentication (which is being run by the executor service)
 */
class SaslAuthTask : public Task {
public:
    SaslAuthTask() = delete;

    SaslAuthTask(const SaslAuthTask&) = delete;

    SaslAuthTask(Cookie& cookie_,
                 Connection& connection_,
                 const std::string& mechanism_,
                 const std::string& challenge_);

    virtual void notifyExecutionComplete() override;


    cbsasl_error_t getError() const {
        return error;
    }

    const char* getResponse() const {
        return response;
    }

    unsigned int getResponse_length() const {
        return response_length;
    }

protected:
    Cookie& cookie;
    Connection& connection;
    std::string mechanism;
    std::string challenge;
    cbsasl_error_t error;
    const char* response;
    unsigned int response_length;
};

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

    virtual bool execute() override;
};

/**
 * The StepSaslAuthTask is used to handle the next SASL
 * authentication messages
 */
class StepSaslAuthTask : public SaslAuthTask {
public:
    StepSaslAuthTask() = delete;

    StepSaslAuthTask(const StepSaslAuthTask&) = delete;

    StepSaslAuthTask(Cookie& cookie_,
                     Connection& connection_,
                     const std::string& mechanism_,
                     const std::string& challenge_);

    virtual bool execute() override;
};
