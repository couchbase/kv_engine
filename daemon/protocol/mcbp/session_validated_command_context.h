/*
 *     Copyright 2021 Couchbase, Inc.
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

#include "steppable_command_context.h"

/**
 * The SessionValidatedCommandContext is a state machine used by all commands
 * which require the session control context to be locked during the duration
 * of their operation (compactDB, Set/DelVbucket to name a few).
 */
class SessionValidatedCommandContext : public SteppableCommandContext {
public:
    explicit SessionValidatedCommandContext(Cookie& cookie);
    ~SessionValidatedCommandContext() override;

protected:
    ENGINE_ERROR_CODE step() override;

private:
    /// Set to true if the CAS for the connection is legal and we've registered
    /// the session control counter
    const bool valid;
};
