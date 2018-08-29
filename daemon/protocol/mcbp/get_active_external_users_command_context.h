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

#include "steppable_command_context.h"

class Task;

/**
 * The GetActiveExternalUsersCommandContext is responsible for counting the
 * number of unique active users (skipping the couchbase internal
 * users)
 */
class GetActiveExternalUsersCommandContext : public SteppableCommandContext {
public:
    enum class State { Initial, Done };

    explicit GetActiveExternalUsersCommandContext(Cookie& cookie);

protected:
    ENGINE_ERROR_CODE step() override;

    ENGINE_ERROR_CODE initial();
    void done();

    std::shared_ptr<Task> task;
    State state;
};
