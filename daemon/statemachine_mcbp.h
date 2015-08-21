/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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

#include "statemachine.h"

/**
 * The state machinery for the Memcached Binary Protocol.
 */
class McbpStateMachine : public StateMachine {
public:
    McbpStateMachine(TaskFunction task)
        : StateMachine(task) {

    }

    virtual void setCurrentTask(Connection& connection,
                                TaskFunction task) override;

    virtual const char* getTaskName(TaskFunction task) const override;
};
