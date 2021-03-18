/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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

class Bucket;

/**
 * The CccpNotificationTask is responsible for walking all of the connections
 * and set a flag that they need to ship the new CCCP config to the client
 * when the client goes back to idle state (and notify idle clients)
 */
class CccpNotificationTask : public Task {
public:
    CccpNotificationTask() = delete;
    CccpNotificationTask(const CccpNotificationTask&) = delete;

    CccpNotificationTask(Bucket& bucket_, int revision_);

    ~CccpNotificationTask() override;

    Status execute() override;

protected:
    /// The bucket we're operating on
    Bucket& bucket;

    /// The revision of the new config
    const int revision;
};