/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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