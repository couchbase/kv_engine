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

#include <atomic>
#include <memory>

class ConnMap;

/**
 * Connection notifier that wakes up paused connections.
 */
class ConnNotifier : public std::enable_shared_from_this<ConnNotifier> {
public:
    explicit ConnNotifier(ConnMap& cm);

    void start();

    void stop();

    void notifyMutationEvent();

    bool notifyConnections();

private:
    ConnMap& connMap;
    std::atomic<size_t> task;
    std::atomic<bool> pendingNotification;
};
