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

#include "conn_notifier.h"
#include "connmap.h"
#include "executorpool.h"
#include "globaltask.h"

#include <chrono>

/**
 * A Callback task for connection notifier
 */
class ConnNotifierCallback : public GlobalTask {
public:
    ConnNotifierCallback(EventuallyPersistentEngine* e,
                         std::shared_ptr<ConnNotifier> notifier)
        : GlobalTask(e, TaskId::ConnNotifierCallback),
          connNotifierPtr(notifier) {
    }

    bool run() override {
        auto connNotifier = connNotifierPtr.lock();
        if (connNotifier) {
            return connNotifier->notifyConnections();
        }
        /* Stop the task as connNotifier is deleted */
        return false;
    }

    std::string getDescription() const override {
        return "DCP connection notifier";
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        // In *theory* this should run very quickly (p50 of 64us); however
        // there's evidence it sometimes takes much longer than that - p99.999
        // of over 1s.
        // Set slow limit to 5s initially to highlight the worst runtimes;
        // consider reducing further when they are solved.
        return std::chrono::seconds(5);
    }

private:
    const std::weak_ptr<ConnNotifier> connNotifierPtr;
};

ConnNotifier::ConnNotifier(ConnMap& cm)
    : connMap(cm), task(0), pendingNotification(false) {
}

void ConnNotifier::start() {
    bool inverse = false;
    pendingNotification.compare_exchange_strong(inverse, true);
    ExTask connotifyTask = std::make_shared<ConnNotifierCallback>(
            &connMap.getEngine(), shared_from_this());
    task = ExecutorPool::get()->schedule(connotifyTask);
}

void ConnNotifier::stop() {
    bool inverse = true;
    pendingNotification.compare_exchange_strong(inverse, false);
    ExecutorPool::get()->cancel(task);
}

void ConnNotifier::notifyMutationEvent() {
    bool inverse = false;
    if (pendingNotification.compare_exchange_strong(inverse, true)) {
        if (task > 0) {
            ExecutorPool::get()->wake(task);
        }
    }
}

bool ConnNotifier::notifyConnections() {
    bool inverse = true;
    pendingNotification.compare_exchange_strong(inverse, false);
    connMap.processPendingNotifications();

    if (!pendingNotification.load()) {
        const double minimumSleepTime{1.0};
        ExecutorPool::get()->snooze(task, minimumSleepTime);
        if (pendingNotification.load()) {
            // Check again if a new notification is arrived right before
            // calling snooze() above.
            ExecutorPool::get()->snooze(task, 0);
        }
    }

    return true;
}
