/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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

    std::string getDescription() override {
        return "DCP connection notifier";
    }

    std::chrono::microseconds maxExpectedDuration() override {
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
