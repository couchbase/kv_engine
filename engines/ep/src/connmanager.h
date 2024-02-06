/*
 *     Copyright 2010-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "configuration.h"
#include "conn_store.h"
#include "connhandler.h"
#include "connmap.h"
#include "ep_task.h"
#include "relaxed_atomic.h"
#include <chrono>

/**
 * A task to manage connections.
 */
class ConnManager : public EpTask {
public:
    using Duration = std::chrono::duration<float>;

    class ConfigChangeListener : public ValueChangedListener {
    public:
        explicit ConfigChangeListener(ConnManager& connManager)
            : connManager(connManager) {
        }

        void floatValueChanged(std::string_view key, float value) override;

    private:
        ConnManager& connManager;
    };

    ConnManager(EventuallyPersistentEngine& e, ConnMap* cmap);

    /**
     * The ConnManager task is used to run the manageConnections function
     * once a second.  This is required for two reasons:
     * (1) To clean-up dead connections
     * (2) To notify idle connections; either for connections that need to be
     * closed or to ensure dcp noop messages are sent once a second.
     */
    bool run() override;

    std::string getDescription() const override {
        return "Connection Manager";
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        // In *theory* this should run very quickly (p50 of <1ms); however
        // there's evidence it sometimes takes much longer than that.
        // Given default interval is 0.1s, consider it "slow" if it takes
        // more than 50ms - i.e. at the default interval it would consume
        // half of a core if it took 50+ms.
        return std::chrono::milliseconds(50);
    }

    cb::RelaxedAtomic<Duration> snoozeTime;

    cb::RelaxedAtomic<Duration> connectionCleanupInterval;

private:
    ConnMap* connmap;
};
