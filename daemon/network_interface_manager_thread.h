/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <folly/io/async/EventBase.h>
#include <platform/thread.h>
#include <statistics/prometheus.h>
#include <memory>

/**
 * The NetworkInterfaceManagerThread class runs all of the code
 * in the network manager (accept clients, reconfigure interface
 * etc). It needs to be run in its own thread as we clients may
 * be performing an operation which touch this code during
 * shutdown where the "main thread" has stopped the event loop
 * and is stopping other components.
 */
class NetworkInterfaceManagerThread : public Couchbase::Thread {
public:
    /**
     * Create a new thread which use the provided callback for
     * prometheus authentication
     *
     * @param authCB callback performing authentication
     */
    NetworkInterfaceManagerThread(cb::prometheus::AuthCallback authCB);
    NetworkInterfaceManagerThread() = delete;
    NetworkInterfaceManagerThread(const NetworkInterfaceManagerThread&) =
            delete;

    /// Request the thread to stop
    void shutdown();

protected:
    /// The main loop of the thread
    void run() override;

    /// The event base used for this thread
    std::unique_ptr<folly::EventBase> base;
};
