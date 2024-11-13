/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "network_interface_manager_thread.h"
#include "network_interface_manager.h"

#include "log_macros.h"

NetworkInterfaceManagerThread::NetworkInterfaceManagerThread(
        cb::prometheus::AuthCallback authCB)
    : Couchbase::Thread("mcd:nim"), base(std::make_unique<folly::EventBase>()) {
    LOG_INFO_RAW("Starting network interface manager");
    networkInterfaceManager =
            std::make_unique<NetworkInterfaceManager>(*base, std::move(authCB));
    networkInterfaceManager->createBootstrapInterface();
}

void NetworkInterfaceManagerThread::run() {
    setRunning();
    try {
        base->loopForever();
    } catch (const std::exception& exception) {
        LOG_ERROR_CTX(
                "NetworkInterfaceManagerThread::run(): received exception",
                {"error", exception.what()});
    }

    LOG_INFO_RAW("Releasing server sockets");
    networkInterfaceManager.reset();
    LOG_INFO_RAW("Network interface manager thread stopped");
}

void NetworkInterfaceManagerThread::shutdown() {
    base->terminateLoopSoon();
}
