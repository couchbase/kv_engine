/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <memcached/server_core_iface.h>
#include <platform/byte_literals.h>
#include <platform/cb_time.h>

#include <chrono>
#include <stdexcept>
#include <string>

/**
 * Implementation of ServerCoreIface for unit tests.
 *
 * In unit tests time stands still, to give deterministic behaviour.
 *
 * Tests which need different behaviour for a subset of members (e.g. the
 * time functions) can subclass this and override just those members rather
 * than implementing the whole (pure virtual) ServerCoreIface.
 */
class UnitTestServerCore : public ServerCoreIface {
public:
    cb::time::steady_clock::time_point get_uptime_now() override {
        // Return a fixed time point of 0.
        return cb::time::steady_clock::time_point(std::chrono::seconds(0));
    }

    rel_time_t get_current_time() override {
        // Return a fixed time of '0'.
        return 0;
    }

    rel_time_t realtime(rel_time_t exptime) override {
        throw std::runtime_error(
                "UnitTestServerCore::realtime() not implemented");
    }

    time_t abstime(rel_time_t reltime) override {
        return get_current_time() + reltime;
    }

    uint32_t limit_expiry_time(uint32_t t,
                               std::chrono::seconds limit) override {
        throw std::runtime_error(
                "UnitTestServerCore::limit_expiry_time() not implemented");
    }

    size_t getQuotaSharingPagerConcurrency() override {
        return 2;
    }

    std::chrono::milliseconds getQuotaSharingPagerSleepTime() override {
        using namespace std::chrono_literals;
        return 5000ms;
    }

    std::chrono::seconds getDcpDisconnectWhenStuckTimeout() override {
        using namespace std::chrono_literals;
        return 720s;
    }

    std::string getDcpDisconnectWhenStuckNameRegex() override {
        return {}; // empty disables the feature
    }

    bool getNotLockedReturnsTmpfail() override {
        return false;
    }

    double getDcpConsumerMaxMarkerVersion() override {
        return 2.2;
    }

    bool isDcpSnapshotMarkerHPSEnabled() override {
        return true;
    }

    bool isDcpSnapshotMarkerPurgeSeqnoEnabled() override {
        return true;
    }

    bool isSyncWritesReturnCommittedSeqno() override {
        return true;
    }

    bool isMagmaBlindWriteOptimisationEnabled() override {
        return true;
    }

    bool isFileFragmentChecksumEnabled() const override {
        return true;
    }

    size_t getFileFragmentChecksumLength() const override {
        // Use a small chunk size in testing which may help improve test
        // coverage
        return 128;
    }

    bool shouldPrepareSnapshotAlwaysChecksum() const override {
        return true;
    }

    size_t getSnapshotDownloadFsyncInterval() const override {
        return 50_MiB;
    }

    size_t getSnapshotDownloadWriteSize() const override {
        return 2_MiB;
    }

    size_t getSnapshotDownloadThrottleBytes() const override {
        return 0;
    }

    cb::io::IoHint getSnapshotDownloadFadvise() const override {
        return cb::io::IoHint::Normal;
    }
};
