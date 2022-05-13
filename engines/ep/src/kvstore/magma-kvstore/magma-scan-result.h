/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "kvstore/kvstore_iface.h"

/**
 * MagmaScanResult expands the KVStore ScanStaus to include one extra status
 * (Next) used internally by the magma scan functions.
 */
struct MagmaScanResult {
    // See ScanStatus for definition of each status
    enum class Status {
        Success,
        Yield,
        Cancelled,
        Failed,
        Next, // scan loop must iterate to the Next key
    } code;

    MagmaScanResult(Status s) : code(s) {
    }

    static MagmaScanResult Success() {
        return MagmaScanResult(Status::Success);
    }
    static MagmaScanResult Yield() {
        return MagmaScanResult(Status::Yield);
    }
    static MagmaScanResult Cancelled() {
        return MagmaScanResult(Status::Cancelled);
    }
    static MagmaScanResult Failed() {
        return MagmaScanResult(Status::Failed);
    }
    static MagmaScanResult Next() {
        return MagmaScanResult(Status::Next);
    }

    explicit operator ScanStatus() const {
        switch (code) {
        case Status::Success:
            return ScanStatus::Success;
        case Status::Yield:
            return ScanStatus::Yield;
        case Status::Cancelled:
            return ScanStatus::Cancelled;
        case Status::Failed:
            return ScanStatus::Failed;
        case Status::Next:
        default:
            throw std::runtime_error(
                    "MagmaScanResult: cannot convert to ScanStatus");
        }
    }
};