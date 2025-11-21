/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

/**
 * This file contains definitions used by the core in order support
 * fusion so that we can write the code without using too many #ifdefs
 * cluttering the logic, and on deployments where we don't have magma, the
 * methods return a Status indicating that the operation is not supported.
 */
#ifdef HAVE_MAGMA_SUPPORT
#include <libmagma/magma.h>
#else
#include <nlohmann/json.hpp>
#include <string>
#include <tuple>

namespace magma {
class Status {
public:
    bool IsOK() const {
        return false;
    }
    std::string String() const {
        return "NotSupported: The system does not support this operation";
    }
};

class Magma {
public:
    using KVStoreID = uint16_t;
    static std::tuple<Status, nlohmann::json> GetFusionNamespaces(
            const std::string&,
            const std::string&,
            const std::string&,
            size_t) {
        return {{}, {}};
    }
    static Status DeleteFusionNamespace(const std::string&,
                                        const std::string&,
                                        const std::string&,
                                        const std::string&) {
        return {};
    }
    static void SetFusionMigrationRateLimit(size_t rate_limit) {
    }
    static void SetFusionSyncRateLimit(size_t rate_limit) {
    }
    static size_t GetFusionMigrationRateLimit() {
        return 0;
    }
    static size_t GetFusionSyncRateLimit() {
        return 0;
    }
    static std::tuple<Status, nlohmann::json> GetFusionStorageSnapshot(
            const std::string& fusionMetadataStoreURI,
            const std::string& fusionMetadataStoreAuthToken,
            const std::string& fusionNamespace,
            const std::vector<KVStoreID>& kvIDs,
            const std::string& snapshotUUID,
            std::chrono::time_point<std::chrono::system_clock> validTill) {
        return {{}, {}};
    }
    static Status ReleaseFusionStorageSnapshot(
            const std::string& fusionMetadataStoreURI,
            const std::string& fusionMetadataStoreAuthToken,
            const std::string& fusionNamespace,
            const std::vector<KVStoreID>& kvIDs,
            const std::string& snapshotUUID) {
        return {};
    }
    enum ThreadType {
        Flusher,
        Compactor,
        FusionUploader,
        FusionMigrator,
    };
    static void SetNumThreads(ThreadType, size_t) {
    }
    static size_t GetNumThreads(ThreadType) {
        return 0;
    }

    static void SetMaxOpenFiles(size_t, bool = false) {
    }

    static size_t GetNumOpenFiles() {
        return 0;
    }
};
} // namespace magma
#endif

/// Returns true if the system supports fusion, false otherwise.
bool isFusionSupportEnabled();

/// Returns true if the system supports magma, false otherwise.
bool isMagmaSupportEnabled();
