/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <memcached/vbucket.h>
#include <nlohmann/json_fwd.hpp>
#include <filesystem>
#include <optional>

class StatCollector;

namespace cb::snapshot {

enum class FileStatus {
    Present, // file exists
    Absent, // file !exists
    Truncated // file exists but smaller than size
};

std::string format_as(FileStatus status);

/// The information tracked for a given file in the snapshot
struct FileInfo {
    FileInfo() = default;
    FileInfo(std::filesystem::path p,
             std::size_t s,
             std::size_t i,
             std::string sha512 = {})
        : path(std::move(p)), size(s), id(i), sha512(std::move(sha512)) {
        // Empty
    }
    /// The relative path of the file within the snapshot
    std::filesystem::path path;
    /// The size of the file within the snapshot which is considered
    /// as the size of the file (the actual file size may exceed this size,
    /// but the additional bytes is not considered as part of the snapshot)
    std::size_t size = 0;
    /// A number identifying this file within the snapshot
    std::size_t id = 0;
    /// An optional SHA-512
    std::string sha512;
    /// status of this file - assume exists unless detected otherwise
    FileStatus status{FileStatus::Present};

    bool operator==(const FileInfo&) const = default;

    /// Add the state of this FileInfo to the collector
    void addDebugStats(std::string_view label, const StatCollector&) const;
};

/// The snapshot manifest
struct Manifest {
    Manifest(const nlohmann::json& json);

    Manifest(Vbid vbid, std::string_view uuid) : vbid(vbid), uuid(uuid) {
    }
    /// The vbucket this snapshot belongs to
    Vbid vbid;
    /// The uuid of the snapshot used to separate two different snapshots
    /// for the same vbucket from eachother
    std::string uuid;
    /// A vector of files containing the database files in the snapshot
    std::vector<FileInfo> files;
    /// A vector of files containing the data encryption keys used within the
    /// snapshot
    std::vector<FileInfo> deks;

    // A status to say if the snapshot has all files or is missing a file or
    // has a truncated file. An incomplete snapshot from the POV of a download
    // could in theory be made complete (resumed).
    enum class Status : uint8_t { Complete, Incomplete };
    Status status{Status::Complete};

    bool operator==(const Manifest&) const = default;

    /// Add the state of this Manifest to the collector
    void addDebugStats(const StatCollector&) const;
    void addDekStats(const StatCollector&) const;
    void addUuidStat(const StatCollector&) const;

    void setIncomplete() {
        status = Status::Incomplete;
    }

    // @return the status for use in snapshot-status stat command
    std::string_view getStatus() const {
        // Use available for complete - as a complete snapshot is available for
        // being used in subsequent FBR steps.
        return status == Status::Complete ? "available" : "incomplete";
    }
};

void to_json(nlohmann::json& json, const FileInfo& info);
void from_json(const nlohmann::json& json, FileInfo& info);
void to_json(nlohmann::json& json, const Manifest& manifest);
void from_json(const nlohmann::json& json, Manifest& manifest);

} // namespace cb::snapshot
