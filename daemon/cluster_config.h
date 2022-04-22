/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <folly/Synchronized.h>
#include <nlohmann/json_fwd.hpp>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>

class Connection;

class ClustermapVersion {
public:
    ClustermapVersion() : epoch(-1), revno(0) {
    }
    ClustermapVersion(int64_t epoch, int64_t revno)
        : epoch(epoch), revno(revno){};

    bool operator==(const ClustermapVersion& other) const {
        return epoch == other.epoch && revno == other.revno;
    }

    bool operator<(const ClustermapVersion& other) const {
        if (epoch < other.epoch) {
            return true;
        }
        if (epoch == other.epoch) {
            return revno < other.revno;
        }
        return false;
    }

    bool operator>(const ClustermapVersion& other) const {
        return !(*this == other || *this < other);
    }

    int64_t getEpoch() const {
        return epoch;
    }
    int64_t getRevno() const {
        return revno;
    }

    nlohmann::json to_json() const;

protected:
    int64_t epoch;
    int64_t revno;
};

/**
 * A class to hold a cluster configuration object for a given bucket.
 */
class ClusterConfiguration {
public:
    struct Configuration {
        Configuration(ClustermapVersion version, std::string_view config)
            : version(version), config(config) {
        }
        const ClustermapVersion version;
        const std::string config;
    };

    void setConfiguration(std::unique_ptr<Configuration> configuration);

    /// Get the configuration if it is newer than the provided version
    std::unique_ptr<Configuration> maybeGetConfiguration(
            const ClustermapVersion& version, bool dedupe = true) const;

    /**
     * Reset the ClusterConfig object to represent that no configuration
     * has been set.
     */
    void reset();

protected:
    folly::Synchronized<std::unique_ptr<Configuration>, std::mutex> config;
};

std::string to_string(const ClustermapVersion& version);
std::ostream& operator<<(std::ostream& os, const ClustermapVersion& version);
