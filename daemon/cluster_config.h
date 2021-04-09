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

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>

/**
 * A class to hold a cluster configuration object for a given bucket.
 *
 * Each configuration object contains a revision number identified by
 *
 *    "rev": number
 */
class ClusterConfiguration {
public:
    static const int NoConfiguration = -1;
    ClusterConfiguration()
        : config(std::make_shared<std::string>()), revision(NoConfiguration) {
    }

    void setConfiguration(std::string_view buffer, int rev);

    void setConfiguration(std::string_view buffer);

    /**
     * Get the current configuration.
     *
     * @return a pair where the first element is the revision number, and
     *         the second element is (a copy) of the configuration.
     */
    std::pair<int, std::shared_ptr<std::string>> getConfiguration() const {
        std::lock_guard<std::mutex> guard(mutex);
        return std::make_pair(revision, config);
    };

    /**
     * Pick out the revision number from the provided cluster configuration.
     *
     * @param buffer The cluster configuration provided by ns_server
     * @return the revision number for the cluster configuration
     */
    static int getRevisionNumber(std::string_view buffer);

    /**
     * Reset the ClusterConfig object to represent that no configuration
     * has been set.
     */
    void reset();

protected:
    /**
     * We use a mutex so that we can get a consistent copy of the revision
     * number and the configuration. (we cache the revision number to avoid
     * parsing the JSON every time we have to handle a not my vbucket reply
     * because we want to be able to avoid sending duplicates of the cluster
     * configuration map to the clients).
     */
    mutable std::mutex mutex;

    /**
     * The actual config
     */
    std::shared_ptr<std::string> config;

    /**
     * Cached revision so we don't have to parse it every time
     */
    int revision;
};
