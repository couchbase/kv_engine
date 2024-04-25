/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "diskdockey.h"
#include "ep_task.h"
#include <memcached/dockey_view.h>
#include <memcached/engine_error.h>
#include <memcached/vbucket.h>
#include <optional>
#include <vector>

class CookieIface;
class EventuallyPersistentEngine;

/// Task that fetches the requested document keys in the background
class FetchAllKeysTask : public EpTask {
public:
    FetchAllKeysTask(EventuallyPersistentEngine& e,
                     CookieIface& c,
                     const DocKeyView start_key_,
                     Vbid vbucket,
                     uint32_t count_,
                     std::optional<CollectionID> collection);

    std::string getDescription() const override {
        return description;
    }

    std::chrono::microseconds maxExpectedDuration() const override {
        // Duration will be a function of how many documents are fetched;
        // however for simplicity just return a fixed "reasonable" duration.
        return std::chrono::milliseconds(100);
    }

    bool run() override;

    /// Get the result of the operation (status and all keys)
    std::pair<cb::engine_errc, std::string_view> getResult() const {
        return {status, {keys.data(), keys.size()}};
    }

protected:
    /// The actual run method (created as a separate method to avoid
    /// duplicate the notification logic)
    cb::engine_errc doRun();

private:
    CookieIface& cookie;
    const std::string description;
    DiskDocKey start_key;
    Vbid vbid;
    uint32_t count;
    std::optional<CollectionID> collection;
    std::vector<char> keys;
    cb::engine_errc status = cb::engine_errc::success;
};
