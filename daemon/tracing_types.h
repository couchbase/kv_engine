/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

#include "task.h"

#include <phosphor/phosphor.h>
#include <phosphor/tools/export.h>
#include <platform/uuid.h>

#include <map>
#include <mutex>

class Connection;

/**
 * DumpContext holds all the information required for an ongoing
 * trace dump
 */
struct DumpContext {
    explicit DumpContext(std::string content)
        : content(std::move(content)),
          last_touch(std::chrono::steady_clock::now()) {
    }

    const std::string content;
    const std::chrono::steady_clock::time_point last_touch;
};

/**
 * Aggregate object to hold a map of dumps and a mutex protecting them
 */
struct TraceDumps {
    std::map<cb::uuid::uuid_t, DumpContext> dumps;
    std::mutex mutex;
};

/**
 * StaleTraceDumpRemover is a periodic task that removes old dumps
 */
class StaleTraceDumpRemover : public PeriodicTask {
public:
    StaleTraceDumpRemover(TraceDumps& traceDumps,
                          std::chrono::seconds period,
                          std::chrono::seconds max_age)
        : PeriodicTask(period), traceDumps(traceDumps), max_age(max_age) {
    }

    Status periodicExecute() override;

private:
    TraceDumps& traceDumps;
    const std::chrono::seconds max_age;
};
