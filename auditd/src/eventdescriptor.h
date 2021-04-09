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

#include <nlohmann/json_fwd.hpp>
#include <string>

/**
 * The EventDescriptor class represents a single event descriptor and
 * contains the meta information for a single event.
 *
 * The current implementation ignores the mandatory and optional attributes
 */
class EventDescriptor {
public:
    /**
     * Initialize an object from the JSON representation of the event
     * descriptor.
     *
     * @param root pointer to the json represenatation
     * @throws std::invalid_argument if called with a JSON representation which
     *         isn't what we expect
     */
    explicit EventDescriptor(const nlohmann::json& root);

    uint32_t getId() const {
        return id;
    }

    const std::string& getName() const {
        return name;
    }

    const std::string& getDescription() const {
        return description;
    }

    bool isSync() const {
        return sync;
    }

    bool isEnabled() const {
        return enabled;
    }

    bool isFilteringPermitted() const {
        return filteringPermitted;
    }

    void setSync(bool sync) {
        EventDescriptor::sync = sync;
    }

    void setEnabled(bool enabled) {
        EventDescriptor::enabled = enabled;
    }

protected:
    const uint32_t id;
    const std::string name;
    const std::string description;
    bool sync;
    bool enabled;
    bool filteringPermitted;
};


