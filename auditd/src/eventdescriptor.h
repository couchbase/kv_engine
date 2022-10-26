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

#include <cstdint>
#include <string>

/**
 * The EventDescriptor class represents a single event descriptor and
 * contains the meta information for a single event.
 *
 * The current implementation ignores the mandatory and optional attributes
 */
class EventDescriptor {
public:
    EventDescriptor(uint32_t id,
                    std::string name,
                    std::string description,
                    bool filteringPermitted)
        : id(id),
          name(std::move(name)),
          description(std::move(description)),
          filteringPermitted(filteringPermitted) {
    }

    uint32_t getId() const {
        return id;
    }

    const std::string& getName() const {
        return name;
    }

    const std::string& getDescription() const {
        return description;
    }

    bool isFilteringPermitted() const {
        return filteringPermitted;
    }

protected:
    const uint32_t id;
    const std::string name;
    const std::string description;
    const bool filteringPermitted;
};
