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

#include <cinttypes>
#include <stdexcept>
#include <string>

#pragma once

/// underlying size of uint32_t as this is to be stored in the Item flags field.
/// Note: The values of each enum entry are encoded into the system event key
/// which is persisted to disk.
enum class SystemEvent : uint32_t {
    /**
     * The Collection system event represents the beginning or end of a
     * collection. Each Collection system event has a key which contains the
     * collection ID. When the event is queued in a checkpoint or stored on
     * disk the seqno of that item states that this is the point when that
     * collection became accessible unless that queued/stored item is deleted,
     * then it represent when that collection became inaccessible (logically
     * deleted).
     *
     * In-use since 7.0 (epoch of SystemEvents)
     *
     */
    Collection = 0,

    /**
     * The Scope system event represents the beginning or end of a
     * scope. Each Scope system event has a key which contains the
     * Scope ID. When the event is queued in a checkpoint or stored on
     * disk the seqno of that item states that this is the point when that
     * scope became accessible unless that queued/stored item is deleted,
     * then it represent when that scope became inaccessible
     *
     * In-use since 7.0 (epoch of SystemEvents)
     *
     */
    Scope = 1,

    /**
     * The ModifyCollection system event represents a change to some mutable
     * collection meta data. The modified event must be different to the create
     * event (different key.
     *
     * Added in 7.2
     *
     */
    ModifyCollection = 2
};

static inline std::string to_string(const SystemEvent se) {
    switch (se) {
    case SystemEvent::Collection:
        return "Collection";
    case SystemEvent::Scope:
        return "Scope";
    case SystemEvent::ModifyCollection:
        return "ModifyCollection";
    }
    throw std::invalid_argument("to_string(SystemEvent) unknown " +
                                std::to_string(int(se)));
}