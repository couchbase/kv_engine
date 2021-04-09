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
enum class SystemEvent : uint32_t {
    /**
     * The Collection system event represents the beginning or end of a
     * collection. Each Collection system event has a key which contains the
     * collection ID. When the event is queued in a checkpoint or stored on
     * disk the seqno of that item states that this is the point when that
     * collection became accessible unless that queued/stored item is deleted,
     * then it represent when that collection became inaccesible (logically
     * deleted).
     *
     * A Collection system event when queued into a checkpoint carries with it
     * a value, the value is used to maintain a per vbucket JSON collection's
     * manifest (for persisted buckets).
     */
    Collection,

    /**
     * The Scope system event represents the beginning or end of a
     * scope. Each Scope system event has a key which contains the
     * Scope ID. When the event is queued in a checkpoint or stored on
     * disk the seqno of that item states that this is the point when that
     * scope became accessible unless that queued/stored item is deleted,
     * then it represent when that scope became inaccessible
     *
     */
    Scope
};

static inline std::string to_string(const SystemEvent se) {
    switch (se) {
    case SystemEvent::Collection:
        return "Collection";
    case SystemEvent::Scope:
        return "Scope";
    }
    throw std::invalid_argument("to_string(SystemEvent) unknown " +
                                std::to_string(int(se)));
}