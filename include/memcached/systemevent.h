/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
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