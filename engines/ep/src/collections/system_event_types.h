/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2021 Couchbase, Inc
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

#pragma once

#include "collections/collections_types.h"

namespace Collections {

/**
 * All of the data a system event needs
 */
struct CreateEventData {
    ManifestUid manifestUid; // The Manifest which generated the event
    CollectionMetaData metaData; // The data of the new collection
};

struct DropEventData {
    ManifestUid manifestUid; // The Manifest which generated the event
    ScopeID sid; // The scope that the collection belonged to
    CollectionID cid; // The collection the event belongs to
};

struct CreateScopeEventData {
    ManifestUid manifestUid; // The Manifest which generated the event
    ScopeMetaData metaData; // The data of the new scope
};

struct DropScopeEventData {
    ManifestUid manifestUid; // The Manifest which generated the event
    ScopeID sid; // The scope the event belongs to
};

std::string to_string(const CreateEventData& event);
std::string to_string(const DropEventData& event);
std::string to_string(const CreateScopeEventData& event);
std::string to_string(const DropScopeEventData& event);

/**
 * All of the data a DCP create event message will transmit in the value of the
 * message. This is the layout to be used on the wire and is in the correct
 * byte order
 */
struct CreateEventDcpData {
    explicit CreateEventDcpData(const CreateEventData& ev)
        : manifestUid(ev.manifestUid),
          sid(ev.metaData.sid),
          cid(ev.metaData.cid) {
    }
    /// The manifest uid stored in network byte order ready for sending
    ManifestUidNetworkOrder manifestUid;
    /// The scope id stored in network byte order ready for sending
    ScopeIDNetworkOrder sid;
    /// The collection id stored in network byte order ready for sending
    CollectionIDNetworkOrder cid;
    // The size is sizeof(manifestUid) + sizeof(cid) + sizeof(sid)
    // (msvc won't allow that expression)
    constexpr static size_t size{16};
};

/**
 * All of the data a DCP create event message will transmit in the value of a
 * DCP system event message (when the collection is created with a TTL). This is
 * the layout to be used on the wire and is in the correct byte order
 */
struct CreateWithMaxTtlEventDcpData {
    explicit CreateWithMaxTtlEventDcpData(const CreateEventData& ev)
        : manifestUid(ev.manifestUid),
          sid(ev.metaData.sid),
          cid(ev.metaData.cid),
          maxTtl(htonl(gsl::narrow_cast<uint32_t>(
                  ev.metaData.maxTtl.value().count()))) {
    }
    /// The manifest uid stored in network byte order ready for sending
    ManifestUidNetworkOrder manifestUid;
    /// The scope id stored in network byte order ready for sending
    ScopeIDNetworkOrder sid;
    /// The collection id stored in network byte order ready for sending
    CollectionIDNetworkOrder cid;
    /// The collection's maxTTL value (in network byte order)
    uint32_t maxTtl;
    // The size is sizeof(manifestUid) + sizeof(cid) + sizeof(sid) +
    //             sizeof(maxTTL) (msvc won't allow that expression)
    constexpr static size_t size{20};
};

/**
 * All of the data a DCP drop event message will transmit in the value of the
 * message. This is the layout to be used on the wire and is in the correct
 * byte order
 */
struct DropEventDcpData {
    explicit DropEventDcpData(const DropEventData& data)
        : manifestUid(data.manifestUid), sid(data.sid), cid(data.cid) {
    }

    /// The manifest uid stored in network byte order ready for sending
    ManifestUidNetworkOrder manifestUid;
    /// The scope id stored in network byte order ready for sending
    ScopeIDNetworkOrder sid;
    /// The collection id stored in network byte order ready for sending
    CollectionIDNetworkOrder cid;
    // The size is sizeof(manifestUid) + sizeof(cid) (msvc won't allow that
    // expression)
    constexpr static size_t size{16};
};

/**
 * All of the data a DCP create scope event message will transmit in the value
 * of the message. This is the layout to be used on the wire and is in the
 * correct byte order
 */
struct CreateScopeEventDcpData {
    explicit CreateScopeEventDcpData(const CreateScopeEventData& data)
        : manifestUid(data.manifestUid), sid(data.metaData.sid) {
    }
    /// The manifest uid stored in network byte order ready for sending
    ManifestUidNetworkOrder manifestUid;
    /// The scope id stored in network byte order ready for sending
    ScopeIDNetworkOrder sid;
    constexpr static size_t size{12};
};

/**
 * All of the data a DCP drop scope event message will transmit in the value of
 * the message. This is the layout to be used on the wire and is in the correct
 * byte order
 */
struct DropScopeEventDcpData {
    explicit DropScopeEventDcpData(const DropScopeEventData& data)
        : manifestUid(data.manifestUid), sid(data.sid) {
    }

    /// The manifest uid stored in network byte order ready for sending
    ManifestUidNetworkOrder manifestUid;
    /// The collection id stored in network byte order ready for sending
    ScopeIDNetworkOrder sid;
    constexpr static size_t size{12};
};

} // namespace Collections