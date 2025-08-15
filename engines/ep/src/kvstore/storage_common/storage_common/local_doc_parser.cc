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

#include "local_doc_parser.h"

#include "flatbuffers/idl.h"

#include "local_doc_constants.h"
#include <collections/events_generated.h>
#include <collections/kvstore_generated.h>
#include <mcbp/protocol/unsigned_leb128.h>
#include <memcached/dockey_view.h>
#include <memcached/systemevent.h>
#include <iostream>

extern const std::string collections_kvstore_schema;
extern const std::string collections_vb_schema;

std::string read_collection_leb128_metadata(std::string_view buf) {
    uint64_t count = 0;
    uint64_t seqno = 0;
    uint64_t diskSize = 0;

    auto decoded1 = cb::mcbp::unsigned_leb128<uint64_t>::decode(
            {reinterpret_cast<const uint8_t*>(buf.data()), buf.size()});
    count = decoded1.first;

    if (!decoded1.second.empty()) {
        decoded1 = cb::mcbp::unsigned_leb128<uint64_t>::decode(decoded1.second);
        seqno = decoded1.first;
    }

    if (!decoded1.second.empty()) {
        decoded1 = cb::mcbp::unsigned_leb128<uint64_t>::decode(decoded1.second);
        diskSize = decoded1.first;
    }

    std::stringstream ss;
    ss << R"({"item_count":)" << count << R"(, "high_seqno":)" << seqno
       << R"(, "disk_size":)" << diskSize << "}";
    return ss.str();
}

template <class RootType>
std::pair<bool, std::string> flatbuffer_to_json(const std::string& name,
                                                const std::string& rootType,
                                                std::string_view value,
                                                const std::string& schema) {
    flatbuffers::Verifier verifier(
            reinterpret_cast<const uint8_t*>(value.data()), value.size());
    if (!verifier.VerifyBuffer<RootType>(nullptr)) {
        std::cerr << "WARNING: \"" << name << "\" root:" << rootType
                  << ", contains invalid "
                     "flatbuffers data of size:"
                  << value.size() << std::endl;
        return {false, ""};
    }

    // Use flatbuffers::Parser to generate JSON output of the binary blob
    flatbuffers::IDLOptions idlOptions;

    // Configure IDL
    // strict_json:true adds quotes to keys
    // indent_step < 0: no indent and no newlines, external tools can format
    idlOptions.strict_json = true;
    idlOptions.indent_step = -1;
    idlOptions.output_default_scalars_in_json = true;
    flatbuffers::Parser parser(idlOptions);
    parser.Parse(schema.c_str());
    parser.SetRootType(rootType.c_str());
    std::string jsongen;
    std::string ret;
    GenerateText(parser, value.data(), &ret);
    return {true, ret};
}

template <class RootType>
std::pair<bool, std::string> decode_kvstore_flatbuffer(
        const std::string& name,
        const std::string& rootType,
        std::string_view value) {
    return flatbuffer_to_json<RootType>(
            name, rootType, value, collections_kvstore_schema);
}

template <class RootType>
std::pair<bool, std::string> decode_system_event_flatbuffer(
        const std::string& name,
        const std::string& rootType,
        std::string_view value) {
    return flatbuffer_to_json<RootType>(
            name, rootType, value, collections_vb_schema);
}

std::pair<bool, std::string> maybe_decode_local_doc(std::string_view key,
                                                    std::string_view value) {
    // Check for known non-JSON meta-data documents
    if (key == LocalDocKey::openCollections) {
        return decode_kvstore_flatbuffer<Collections::KVStore::OpenCollections>(
                key.data(), "OpenCollections", value);
    }
    if (key == LocalDocKey::droppedCollections) {
        return decode_kvstore_flatbuffer<
                Collections::KVStore::DroppedCollections>(
                key.data(), "DroppedCollections", value);
    }
    if (key == LocalDocKey::openScopes) {
        return decode_kvstore_flatbuffer<Collections::KVStore::Scopes>(
                key.data(), "Scopes", value);
    }
    if (key == LocalDocKey::manifest) {
        return decode_kvstore_flatbuffer<
                Collections::KVStore::CommittedManifest>(
                key.data(), "CommittedManifest", value);
    }
    if (key.front() == '|') {
        return {true /*success*/, read_collection_leb128_metadata(value)};
    }

    // Nothing to do
    return {true /*success*/, std::string(value)};
}

std::pair<bool, std::string> maybe_decode_doc(std::string_view key,
                                              std::string_view value,
                                              bool deleted,
                                              uint8_t datatype) {
    DocKeyView docKey(key, DocKeyEncodesCollectionId::Yes);
    if (!docKey.isInSystemEventCollection() || datatype != 0) {
        // Note: once upon a time some system events came with xattr, this was
        // only for the default collection and later we stopped using xattr
        // in system events.
        return {false, std::string{}};
    }

    auto sysKey = docKey.makeDocKeyWithoutCollectionID();

    // After the system event is a second leb128 encoding the event type.
    auto [eventType, keyWithoutEvent] =
            cb::mcbp::unsigned_leb128<uint32_t>::decodeNoThrow(
                    {sysKey.data(), sysKey.size()});
    if (!keyWithoutEvent.data()) {
        return {false, std::string{}};
    }

    // switch and decode the system event to a string (stringified json)
    switch (SystemEvent(eventType)) {
    case SystemEvent::Collection: {
        if (deleted) {
            auto rv = decode_system_event_flatbuffer<
                    Collections::VB::DroppedCollection>(
                    key.data(), "DroppedCollection", value);
            return {rv.first, "dropped collection " + rv.second};
        }
    }
        [[fallthrough]];
    case SystemEvent::ModifyCollection: {
        auto rv = decode_system_event_flatbuffer<Collections::VB::Collection>(
                key.data(), "Collection", value);
        return {rv.first, "modify/create collection " + rv.second};
    }
    case SystemEvent::Scope: {
        if (deleted) {
            auto rv = decode_system_event_flatbuffer<
                    Collections::VB::DroppedScope>(
                    key.data(), "DroppedScope", value);
            return {rv.first, "dropped scope " + rv.second};
        }
        auto rv = decode_system_event_flatbuffer<Collections::VB::Scope>(
                key.data(), "Scope", value);
        return {rv.first, "scope " + rv.second};
    }
    }

    return {false, std::string{}};
}