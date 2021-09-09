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

#include <collections/kvstore_generated.h>
#include <mcbp/protocol/unsigned_leb128.h>

#include <iostream>

extern const std::string collections_kvstore_schema;

std::string read_collection_leb128_metadata(std::string_view buf) {
    uint64_t count = 0;
    uint64_t seqno = 0;
    uint64_t diskSize = 0;

    auto decoded1 = cb::mcbp::unsigned_leb128<uint64_t>::decode(
            {reinterpret_cast<const uint8_t*>(buf.data()), buf.size()});
    count = decoded1.first;

    if (decoded1.second.size()) {
        decoded1 = cb::mcbp::unsigned_leb128<uint64_t>::decode(decoded1.second);
        seqno = decoded1.first;
    }

    if (decoded1.second.size()) {
        decoded1 = cb::mcbp::unsigned_leb128<uint64_t>::decode(decoded1.second);
        diskSize = decoded1.first;
    }

    std::stringstream ss;
    ss << R"({"item_count":)" << count << R"(, "high_seqno":)" << seqno
       << R"(, "disk_size":)" << diskSize << "}";
    return ss.str();
}

template <class RootType>
std::pair<bool, std::string> read_collection_flatbuffer_collections(
        const std::string& name,
        const std::string& rootType,
        std::string_view value) {
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
    parser.Parse(collections_kvstore_schema.c_str());
    parser.SetRootType(rootType.c_str());
    std::string jsongen;
    std::string ret;
    GenerateText(parser, value.data(), &ret);
    return {true, ret};
}

std::pair<bool, std::string> maybe_decode_local_doc(std::string_view key,
                                                    std::string_view value) {
    // Check for known non-JSON meta-data documents
    if (strncmp(key.data(), "_local/collections/open", key.size()) == 0) {
        return read_collection_flatbuffer_collections<
                Collections::KVStore::OpenCollections>(
                key.data(), "OpenCollections", value);
    } else if (strncmp(key.data(), "_local/collections/dropped", key.size()) ==
               0) {
        return read_collection_flatbuffer_collections<
                Collections::KVStore::DroppedCollections>(
                key.data(), "DroppedCollections", value);
    } else if (strncmp(key.data(), "_local/scope/open", key.size()) == 0) {
        return read_collection_flatbuffer_collections<
                Collections::KVStore::Scopes>(key.data(), "Scopes", value);
    } else if (strncmp(key.data(), "_local/collections/manifest", key.size()) ==
               0) {
        return read_collection_flatbuffer_collections<
                Collections::KVStore::CommittedManifest>(
                key.data(), "CommittedManifest", value);
    } else if (key.data()[0] == '|') {
        return {true /*success*/, read_collection_leb128_metadata(value)};
    }

    // Nothing to do
    return {true /*success*/, std::string(value)};
}
