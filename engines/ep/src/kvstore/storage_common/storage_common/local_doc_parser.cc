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

couchstore_error_t read_collection_leb128_metadata(const sized_buf* v,
                                                   std::string& out) {
    uint64_t count = 0;
    uint64_t seqno = 0;
    uint64_t diskSize = 0;

    auto decoded1 = cb::mcbp::unsigned_leb128<uint64_t>::decode(
            {reinterpret_cast<uint8_t*>(v->buf), v->size});
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
    out = ss.str();

    return COUCHSTORE_SUCCESS;
}

template <class RootType>
couchstore_error_t read_collection_flatbuffer_collections(
        const std::string& name,
        const std::string& rootType,
        const sized_buf* v,
        std::string& out) {
    flatbuffers::Verifier verifier(reinterpret_cast<uint8_t*>(v->buf), v->size);
    if (!verifier.VerifyBuffer<RootType>(nullptr)) {
        std::cerr << "WARNING: \"" << name << "\" root:" << rootType
                  << ", contains invalid "
                     "flatbuffers data of size:"
                  << v->size << std::endl;
        ;
        return COUCHSTORE_ERROR_CORRUPT;
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
    GenerateText(parser, v->buf, &out);
    return COUCHSTORE_SUCCESS;
}

couchstore_error_t maybe_decode_local_doc(const sized_buf* id,
                                          const sized_buf* v,
                                          std::string& decodedData) {
    // Check for known non-JSON meta-data documents
    if (strncmp(id->buf, "_local/collections/open", id->size) == 0) {
        return read_collection_flatbuffer_collections<
                Collections::KVStore::OpenCollections>(
                id->buf, "OpenCollections", v, decodedData);
    } else if (strncmp(id->buf, "_local/collections/dropped", id->size) == 0) {
        return read_collection_flatbuffer_collections<
                Collections::KVStore::DroppedCollections>(
                id->buf, "DroppedCollections", v, decodedData);
    } else if (strncmp(id->buf, "_local/scope/open", id->size) == 0) {
        return read_collection_flatbuffer_collections<
                Collections::KVStore::Scopes>(
                id->buf, "Scopes", v, decodedData);
    } else if (strncmp(id->buf, "_local/collections/manifest", id->size) == 0) {
        return read_collection_flatbuffer_collections<
                Collections::KVStore::CommittedManifest>(
                id->buf, "CommittedManifest", v, decodedData);
    } else if (id->buf[0] == '|') {
        return read_collection_leb128_metadata(v, decodedData);
    }

    // Nothing todo
    return COUCHSTORE_SUCCESS;
}
