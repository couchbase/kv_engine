/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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

#include "output_couchfile.h"
#include "input_couchfile.h"

#include <mcbp/protocol/unsigned_leb128.h>
#include <nlohmann/json.hpp>

#include <iostream>

namespace Collections {

OutputCouchFile::OutputCouchFile(OptionsSet options,
                                 const std::string& filename,
                                 CollectionID newCollection)
    : CouchFile(options, filename, COUCHSTORE_OPEN_FLAG_CREATE),
      collection(newCollection) {
}

void OutputCouchFile::commit() const {
    auto errcode = couchstore_commit(db);
    if (errcode) {
        throw std::runtime_error(
                "OutputCouchFile::commit couchstore_commit failed errcode:" +
                std::to_string(errcode));
    }
    verbose("commit");
}

// Moving a document to a collection in the context of this upgrade is to
// prefix the key with a unsigned_leb128 collection-id (cid)
std::string OutputCouchFile::moveDocToCollection(const sized_buf in,
                                                 CollectionID cid) const {
    cb::mcbp::unsigned_leb128<CollectionIDType> encodedCollection(cid);
    std::string rv(in.size + encodedCollection.size(), ' ');
    auto next =
            std::copy_n(reinterpret_cast<const char*>(encodedCollection.data()),
                        encodedCollection.size(),
                        rv.begin());
    std::copy(in.buf, in.buf + in.size, next);
    return rv;
}

void OutputCouchFile::processDocument(const Doc* doc,
                                      const DocInfo* docinfo) const {
    auto newName = moveDocToCollection(doc->id, collection);
    Doc newDoc = *doc;
    DocInfo newDocInfo = *docinfo;
    newDoc.id.buf = const_cast<char*>(newName.data());
    newDoc.id.size = newName.size();
    newDocInfo.id = newDoc.id;

    writeDocument(&newDoc, &newDocInfo);
}

void OutputCouchFile::writeDocument(const Doc* doc,
                                    const DocInfo* docinfo) const {
    auto errcode = couchstore_save_document(
            db,
            doc,
            const_cast<DocInfo*>(docinfo),
            COMPRESS_DOC_BODIES | COUCHSTORE_SEQUENCE_AS_IS);

    if (errcode) {
        throw std::runtime_error(
                "OutputCouchFile::writeDocument couchstore_save_document "
                "errcode:" +
                std::to_string(errcode));
    }

    verbose("writeDocument(" + std::string(doc->id.buf, doc->id.size) +
            ", db_seq:" + std::to_string(docinfo->db_seq) + ", rev_seq:" +
            std::to_string(docinfo->rev_seq));
}

void OutputCouchFile::setVBState(const std::string& inputVBS) {
    writeLocalDocument("_local/vbstate", inputVBS);
}

void OutputCouchFile::writeLocalDocument(const std::string& documentName,
                                         const std::string& value) const {
    LocalDoc localDoc;
    localDoc.id.buf = const_cast<char*>(documentName.c_str());
    localDoc.id.size = documentName.size();
    localDoc.json.buf = const_cast<char*>(value.c_str());
    localDoc.json.size = value.size();
    localDoc.deleted = 0;

    auto errcode = couchstore_save_local_document(db, &localDoc);
    if (errcode) {
        throw std::runtime_error(
                "OutputCouchFile::writeLocalDocument failed "
                "couchstore_open_local_document documentName:" +
                documentName + " value:" + value + " errcode:" +
                std::to_string(errcode));
    }
    verbose("writeLocalDocument(" + documentName + ", " + value + ") success");
}

void OutputCouchFile::writeUpgradeBegin(const InputCouchFile& input) const {
    writeSupportsCollections(input.getLocalDocument("_local/vbstate"), false);
}

void OutputCouchFile::writeUpgradeComplete(const InputCouchFile& input) const {
    writeSupportsCollections(input.getLocalDocument("_local/vbstate"), true);
}

void OutputCouchFile::writeSupportsCollections(const std::string& vbs,
                                               bool value) const {
    nlohmann::json json;
    try {
        json = nlohmann::json::parse(vbs);
    } catch (const nlohmann::json::exception& e) {
        throw std::invalid_argument(
                "OutputCouchFile::writePartiallyNamespaced cannot parse "
                " json:" +
                vbs + " exception:" + e.what());
    }
    json[CollectionsSupportedKey] = value;
    std::cout << json.dump() << std::endl;
    writeLocalDocument("_local/vbstate", json.dump());
}
} // end namespace Collections
