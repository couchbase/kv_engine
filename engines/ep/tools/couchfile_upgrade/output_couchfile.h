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

#pragma once

#include "couchfile.h"

#include <include/memcached/dockey.h>

namespace Collections {

class InputCouchFile;

class OutputCouchFile : public CouchFile {
public:
    /**
     * @param newCollection - the CollectionID to assign all documents to.
     */
    OutputCouchFile(OptionsSet options,
                    const std::string& filename,
                    CollectionID newCollection);

    void commit() const;

    void processDocument(const Doc* doc, const DocInfo* docinfo) const;

    void setVBState(const std::string& inputVBS);

    /**
     * Write to the output file that an upgrade has begun
     * adds "collections_supported:false" to the _local vbstate
     */
    void writeUpgradeBegin(const InputCouchFile& input) const;

    /**
     * Write to the output file that an upgrade has complete
     * adds "collections_supported:true" to the _local vbstate
     */
    void writeUpgradeComplete(const InputCouchFile& input) const;

    void writeDocument(const Doc* doc, const DocInfo* docinfo) const;

protected:
    /**
     * Given a document, move it into the collection as specified by cid
     */
    std::string moveDocToCollection(const sized_buf in, CollectionID cid) const;

    void writeLocalDocument(const std::string& documentName,
                            const std::string& value) const;

    void writeSupportsCollections(const std::string& vbs, bool value) const;

    /// The destination collection to use in the upgrade
    CollectionID collection;
};
} // end namespace Collections