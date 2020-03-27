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

#include <engines/ep/src/collections/collections_types.h>
#include <include/memcached/dockey.h>

namespace Collections {

namespace VB {
struct PersistedStats;
}

class InputCouchFile;

class OutputCouchFile : public CouchFile {
public:
    /**
     * @param newCollection - the CollectionID to assign all documents to.
     */
    OutputCouchFile(OptionsSet options,
                    const std::string& filename,
                    CollectionID newCollection,
                    size_t maxBufferedSize);

    void commit();

    void processDocument(const Doc* doc, const DocInfo& docinfo);

    /**
     * Write to the output file that an upgrade has begun
     * adds "namespaces_supported:false" to the _local vbstate
     */
    void writeUpgradeBegin(const InputCouchFile& input) const;

    /**
     * Write to the output file that an upgrade has complete
     * adds "namespaces_supported:true" to the _local vbstate
     */
    void writeUpgradeComplete(const InputCouchFile& input) const;

    /**
     * write all documents that are in the BufferedOutputDocuments
     */
    void writeDocuments();

protected:
    /**
     * Given a document, move it into the collection as specified by cid
     */
    std::string moveDocToCollection(const sized_buf in, CollectionID cid) const;

    void writeLocalDocument(const std::string& documentName,
                            const std::string& value) const;

    void writeSupportsNamespaces(const std::string& vbs, bool value) const;

    /**
     * Update item of the new file, this creates the _local counter doc
     */
    void setCollectionStats(CollectionID cid,
                            Collections::VB::PersistedStats stats) const;

    /// The destination collection to use in the upgrade
    CollectionID collection;

    /**
     * Inner class for storing the documents ready for buffered writing, the
     * buffer itself tracks an approx memory usage and returns true from
     * addDocument if that memory usage exceeds the maxBufferedSize used in
     * construction.
     */
    class BufferedOutputDocuments {
    public:
        BufferedOutputDocuments(size_t maxBufferedSize);

        /**
         * Add a document to the buffer and return true if the buffer should
         * now be written to disk
         * @param newDocKey Key to use instead of the 'id' from DocInfo/Doc
         * @param doc the Doc which can be null for documents with no body
         * @param docInfo the document's meta
         */
        bool addDocument(const std::string& newDocKey,
                         const Doc* doc,
                         const DocInfo& docInfo);

        /**
         * the couchstore_save_docs expects a plain array of Doc* and DocInfo*
         * This method is called just before we use couchstore_save_docs and
         * creates arrays of Doc* and DocInfo* from the internal Document
         * buffers
         */
        void prepareForWrite();

        Doc** getDocs() {
            return outputDocs.data();
        }

        DocInfo** getDocInfos() {
            return outputDocInfos.data();
        }

        size_t size() const {
            return outputDocuments.size();
        }

        void reset() {
            outputDocs.clear();
            outputDocInfos.clear();
            outputDocuments.clear();
            approxBufferedSize = 0;
        }

        bool empty() const {
            return outputDocuments.empty();
        }

    private:
        struct Document {
            Document(std::string newDocKey,
                     const Doc* doc,
                     const DocInfo& docInfo);
            ~Document();

            Document(const Document& other) = delete;
            Document(Document&& other);

            std::string newDocKey;
            std::vector<char> revMeta;
            Doc newDoc;
            DocInfo newDocInfo;
            const Doc* doc;
        };

        std::vector<Doc*> outputDocs;
        std::vector<DocInfo*> outputDocInfos;
        std::vector<Document> outputDocuments;
        size_t approxBufferedSize = 0;
        size_t maxBufferedSize = 0;
    } bufferedOutput;
};
} // end namespace Collections