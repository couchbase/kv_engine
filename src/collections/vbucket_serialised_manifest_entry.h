/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

#include "collections/vbucket_manifest_entry.h"
#include "config.h"

#include <functional>

#pragma once

namespace Collections {
namespace VB {

/**
 * A VB::Manifest is serialised into an Item when it is updated. The
 * Item carries a copy of the VB::Manifest down to the flusher so that
 * a JSON version can be written to persistent storage.
 *
 * The serialised data is created by VB::Manifest and is a copy of the
 * manifest *before* the update is applied. The update being applied to the
 * manifest is serialised as the final entry. This is done because the seqno
 * of the final entry needs correcting during the creation of the JSON. This
 * occurs because the Item is created but cannot be allocated a seqno until it
 * is queued, and it is not safe to mutate the Item's value after it is queued.
 *
 */

/**
 * SerialisedManifest just stores the entry count and can return a pointer
 * to the buffer where entries can be written.
 */
class SerialisedManifest {
public:
    static size_t getObjectSize() {
        return sizeof(SerialisedManifest);
    }

    void setEntryCount(uint32_t items) {
        itemCount = items;
    }

    uint32_t getEntryCount() const {
        return itemCount;
    }

    /**
     * Return a non const pointer to where the entries can be written
     */
    char* getManifestEntryBuffer() {
        return reinterpret_cast<char*>(this + 1);
    }

    /**
     * Return a const pointer from where the entries can be read
     */
    const char* getManifestEntryBuffer() const {
        return reinterpret_cast<const char*>(this + 1);
    }

private:
    uint32_t itemCount;
};

class SerialisedManifestEntry {
public:

    static size_t getObjectSize(size_t collectionNameLen) {
        return sizeof(SerialisedManifestEntry) - 1 + collectionNameLen;
    }

    size_t getObjectSize() const {
        return getObjectSize(getCollectionNameLen());
    }

    void setRevision(uint32_t rev) {
        revision = rev;
    }

    size_t getCollectionNameLen() const {
        return collectionNameLen;
    }

    const char* nextEntry() const {
        return reinterpret_cast<const char*>(this) + getObjectSize();
    }

    char* nextEntry() {
        return const_cast<char*>(
                static_cast<const SerialisedManifestEntry*>(this)->nextEntry());
    }

    std::string toJson() const {
        return toJson(startSeqno, endSeqno);
    }

    std::string toJson(SystemEvent se, int64_t correctedSeqno) const {
        switch (se) {
        case SystemEvent::BeginDeleteCollection:
            return toJson(startSeqno, correctedSeqno);
        case SystemEvent::CreateCollection:
            return toJson(correctedSeqno, endSeqno);
        case SystemEvent::DeleteCollectionHard:
            return std::string(); // return nothing - collection gone
        case SystemEvent::DeleteCollectionSoft:
            return toJson(startSeqno, StoredValue::state_collection_open);
        }

        throw std::invalid_argument(
                "SerialisedManifestEntry::toJson invalid event " +
                to_string(se));
        return {};
    }

private:
    friend Collections::VB::Manifest;
    static SerialisedManifestEntry* make(
            char* address,
            const Collections::VB::ManifestEntry& me,
            cb::char_buffer out) {
        return new (address) SerialisedManifestEntry(me, out);
    }

    static SerialisedManifestEntry* make(char* address,
                                         int32_t revision,
                                         cb::const_char_buffer collection,
                                         cb::char_buffer out) {
        return new (address) SerialisedManifestEntry(revision, collection, out);
    }

    SerialisedManifestEntry(const Collections::VB::ManifestEntry& me,
                            cb::char_buffer out) {
        tryConstruction(out,
                        me.getRevision(),
                        me.getStartSeqno(),
                        me.getEndSeqno(),
                        me.getCollectionName());
    }

    SerialisedManifestEntry(int revision,
                            cb::const_char_buffer collection,
                            cb::char_buffer out) {
        tryConstruction(out,
                        revision,
                        0,
                        StoredValue::state_collection_open,
                        collection);
    }

    /**
     * throw an exception if the construction of a serialised object overflows
     * the memory allocation represented by out.
     *
     * @param out The buffer we are writing to
     * @param entryData The struct to update (which should be enclosed by out)
     * @param revision Collections::Manifest revision that got us here
     * @param startSeqno The startSeqno value to be used
     * @param endSeqno The endSeqno value to be used
     * @param collection The name of the collection to copy-in
     */
    void tryConstruction(cb::char_buffer out,
                         uint32_t revision,
                         int64_t startSeqno,
                         int64_t endSeqno,
                         cb::const_char_buffer collection) {
        if (!((out.data() + out.size()) >=
              (reinterpret_cast<char*>(this) +
               getObjectSize(collection.size())))) {
            throw std::length_error("tryConstruction with collection size " +
                                    std::to_string(collection.size()) +
                                    " exceeds the buffer of size " +
                                    std::to_string(out.size()));
        }
        this->revision = revision;
        this->startSeqno = startSeqno;
        this->endSeqno = endSeqno;
        this->collectionNameLen = collection.size();
        std::memcpy(this->collectionName,
                    collection.data(),
                    this->collectionNameLen);
    }

    /**
     * Return a std::string JSON representation of this object with the callers
     * chosen startSeqno/endSeqno (based on isDelete)
     *
     * @param _startSeqno The startSeqno value to be used
     * @param _endSeqno The endSeqno value to be used
     */
    std::string toJson(int64_t _startSeqno, int64_t _endSeqno) const {
        std::string json =
                R"({"name":")" +
                std::string(collectionName, collectionNameLen) +
                R"(","revision":")" + std::to_string(revision) + "\"," +
                R"("startSeqno":")" + std::to_string(_startSeqno) + "\"," +
                R"("endSeqno":")" + std::to_string(_endSeqno) + "\"}";
        return json;
    }

    uint32_t revision;
    int64_t startSeqno;
    int64_t endSeqno;
    int32_t collectionNameLen;
    char collectionName[1];
};

static_assert(std::is_standard_layout<SerialisedManifestEntry>::value,
              "SerialisedManifestEntry must satisfy StandardLayoutType");

} // end namespace VB
} // end namespace Collections