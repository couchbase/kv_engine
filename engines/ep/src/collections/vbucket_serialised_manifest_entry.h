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

class SerialisedManifestEntry;

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
    static size_t getObjectSize(uint32_t separatorLen) {
        return sizeof(SerialisedManifest) + separatorLen;
    }

    void setEntryCount(uint32_t items) {
        itemCount = items;
    }

    uint32_t getEntryCount() const {
        return itemCount;
    }

    std::string getSeparator() const {
        return {getSeparatorPtr(), separatorLen};
    }

    cb::const_char_buffer getSeparatorBuffer() const {
        return {getSeparatorPtr(), separatorLen};
    }

    /**
     * For code to locate the final entry this function is used to calculate
     * a byte offset so that getFinalManifestEntry can return the final entry
     * without any iteration.
     *
     * @param sme pointer to the final entry constructed in the serial manifest.
     */
    void calculateFinalEntryOffest(const SerialisedManifestEntry* sme) {
        finalEntryOffset =
                reinterpret_cast<const char*>(sme) - getManifestEntryBuffer();
    }

    /**
     * Return a non const pointer to where the entries can be written
     */
    char* getManifestEntryBuffer() {
        return getSeparatorPtr() + separatorLen;
    }

    /**
     * Return a const pointer from where the entries can be read
     */
    const char* getManifestEntryBuffer() const {
        return getSeparatorPtr() + separatorLen;
    }

    /**
     * Return the final entry in the SerialManifest - this entry is always the
     * entry which is being created or deleted. DCP event generation can use
     * this method to obtain the data needed to send to replicas.
     *
     * @return The entry which is being added or removed.
     */
    const SerialisedManifestEntry* getFinalManifestEntry() const {
        return reinterpret_cast<const SerialisedManifestEntry*>(
                getManifestEntryBuffer() + finalEntryOffset);
    }

    uid_t getManifestUid() const {
        return manifestUid;
    }

private:

    friend Collections::VB::Manifest;
    static SerialisedManifest* make(char* address,
                                    const std::string& separator,
                                    uid_t manifestUid,
                                    cb::char_buffer out) {
        return new (address) SerialisedManifest(separator, manifestUid, out);
    }

    /**
     * Construct a SerialisedManifest to have 0 items and the separator string.
     *
     * @param separator The separator for the manifest
     * @param out The buffer into which this object is being constructed
     * @throws length_error if the consruction would access outside of out
     */
    SerialisedManifest(const std::string& separator,
                       uid_t manifestUid,
                       cb::char_buffer out)
        : itemCount(0),
          separatorLen(separator.size()),
          finalEntryOffset(0),
          manifestUid(manifestUid) {
        if (!((out.data() + out.size()) >=
              (reinterpret_cast<char*>(this) +
               getObjectSize(separator.size())))) {
            throw std::length_error(
                    "SerialisedManifest::tryConstruction with separator.size " +
                    std::to_string(separator.size()) +
                    " exceeds the buffer of size " +
                    std::to_string(out.size()));
        }
        std::copy_n(separator.data(), separator.size(), getSeparatorPtr());
    }

    const char* getSeparatorPtr() const {
        return reinterpret_cast<const char*>(this + 1);
    }

    char* getSeparatorPtr() {
        return reinterpret_cast<char*>(this + 1);
    }

    uint32_t itemCount;
    uint32_t separatorLen;
    uint32_t finalEntryOffset;
    uid_t manifestUid;
};

class SerialisedManifestEntry {
public:
    /**
     * Object is not copyable or movable
     */
    SerialisedManifestEntry(const SerialisedManifestEntry& other) = delete;
    SerialisedManifestEntry(SerialisedManifestEntry&& other) = delete;

    static size_t getObjectSize(size_t collectionNameLen) {
        return sizeof(SerialisedManifestEntry) + collectionNameLen;
    }

    size_t getObjectSize() const {
        return getObjectSize(getCollectionNameLen());
    }

    uid_t getUid() const {
        return uid;
    }

    void setUid(uid_t uid) {
        this->uid = uid;
    }

    /**
     * @return the entry's collection name as a const char buffer
     */
    cb::const_char_buffer getCollectionName() const {
        return {getCollectionNamePtr(), size_t(collectionNameLen)};
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

    std::string toJsonCreateOrDelete(bool isCollectionDelete,
                                     int64_t correctedSeqno) const {
        if (isCollectionDelete) {
            return toJson(startSeqno, correctedSeqno);
        }
        return toJson(correctedSeqno, endSeqno);
    }

    std::string toJsonResetEnd() const {
        return toJson(startSeqno, StoredValue::state_collection_open);
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
                                         Identifier identifier,
                                         cb::char_buffer out) {
        return new (address) SerialisedManifestEntry(identifier, out);
    }

    SerialisedManifestEntry(const Collections::VB::ManifestEntry& me,
                            cb::char_buffer out) {
        tryConstruction(
                out, me.getIdentifier(), me.getStartSeqno(), me.getEndSeqno());
    }

    SerialisedManifestEntry(Identifier identifier, cb::char_buffer out) {
        tryConstruction(out, identifier, 0, StoredValue::state_collection_open);
    }

    /**
     * The collection name is stored in memory allocated after this object.
     *
     * @return pointer to the collection name, located at the address after
     *         'this'.
     */
    char* getCollectionNamePtr() {
        return reinterpret_cast<char*>(this + 1);
    }

    /**
     * The collection name is stored in memory allocated after this object.
     *
     * @return const pointer to the collection name, located at the address
     *         after 'this'.
     */
    const char* getCollectionNamePtr() const {
        return reinterpret_cast<const char*>(this + 1);
    }

    /**
     * throw an exception if the construction of a serialised object overflows
     * the memory allocation represented by out.
     *
     * @param out The buffer we are writing to
     * @param identifier The Identifier of the collection to save in this entry
     * @param startSeqno The startSeqno value to be used
     * @param endSeqno The endSeqno value to be used
     * @throws std::length_error if the function would write outside of out's
     *         bounds.
     */
    void tryConstruction(cb::char_buffer out,
                         Identifier identifier,
                         int64_t startSeqno,
                         int64_t endSeqno) {
        if (!((out.data() + out.size()) >=
              (reinterpret_cast<char*>(this) +
               getObjectSize(identifier.getName().size())))) {
            throw std::length_error(
                    "SerialisedManifestEntry::tryConstruction with collection "
                    "size " +
                    std::to_string(identifier.getName().size()) +
                    " exceeds the buffer of size " +
                    std::to_string(out.size()));
        }
        this->uid = identifier.getUid();
        this->startSeqno = startSeqno;
        this->endSeqno = endSeqno;
        this->collectionNameLen = identifier.getName().size();
        std::memcpy(getCollectionNamePtr(),
                    identifier.getName().data(),
                    this->collectionNameLen);
    }

    /**
     * Return a std::string JSON representation of this object with the callers
     * chosen startSeqno/endSeqno (based on isDelete)
     *
     * @param _startSeqno The startSeqno value to be used
     * @param _endSeqno The endSeqno value to be used
     * @return A std::string JSON object for this object.
     */
    std::string toJson(int64_t _startSeqno, int64_t _endSeqno) const {
        std::stringstream json;
        json << R"({"name":")"
             << std::string(getCollectionNamePtr(), collectionNameLen)
             << R"(","uid":")" << std::hex << uid << "\"," << std::dec
             << R"("startSeqno":")" << _startSeqno << "\","
             << R"("endSeqno":")" << _endSeqno << "\"}";
        return json.str();
    }

    int32_t collectionNameLen;
    uid_t uid;
    int64_t startSeqno;
    int64_t endSeqno;
};

static_assert(std::is_standard_layout<SerialisedManifestEntry>::value,
              "SerialisedManifestEntry must satisfy StandardLayoutType");

} // end namespace VB
} // end namespace Collections
