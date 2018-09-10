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

class SerialisedManifestEntry {
public:
    /**
     * Object is not copyable or movable
     */
    SerialisedManifestEntry(const SerialisedManifestEntry& other) = delete;
    SerialisedManifestEntry(SerialisedManifestEntry&& other) = delete;

    static size_t getObjectSize() {
        return sizeof(SerialisedManifestEntry);
    }

    CollectionID getCollectionID() const {
        return cid;
    }

    void setCollectionID(CollectionID cid) {
        this->cid = cid;
    }

    ScopeID getScopeID() const {
        return sid;
    }

    void setScopeID(ScopeID sid) {
        this->sid = sid;
    }

    const SerialisedManifestEntry* nextEntry() const {
        return const_cast<SerialisedManifestEntry*>(this)->nextEntry();
    }

    SerialisedManifestEntry* nextEntry() {
        return this + 1;
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
            SerialisedManifestEntry* address,
            ScopeCollectionPair identifiers,
            const Collections::VB::ManifestEntry& me,
            cb::char_buffer out) {
        return new (address) SerialisedManifestEntry(identifiers, me, out);
    }

    static SerialisedManifestEntry* make(SerialisedManifestEntry* address,
                                         ScopeCollectionPair identifiers,
                                         cb::char_buffer out) {
        return new (address) SerialisedManifestEntry(identifiers, out);
    }

    SerialisedManifestEntry(ScopeCollectionPair identifiers,
                            const Collections::VB::ManifestEntry& me,
                            cb::char_buffer out) {
        tryConstruction(out, identifiers, me.getStartSeqno(), me.getEndSeqno());
    }

    SerialisedManifestEntry(ScopeCollectionPair identifiers,
                            cb::char_buffer out) {
        tryConstruction(
                out, identifiers, 0, StoredValue::state_collection_open);
    }

    /**
     * throw an exception if the construction of a serialised object overflows
     * the memory allocation represented by out.
     *
     * @param out The buffer we are writing to
     * @param identifiers ScopeID and CollectionID pair
     * @param endSeqno The endSeqno value to be used
     * @throws std::length_error if the function would write outside of out's
     *         bounds.
     */
    void tryConstruction(cb::char_buffer out,
                         ScopeCollectionPair identifiers,
                         int64_t startSeqno,
                         int64_t endSeqno) {
        if (!((out.data() + out.size()) >=
              (reinterpret_cast<char*>(this) + getObjectSize()))) {
            throw std::length_error(
                    "SerialisedManifestEntry::tryConstruction exceeds the "
                    "buffer of size " +
                    std::to_string(out.size()));
        }
        this->sid = identifiers.first;
        this->cid = identifiers.second;
        this->startSeqno = startSeqno;
        this->endSeqno = endSeqno;
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
        json << R"({"uid":")" << std::hex << cid << "\","
             << R"("sid":")" << std::hex << sid << "\"," << std::dec
             << R"("startSeqno":")" << _startSeqno << "\","
             << R"("endSeqno":")" << _endSeqno << "\"}";
        return json.str();
    }

    CollectionID cid;
    ScopeID sid;
    int64_t startSeqno;
    int64_t endSeqno;
};

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
    enum class Version : uint8_t { Zero = 0 };

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
     * For code to locate the final entry this function is used to calculate
     * a byte offset so that getFinalManifestEntry can return the final entry
     * without any iteration.
     *
     * @param sme pointer to the final entry constructed in the serial manifest.
     */
    void calculateFinalEntryOffest(const SerialisedManifestEntry* sme) {
        finalEntryOffset =
                reinterpret_cast<const char*>(sme) -
                reinterpret_cast<const char*>(getManifestEntryBuffer());
        // Convert byte offset into an array index
        finalEntryOffset /= sizeof(SerialisedManifestEntry);
    }

    /**
     * Return a non const pointer to where the entries can be written
     */
    SerialisedManifestEntry* getManifestEntryBuffer() {
        return reinterpret_cast<SerialisedManifestEntry*>(this + 1);
    }

    /**
     * Return a const pointer from where the entries can be read
     */
    const SerialisedManifestEntry* getManifestEntryBuffer() const {
        return reinterpret_cast<const SerialisedManifestEntry*>(this + 1);
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

    ManifestUid getManifestUid() const {
        return manifestUid;
    }

private:
    friend Collections::VB::Manifest;
    static SerialisedManifest* make(char* address,
                                    ManifestUid manifestUid,
                                    cb::char_buffer out) {
        return new (address) SerialisedManifest(manifestUid, out);
    }

    /**
     * Factory method to return a SerialisedManifest from an existing buffer.
     * validation is performed to check we are appear to be casting a valid
     * buffer
     */
    static const SerialisedManifest* make(cb::const_char_buffer in) {
        auto rv = reinterpret_cast<const SerialisedManifest*>(in.data());
        rv->validate(in); // throws for problems
        return rv;
    }

    /**
     * Construct a SerialisedManifest to have 0 items.
     *
     * @param manifestUid The UID to store
     * @param out The buffer into which this object is being constructed
     * @throws length_error if the consruction would access outside of out
     */
    SerialisedManifest(ManifestUid manifestUid, cb::char_buffer out)
        : version(Version::Zero),
          pad{},
          itemCount(0),
          finalEntryOffset(0),
          manifestUid(manifestUid) {
        if (!((out.data() + out.size()) >= (reinterpret_cast<char*>(this)))) {
            throw std::length_error(
                    "SerialisedManifest::Constructor#1"
                    " exceeds the buffer of size " +
                    std::to_string(out.size()));
        }
    }

    /**
     * Validate this object looks genuine and that the in buffer represents
     * a SerialisedManifest
     *
     * @param in The existing buffer we are casting
     * @throws length_error if in is not the correct size
     * @throws invalid_argument if version is not known
     */
    void validate(cb::const_char_buffer in) const {
        if (!((in.data() + in.size()) >=
              (reinterpret_cast<const char*>(this)))) {
            throw std::length_error(
                    "SerialisedManifest::validate"
                    " exceeds the buffer of size " +
                    std::to_string(in.size()));
        }
        if (version != Version::Zero) {
            throw std::invalid_argument(
                    "SerialisedManifest::validate"
                    " version mismatch version:" +
                    std::to_string(uint32_t(version)));
        }
    }

    Version version;
    uint8_t pad[3];
    uint32_t itemCount;
    uint32_t finalEntryOffset;
    ManifestUid manifestUid;
};

static_assert(std::is_standard_layout<SerialisedManifestEntry>::value,
              "SerialisedManifestEntry must satisfy StandardLayoutType");

} // end namespace VB
} // end namespace Collections
