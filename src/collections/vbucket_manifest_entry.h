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

#pragma once

#include "stored-value.h"

#include <platform/make_unique.h>
#include <platform/sized_buffer.h>

#include <memory>

namespace Collections {
namespace VB {

/**
 * The Collections::VB::ManifestEntry stores the data a collection
 * needs from a vbucket's perspective.
 * - The Collections::Manifest revision
 * - The seqno lifespace of the collection
 *
 * Additionally this object is designed for use by Collections::VB::Manifest,
 * this is why the object stores a pointer to a std::string collection name,
 * rather than including the entire object std::string.
 * Thus a std::map can map from cb::const_char_buffer, ManifestEntry, where
 * the buffer refers to the key we own via the pointer.
 */
class ManifestEntry {
public:
    ManifestEntry(const cb::const_char_buffer& name,
                  uint32_t rev,
                  int64_t _startSeqno,
                  int64_t _endSeqno)
        : collectionName(
                  std::make_unique<std::string>(name.data(), name.size())),
          revision(rev),
          startSeqno(-1),
          endSeqno(-1) {
        // Setters validate the start/end range is valid
        setStartSeqno(_startSeqno);
        setEndSeqno(_endSeqno);
    }

    ManifestEntry(const ManifestEntry& rhs)
        : collectionName(
                  std::make_unique<std::string>(rhs.collectionName->c_str())),
          revision(rhs.revision),
          startSeqno(rhs.startSeqno),
          endSeqno(rhs.endSeqno) {
    }

    ManifestEntry(ManifestEntry&& rhs)
        : collectionName(std::move(rhs.collectionName)),
          revision(rhs.revision),
          startSeqno(rhs.startSeqno),
          endSeqno(rhs.endSeqno) {
    }

    ManifestEntry& operator=(ManifestEntry&& rhs) {
        std::swap(collectionName, rhs.collectionName);
        revision = rhs.revision;
        startSeqno = rhs.startSeqno;
        endSeqno = rhs.endSeqno;
        return *this;
    }

    ManifestEntry& operator=(const ManifestEntry& rhs) {
        collectionName.reset(new std::string(rhs.collectionName->c_str()));
        revision = rhs.revision;
        startSeqno = rhs.startSeqno;
        endSeqno = rhs.endSeqno;
        return *this;
    }

    const std::string& getCollectionName() const {
        return *collectionName;
    }

    /**
     * @return const_char_buffer initialised with address/sie of the internal
     *         collection-name string data.
     */
    cb::const_char_buffer getCharBuffer() const {
        return cb::const_char_buffer(collectionName->data(),
                                     collectionName->size());
    }

    int64_t getStartSeqno() const {
        return startSeqno;
    }

    void setStartSeqno(int64_t seqno) {
        // Enforcing that start/end are not the same, they should always be
        // separated because they represent start/end mutations.
        if (seqno < 0 || seqno <= startSeqno || seqno == endSeqno) {
            throwInvalidArg(
                    "ManifestEntry::setStartSeqno: cannot set startSeqno to " +
                    std::to_string(seqno));
        }
        startSeqno = seqno;
    }

    int64_t getEndSeqno() const {
        return endSeqno;
    }

    void setEndSeqno(int64_t seqno) {
        // Enforcing that start/end are not the same, they should always be
        // separated because they represent start/end mutations.
        if (seqno != StoredValue::state_collection_open &&
            (seqno <= endSeqno || seqno == startSeqno)) {
            throwInvalidArg(
                    "ManifestEntry::setEndSeqno: cannot set endSeqno to " +
                    std::to_string(seqno));
        }
        endSeqno = seqno;
    }

    void resetEndSeqno() {
        endSeqno = StoredValue::state_collection_open;
    }

    uint32_t getRevision() const {
        return revision;
    }

    void setRevision(uint32_t rev) {
        revision = rev;
    }

    /**
     * A collection is open when the start is greater than the end
     */
    bool isOpen() const {
        return startSeqno > endSeqno;
    }

    /**
     * A collection has deletion in process when the end is not
     * StoredValue::state_collection_open
     */
    bool isDeleting() const {
        return endSeqno != StoredValue::state_collection_open;
    }

private:
    /**
     * Throws std::invalid argument with a message that is prefix combined
     * with a call to this objects ostream operator.
     *
     * @param prefix A prefix string for the exception message.
     */
    void throwInvalidArg(const std::string& prefix);

    /**
     * An entry has a name that is heap allocated... this is due to the
     * way ManifestEntry objects are stored in the VB::Manifest
     */
    std::unique_ptr<std::string> collectionName;

    /**
     * The revision of the Collections::Manifest that this entry was added from
     */
    uint32_t revision;

    /**
     * Collection life-time is recorded as the seqno the collection was added
     * to the seqno of the point we started to delete it.
     *
     * If a collection is not being deleted then endSeqno has a value of
     * StoredValue::state_collection_open to indicate this.
     */
    int64_t startSeqno;
    int64_t endSeqno;
};

std::ostream& operator<<(std::ostream& os, const ManifestEntry& manifestEntry);

} // end namespace VB
} // end namespace Collections