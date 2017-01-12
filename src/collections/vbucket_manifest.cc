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

#include "collections/vbucket_manifest.h"
#include "collections/manifest.h"

#include <platform/make_unique.h>

//
// A very basic update function that will eventually need to do more with the
// VBucket (e.g queue an item and get its seqno)
//
void Collections::VB::Manifest::update(const Collections::Manifest& manifest) {
    auto changes = processManifest(manifest);

    // Now apply additions
    for (const auto& collection : changes.first) {
        addCollection(collection,
                      manifest.getRevision(),
                      fakeSeqno++ /*startseq*/,
                      StoredValue::state_collection_open);
    }
    // Now apply deletions
    for (const auto& collection : changes.second) {
        beginDelCollection(
                collection, manifest.getRevision(), fakeSeqno++ /*endseq*/);
    }
}

void Collections::VB::Manifest::addCollection(const std::string& collection,
                                              uint32_t revision,
                                              int64_t startSeqno,
                                              int64_t endSeqno) {
    std::lock_guard<WriterLock> writeLock(lock.writer());
    auto itr = map.find(collection);
    if (itr == map.end()) {
        auto m = std::make_unique<Collections::VB::ManifestEntry>(
                collection, revision, startSeqno, endSeqno);
        map.emplace(m->getCharBuffer(), std::move(m));
    } else if (!itr->second->isOpen()) {
        itr->second->setRevision(revision);
        itr->second->setStartSeqno(startSeqno);
    } else {
        std::stringstream ss;
        ss << *itr->second;
        throw std::logic_error("addCollection: cannot add collection:" +
                               collection + ", startSeqno:" +
                               std::to_string(startSeqno) + ", endSeqno:" +
                               std::to_string(endSeqno) + " " + ss.str());
    }

    if (collection == DefaultCollectionIdentifier) {
        defaultCollectionExists = true;
    }
}

void Collections::VB::Manifest::beginDelCollection(
        const std::string& collection, uint32_t revision, int64_t seqno) {
    std::lock_guard<WriterLock> writeLock(lock.writer());
    auto itr = map.find({collection.data(), collection.size()});
    if (itr != map.end()) {
        itr->second->setRevision(revision);
        itr->second->setEndSeqno(seqno);
    }

    if (collection == DefaultCollectionIdentifier) {
        defaultCollectionExists = false;
    }
}

void Collections::VB::Manifest::completeDeletion(
        const std::string& collection) {
    std::lock_guard<WriterLock> writeLock(lock.writer());
    auto itr = map.find(collection);

    if (itr == map.end()) {
        throw std::logic_error("completeDeletion: could not find collection:" +
                               collection);
    }

    if (!itr->second->isOpen()) {
        map.erase(itr);
    } else {
        itr->second->resetEndSeqno();
    }
}

Collections::VB::Manifest::processResult
Collections::VB::Manifest::processManifest(
        const Collections::Manifest& manifest) const {
    std::vector<std::string> additions, deletions;

    std::lock_guard<WriterLock> writeLock(lock.writer());
    for (auto& manifestEntry : map) {
        // Does manifestEntry::collectionName exist in manifest? NO - delete
        // time
        if (manifest.find(manifestEntry.second->getCollectionName()) ==
            manifest.end()) {
            deletions.push_back(std::string(
                    reinterpret_cast<const char*>(manifestEntry.first.data()),
                    manifestEntry.first.size()));
        }
    }

    // iterate manifest and add all non-existent collection
    for (const auto& m : manifest) {
        // if we don't find the collection, then it must be an addition.
        // if we do find a name match, then check if the collection is in the
        //  process of being deleted.
        auto itr = map.find({m.data(), m.size()});

        if (itr == map.end() || !itr->second->isOpen()) {
            additions.push_back(m);
        }
    }
    return std::make_pair(additions, deletions);
}

bool Collections::VB::Manifest::doesKeyContainValidCollection(
        const ::DocKey& key) const {
    // TODO: The separator under strict conditions *can* be changed, and by
    // another thread
    const auto cKey = Collections::DocKey::make(key, separator);

    std::lock_guard<ReaderLock> readLock(lock.reader());

    // TODO: The default collection check could be done with an atomic
    if (defaultCollectionExists &&
        cKey.getDocNamespace() == DocNamespace::DefaultCollection) {
        return true;
    } else if (cKey.getDocNamespace() == DocNamespace::Collections) {
        auto itr = map.find({reinterpret_cast<const char*>(cKey.data()),
                             cKey.getCollectionLen()});
        if (itr != map.end()) {
            return itr->second->isOpen();
        }
    }
    return false;
}

std::ostream& Collections::VB::operator<<(
        std::ostream& os, const Collections::VB::Manifest& manifest) {
    os << "VBucket::Manifest: size:" << manifest.map.size() << std::endl;
    std::lock_guard<ReaderLock> readLock(manifest.lock.reader());
    for (auto& m : manifest.map) {
        os << *m.second << std::endl;
    }
    return os;
}