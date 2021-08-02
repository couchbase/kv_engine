/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "collections/vbucket_manifest.h"
#include "collections/vbucket_manifest_handles.h"

#include "kvshard.h"
#include "kvstore/kvstore.h"

#include "stats.h"
#include "tests/module_tests/collections/stat_checker.h"
#include "tests/module_tests/vbucket_utils.h"

#include "vbucket.h"
#include "vbucket_fwd.h"

#include <folly/portability/GMock.h>
#include <utilities/test_manifest.h>

StatChecker::StatChecker(
        VBucketPtr& vb,
        const CollectionEntry::Entry& entry,
        std::function<size_t(VBucketPtr&, CollectionID)> getter,
        std::string statName,
        PostFunc postCondition)
    : vb(vb),
      entry(entry),
      getter(std::move(getter)),
      statName(std::move(statName)),
      postCondition(std::move(postCondition)) {
    initialValue = getValue();
}

StatChecker::~StatChecker() {
    auto newValue = getValue();
    EXPECT_TRUE(postCondition(newValue, initialValue))
            << statName << " for collection: " << entry.name
            << " did not meet expected condition. "
            << "Was: " << initialValue << " New: " << newValue;

    if (!vb->getShard()) {
        // ephemeral has no Shard/KVStore, no check needed for memory vs disk
        return;
    }
    auto kvs = vb->getShard()->getRWUnderlying();
    auto fileHandle = kvs->makeFileHandle(vb->getId());
    EXPECT_TRUE(fileHandle);
    auto stats = kvs->getCollectionStats(*fileHandle, entry.uid);
    EXPECT_TRUE(stats.first);

    // Regardless of the postCondition the following should all be equal
    auto inMemoryStats = vb->getManifest().lock(entry.uid);
    EXPECT_EQ(stats.second.diskSize, inMemoryStats.getDiskSize());
    EXPECT_EQ(stats.second.itemCount, inMemoryStats.getItemCount());
}

size_t StatChecker::getValue() {
    // uses a provided getter func rather than a virtual method
    // as it needs to be called in the base type constructor
    // and destructor
    return getter(vb, entry.uid);
}

size_t getCollectionMemUsed(VBucketPtr& vb, CollectionID cid) {
    const auto& stats = VBucketTestIntrospector::getStats(*vb);
    return stats.getCollectionMemUsed(cid);
}

MemChecker::MemChecker(VBucketPtr& vb,
                       const CollectionEntry::Entry& entry,
                       PostFunc postCondition)
    : StatChecker(vb,
                  entry,
                  getCollectionMemUsed,
                  "mem_used",
                  std::move(postCondition)) {
}

size_t getCollectionDiskSize(VBucketPtr& vb, CollectionID cid) {
    return vb->getManifest().lock(cid).getDiskSize();
}

DiskChecker::DiskChecker(VBucketPtr& vb,
                         const CollectionEntry::Entry& entry,
                         PostFunc postCondition)
    : StatChecker(vb,
                  entry,
                  getCollectionDiskSize,
                  "disk_size",
                  std::move(postCondition)) {
}
