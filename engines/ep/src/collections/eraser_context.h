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

#include "collections/scan_context.h"
#include "systemevent.h"

#pragma once

namespace Collections {
namespace VB {

/**
 * The EraserContext subclasses ScanContext and provides extra methods for
 * tracking when collections are completely erased.
 */
class EraserContext : public ScanContext {
public:
    explicit EraserContext(
            const std::vector<Collections::KVStore::DroppedCollection>&
                    droppedCollections);

    /**
     * Called by kvstore for deleted keys, when the deleted key is a drop of a
     * collection, the set of dropped collections is updated.
     * @param key The key of the deleted value (will returns if not system)
     * @param se The flags...SystemEvent of the value
     */
    void processSystemEvent(const DocKey& key, SystemEvent se);
    /**
     * @return true if the on-disk collection meta-data should be updated
     */
    bool needToUpdateCollectionsMetadata() const;

private:
    friend std::ostream& operator<<(std::ostream&, const EraserContext&);

    /// set to true if an entry was removed from the 'dropped' container
    bool removed = false;
};

std::ostream& operator<<(std::ostream&, const EraserContext&);

} // namespace VB
} // namespace Collections
