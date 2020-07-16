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

#include "stored_value_factories.h"

#include "item.h"

StoredValue::UniquePtr StoredValueFactory::operator()(
        const Item& itm, StoredValue::UniquePtr next) {
    // Allocate a buffer to store the StoredValue and any trailing bytes
    // that maybe required.
    return StoredValue::UniquePtr(TaggedPtr<StoredValue>(
            new (::operator new(StoredValue::getRequiredStorage(itm.getKey())))
                    StoredValue(itm,
                                std::move(next),
                                *stats,
                                /*isOrdered*/ false),
            TaggedPtrBase::NoTagValue));
}

StoredValue::UniquePtr StoredValueFactory::copyStoredValue(
        const StoredValue& other, StoredValue::UniquePtr next) {
    // Allocate a buffer to store the copy of StoredValue and any
    // trailing bytes required for the key.
    return StoredValue::UniquePtr(TaggedPtr<StoredValue>(
            new (::operator new(other.getObjectSize()))
                    StoredValue(other, std::move(next), *stats),
            TaggedPtrBase::NoTagValue));
}

StoredValue::UniquePtr OrderedStoredValueFactory::operator()(
        const Item& itm, StoredValue::UniquePtr next) {
    // Allocate a buffer to store the OrderStoredValue and any trailing
    // bytes required for the key.
    return StoredValue::UniquePtr(TaggedPtr<StoredValue>(
            new (::operator new(
                    OrderedStoredValue::getRequiredStorage(itm.getKey())))
                    OrderedStoredValue(itm, std::move(next), *stats),
            TaggedPtrBase::NoTagValue));
}

StoredValue::UniquePtr OrderedStoredValueFactory::copyStoredValue(
        const StoredValue& other, StoredValue::UniquePtr next) {
    // Allocate a buffer to store the copy ofOrderStoredValue and any
    // trailing bytes required for the key.
    return StoredValue::UniquePtr(TaggedPtr<StoredValue>(
            new (::operator new(other.getObjectSize()))
                    OrderedStoredValue(other, std::move(next), *stats),
            TaggedPtrBase::NoTagValue));
}
