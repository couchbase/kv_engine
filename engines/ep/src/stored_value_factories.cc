/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
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
