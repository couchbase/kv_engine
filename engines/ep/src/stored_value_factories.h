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

/**
 * Factories for creating StoredValue and subclasses of StoredValue.
 */

#include <memory>

#include "stored-value.h"

/**
 * Abstract base class for StoredValue factories.
 */
class AbstractStoredValueFactory {
public:
    virtual ~AbstractStoredValueFactory() {}

    /**
     * Create a new StoredValue (or subclass) with the given item.
     *
     * @param itm the item the StoredValue should contain
     * @param next The StoredValue which will follow the new stored value in
     *             the hash bucket chain, which this new item will take
     *             ownership of. (Typically the top of the hash bucket into
     *             which the new item is being inserted).
     */
    virtual StoredValue::UniquePtr operator()(const Item& itm,
                                              StoredValue::UniquePtr next) = 0;

    /**
     * Create a new StoredValue (or subclass) from the given StoredValue.
     *
     * @param other The StoredValue to be copied
     * @param next The StoredValue which will follow the new StoredValue (the
     *             copy) in the hash bucket chain (typically the top of the
     *             hash bucket into which the new item is being inserted).
     */
    virtual StoredValue::UniquePtr copyStoredValue(const StoredValue& other,
                                                   StoredValue::UniquePtr next) = 0;
};

/**
 * Creator of StoredValue instances.
 */
class StoredValueFactory : public AbstractStoredValueFactory {
public:
    using value_type = StoredValue;

    StoredValueFactory(EPStats& s) : stats(&s) {
    }

    /**
     * Create an concrete StoredValue object.
     */
    StoredValue::UniquePtr operator()(const Item& itm,
                                      StoredValue::UniquePtr next) override {
        // Allocate a buffer to store the StoredValue and any trailing bytes
        // that maybe required.
        return StoredValue::UniquePtr(
                new (::operator new(StoredValue::getRequiredStorage(itm)))
                        StoredValue(itm,
                                    std::move(next),
                                    *stats,
                                    /*isOrdered*/ false));
    }

    StoredValue::UniquePtr copyStoredValue(const StoredValue& other,
                                           StoredValue::UniquePtr next) override {
        // Allocate a buffer to store the copy of StoredValue and any
        // trailing bytes required for the key.
        return StoredValue::UniquePtr(
                new (::operator new(other.getObjectSize()))
                        StoredValue(other, std::move(next), *stats));
    }

private:
    EPStats* stats;
};

/**
 * Creator of OrderedStoredValue instances.
 */
class OrderedStoredValueFactory : public AbstractStoredValueFactory {
public:
    using value_type = OrderedStoredValue;

    OrderedStoredValueFactory(EPStats& s) : stats(&s) {
    }

    /**
     * Create a new OrderedStoredValue with the given item.
     */
    StoredValue::UniquePtr operator()(const Item& itm,
                                      StoredValue::UniquePtr next) override {
        // Allocate a buffer to store the OrderStoredValue and any trailing
        // bytes required for the key.
        return StoredValue::UniquePtr(
                new (::operator new(
                        OrderedStoredValue::getRequiredStorage(itm)))
                        OrderedStoredValue(itm, std::move(next), *stats));
    }

    /**
     * Create a copy of OrderedStoredValue from the given one.
     */
    StoredValue::UniquePtr copyStoredValue(const StoredValue& other,
                                           StoredValue::UniquePtr next) override {
        // Allocate a buffer to store the copy ofOrderStoredValue and any
        // trailing bytes required for the key.
        return StoredValue::UniquePtr(
                new (::operator new(other.getObjectSize()))
                        OrderedStoredValue(other, std::move(next), *stats));
    }

private:
    EPStats* stats;
};
