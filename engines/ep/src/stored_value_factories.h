/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
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

    explicit StoredValueFactory(EPStats& s) : stats(&s) {
    }

    /**
     * Create an concrete StoredValue object.
     */
    StoredValue::UniquePtr operator()(const Item& itm,
                                      StoredValue::UniquePtr next) override;

    StoredValue::UniquePtr copyStoredValue(
            const StoredValue& other, StoredValue::UniquePtr next) override;

private:
    EPStats* stats;
};

/**
 * Creator of OrderedStoredValue instances.
 */
class OrderedStoredValueFactory : public AbstractStoredValueFactory {
public:
    using value_type = OrderedStoredValue;

    explicit OrderedStoredValueFactory(EPStats& s) : stats(&s) {
    }

    /**
     * Create a new OrderedStoredValue with the given item.
     */
    StoredValue::UniquePtr operator()(const Item& itm,
                                      StoredValue::UniquePtr next) override;

    /**
     * Create a copy of OrderedStoredValue from the given one.
     */
    StoredValue::UniquePtr copyStoredValue(
            const StoredValue& other, StoredValue::UniquePtr next) override;

private:
    EPStats* stats;
};
