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

#pragma once

#include <platform/atomic.h>

#include <exception>
#include <unordered_map>

namespace Collections {

/**
 * Class for mapping a Key to a Value and having shared references to the Value.
 * To achieve shared references, the Value must be a sub-class of RCValue and
 * the class gives out SingleThreadedRCPtr<Value> to users of the class.
 *
 * @tparam Key the key type to use - this must have a to_string method for
 *         exception messages
 * @tparam Value The 'owned' Value type, this is the object that will be stored
 *         in the SharedMetaDataTable and the type of reference given out.
 *         Value must support ostream<< for debug dumping. This type must
 *         inherit from RCValue.
 */
template <class Key, class Value>
class SharedMetaDataTable {
public:
    ~SharedMetaDataTable() {
#if CB_DEVELOPMENT_ASSERTS
        Expects(smt.empty());
#endif
    }

    /**
     * Function returns a Value which references the given name. This may
     * reference an existing Value or insert it as a new Value in the map. The
     * returned shared pointer is owned by this container and shared with
     * callers via this method.
     *
     * @tparam ValueView A different type to Value which is used in look-up.
     *         This type could match Value or be different. For example if Value
     *         contains a string, ValueView could contain a string_view
     *         equivalent. Note that Value and ValueView must be comparable with
     *         == ValueView must have a to_string operator for exception
     *         messages.
     *
     * @param id The id associated with the value
     * @param valueView A view (non-owning) of the value
     * @returns shared pointer type (no copying) for the Value representing the
     *          given id->valueView.
     */
    template <class ValueView>
    SingleThreadedRCPtr<Value> createOrReference(Key id,
                                                 const ValueView& valueView);

    /**
     * Function is for informing the container when data obtained by
     * createOrReference (associated with key) has been released.
     *
     * @param id The id associated with the value
     * @param meta Reference to the meta that was originally given out by
     *        createOrReference.
     */
    void dereference(Key id, SingleThreadedRCPtr<Value>&& meta);
    size_t count(Key id) const {
        return smt.count(id);
    }

private:
    template <class K, class V>
    friend std::ostream& operator<<(std::ostream& os,
                                    const SharedMetaDataTable<K, V>& table);
    std::unordered_multimap<Key, SingleThreadedRCPtr<Value>> smt;
};

template <class Key, class Value>
template <class ValueView>
SingleThreadedRCPtr<Value> SharedMetaDataTable<Key, Value>::createOrReference(
        Key id, const ValueView& valueView) {
    for (auto [itr, end] = smt.equal_range(id); itr != end; itr++) {
        if (*itr->second == valueView) {
            // Found a match, return it
            return itr->second;
        }
    }

    // Here the key is not mapped or the key does not have a matching value.
    // We now must store a new Value (reference of 1) and return the reference.
    auto itr = smt.emplace(id, make_STRCPtr<Value>(valueView));
    return itr->second;
}

template <class Key, class Value>
void SharedMetaDataTable<Key, Value>::dereference(
        Key id, SingleThreadedRCPtr<Value>&& meta) {
    auto [itr, end] = smt.equal_range(id);
    if (itr == end) {
        throw std::invalid_argument(
                "SharedMetaDataTable<Key>::dereference nothing found for id:" +
                id.to_string());
    }
    // Now reset, dropping the reference the user had on the metadata
    meta.reset();
    while (itr != end) {
        if (itr->second.refCount() == 1) {
            itr = smt.erase(itr);
        } else {
            itr++;
        }
    }
}

template <class Key, class Value>
std::ostream& operator<<(std::ostream& os,
                         const SharedMetaDataTable<Key, Value>& table) {
    os << "SharedMetaDataTable: size:" << table.smt.size() << std::endl;
    for (const auto& [key, value] : table.smt) {
        os << "  id:" << key.to_string() << ", value:" << *value
           << ", refs:" << value.refCount() << std::endl;
    }
    return os;
}

} // namespace Collections