/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/**
 * AtomicUnorderedMap - A thread-safe map class.
 *
 * AtomicUnorderedMap is a thread-safe unordered map (associative array).
 * Elements can be added, removed and found concurrently from different
 * threads safely.
 *
 *
 * THREAD SAFETY
 * Items are returned by value (instead of via an iterator) - this ensures that
 * once an item is passed back to the caller, it can safely be accessed even if
 * another thread has concurrently deleted it from the map.
 *
 * While this may seen limiting, the value type can be a (smart) pointer if
 * desired, removing the need to copy the actual underlying object. However,
 * if a pointer type is used then operations on the pointed-to objects are
 * *not* automatically thread-safe. In other words, while you can safely call
 * insert(<ptr>) from multiple threads, you *cannot* safely mutate the object
 * pointed to (by the pointer which insert() returns) from multiple threads
 * without additional synchronization. For example, having an per-object
 * mutex, or making the object atomic.
 *
 *
 * FUNCTIONALITY
 * Implements a relatively simple set of operations modeled on
 * std::unordered_map:
 *
 *   - empty() returns true if the map is empty.
 *   - size() to return the number of elements in the map.
 *   - insert() to add an element
 *   - find() to search for an element
 *   - erase() to delete an element
 *   - clear() to delete all elements
 *
 * Iteration, a la `auto it = begin(); it++; ...` isn't directly supported;
 * the main reason is that another thread may have removed an item between
 * calling begin() and moving to the next item, so it's not possible to ensure
 * all elements are acted on. Instead, a number of functions similar to
 * std::algorithm are provided:
 *
 *   - find_if() to search for the first element matching a given predicate.
 *   - for_each() to apply a function to every element in the map.
 *
 *
 * LOCKING STRATEGIES
 * There are two locking strategies available:
 * - Internal locking, where the methods themselves lock on entry (and unlock
 *   on exit).
 * - External locking, where a lock is acquired before calling the methods.
 *
 * For simple use-cases internal locking is sufficient (and safer) - the caller
 * doesn't have to concern themselves with locking, and can rely on the object
 * doing the right thing.
 * However if the caller needs to ensure that multiple operations on the map
 * are atomic (e.g. find()ing an item and then conditionally erase()ing it) then
 * external locking can be used.
 *
 * For example, to atomically remove an key only if it's value is false:
 *
 *     typedef AtomicUnorderedMap<int, bool> M; // Key:int, Value:bool
 *     M map;
 *     ...
 *     { // Create new scope for external lock guard.
 *         std::lock_guard<M> guard(map);
 *         bool it = map.find(key_of_interest, guard);
 *         if (it && *it == false) {
 *             map.erase(it, guard);
 *         }
 *     } // end of scope, map unlocked.
 *
 * Note that the guard is passed into the find() and erase() functions to
 * indicate that an external lock is already acquired (and hence an internal
 * lock should not be acquired).
 *
 * See Boost Synchronization
 * (http://www.boost.org/doc/libs/1_60_0/doc/html/thread/synchronization.html)
 * for more details & background on the internal / external locking strategies
 * used here.
 */

#pragma once

#include <folly/lang/Aligned.h>
#include <platform/rwlock.h>

#include <algorithm>
#include <shared_mutex>
#include <unordered_map>

template <class Key,
          class T,
          class Hash = std::hash<Key>,
          class KeyEqual = std::equal_to<Key>,
          class Allocator = std::allocator<std::pair<const Key, T> > >
class AtomicUnorderedMap;

template <class Key, class T, class Hash, class KeyEqual, class Allocator>
class AtomicUnorderedMap {
public:
    using map_type = AtomicUnorderedMap<Key, T, Hash, KeyEqual, Allocator>;

    // Alias to simplify all the other defs
    using base_map_type =
            typename std::unordered_map<Key, T, Hash, KeyEqual, Allocator>;

    // Map to the type aliases in the underlying map.
    using key_type = typename base_map_type::key_type;
    using mapped_type = typename base_map_type::mapped_type;
    using value_type = typename base_map_type::value_type;
    using size_type = typename base_map_type::size_type;

    bool empty() const {
        std::shared_lock<cb::RWLock> guard(*this->rwlock); // internally locked
        return map.empty();
    }

    size_type size() const {
        std::shared_lock<cb::RWLock> guard(*this->rwlock); // internally locked
        return map.size();
    }

    /* Lookup */

    /**
     * Searches for the given key in the map using external shared locking
     * @param key Reference to the key to find
     * @param shared_lock reference to the shared_lock
     * @returns a pair consisting of:
     *  - the found element (or a default-constructed element if not found)
     *  - and bool denoting if the given key was found.
     */
    std::pair<T, bool> find(const Key& key, std::shared_lock<map_type>&) {
        return find_UNLOCKED(key);
    }

    /**
     * Searches for the given key in the map using external exclusive locking
     * @param key Reference to the key to find
     * @param lock_guard reference to the lock_guard
     * @returns a pair consisting of:
     *  - the found element (or a default-constructed element if not found)
     *  - and bool denoting if the given key was found.
     */
    std::pair<T, bool> find(const Key& key, std::lock_guard<map_type>&) {
        return find_UNLOCKED(key);
    }

    /**
     * Searches for the given key in the map, internally locked
     * @param key Reference to the key to find
     * @returns a pair consisting of:
     *  - the found element (or a default-constructed element if not found)
     *  - and bool denoting if the given key was found.
     */
    std::pair<T, bool> find(const Key& key) {
        std::shared_lock<map_type> guard(*this); // internally locked
        return find(key, guard);
    }

    /** Searches for first element which matches the given predicate.
     *  Returns a pair consisting of:
     *  - the first found element (or a default-constructed element if not
     * found)
     *  - and bool denoting if a matching element was found.
     */
    template <class UnaryPredicate>
    std::pair<T, bool> find_if(UnaryPredicate p) {
        std::shared_lock<map_type> guard(*this); // internally locked
        auto iter = std::find_if(map.begin(), map.end(), p);
        if (iter != map.end()) {
            return {iter->second, true};
        } else {
            return std::make_pair(T(), false);
        }
    }

    /**
     * Applies the given function object to every mapped value and returns from
     * f some other value only if f returns a value that evaluates to true
     * (bool operator)
     *
     * The function should take a value_type reference as a parameter and return
     * some-type by value. some-type must be a type which supports operator bool
     * e.g. std::shared_ptr. As each map element is evaluated, the iteration
     * will stop when f returns a value which 'if (value)' evaluates to true,
     * the value is then returned.
     * If every element is visited and nothing evaluated to true, then a default
     * initialised some-type is returned.
     *
     * @param key Key value to lookup
     * @param f Function object to be applied
     * @returns The value found by f or a default initialised value
     */
    template <class UnaryFunction>
    auto find_if2(UnaryFunction f) {
        std::shared_lock<map_type> guard(*this);
        return find_if2_UNLOCKED(f);
    }

    /* Modifiers */

    void clear(std::lock_guard<map_type>&) {
        // Externally locked
        map.clear();
    }
    void clear() {
        std::lock_guard<map_type> guard(*this); // internally locked
        clear(guard);
    }

    /**
     * Applies the given function object to every element in the map using
     * exclusive locking
     *
     * @param f Function object to be applied to every element in the map
     * @param lock_guard externally held lock
     */
    template <class UnaryFunction>
    void for_each(UnaryFunction f, std::lock_guard<map_type>&) {
        std::for_each(map.begin(), map.end(), f);
    }

    /**
     * Applies the given function object to every element in the map
     *
     * @param f Function object to be applied to every element in the map
     */
    template <class UnaryFunction>
    void for_each(UnaryFunction f) {
        std::shared_lock<map_type> guard(*this); // internally locked
        for_each(f, guard);
    }

    /**
     * Applies the given function object to the key (if mapped)
     *
     * The function should take a value_type reference as a parameter
     *
     * @param key Key value to lookup
     * @param f Function object to be applied
     * @returns true if the key was found and f applied
     */
    template <class UnaryFunction>
    bool apply(const key_type& key, UnaryFunction f) {
        std::shared_lock<map_type> guard(*this);
        auto iter = map.find(key);
        if (iter != map.end()) {
            f(*iter);
        }
        return iter != map.end();
    }

    /**
     * Applies the given function object to every element in the map using
     * shared_lock locking
     *
     * @param f Function object to be applied to every element in the map
     * @param lock_guard externally held lock
     */
    template <class UnaryFunction>
    void for_each(UnaryFunction f, std::shared_lock<map_type>&) {
        std::for_each(map.begin(), map.end(), f);
    }

    /**
     * Attempts to erase the given key from the map.
     *  Returns a pair consisting of:
     *  - the erased element (or a default-constructed element if not found)
     *  - and bool denoting if the given key was erased.
     */
    std::pair<T, bool> erase(const key_type& key, std::lock_guard<map_type>&) {
        // Externally locked
        auto iter = map.find(key);
        if (iter != map.end()) {
            T result = iter->second;
            map.erase(iter);
            return {result, true};
        } else {
            return std::make_pair(T(), false);
        }
    }
    std::pair<T, bool> erase(const key_type& key) {
        std::lock_guard<map_type> guard(*this); // internally locked
        return erase(key, guard);
    }

    /**
     * Attempts to insert the given key into the map, if it does not already
     * exist.
     *  Returns true if the element was inserted, or false if an element
     *  with the given key already exists.
     */
    bool insert(const value_type& value) {
        std::lock_guard<map_type> guard(*this); // internally locked
        auto result = map.insert(value);
        return result.second;
    }

    /**
     * Attempts to insert the given key into the map, if it does not already
     * exist.
     *  Returns true if the element was inserted, or false if an element
     *  with the given key already exists.
     */
    bool insert(const value_type& value, std::lock_guard<map_type>&) {
        auto result = map.insert(value);
        return result.second;
    }

    /*
     * Locking
     *
     * Note: Prefer to use RAII-style lock holders (e.g. std::lock_guard<>())
     *       instead of the raw methods here.
     */

    /* Explicitly locks the container. */
    void lock() {
        rwlock->lock();
    }

    void unlock() {
        rwlock->unlock();
    }

    void lock_shared() {
        rwlock->lock_shared();
    }

    void unlock_shared() {
        rwlock->unlock_shared();
    }

private:
    std::pair<T, bool> find_UNLOCKED(const Key& key) {
        auto iter = map.find(key);
        if (iter != map.end()) {
            return {iter->second, true};
        } else {
            return std::make_pair(T(), false);
        }
    }

    template <class UnaryFunction>
    auto find_if2_UNLOCKED(UnaryFunction f) {
        using UnaryFunctionRval = decltype(f(*map.find({})));
        for (auto& kv : map) {
            auto rv = f(kv);
            if (rv) {
                return rv;
            }
        }
        return UnaryFunctionRval{};
    }

    std::unordered_map<Key, T, Hash, KeyEqual, Allocator> map;

    // MB-32107
    // Cacheline padded as it was identified that this lock shared with the
    // preceeding map and in the case of the DcpProducer some following atomic
    // variables. As this mutex occupies 56 bytes on Linux (almost an entire
    // cache line) we should pad it to prevent the shuffling of members in the
    // DcpProducer class moving this mutex and causing false sharing that
    // affects performance.
    mutable folly::cacheline_aligned<cb::RWLock> rwlock;
};
