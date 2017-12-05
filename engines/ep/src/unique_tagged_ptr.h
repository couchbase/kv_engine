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

#include "tagged_ptr.h"

/*
 * The class provides a Deleter for std::unique_ptr.  It defines a type pointer
 * which is the TaggedPtr<T> and means that get() will return the TaggedPtr<T>.
 * It also templates on the Deleter, so a customer deleter can be passed in.
 */

template <typename T, typename S>
class TaggedPtrDeleter {
public:
    // Need to specify a custom pointer type
    using pointer = TaggedPtr<T>;

    void operator()(TaggedPtr<T> item) {
        S()(item.get());
    }
};

/*
 * The class provides a wrapper for std::unique_ptr of TaggedPtr<T>.
 * This ensures we only require limited changes to the external interface,
 * e.g. do not have get().get() throughout the code.  It also hides the
 * complexity of setting, which requires a release followed by a reset because
 * TaggedPtrDeleter::pointer is passed by value.
 */
template <typename T, typename S>
class UniqueTaggedPtr {
public:
    UniqueTaggedPtr() {
    }

    UniqueTaggedPtr(T* obj) {
        set(obj);
    }

    UniqueTaggedPtr(T* obj, uint16_t tag) {
        set(obj);
        setTag(tag);
    }

    T* operator->() const noexcept {
        return get();
    }

    T& operator*() const noexcept {
        return *get();
    }

    bool operator==(const T* other) const {
        return get() == other;
    }

    bool operator!=(const T* other) const {
        return !operator==(other);
    }

    operator bool() const {
        return get() != nullptr;
    }

    void reset(TaggedPtr<T> b = nullptr) {
        up.reset(b);
    }

    T* release() {
        return up.release().get();
    }

    T* get() const {
        return up.get().get();
    }

    uint16_t getTag() const {
        return up.get().getTag();
    }

    void set(T* p) {
        // TaggedPtrDeleter::pointer is passed by value everywhere by unique_ptr
        // therefore need to release, set the tag and then reset the pointer.
        auto ptr = up.release();
        ptr.set(p);
        reset(ptr);
    }

    void setTag(uint16_t tag) {
        // TaggedPtrDeleter::pointer is passed by value everywhere by unique_ptr
        // therefore need to release, set the tag and then reset the pointer.
        auto ptr = up.release();
        ptr.setTag(tag);
        reset(ptr);
    }

private:
    std::unique_ptr<TaggedPtr<T>, TaggedPtrDeleter<T, S>> up;
};
