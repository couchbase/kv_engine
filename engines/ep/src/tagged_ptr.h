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

#include <memory>

#if (defined(__x86_64__) || defined(_M_X64) || defined(__s390x__))

class TaggedPtrBase {
public:
    static const uint16_t NoTagValue = 0;
};

/*
 * This class provides a tagged pointer, which means it is a pointer however
 * the top 16 bits (the tag) can be used to hold user data.  This works
 * because on x86-64 it only addresses using the bottom 48-bits (the top 16-bits
 * are not used).
 *
 * A unsigned 16 bit value can be stored in the pointer using the setTag method.
 * The value can be retrieved using the setTag method.
 *
 * To avoid an address error being raised it is important to mask out the top
 * 16-bits when using the pointer.  This is achieved by using the getOBj method.
 * setObj is used to set the pointer value, without affecting the tag.
 */

template <typename T>
class TaggedPtr : public TaggedPtrBase {
public:
    using element_type = T;

    // Need to define all methods which unique_ptr expects from a pointer type
    TaggedPtr() : raw(NoTagValue) {
    }

    TaggedPtr(T* obj, uint16_t tag) : TaggedPtr() {
        set(obj);
        setTag(tag);
    }

    bool operator!=(const T* other) const {
        return get() != other;
    }

    bool operator==(const T* other) const {
        return !operator!=(other);
    }

    bool operator==(const TaggedPtr& other) const {
        return get() == other.get();
    }

    bool operator!=(const TaggedPtr& other) const {
        return get() != other.get();
    }

    explicit operator bool() const {
        return extractPointer(raw) != 0;
    }

    // Implement pointer operator to allow existing code to transparently
    // access the underlying object (ignoring the tag).
    T* operator->() const noexcept {
        return get();
    }

    // Required by MS Compiler
    T& operator*() const noexcept {
        return *get();
    }

    void setTag(uint16_t tag) {
        raw = extractPointer(raw);
        raw |= (uintptr_t(tag) << 48ull);
    }

    uint16_t getTag() const {
        return (extractTag(raw) >> 48ull);
    }

    void set(const T* obj) {
        raw = extractTag(raw);
        raw |= extractPointer(reinterpret_cast<uintptr_t>(obj));
    }

    T* get() const {
        return reinterpret_cast<T*>(extractPointer(raw));
    }

    // Required by MS Compiler
    static TaggedPtr<T> pointer_to(element_type& r) {
        return TaggedPtr<T>(std::addressof(r));
    }

    /**
     * Sets the tag of a TaggedPtr that is wrapped inside a pointer
     * e.g. SingleThreadedRCPtr
     *
     * @param ptr  pointer that wraps a TaggedPtr
     * @param value  u16int value that is to be stored in the tag
     */
    template <typename Pointer>
    static void updateTag(Pointer& ptr, uint16_t value) {
        auto taggedPtr = ptr.get();
        taggedPtr.setTag(value);
        ptr.reset(taggedPtr);
    }

private:
    uintptr_t extractPointer(uintptr_t ptr) const {
        return (ptr & 0x0000ffffffffffffull);
    }

    uintptr_t extractTag(uintptr_t ptr) const {
        return (ptr & 0xffff000000000000ull);
    }

    // Tag held in top 16 bits.
    uintptr_t raw;
};

#else

#define CB_MEMORY_INEFFICIENT_TAGGED_PTR 1

/*
 * A memory inefficient version which use the full pointer _AND_ a 16 bit
 * flag section for platforms which use the full address space of a 64 pointer
 */
template <typename T>
class TaggedPtr {
public:
    using element_type = T;

    TaggedPtr() : TaggedPtr(nullptr) {
    }

    // Need to define all methods which unique_ptr expects from a pointer type
    TaggedPtr(T* obj) : TaggedPtr(obj, 0) {
    }

    TaggedPtr(T* obj, uint16_t tag) : thePointer(obj), theTag(tag) {
    }

    bool operator!=(const T* other) const {
        return get() != other;
    }

    bool operator==(const T* other) const {
        return !operator!=(other);
    }

    operator bool() const {
        return thePointer != nullptr;
    }

    // Implement pointer operator to allow existing code to transparently
    // access the underlying object (ignoring the tag).
    T* operator->() const noexcept {
        return get();
    }

    // Required by MS Compiler
    T& operator*() const noexcept {
        return *get();
    }

    void setTag(uint16_t tag) {
        theTag = tag;
    }

    uint16_t getTag() const {
        return theTag;
    }

    void set(const T* obj) {
        thePointer = const_cast<T*>(obj);
    }

    T* get() const {
        return const_cast<T*>(thePointer);
    }

    // Required by MS Compiler
    static TaggedPtr<T> pointer_to(element_type& r) {
        return TaggedPtr<T>(std::addressof(r));
    }

    /**
     * Sets the tag of a TaggedPtr that is wrapped inside a pointer
     * e.g. SingleThreadedRCPtr
     *
     * @param ptr  pointer that wraps a TaggedPtr
     * @param value  u16int value that is to be stored in the tag
     */
    template <typename Pointer>
    static void updateTag(Pointer& ptr, uint16_t value) {
        auto taggedPtr = ptr.get();
        taggedPtr.setTag(value);
        ptr.reset(taggedPtr);
    }

private:
    T* thePointer = nullptr;
    uint16_t theTag = 0;
};

#endif

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
