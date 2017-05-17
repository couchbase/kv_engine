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

#include "config.h"

#include "atomic.h"

/**
 * A blob is a minimal sized storage for data up to 2^32 bytes long.
 */
class Blob : public RCValue {
public:
    // Constructors.

    /**
     * Create a new Blob holding the given data.
     *
     * @param start the beginning of the data to copy into this blob
     * @param len the amount of data to copy in
     * @param ext_meta pointer to the extended meta section to be added
     * @param ext_len length of the extended meta section
     *
     * @return the new Blob instance
     */
    static Blob* New(const char* start,
                     const size_t len,
                     uint8_t* ext_meta,
                     uint8_t ext_len);

    /**
     * Create a new Blob of the given size, with ext_meta set to the specified
     * extended metadata
     *
     * @param len the size of the blob
     * @param ext_meta pointer to the extended meta section to be copied in.
     * @param ext_len length of the extended meta section
     *
     * @return the new Blob instance
     */
    static Blob* New(const size_t len, uint8_t* ext_meta, uint8_t ext_len);

    /**
     * Create a new Blob of the given size.
     * (Used for appends/prepends)
     *
     * @param len the size of the blob
     * @param ext_len length of the extended meta section
     *
     * @return the new Blob instance
     */
    static Blob* New(const size_t len, uint8_t ext_len);

    /**
     * Creates an exact copy of the specified Blob.
     */
    static Blob* Copy(const Blob& other);

    // Actual accessorish things.

    /**
     * Get the pointer to the contents of the Value part of this Blob.
     */
    const char* getData() const {
        return data + FLEX_DATA_OFFSET + extMetaLen;
    }

    /**
     * Get the pointer to the contents of Blob.
     */
    const char* getBlob() const {
        return data;
    }

    /**
     * Return datatype stored in Value Blob.
     */
    const protocol_binary_datatype_t getDataType() const {
        return extMetaLen > 0
                       ? protocol_binary_datatype_t(*(data + FLEX_DATA_OFFSET))
                       : PROTOCOL_BINARY_RAW_BYTES;
    }

    /**
     * Set datatype for the value Blob.
     */
    void setDataType(uint8_t datatype) {
        data[FLEX_DATA_OFFSET] = char(datatype);
    }

    /**
     * Return the pointer to exteneded metadata, stored in the Blob.
     */
    const char* getExtMeta() const {
        return extMetaLen > 0 ? data + FLEX_DATA_OFFSET : NULL;
    }

    /**
     * Get the length of this Blob value.
     */
    size_t length() const {
        return size;
    }

    /**
     * Get the length of just the value part in the Blob.
     */
    size_t vlength() const {
        return size - extMetaLen - FLEX_DATA_OFFSET;
    }

    /**
     * Get the size of this Blob instance.
     */
    size_t getSize() const {
        return size + sizeof(Blob);
    }

    /**
     * Get extended meta data length, after subtracting the
     * size of FLEX_META_CODE.
     */
    uint8_t getExtLen() const {
        return extMetaLen;
    }

    /**
     * Returns how old this Blob is (how many epochs have passed since it was
     * created).
     */
    uint8_t getAge() const {
        return age;
    }

    /**
     * Increment the age of the Blob. Saturates at 255.
     */
    void incrementAge() {
        age++;
        // Saturate the result at 255 if we wrapped.
        if (age == 0) {
            age = 255;
        }
    }

    /**
     * Get a std::string representation of this blob.
     */
    const std::string to_s() const;

    // This is necessary for making C++ happy when I'm doing a
    // placement new on fairly "normal" c++ heap allocations, just
    // with variable-sized objects.
    void operator delete(void* p) {
        ::operator delete(p);
    }

    ~Blob();

protected:
    /* Constructor.
     * @param start If non-NULL, pointer to array which will be copied into
     *              the newly-created Blob.
     * @param len   Size of the data the Blob object will hold, and size of
     *              the data at {start}.
     * @param ext_meta Pointer to any extended metadata, which will be copied
     *                 into the newly created Blob.
     * @param ext_len Size of the data pointed to by {ext_meta}
     */
    explicit Blob(const char* start,
                  const size_t len,
                  uint8_t* ext_meta,
                  uint8_t ext_len);

    explicit Blob(const size_t len, uint8_t ext_len);

    explicit Blob(const Blob& other);

    static size_t getAllocationSize(size_t len) {
        return sizeof(Blob) + len - sizeof(Blob(0, 0).data);
    }

    const uint32_t size;
    const uint8_t extMetaLen;

    // The age of this Blob, in terms of some unspecified units of time.
    uint8_t age;
    // Pad Blob to 12 bytes by having an array of size 2.
    char data[2];

    DISALLOW_ASSIGN(Blob);

    friend bool operator==(const Blob& lhs, const Blob& rhs);
    friend std::ostream& operator<<(std::ostream& os, const Blob& b);
};

bool operator==(const Blob& lhs, const Blob& rhs);
std::ostream& operator<<(std::ostream& os, const Blob& b);

typedef SingleThreadedRCPtr<Blob> value_t;
