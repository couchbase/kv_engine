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
     *
     * @return the new Blob instance
     */
    static Blob* New(const char* start, const size_t len);

    /**
     * Create a new Blob of the given size.
     * (Used for appends/prepends)
     *
     * @param len the size of the blob
     *
     * @return the new Blob instance
     */
    static Blob* New(const size_t len);

    /**
     * Creates an exact copy of the specified Blob.
     */
    static Blob* Copy(const Blob& other);

    // Actual accessorish things.

    /**
     * Get the pointer to the contents of the Value part of this Blob.
     */
    const char* getData() const {
        return data;
    }

    /**
     * Get the size of this Blob's value.
     */
    size_t valueSize() const {
        return size;
    }

    /**
     * Get the size of this Blob instance.
     */
    size_t getSize() const {
        return size + sizeof(Blob);
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
     */
    explicit Blob(const char* start, const size_t len);

    explicit Blob(const size_t len);

    explicit Blob(const Blob& other);

    static size_t getAllocationSize(size_t len) {
        return sizeof(Blob) + len - sizeof(Blob(0, 0).data);
    }

    const uint32_t size;

    // The age of this Blob, in terms of some unspecified units of time.
    uint8_t age;
    // Pad Blob to 12 bytes by having an array of size 3.
    char data[3];

    DISALLOW_ASSIGN(Blob);

    friend bool operator==(const Blob& lhs, const Blob& rhs);
    friend std::ostream& operator<<(std::ostream& os, const Blob& b);
};

bool operator==(const Blob& lhs, const Blob& rhs);
std::ostream& operator<<(std::ostream& os, const Blob& b);

typedef SingleThreadedRCPtr<Blob> value_t;
