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

#include "tagged_ptr.h"
#include "utility.h"
#include <platform/atomic.h>

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
        return size & ~(0x80000000);
    }

    /**
     * Get the size of this Blob instance.
     */
    size_t getSize() const {
        return valueSize() + sizeof(Blob) - paddingSize;
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
     * Check if the given data is compressible
     */
    bool isCompressible() {
        return ~(size & 0x80000000);
    }

    /**
     * Set the highest bit in size to mark the given data as uncompressible.
     * This should be fine given that the maximum value we support is 20 MiB
     */
    void setUncompressible() {
        size |= 0x80000000;
    }

    /**
     * Get a std::string representation of this blob.
     */
    const std::string to_s() const;

    /**
     * Class-specific deallocation function. We need to specify this
     * because with it, C++ runtime will call the sized delete version
     * with the size it _thinks_ Blob is, but we allocate the
     * object by using the new operator with a custom size (the value is
     * packed after the object) so the runtime's size is incorrect.
     */
    static void operator delete(void* ptr) {
        // We actually know the size of the allocation, so use that to
        // optimise deletion.
        auto* blob = reinterpret_cast<Blob*>(ptr);
        auto size = Blob::getAllocationSize(blob->valueSize());
        ::operator delete(blob, size);
    }

    ~Blob();

    /*
     * The class provides a customer deleter for SingleThreadedRCPtr templated
     * on a Blob with a pointer type of TaggedPtr.
     */

    class Deleter {
    public:
        void operator()(TaggedPtr<Blob> item) {
            delete item.get();
        }
    };

private:
    //Ensure Blob size of 12 bytes by padding by 3.
    static constexpr int paddingSize{3};

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
        return sizeof(Blob) + len - sizeof(Blob(nullptr, 0).data);
    }

    // Size of the value. The highest bit is used to represent if the
    // value is compressible or not. If set, then the value is not
    // compressible. This needs to be an atomic variable as there could
    // be a data race between threads that update the size
    // (e.g, the setUncompressible API) and the ones that read the size
    std::atomic<uint32_t> size;

    // The age of this Blob, in terms of some unspecified units of time.
    uint8_t age;
    // Pad Blob to 12 bytes.
    char data[paddingSize];

    DISALLOW_ASSIGN(Blob);

    friend bool operator==(const Blob& lhs, const Blob& rhs);
    friend std::ostream& operator<<(std::ostream& os, const Blob& b);
};

bool operator==(const Blob& lhs, const Blob& rhs);
std::ostream& operator<<(std::ostream& os, const Blob& b);

typedef SingleThreadedRCPtr<Blob, TaggedPtr<Blob>, Blob::Deleter> value_t;
