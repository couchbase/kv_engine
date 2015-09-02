/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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

#include <cstdlib>
#include <stdexcept>
#include <string>

class DynamicBuffer {
public:
    DynamicBuffer()
        : buffer(nullptr),
          offset(0),
          size(0) {
        // empty
    }

    DynamicBuffer(const DynamicBuffer&) = delete;

    ~DynamicBuffer() {
        clear();
    }

    /**
     * Grow the buffer to have at least <em>needed</em> bytes
     *
     * @param needed the number of bytes needed in the buffer
     * @return true if success, false otherwise
     */
    bool grow(size_t needed);

    /**
     * Clear the buffer and release all allocated resources
     */
    void clear();

    /**
     * Get the root of the buffer
     */
    char *getRoot() const {
        return buffer;
    }

    /**
     * Get the "current" pointer (root + offset)
     */
    char *getCurrent() const {
        return buffer + offset;
    }

    /**
     * Get the current offset in the buffer
     */
    size_t getOffset() const {
        return offset;
    }

    /**
     * Move the offset in the buffer (we've just inserted more data into
     * the buffer)
     */
    void moveOffset(size_t num) {
        offset += num;
        // Validate that we've not run over the buffer size
        if (offset > size) {
            throw std::out_of_range("DynamicBuffer::moveOffset: offset (" +
                                    std::to_string(offset) + ") > size (" +
                                    std::to_string(size) + ")");
        }
    }

    /**
     * Transfer the ownership of the underlying buffer. The caller is
     * responsible for freeing the underlying data
     */
    void takeOwnership() {
        buffer = nullptr;
        size = 0;
        offset = 0;
    }

    /**
     * Get the total size of the allocated buffer. Note that the bytes in
     * the range [offset, size> is not initialized or used.
     */
    size_t getSize() const {
        return size;
    }

private:
    /** Start of the allocated buffer */
    char* buffer;
    /** How much of the buffer has been used so far. */
    size_t offset;
    /** Total allocated size */
    size_t size;
};
