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

#include <vector>
#include <cstdint>
#include <cstring>
#include <stdexcept>

namespace Greenstack {

    /**
     * The Buffer class is an abstract class representing an area in memory
     * used as a buffer. It is used to provide an interface between different
     * backends.
     */
    class Buffer {
    public:
        /**
         * Release all allocated resources
         */
        virtual ~Buffer() { }

        /**
        * Get the pointer to the data area of this buffer. It has to be available
        * for the lifefime of the Buffer object.
        */
        virtual uint8_t* getData() const = 0;

        /**
        * Get the size of this buffer.
        */
        virtual const size_t getSize() const = 0;

        /**
         * Try to resize the buffer
         *
         * @param new_size The new size of the buffer
         */
        virtual void resize(size_t new_size) {
            if (supportsResize()) {
                throw std::runtime_error(
                    "Internal error: resize not implemented");
            } else {
                throw std::runtime_error("Buffer type does not support resize");
            }
        }

        /**
         * Does this buffer type support resize or not
         */
        virtual bool supportsResize() const {
            return false;
        }
    };

    /**
     * A buffer on top of a fixed size character array.
     */
    class FixedByteArrayBuffer : public Buffer {
    public:
        /**
         * Create an array that wraps an existing bucket
         *
         * @param ptr pointer to the first byte in the buffer
         * @param sz the size of the buffer
         */
        FixedByteArrayBuffer(uint8_t* ptr, size_t sz)
            : Buffer(),
              allocated(false),
              data(ptr),
              size(sz) {
            data = ptr;
        }

        /**
         * Create an array and allocate the underlying buffer
         *
         * @param sz the size of the buffer
         */
        FixedByteArrayBuffer(size_t sz)
            : Buffer(),
              allocated(true),
              data(new uint8_t[sz]),
              size(sz) {
            data = new uint8_t[size];
        }

        /**
         * Delete the buffer if we allocated it
         */
        virtual ~FixedByteArrayBuffer() {
            if (allocated) {
                delete[]data;
            }
        }

        /**
         * Get a pointer to the beginning of the buffer
         */
        virtual uint8_t* getData() const override {
            return data;
        }

        /**
         * Get the size of the buffer
         */
        virtual const size_t getSize() const override {
            return size;
        }

    private:
        bool allocated;
        uint8_t* data;
        size_t size;
    };
}
