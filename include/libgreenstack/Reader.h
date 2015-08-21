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
#include <limits>
#include <cstring>
#include <stdexcept>
#include <stddef.h>
#include <iostream>
#include <cassert>

#include "config.h"
#include "Buffer.h"

namespace Greenstack {
    /**
     * The Reader class is an abstract class that allows for reading from a
     * stream in host local byte order.
     */
    class Reader {
    public:
        /**
         * Initialize the reader
         *
         * @param off the initial offset to start reading from
         * @param endOffset the offset that ends the stream
         */
        Reader(size_t off, size_t endOffset)
            : offset(off),
              size(endOffset) {

        }

        /**
         * Release all allocated resources
         */
        virtual ~Reader() {

        }

        /**
         * Seek in the buffer
         *
         * @param nbytes the number of bytes to seek
         */
        void seek(int nbytes) {
            offset += nbytes;
        }

        /**
         * Skip a number of bytes
         *
         * @param nbytes the number of bytes to skip
         */
        void skip(size_t nbytes = 1) {
            seek(nbytes);
        }

        /**
         * Reset the buffer
         *
         * @todo we should possibly move back to the initial offset!!!
         */
        void reset() {
            offset = 0;
        }

        /**
         * Read a single byte from the stream and move the current pointer
         *
         * @param val the value read from the stream (in local byte order)
         */
        void read(uint8_t& val) {
            pread(val, offset);
            skip(sizeof(val));
        }

        /**
         * Read a 16 bit word from the stream and move the current pointer
         *
         * @param val the value read from the stream (in local byte order)
         */
        void read(uint16_t& val) {
            pread(val, offset);
            skip(sizeof(val));
        }

        /**
         * Read a 32 bit word from the stream and move the current pointer
         *
         * @param val the value read from the stream (in local byte order)
         */
        void read(uint32_t& val) {
            pread(val, offset);
            skip(sizeof(val));
        }

        /**
         * Read a 64 bit word from the stream and move the current pointer
         *
         * @param val the value read from the stream (in local byte order)
         */
        void read(uint64_t& val) {
            pread(val, offset);
            skip(sizeof(val));
        }

        /**
         * Read a number of bytes the stream and move the current pointer
         *
         * @param ptr where to store the data
         * @param len the number of bytes to read
         */
        void read(uint8_t* ptr, size_t len) {
            pread(ptr, len, offset);
            offset += len;
        }

        /**
         * Get the current offset in the buffer
         *
         * @return the current offset
         */
        size_t getOffset() const {
            return offset;
        }

        /**
         * Read a single byte at a given location in the buffer
         *
         * @param val where to store the result
         * @param off the offset in the buffer
         */
        void pread(uint8_t& val, size_t off) const {
            pread(&val, sizeof(val), off);
        }

        /**
         * Read a 16 bit word at a given location in the buffer and convert to
         * host local byte order
         *
         * @param val where to store the result
         * @param off the offset in the buffer
         */
        void pread(uint16_t& val, size_t off) const {
            uint16_t value;
            pread(reinterpret_cast<uint8_t*>(&value), sizeof(value), off);
            val = ntohs(value);
        }

        /**
         * Read a 32 bit word at a given location in the buffer and convert to
         * host local byte order
         *
         * @param val where to store the result
         * @param off the offset in the buffer
         */
        void pread(uint32_t& val, size_t off) const {
            uint32_t value;
            pread(reinterpret_cast<uint8_t*>(&value), sizeof(value), off);
            val = ntohl(value);
        }

        /**
         * Read a 64 bit word at a given location in the buffer and convert to
         * host local byte order
         *
         * @param val where to store the result
         * @param off the offset in the buffer
         */
        void pread(uint64_t& val, size_t off) const {
            uint64_t value;
            pread(reinterpret_cast<uint8_t*>(&value), sizeof(value), off);
            val = ntohll(value);
        }

        /**
         * Read a specified number of bytes at a given location in the buffer.
         *
         * @param ptr where to store the data
         * @param len the number of bytes to read
         * @param off the offset in the buffer
         */
        virtual void pread(uint8_t* ptr, size_t len, size_t off) const = 0;

        /**
         * Get the number of bytes left in the reader
         *
         * @return the number of bytes remaining in the buffer
         */
        size_t getRemainder() const {
            return size - offset;
        }

        /**
         * get the size of the buffer
         *
         * @return the full size of the buffer
         */
        size_t getSize() const {
            return size;
        }

    protected:
        /**
         * Guard to protect access outside the buffer
         *
         * @param off the offset to access
         * @param len the number of bytes to access
         * @throws std::out_of_range for any access outside the buffer
         */
        void guard(size_t off, size_t len) const {
            if ((off + len) > size) {
                throw std::out_of_range("Access outside buffer");
            }
        }

        size_t offset;
        size_t size;
    };

    /**
     * A concrete implementation of a reader that operates on a byte array
     */
    class ByteArrayReader : public Reader {
    public:
        /**
         * Initialize the reader on a fixed data array
         *
         * @param ptr pointer to the first byte
         * @param off the offset to start
         * @param len the number of bytes in the buffer
         */
        ByteArrayReader(const uint8_t* ptr, size_t off, size_t len)
            : Reader(off, len),
              data(ptr) {
        }

        /**
         * Initialize the reader on a buffer oject
         *
         * @param buff the buffer object to read from
         * @param off the offset to start
         */
        ByteArrayReader(const Greenstack::Buffer& buff, size_t off = 0)
            : Reader(off, buff.getSize()),
              data(buff.getData()) {
        }

        /**
         * Initialize the reader on a std::vector
         *
         * @param vec the vector containing the data
         * @param off the offset to start
         */
        ByteArrayReader(const std::vector<uint8_t>& vec, size_t off = 0)
            : Reader(off, vec.size()),
              data(vec.data()) {

        }

        /**
         * Initialize the reader on a std::vector
         *
         * @param vec the vector containing the data
         * @param off the offset to start
         * @param end the last byte in the array
         */
        ByteArrayReader(const std::vector<uint8_t>& vec, size_t off, size_t end)
            : Reader(off, end),
              data(vec.data()) {
            if (end > vec.size()) {
                throw std::logic_error("Can't create a reader outside the vector");
            }
        }

        /**
         * Read a number of bytes from the underlying buffer
         *
         * @param ptr where to copy data
         * @param len the number of bytes to copy
         * @param off the offset to copy from
         */
        virtual void pread(uint8_t* ptr, size_t len, size_t off) const {
            guard(off, len);
            memcpy(ptr, data + off, len);
        }

    protected:
        /**
         * Pointer to the data
         */
        const uint8_t* data;
    };
}
