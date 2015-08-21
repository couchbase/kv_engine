/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

#include <libgreenstack/Buffer.h>
#include "config.h"

#include <iostream>

namespace Greenstack {

    /**
     * The Writer class is an abstract class that allows for writing to a
     * stream in network byte order.
     */
    class Writer {
    public:
        /**
         * Create a new writer that starts writing at the specified offset
         *
         * @param off the offset to start inserting data
         */
        Writer(size_t off = 0)
            : offset(off) {

        }

        virtual ~Writer() {

        }

        /**
         * Write a single byte to the stream and advance the current pointer
         *
         * @param val the value to insert
         */
        void write(uint8_t val) {
            pwrite(val, offset);
            skip(sizeof(val));
        }

        /**
         * Write a 16 bit word in network byte order to the stream and advance
         * the current pointer
         *
         * @param val the value to insert
         */
        void write(uint16_t val) {
            pwrite(val, offset);
            skip(sizeof(val));
        }

        /**
         * Write a 32 bit word in network byte order to the stream and advance
         * the current pointer
         *
         * @param val the value to insert
         */
        void write(uint32_t val) {
            pwrite(val, offset);
            skip(sizeof(val));
        }

        /**
         * Write a 64 bit word in network byte order to the stream and advance
         * the current pointer
         *
         * @param val the value to insert
         */
        void write(uint64_t val) {
            pwrite(val, offset);
            skip(sizeof(val));
        }

        /**
         * Write a sequence of bytes to the stream and advance the current
         * pointer.
         *
         * @param ptr pointer to the first byte to insert
         * @param len the number of bytes to insert
         */
        void write(const uint8_t* ptr, size_t len) {
            pwrite(ptr, len, offset);
            skip(len);
        }

        /**
         * Get the current offset in the buffer
         *
         * @return the current offset in the buffer
         */
        size_t getOffset() const {
            return offset;
        }

        /**
         * Write a byte at the specified location in the buffer.
         *
         * @param value the value to write
         * @param off the offset to write the value
         */
        void pwrite(uint8_t value, size_t off) {
            pwrite(reinterpret_cast<uint8_t*>(&value), sizeof(value), off);
        }

        /**
         * Write a 16 bit word at the specified location in the buffer without
         * doing any bytorder translation.
         *
         * @param value the value to write
         * @param off the offset to write the value
         */
        void pwrite(uint16_t val, size_t off) {
            uint16_t value = htons(val);
            pwrite(reinterpret_cast<uint8_t*>(&value), sizeof(value), off);
        }

        /**
         * Write a 32 bit word at the specified location in the buffer without
         * doing any bytorder translation.
         *
         * @param value the value to write
         * @param off the offset to write the value
         */
        void pwrite(uint32_t val, size_t off) {
            uint32_t value = htonl(val);
            pwrite(reinterpret_cast<uint8_t*>(&value), sizeof(value), off);
        }

        /**
         * Write a 64 bit word at the specified location in the buffer without
         * doing any bytorder translation.
         *
         * @param value the value to write
         * @param off the offset to write the value
         */
        void pwrite(uint64_t val, size_t off) {
            uint64_t value = htonll(val);
            pwrite(reinterpret_cast<uint8_t*>(&value), sizeof(value), off);
        }

        /**
         * Write a sequence of bytes to a given location in the buffer.
         *
         * @param ptr pointer to the first byte to write
         * @param len the number of bytes to write
         * @param off the offset to write the value
         */
        virtual void pwrite(const uint8_t* ptr, size_t len, size_t off) = 0;

        /**
         * Move the current pointer.
         *
         * @param nbytes the number of bytes to move the pointer.
         */
        void skip(size_t nbytes = 1) {
            offset += nbytes;
        }

    protected:
        /**
         * The current offset in the buffer
         */
        size_t offset;
    };

    /**
     * A concrete implementation of a writer that operates on a vector
     * of bytes. The underlying vector will automatically grow to fit
     * all of the data written to the writer.
     */
    class VectorWriter : public Writer {
    public:
        /**
         * Initialize the VectorWriter
         *
         * @param vec the vector to operate on
         * @param start the start offset in the vector
         */
        VectorWriter(std::vector<uint8_t>& vec, size_t start = 0)
            : Writer(0),
              startOffset(start),
              vector(vec) {
            // EMPTY
        }

        /**
         * Implementation of the virtual function pwrite to insert the data
         * at the correct location in the buffer. Grow if necessary
         *
         * @param ptr the pointer to the first byte
         * @param len the number of bytes to insert
         * @param off offset in the buffer
         */
        virtual void pwrite(const uint8_t* ptr, size_t len, size_t off) override {
            if ((startOffset + off + len) > vector.size()) {
                vector.resize(startOffset + off + len);
            }
            memcpy(vector.data() + off + startOffset, ptr, len);
        }

    private:
        /**
         * The initial offset in the vector
         */
        const size_t startOffset;
        /**
         * The vector to receive all the data written to the writer.
         */
        std::vector<uint8_t>& vector;
    };

    /**
     * A concrete implementation of a Writer that writes data to a Buffer
     */
    class BufferWriter : public Writer {
    public:
        /**
         * Initialize the BufferWriter
         *
         * @param buff the buffer so store the data in
         * @param start the initial offset to start writing data
         */
        BufferWriter(Buffer& buff, size_t start = 0)
            : Writer(0),
              startOffset(start),
              buffer(buff) {
            // EMPTY
        }

        /**
         * Implementation of the virtual function pwrite to insert the data
         * at the correct location in the buffer. Grow if necessary
         *
         * @param ptr the pointer to the first byte
         * @param len the number of bytes to insert
         * @param off offset in the buffer
         */
        virtual void pwrite(const uint8_t* ptr, size_t len, size_t off) {
            if ((startOffset + off + len) > buffer.getSize()) {
                // May throw exception if the underlying buffer don't support resize
                buffer.resize(startOffset + off + len);
            }
            memcpy(buffer.getData() + off + startOffset, ptr, len);
        }

    private:
        /**
         * The initial offset in the vector
         */
        const size_t startOffset;
        /**
         * The buffer to receive all of the data written to the writer
         */
        Buffer& buffer;
    };

}
