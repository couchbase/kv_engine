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

#include <limits>
#include <vector>
#include <cstdint>
#include <libgreenstack/FlexHeader.h>
#include <libgreenstack/Opcodes.h>
#include <libgreenstack/Reader.h>
#include <ostream>

namespace Greenstack {
    class VectorWriter;

    /**
     * The Message class represents the message packed inside the frame
     * in the Greenstack protocol. It may be either a Request or a Response.
     */
    class Message {
    public:
        /**
         * Factory method to create a Message object off the given reader.
         *
         * @todo figure out if this should be protected or not (it should
         *       only be called from Frame and the test code...
         *
         * @param reader the stream to read data from
         * @param size the number of bytes in the reader belonging to this
         *             message
         * @return a newly created message
         */
        static Message* create(Reader& reader, size_t size);

        /**
         * Release all allocated resources
         */
        virtual ~Message();

        /**
         * Set the opaque field in the message. The value in the opaque filed
         * is unspecified unless this method is called.
         *
         * @param value the value insert in the opaque field
         */
        void setOpaque(uint32_t value);

        /**
         * Get the opaque field from the message.
         *
         * @return the opaque set in the message
         */
        uint32_t getOpaque() const;

        /**
         * Set the opcode for the message.
         *
         * @param value the opcode to insert in the opaque field
         */
        void setOpcode(const Opcode& value);

        /**
         * Get the opcode in the message
         *
         * @return the opcode for the message
         */
        Opcode getOpcode() const;

        /**
         * Set or clear the fence bit in the flags section
         *
         * @param enable if true the bit is set otherwise cleared
         */
        void setFenceBit(bool enable);

        /**
         * Check to see if the fence bit is set
         *
         * @return true if set, false otherwise
         */
        bool isFenceBitSet() const;

        /**
         * Set or clear the more bit in the flags section
         *
         * @param enable if true the bit is set otherwise cleared
         */
        void setMoreBit(bool enable);

        /**
         * Check to see if the more bit is set
         *
         * @return true if set, false otherwise
         */
        bool isMoreBitSet() const;

        /**
         * Set or clear the quiet bit in the flags section
         *
         * @param enable if true the bit is set otherwise cleared
         */
        void setQuietBit(bool enable);

        /**
          * Check to see if the quiet bit is set
          *
          * @return true if set, false otherwise
          */
        bool isQuietBitSet() const;

        /**
         * Get the flex header
         *
         * @return the flex header
         */
        Greenstack::FlexHeader& getFlexHeader();

        /**
         * Get a const version of the flex header from a const message
         *
         * @return the flex header
         */
        const Greenstack::FlexHeader& getFlexHeader() const;

        /**
         * Set the payload for the message
         *
         * @param data the payload to insert into the message
         */
        void setPayload(std::vector<uint8_t>& data);

        /**
         * Encode the content of this Request into the provided
         * vector at the given offset. Encode may call resize() in
         * order to grow the buffer to fit the data. This <em>may</em>
         * invalidate any pointers previously held pointing into
         * the buffer.
         *
         * @param vector the destination for the encoded packet
         * @param offset the offset inside the vector to encode the
         *               request
         * @return the number of bytes the encoded object occupied
         *
         * @todo use a Writer
         */
        virtual size_t encode(std::vector<uint8_t>& vector,
                              size_t offset = 0) const;

    protected:
        /**
         * Protected destructor to create a message object
         *
         * @param response set to true if this is a response message, false
         *        for a request object
         */
        Message(bool response);

        /**
         * validate that the payload is correct. Throw an exception otherwise
         */
        virtual void validate();

        /**
         * This messages opaque field
         */
        uint32_t opaque;

        /**
         * This messages opcode
         */
        Opcode opcode;

        /**
         * The flags in the message
         */
        struct Flags {
            bool response
                : 1;
            bool flexHeaderPresent
                : 1;
            bool fence
                : 1;
            bool more
                : 1;
            bool quiet
                : 1;
            bool unassigned
                : 2;
            bool next
                : 1;
        } flags;

        /**
         * The flex header in the message
         */
        FlexHeader flexHeader;

        /**
         * The actual paylad in the message
         * @todo shoud I refactor this to an abstract buffer?
         */
        std::vector<uint8_t> payload;

    private:
        /**
         * Private factory method to create and initialize the Message
         * object
         *
         * @param response set to true if a response object should be created
         *                 false to create a request object
         * @param opcode the opcode to fill in
         */
        static Message* createInstance(bool response, const Opcode& opcode);
    };
}
