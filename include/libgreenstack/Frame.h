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

#include <cstddef>
#include <cstdint>
#include <libgreenstack/Reader.h>
#include <memory>
#include <vector>

namespace Greenstack {
    class Message;

    typedef std::unique_ptr<Message> UniqueMessagePtr;

    /**
     * The Frame class represent a single frame in the Greenstack protocol. It
     * is purely a static class and only contains methods to encode/decode a
     * Message object.
     *
     * @todo Define a set of exception types to throw from create()
     */
    class Frame {
    public:
        /**
         * Encode a message object into a frame and store the result
         * in a vector
         *
         * @param message the message to encode
         * @param vector the destination
         * @param offset where to store the object
         * @return the number of bytes the Frame object occupied
         */
        static size_t encode(const Message& message,
                             std::vector<uint8_t>& vector, size_t offset = 0) {
            return encode(&message, vector, offset);
        }

        /**
         * Encode a message object into a frame and store the result
         * in a vector
         *
         * @param message the message to encode
         * @param vector the destination
         * @param offset where to store the object
         * @return the number of bytes the Frame object occupied
         */
        static size_t encode(const Message* message,
                             std::vector<uint8_t>& vector, size_t offset = 0);


        /**
         * Create a Message from a reader
         *
         * @param reader the reader object to read out the data for the frame
         * @return the newly created message
         */
        static UniqueMessagePtr createUnique(Reader& reader);

    private:
        /**
         * Create a Message from a reader
         *
         * @param reader a reader object to read out the data for the frame
         * @return the newly created message
         */
        static Message* create(Reader& reader);
    };
}
