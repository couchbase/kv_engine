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

#include <cstdint>
#include <string>
#include <vector>
#include <map>

namespace Greenstack {
    // Forward declaration of types used in the header to avoid including
    // all of them.
    class Writer;

    class ByteArrayReader;

    class Reader;

    class Request;

    class Response;

    class Message;


    /**
     * The FlexHeader represent the flexible header in the Message in the
     * Greenstack spec.
     */
    class FlexHeader {
    public:
        /**
         * Create a flex header from the requested stream
         *
         * @param reader where to read the data from
         */
        static FlexHeader create(Reader& reader);

        /**
         * Is this header empty or not
         */
        bool isEmpty() const {
            return header.empty();
        }

        void setLaneId(const std::string& lane);

        bool haveLaneId() const;

        std::string getLaneId() const;

        void setTXID(const std::string& txid);

        bool haveTXID() const;

        std::string getTXID() const;

        void setPriority(uint8_t priority);

        bool havePriority() const;

        uint8_t getPriority() const;

        void setDcpId(const std::string& dcpid);

        bool haveDcpId() const;

        std::string getDcpId() const;

        void setVbucketId(uint16_t vbid);

        bool haveVbucketId() const;

        uint16_t getVbucketId() const;

        void setHash(uint32_t hash);

        bool haveHash() const;

        uint32_t getHash() const;

        void setTimeout(uint32_t value);

        bool haveTimeout() const;

        uint32_t getTimeout() const;

        void setCommandTimings(const std::string& value);

        bool haveCommandTimings() const;

        std::string getCommandTimings() const;

    protected:
        size_t encode(Writer& writer) const;

        friend class Request;

        friend class Response;

        friend class Message;

        void insertField(uint16_t opcode, const uint8_t* value, size_t length);

        std::map<uint16_t, std::vector<uint8_t> > header;
    };
}
