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

#include "memorymap.h"

#include <stdint.h>
#include <ostream>

namespace Pcap {

    class Header {
    public:
        uint32_t magic;
        /* magic number */
        uint16_t major;
        /* major version number */
        uint16_t minor;
        /* minor version number */
        int32_t thiszone;
        /* GMT to local correction */
        uint32_t sigfigs;
        /* accuracy of timestamps */
        uint32_t snaplen;
        /* max length of captured packets, in octets */
        uint32_t network;   /* data link type */
    };

    class Packet {
    public:
        uint32_t ts_sec;
        /* timestamp seconds */
        uint32_t ts_usec;
        /* timestamp microseconds */
        uint32_t incl_len;
        /* number of octets of packet saved in file */
        uint32_t orig_len;
        /* actual length of packet */

        uint8_t* payload;
        uint8_t* ip;
        /* pointer to the IP header */
        uint8_t* tcp;
        /* Pointer to the TCP header */
        uint8_t* data;           /* Pointer to the TCP data */
    };

    class Parser {
    public:
        Parser()
            : curr(NULL),
              offset(0) { }

        void create(const char* fname);

        Header& getHeader(void) { return header; }

        bool next(Packet& packet);

    private:
        MemoryMappedFile file;
        Header header;
        uint8_t* curr;
        size_t offset;
    };

}
