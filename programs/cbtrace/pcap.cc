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
#include <string.h>
#include <cstdlib>
#include <stdio.h>
#include <iostream>
#include <stdexcept>

#include "pcap.h"

void Pcap::Parser::create(const char* fname) {
    file.create(fname);
    if (file.size < sizeof(header)) {
        throw std::runtime_error("Invalid file");
    }
    memcpy(&header, file.root, sizeof(header));
    curr = static_cast<uint8_t*>(file.root) + sizeof(header);
    offset += sizeof(header);

    if (header.magic != 0xa1b2c3d4) {
        throw std::runtime_error("Unsupported file magic");
    }

    if (header.network != 1) {
        throw std::runtime_error("Unsupported network type");
    }
}

bool Pcap::Parser::next(Pcap::Packet& packet) {
    if ((offset + sizeof(packet)) >= file.size) {
        return false;
    }

    memcpy(&packet.ts_sec, curr, 4);
    curr += 4;
    memcpy(&packet.ts_usec, curr, 4);
    curr += 4;
    memcpy(&packet.incl_len, curr, 4);
    curr += 4;
    memcpy(&packet.orig_len, curr, 4);
    curr += 4;
    packet.payload = curr;
    packet.data = NULL;
    curr += packet.incl_len;
    offset += packet.incl_len + 16;

    if (offset > file.size) {
        // truncated
        packet.incl_len = 0;
    }

    return true;
}
