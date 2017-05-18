/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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

#include <mcbp/protocol/datatype.h>
#include <mcbp/protocol/magic.h>
#include <mcbp/protocol/opcode.h>
#include <mcbp/protocol/response.h>
#include <mcbp/protocol/request.h>
#include <mcbp/protocol/status.h>


// This file contains various methods for the Memcached Binary Protocol
// used for backwards source compatibility
#include <memcached/protocol_binary.h>


#include <ostream>
#include <vector>

namespace cb {
namespace mcbp {

/**
 * Dump a raw packet to the named stream (the packet is expected to contain
 * a valid packet)
 *
 * @param packet pointer to the first byte of a correctly encoded mcbp packet
 * @param out where to dump the bytes
 */
void dump(const uint8_t* packet, std::ostream& out);

/**
 * Dump all of the messages within the stream
 *
 * @param buffer the buffer containing the data
 * @param out Where to dump the data
 */
void dumpStream(cb::byte_buffer buffer, std::ostream& out);


/**
 * Print a byte dump of a buffer in the following format:
 *
 * 0xoffset 0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x00    ........
 *
 * @param buffer The data do dump
 * @param out Where to dump it
 * @param offset The offset to print (note that the data to be displayed is
 *               specified in the buffer, and this is may be used if the
 *               buffer to display is a small segment of a larger blob)
 */
void dumpBytes(cb::byte_buffer buffer, std::ostream& out, size_t offset = 0);


namespace gdb {
/**
 * Parse the output from examine and return it as a vector of bytes.
 *
 * (gdb) x /24xb c->rcurr
 * 0x7f43387d7e7a: 0x81 0x0d 0x00 0x00 0x00 0x00 0x00 0x00
 * 0x7f43387d7e82: 0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x00
 * 0x7f43387d7e8a: 0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x00
 */
std::vector<uint8_t> parseDump(cb::byte_buffer blob);
} // namespace gdb

namespace lldb {
/**
 * Parse the output from examine and return it as a vector of bytes.
 *
 * (lldb) x -c 100 c->rbuf
 * 0xaddr: 81 0d 00 01 04 00 00 00 00 00 00 06 00 00 00 06  ................
 * 0xaddr: 14 bf f4 26 8a e0 00 00 00 00 00 00 61 61 81 0a  .��&.�......aa..
 */
std::vector<uint8_t> parseDump(cb::byte_buffer blob);
}
}
}
