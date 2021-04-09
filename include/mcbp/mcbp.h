/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

// We use [nh]to[nh]ll in a lot of these headers
#include <mcbp/protocol/datatype.h>
#include <mcbp/protocol/feature.h>
#include <mcbp/protocol/header.h>
#include <mcbp/protocol/magic.h>
#include <mcbp/protocol/opcode.h>
#include <mcbp/protocol/request.h>
#include <mcbp/protocol/response.h>
#include <mcbp/protocol/status.h>

// This file contains various methods for the Memcached Binary Protocol
// used for backwards source compatibility
#include <memcached/protocol_binary.h>

#include <nlohmann/json_fwd.hpp>

#include <chrono>
#include <iosfwd>
#include <vector>

namespace cb::mcbp {
/**
 * Dump a raw packet to the named stream (the packet is expected to contain
 * a valid packet)
 *
 * @param packet pointer to the first byte of a correctly encoded mcbp packet
 * @param out where to dump the bytes
 */
void dump(const uint8_t* packet, std::ostream& out);

void dump(const Header& header, std::ostream& out);

/**
 * Dump all of the messages within the stream
 *
 * @param buffer the buffer containing the data
 * @param out Where to dump the data
 */
void dumpStream(cb::const_byte_buffer buffer, std::ostream& out);

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
void dumpBytes(cb::const_byte_buffer buffer,
               std::ostream& out,
               size_t offset = 0);

namespace gdb {
/**
 * Parse the output from examine and return it as a vector of bytes.
 *
 * (gdb) x /24xb c->rcurr
 * 0x7f43387d7e7a: 0x81 0x0d 0x00 0x00 0x00 0x00 0x00 0x00
 * 0x7f43387d7e82: 0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x00
 * 0x7f43387d7e8a: 0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x00
 */
std::vector<uint8_t> parseDump(cb::const_byte_buffer blob);
} // namespace gdb

namespace lldb {
/**
 * Parse the output from examine and return it as a vector of bytes.
 *
 * (lldb) x -c 100 c->rbuf
 * 0xaddr: 81 0d 00 01 04 00 00 00 00 00 00 06 00 00 00 06  ................
 * 0xaddr: 14 bf f4 26 8a e0 00 00 00 00 00 00 61 61 81 0a  .��&.�......aa..
 */
std::vector<uint8_t> parseDump(cb::const_byte_buffer blob);
}

namespace sla {

/**
 * Reconfigure the SLA module by parsing the provided JSON document
 *
 * @param doc the root element in of the document
 * @param apply apply the values in the file or just parse the value
 * @throws std::invalid_argument if there is a format error in the provided
 *                               document
 */
void reconfigure(const nlohmann::json& doc, bool apply = true);

/**
 * Reconfigure the SLA module by parsing the configuration files stored
 * under the root installation:
 *    <root>/etc/couchbase/kv/copcode-attributes.json
 *    <root>/etc/couchbase/kv/copcode-attributes.d/<*>.json
 *
 * @param root The root directory of the couchbase installation
 */
void reconfigure(const std::string& root);

/**
 * Reconfigure the SLA module by parsing the configuration files stored
 * under the root installation:
 *    <root>/etc/couchbase/kv/copcode-attributes.json
 *    <root>/etc/couchbase/kv/copcode-attributes.d/<*>.json
 *
 * and then apply the specified override
 *
 * @param root The root directory of the couchbase installation
 * @param override The final override specification
 */
void reconfigure(const std::string& root, const nlohmann::json& override);

/**
 * Get the threshold for an opcode to be reported as a slow operation.
 * Note that even if the threshold is reported in ns, the most typical
 * values here would be in the us/ms range. According to
 * http://en.cppreference.com/w/cpp/chrono/duration they'll still require
 * a 64 bit integer type to hold the value so we can might as well use
 * ns.
 *
 * @param opcode the opcode to get get the threshold for
 * @return the number of nanoseconds for the threshold.
 */
std::chrono::nanoseconds getSlowOpThreshold(cb::mcbp::ClientOpcode opcode);

/**
 * Get a JSON representation of the current SLA setting.
 *
 * This method is mostly intended for using as part of unit testing of
 * the server and not intended to be used in production. It currently
 * always report the threshold for all operations in ms.
 */
nlohmann::json to_json();

/**
 * Get the slow operation threshold for the given doc (this should be
 * the per-opcode section of the full SLA entry
 */
std::chrono::nanoseconds getSlowOpThreshold(const nlohmann::json& doc);

} // namespace sla
} // namespace cb::mcbp
