/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <mcbp/codec/crc_sink.h>

#include <fmt/format.h>
#include <gsl/gsl-lite.hpp>
#include <platform/crc32c.h>

#ifdef WIN32
#include <winsock2.h>
#else
#include <arpa/inet.h>
#endif

namespace cb::snapshot {

CRCSink::CRCSink(size_t checksummed_length,
                 size_t read_length,
                 cb::io::Sink& real_sink)
    : next_checksum_offset(checksummed_length),
      checksummed_length(checksummed_length),
      real_sink(real_sink) {
    Expects(checksummed_length > 0);
    Expects(read_length > 0);
    // If the file read does not divide evenly into the checksummed length then
    // figure out the tail file size and final checksum offset
    const size_t chunk_size = checksummed_length + sizeof(uint32_t);
    const size_t tail_remainder = read_length % chunk_size;
    if (tail_remainder) {
        const size_t whole_chunks = read_length / chunk_size;
        // The offset of where our tail begins
        tail_offset = whole_chunks * chunk_size;
        // The offset of the checksum of that tail
        tail_checksum_offset = (chunk_size * whole_chunks) +
                               (tail_remainder - sizeof(uint32_t));
    }
}

void CRCSink::sink(std::string_view data) {
    if (data.empty()) {
        return;
    }
    // data is on a checksum, could be partial or whole.
    if (network_stream_offset >= next_checksum_offset &&
        network_stream_offset <= next_checksum_offset + sizeof(uint32_t)) {
        // And resume processing
        sink(consume_checksum(data));
        return;
    }

    // data.front is a file byte
    // data could span just file bytes
    // data could span file bytes and a partial checksum
    // data could span file bytes and a whole checksum.

    // Check if we're now in the tail which is not a whole chunk
    if (tail_offset && network_stream_offset >= tail_offset) {
        // Setup for the final checksum
        next_checksum_offset = tail_checksum_offset;
    }

    auto file_bytes =
            data.substr(0,
                        std::min(next_checksum_offset - network_stream_offset,
                                 data.size()));
    network_stream_offset += file_bytes.size();
    data.remove_prefix(file_bytes.size());
    current_crc = crc32c(file_bytes, current_crc);

    // We have file bytes - write them to the real sink.
    writeFileBytes(file_bytes);

    // data now is:
    //  - either empty
    //  - checksum (whole or partial)
    //  - checksum and more file bytes
    // either case call consume_checkum and resurse.
    sink(consume_checksum(data));
}

std::string_view CRCSink::consume_checksum(std::string_view data) {
    if (data.empty()) {
        return data;
    }

    // Drain data
    while (data.size() && partial_checksum.size() < sizeof(uint32_t)) {
        partial_checksum.push_back(data.front());
        data.remove_prefix(1);
        ++network_stream_offset;
    }

    // We have a whole checksum, check if it matches the current crc.
    if (partial_checksum.size() == sizeof(uint32_t)) {
        uint32_t checksum_value = ntohl(
                *reinterpret_cast<const uint32_t*>(partial_checksum.data()));
        if (current_crc != checksum_value) {
            // todo: format error message with all member state
            throw std::runtime_error(fmt::format(
                    "CRCSink: mismatch: calculated: 0x{:08x}, found: 0x{:08x}, "
                    "network_stream_offset: {}, "
                    "next_checksum_offset: {}, checksummed_length: {}, "
                    "tail_offset: {}, tail_checksum_offset: {}",
                    current_crc,
                    checksum_value,
                    network_stream_offset,
                    next_checksum_offset,
                    checksummed_length,
                    tail_offset,
                    tail_checksum_offset));
        }
        current_crc = 0;
        partial_checksum.resize(0);
        next_checksum_offset += (checksummed_length + sizeof(uint32_t));
    }
    return data;
}

} // namespace cb::snapshot
