/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <platform/sink.h>

#include <cstdint>
#include <string_view>
#include <vector>

namespace cb::snapshot {

/**
 * When GetFileFragment is invoked with checksumming enabled the returned data
 * will be formatted as:
 *
 * {n-bytes file}{4b checksum}
 * {n-bytes file}{4b checksum}
 * ...
 * {1 to n-bytes file}{4b checksum} (tail of file)
 *
 * This class assumes nothing about n (n is at least > 0) or the data.size
 * passed into sink. The network reading layer may call sink with varying buffer
 * size that are various sub-spans of the overall checksummed file stream.
 * CRCSink processes the stream irrespective of n or the size of data passed to
 * sink.
 */
class CRCSink : public cb::io::Sink {
public:
    CRCSink(size_t checksum_length,
            size_t read_length,
            cb::io::Sink& real_sink);

    void sink(std::string_view data) override;

    std::size_t fsync() override {
        return real_sink.fsync();
    }
    std::size_t close() override {
        return real_sink.close();
    }

    /// @return the number of bytes written from the real_sink
    std::size_t getBytesWritten() const override {
        return real_sink.getBytesWritten();
    }

    /// @return the number of bytes passed (written) to the real_sink for this
    /// instance of the CRCSink
    size_t getBytesPassedThrough() const {
        return bytes_passed_through;
    }

private:
    void writeFileBytes(std::string_view data) {
        bytes_passed_through += data.size();
        real_sink.sink(data);
    }

    /**
     * Helper method that will consume checksum bytes from the data buffer.
     *
     * @param data The data to consume the checksum from.
     * @return The data after the checksum
     */
    std::string_view consume_checksum(std::string_view data);

    // offset into the network stream
    size_t network_stream_offset{0};
    // the next offset of the next checksum
    size_t next_checksum_offset{0};
    // length of the checksummed data
    size_t checksummed_length{0};

    // bytes passed through to the real sink
    size_t bytes_passed_through{0};

    // pre-calculated offsets for the tail of the file
    size_t tail_offset{0};
    size_t tail_checksum_offset{0};

    // the sink to write the data to
    cb::io::Sink& real_sink;

    // The current accumulated crc for the current file chunk
    uint32_t current_crc{0};

    // Partial checksum bytes if a sink call ends mid-checksum
    std::vector<uint8_t> partial_checksum;
};

} // namespace cb::snapshot