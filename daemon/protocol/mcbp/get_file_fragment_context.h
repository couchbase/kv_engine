/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "steppable_command_context.h"

#include <folly/Synchronized.h>
#include <folly/io/IOBuf.h>

/**
 * Implementation of the "GetFileFragment" command.
 *
 * The command takes the UUID of the snapshot in the command and the
 * value field contains a "metadata" section containing the fragment
 * to retrieve:
 *
 *    {
 *      "id": 0,        // The file id in the snapshot
 *      "offset":"0",   // The offset in the file to retrieve
 *      "length":"1234" // The number of bytes to read
 *    }
 *
 * Given that we can't do file IO in the worker thread it'll dispatch
 * a task to open the file before it returns and send the header.
 * If the connection supports sendfile we'll try to send the data
 * using sendfile, if not we'll schedule another task to read the
 * next chuck of data (50MB) then return to the worker thread context
 * and send the chunk to the client (then a new task for the next chunk
 * etc).
 */
class GetFileFragmentContext : public SteppableCommandContext {
public:
    enum class State : uint8_t {
        Initialize,
        SendResponseHeader,
        ReadFileChunk,
        ChainFileChunk,
        Done
    };

    explicit GetFileFragmentContext(Cookie& cookie);

    ~GetFileFragmentContext() override;

protected:
    cb::engine_errc step() override;

    cb::engine_errc initialize();
    cb::engine_errc send_response_header();
    cb::engine_errc read_file_chunk();
    cb::engine_errc chain_file_chunk();

    /// Helper to calculate the checksum of the given IOBuf and append it to the
    /// IOBuf
    void calculate_and_append_checksum(folly::IOBuf& iob);
    /// Helper to create an IOBuf with the given length + optional checksum
    /// space
    std::unique_ptr<folly::IOBuf> create_io_buf(size_t length) const;

    static size_t get_network_length(size_t file_read_length,
                                     size_t file_checksum_length);
    const std::string uuid;
    std::string filename;
    std::size_t id{0};
    std::size_t offset{0};
    /// How many bytes we need to read from the file.
    std::size_t length{0};
    /// How many bytes of the file are check-summed (0 not checksumming)
    std::size_t checksum_length{0};

    folly::Synchronized<std::unique_ptr<folly::IOBuf>> chunk;
    /// We're using a file_stream to read the file if we need
    /// to read it in chunks (i.e., not using sendfile).
    std::ifstream filestream;
    /// In configurations where we may use sendfile (Linux/MacOS
    /// and not TLS) we need a file descriptor to pass along
    /// to the sendfile(2) call
    int fd{-1};

    State state;

    /// The size of the chunks we read from the file.
    std::size_t chunk_size{0};
    /// How many bytes we need to send to the network. If check-summing this
    /// can be larger than the requested length, but always less than or equal
    /// to the max_read_size.
    std::size_t network_length{0};
};
