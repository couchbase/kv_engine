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
    const std::string uuid;
    std::string filename;
    std::size_t id{0};
    std::size_t offset{0};
    std::size_t length{0};
    const std::size_t max_read_size{0};
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
    const std::size_t chunk_size;
};
