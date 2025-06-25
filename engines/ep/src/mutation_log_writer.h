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

#include <cbcrypto/common.h>
#include <cbcrypto/file_writer.h>
#include <folly/io/IOBuf.h>
#include <memcached/storeddockey_fwd.h>
#include <memcached/vbucket.h>
#include <mutation_log_entry.h>
#include <platform/byte_literals.h>
#include <functional>

/**
 * A class used to write the mutation log
 *
 * The MutationLogWriter class differs from MutationLog that it use RAII style
 * and throws exceptions to the caller to allow the caller to properly deal
 * with the error (log all details etc)
 */
class MutationLogWriter {
public:
    constexpr static size_t MinBlockSize = 512;
    constexpr static size_t MaxBlockSize = 1_MiB;

    MutationLogWriter(
            std::string path,
            size_t blocksize,
            cb::crypto::SharedEncryptionKey encryption_key = {},
            cb::crypto::Compression compression = cb::crypto::Compression::None,
            std::function<void(std::string_view)> fileWriteTestHook = [](auto) {
            });
    ~MutationLogWriter();

    MutationLogWriter(const MutationLogWriter&) = delete;
    const MutationLogWriter& operator=(const MutationLogWriter&) = delete;

    void newItem(Vbid vbucket, const StoredDocKey& key);

    void commit1();

    void commit2();

    void flush();

    void sync();

    void disable();

    bool isEnabled() const;

    std::size_t getItemsLogged(MutationLogType type) const;

    void close();

protected:
    void writeEntry(const MutationLogEntry& mle);

    std::function<void(std::string_view)> fileWriteTestHook;
    // The current block we're writing
    std::unique_ptr<folly::IOBuf> block;
    std::unique_ptr<cb::crypto::FileWriter> fileWriter;
    std::vector<uint8_t> entryBuffer;

    //! Items logged by type.
    std::array<size_t, size_t(MutationLogType::NumberOfTypes)> itemsLogged = {};
    /// The number of entries in the current block
    uint16_t entries = 0;
};
