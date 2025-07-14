/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "mutation_log_writer.h"
#include "mutation_log.h"

#include <bucket_logger.h>
#include <platform/crc32c.h>
#include <stdexcept>

MutationLogWriter::MutationLogWriter(
        std::string path,
        const size_t bs,
        cb::crypto::SharedEncryptionKey encryption_key,
        cb::crypto::Compression compression,
        std::function<void(std::string_view)> fileWriteTestHook)
    : fileWriteTestHook(std::move(fileWriteTestHook)),
      block(folly::IOBuf::createCombined(bs)),
      fileWriter(cb::crypto::FileWriter::create(
              encryption_key, path, 16_KiB, compression)),
      entryBuffer(bs) {
    Expects(bs >= MutationLogWriter::MinBlockSize &&
            bs <= MutationLogWriter::MaxBlockSize);

    // Write the initial file header!
    LogHeaderBlock header;
    header.set(gsl::narrow<uint32_t>(bs));
    std::copy_n(reinterpret_cast<const uint8_t*>(&header),
                sizeof(header),
                entryBuffer.data());
    fileWriter->write({reinterpret_cast<const char*>(entryBuffer.data()),
                       entryBuffer.size()});
    fileWriter->flush();
    this->fileWriteTestHook(__func__);

    // Leave room in the head for the CRC32
    block->append(sizeof(uint32_t));
}

MutationLogWriter::~MutationLogWriter() {
    try {
        close();
        fileWriteTestHook(__func__);
    } catch (const std::exception& e) {
        // The destructor cannot throw exceptions
        EP_LOG_WARN_CTX("~MutationLogWriter(): Ignoring exception",
                        {"error", e.what()});
    }
}

void MutationLogWriter::newItem(Vbid vbucket, const StoredDocKey& key) {
    if (!isEnabled()) {
        return;
    }
    auto* mle = MutationLogEntry::newEntry(
            entryBuffer.data(), MutationLogType::New, vbucket, key);
    writeEntry(*mle);
}

void MutationLogWriter::commit1() {
    if (!isEnabled()) {
        return;
    }

    auto* mle = MutationLogEntry::newEntry(
            entryBuffer.data(), MutationLogType::Commit1, Vbid(0));
    writeEntry(*mle);
}

void MutationLogWriter::commit2() {
    if (!isEnabled()) {
        return;
    }
    auto* mle = MutationLogEntry::newEntry(
            entryBuffer.data(), MutationLogType::Commit2, Vbid(0));
    writeEntry(*mle);
    flush();
    sync();
}

void MutationLogWriter::sync() {
    // not implemented for now
}

void MutationLogWriter::disable() {
    fileWriter.reset();
}

bool MutationLogWriter::isEnabled() const {
    if (!fileWriter) {
        return false;
    }
    return true;
}

std::size_t MutationLogWriter::getItemsLogged(MutationLogType type) const {
    return itemsLogged[static_cast<int>(type)];
}

void MutationLogWriter::close() {
    if (isEnabled()) {
        flush();
        try {
            fileWriter->flush();
            fileWriter->close();
        } catch (const std::exception& e) {
            fileWriter.reset();
            throw;
        }
    }

    fileWriter.reset();
}

void MutationLogWriter::writeEntry(const MutationLogEntry& mle) {
    size_t len(mle.len());
    if (len > block->tailroom()) {
        flush();
    }

    std::copy_n(
            reinterpret_cast<const uint8_t*>(&mle), len, block->writableTail());
    block->append(len);
    ++entries;
    ++itemsLogged[static_cast<int>(mle.type())];
}

void MutationLogWriter::flush() {
    struct BlockHeader {
        uint16_t crc;
        uint16_t entries;
    }* header = reinterpret_cast<struct BlockHeader*>(block->writableData());
    static_assert(sizeof(BlockHeader) == sizeof(uint32_t));

    if (entries == 0) {
        // We've not written anything to the block yet
        return;
    }

    // Fill the rest of the block with 0x00 as it is included in the checksum
    std::fill(block->writableTail(),
              block->writableData() + block->capacity(),
              0);

    header->entries = ntohs(entries);
    header->crc =
            ntohs(crc32c(block->data() + 2, block->capacity() - 2, 0) & 0xffff);
    try {
        fileWriter->write({reinterpret_cast<const char*>(block->data()),
                           block->capacity()});
    } catch (std::exception& e) {
        fileWriter.reset();
        throw;
    }
    fileWriteTestHook(__func__);
    block->trimEnd(block->length() - sizeof(BlockHeader));
    entries = 0;
}
