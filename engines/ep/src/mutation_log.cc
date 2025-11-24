/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <fcntl.h>
#include <platform/crc32c.h>
#include <platform/dirutils.h>
#include <hdrhistogram/hdrhistogram.h>
#include <platform/histogram.h>
#include <platform/strerror.h>
#include <sys/stat.h>
#include <algorithm>
#include <string>
#include <system_error>
#include <utility>

#include "bucket_logger.h"
#include "ep_engine.h"
#include "kv_bucket.h"
#include "mutation_log.h"
#include "vbucket.h"

#include <cbcrypto/file_reader.h>

/**
 * A class to allow for random access to the cb::crypto::FileReader (which
 * only support sequential read). If the caller tries to read a block
 * earlier than the current block we need to reopen the file and seek
 * to the required block. Luckily for us the primary use case is
 * sequential reads.
 */
class RandomIoFileReader {
public:
    RandomIoFileReader(
            std::filesystem::path path,
            std::function<cb::crypto::SharedKeyDerivationKey(std::string_view)>
                    keyLookupFunction)
        : path(std::move(path)), lookupFunction(std::move(keyLookupFunction)) {
        open();
    }

    std::size_t pread(std::span<uint8_t> blob, uint64_t offset);

protected:
    void open() {
        current_block.clear();
        current_offset = 0;
        fileReader = cb::crypto::FileReader::create(path, lookupFunction);
    }

    std::filesystem::path path;
    std::function<cb::crypto::SharedKeyDerivationKey(std::string_view)>
            lookupFunction;
    uint64_t current_offset = 0;
    std::string current_block;
    std::unique_ptr<cb::crypto::FileReader> fileReader;
};

std::size_t RandomIoFileReader::pread(std::span<uint8_t> blob,
                                      uint64_t offset) {
    if (offset < current_offset) {
        // we can't rewind the buffer. Reopen and seek forward
        open();
    }

    // We need to seek the file up to the current block
    if (offset != current_offset) {
        std::vector<uint8_t> buffer(offset - current_offset);
        current_offset += fileReader->read(buffer);
        if (offset != current_offset) {
            throw std::runtime_error("Short read!!");
        }
    }

    auto nr = fileReader->read(blob);
    current_offset += nr;
    return nr;
}

MutationLogReader::MutationLogReader(
        std::string path,
        std::function<cb::crypto::SharedKeyDerivationKey(std::string_view)>
                keyLookupFunction,
        std::function<void()> fileIoTestingHook)
    : fileIoTestingHook(std::move(fileIoTestingHook)),
      logPath(std::move(path)),
      openTimePoint(cb::time::steady_clock::now()),
      blockSize(0),
      resumeItr(end()),
      random_io_reader(std::make_unique<RandomIoFileReader>(
              this->logPath, std::move(keyLookupFunction))) {
    for (auto& ii : itemsLogged) {
        ii.store(0);
    }
    logSize.store(0);

    try {
        readInitialBlock();
    } catch (ShortReadException&) {
        close();
        throw;
    }
}

MutationLogReader::~MutationLogReader() {
    EP_LOG_INFO("{}", *this);
    close();
}

static bool validateHeaderBlockVersion(MutationLogVersion version) {
    return version == MutationLogVersion::V4;
}

void MutationLogReader::readInitialBlock() {
    std::array<uint8_t, LogHeaderBlock::HeaderSize> buf;
    const auto bytesread = random_io_reader->pread(buf, 0);
    if (bytesread != sizeof(buf)) {
        EP_LOG_WARN_CTX(
                "MutationLogReader::readInitialBlock: Failed to read initial "
                "block",
                {"path", getLogFile()},
                {"error", strerror(errno)});
        throw ShortReadException();
    }

    headerBlock.set(buf);
    if (!validateHeaderBlockVersion(headerBlock.version())) {
        throw ReadException("HeaderBlock version is unknown " +
                            std::to_string(int(headerBlock.version())));
    }

    if (headerBlock.blockCount() != 1) {
        throw ReadException("HeaderBlock blockCount mismatch " +
                            std::to_string(headerBlock.blockCount()));
    }

    blockSize = headerBlock.blockSize();
}

bool MutationLogReader::isOpen() const {
    if (random_io_reader) {
        return true;
    }
    return false;
}

void MutationLogReader::close() {
    random_io_reader.reset();
}

// ----------------------------------------------------------------------
// Mutation log iterator
// ----------------------------------------------------------------------

MutationLogReader::iterator::iterator(const MutationLogReader* l, bool e)
    : log(l),
      p(buf.begin()),
      offset(l->header().blockSize() * l->header().blockCount()),
      items(0),
      isEnd(e) {
}

MutationLogReader::iterator::iterator(const MutationLogReader::iterator& mit)
    : log(mit.log),
      entryBuf(mit.entryBuf),
      buf(mit.buf),
      p(buf.begin() + (mit.p - mit.buf.begin())),
      offset(mit.offset),
      items(mit.items),
      isEnd(mit.isEnd) {
}

MutationLogReader::iterator& MutationLogReader::iterator::operator=(
        const MutationLogReader::iterator& other) {
    if (this == &other) {
        return *this;
    }
    log = other.log;
    entryBuf = other.entryBuf;
    buf = other.buf;
    p = buf.begin() + (other.p - other.buf.begin());
    offset = other.offset;
    items = other.items;
    isEnd = other.isEnd;

    return *this;
}

MutationLogReader::iterator::~iterator() = default;

void MutationLogReader::iterator::prepItem() {
    const auto copyLen =
            MutationLogEntry::newEntry(p, bufferBytesRemaining())->len();
    Expects(copyLen <= LOG_ENTRY_BUF_SIZE);
    entryBuf.resize(LOG_ENTRY_BUF_SIZE);
    std::copy_n(p, copyLen, entryBuf.begin());
}

size_t MutationLogReader::iterator::getCurrentEntryLen() const {
    Expects(log->headerBlock.version() == MutationLogVersion::V4);
    return MutationLogEntry::newEntry(entryBuf.begin(), entryBuf.size())->len();
}

MutationLogReader::iterator& MutationLogReader::iterator::operator++() {
    if (--items == 0) {
        nextBlock();
    } else {
        p += getCurrentEntryLen();

        prepItem();
    }
    return *this;
}

bool MutationLogReader::iterator::operator==(
        const MutationLogReader::iterator& rhs) const {
    return log->random_io_reader == rhs.log->random_io_reader &&
           ((isEnd == rhs.isEnd) ||
            (offset == rhs.offset && items == rhs.items));
}

bool MutationLogReader::iterator::operator!=(
        const MutationLogReader::iterator& rhs) const {
    return ! operator==(rhs);
}

MutationLogReader::MutationLogEntryHolder MutationLogReader::iterator::operator*() {
    Expects(log->headerBlock.version() == MutationLogVersion::Current);
    return {entryBuf.data(), false /*not allocated*/};
}

size_t MutationLogReader::iterator::bufferBytesRemaining() {
    return buf.size() - (p - buf.begin());
}

uint16_t MutationLogReader::calculateCrc(cb::const_byte_buffer data) const {
    Expects(headerBlock.version() == MutationLogVersion::V4);
    const auto crc32 = crc32c(data.data(), data.size(), 0);
    const uint16_t computed_crc16(crc32 & 0xffff);
    return computed_crc16;
}

void MutationLogReader::iterator::nextBlock() {
    if (!log->isOpen()) {
        throw std::logic_error(
                "MutationLogReader::iterator::nextBlock: log is not open");
    }

    buf.resize(log->header().blockSize());
    const auto bytesread = log->random_io_reader->pread(buf, offset);
    if (bytesread == 0) {
        isEnd = true;
        return;
    }
    if (bytesread != buf.size()) {
        EP_LOG_WARN("FATAL: too few bytes read in access log '{}': {}",
                    log->getLogFile(),
                    strerror(errno));
        throw ShortReadException();
    }
    offset += bytesread;

    // block starts with 2 byte crc and 2 byte item count
    const auto crc16 = log->calculateCrc(
            {buf.data() + sizeof(uint16_t), buf.size() - sizeof(uint16_t)});
    uint16_t retrieved_crc16;
    memcpy(&retrieved_crc16, buf.data(), sizeof(retrieved_crc16));
    retrieved_crc16 = ntohs(retrieved_crc16);
    if (retrieved_crc16 != crc16) {
        throw CRCReadException();
    }

    std::copy_n(buf.data() + sizeof(uint16_t),
                sizeof(uint16_t),
                reinterpret_cast<uint8_t*>(&items));

    items = ntohs(items);

    // adjust p so it skips the 2 byte crc and 2 byte item count and points to
    // the first item.
    p = buf.begin() + sizeof(uint16_t) + sizeof(uint16_t);

    prepItem();
}

void MutationLogReader::resetCounts(size_t* items) {
    for (int i(0); i < int(MutationLogType::NumberOfTypes); ++i) {
        itemsLogged[i] = items[i];
    }
}

// ----------------------------------------------------------------------
// Reading entries
// ----------------------------------------------------------------------

bool MutationLogHarvester::load() {
    bool clean(false);
    std::set<uint16_t> shouldClear;
    for (const auto& le : mlog) {
        ++itemsSeen[int(le->type())];
        clean = false;

        switch (le->type()) {
        case MutationLogType::New:
            if (vbid_set.contains(le->vbucket())) {
                loading[le->vbucket()].emplace(le->key());
            }
            break;
        case MutationLogType::Commit2:
            clean = true;

            for (auto vb : vbid_set) {
                for (auto& item : loading[vb]) {
                    committed[vb].emplace(item);
                }
            }
            loading.clear();
            break;
        case MutationLogType::Commit1:
            // nothing in particular
            break;
        case MutationLogType::NumberOfTypes:
        default:
            throw std::logic_error(
                    "MutationLogHarvester::load: Invalid log "
                    "entry type:" +
                    to_string(le->type()));
        }
    }
    return clean;
}

bool MutationLogHarvester::loadBatchAndApply(size_t limit,
                                        void* arg,
                                        mlCallbackWithQueue mlc,
                                        bool removeNonExistentKeys) {
    const bool more = loadBatch(limit);
    apply(arg, mlc, removeNonExistentKeys);
    return more;
}

bool MutationLogHarvester::loadBatch(size_t limit) {
    size_t count = 0;
    committed.clear();
    auto& it = mlog.resume();
    for (; it != mlog.end() && count < limit; ++it) {
        const auto& le = *it;
        ++itemsSeen[int(le->type())];

        switch (le->type()) {
        case MutationLogType::New:
            if (vbid_set.contains(le->vbucket())) {
                committed[le->vbucket()].emplace(le->key());
                count++;
            }
            break;

        case MutationLogType::Commit1:
        case MutationLogType::Commit2:
        case MutationLogType::NumberOfTypes:
            // We ignore COMMIT2 for Access log, was only relevent to the
            // 'proper' mutation log.
            // all other types ignored as well.
            break;
        }
    }
    return it != mlog.end();
}

void MutationLogHarvester::apply(void *arg, mlCallback mlc) {
    for (const auto vb : vbid_set) {
        for (const auto& key : committed[vb]) {
            if (!mlc(arg, vb, key)) { // Stop loading from an access log
                return;
            }
        }
    }
}

void MutationLogHarvester::apply(void* arg,
                                 mlCallbackWithQueue mlc,
                                 bool removeNonExistentKeys) {
    for (const auto vb : vbid_set) {
        if (removeNonExistentKeys) {
            this->removeNonExistentKeys(vb);
        }

        if (committed[vb].empty()) {
            // No valid items for this vBucket; move to next.
            continue;
        }

        if (!mlc(vb, committed[vb], arg)) {
            return;
        }
        committed[vb].clear();
    }
}

void MutationLogHarvester::removeNonExistentKeys(Vbid vbid) {
    Expects(engine);
    auto vb = engine->getKVBucket()->getVBucket(vbid);
    if (!vb) {
        return;
    }

    // Remove any items which are no longer valid in the VBucket.
    for (auto it = committed[vbid].begin(); it != committed[vbid].end();) {
        if ((!vb->ht.findForRead(*it, TrackReference::No).storedValue)) {
            it = committed[vbid].erase(it);
        } else {
            ++it;
        }
    }
}

size_t MutationLogHarvester::total() {
    size_t rv(0);
    for (auto i : itemsSeen) {
        rv += i;
    }
    return rv;
}

std::ostream& operator<<(std::ostream& out, const MutationLogReader& mlog) {
    out << "MutationLogReader{logPath:" << mlog.logPath << ", "
        << "blockSize:" << mlog.blockSize << "}";
    return out;
}
