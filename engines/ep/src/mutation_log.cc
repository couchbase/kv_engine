/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

#ifdef WIN32
ssize_t pread(file_handle_t fd, void *buf, size_t nbyte, uint64_t offset)
{
    DWORD bytesread;
    OVERLAPPED winoffs;
    memset(&winoffs, 0, sizeof(winoffs));
    winoffs.Offset = offset & 0xFFFFFFFF;
    winoffs.OffsetHigh = (offset >> 32) & 0x7FFFFFFF;
    if (!ReadFile(fd, buf, nbyte, &bytesread, &winoffs)) {
        /* luckily we don't check errno so we don't need to care about that */
        return -1;
    }

    return bytesread;
}

static inline void doClose(file_handle_t fd) {
    if (!CloseHandle(fd)) {
        throw std::system_error(GetLastError(), std::system_category(),
                                "doClose: failed");
    }
}

file_handle_t OpenFile(const std::string& fname) {
    file_handle_t fd;
    fd = CreateFile(const_cast<char*>(fname.c_str()),
                    GENERIC_READ,
                    FILE_SHARE_READ | FILE_SHARE_WRITE,
                    NULL,
                    OPEN_EXISTING,
                    FILE_ATTRIBUTE_NORMAL,
                    NULL);

    if (fd == INVALID_FILE_VALUE) {
        throw std::system_error(GetLastError(),
                                std::system_category(),
                                fmt::format("Failed to open file '{}'", fname));
    }

    return fd;
}

#else

static void doClose(file_handle_t fd) {
    int ret;
    while ((ret = close(fd)) == -1 && (errno == EINTR)) {
        /* Retry */
    }
    if (ret == -1) {
        // Non EINTR error
        throw std::system_error(errno, std::system_category(),
                                "doClose: failed");
    }
}

file_handle_t OpenFile(const std::string& fname) {
    file_handle_t fd;
    fd = ::open(const_cast<char*>(fname.c_str()), O_RDONLY);

    if (fd < 0) {
        throw std::system_error(errno,
                                std::system_category(),
                                fmt::format("Failed to open file '{}'", fname));
    }

    return fd;
}

#endif

MutationLogReader::MutationLogReader(std::string path,
                                     std::function<void()> fileIoTestingHook)
    : fileIoTestingHook(std::move(fileIoTestingHook)),
      logPath(std::move(path)),
      openTimePoint(cb::time::steady_clock::now()),
      file(OpenFile(this->logPath)),
      blockSize(0),
      resumeItr(end()) {
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
    try {
        close();
    } catch (const std::exception& e) {
        EP_LOG_ERR(
                "MutationLogReader::~MutationLogReader: Exception thrown "
                "during "
                "destruction: '{}' - forcefully closing file",
                e.what());
        // We need to close() the underlying FD to ensure we don't leak FDs;
        // on Windows this is particulary problematic as one cannot delete
        // the file if there's still a handle open.
        try {
            doClose(file);
        } catch (const std::exception& ee) {
            // If doClose fails then there's not much more we can do other
            // than report the error and leave the file in whatever state it
            // is in...
            EP_LOG_ERR(
                    "MutationLogReader::~MutationLogReader: Exception thrown "
                    "during "
                    "forceful close of file: '{}' - leaving file as-is.",
                    ee.what());
        }
    }
}

static bool validateHeaderBlockVersion(MutationLogVersion version) {
    return version == MutationLogVersion::V4;
}

void MutationLogReader::readInitialBlock() {
    std::array<uint8_t, LogHeaderBlock::HeaderSize> buf;
    ssize_t bytesread = pread(file, buf.data(), sizeof(buf), 0);

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

void MutationLogReader::close() {
    if (!isOpen()) {
        return;
    }

    doClose(file);
    file = INVALID_FILE_VALUE;
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
    return log->fd() == rhs.log->fd()
        && (
            (isEnd == rhs.isEnd)
            || (offset == rhs.offset
                && items == rhs.items));
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
    ssize_t bytesread = pread(log->fd(), buf.data(), buf.size(), offset);
    if (bytesread < 1) {
        isEnd = true;
        return;
    }
    if (bytesread != (ssize_t)(log->header().blockSize())) {
        EP_LOG_WARN(
                "FATAL: too few bytes read in access log"
                "'{}': {}",
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
        << "blockSize:" << mlog.blockSize << ", "
        << "file:" << mlog.file << "}";
    return out;
}
