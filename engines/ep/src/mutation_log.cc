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

ssize_t mlog::DefaultFileIface::doWrite(file_handle_t fd,
                                        const uint8_t* buf,
                                        size_t nbytes) {
    DWORD byteswritten;
    if (!WriteFile(fd, buf, nbytes, &byteswritten, NULL)) {
        /* luckily we don't check errno so we don't need to care about that */
        throw std::system_error(GetLastError(), std::system_category(),
                                "doWrite: failed");
    }
    return byteswritten;
}

static inline void doClose(file_handle_t fd) {
    if (!CloseHandle(fd)) {
        throw std::system_error(GetLastError(), std::system_category(),
                                "doClose: failed");
    }
}

static inline void doFsync(file_handle_t fd) {
    if (!FlushFileBuffers(fd)) {
        throw std::system_error(GetLastError(), std::system_category(),
                                "doFsync: failed");
    }
}

static int64_t SeekFile(file_handle_t fd, const std::string &fname,
                        uint64_t offset, bool end)
{
    LARGE_INTEGER li;
    li.QuadPart = offset;

    if (end) {
        li.LowPart = SetFilePointer(fd, li.LowPart, &li.HighPart, FILE_END);
    } else {
        li.LowPart = SetFilePointer(fd, li.LowPart, &li.HighPart, FILE_BEGIN);
    }

    if (li.LowPart == INVALID_SET_FILE_POINTER &&
        GetLastError() != ERROR_SUCCESS) {
        EP_LOG_WARN(
                "FATAL: SetFilePointer failed {}: {}", fname, cb_strerror());
        li.QuadPart = -1;
    }

    return li.QuadPart;
}

file_handle_t OpenFile(const std::string &fname, std::string &error,
                       bool rdonly) {
    file_handle_t fd;
    if (rdonly) {
        fd = CreateFile(const_cast<char*>(fname.c_str()),
            GENERIC_READ,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            NULL,
            OPEN_EXISTING,
            FILE_ATTRIBUTE_NORMAL,
            NULL);
    } else {
        fd = CreateFile(const_cast<char*>(fname.c_str()),
            GENERIC_READ | GENERIC_WRITE,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            NULL,
            OPEN_ALWAYS,
            FILE_ATTRIBUTE_NORMAL,
            NULL);
    }

    if (fd == INVALID_FILE_VALUE) {
        error.assign(cb_strerror());
    }

    return fd;
}

int64_t getFileSize(file_handle_t fd) {
    LARGE_INTEGER li;
    if (GetFileSizeEx(fd, &li)) {
        return li.QuadPart;
    }
    throw std::system_error(GetLastError(), std::system_category(),
                            "getFileSize: failed");
}

#else

ssize_t mlog::DefaultFileIface::doWrite(file_handle_t fd,
                                        const uint8_t* buf,
                                        size_t nbytes) {
    ssize_t ret;
    while ((ret = write(fd, buf, nbytes)) == -1 && (errno == EINTR)) {
        /* Retry */
    }
    if (ret == -1) {
        // Non EINTR error
        throw std::system_error(errno, std::system_category(),
                                "doWrite: failed");
    }
    return ret;
}

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

static void doFsync(file_handle_t fd) {
    int ret;
    while ((ret = fsync(fd)) == -1 && (errno == EINTR)) {
        /* Retry */
    }
    if (ret == -1) {
        // Non EINTR error
        throw std::system_error(errno, std::system_category(),
                                "doFsync: failed");
    }
}

static int64_t SeekFile(file_handle_t fd, const std::string &fname,
                        uint64_t offset, bool end)
{
    int64_t ret;
    if (end) {
        ret = lseek(fd, offset, SEEK_END);
    } else {
        ret = lseek(fd, offset, SEEK_SET);
    }

    if (ret < 0) {
        EP_LOG_WARN("FATAL: lseek failed '{}': {}", fname, strerror(errno));
    }
    return ret;
}

file_handle_t OpenFile(const std::string &fname, std::string &error,
                       bool rdonly) {
    file_handle_t fd;
    if (rdonly) {
        fd = ::open(const_cast<char*>(fname.c_str()), O_RDONLY);
    } else {
        fd = ::open(const_cast<char*>(fname.c_str()), O_RDWR | O_CREAT, 0666);
    }

    if (fd < 0) {
        error.assign(strerror(errno));
    }

    return fd;
}

int64_t getFileSize(file_handle_t fd) {
    struct stat st;
    int stat_result = fstat(fd, &st);
    if (stat_result != 0) {
        throw std::system_error(errno, std::system_category(),
                                "getFileSize: failed");
    }
    return st.st_size;
}
#endif

bool MutationLog::writeFully(file_handle_t fd,
                             const uint8_t* buf,
                             size_t nbytes) {
    while (nbytes > 0) {
        try {
            ssize_t written = fileIface->doWrite(fd, buf, nbytes);
            nbytes -= written;
            buf += written;
        } catch (std::system_error&) {
            EP_LOG_WARN(
                    "writeFully: Failed to write to mutation log with error: "
                    "{}",
                    cb_strerror());

            return false;
        }
    }

    return true;
}

MutationLog::MutationLog(std::string path,
                         const size_t bs,
                         std::unique_ptr<mlog::FileIface> fileIface)
    : fileIface(std::move(fileIface)),
      logPath(std::move(path)),
      blockSize(bs),
      blockPos(HEADER_RESERVED),
      file(INVALID_FILE_VALUE),
      disabled(false),
      entries(0),
      entryBuffer(MutationLogEntry::len(256)),
      blockBuffer(bs),
      syncConfig(DEFAULT_SYNC_CONF),
      readOnly(false),
      resumeItr(end()) {
    for (auto& ii : itemsLogged) {
        ii.store(0);
    }
    logSize.store(0);

    if (logPath.empty()) {
        disabled = true;
    }
}

MutationLog::~MutationLog() {
    EP_LOG_INFO("{}", *this);
    auto doLog = entries > 0;
    try {
        if (doLog) {
            EP_LOG_INFO_RAW("MutationLog::~MutationLog flush");
        }
        flush();
        if (doLog) {
            EP_LOG_INFO_RAW("MutationLog::~MutationLog close");
        }
        close();
        if (doLog) {
            EP_LOG_INFO_RAW("MutationLog::~MutationLog done");
        }
    } catch (const std::exception& e) {
        EP_LOG_ERR(
                "MutationLog::~MutationLog: Exception thrown during "
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
                    "MutationLog::~MutationLog: Exception thrown during "
                    "forceful close of file: '{}' - leaving file as-is.",
                    ee.what());
        }
    }
}

void MutationLog::disable() {
    if (isOpen()) {
        close();
    }
    disabled = true;
}

void MutationLog::newItem(Vbid vbucket, const StoredDocKey& key) {
    if (isEnabled()) {
        MutationLogEntry* mle = MutationLogEntry::newEntry(
                entryBuffer.data(), MutationLogType::New, vbucket, key);
        writeEntry(mle);
    }
}

void MutationLog::sync() {
    if (!isOpen()) {
        throw std::logic_error("MutationLog::sync: Not valid on a closed log");
    }

    HdrMicroSecBlockTimer timer(&syncTimeHisto);
    try {
        doFsync(file);
    } catch (std::system_error& e) {
        throw WriteException(e.what());
    }
}

void MutationLog::commit1() {
    if (isEnabled()) {
        MutationLogEntry* mle = MutationLogEntry::newEntry(
                entryBuffer.data(), MutationLogType::Commit1, Vbid(0));
        writeEntry(mle);

        if ((getSyncConfig() & FLUSH_COMMIT_1) != 0) {
            flush();
        }
        if ((getSyncConfig() & SYNC_COMMIT_1) != 0) {
            sync();
        }
    }
}

void MutationLog::commit2() {
    if (isEnabled()) {
        MutationLogEntry* mle = MutationLogEntry::newEntry(
                entryBuffer.data(), MutationLogType::Commit2, Vbid(0));
        writeEntry(mle);

        if ((getSyncConfig() & FLUSH_COMMIT_2) != 0) {
            flush();
        }
        if ((getSyncConfig() & SYNC_COMMIT_2) != 0) {
            sync();
        }
    }
}

bool MutationLog::writeInitialBlock() {
    if (readOnly) {
        throw std::logic_error("MutationLog::writeInitialBlock: Not valid on "
                               "a read-only log");
    }
    if (!isEnabled()) {
        throw std::logic_error("MutationLog::writeInitialBlock: Not valid on "
                               "a disabled log");
    }
    if (!isOpen()) {
        throw std::logic_error("MutationLog::writeInitialBlock: Not valid on "
                               "a closed log");
    }
    headerBlock.set(blockSize);

    std::vector<uint8_t> block(headerBlock.blockSize());
    std::copy_n(reinterpret_cast<uint8_t*>(&headerBlock),
                sizeof(headerBlock),
                block.data());

    if (!writeFully(file, block.data(), block.size())) {
        return false;
    }

    return true;
}

static bool validateHeaderBlockVersion(MutationLogVersion version) {
    return version == MutationLogVersion::V4;
}

void MutationLog::readInitialBlock() {
    if (!isOpen()) {
        throw std::logic_error("MutationLog::readInitialBlock: Not valid on "
                               "a closed log");
    }
    std::array<uint8_t, LogHeaderBlock::HeaderSize> buf;
    ssize_t bytesread = pread(file, buf.data(), sizeof(buf), 0);

    if (bytesread != sizeof(buf)) {
        EP_LOG_WARN(
                "FATAL: initial block read failed"
                "'{}': {}",
                getLogFile(),
                strerror(errno));
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

bool MutationLog::prepareWrites() {
    if (isEnabled()) {
        if (!isOpen()) {
            throw std::logic_error("MutationLog::prepareWrites: Not valid on "
                                   "a closed log");
        }
        int64_t seek_result = SeekFile(file, getLogFile(), 0, true);
        if (seek_result < 0) {
            return false;
        }
        int64_t unaligned_bytes = seek_result % blockSize;
        if (unaligned_bytes != 0) {
            EP_LOG_WARN("WARNING: filesize {} not block aligned '{}': {}",
                        seek_result,
                        getLogFile(),
                        strerror(errno));
            if (blockSize < (size_t)seek_result) {
                if (SeekFile(file, getLogFile(),
                    seek_result - unaligned_bytes, false) < 0) {
                    EP_LOG_WARN("FATAL: lseek failed '{}': {}",
                                getLogFile(),
                                strerror(errno));
                    return false;
                }
            } else {
                throw ShortReadException();
            }
        }
        logSize = static_cast<size_t>(seek_result);
    }
    return true;
}

static uint8_t parseConfigString(const std::string &s) {
    uint8_t rv(0);
    if (s == "off") {
        rv = 0;
    } else if (s == "commit1") {
        rv = 1;
    } else if (s == "commit2") {
        rv = 2;
    } else if (s == "full") {
        rv = 3;
    } else {
        rv = 0xff;
    }
    return rv;
}

bool MutationLog::setSyncConfig(const std::string &s) {
    uint8_t v(parseConfigString(s));
    if (v != 0xff) {
        syncConfig = (syncConfig & ~SYNC_FULL) | v;
    }
    return v != 0xff;
}

bool MutationLog::setFlushConfig(const std::string &s) {
    uint8_t v(parseConfigString(s));
    if (v != 0xff) {
        syncConfig = (syncConfig & ~FLUSH_FULL) | (v << 2);
    }
    return v != 0xff;
}

bool MutationLog::exists() const {
    return cb::io::isFile(logPath);
}

void MutationLog::open(bool _readOnly) {
    if (!isEnabled()) {
        return;
    }
    openTimePoint = cb::time::steady_clock::now();
    readOnly = _readOnly;
    std::string error;
    if (readOnly) {
        if (!exists()) {
            throw FileNotFoundException(logPath);
        }
        file = OpenFile(logPath, error, true);
    } else {
        file = OpenFile(logPath, error, false);
    }

    if (file == INVALID_FILE_VALUE) {
        std::stringstream ss;
        ss << "Unable to open log file: " << error; // strerror(errno);
        throw ReadException(ss.str());
    }

    int64_t size;
    try {
         size = getFileSize(file);
    } catch (std::system_error& e) {
        throw ReadException(e.what());
    }
    if (size && size < static_cast<int64_t>(sizeof(LogHeaderBlock))) {
        try {
            EP_LOG_WARN("WARNING: Corrupted access log '{}'", getLogFile());
            reset();
            return;
        } catch (ShortReadException &) {
            close();
            disabled = true;
            throw ShortReadException();
        }
    }
    if (size == 0) {
        if (!writeInitialBlock()) {
            close();
            disabled = true;
            return;
        }
    } else {
        try {
            readInitialBlock();
        } catch (ShortReadException &) {
            close();
            disabled = true;
            throw ShortReadException();
        }
    }

    if (!prepareWrites()) {
        close();
        disabled = true;
    }
}

void MutationLog::close() {
    if (!isOpen()) {
        return;
    }

    if (!isEnabled()) {
        doClose(file);
        file = INVALID_FILE_VALUE;
        return;
    }

    if (!readOnly) {
        flush();
        sync();
    }

    doClose(file);
    file = INVALID_FILE_VALUE;
}

bool MutationLog::reset() {
    if (!isEnabled()) {
        return false;
    }
    close();

    if (remove(getLogFile().c_str()) == -1) {
        EP_LOG_WARN("FATAL: Failed to remove '{}': {}",
                    getLogFile(),
                    strerror(errno));
        return false;
    }

    open();
    EP_LOG_DEBUG("Reset a mutation log '{}' successfully.", getLogFile());
    return true;
}

bool MutationLog::flush() {
    if (isEnabled() && blockPos > HEADER_RESERVED) {
        if (!isOpen()) {
            throw std::logic_error("MutationLog::flush: "
                                   "Not valid on a closed log");
        }
        needWriteAccess();
        HdrMicroSecBlockTimer timer(&flushTimeHisto);

        if (blockPos < blockSize) {
            size_t padding(blockSize - blockPos);
            memset(blockBuffer.data() + blockPos, 0x00, padding);
        }

        entries = htons(entries);
        memcpy(blockBuffer.data() + 2, &entries, sizeof(entries));

        uint16_t crc16(
                htons(calculateCrc({blockBuffer.data() + 2, blockSize - 2})));
        memcpy(blockBuffer.data(), &crc16, sizeof(crc16));

        if (writeFully(file, blockBuffer.data(), blockSize)) {
            logSize.fetch_add(blockSize);
            blockPos = HEADER_RESERVED;
            entries = 0;
        } else {
            /* write to the mutation log failed. Disable the log */
            disabled = true;
            EP_LOG_WARN_RAW("Disabling access log due to write failures");
            return false;
        }
    }

    return true;
}

void MutationLog::writeEntry(MutationLogEntry *mle) {
    if (mle->len() >= blockSize) {
        throw std::invalid_argument("MutationLog::writeEntry: argument mle "
                "has length (which is " + std::to_string(mle->len()) +
                ") greater than or equal to blockSize (which is" +
                std::to_string(blockSize) + ")");
    }
    if (!isEnabled()) {
        throw std::logic_error("MutationLog::writeEntry: Not valid on "
                "a disabled log");
    }
    if (!isOpen()) {
        throw std::logic_error("MutationLog::writeEntry: Not valid on "
                "a closed log");
    }
    needWriteAccess();

    size_t len(mle->len());
    if (blockPos + len > blockSize) {
        if (!flush()) {
            throw WriteException(
                    "MutationLog::writeEntry - Failed flushing mutation log "
                    "buffer to disk");
        }
    }

    memcpy(blockBuffer.data() + blockPos, mle, len);
    blockPos += len;
    ++entries;

    ++itemsLogged[int(mle->type())];
}

// ----------------------------------------------------------------------
// Mutation log iterator
// ----------------------------------------------------------------------

MutationLog::iterator::iterator(const MutationLog* l, bool e)
    : log(l),
      p(buf.begin()),
      offset(l->header().blockSize() * l->header().blockCount()),
      items(0),
      isEnd(e) {
}

MutationLog::iterator::iterator(const MutationLog::iterator& mit)
    : log(mit.log),
      entryBuf(mit.entryBuf),
      buf(mit.buf),
      p(buf.begin() + (mit.p - mit.buf.begin())),
      offset(mit.offset),
      items(mit.items),
      isEnd(mit.isEnd) {
}

MutationLog::iterator& MutationLog::iterator::operator=(const MutationLog::iterator& other)
{
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

MutationLog::iterator::~iterator() = default;

void MutationLog::iterator::prepItem() {
    const auto copyLen =
            MutationLogEntry::newEntry(p, bufferBytesRemaining())->len();
    Expects(copyLen <= LOG_ENTRY_BUF_SIZE);
    entryBuf.resize(LOG_ENTRY_BUF_SIZE);
    std::copy_n(p, copyLen, entryBuf.begin());
}

size_t MutationLog::iterator::getCurrentEntryLen() const {
    Expects(log->headerBlock.version() == MutationLogVersion::V4);
    return MutationLogEntry::newEntry(entryBuf.begin(), entryBuf.size())->len();
}

MutationLog::iterator& MutationLog::iterator::operator++() {
    if (--items == 0) {
        nextBlock();
    } else {
        p += getCurrentEntryLen();

        prepItem();
    }
    return *this;
}

bool MutationLog::iterator::operator==(const MutationLog::iterator& rhs) const {
    return log->fd() == rhs.log->fd()
        && (
            (isEnd == rhs.isEnd)
            || (offset == rhs.offset
                && items == rhs.items));
}

bool MutationLog::iterator::operator!=(const MutationLog::iterator& rhs) const {
    return ! operator==(rhs);
}

MutationLog::MutationLogEntryHolder MutationLog::iterator::operator*() {
    Expects(log->headerBlock.version() == MutationLogVersion::Current);
    return {entryBuf.data(), false /*not allocated*/};
}

size_t MutationLog::iterator::bufferBytesRemaining() {
    return buf.size() - (p - buf.begin());
}

uint16_t MutationLog::calculateCrc(cb::const_byte_buffer data) const {
    Expects(headerBlock.version() == MutationLogVersion::V4);
    const auto crc32 = crc32c(data.data(), data.size(), 0);
    const uint16_t computed_crc16(crc32 & 0xffff);
    return computed_crc16;
}

void MutationLog::iterator::nextBlock() {
    if (log->isEnabled() && !log->isOpen()) {
        throw std::logic_error("MutationLog::iterator::nextBlock: "
                "log is enabled and not open");
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

void MutationLog::resetCounts(size_t *items) {
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

std::ostream& operator<<(std::ostream& out, const MutationLog& mlog) {
    out << "MutationLog{logPath:" << mlog.logPath << ", "
        << "blockSize:" << mlog.blockSize << ", "
        << "blockPos:" << mlog.blockPos << ", "
        << "file:" << mlog.file << ", "
        << "disabled:" << mlog.disabled << ", "
        << "entries:" << mlog.entries << ", "
        << "entryBuffer:"
        << reinterpret_cast<const void*>(mlog.entryBuffer.data()) << ", "
        << "blockBuffer:"
        << reinterpret_cast<const void*>(mlog.blockBuffer.data()) << ", "
        << "syncConfig:" << int(mlog.syncConfig) << ", "
        << "readOnly:" << mlog.readOnly << "}";
    return out;
}
