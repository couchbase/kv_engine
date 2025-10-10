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

extern "C" {
#include "crc32.h"
}
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

ssize_t mlog::DefaultFileIface::pwrite(file_handle_t fd,
                                       const void *buf,
                                       size_t nbyte,
                                       uint64_t offset)
{
    DWORD byteswritten;
    OVERLAPPED winoffs;
    memset(&winoffs, 0, sizeof(winoffs));
    winoffs.Offset = offset & 0xFFFFFFFF;
    winoffs.OffsetHigh = (offset >> 32) & 0x7FFFFFFF;
    if (!WriteFile(fd, buf, nbyte, &byteswritten, &winoffs)) {
        /* luckily we don't check errno so we don't need to care about that */
        return -1;
    }

    return byteswritten;
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

ssize_t mlog::DefaultFileIface::pwrite(file_handle_t fd,
                                 const void* buf,
                                 size_t nbyte,
                                 uint64_t offset) {
    return ::pwrite(fd, buf, nbyte, offset);
}

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

static inline void doClose(file_handle_t fd) {
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

static inline void doFsync(file_handle_t fd) {
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
      entryBuffer(new uint8_t[MutationLogEntry::len(256)]()),
      blockBuffer(new uint8_t[bs]()),
      syncConfig(DEFAULT_SYNC_CONF),
      readOnly(false) {
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
    } catch (std::exception& e) {
        EP_LOG_ERR(
                "MutationLog::~MutationLog: Exception thrown during "
                "destruction: '{}' - forcefully closing file",
                e.what());
        // We need to close() the underlying FD to ensure we don't leak FDs;
        // on Windows this is particulary problematic as one cannot delete
        // the file if there's still a handle open.
        try {
            doClose(file);
        } catch (std::exception& e) {
            // If doClose fails then there's not much more we can do other
            // than report the error and leave the file in whatever state it
            // is in...
            EP_LOG_ERR(
                    "MutationLog::~MutationLog: Exception thrown during "
                    "forceful close of file: '{}' - leaving file as-is.",
                    e.what());
        }
    }
}

void MutationLog::disable() {
    if (file >= INVALID_FILE_VALUE) {
        close();
        disabled = true;
    }
}

void MutationLog::newItem(Vbid vbucket, const StoredDocKey& key) {
    if (isEnabled()) {
        MutationLogEntry* mle = MutationLogEntry::newEntry(
                entryBuffer.get(), MutationLogType::New, vbucket, key);
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
                entryBuffer.get(), MutationLogType::Commit1, Vbid(0));
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
                entryBuffer.get(), MutationLogType::Commit2, Vbid(0));
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

    if (!writeFully(file, (uint8_t*)&headerBlock, sizeof(headerBlock))) {
        return false;
    }

    int64_t seek_result = SeekFile(file, getLogFile(),
                            std::max(
                            static_cast<uint32_t>(MIN_LOG_HEADER_SIZE),
                            headerBlock.blockSize() * headerBlock.blockCount())
                             - 1, false);
    if (seek_result < 0) {
        EP_LOG_WARN(
                "FATAL: lseek failed '{}': {}", getLogFile(), strerror(errno));
        return false;
    }

    uint8_t zero(0);
    if (!writeFully(file, &zero, sizeof(zero))) {
        return false;
    }
    return true;
}

static bool validateHeaderBlockVersion(MutationLogVersion version) {
    switch (version) {
    case MutationLogVersion::V1:
    case MutationLogVersion::V2:
    case MutationLogVersion::V3:
    case MutationLogVersion::V4:
        return true;
    }
    return false;
}

void MutationLog::readInitialBlock() {
    if (!isOpen()) {
        throw std::logic_error("MutationLog::readInitialBlock: Not valid on "
                               "a closed log");
    }
    std::array<uint8_t, MIN_LOG_HEADER_SIZE> buf;
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

void MutationLog::updateInitialBlock() {
    if (readOnly) {
        throw std::logic_error("MutationLog::updateInitialBlock: Not valid on "
                               "a read-only log");
    }
    if (!isOpen()) {
        throw std::logic_error("MutationLog::updateInitialBlock: Not valid on "
                               "a closed log");
    }
    needWriteAccess();

    std::vector<uint8_t> buf(MIN_LOG_HEADER_SIZE);
    memcpy(buf.data(), &headerBlock, sizeof(headerBlock));

    ssize_t byteswritten = fileIface->pwrite(file, buf.data(), buf.size(), 0);
    if (byteswritten != ssize_t(buf.size())) {
        throw WriteException("Failed to update header block");
    }
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
    if (size && size < static_cast<int64_t>(MIN_LOG_HEADER_SIZE)) {
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

        if (!readOnly) {
            headerBlock.setRdwr(1);
            updateInitialBlock();
        }
    }

    if (!prepareWrites()) {
        close();
        disabled = true;
        return;
    }
}

void MutationLog::close() {
    if (!isEnabled() || !isOpen()) {
        return;
    }

    if (!readOnly) {
        flush();
        sync();
        headerBlock.setRdwr(0);
        updateInitialBlock();
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
            memset(blockBuffer.get() + blockPos, 0x00, padding);
        }

        entries = htons(entries);
        memcpy(blockBuffer.get() + 2, &entries, sizeof(entries));

        uint16_t crc16(
                htons(calculateCrc({blockBuffer.get() + 2, blockSize - 2})));
        memcpy(blockBuffer.get(), &crc16, sizeof(crc16));

        if (writeFully(file, blockBuffer.get(), blockSize)) {
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

    memcpy(blockBuffer.get() + blockPos, mle, len);
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
    size_t copyLen = 0;
    // Determine which version we are reading and use that versions newEntry
    // to obtain the copy length. newEntry also validates magic is valid and
    // the length doesn't overflow bufferBytesRemaining()
    switch (log->headerBlock.version()) {
    case MutationLogVersion::V1:
        copyLen =
                MutationLogEntryV1::newEntry(p, bufferBytesRemaining())->len();
        break;
    case MutationLogVersion::V2:
        copyLen =
                MutationLogEntryV2::newEntry(p, bufferBytesRemaining())->len();
        break;
        // V3 and V4 use the same on-disk format (just use different CRC)
    case MutationLogVersion::V3:
    case MutationLogVersion::V4:
        copyLen =
                MutationLogEntryV3::newEntry(p, bufferBytesRemaining())->len();
        break;
    }

    entryBuf.resize(LOG_ENTRY_BUF_SIZE);
    std::copy_n(p, copyLen, entryBuf.begin());
}

size_t MutationLog::iterator::getCurrentEntryLen() const {
    switch (log->headerBlock.version()) {
    case MutationLogVersion::V1:
        return MutationLogEntryV1::newEntry(entryBuf.begin(), entryBuf.size())
                ->len();
    case MutationLogVersion::V2:
        return MutationLogEntryV2::newEntry(entryBuf.begin(), entryBuf.size())
                ->len();
        // V3 and V4 use the same format (just use different CRC calculation)
    case MutationLogVersion::V3:
    case MutationLogVersion::V4:
        return MutationLogEntryV3::newEntry(entryBuf.begin(), entryBuf.size())
                ->len();
    }
    throw std::logic_error(
            "MutationLog::iterator::getCurrentEntryLen unknown version " +
            std::to_string(int(log->headerBlock.version())));
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

/**
 * Return a MutationLogEntryHolder which is built from a down-level entry
 * The upgrade technique is to upgrade from n to n+1 without any skips, this
 * simplifies each upgrade step, but may have a cost if many steps exist.
 *
 * git blame on the addition of MutationLogEntryV3 to see how V4 can be reached
 *
 */
MutationLog::MutationLogEntryHolder MutationLog::iterator::upgradeEntry()
        const {
    // The addition of more source versions would mean adding more const
    // pointers and unique_ptr for temp storage.
    const MutationLogEntryV1* mleV1 = nullptr;
    const MutationLogEntryV2* mleV2 = nullptr;
    std::unique_ptr<uint8_t[]> allocatedV2;
    std::unique_ptr<uint8_t[]> allocated;

    static_assert(MutationLogVersion::Current == MutationLogVersion::V4,
                  "The difference between V3 and V4 is the CRC used on disk so "
                  "there isn't a upgrade from V3 to V4");

    // With only two versions this code is a little unnecessary but will
    // cause the addition of V4 to fail compile. The aim is that the addition of
    // V4 should now be obvious. I.e. we can step V1->V2->V3->V4 or V2->V3->V4
    switch (log->headerBlock.version()) {
    case MutationLogVersion::V1:
        mleV1 = MutationLogEntryV1::newEntry(entryBuf.begin(), entryBuf.size());
        break;
    case MutationLogVersion::V2:
        mleV2 = MutationLogEntryV2::newEntry(entryBuf.begin(), entryBuf.size());
        break;
    case MutationLogVersion::V3:
        // V3 == V4 with a different CRC (which we've already checked) so we
        // can treat it as upgraded.
        return {entryBuf.data(), false /*not allocated*/};

    /* If V4 exists then add a case for V3, for example:
    case MutationLogVersion::V3: {
        mleV3 = MutationLogEntryV3::newEntry(entryBuf.begin(), entryBuf.size());
        break;
    }
    */
    case MutationLogVersion::Current:
        throw std::invalid_argument(
                "MutationLog::iterator::upgradeEntry cannot"
                " upgrade if version == current");
    }

    // Next switch runs the upgrade. Each version must be constructable from
    // its parent. This means each step is much simpler and only needs to
    // consider a single parent and there's no complexity of trying to jump
    // versions.
    // We start the upgrade at version + 1
    switch (MutationLogVersion(int(log->headerBlock.version()) + 1)) {
    case MutationLogVersion::V1:
        // fall through
    case MutationLogVersion::V2:
        if (!mleV1) {
            throw std::logic_error(
                    "MutationLog::iterator::upgradeEntry mleV1 is null");
        }
        // Upgrade V1 to V2.
        // Alloc a buffer using the length read from V1 as input to V2::len
        allocatedV2 = std::make_unique<uint8_t[]>(
                MutationLogEntryV2::len(mleV1->getKeylen()));

        // Now in-place construct into the buffer
        mleV2 = new (allocatedV2.get()) MutationLogEntryV2(*mleV1);

        // fall through
    case MutationLogVersion::V3:
    case MutationLogVersion::V4:
        if (!mleV2) {
            throw std::logic_error(
                    "MutationLog::iterator::upgradeEntry mleV2 is null");
        }
        // Upgrade V2 to V3
        // Alloc a buffer using the length read from V2 as input to V3::len
        allocated =
                std::make_unique<uint8_t[]>(MutationLogEntryV3::len(*mleV2));

        // Now in-place construct into the new buffer and assign to mleV3
        (void)new (allocated.get()) MutationLogEntryV3(*mleV2);
        // If adding more cases, we should assign the above "new" pointer to a
        // mleV3 and allow the next case to read it.

        // fall through

        /* If V4 exists then add a case (which is hit by V3 falling through)
        case MutationLogVersion::V4: {
            // Upgrade V3 to V4
            // Alloc a buffer using the length read from V3 as input to V4::len
            allocated = std::make_unique<uint8_t[]>(
                    MutationLogEntryV4::len(mleV3->getKeylen()));

            // Now in-place construct into the new buffer and assign to mleV4
            mleV4 = new (allocated.get()) MutationLogEntryV3(*mleV3);
            // fall through
        }
        */
    }

    // transfer ownership to the MutationLogEntryHolder and mark that it's
    // allocated requiring deletion once the holder is destructed
    return {allocated.release(), true /*allocated*/};
}

MutationLog::MutationLogEntryHolder MutationLog::iterator::operator*() {
    // If the file version is down-level return an upgraded entry
    if (log->headerBlock.version() == MutationLogVersion::Current) {
        return {entryBuf.data(), false /*not allocated*/};
    } else {
        return upgradeEntry();
    }
}

size_t MutationLog::iterator::bufferBytesRemaining() {
    return buf.size() - (p - buf.begin());
}

uint16_t MutationLog::calculateCrc(cb::const_byte_buffer data) const {
    uint32_t crc32 = 0;
    // Note: The header block version was checked as part of opening
    //       the file (for reading) and initialized to current version
    //       when writing so we don't need to worry about "illegal"
    //       enum values
    switch (headerBlock.version()) {
    case MutationLogVersion::V1:
    case MutationLogVersion::V2:
    case MutationLogVersion::V3:
        crc32 = crc32buf(const_cast<uint8_t*>(data.data()), data.size());
        break;
    case MutationLogVersion::V4:
        crc32 = crc32c(data.data(), data.size(), 0);
        break;
    }

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
            if (vbid_set.find(le->vbucket()) != vbid_set.end()) {
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

MutationLog::iterator MutationLogHarvester::loadBatch(
        const MutationLog::iterator& start, size_t limit) {
    if (limit == 0) {
        limit = std::numeric_limits<size_t>::max();
    }
    auto it = start;
    size_t count = 0;
    committed.clear();
    for (; it != mlog.end() && count < limit; ++it) {
        const auto& le = *it;
        ++itemsSeen[int(le->type())];

        switch (le->type()) {
        case MutationLogType::New:
            if (vbid_set.find(le->vbucket()) != vbid_set.end()) {
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
    return it;
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

void MutationLogHarvester::apply(void *arg, mlCallbackWithQueue mlc) {
    if (engine == nullptr) {
        throw std::logic_error("MutationLogHarvester::apply: Cannot apply "
                "when engine is NULL");
    }
    for (const auto vb : vbid_set) {
        VBucketPtr vbucket = engine->getKVBucket()->getVBucket(vb);
        if (!vbucket) {
            continue;
        }

        // Remove any items which are no longer valid in the VBucket.
        for (auto it = committed[vb].begin(); it != committed[vb].end(); ) {
            if ((vbucket->ht
                         .findForRead(*it, TrackReference::No, WantsDeleted::No)
                         .storedValue == nullptr)) {
                it = committed[vb].erase(it);
            } else {
                ++it;
            }
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
        << "entryBuffer:" << reinterpret_cast<void*>(mlog.entryBuffer.get())
        << ", "
        << "blockBuffer:" << reinterpret_cast<void*>(mlog.blockBuffer.get())
        << ", "
        << "syncConfig:" << int(mlog.syncConfig) << ", "
        << "readOnly:" << mlog.readOnly << "}";
    return out;
}
