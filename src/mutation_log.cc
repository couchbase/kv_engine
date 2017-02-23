/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include "config.h"

#include <algorithm>
#include <fcntl.h>
#include <platform/strerror.h>
#include <string>
#include <sys/stat.h>
#include <system_error>
#include <utility>

extern "C" {
#include "crc32.h"
}
#include "ep_engine.h"
#include "mutation_log.h"

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

ssize_t pwrite(file_handle_t fd, const void *buf, size_t nbyte,
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

static inline ssize_t doWrite(file_handle_t fd, const uint8_t *buf,
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

    if (li.LowPart == INVALID_SET_FILE_POINTER && GetLastError() != NO_ERROR) {
        std::stringstream ss;
        ss << "FATAL: SetFilePointer failed " << fname << ": " <<
              cb_strerror();
        LOG(EXTENSION_LOG_WARNING, ss.str().c_str());
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

static inline ssize_t doWrite(file_handle_t fd, const uint8_t *buf,
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
        LOG(EXTENSION_LOG_WARNING, "FATAL: lseek failed '%s': %s",
            fname.c_str(),
            strerror(errno));
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


static bool writeFully(file_handle_t fd, const uint8_t *buf, size_t nbytes) {
    while (nbytes > 0) {
        try {
            ssize_t written = doWrite(fd, buf, nbytes);
            nbytes -= written;
            buf += written;
        } catch (std::system_error& e) {
            LOG(EXTENSION_LOG_WARNING,
                "writeFully: Failed to write to mutation log with error: %s",
                cb_strerror().c_str());

            return false;
        }
    }

    return true;
}

MutationLog::MutationLog(const std::string &path,
                         const size_t bs)
    : paddingHisto(GrowingWidthGenerator<uint32_t>(0, 8, 1.5), 32),
    logPath(path),
    blockSize(bs),
    blockPos(HEADER_RESERVED),
    file(INVALID_FILE_VALUE),
    disabled(false),
    entries(0),
    entryBuffer(new uint8_t[MutationLogEntry::len(256)]()),
    blockBuffer(new uint8_t[bs]()),
    syncConfig(DEFAULT_SYNC_CONF),
    readOnly(false)
{
    for (int ii = 0; ii < int(MutationLogType::NumberOfTypes); ++ii) {
        itemsLogged[ii].store(0);
    }
    logSize.store(0);

    if (logPath == "") {
        disabled = true;
    }
}

MutationLog::~MutationLog() {
    flush();
    close();
}

void MutationLog::disable() {
    if (file >= 0) {
        close();
        disabled = true;
    }
}

void MutationLog::newItem(uint16_t vbucket, const DocKey& key, uint64_t rowid) {
    if (isEnabled()) {
        MutationLogEntry* mle = MutationLogEntry::newEntry(
                entryBuffer.get(), rowid, MutationLogType::New, vbucket, key);
        writeEntry(mle);
    }
}

void MutationLog::sync() {
    if (!isOpen()) {
        throw std::logic_error("MutationLog::sync: Not valid on a closed log");
    }

    BlockTimer timer(&syncTimeHisto);
    try {
        doFsync(file);
    } catch (std::system_error& e) {
        throw WriteException(e.what());
    }
}

void MutationLog::commit1() {
    if (isEnabled()) {
        MutationLogEntry* mle = MutationLogEntry::newEntry(
                entryBuffer.get(), 0, MutationLogType::Commit1, 0);
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
                entryBuffer.get(), 0, MutationLogType::Commit2, 0);
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
        LOG(EXTENSION_LOG_WARNING, "FATAL: lseek failed '%s': %s",
            getLogFile().c_str(), strerror(errno));
        return false;
    }

    uint8_t zero(0);
    if (!writeFully(file, &zero, sizeof(zero))) {
        return false;
    }
    return true;
}

void MutationLog::readInitialBlock() {
    if (!isOpen()) {
        throw std::logic_error("MutationLog::readInitialBlock: Not valid on "
                               "a closed log");
    }
    std::array<uint8_t, MIN_LOG_HEADER_SIZE> buf;
    ssize_t bytesread = pread(file, buf.data(), sizeof(buf), 0);

    if (bytesread != sizeof(buf)) {
        LOG(EXTENSION_LOG_WARNING, "FATAL: initial block read failed"
                "'%s': %s", getLogFile().c_str(), strerror(errno));
        throw ShortReadException();
    }

    headerBlock.set(buf);

    // Check the version is one we can handle, V1 and V2.
    switch (headerBlock.version()) {
    case MutationLogVersion::V1:
    case MutationLogVersion::V2:
        break;
    default: {
        std::stringstream ss;
        ss << "HeaderBlock version is unknown " +
                        std::to_string(int(headerBlock.version()));
        throw ReadException(ss.str());
    }
    }

    if (headerBlock.blockCount() != 1) {
        std::stringstream ss;
        ss << "HeaderBlock blockCount mismatch " +
                        std::to_string(headerBlock.blockCount());
        throw ReadException(ss.str());
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

    uint8_t buf[MIN_LOG_HEADER_SIZE];
    memset(buf, 0, sizeof(buf));
    memcpy(buf, (uint8_t*)&headerBlock, sizeof(headerBlock));

    ssize_t byteswritten = pwrite(file, buf, sizeof(buf), 0);

    // @todo we need a write exception
    if (byteswritten != sizeof(buf)) {
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
            LOG(EXTENSION_LOG_WARNING,
                "WARNING: filesize %" PRId64 " not block aligned '%s': %s",
                seek_result, getLogFile().c_str(), strerror(errno));
            if (blockSize < (size_t)seek_result) {
                if (SeekFile(file, getLogFile(),
                    seek_result - unaligned_bytes, false) < 0) {
                    LOG(EXTENSION_LOG_WARNING, "FATAL: lseek failed '%s': %s",
                            getLogFile().c_str(), strerror(errno));
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
    return access(logPath.c_str(), F_OK) == 0;
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
            LOG(EXTENSION_LOG_WARNING, "WARNING: Corrupted access log '%s'",
                    getLogFile().c_str());
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
        LOG(EXTENSION_LOG_WARNING, "FATAL: Failed to remove '%s': %s",
            getLogFile().c_str(), strerror(errno));
        return false;
    }

    open();
    LOG(EXTENSION_LOG_INFO, "Reset a mutation log '%s' successfully.",
        getLogFile().c_str());
    return true;
}

bool MutationLog::replaceWith(MutationLog &mlog) {
    if (!mlog.isEnabled()) {
        throw std::invalid_argument("MutationLog::replaceWith: "
                                    "mlog is not enabled");
    }
    if (!isEnabled()) {
        throw std::logic_error("MutationLog::replaceWith: Not valid on "
                               "a disabled log");
    }

    if (!mlog.flush()) {
        return false;
    }
    mlog.close();

    if (!flush()) {
        return false;
    }
    close();

    for (int i(0); i < int(MutationLogType::NumberOfTypes); ++i) {
        itemsLogged[i].store(mlog.itemsLogged[i]);
    }

    if (rename(mlog.getLogFile().c_str(), getLogFile().c_str()) != 0) {
        open();
        std::stringstream ss;
        ss <<
        "Unable to rename a mutation log \"" << mlog.getLogFile() << "\" "
           << "to \"" << getLogFile() << "\": " << strerror(errno);
        LOG(EXTENSION_LOG_WARNING, "%s!!! Reopened the old log file",
            ss.str().c_str());
        return false;
    }

    open();
    LOG(EXTENSION_LOG_INFO,
        "Renamed a mutation log \"%s\" to \"%s\" and reopened it",
        mlog.getLogFile().c_str(), getLogFile().c_str());
    return true;
}

bool MutationLog::flush() {
    if (isEnabled() && blockPos > HEADER_RESERVED) {
        if (!isOpen()) {
            throw std::logic_error("MutationLog::flush: "
                                   "Not valid on a closed log");
        }
        needWriteAccess();
        BlockTimer timer(&flushTimeHisto);

        if (blockPos < blockSize) {
            size_t padding(blockSize - blockPos);
            memset(blockBuffer.get() + blockPos, 0x00, padding);
            paddingHisto.add(padding);
        }

        entries = htons(entries);
        memcpy(blockBuffer.get() + 2, &entries, sizeof(entries));

        uint32_t crc32(crc32buf(blockBuffer.get() + 2, blockSize - 2));
        uint16_t crc16(htons(crc32 & 0xffff));
        memcpy(blockBuffer.get(), &crc16, sizeof(crc16));

        if (writeFully(file, blockBuffer.get(), blockSize)) {
            logSize.fetch_add(blockSize);
            blockPos = HEADER_RESERVED;
            entries = 0;
        } else {
            /* write to the mutation log failed. Disable the log */
            disabled = true;
            LOG(EXTENSION_LOG_WARNING,
                "Disabling access log due to write failures");
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
        flush();
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
      entryBuf(LOG_ENTRY_BUF_SIZE),
      buf(log->header().blockSize()),
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

MutationLog::iterator::~iterator() {
}

void MutationLog::iterator::prepItem() {
    size_t copyLen = 0;
    // Determine which version we are reading and use that versions newEntry
    // to obtain the copy length. newEntry also validates magic is valid and
    // the length doesn't overflow bufferBytesRemaining()
    switch (log->headerBlock.version()) {
    case MutationLogVersion::V1: {
        copyLen =
                MutationLogEntryV1::newEntry(p, bufferBytesRemaining())->len();
        break;
    }
    case MutationLogVersion::V2: {
        copyLen =
                MutationLogEntryV2::newEntry(p, bufferBytesRemaining())->len();
        break;
    }
    }

    std::copy_n(p, copyLen, entryBuf.begin());
}

size_t MutationLog::iterator::getCurrentEntryLen() const {
    switch (log->headerBlock.version()) {
    case MutationLogVersion::V1: {
        return MutationLogEntryV1::newEntry(entryBuf.begin(), entryBuf.size())
                ->len();
    }
    case MutationLogVersion::V2: {
        return MutationLogEntryV2::newEntry(entryBuf.begin(), entryBuf.size())
                ->len();
    }
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
 * simplifes each upgrade step, but may have a cost if many steps exist.
 */
MutationLog::MutationLogEntryHolder MutationLog::iterator::upgradeEntry()
        const {
    // The addition of more source versions would mean adding more const
    // pointers here.
    const MutationLogEntryV1* mleV1 = nullptr;
    std::unique_ptr<uint8_t[]> allocated;

    // With only two versions this code is a little unnecessary but will
    // cause the addition of V3 to fail compile. The aim is that the addition of
    // V3 should now be obvious. I.e. we can step V1->V2->V3 or V2->V3
    switch (log->headerBlock.version()) {
    case MutationLogVersion::V1: {
        mleV1 = MutationLogEntryV1::newEntry(entryBuf.begin(), entryBuf.size());
        break;
    }
    /* If V3 exists then add a case for V2, for example:
    case MutationLogVersion::V2: {
        mleV2 = MutationLogEntryV2::newEntry(entryBuf.begin(), entryBuf.size());
        break;
    }
    */
    case MutationLogVersion::Current: {
        throw std::invalid_argument(
                "MutationLog::iterator::upgradeEntry cannot"
                " upgrade if version == current");
    }
    }

    // Next switch runs the upgrade. Each version must be constructable from
    // its parent. This means each step is much simpler and only needs to
    // consider a single parent and there's no complexity of trying to jump
    // versions.
    // We start the upgrade at version + 1
    switch (MutationLogVersion(int(log->headerBlock.version()) + 1)) {
    case MutationLogVersion::V1: {
        // fall through
    }
    case MutationLogVersion::V2: {
        // Upgrade V1 to V2.
        // Alloc a buffer using the length read from V1 as input to V2::len
        allocated = std::make_unique<uint8_t[]>(
                MutationLogEntryV2::len(mleV1->getKeylen()));

        // Now in-place construct into the buffer
        (void) new (allocated.get()) MutationLogEntryV2(*mleV1);
        // If adding more cases, we should assign the above "new" pointer to a
        // mleV2 and allow the next case to read it.

        // fall through
    }
    /* If V3 exists then add a case (which is hit by V2 falling through)
    case MutationLogVersion::V3: {
        // Upgrade V2 to V3
        // Alloc a buffer using the length read from V2 as input to V3::len
        allocated = std::make_unique<uint8_t[]>(
                MutationLogEntryV3::len(mleV2->getKeylen()));

        // Now in-place construct into the new buffer and assign to mleV3
        mleV3 = new (allocated.get()) MutationLogEntryV3(*mleV3);
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
    if (log->headerBlock.version() != MutationLogVersion::Current) {
        return upgradeEntry();
    } else {
        return {entryBuf.data(), false /*not allocated*/};
    }
}

size_t MutationLog::iterator::bufferBytesRemaining() {
    return buf.size() - (p - buf.begin());
}

void MutationLog::iterator::nextBlock() {
    if (log->isEnabled() && !log->isOpen()) {
        throw std::logic_error("MutationLog::iterator::nextBlock: "
                "log is enabled and not open");
    }

    ssize_t bytesread = pread(log->fd(), buf.data(), buf.size(), offset);
    if (bytesread < 1) {
        isEnd = true;
        return;
    }
    if (bytesread != (ssize_t)(log->header().blockSize())) {
        LOG(EXTENSION_LOG_WARNING, "FATAL: too few bytes read in access log"
                "'%s': %s", log->getLogFile().c_str(), strerror(errno));
        throw ShortReadException();
    }
    offset += bytesread;

    // block starts with 2 byte crc and 2 byte item count
    uint32_t crc32(crc32buf(buf.data() + sizeof(uint16_t), buf.size() - sizeof(uint16_t)));
    uint16_t computed_crc16(crc32 & 0xffff);
    uint16_t retrieved_crc16;
    memcpy(&retrieved_crc16, buf.data(), sizeof(retrieved_crc16));
    retrieved_crc16 = ntohs(retrieved_crc16);
    if (computed_crc16 != retrieved_crc16) {
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

            for (const uint16_t vb : vbid_set) {
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
        case MutationLogType::NumberOfTypes: {
            // We ignore COMMIT2 for Access log, was only relevent to the
            // 'proper' mutation log.
            // all other types ignored as well.
            break;
        }
        }
    }
    return it;
}

void MutationLogHarvester::apply(void *arg, mlCallback mlc) {
    for (const uint16_t vb : vbid_set) {
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
    for (const uint16_t vb : vbid_set) {
        RCPtr<VBucket> vbucket = engine->getKVBucket()->getVBucket(vb);
        if (!vbucket) {
            continue;
        }

        // Remove any items which are no longer valid in the VBucket.
        for (auto it = committed[vb].begin(); it != committed[vb].end(); ) {
            if ((vbucket->ht.find(*it, false) == nullptr)) {
                it = committed[vb].erase(it);
            }
            else {
                ++it;
            }
        }

        if (!mlc(vb, committed[vb], arg)) {
            return;
        }
        committed[vb].clear();
    }
}

size_t MutationLogHarvester::total() {
    size_t rv(0);
    for (int i = 0; i < int(MutationLogType::NumberOfTypes); ++i) {
        rv += itemsSeen[i];
    }
    return rv;
}
