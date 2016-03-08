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

#include <sys/stat.h>

#include <algorithm>
#include <string>
#include <system_error>
#include <utility>

extern "C" {
#include "crc32.h"
}
#include "ep_engine.h"
#include "mutation_log.h"

const char *mutation_log_type_names[] = {
    "new", "del", "del_all", "commit1", "commit2", NULL
};

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
    abort();
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

uint64_t MutationLogEntry::rowid() const {
    return ntohll(_rowid);
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
    for (int ii = 0; ii < MUTATION_LOG_TYPES; ++ii) {
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

void MutationLog::newItem(uint16_t vbucket, const std::string &key,
                          uint64_t rowid) {
    if (isEnabled()) {
        MutationLogEntry *mle = MutationLogEntry::newEntry(entryBuffer.get(),
                                                           rowid, ML_NEW,
                                                           vbucket, key);
        writeEntry(mle);
    }
}

void MutationLog::delItem(uint16_t vbucket, const std::string &key) {
    if (isEnabled()) {
        MutationLogEntry *mle = MutationLogEntry::newEntry(entryBuffer.get(),
                                                           0, ML_DEL, vbucket,
                                                           key);
        writeEntry(mle);
    }
}

void MutationLog::deleteAll(uint16_t vbucket) {
    if (isEnabled()) {
        MutationLogEntry *mle = MutationLogEntry::newEntry(entryBuffer.get(),
                                                           0, ML_DEL_ALL,
                                                           vbucket, "");
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
        MutationLogEntry *mle = MutationLogEntry::newEntry(entryBuffer.get(),
                                                           0, ML_COMMIT1, 0,
                                                           "");
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
        MutationLogEntry *mle = MutationLogEntry::newEntry(entryBuffer.get(),
                                                           0, ML_COMMIT2, 0,
                                                           "");
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

    // These are reserved for future use.
    if (headerBlock.version() != LOG_VERSION ||
            headerBlock.blockCount() != 1) {
        std::stringstream ss;
        ss << "HeaderBlock version/blockCount mismatch";
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

    for (int i(0); i < MUTATION_LOG_TYPES; ++i) {
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

    ++itemsLogged[mle->type()];

    delete mle;
}

static const char* logType(uint8_t t) {
    switch(t) {
    case ML_NEW:
        return "new";
        break;
    case ML_DEL:
        return "del";
        break;
    case ML_DEL_ALL:
        return "delall";
        break;
    case ML_COMMIT1:
        return "commit1";
        break;
    case ML_COMMIT2:
        return "commit2";
        break;
    }
    return "UNKNOWN";
}

// ----------------------------------------------------------------------
// Mutation log iterator
// ----------------------------------------------------------------------

MutationLog::iterator::iterator(const MutationLog& l, bool e)
  : log(l),
    entryBuf(),
    buf(),
    p(nullptr),
    offset(l.header().blockSize() * l.header().blockCount()),
    items(0),
    isEnd(e)
{
}

MutationLog::iterator::iterator(const MutationLog::iterator& mit)
  : log(mit.log),
    entryBuf(),
    buf(),
    p(nullptr),
    offset(mit.offset),
    items(mit.items),
    isEnd(mit.isEnd)
{
    if (mit.buf != NULL) {
        buf.reset(new uint8_t[log.header().blockSize()]());
        memcpy(buf.get(), mit.buf.get(), log.header().blockSize());
        p = buf.get() + (mit.p - mit.buf.get());
    }

    if (mit.entryBuf != NULL) {
        entryBuf.reset(new uint8_t[LOG_ENTRY_BUF_SIZE]());
        memcpy(entryBuf.get(), mit.entryBuf.get(), LOG_ENTRY_BUF_SIZE);
    }
}

MutationLog::iterator::~iterator() {
}

void MutationLog::iterator::prepItem() {
    MutationLogEntry *e = MutationLogEntry::newEntry(p,
                                                     bufferBytesRemaining());
    if (entryBuf == NULL) {
        entryBuf.reset(new uint8_t[LOG_ENTRY_BUF_SIZE]());
    }
    memcpy(entryBuf.get(), p, e->len());
}

MutationLog::iterator& MutationLog::iterator::operator++() {
    if (--items == 0) {
        nextBlock();
    } else {
        size_t l(operator*()->len());
        p += l;

        prepItem();
    }
    return *this;
}

bool MutationLog::iterator::operator==(const MutationLog::iterator& rhs) {
    return log.fd() == rhs.log.fd()
        && (
            (isEnd == rhs.isEnd)
            || (offset == rhs.offset
                && items == rhs.items));
}

bool MutationLog::iterator::operator!=(const MutationLog::iterator& rhs) {
    return ! operator==(rhs);
}

const MutationLogEntry* MutationLog::iterator::operator*() {
    if (entryBuf == nullptr) {
        throw std::logic_error("MutationLog::iterator::operator*(): "
                "entryBuf is NULL");
    }
    return MutationLogEntry::newEntry(entryBuf.get(), LOG_ENTRY_BUF_SIZE);
}

size_t MutationLog::iterator::bufferBytesRemaining() {
    return log.header().blockSize() - (p - buf.get());
}

void MutationLog::iterator::nextBlock() {
    if (log.isEnabled() && !log.isOpen()) {
        throw std::logic_error("MutationLog::iterator::nextBlock: "
                "log is enabled and not open");
    }

    if (buf == NULL) {
        buf.reset(new uint8_t[log.header().blockSize()]());
    }
    p = buf.get();

    ssize_t bytesread = pread(log.fd(), buf.get(), log.header().blockSize(),
                              offset);
    if (bytesread < 1) {
        isEnd = true;
        return;
    }
    if (bytesread != (ssize_t)(log.header().blockSize())) {
        LOG(EXTENSION_LOG_WARNING, "FATAL: too few bytes read in access log"
                "'%s': %s", log.getLogFile().c_str(), strerror(errno));
        throw ShortReadException();
    }
    offset += bytesread;

    uint32_t crc32(crc32buf(buf.get() + 2, log.header().blockSize() - 2));
    uint16_t computed_crc16(crc32 & 0xffff);
    uint16_t retrieved_crc16;
    memcpy(&retrieved_crc16, buf.get(), sizeof(retrieved_crc16));
    retrieved_crc16 = ntohs(retrieved_crc16);
    if (computed_crc16 != retrieved_crc16) {
        throw CRCReadException();
    }

    memcpy(&items, buf.get() + 2, 2);
    items = ntohs(items);

    p = p + 4;

    prepItem();
}

void MutationLog::resetCounts(size_t *items) {
    for (int i(0); i < MUTATION_LOG_TYPES; ++i) {
        itemsLogged[i] = items[i];
    }
}

// ----------------------------------------------------------------------
// Reading entries
// ----------------------------------------------------------------------

bool MutationLogHarvester::load() {
    bool clean(false);
    std::set<uint16_t> shouldClear;
    for (MutationLog::iterator it(mlog.begin()); it != mlog.end(); ++it) {
        const MutationLogEntry *le = *it;
        ++itemsSeen[le->type()];
        clean = false;

        switch (le->type()) {
        case ML_DEL:
            // FALLTHROUGH
        case ML_NEW:
            if (vbid_set.find(le->vbucket()) != vbid_set.end()) {
                loading[le->vbucket()][le->key()] =
                                      std::make_pair(le->rowid(), le->type());
            }
            break;
        case ML_COMMIT2: {
            clean = true;
            for (const uint16_t vb :shouldClear) {
                committed[vb].clear();
            }
            shouldClear.clear();

            for (const uint16_t vb : vbid_set) {
                for (auto& copyit2 : loading[vb]) {

                    mutation_log_event_t t = copyit2.second;

                    switch (t.second) {
                    case ML_NEW:
                        committed[vb][copyit2.first] = t.first;
                        break;
                    case ML_DEL:
                        committed[vb].erase(copyit2.first);
                        break;
                    default:
                        abort();
                    }
                }
            }
        }
            loading.clear();
            break;
        case ML_COMMIT1:
            // nothing in particular
            break;
        case ML_DEL_ALL:
            if (vbid_set.find(le->vbucket()) != vbid_set.end()) {
                loading[le->vbucket()].clear();
                shouldClear.insert(le->vbucket());
            }
            break;
        default:
            abort();
        }
    }
    return clean;
}

void MutationLogHarvester::apply(void *arg, mlCallback mlc) {
    for (const uint16_t vb : vbid_set) {
        for (const auto& it2 : committed[vb]) {
            const std::string& key = it2.first;
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
    std::vector<std::pair<std::string, uint64_t> > fetches;
    for (const uint16_t vb : vbid_set) {

        RCPtr<VBucket> vbucket = engine->getEpStore()->getVBucket(vb);
        if (!vbucket) {
            continue;
        }

        for (const auto& it2 : committed[vb]) {
            // cannot use rowid from access log, so must read from hashtable
            const std::string& key = it2.first;
            StoredValue *v = NULL;
            if ((v = vbucket->ht.find(key, false))) {
                fetches.push_back(std::make_pair(it2.first, v->getBySeqno()));
            }
        }
        if (!mlc(vb, fetches, arg)) {
            return;
        }
        fetches.clear();
    }
}

void MutationLogHarvester::getUncommitted(
                             std::vector<mutation_log_uncommitted_t> &uitems) {

    for (const uint16_t vb : vbid_set) {
        mutation_log_uncommitted_t leftover;
        leftover.vbucket = vb;

        for (const auto& copyit2 : loading[vb]) {

            const mutation_log_event_t& t = copyit2.second;
            leftover.key = copyit2.first;
            leftover.rowid = t.first;
            leftover.type = static_cast<mutation_log_type_t>(t.second);

            uitems.push_back(leftover);
        }
    }
}

size_t MutationLogHarvester::total() {
    size_t rv(0);
    for (int i = 0; i < MUTATION_LOG_TYPES; ++i) {
        rv += itemsSeen[i];
    }
    return rv;
}

// ----------------------------------------------------------------------
// Output of entries
// ----------------------------------------------------------------------

std::ostream& operator <<(std::ostream &out, const MutationLogEntry &mle) {
    out << "{MutationLogEntry rowid=" << mle.rowid()
        << ", vbucket=" << mle.vbucket()
        << ", magic=0x" << std::hex << static_cast<uint16_t>(mle.magic)
        << std::dec
        << ", type=" << logType(mle.type())
        << ", key=``" << mle.key() << "''";
    return out;
}
