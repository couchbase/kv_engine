/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2011 Couchbase, Inc
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
        return -1;
    }

    cb_assert(GetLastError() != ERROR_IO_PENDING);
    return byteswritten;
}

static inline int doClose(file_handle_t fd) {
    if (CloseHandle(fd)) {
        return 0;
    } else {
        return -1;
    }
}

static inline int doFsync(file_handle_t fd) {
    if (FlushFileBuffers(fd)) {
        return 0;
    } else {
        return -1;
    }
}

static inline std::string getErrorString(void) {
    std::string ret;
    char* win_msg = NULL;
    DWORD err = GetLastError();
    FormatMessageA(FORMAT_MESSAGE_ALLOCATE_BUFFER |
        FORMAT_MESSAGE_FROM_SYSTEM |
        FORMAT_MESSAGE_IGNORE_INSERTS,
        NULL, err,
        MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
        (LPTSTR)&win_msg,
        0, NULL);
    ret.assign(win_msg);
    LocalFree(win_msg);
    return ret;
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
              getErrorString();
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
        error.assign(getErrorString());
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
    return ret;
}

static inline int doClose(file_handle_t fd) {
    int ret;
    while ((ret = close(fd)) == -1 && (errno == EINTR)) {
        /* Retry */
    }
    return ret;
}

static inline int doFsync(file_handle_t fd) {
    int ret;
    while ((ret = fsync(fd)) == -1 && (errno == EINTR)) {
        /* Retry */
    }
    return ret;
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
    cb_assert(stat_result == 0);
    return st.st_size;
}
#endif


static void writeFully(file_handle_t fd, const uint8_t *buf, size_t nbytes) {
    while (nbytes > 0) {
        ssize_t written = doWrite(fd, buf, nbytes);
        cb_assert(written >= 0);

        nbytes -= written;
        buf += written;
    }
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
    entryBuffer(static_cast<uint8_t*>(calloc(MutationLogEntry::len(256), 1))),
    blockBuffer(static_cast<uint8_t*>(calloc(bs, 1))),
    syncConfig(DEFAULT_SYNC_CONF),
    readOnly(false)
{
    for (int ii = 0; ii < MUTATION_LOG_TYPES; ++ii) {
        itemsLogged[ii].store(0);
    }
    logSize.store(0);

    cb_assert(entryBuffer);
    cb_assert(blockBuffer);
    if (logPath == "") {
        disabled = true;
    }
}

MutationLog::~MutationLog() {
    flush();
    close();
    free(entryBuffer);
    free(blockBuffer);
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
        MutationLogEntry *mle = MutationLogEntry::newEntry(entryBuffer,
                                                           rowid, ML_NEW,
                                                           vbucket, key);
        writeEntry(mle);
    }
}

void MutationLog::delItem(uint16_t vbucket, const std::string &key) {
    if (isEnabled()) {
        MutationLogEntry *mle = MutationLogEntry::newEntry(entryBuffer,
                                                           0, ML_DEL, vbucket,
                                                           key);
        writeEntry(mle);
    }
}

void MutationLog::deleteAll(uint16_t vbucket) {
    if (isEnabled()) {
        MutationLogEntry *mle = MutationLogEntry::newEntry(entryBuffer,
                                                           0, ML_DEL_ALL,
                                                           vbucket, "");
        writeEntry(mle);
    }
}

void MutationLog::sync() {
    cb_assert(isOpen());
    BlockTimer timer(&syncTimeHisto);
    int fsyncResult = doFsync(file);
    cb_assert(fsyncResult != -1);
}

void MutationLog::commit1() {
    if (isEnabled()) {
        MutationLogEntry *mle = MutationLogEntry::newEntry(entryBuffer,
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
        MutationLogEntry *mle = MutationLogEntry::newEntry(entryBuffer,
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
    cb_assert(!readOnly);
    cb_assert(isEnabled());
    cb_assert(isOpen());
    headerBlock.set(blockSize);

    writeFully(file, (uint8_t*)&headerBlock, sizeof(headerBlock));

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
    writeFully(file, &zero, sizeof(zero));
    return true;
}

void MutationLog::readInitialBlock() {
    cb_assert(isOpen());
    uint8_t buf[MIN_LOG_HEADER_SIZE];
    ssize_t bytesread = pread(file, buf, sizeof(buf), 0);

    if (bytesread != sizeof(buf)) {
        throw ShortReadException();
    }

    headerBlock.set(buf, sizeof(buf));

    // These are reserved for future use.
    if (headerBlock.version() == LOG_VERSION ||
            headerBlock.blockCount() == 1) {
        std::stringstream ss;
        ss << "HeaderBlock version/blockCount mismatch";
        throw ReadException(ss.str());
    }

    blockSize = headerBlock.blockSize();
}

void MutationLog::updateInitialBlock() {
    cb_assert(!readOnly);
    cb_assert(isOpen());
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
        cb_assert(isOpen());
        int64_t seek_result = SeekFile(file, getLogFile(), 0, true);
        if (seek_result < 0) {
            LOG(EXTENSION_LOG_WARNING, "FATAL: lseek failed '%s': %s",
                    getLogFile().c_str(), strerror(errno));
            return false;
        }
        if (seek_result % blockSize != 0) {
            throw ShortReadException();
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

    int64_t size = getFileSize(file);
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

    cb_assert(isOpen());
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

    int close_result = doClose(file);
    cb_assert(close_result != -1);
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
    cb_assert(mlog.isEnabled());
    cb_assert(isEnabled());

    mlog.flush();
    mlog.close();
    flush();
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

void MutationLog::flush() {
    if (isEnabled() && blockPos > HEADER_RESERVED) {
        cb_assert(isOpen());
        needWriteAccess();
        BlockTimer timer(&flushTimeHisto);

        if (blockPos < blockSize) {
            size_t padding(blockSize - blockPos);
            memset(blockBuffer + blockPos, 0x00, padding);
            paddingHisto.add(padding);
        }

        entries = htons(entries);
        memcpy(blockBuffer + 2, &entries, sizeof(entries));

        uint32_t crc32(crc32buf(blockBuffer + 2, blockSize - 2));
        uint16_t crc16(htons(crc32 & 0xffff));
        memcpy(blockBuffer, &crc16, sizeof(crc16));

        writeFully(file, blockBuffer, blockSize);
        logSize.fetch_add(blockSize);

        blockPos = HEADER_RESERVED;
        entries = 0;
    }
}

void MutationLog::writeEntry(MutationLogEntry *mle) {
    cb_assert(isEnabled());
    cb_assert(isOpen());
    needWriteAccess();

    size_t len(mle->len());
    if (blockPos + len > blockSize) {
        flush();
    }
    cb_assert(len < blockSize);

    memcpy(blockBuffer + blockPos, mle, len);
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

MutationLog::iterator::iterator(const MutationLog *l, bool e)
  : log(l),
    entryBuf(NULL),
    buf(NULL),
    p(buf),
    offset(l->header().blockSize() * l->header().blockCount()),
    items(0),
    isEnd(e)
{
    cb_assert(log);
}

MutationLog::iterator::iterator(const MutationLog::iterator& mit)
  : log(mit.log),
    entryBuf(NULL),
    buf(NULL),
    p(NULL),
    offset(mit.offset),
    items(mit.items),
    isEnd(mit.isEnd)
{
    cb_assert(log);
    if (mit.buf != NULL) {
        buf = static_cast<uint8_t*>(calloc(1, log->header().blockSize()));
        cb_assert(buf);
        memcpy(buf, mit.buf, log->header().blockSize());
        p = buf + (mit.p - mit.buf);
    }

    if (mit.entryBuf != NULL) {
        entryBuf = static_cast<uint8_t*>(calloc(1, LOG_ENTRY_BUF_SIZE));
        cb_assert(entryBuf);
        memcpy(entryBuf, mit.entryBuf, LOG_ENTRY_BUF_SIZE);
    }
}

MutationLog::iterator::~iterator() {
    free(entryBuf);
    free(buf);
}

void MutationLog::iterator::prepItem() {
    MutationLogEntry *e = MutationLogEntry::newEntry(p,
                                                     bufferBytesRemaining());
    if (entryBuf == NULL) {
        entryBuf = static_cast<uint8_t*>(calloc(1, LOG_ENTRY_BUF_SIZE));
        cb_assert(entryBuf);
    }
    memcpy(entryBuf, p, e->len());
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

MutationLog::iterator& MutationLog::iterator::operator++(int) {
    abort();
    return *this;
}

bool MutationLog::iterator::operator==(const MutationLog::iterator& rhs) {
    return log->fd() == rhs.log->fd()
        && (
            (isEnd == rhs.isEnd)
            || (offset == rhs.offset
                && items == rhs.items));
}

bool MutationLog::iterator::operator!=(const MutationLog::iterator& rhs) {
    return ! operator==(rhs);
}

const MutationLogEntry* MutationLog::iterator::operator*() {
    cb_assert(entryBuf != NULL);
    return MutationLogEntry::newEntry(entryBuf, LOG_ENTRY_BUF_SIZE);
}

size_t MutationLog::iterator::bufferBytesRemaining() {
    return log->header().blockSize() - (p - buf);
}

void MutationLog::iterator::nextBlock() {
    cb_assert(!log->isEnabled() || log->isOpen());
    if (buf == NULL) {
        buf = static_cast<uint8_t*>(calloc(1, log->header().blockSize()));
        cb_assert(buf);
    }
    p = buf;

    ssize_t bytesread = pread(log->fd(), buf, log->header().blockSize(),
                              offset);
    if (bytesread < 1) {
        isEnd = true;
        return;
    }
    if (bytesread != (ssize_t)(log->header().blockSize())) {
        throw ShortReadException();
    }
    offset += bytesread;

    uint32_t crc32(crc32buf(buf + 2, log->header().blockSize() - 2));
    uint16_t computed_crc16(crc32 & 0xffff);
    uint16_t retrieved_crc16;
    memcpy(&retrieved_crc16, buf, sizeof(retrieved_crc16));
    retrieved_crc16 = ntohs(retrieved_crc16);
    if (computed_crc16 != retrieved_crc16) {
        throw CRCReadException();
    }

    memcpy(&items, buf + 2, 2);
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
            for (std::set<uint16_t>::iterator vit(shouldClear.begin());
                 vit != shouldClear.end(); ++vit) {
                committed[*vit].clear();
            }
            shouldClear.clear();

            for (std::set<uint16_t>::const_iterator vit = vbid_set.begin();
                 vit != vbid_set.end(); ++vit) {
                uint16_t vb(*vit);

                unordered_map<std::string, mutation_log_event_t>::iterator
                              copyit2;
                for (copyit2 = loading[vb].begin();
                     copyit2 != loading[vb].end();
                     ++copyit2) {

                    mutation_log_event_t t = copyit2->second;

                    switch (t.second) {
                    case ML_NEW:
                        committed[vb][copyit2->first] = t.first;
                        break;
                    case ML_DEL:
                        committed[vb].erase(copyit2->first);
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
    for (std::set<uint16_t>::const_iterator it = vbid_set.begin();
         it != vbid_set.end(); ++it) {
        uint16_t vb(*it);

        for (unordered_map<std::string, uint64_t>::iterator it2 =
                                                       committed[vb].begin();
             it2 != committed[vb].end(); ++it2) {
            const std::string key(it2->first);
            uint64_t rowid(it2->second);
            if (!mlc(arg, vb, key, rowid)) { // Stop loading from an access log
                return;
            }
        }
    }
}

void MutationLogHarvester::apply(void *arg, mlCallbackWithQueue mlc) {
    cb_assert(engine);
    std::vector<std::pair<std::string, uint64_t> > fetches;
    std::set<uint16_t>::const_iterator it = vbid_set.begin();
    for (; it != vbid_set.end(); ++it) {
        uint16_t vb(*it);
        RCPtr<VBucket> vbucket = engine->getEpStore()->getVBucket(vb);
        if (!vbucket) {
            continue;
        }
        unordered_map<std::string, uint64_t>::iterator it2 =
                                                         committed[vb].begin();
        for (; it2 != committed[vb].end(); ++it2) {
            // cannot use rowid from access log, so must read from hashtable
            std::string key = it2->first;
            StoredValue *v = NULL;
            if ((v = vbucket->ht.find(key, false))) {
                fetches.push_back(std::make_pair(it2->first, v->getBySeqno()));
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

    for (std::set<uint16_t>::const_iterator vit = vbid_set.begin();
         vit != vbid_set.end(); ++vit) {
        uint16_t vb(*vit);
        mutation_log_uncommitted_t leftover;
        leftover.vbucket = vb;

        unordered_map<std::string, mutation_log_event_t>::iterator copyit2;
        for (copyit2 = loading[vb].begin();
             copyit2 != loading[vb].end();
             ++copyit2) {

            mutation_log_event_t t = copyit2->second;
            leftover.key = copyit2->first;
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
