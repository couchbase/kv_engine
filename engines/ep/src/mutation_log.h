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

#pragma once

/**
 * 'Mutation' Log
 *
 * The MutationLog is used to maintain a log of mutations which have occurred
 * in one or more vbuckets. It only records the additions or removals of keys,
 * and then only the key of the item (no value).
 *
 * The original intent of this class was to record a log in parallel with the
 * normal couchstore snapshots, see docs/klog.org, however this has not been
 * used since MB-7590 (March 2013).
 *
 * The current use of MutationLog is for the access.log. This is a slightly
 * different use-case - periodically (default daily) the AccessScanner walks
 * each vBucket's HashTable and records the set of keys currently resident.
 * This doesn't make use of the MutationLog's commit functionality - its simply
 * a list of keys which were resident. When we later come to read the Access log
 * during warmup there's no guarantee that the keys listed still exist - the
 * contents of the Access log is essentially just a hint / suggestion.
 *
 */

#include "config.h"

#include "mutation_log_entry.h"

#include <array>
#include <cstring>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include <atomic>
#include <platform/histogram.h>
#include "utility.h"

#define ML_BUFLEN (128 * 1024 * 1024)

#ifdef WIN32
typedef HANDLE file_handle_t;
#define INVALID_FILE_VALUE INVALID_HANDLE_VALUE
#else
typedef int file_handle_t;
#define INVALID_FILE_VALUE -1
#endif


const size_t MAX_LOG_SIZE((size_t)(unsigned int)-1);
const size_t MAX_ENTRY_RATIO(10);
const size_t LOG_COMPACTOR_QUEUE_CAP(500000);
const int MUTATION_LOG_COMPACTOR_FREQ(3600);

const size_t MIN_LOG_HEADER_SIZE(4096);
const size_t HEADER_RESERVED(4);

enum class MutationLogVersion { V1 = 1, V2 = 2, V3 = 3, Current = V3 };

const size_t LOG_ENTRY_BUF_SIZE(512);

const uint8_t SYNC_COMMIT_1(1);
const uint8_t SYNC_COMMIT_2(2);
const uint8_t SYNC_FULL(SYNC_COMMIT_1 | SYNC_COMMIT_2);
const uint8_t FLUSH_COMMIT_1(4);
const uint8_t FLUSH_COMMIT_2(8);
const uint8_t FLUSH_FULL(FLUSH_COMMIT_1 | FLUSH_COMMIT_2);

const uint8_t DEFAULT_SYNC_CONF(FLUSH_COMMIT_2 | SYNC_COMMIT_2);

/**
 * The header block representing the first 4k (or so) of a MutationLog
 * file.
 */
class LogHeaderBlock {
public:
    LogHeaderBlock(MutationLogVersion version = MutationLogVersion::Current)
        : _version(htonl(int(version))),
          _blockSize(0),
          _blockCount(0),
          _rdwr(1) {
    }

    void set(uint32_t bs, uint32_t bc=1) {
        _blockSize = htonl(bs);
        _blockCount = htonl(bc);
    }

    void set(const std::array<uint8_t, MIN_LOG_HEADER_SIZE>& buf) {
        int offset(0);
        memcpy(&_version, buf.data() + offset, sizeof(_version));
        offset += sizeof(_version);
        memcpy(&_blockSize, buf.data() + offset, sizeof(_blockSize));
        offset += sizeof(_blockSize);
        memcpy(&_blockCount, buf.data() + offset, sizeof(_blockCount));
        offset += sizeof(_blockCount);
        memcpy(&_rdwr, buf.data() + offset, sizeof(_rdwr));
    }

    MutationLogVersion version() const {
        return MutationLogVersion(ntohl(_version));
    }

    uint32_t blockSize() const {
        return ntohl(_blockSize);
    }

    uint32_t blockCount() const {
        return ntohl(_blockCount);
    }

    uint32_t rdwr() const {
        return ntohl(_rdwr);
    }

    void setRdwr(uint32_t nval) {
        _rdwr = htonl(nval);
    }

private:

    uint32_t _version;
    uint32_t _blockSize;
    uint32_t _blockCount;
    uint32_t _rdwr;
};

/**
 * Mutation log compactor config that is used to control the scheduling of
 * the log compactor
 */
class MutationLogCompactorConfig {
public:
    MutationLogCompactorConfig() :
        maxLogSize(MAX_LOG_SIZE), maxEntryRatio(MAX_ENTRY_RATIO),
        queueCap(LOG_COMPACTOR_QUEUE_CAP),
        sleepTime(MUTATION_LOG_COMPACTOR_FREQ) { }

    MutationLogCompactorConfig(size_t max_log_size,
                               size_t max_entry_ratio,
                               size_t queue_cap,
                               size_t stime) :
        maxLogSize(max_log_size), maxEntryRatio(max_entry_ratio),
        queueCap(queue_cap), sleepTime(stime) { }

    void setMaxLogSize(size_t max_log_size) {
        maxLogSize = max_log_size;
    }

    size_t getMaxLogSize() const {
        return maxLogSize;
    }

    void setMaxEntryRatio(size_t max_entry_ratio) {
        maxEntryRatio = max_entry_ratio;
    }

    size_t getMaxEntryRatio() const {
        return maxEntryRatio;
    }

    void setQueueCap(size_t queue_cap) {
        queueCap = queue_cap;
    }

    size_t getQueueCap() const {
        return queueCap;
    }

    void setSleepTime(size_t stime) {
        sleepTime = stime;
    }

    size_t getSleepTime() const {
        return sleepTime;
    }

private:
    size_t maxLogSize;
    size_t maxEntryRatio;
    size_t queueCap;
    size_t sleepTime;
};

/**
 * The MutationLog records major key events to allow ep-engine to more
 * quickly restore the server to its previous state upon restart.
 */
class MutationLog {
public:
    MutationLog(const std::string& path, const size_t bs = MIN_LOG_HEADER_SIZE);

    ~MutationLog();

    void newItem(uint16_t vbucket, const StoredDocKey& key);

    void commit1();

    void commit2();

    bool flush();

    void sync();

    void disable();

    bool isEnabled() const {
        return !disabled;
    }

    bool isOpen() const {
        return file != INVALID_FILE_VALUE;
    }

    LogHeaderBlock header() const {
        return headerBlock;
    }

    void setSyncConfig(uint8_t sconf) {
        syncConfig = sconf;
    }

    uint8_t getSyncConfig() const {
        return syncConfig & SYNC_FULL;
    }

    uint8_t getFlushConfig() const {
        return syncConfig & FLUSH_FULL;
    }

    size_t getBlockSize() const {
        return blockSize;
    }

    bool exists() const;

    const std::string &getLogFile() const { return logPath; }

    /**
     * Open and initialize the log.
     *
     * This typically happens automatically.
     */
    void open(bool _readOnly = false);

    /**
     * Close the log file.
     */
    void close();

    /**
     * Reset the log.
     */
    bool reset();

    /**
     * Replace the current log with a given log.
     */
    bool replaceWith(MutationLog &mlog);

    bool setSyncConfig(const std::string &s);
    bool setFlushConfig(const std::string &s);

    /**
     * Reset the item type counts to the given values.
     *
     * This is used by the loader as part of initialization.
     */
    void resetCounts(size_t *);

    /**
     * Exception thrown upon failure to write a mutation log.
     */
    class WriteException : public std::runtime_error {
    public:
        WriteException(const std::string &s) : std::runtime_error(s) {}
    };

    /**
     * Exception thrown upon failure to read a mutation log.
     */
    class ReadException : public std::runtime_error {
    public:
        ReadException(const std::string &s) : std::runtime_error(s) {}
    };

    class FileNotFoundException : public ReadException {
    public:
        FileNotFoundException(const std::string &s) : ReadException(s) {}
    };

    /**
     * Exception thrown when a CRC mismatch is read in the log.
     */
    class CRCReadException : public ReadException {
    public:
        CRCReadException() : ReadException("CRC Mismatch") {}
    };

    /**
     * Exception thrown when a short read occurred.
     */
    class ShortReadException : public ReadException {
    public:
        ShortReadException() : ReadException("Short Read") {}
    };

    /**
     * The MutationLog::iterator will return MutationLogEntryHolder objects
     * which handle resource destruction if necessary. In some cases the entry
     * being read is a temporary heap allocation which will need deleting.
     * Sometimes the entry is owned by the iterator and the iterator will
     * sort the deletion.
     */
    class MutationLogEntryHolder {
    public:
        /**
         * @param _mle A pointer to a buffer which contains a MutationLogEntry
         * @param _destroy Set to true if the _mle buffer must be deleted once
         *        the holder's life is complete.
         */
        MutationLogEntryHolder(const uint8_t* _mle, bool _destroy)
            : mle(_mle), destroy(_destroy) {
        }

        MutationLogEntryHolder(MutationLogEntryHolder&& rhs)
            : mle(rhs.mle), destroy(rhs.destroy) {
            rhs.mle = nullptr;
        }

        MutationLogEntryHolder(const MutationLogEntryHolder& rhs) = delete;

        /**
         * Destructor will delete the mle data only if we're told to by the
         * constructing code
         */
        ~MutationLogEntryHolder() {
            if (destroy) {
                delete[] mle;
            }
        }

        const MutationLogEntry* operator->() const {
            return reinterpret_cast<const MutationLogEntry*>(mle);
        }

    private:
        const uint8_t* mle;
        bool destroy;
    };

    /**
     * An iterator for the mutation log.
     *
     * A ReadException may be thrown at any point along iteration.
     */
    class iterator  : public std::iterator<std::input_iterator_tag,
                                           const MutationLogEntry> {
    public:

        iterator(const iterator& mit);

        iterator& operator=(const iterator& other);

        ~iterator();

        iterator& operator++();

        bool operator==(const iterator& rhs) const;

        bool operator!=(const iterator& rhs) const;

        MutationLogEntryHolder operator*();

    private:

        friend class MutationLog;

        iterator(const MutationLog* l, bool e=false);

        /// @returns the length of the entry the iterator is currently at
        size_t getCurrentEntryLen() const;
        void nextBlock();
        size_t bufferBytesRemaining();
        void prepItem();

        /**
         * Upgrades the entry the iterator is currently at and returns it
         * via a MutationLogEntryHolder
         */
        MutationLogEntryHolder upgradeEntry() const;

        const MutationLog* log;
        std::vector<uint8_t> entryBuf;
        std::vector<uint8_t> buf;
        std::vector<uint8_t>::const_iterator p;
        off_t              offset;
        uint16_t           items;
        bool               isEnd;
    };

    /**
     * An iterator pointing to the beginning of the log file.
     */
    iterator begin() {
        iterator it(iterator(this));
        it.nextBlock();
        return it;
    }

    /**
     * An iterator pointing at the end of the log file.
     */
    iterator end() {
        return iterator(this, true);
    }

    //! Items logged by type.
    std::atomic<size_t> itemsLogged[int(MutationLogType::NumberOfTypes)];
    //! Histogram of block padding sizes.
    Histogram<uint32_t> paddingHisto;
    //! Flush time histogram.
    MicrosecondHistogram flushTimeHisto;
    //! Sync time histogram.
    MicrosecondHistogram syncTimeHisto;
    //! Size of the log
    std::atomic<size_t> logSize;

private:
    void needWriteAccess(void) {
        if (readOnly) {
            throw WriteException("Invalid access (file opened read only)");
        }
    }
    void writeEntry(MutationLogEntry *mle);

    bool writeInitialBlock();
    void readInitialBlock();
    void updateInitialBlock(void);

    bool prepareWrites();

    file_handle_t fd() const { return file; }

    LogHeaderBlock     headerBlock;
    const std::string  logPath;
    size_t             blockSize;
    size_t             blockPos;
    file_handle_t      file;
    bool               disabled;
    uint16_t           entries;
    std::unique_ptr<uint8_t[]> entryBuffer;
    std::unique_ptr<uint8_t[]> blockBuffer;
    uint8_t            syncConfig;
    bool               readOnly;

    friend std::ostream& operator<<(std::ostream& os, const MutationLog& mlog);

    DISALLOW_COPY_AND_ASSIGN(MutationLog);
};

std::ostream& operator<<(std::ostream& os, const MutationLog& mlog);

/// @cond DETAILS

//! rowid, (uint8_t)mutation_log_type_t
typedef std::pair<uint64_t, uint8_t> mutation_log_event_t;

/// @endcond

/**
 * MutationLogHarvester::apply callback type.
 */
typedef bool (*mlCallback)(void*, uint16_t, const DocKey&);
typedef bool (*mlCallbackWithQueue)(uint16_t,
                                    const std::set<StoredDocKey>&,
                                    void *arg);

/**
 * Type for mutation log leftovers.
 */
struct mutation_log_uncommitted_t {
    StoredDocKey        key;
    uint64_t            rowid;
    MutationLogType     type;
    uint16_t            vbucket;
};

class EventuallyPersistentEngine;

/**
 * Read log entries back from the log to reconstruct the state.
 */
class MutationLogHarvester {
public:
    MutationLogHarvester(MutationLog &ml, EventuallyPersistentEngine *e = NULL) :
        mlog(ml), engine(e)
    {
        memset(itemsSeen, 0, sizeof(itemsSeen));
    }

    /**
     * Set a vbucket before loading.
     */
    void setVBucket(uint16_t vb) {
        vbid_set.insert(vb);
    }

    /**
     * Load the entries from the file.
     *
     * @return true if the file was clean and can likely be trusted.
     */
    bool load();

    /**
     * Load a batch of entries from the file, starting from the given iterator.
     * Loaded entries are inserted into `committed`, which is cleared at the
     * start of each call.
     *
     * @param start Iterator of where to start loading from.
     * @param limit Limit of now many entries should be loaded. Zero means no
     *              limit.
     * @return iterator of where to resume in the log (if the end was not
     *         reached), or MutationLog::iterator::end().
     */
    MutationLog::iterator loadBatch(const MutationLog::iterator& start,
                                        size_t limit);

    /**
     * Apply the processed log entries through the given function.
     */
    void apply(void *arg, mlCallback mlc);
    void apply(void *arg, mlCallbackWithQueue mlc);

    /**
     * Get the total number of entries found in the log.
     */
    size_t total();

    /**
     * Get all of the counts of log entries by type.
     */
    size_t *getItemsSeen() {
        return itemsSeen;
    }

private:

    MutationLog &mlog;
    EventuallyPersistentEngine *engine;
    std::set<uint16_t> vbid_set;

    std::unordered_map<uint16_t, std::set<StoredDocKey>> committed;
    std::unordered_map<uint16_t, std::set<StoredDocKey>> loading;
    size_t itemsSeen[int(MutationLogType::NumberOfTypes)];
};
