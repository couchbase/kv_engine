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

#include "mutation_log_entry.h"

#include "utility.h"
#include <fmt/ostream.h>
#include <hdrhistogram/hdrhistogram.h>
#include <memcached/storeddockey.h>
#include <memcached/vbucket.h>
#include <array>
#include <atomic>
#include <cstring>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#ifdef WIN32
using file_handle_t = HANDLE;
#define INVALID_FILE_VALUE INVALID_HANDLE_VALUE
#else
using file_handle_t = int;
#define INVALID_FILE_VALUE -1
#endif

const size_t MIN_LOG_HEADER_SIZE(4096);
const size_t HEADER_RESERVED(4);

/**
 * The versions of the layout for the mutation log
 *
 * V4 is identical with V3 except that it use the HW enabled CRC32 calculation
 */
enum class MutationLogVersion { V1 = 1, V2 = 2, V3 = 3, V4 = 4, Current = V4 };

const size_t LOG_ENTRY_BUF_SIZE(512);

const uint8_t SYNC_COMMIT_1(1);
const uint8_t SYNC_COMMIT_2(2);
const uint8_t SYNC_FULL(SYNC_COMMIT_1 | SYNC_COMMIT_2);
const uint8_t FLUSH_COMMIT_1(4);
const uint8_t FLUSH_COMMIT_2(8);
const uint8_t FLUSH_FULL(FLUSH_COMMIT_1 | FLUSH_COMMIT_2);

const uint8_t DEFAULT_SYNC_CONF(FLUSH_COMMIT_2 | SYNC_COMMIT_2);

namespace mlog {
/**
 * Interface for file IO operations. Exists to allow testing of MutationLog
 * behaviour related to file I/O.
 * TODO: Expand to additional file functions as and when we expand test
 *       coverage.
 */
struct FileIface {
    virtual ~FileIface() = default;

    /**
     * Write `nbytes` of data from `buf` to the specified fd at position
     * `offset`.
     */
    virtual ssize_t pwrite(file_handle_t fd,
                           const void* buf,
                           size_t nbyte,
                           uint64_t offset) = 0;
};

/**
 * Default implementation of FileInterface - uses the standard C library
 * IO functions from unistd.h of the same name.
 *
 */
struct DefaultFileIface : public FileIface {
    ssize_t pwrite(file_handle_t fd,
                   const void* buf,
                   size_t nbyte,
                   uint64_t offset) override;
};

} // namespace mlog

/**
 * The header block representing the first 4k (or so) of a MutationLog
 * file.
 */
class LogHeaderBlock {
public:
    explicit LogHeaderBlock(
            MutationLogVersion version = MutationLogVersion::Current)
        : _version(htonl(int(version))) {
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

    void setVersion(MutationLogVersion version) {
        _version = htonl(uint32_t(version));
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
    uint32_t _blockSize = 0;
    uint32_t _blockCount = 0;
    uint32_t _rdwr = 1;
};

/**
 * The MutationLog records major key events to allow ep-engine to more
 * quickly restore the server to its previous state upon restart.
 */
class MutationLog {
public:
    explicit MutationLog(std::string path,
                         const size_t bs = MIN_LOG_HEADER_SIZE,
                         std::unique_ptr<mlog::FileIface> fileIface =
                                 std::make_unique<mlog::DefaultFileIface>());

    ~MutationLog();
    MutationLog(const MutationLog&) = delete;
    const MutationLog& operator=(const MutationLog&) = delete;

    void newItem(Vbid vbucket, const StoredDocKey& key);

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
        explicit WriteException(const std::string& s) : std::runtime_error(s) {
        }
    };

    /**
     * Exception thrown upon failure to read a mutation log.
     */
    class ReadException : public std::runtime_error {
    public:
        explicit ReadException(const std::string& s) : std::runtime_error(s) {
        }
    };

    class FileNotFoundException : public ReadException {
    public:
        explicit FileNotFoundException(const std::string& s)
            : ReadException(s) {
        }
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
    class iterator {
    public:
        using iterator_category = std::input_iterator_tag;
        using value_type = const MutationLogEntry;
        using difference_type = const MutationLogEntry;
        using pointer = const MutationLogEntry*;
        using reference = const MutationLogEntry&;

        iterator(const iterator& mit);

        iterator& operator=(const iterator& other);

        ~iterator();

        iterator& operator++();

        bool operator==(const iterator& rhs) const;

        bool operator!=(const iterator& rhs) const;

        MutationLogEntryHolder operator*();

    private:

        friend class MutationLog;

        explicit iterator(const MutationLog* l, bool e = false);

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
     * The MutationLog has an iterator member that permits a pause/resume
     * iteration pattern. This method on first call (or any call when the resume
     * iterator equals end()) will initialise the resume iterator to begin().
     *
     * @return the resume iterator
     */
    iterator& resume() {
        if (resumeItr == end()) {
            resumeItr = begin();
        }
        return resumeItr;
    }

    /**
     * An iterator pointing at the end of the log file.
     */
    iterator end() {
        return iterator(this, true);
    }

    void incrementKeyLoaded() {
        ++keyLoaded;
    }

    void incrementKeySkipped() {
        ++keySkipped;
    }

    void incrementKeyError() {
        ++keyError;
    }

    size_t getLoaded() const {
        return keyLoaded;
    }

    size_t getSkipped() const {
        return keySkipped;
    }

    size_t getError() const {
        return keyError;
    }

    /**
     * @return how long it has been since this instance opened the log file.
     */
    const std::chrono::nanoseconds getDurationSinceOpen() const {
        return std::chrono::steady_clock::now() - openTimePoint;
    }

    //! Items logged by type.
    std::array<std::atomic<size_t>, size_t(MutationLogType::NumberOfTypes)>
            itemsLogged;
    //! Flush time histogram.
    Hdr1sfMicroSecHistogram flushTimeHisto;
    //! Sync time histogram.
    Hdr1sfMicroSecHistogram syncTimeHisto;
    //! Size of the log
    std::atomic<size_t> logSize;

protected:
    /// Calculate the CRC for the provided data according to the version
    /// number set in the header
    uint16_t calculateCrc(cb::const_byte_buffer data) const;

    void needWriteAccess() {
        if (readOnly) {
            throw WriteException("Invalid access (file opened read only)");
        }
    }
    void writeEntry(MutationLogEntry *mle);

    bool writeInitialBlock();
    void readInitialBlock();
    void updateInitialBlock();

    bool prepareWrites();

    file_handle_t fd() const { return file; }

    LogHeaderBlock     headerBlock;
    /// IO interface for accessing mutation log file.
    std::unique_ptr<mlog::FileIface> fileIface;
    const std::string  logPath;
    size_t             blockSize;
    size_t             blockPos;
    file_handle_t      file;
    bool               disabled;
    uint16_t           entries;
    std::vector<uint8_t> entryBuffer;
    std::vector<uint8_t> blockBuffer;
    uint8_t            syncConfig;
    bool               readOnly;
    iterator resumeItr;
    std::chrono::steady_clock::time_point openTimePoint;
    size_t keyLoaded{0};
    size_t keySkipped{0};
    size_t keyError{0};

    friend std::ostream& operator<<(std::ostream& os, const MutationLog& mlog);

};

std::ostream& operator<<(std::ostream& os, const MutationLog& mlog);

/**
 * MutationLogHarvester::apply callback type.
 */
using mlCallback = bool (*)(void*, Vbid, const DocKey&);
using mlCallbackWithQueue = bool (*)(Vbid,
                                     const std::set<StoredDocKey>&,
                                     void*);

class EventuallyPersistentEngine;

/**
 * Read log entries back from the log to reconstruct the state.
 */
class MutationLogHarvester {
public:
    explicit MutationLogHarvester(MutationLog& ml,
                                  EventuallyPersistentEngine* e = nullptr)
        : mlog(ml), engine(e), itemsSeen() {
    }

    /**
     * Set a vbucket before loading.
     */
    void setVBucket(Vbid vb) {
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
     * @param limit Limit of now many entries should be loaded.
     * @return iterator of where to resume in the log (if the end was not
     *         reached), or MutationLog::iterator::end().
     */
    bool loadBatch(size_t limit);

    /**
     * Apply the processed log entries through the given function.
     */
    void apply(void *arg, mlCallback mlc);

    /**
     * Apply the processed log entries through the given function (queue
     * variant)
     *
     * @param arg passed onto the mlc callback
     * @param mlc callback function
     * @param removeNonExistentKeys if true, remove keys from committed which
     *        are not in the vbucket hash table. Value eviction use-case will
     *        have pre-populated the hash table and makes this a valid operation
     */
    void apply(void* arg, mlCallbackWithQueue mlc, bool removeNonExistentKeys);

    /**
     * Load "limit" keys from the log and call apply(3) for each key.
     *
     * @param limit Limit of now many entries should be loaded.
     * @param arg see apply()
     * @param mlc see apply()
     * @param removeNonExistentKeys see apply()
     */
    bool loadBatchAndApply(size_t limit,
                      void* arg,
                      mlCallbackWithQueue mlc,
                      bool removeNonExistentKeys);

    /**
     * Get the total number of entries found in the log.
     */
    size_t total();

    /**
     * Get all of the counts of log entries by type.
     */
    size_t *getItemsSeen() {
        return itemsSeen.data();
    }

private:
    /**
     * For all keys in committed[vb] remove those which are not found in the
     * vbucket hash-table.
     */
    void removeNonExistentKeys(Vbid vb);

    MutationLog &mlog;
    EventuallyPersistentEngine *engine;
    std::set<Vbid> vbid_set;

    std::unordered_map<Vbid, std::set<StoredDocKey>> committed;
    std::unordered_map<Vbid, std::set<StoredDocKey>> loading;
    std::array<size_t, int(MutationLogType::NumberOfTypes)> itemsSeen;
};

template <>
struct fmt::formatter<MutationLog> : ostream_formatter {};
