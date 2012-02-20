#ifndef MUTATION_LOG_HH
#define MUTATION_LOG_HH 1

#include <vector>
#include <set>
#include <iterator>
#include <limits>
#include <algorithm>
#include <functional>
#include <numeric>
#include <iterator>
#include <exception>

#include <cstdio>

#include <fcntl.h>
#include <unistd.h>

#include "common.hh"
#include "atomic.hh"
#include "histo.hh"

#define ML_BUFLEN (128 * 1024 * 1024)

const size_t MIN_LOG_HEADER_SIZE(4096);
const uint8_t MUTATION_LOG_MAGIC(0x45);
const size_t HEADER_RESERVED(4);
const uint32_t LOG_VERSION(1);
const size_t LOG_ENTRY_BUF_SIZE(512);
const int DISABLED_FD(-3);

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
    LogHeaderBlock() : _version(htonl(LOG_VERSION)), _blockSize(0), _blockCount(0) {
    }

    void set(uint32_t bs, uint32_t bc=1) {
        _blockSize = htonl(bs);
        _blockCount = htonl(bc);
    }

    void set(const uint8_t *buf, size_t buflen) {
        assert(buflen == MIN_LOG_HEADER_SIZE);
        int offset(0);
        memcpy(&_version, buf + offset, sizeof(_version));
        offset += sizeof(_version);
        memcpy(&_blockSize, buf + offset, sizeof(_blockSize));
        offset += sizeof(_blockSize);
        memcpy(&_blockCount, buf + offset, sizeof(_blockCount));
        offset += sizeof(_blockCount);
    }

    uint32_t version() const {
        return ntohl(_version);
    }

    uint32_t blockSize() const {
        return ntohl(_blockSize);
    }

    uint32_t blockCount() const {
        return ntohl(_blockCount);
    }

private:

    uint32_t _version;
    uint32_t _blockSize;
    uint32_t _blockCount;
};

typedef enum {
    ML_NEW, ML_DEL, ML_DEL_ALL, ML_COMMIT1, ML_COMMIT2
} mutation_log_type_t;

#define MUTATION_LOG_TYPES 5

extern const char *mutation_log_type_names[];

/**
 * An entry in the MutationLog.
 */
class MutationLogEntry {
public:

    /**
     * Initialize a new entry inside the given buffer.
     *
     * @param r the rowid
     * @param t the type of log entry
     * @param vb the vbucket
     * @param k the key
     */
    static MutationLogEntry* newEntry(uint8_t *buf,
                                      uint64_t r, mutation_log_type_t t,
                                      uint16_t vb, const std::string &k) {
        return new (buf) MutationLogEntry(r, t, vb, k);
    }

    /**
     * Initialize a new entry using the contents of the given buffer.
     *
     * @param buf a chunk of memory thought to contain a valid MutationLogEntry
     * @param buflen the length of said buf
     */
    static MutationLogEntry* newEntry(uint8_t *buf, size_t buflen) {
        assert(buflen >= len(0));
        MutationLogEntry *me = reinterpret_cast<MutationLogEntry*>(buf);
        assert(me->magic == MUTATION_LOG_MAGIC);
        assert(buflen >= me->len());
        return me;
    }

    void operator delete(void *) {
        // Statically buffered.  There is no delete.
    }

    /**
     * The size of a MutationLogEntry, in bytes, containing a key of
     * the specified length.
     */
    static size_t len(size_t klen) {
        // 13 == the exact empty record size as will be packed into
        // the layout
        return 13 + klen;
    }

    /**
     * The number of bytes of the serialized form of this
     * MutationLogEntry.
     */
    size_t len() const {
        return MutationLogEntry::len(keylen);
    }

    /**
     * This entry's key.
     */
    const std::string key() const {
        return std::string(_key, keylen);
    }

    /**
     * This entry's rowid.
     */
    uint64_t rowid() const;

    /**
     * This entry's vbucket.
     */
    uint16_t vbucket() const {
        return ntohs(_vbucket);
    }

    /**
     * The type of this log entry.
     */
    uint8_t type() const {
        return _type;
    }

private:

    friend std::ostream& operator<< (std::ostream& out,
                                     const MutationLogEntry &e);

    MutationLogEntry(uint64_t r, mutation_log_type_t t,
                     uint16_t vb, const std::string &k)
        : _rowid(htonll(r)), _vbucket(htons(vb)), magic(MUTATION_LOG_MAGIC),
          _type(static_cast<uint8_t>(t)),
          keylen(static_cast<uint8_t>(k.length())) {
        assert(k.length() <= std::numeric_limits<uint8_t>::max());
        memcpy(_key, k.data(), k.length());
    }

    uint64_t _rowid;
    uint16_t _vbucket;
    uint8_t  magic;
    uint8_t  _type;
    uint8_t  keylen;
    char     _key[1];

    DISALLOW_COPY_AND_ASSIGN(MutationLogEntry);
};

std::ostream& operator <<(std::ostream &out, const MutationLogEntry &mle);


/**
 * The MutationLog records major key events to allow ep-engine to more
 * quickly restore the server to its previous state upon restart.
 */
class MutationLog {
public:

    MutationLog(const std::string &path, const size_t bs=4096);

    ~MutationLog();

    void newItem(uint16_t vbucket, const std::string &key, uint64_t rowid);

    void delItem(uint16_t vbucket, const std::string &key);

    void deleteAll(uint16_t vbucket);

    void commit1();

    void commit2();

    void flush();

    void sync();

    void disable();

    bool isEnabled() const {
        return file != DISABLED_FD;
    }

    bool isOpen() const {
        return file >= 0;
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
    void open();

    bool setSyncConfig(const std::string &s);
    bool setFlushConfig(const std::string &s);

    /**
     * Reset the item type counts to the given values.
     *
     * This is used by the loader as part of initialization.
     */
    void resetCounts(size_t *);

    /**
     * Exception thrown upon failure to read a mutation log.
     */
    class ReadException : public std::runtime_error {
    public:
        ReadException(const std::string &s) : std::runtime_error(s) {}
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
     * An iterator for the mutation log.
     *
     * A ReadException may be thrown at any point along iteration.
     */
    class iterator  : public std::iterator<std::input_iterator_tag,
                                           const MutationLogEntry*> {
    public:

        iterator(const iterator& mit);

        ~iterator();

        iterator& operator++();

        iterator& operator++(int);

        bool operator==(const iterator& rhs);

        bool operator!=(const iterator& rhs);

        const MutationLogEntry* operator*();

    private:

        friend class MutationLog;

        iterator(const MutationLog *l, bool e=false);

        void nextBlock();
        size_t bufferBytesRemaining();
        void prepItem();

        const MutationLog *log;
        uint8_t           *entryBuf;
        uint8_t           *buf;
        uint8_t           *p;
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
    Atomic<size_t> itemsLogged[MUTATION_LOG_TYPES];
    //! Histogram of block padding sizes.
    Histogram<uint32_t> paddingHisto;
    //! Flush time histogram.
    Histogram<hrtime_t> flushTimeHisto;
    //! Sync time histogram.
    Histogram<hrtime_t> syncTimeHisto;
    //! Size of the log
    Atomic<size_t> logSize;

private:

    void writeEntry(MutationLogEntry *mle);

    void writeInitialBlock();
    void readInitialBlock();

    void prepareWrites();

    int fd() const { return file; }

    LogHeaderBlock     headerBlock;
    const std::string  logPath;
    size_t             blockSize;
    size_t             blockPos;
    int                file;
    uint16_t           entries;
    uint8_t           *entryBuffer;
    uint8_t           *blockBuffer;
    uint8_t            syncConfig;

    DISALLOW_COPY_AND_ASSIGN(MutationLog);
};

/// @cond DETAILS

//! rowid, (uint8_t)mutation_log_type_t
typedef std::pair<uint64_t, uint8_t> mutation_log_event_t;

/// @endcond

/**
 * MutationLogHarvester::apply callback type.
 */
typedef void (*mlCallback)(void*, uint16_t, uint16_t, const std::string &, uint64_t);

/**
 * Type for mutation log leftovers.
 */
struct mutation_log_uncommitted_t {
    std::string         key;
    uint64_t            rowid;
    mutation_log_type_t type;
    uint16_t            vbucket;
};

/**
 * Read log entries back from the log to reconstruct the state.
 */
class MutationLogHarvester {
public:
    MutationLogHarvester(MutationLog &ml) : mlog(ml) {
        memset(vbids, 0, sizeof(vbids));
        memset(itemsSeen, 0, sizeof(itemsSeen));
    }

    /**
     * Set a vbucket version before loading.
     *
     * Provided versions will be given to the callbacks at apply time.
     * vbuckets that are not registered here will not be considered.
     */
    void setVbVer(uint16_t vb, uint16_t ver) {
        vbids[vb] = ver;
        vbid_set.insert(vb);
    }

    /**
     * Load the entries from the file.
     *
     * @return true if the file was clean and can likely be trusted.
     */
    bool load();

    /**
     * Apply the processed log entries through the given function.
     */
    void apply(void *arg, mlCallback mlc);

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

    /**
     * Get the list of uncommitted keys and stuff from the log.
     */
    std::vector<mutation_log_uncommitted_t> getUncommitted();

private:

    MutationLog &mlog;

    std::set<uint16_t> vbid_set;
    uint16_t vbids[65536];

    unordered_map<uint16_t, unordered_map<std::string, uint64_t> > committed;
    unordered_map<uint16_t, unordered_map<std::string, mutation_log_event_t> > loading;
    size_t itemsSeen[MUTATION_LOG_TYPES];
};

#endif /* MUTATION_LOG_HH */
