/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef STORED_VALUE_H
#define STORED_VALUE_H 1

#include <climits>
#include <cstring>
#include <algorithm>

#include "common.hh"
#include "item.hh"
#include "locks.hh"

extern "C" {
    extern rel_time_t (*ep_current_time)();
}

// Forward declaration for StoredValue
class HashTable;

class StoredValue {
public:

    static StoredValue* New(const Item &itm, StoredValue *n, bool setDirty = true) {
        std::string key = itm.getKey();
        size_t len = key.length() + sizeof(StoredValue);

        StoredValue *t = new (::operator new(len))
            StoredValue(itm, n, setDirty);
        t->keylen = key.length();
        std::memcpy(t->keybytes, key.data(), key.length());

        return t;
    }

    void operator delete(void* p) {
        ::operator delete(p);
     }

    ~StoredValue() {
    }

    void markDirty() {
        data_age = ep_current_time();
        if (!isDirty()) {
            dirtied = data_age;
        }
    }

    void reDirty(rel_time_t dirtyAge, rel_time_t dataAge) {
        data_age = dataAge;
        dirtied = dirtyAge;
    }

    // returns time this object was dirtied.
    void markClean(rel_time_t *dirtyAge, rel_time_t *dataAge) {
        if (dirtyAge) {
            *dirtyAge = dirtied;
        }
        if (dataAge) {
            *dataAge = data_age;
        }
        dirtied = 0;
        data_age = 0;
    }

    bool isDirty() const {
        return dirtied != 0;
    }

    bool isClean() const {
        return dirtied == 0;
    }

    const char* getKeyBytes() const {
        return keybytes;
    }

    uint8_t getKeyLen() const {
        return keylen;
    }

    bool hasKey(const std::string &k) const {
        return k.length() == keylen
            && (std::memcmp(k.data(), keybytes, keylen) == 0);
    }

    const std::string getKey() const {
        return std::string(keybytes, keylen);
    }

    value_t getValue() const {
        return value;
    }

    rel_time_t getExptime() const {
        return exptime;
    }

    uint32_t getFlags() const {
        return flags;
    }

    void setValue(value_t v,
                  uint32_t newFlags, rel_time_t newExp, uint64_t theCas) {
        cas = theCas;
        flags = newFlags;
        exptime = newExp;
        value = v;
        markDirty();
    }

    uint64_t getCas() const {
        return cas;
    }

    // for stats
    rel_time_t getDirtied() const {
        return dirtied;
    }

    rel_time_t getDataAge() const {
        return data_age;
    }

    void setCas(uint64_t c) {
        cas = c;
    }

    void lock(rel_time_t expiry) {
        locked = true;
        lock_expiry = expiry;
    }

    void unlock() {
        locked = false;
        lock_expiry = 0;
    }

    bool hasId() {
        return id > 0;
    }

    int64_t getId() {
        return id;
    }

    void setId(int64_t to) {
        assert(!hasId());
        id = to;
    }

    bool isLocked(rel_time_t curtime)  {
        if (locked && (curtime > lock_expiry)) {
            locked = false;
            return locked;
        }
        return locked;
    }

private:

    StoredValue(const Item &itm, StoredValue *n, bool setDirty = true) :
        value(itm.getValue()),
        flags(itm.getFlags()), exptime(itm.getExptime()), dirtied(0), next(n),
        cas(itm.getCas()), id(itm.getId()), locked(false), lock_expiry(0)
    {
        if (setDirty) {
            markDirty();
        } else {
            markClean(NULL, NULL);
        }
    }

    friend class HashTable;

    value_t value;
    uint32_t flags;
    rel_time_t exptime;
    rel_time_t dirtied;
    rel_time_t data_age;
    StoredValue *next;
    uint64_t cas;
    int64_t id;
    bool locked;
    rel_time_t lock_expiry;
    uint8_t keylen;
    char keybytes[1];
    DISALLOW_COPY_AND_ASSIGN(StoredValue);
};

typedef enum {
    NOT_FOUND, INVALID_CAS, WAS_CLEAN, WAS_DIRTY, IS_LOCKED, SUCCESS
} mutation_type_t;

class HashTableVisitor {
public:
    virtual ~HashTableVisitor() {}
    virtual void visit(StoredValue *v) = 0;
};

class HashTableDepthVisitor {
public:
    virtual ~HashTableDepthVisitor() {}
    virtual void visit(int bucket, int depth) = 0;
};

class HashTableDepthStatVisitor : public HashTableDepthVisitor {
public:

    HashTableDepthStatVisitor() : min(INT_MAX), max(0) {}

    void visit(int bucket, int depth) {
        (void)bucket;
        min = std::min(min, depth);
        max = std::max(max, depth);
    }

    int min;
    int max;
};

class HashTable {
public:

    // Construct with number of buckets and locks.
    HashTable(size_t s = 0, size_t l = 0) {
        size = HashTable::getNumBuckets(s);
        n_locks = HashTable::getNumLocks(l);
        assert(size > 0);
        assert(n_locks > 0);
        active = true;
        values = new StoredValue*[size];
        std::fill_n(values, size, static_cast<StoredValue*>(NULL));
        mutexes = new Mutex[n_locks];
        depths = new int[size];
        std::fill_n(depths, size, 0);
    }

    ~HashTable() {
        clear();
        active = false;
        delete []mutexes;
        delete []values;
        delete []depths;
    }

    size_t getSize(void) { return size; }
    size_t getNumLocks(void) { return n_locks; }

    void clear();

    StoredValue *find(std::string &key) {
        assert(active);
        int bucket_num = bucket(key);
        LockHolder lh(getMutex(bucket_num));
        return unlocked_find(key, bucket_num);
    }

    mutation_type_t set(const Item &val) {
        assert(active);
        mutation_type_t rv = NOT_FOUND;
        int bucket_num = bucket(val.getKey());
        LockHolder lh(getMutex(bucket_num));
        StoredValue *v = unlocked_find(val.getKey(), bucket_num);
        Item &itm = const_cast<Item&>(val);
        if (v) {

            if (v->isLocked(ep_current_time())) {
                /*
                 * item is locked, deny if there is cas value mismatch
                 * or no cas value is provided by the user
                 */
                 if (val.getCas() != v->getCas()) {
                     return IS_LOCKED;
                 }
                 /* allow operation*/
                 v->unlock();
            } else if (val.getCas() != 0 && val.getCas() != v->getCas()) {
                return INVALID_CAS;
            }
            itm.setCas();
            rv = v->isClean() ? WAS_CLEAN : WAS_DIRTY;
            v->setValue(itm.getValue(),
                        itm.getFlags(), itm.getExptime(),
                        itm.getCas());
        } else {
            if (itm.getCas() != 0) {
                return INVALID_CAS;
            }
            itm.setCas();
            v = StoredValue::New(itm, values[bucket_num]);
            values[bucket_num] = v;
            depths[bucket_num]++;
        }
        return rv;
    }

    bool add(const Item &val, bool isDirty = true) {
        assert(active);
        int bucket_num = bucket(val.getKey());
        LockHolder lh(getMutex(bucket_num));
        StoredValue *v = unlocked_find(val.getKey(), bucket_num);
        if (v) {
            return false;
        } else {
            Item &itm = const_cast<Item&>(val);
            itm.setCas();
            v = StoredValue::New(itm, values[bucket_num], isDirty);
            values[bucket_num] = v;
            depths[bucket_num]++;
        }

        return true;
    }

    StoredValue *unlocked_find(const std::string &key, int bucket_num) {
        StoredValue *v = values[bucket_num];
        while (v) {
            if (v->hasKey(key)) {
                return v;
            }
            v = v->next;
        }
        return NULL;
    }

    inline int bucket(const char *str, const size_t len) {
        assert(active);
        int h=5381;

        for(size_t i=0; i < len; i++) {
            h = ((h << 5) + h) ^ str[i];
        }

        return abs(h) % (int)size;
    }

    inline int bucket(const std::string &s) {
        return bucket(s.data(), s.length());
    }

    // Get the mutex for a bucket (for doing your own lock management)
    inline Mutex &getMutex(int bucket_num) {
        assert(active);
        assert(bucket_num < (int)size);
        assert(bucket_num >= 0);
        int lock_num = bucket_num % (int)n_locks;
        assert(lock_num < (int)n_locks);
        assert(lock_num >= 0);
        return mutexes[lock_num];
    }

    // True if it existed
    bool del(const std::string &key) {
        assert(active);
        int bucket_num = bucket(key);
        LockHolder lh(getMutex(bucket_num));

        StoredValue *v = values[bucket_num];

        // Special case empty bucket.
        if (!v) {
            return false;
        }

        // Special case the first one
        if (v->hasKey(key)) {
            if (v->isLocked(ep_current_time())) {
                return false;
            }
            values[bucket_num] = v->next;
            depths[bucket_num]--;
            delete v;
            return true;
        }

        while (v->next) {
            if (v->next->hasKey(key)) {
                StoredValue *tmp = v->next;
                if (tmp->isLocked(ep_current_time())) {
                    return false;
                }
                v->next = v->next->next;
                delete tmp;
                depths[bucket_num]--;
                return true;
            } else {
                v = v->next;
            }
        }

        return false;
    }

    void visit(HashTableVisitor &visitor);

    void visitDepth(HashTableDepthVisitor &visitor);

    static size_t getNumBuckets(size_t);
    static size_t getNumLocks(size_t);

    static void setDefaultNumBuckets(size_t);
    static void setDefaultNumLocks(size_t);

private:
    size_t        size;
    size_t        n_locks;
    bool          active;
    StoredValue **values;
    Mutex        *mutexes;
    int          *depths;

    static size_t defaultNumBuckets;
    static size_t defaultNumLocks;

    DISALLOW_COPY_AND_ASSIGN(HashTable);
};

#endif /* STORED_VALUE_H */
