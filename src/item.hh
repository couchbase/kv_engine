/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef ITEM_HH
#define ITEM_HH
#include "config.h"

#include <string>
#include <string.h>
#include <stdio.h>
#include <memcached/engine.h>

#include "mutex.hh"
#include "locks.hh"
#include "atomic.hh"
#include "objectregistry.hh"
#include "stats.hh"

/**
 * A blob is a minimal sized storage for data up to 2^32 bytes long.
 */
class Blob : public RCValue {
public:

    // Constructors.

    /**
     * Create a new Blob holding the given data.
     *
     * @param start the beginning of the data to copy into this blob
     * @param len the amount of data to copy in
     *
     * @return the new Blob instance
     */
    static Blob* New(const char *start, const size_t len) {
        size_t total_len = len + sizeof(Blob);
        Blob *t = new (::operator new(total_len)) Blob(start, len);
        assert(t->length() == len);
        return t;
    }

    /**
     * Create a new Blob holding the contents of the given string.
     *
     * @param s the string whose contents go into the blob
     *
     * @return the new Blob instance
     */
    static Blob* New(const std::string& s) {
        return New(s.data(), s.length());
    }

    /**
     * Create a new Blob pre-filled with the given character.
     *
     * @param len the size of the blob
     *
     * @return the new Blob instance
     */
    static Blob* New(const size_t len) {
        size_t total_len = len + sizeof(Blob);
        Blob *t = new (::operator new(total_len)) Blob(len);
        assert(t->length() == len);
        return t;
    }

    // Actual accessorish things.

    /**
     * Get the pointer to the contents of this Blob.
     */
    const char* getData() const {
        return data;
    }

    /**
     * Get the length of this Blob value.
     */
    size_t length() const {
        return size;
    }

    /**
     * Get the size of this Blob instance.
     */
    size_t getSize() const {
        return size + sizeof(Blob);
    }

    /**
     * Get a std::string representation of this blob.
     */
    const std::string to_s() const {
        return std::string(data, size);
    }

    // This is necessary for making C++ happy when I'm doing a
    // placement new on fairly "normal" c++ heap allocations, just
    // with variable-sized objects.
    void operator delete(void* p) { ::operator delete(p); }

    ~Blob() {
        ObjectRegistry::onDeleteBlob(this);
    }

private:

    explicit Blob(const char *start, const size_t len) :
        size(static_cast<uint32_t>(len))
    {
        std::memcpy(data, start, len);
        ObjectRegistry::onCreateBlob(this);
    }

    explicit Blob(const size_t len) :
        size(static_cast<uint32_t>(len))
    {
#ifdef VALGRIND
        memset(data, 0, len);
#endif
        ObjectRegistry::onCreateBlob(this);
    }

    const uint32_t size;
    char data[1];

    DISALLOW_COPY_AND_ASSIGN(Blob);
};

typedef SingleThreadedRCPtr<Blob> value_t;

const uint64_t DEFAULT_REV_SEQ_NUM = 1;

/**
 * The ItemMetaData structure is used to pass meata data information of
 * an Item.
 */
class ItemMetaData {
public:
    ItemMetaData() :
        cas(0), seqno(DEFAULT_REV_SEQ_NUM), flags(0), exptime(0) {
    }

    ItemMetaData(uint64_t c, uint64_t s, uint32_t f, time_t e) :
        cas(c), seqno(s == 0 ? DEFAULT_REV_SEQ_NUM : s), flags(f), exptime(e) {
    }

    uint64_t cas;
    uint64_t seqno;
    uint32_t flags;
    time_t exptime;
};

/**
 * The Item structure we use to pass information between the memcached
 * core and the backend. Please note that the kvstore don't store these
 * objects, so we do have an extra layer of memory copying :(
 */
class Item {
public:
    Item(const void* k, const size_t nk, const size_t nb,
         const uint32_t fl, const time_t exp, uint64_t theCas = 0,
         int64_t i = -1, uint16_t vbid = 0) :
        metaData(theCas, 1, fl, exp), id(i), vbucketId(vbid)
    {
        key.assign(static_cast<const char*>(k), nk);
        assert(id != 0);
        setData(NULL, nb);
        ObjectRegistry::onCreateItem(this);
    }

    Item(const std::string &k, const uint32_t fl, const time_t exp,
         const void *dta, const size_t nb, uint64_t theCas = 0,
         int64_t i = -1, uint16_t vbid = 0) :
        metaData(theCas, 1, fl, exp), id(i), vbucketId(vbid)
    {
        key.assign(k);
        assert(id != 0);
        setData(static_cast<const char*>(dta), nb);
        ObjectRegistry::onCreateItem(this);
    }

    Item(const std::string &k, const uint32_t fl, const time_t exp,
         const value_t &val, uint64_t theCas = 0,  int64_t i = -1, uint16_t vbid = 0,
         uint64_t sno = 1) :
         metaData(theCas, sno, fl, exp), value(val), id(i), vbucketId(vbid)
    {
        assert(id != 0);
        key.assign(k);
        ObjectRegistry::onCreateItem(this);
    }

    Item(const void *k, uint16_t nk, const uint32_t fl, const time_t exp,
         const void *dta, const size_t nb, uint64_t theCas = 0,
         int64_t i = -1, uint16_t vbid = 0, uint64_t sno = 1) :
         metaData(theCas, sno, fl, exp), id(i), vbucketId(vbid)
    {
        assert(id != 0);
        key.assign(static_cast<const char*>(k), nk);
        setData(static_cast<const char*>(dta), nb);
        ObjectRegistry::onCreateItem(this);
    }

    ~Item() {
        ObjectRegistry::onDeleteItem(this);
    }

    const char *getData() const {
        return value.get() ? value->getData() : NULL;
    }

    const value_t &getValue() const {
        return value;
    }

    const std::string &getKey() const {
        return key;
    }

    int64_t getId() const {
        return id;
    }

    void setId(int64_t to) {
        id = to;
    }

    int getNKey() const {
        return static_cast<int>(key.length());
    }

    uint32_t getNBytes() const {
        return value.get() ? static_cast<uint32_t>(value->length()) : 0;
    }

    size_t getValMemSize() const {
        return value.get() ? value->getSize() : 0;
    }

    time_t getExptime() const {
        return metaData.exptime;
    }

    uint32_t getFlags() const {
        return metaData.flags;
    }

    uint64_t getCas() const {
        return metaData.cas;
    }

    void setCas() {
        metaData.cas = nextCas();
    }

    void setCas(uint64_t ncas) {
        metaData.cas = ncas;
    }

    void setValue(const value_t &v) {
        value.reset(v);
    }

    void setFlags(uint32_t f) {
        metaData.flags = f;
    }

    void setExpTime(time_t exp_time) {
        metaData.exptime = exp_time;
    }

    /**
     * Append another item to this item
     *
     * @param item the item to append to this one
     * @return true if success
     */
    bool append(const Item &item);

    /**
     * Prepend another item to this item
     *
     * @param item the item to prepend to this one
     * @return true if success
     */
    bool prepend(const Item &item);

    uint16_t getVBucketId(void) const {
        return vbucketId;
    }

    void setVBucketId(uint16_t to) {
        vbucketId = to;
    }

    /**
     * Check if this item is expired or not.
     *
     * @param asOf the time to be compared with this item's expiry time
     * @return true if this item's expiry time < asOf
     */
    bool isExpired(time_t asOf) const {
        if (metaData.exptime != 0 && metaData.exptime < asOf) {
            return true;
        }
        return false;
    }

    size_t size() {
        return sizeof(Item) + key.size() + getValMemSize();
    }

    uint64_t getSeqno() const {
        return metaData.seqno;
    }

    void setSeqno(uint64_t to) {
        if (to == 0) {
            to = DEFAULT_REV_SEQ_NUM;
        }
        metaData.seqno = to;
    }

    static uint32_t getNMetaBytes() {
        return metaDataSize;
    }

    const ItemMetaData& getMetaData() const {
        return metaData;
    }

    static uint64_t nextCas(void) {
        uint64_t ret = gethrtime();
        if ((ret & 1000) == 0) {
            // we don't have a good enough resolution on the clock
            ret |= casCounter++;
            if (casCounter > 1000) {
                casCounter = 1;
            }
        }

        return ret;
    }

private:
    /**
     * Set the item's data. This is only used by constructors, so we
     * make it private.
     */
    void setData(const char *dta, const size_t nb) {
        Blob *data;
        if (dta == NULL) {
            data = Blob::New(nb);
        } else {
            data = Blob::New(dta, nb);
        }

        assert(data);
        value.reset(data);
    }

    ItemMetaData metaData;
    value_t value;
    std::string key;
    int64_t id;
    uint16_t vbucketId;

    static Atomic<uint64_t> casCounter;
    static const uint32_t metaDataSize;
    DISALLOW_COPY_AND_ASSIGN(Item);
};

#endif
