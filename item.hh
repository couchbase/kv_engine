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

class Blob {
public:

    // Constructors.

    static Blob* New(const char *start, const size_t len) {
        size_t total_len = len + sizeof(Blob);
        Blob *t = new (::operator new(total_len)) Blob(start, len);
        assert(t->length() == len);
        return t;
    }

    static Blob* New(const std::string& s) {
        return New(s.data(), s.length());
    }

    static Blob* New(const size_t len, const char c) {
        size_t total_len = len + sizeof(Blob);
        Blob *t = new (::operator new(total_len)) Blob(c, len);
        assert(t->length() == len);
        return t;
    }

    // Actual accessorish things.

    const char* getData() const {
        return data;
    }

    size_t length() const {
        return size;
    }

    const std::string to_s() const {
        return std::string(data, size);
    }

    // This is necessary for making C++ happy when I'm doing a
    // placement new on fairly "normal" c++ heap allocations, just
    // with variable-sized objects.
    void operator delete(void* p) { ::operator delete(p); }

private:

    explicit Blob(const char *start, const size_t len) : size(static_cast<uint32_t>(len)) {
        std::memcpy(data, start, len);
    }

    explicit Blob(const char c, const size_t len) : size(static_cast<uint32_t>(len)) {
        std::memset(data, c, len);
    }

    const uint32_t size;
    char data[1];

    DISALLOW_COPY_AND_ASSIGN(Blob);
};

typedef shared_ptr<const Blob> value_t;

/**
 * The Item structure we use to pass information between the memcached
 * core and the backend. Please note that the kvstore don't store these
 * objects, so we do have an extra layer of memory copying :(
 */
class Item {
public:
    Item(const void* k, const size_t nk, const size_t nb,
         const int fl, const rel_time_t exp, uint64_t theCas = 0,
         int64_t i = -1) :
        flags(fl), exptime(exp), cas(theCas), id(i)
    {
        key.assign(static_cast<const char*>(k), nk);
        assert(id != 0);
        setData(NULL, nb);
    }

    Item(const std::string &k, const int fl, const rel_time_t exp,
         const void *dta, const size_t nb, uint64_t theCas = 0, int64_t i = -1) :
        flags(fl), exptime(exp), cas(theCas), id(i)
    {
        key.assign(k);
        assert(id != 0);
        setData(static_cast<const char*>(dta), nb);
    }

    Item(const std::string &k, const int fl, const rel_time_t exp,
         value_t val, uint64_t theCas = 0, int64_t i = -1) :
        flags(fl), exptime(exp), value(val), cas(theCas), id(i)
    {
        assert(id != 0);
        key.assign(k);
    }

    Item(const void *k, uint16_t nk, const int fl, const rel_time_t exp,
         const void *dta, const size_t nb, uint64_t theCas = 0, int64_t i = -1) :
        flags(fl), exptime(exp), cas(theCas), id(i)
    {
        assert(id != 0);
        key.assign(static_cast<const char*>(k), nk);
        setData(static_cast<const char*>(dta), nb);
    }

    ~Item() {}

    const char *getData() const {
        return value->getData();
    }

    value_t getValue() const {
        return value;
    }

    const std::string &getKey() const {
        return key;
    }

    int64_t getId() const {
        return id;
    }

    int getNKey() const {
        return static_cast<int>(key.length());
    }

    uint32_t getNBytes() const {
        return static_cast<uint32_t>(value->length());
    }

    rel_time_t getExptime() const {
        return exptime;
    }

    int getFlags() const {
        return flags;
    }

    uint64_t getCas() const {
        return cas;
    }

    void setCas() {
        cas = nextCas();
    }

    void setCas(uint64_t ncas) {
        cas = ncas;
    }

    /**
     * Append another item to this item
     *
     * @param item the item to append to this one
     * @return true if success
     */
    bool append(const Item &item) {
        std::string newValue(value->getData(), 0, value->length() - 2);
        newValue.append(item.getValue()->to_s());
        value.reset(Blob::New(newValue));
        return true;
    }

    /**
     * Prepend another item to this item
     *
     * @param item the item to prepend to this one
     * @return true if success
     */
    bool prepend(const Item &item) {
        std::string newValue(item.getValue()->to_s(), 0, item.getNBytes() - 2);
        newValue.append(value->to_s());
        value.reset(Blob::New(newValue));
        return true;
    }

private:
    /**
     * Set the item's data. This is only used by constructors, so we
     * make it private.
     */
    void setData(const char *dta, const size_t nb) {
        Blob *data;
        if (dta == NULL) {
            data = Blob::New(nb, '\0');
        } else {
            data = Blob::New(dta, nb);
        }

        assert(data);
        value.reset(data);
    }

    int flags;
    rel_time_t exptime;
    std::string key;
    value_t value;
    uint64_t cas;
    int64_t id;

    static uint64_t nextCas(void) {
        uint64_t ret;
        ret = casCounter++;
        if ((ret % casNotificationFrequency) == 0) {
            casNotifier(ret);
        }

        return ret;
    }

    static void initializeCas(uint64_t initial, void (*notifier)(uint64_t current),
                              uint64_t frequency) {
        casCounter = initial;
        casNotifier = notifier;
        casNotificationFrequency = frequency;
    }

    static uint64_t casNotificationFrequency;
    static void (*casNotifier)(uint64_t);
    static Atomic<uint64_t> casCounter;
    DISALLOW_COPY_AND_ASSIGN(Item);
};

#endif
