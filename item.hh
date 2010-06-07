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

typedef shared_ptr<const std::string> value_t;

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
        return value->c_str();
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
        std::string *newValue = new std::string(*value, 0, value->length() - 2);
        if (newValue != NULL) {
            newValue->append(*item.getValue());
            value.reset(newValue);
            return true;
        } else {
            return false;
        }
    }

    /**
     * Prepend another item to this item
     *
     * @param item the item to prepend to this one
     * @return true if success
     */
    bool prepend(const Item &item) {
        std::string *newValue = new std::string(*item.getValue(), 0, item.getNBytes() - 2);
        if (newValue != NULL) {
            newValue->append(*value);
            value.reset(newValue);
            return true;
        } else {
            return false;
        }
    }

private:
    /**
     * Set the item's data. This is only used by constructors, so we
     * make it private.
     */
    void setData(const char *dta, const size_t nb) {
        std::string *data;
        if (dta == NULL) {
            data = new std::string(nb, '\0');
        } else {
            data = new std::string(dta, nb);
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
