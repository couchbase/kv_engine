/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef ITEM_HH
#define ITEM_HH
#include "config.h"
#include "mutex.hh"
#include <string>
#include <string.h>
#include <stdio.h>
#include <memcached/engine.h>

/**
 * The Item structure we use to pass information between the memcached
 * core and the backend. Please note that the kvstore don't store these
 * objects, so we do have an extra layer of memory copying :(
 */
class Item {
public:
    Item() {
        initialize("", 0, 0, NULL, 0, 0);
    }

    Item(const void* k, const size_t nk, const size_t nb,
         const int fl, const rel_time_t exp)
    {
        std::string _k(static_cast<const char*>(k), nk);
        initialize(_k, fl, exp, NULL, nb, 0);
    }

    Item(const void* k, const size_t nk, const size_t nb,
         const int fl, const rel_time_t exp, uint64_t theCas)
    {
        std::string _k(static_cast<const char*>(k), nk);
        initialize(_k, fl, exp, NULL, nb, theCas);
    }

    Item(const std::string &k, const int fl, const rel_time_t exp,
         const void *dta, const size_t nb)
    {
        initialize(k, fl, exp, static_cast<const char*>(dta), nb, 0);
    }

    Item(const std::string &k, const int fl, const rel_time_t exp,
         const void *dta, const size_t nb, uint64_t theCas)
    {
        initialize(k, fl, exp, static_cast<const char*>(dta), nb, theCas);
    }

    Item(const std::string &k, const int fl, const rel_time_t exp,
         const std::string &val)
    {
        initialize(k, fl, exp, val.c_str(), val.size(), 0);
    }

    Item(const std::string &k, const int fl, const rel_time_t exp,
         const std::string &val, uint64_t theCas)
    {
        initialize(k, fl, exp, val.c_str(), val.size(), theCas);
    }

    Item(const void *k, uint16_t nk, const int fl, const rel_time_t exp,
         const void *dta, const size_t nb) :
        key(static_cast<const char*>(k), nk)
    {
        std::string _k(static_cast<const char*>(k), nk);
        initialize(_k, fl, exp, static_cast<const char*>(dta), nb, 0);
    }

    Item(const void *k, uint16_t nk, const int fl, const rel_time_t exp,
         const void *dta, const size_t nb, uint64_t theCas) :
        key(static_cast<const char*>(k), nk)
    {
        std::string _k(static_cast<const char*>(k), nk);
        initialize(_k, fl, exp, static_cast<const char*>(dta), nb, theCas);
    }

    ~Item() {
        delete []data;
    }

    Item(const Item &itm) {
        initialize(itm.key, itm.flags, itm.exptime, itm.data, itm.nbytes, itm.cas);
    }

    Item* clone() {
        return new Item(key, flags, exptime, data, nbytes, cas);
    }

    char *getData() {
        return data;
    }

    const std::string &getKey() const {
        return key;
    }

    int getNKey() const {
        return key.length();
    }

    uint32_t getNBytes() const {
        return nbytes;
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

private:
    /**
     * Initialize all of the members in this object. Unfortunately the items
     * needs to end with "\r\n". Initialize adds this sequence if you pass
     * data along and append this sequence.
     */
    void initialize(const std::string &k, const int fl, const rel_time_t exp,
                    const char *dta, const size_t nb, uint64_t theCas)
    {
        key.assign(k);
        nbytes = static_cast<uint32_t>(nb);
        flags = fl;
        exptime = exp;
        cas = theCas;

        if (dta != NULL) {
            if (nbytes < 2 || memcmp(dta + nb - 2, "\r\n", 2) != 0) {
                nbytes += 2;
            }
        }

        if (nbytes > 0) {
            data = new char[nbytes];
        } else {
            data = NULL;
        }

        if (data && dta) {
            memcpy(data, dta, nbytes);
            if (nb != nbytes) {
                memcpy(data + nb, "\r\n", 2);
            }
        }
    }

    int flags;
    rel_time_t exptime;
    uint32_t nbytes;
    std::string key;
    char *data;
    uint64_t cas;

    static uint64_t nextCas(void) {
        uint64_t ret;
        casMutex.aquire();
        ret = casCounter++;
        casMutex.release();
        if ((ret % casNotificationFrequency) == 0) {
            casNotifier(ret);
        }

        return ret;
    }

    static void initializeCas(uint64_t initial, void (*notifier)(uint64_t current), uint64_t frequency) {
        casCounter = initial;
        casNotifier = notifier;
        casNotificationFrequency = frequency;
    }

    static uint64_t casNotificationFrequency;
    static void (*casNotifier)(uint64_t);
    static uint64_t casCounter;
    static Mutex casMutex;
};

#endif
