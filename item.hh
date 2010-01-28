#ifndef ITEM_HH
#define ITEM_HH
#include "config.h"
#include <stdio.h>
#include <memcached/engine.h>

/**
 * The Item structure we use to pass information between the memcached
 * core and the backend. Please note that the kvstore don't store these
 * objects, so we do have an extra layer of memory copying :(
 */
class Item : public item {
public:
    Item() {
        initialize("", 0, 0, NULL, 0);
    }

    Item(const void* k, const size_t nk, const size_t nb,
         const int fl, const rel_time_t exp)
    {
        std::string _k(static_cast<const char*>(k), nk);
        initialize(_k, fl, exp, NULL, nb);
    }

    Item(const std::string &k, const int fl, const rel_time_t exp,
         const void *dta, const size_t nb)
    {
        initialize(k, fl, exp, static_cast<const char*>(dta), nb);
    }

    Item(const std::string &k, const int fl, const rel_time_t exp,
         const std::string &val)
    {
        initialize(k, fl, exp, val.c_str(), val.size());
    }

    Item(const void *k, uint16_t nk, const int fl, const rel_time_t exp,
         const void *dta, const size_t nb) :
         key(static_cast<const char*>(k), nk)
    {
        std::string _k(static_cast<const char*>(k), nk);
        initialize(_k, fl, exp, static_cast<const char*>(dta), nb);
    }

    ~Item() {
        delete []data;
    }

    Item(const Item &itm) {
        initialize(itm.key, itm.flags, itm.exptime, itm.data, itm.nbytes);
    }

    Item* clone() {
        return new Item(key, flags, exptime, data, nbytes);
    }

    char *getData() {
        return data;
    }

    const std::string &getKey() const {
        return key;
    }

private:
    /**
     * Initialize all of the members in this object. Unfortunately the items
     * needs to end with "\r\n". Initialize adds this sequence if you pass
     * data along and append this sequence.
     */
    void initialize(const std::string &k, const int fl, const rel_time_t exp,
                    const char *dta, const size_t nb)
    {
        key.assign(k);
        nkey = static_cast<uint16_t>(key.length());
        nbytes = static_cast<uint32_t>(nb);
        flags = fl;
        iflag = 0;
        exptime = exp;

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

    std::string key;
    char *data;
};

#endif
