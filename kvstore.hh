/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef KVSTORE_H
#define KVSTORE_H 1

#include <assert.h>
#include <stdbool.h>
#include <stdexcept>
#include <iostream>
#include <sstream>
#include <list>

#include "common.hh"
#include "callbacks.hh"
#include "item.hh"

/**
 * Value for callback for GET operations.
 */
class GetValue {
public:
    GetValue() : value(NULL), success(false) { }

    GetValue(bool s) : value(NULL), success(s) { }
    GetValue(Item *v) : value(v), success(true) { }

    /**
     * The value retrieved for the key.
     */
    Item* getValue() { return value; }

    /**
     * True if a value was successfully retrieved.
     */
    bool isSuccess() const { return success; }

private:

    Item* value;
    bool success;
};

/**
 * An individual kv storage (or way to access a kv storage).
 */
class KVStore {
public:

    KVStore() {}

    virtual ~KVStore() {}

    /**
     * Called after each test to reinitialize the test.
     */
    virtual void reset() {}

    /**
     * Method that should not return until the driver has done its job.
     *
     * @param c the callback that will fire when the noop is evalutated
     */
    virtual void noop(Callback<bool> &c) {
        bool t = true;
        c.callback(t);
    }

    /**
     * Set a given key and value.
     *
     * @param key the key to set
     * @param val the value to set
     * @param cb callback that will fire with true if the set succeeded
     */
    virtual void set(const Item &item,
                     Callback<std::pair<bool, int64_t> > &cb) = 0;

    /**
     * Get the value for the given key.
     *
     * @param key the key
     * @param cb callback that will fire with the retrieved value
     */
    virtual void get(const std::string &key, Callback<GetValue> &cb) = 0;

    /**
     * Delete a value for a key.
     *
     * @param key the key
     * @param cb callback that will fire with true if the value
     *           existed and then was deleted
     */
    virtual void del(const std::string &key, Callback<bool> &cb) = 0;

    /**
     * Dump the kvstore
     * @param cb callback that will fire with the value
     */
    virtual void dump(Callback<GetValue> &cb) = 0;

    /**
     * For things that support transactions, this signals the
     * beginning of one.
     */
    virtual void begin() {}

    /**
     * For things that support transactions, this signals the
     * successful completion of one.
     *
     * Returns true on success.
     */
    virtual bool commit() { return true; }

    /**
     * For things that support transactions, this signals the
     * unsuccessful completion of one.
     */
    virtual void rollback() {}

    /**
     * get the value for a give item and lock it
     */
    virtual bool getLocked(const std::string &key, Callback<GetValue> &cb, rel_time_t currentTime, uint32_t lockTimeout) {
        (void)key;
        (void)currentTime;
        (void) lockTimeout;
        GetValue v(false);
        cb.callback(v);
        return false;
    }

private:
    DISALLOW_COPY_AND_ASSIGN(KVStore);
};

#endif /* KVSTORE_H */
