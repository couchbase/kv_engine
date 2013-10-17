/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013 Couchbase, Inc
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

#ifndef SRC_CALLBACKS_H_
#define SRC_CALLBACKS_H_ 1

#include "config.h"

#include <cassert>
#include <string>

#include "locks.h"

class Item;

class CacheLookup {
public:
    CacheLookup(std::string k, int64_t s, uint16_t vb) :
        key(k), bySeqno(s), vbid(vb) {}

    ~CacheLookup() {}

    std::string& getKey() { return key; }

    int64_t getBySeqno() { return bySeqno; }

    uint16_t getVBucketId() { return vbid; }
private:
    std::string key;
    int64_t bySeqno;
    uint16_t vbid;
};

/**
 * Value for callback for GET operations.
 */
class GetValue {
public:
    GetValue() : value(NULL), id(-1),
                 status(ENGINE_KEY_ENOENT),
                 partial(false), nru(0xff) { }

    explicit GetValue(Item *v, ENGINE_ERROR_CODE s=ENGINE_SUCCESS,
                      uint64_t i = -1,
                      bool incomplete = false, uint8_t _nru = 0xff) :
        value(v), id(i), status(s), partial(incomplete), nru(_nru) { }

    /**
     * The value retrieved for the key.
     */
    Item* getValue() { return value; }

    /**
     * Engine code describing what happened.
     */
    ENGINE_ERROR_CODE getStatus() const { return status; }

    /**
     * Set the status code
     */
    void setStatus(ENGINE_ERROR_CODE s) { status = s; }

    /**
     * Get the item's underlying ID (if applicable).
     */
    uint64_t getId() { return id; }

    /**
     * Set the item's underlying ID.
     */
    void setId(uint64_t newId) { id = newId; }

    bool isPartial() const { return partial; }

    void setPartial() { partial = true; }

    uint8_t getNRUValue() const { return nru; }

    void setValue(Item *i) { value = i; }

private:

    Item* value;
    uint64_t id;
    ENGINE_ERROR_CODE status;
    bool partial;
    uint8_t nru;
};

/**
 * Interface for callbacks from storage APIs.
 */
template <typename RV>
class Callback {
public:

    Callback() : myStatus(0) {}

    virtual ~Callback() {}

    /**
     * Method called on callback.
     */
    virtual void callback(RV &value) = 0;

    virtual void setStatus(int status) {
        myStatus = status;
    }

    virtual int getStatus() {
        return myStatus;
    }

private:

    int myStatus;
};

/**
 * Threadsafe callback implementation that just captures the value.
 */
template <typename T>
class RememberingCallback : public Callback<T> {
public:

    /**
     * Construct a remembering callback.
     */
    RememberingCallback() : fired(false), so() { }

    /**
     * Clean up (including lock resources).
     */
    ~RememberingCallback() {
    }

    /**
     * The callback implementation -- just store a value.
     */
    void callback(T &value) {
        LockHolder lh(so);
        val = value;
        fired = true;
        so.notify();
    }

    /**
     * Wait for a value to be available.
     *
     * This method will return immediately if a value is currently
     * available, otherwise it will wait indefinitely for a value
     * to arrive.
     */
    void waitForValue() {
        LockHolder lh(so);
        if (!fired) {
            so.wait();
        }
        assert(fired);
    }

    /**
     * The value that was captured from the callback.
     */
    T    val;
    /**
     * True if the callback has fired.
     */
    bool fired;

private:
    SyncObject so;

    DISALLOW_COPY_AND_ASSIGN(RememberingCallback);
};

template <typename T>
class TimedRememberingCallback : public RememberingCallback<T> {
public:
    TimedRememberingCallback() :
        RememberingCallback<T>(), start(gethrtime()), stop(0)
    { }

    ~TimedRememberingCallback() {
    }

    void callback(T &value) {
        stop = gethrtime();
        RememberingCallback<T>::callback(value);
    }

    hrtime_t getDelta() const {
        return stop - start;
    }

private:
    hrtime_t start;
    hrtime_t stop;

    DISALLOW_COPY_AND_ASSIGN(TimedRememberingCallback);
};

#endif  // SRC_CALLBACKS_H_
