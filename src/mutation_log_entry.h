/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

#pragma once

#include "storeddockey.h"
#include "utility.h"

#include <type_traits>

enum class MutationLogType : uint8_t {
    New = 0,
    /* removed: ML_DEL = 1 */
    /* removed: ML_DEL_ALL = 2 */
    Commit1 = 3,
    Commit2 = 4,
    NumberOfTypes
};

std::string to_string(MutationLogType t);

class MutationLogEntryV2;

/**
 * An entry in the MutationLog.
 * This is the V1 layout which pre-dates the addition of document namespaces and
 * is only defined to permit upgrading to V2.
 */
class MutationLogEntryV1 {
public:
    static const uint8_t MagicMarker = 0x45;

    /**
     * Initialize a new entry using the contents of the given buffer.
     *
     * @param buf a chunk of memory thought to contain a valid
     *        MutationLogEntryV1
     * @param buflen the length of said buf
     */
    static const MutationLogEntryV1* newEntry(
            std::vector<uint8_t>::const_iterator itr, size_t buflen) {
        if (buflen < len(0)) {
            throw std::invalid_argument(
                    "MutationLogEntryV1::newEntry: buflen "
                    "(which is " +
                    std::to_string(buflen) +
                    ") is less than minimum required (which is " +
                    std::to_string(len(0)) + ")");
        }

        const auto* me = reinterpret_cast<const MutationLogEntryV1*>(&(*itr));

        if (me->magic != MagicMarker) {
            throw std::invalid_argument(
                    "MutationLogEntryV1::newEntry: "
                    "magic (which is " +
                    std::to_string(me->magic) + ") is not equal to " +
                    std::to_string(MagicMarker));
        }
        if (me->len() > buflen) {
            throw std::invalid_argument(
                    "MutationLogEntryV1::newEntry: "
                    "entry length (which is " +
                    std::to_string(me->len()) +
                    ") is greater than available buflen (which is " +
                    std::to_string(buflen) + ")");
        }
        return me;
    }

    void operator delete(void*) {
        // Statically buffered.  There is no delete.
        throw std::logic_error("MutationLogEntryV1 delete is not allowed");
    }

    /**
     * The size of a MutationLogEntryV1, in bytes, containing a key of
     * the specified length.
     */
    static size_t len(size_t klen) {
        // 13 == the exact empty record size as will be packed into
        // the layout
        return 13 + klen;
    }

    /**
     * The number of bytes of the serialized form of this
     * MutationLogEntryV1.
     */
    size_t len() const {
        return len(keylen);
    }

    /**
     * This entry's key.
     */
    const std::string key() const {
        return std::string(_key, keylen);
    }

    uint8_t getKeylen() const {
        return keylen;
    }

    /**
     * This entry's rowid.
     */
    uint64_t rowid() const {
        return ntohll(_rowid);
    }

    /**
     * This entry's vbucket.
     */
    uint16_t vbucket() const {
        return ntohs(_vbucket);
    }

    /**
     * The type of this log entry.
     */
    MutationLogType type() const {
        return _type;
    }

protected:
    friend MutationLogEntryV2;

    MutationLogEntryV1(uint64_t r,
                       MutationLogType t,
                       uint16_t vb,
                       const std::string& k)
        : _rowid(htonll(r)),
          _vbucket(htons(vb)),
          magic(MagicMarker),
          _type(t),
          keylen(static_cast<uint8_t>(k.length())) {
        if (k.length() > std::numeric_limits<uint8_t>::max()) {
            throw std::invalid_argument(
                    "MutationLogEntryV1(): key length "
                    "(which is " +
                    std::to_string(k.length()) + ") is greater than " +
                    std::to_string(std::numeric_limits<uint8_t>::max()));
        }
        memcpy(_key, k.data(), k.length());
    }

    friend std::ostream& operator<<(std::ostream& out,
                                    const MutationLogEntryV1& e);

    uint64_t _rowid;
    uint16_t _vbucket;
    uint8_t magic;
    MutationLogType _type;
    uint8_t keylen;
    char _key[1];

    DISALLOW_COPY_AND_ASSIGN(MutationLogEntryV1);
};

std::ostream& operator<<(std::ostream& out, const MutationLogEntryV1& mle);

/**
 * An entry in the MutationLog.
 * This is the V2 layout which stores document namespaces.
 */
class MutationLogEntryV2 {
public:
    static const uint8_t MagicMarker = 0x46;

    /**
     * Construct a V2 from V1, this places the key into the default collection
     * No constructor delegation, copy the values raw (so no byte swaps occur)
     * key is initialised into the default collection
     */
    MutationLogEntryV2(const MutationLogEntryV1& mleV1)
        : _rowid(mleV1._rowid),
          _vbucket(mleV1._vbucket),
          magic(MagicMarker),
          _type(mleV1._type),
          pad(0),
          _key({(const uint8_t*)mleV1._key,
                mleV1.keylen,
                DocNamespace::DefaultCollection}) {
    }

    /**
     * Initialize a new entry inside the given buffer.
     *
     * @param r the rowid
     * @param t the type of log entry
     * @param vb the vbucket
     * @param k the key
     */
    static MutationLogEntryV2* newEntry(uint8_t* buf,
                                        uint64_t r,
                                        MutationLogType t,
                                        uint16_t vb,
                                        const DocKey& k) {
        return new (buf) MutationLogEntryV2(r, t, vb, k);
    }

    static MutationLogEntryV2* newEntry(uint8_t* buf,
                                        uint64_t r,
                                        MutationLogType t,
                                        uint16_t vb) {
        if (MutationLogType::Commit1 != t && MutationLogType::Commit2 != t) {
            throw std::invalid_argument(
                    "MutationLogEntryV2::newEntry: invalid type");
        }
        return new (buf) MutationLogEntryV2(r, t, vb);
    }

    /**
     * Initialize a new entry using the contents of the given buffer.
     *
     * @param buf a chunk of memory thought to contain a valid
     *        MutationLogEntryV2
     * @param buflen the length of said buf
     */
    static const MutationLogEntryV2* newEntry(
            std::vector<uint8_t>::const_iterator itr, size_t buflen) {
        if (buflen < len(0)) {
            throw std::invalid_argument(
                    "MutationLogEntryV2::newEntry: buflen "
                    "(which is " +
                    std::to_string(buflen) +
                    ") is less than minimum required (which is " +
                    std::to_string(len(0)) + ")");
        }

        const auto* me = reinterpret_cast<const MutationLogEntryV2*>(&(*itr));

        if (me->magic != MagicMarker) {
            throw std::invalid_argument(
                    "MutationLogEntryV2::newEntry: "
                    "magic (which is " +
                    std::to_string(me->magic) + ") is not equal to " +
                    std::to_string(MagicMarker));
        }
        if (me->len() > buflen) {
            throw std::invalid_argument(
                    "MutationLogEntryV2::newEntry: "
                    "entry length (which is " +
                    std::to_string(me->len()) +
                    ") is greater than available buflen (which is " +
                    std::to_string(buflen) + ")");
        }
        return me;
    }

    void operator delete(void*) {
        // Statically buffered.  There is no delete.
        throw std::logic_error("MutationLogEntryV2 delete is not allowed");
    }

    /**
     * The size of a MutationLogEntryV2, in bytes, containing a key of
     * the specified length.
     */
    static size_t len(size_t klen) {
        // the exact empty record size as will be packed into the layout
        return sizeof(MutationLogEntryV2) + (klen - 1);
    }

    /**
     * The number of bytes of the serialized form of this
     * MutationLogEntryV2.
     */
    size_t len() const {
        return len(_key.size());
    }

    /**
     * This entry's key.
     */
    const SerialisedDocKey& key() const {
        return _key;
    }

    /**
     * This entry's rowid.
     */
    uint64_t rowid() const {
        return ntohll(_rowid);
    }

    /**
     * This entry's vbucket.
     */
    uint16_t vbucket() const {
        return ntohs(_vbucket);
    }

    /**
     * The type of this log entry.
     */
    MutationLogType type() const {
        return _type;
    }

private:
    friend std::ostream& operator<<(std::ostream& out,
                                    const MutationLogEntryV2& e);

    MutationLogEntryV2(uint64_t r,
                       MutationLogType t,
                       uint16_t vb,
                       const DocKey& k)
        : _rowid(htonll(r)),
          _vbucket(htons(vb)),
          magic(MagicMarker),
          _type(t),
          pad(0),
          _key(k) {
        (void)pad;
        // Assert that _key is the final member
        static_assert(
                offsetof(MutationLogEntryV2, _key) ==
                        (sizeof(MutationLogEntryV2) - sizeof(SerialisedDocKey)),
                "_key must be the final member of MutationLogEntryV2");
    }

    MutationLogEntryV2(uint64_t r, MutationLogType t, uint16_t vb)
        : MutationLogEntryV2(
                  r, t, vb, {nullptr, 0, DocNamespace::DefaultCollection}) {
    }

    uint64_t _rowid;
    uint16_t _vbucket;
    uint8_t magic;
    MutationLogType _type;
    uint8_t pad; // explicit padding to ensure _key is the final member
    SerialisedDocKey _key;

    DISALLOW_COPY_AND_ASSIGN(MutationLogEntryV2);

    static_assert(sizeof(MutationLogType) == sizeof(uint8_t),
                  "_type must be a uint8_t");
};

using MutationLogEntry = MutationLogEntryV2;

std::ostream& operator<<(std::ostream& out, const MutationLogEntryV2& mle);
