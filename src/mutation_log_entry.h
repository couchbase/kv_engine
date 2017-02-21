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

enum mutation_log_type_t {
    ML_NEW = 0,
    /* removed: ML_DEL = 1 */
    /* removed: ML_DEL_ALL = 2 */
    ML_COMMIT1 = 3,
    ML_COMMIT2 = 4
};

const uint8_t MUTATION_LOG_MAGIC(0x45);

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
    static MutationLogEntry* newEntry(uint8_t* buf,
                                      uint64_t r,
                                      mutation_log_type_t t,
                                      uint16_t vb,
                                      const DocKey& k) {
        return new (buf) MutationLogEntry(r, t, vb, k);
    }

    static MutationLogEntry* newEntry(uint8_t* buf,
                                      uint64_t r,
                                      mutation_log_type_t t,
                                      uint16_t vb) {
        if (t != ML_COMMIT1 && t != ML_COMMIT2) {
            throw std::invalid_argument(
                    "MutationLogEntry::newEntry: invalid type");
        }
        return new (buf) MutationLogEntry(r, t, vb);
    }

    /**
     * Initialize a new entry using the contents of the given buffer.
     *
     * @param buf a chunk of memory thought to contain a valid MutationLogEntry
     * @param buflen the length of said buf
     */
    static MutationLogEntry* newEntry(uint8_t* buf, size_t buflen) {
        if (buflen < len(0)) {
            throw std::invalid_argument(
                    "MutationLogEntry::newEntry: buflen "
                    "(which is " +
                    std::to_string(buflen) +
                    ") is less than minimum required (which is " +
                    std::to_string(len(0)) + ")");
        }

        MutationLogEntry* me = reinterpret_cast<MutationLogEntry*>(buf);

        if (me->magic != MUTATION_LOG_MAGIC) {
            throw std::invalid_argument(
                    "MutationLogEntry::newEntry: "
                    "magic (which is " +
                    std::to_string(me->magic) + ") is not equal to " +
                    std::to_string(MUTATION_LOG_MAGIC));
        }
        if (me->len() > buflen) {
            throw std::invalid_argument(
                    "MutationLogEntry::newEntry: "
                    "entry length (which is " +
                    std::to_string(me->len()) +
                    ") is greater than available buflen (which is " +
                    std::to_string(buflen) + ")");
        }
        return me;
    }

    void operator delete(void*) {
        // Statically buffered.  There is no delete.
    }

    /**
     * The size of a MutationLogEntry, in bytes, containing a key of
     * the specified length.
     */
    static size_t len(size_t klen) {
        // the exact empty record size as will be packed into the layout
        return sizeof(MutationLogEntry) + (klen - 1);
    }

    /**
     * The number of bytes of the serialized form of this
     * MutationLogEntry.
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
    friend std::ostream& operator<<(std::ostream& out,
                                    const MutationLogEntry& e);

    MutationLogEntry(uint64_t r,
                     mutation_log_type_t t,
                     uint16_t vb,
                     const DocKey& k)
        : _rowid(htonll(r)),
          _vbucket(htons(vb)),
          magic(MUTATION_LOG_MAGIC),
          _type(static_cast<uint8_t>(t)),
          pad(0),
          _key(k) {
        (void)pad;
        // Assert that _key is the final member
        static_assert(
                offsetof(MutationLogEntry, _key) ==
                        (sizeof(MutationLogEntry) - sizeof(SerialisedDocKey)),
                "_key must be the final member of MutationLogEntry");
    }

    MutationLogEntry(uint64_t r, mutation_log_type_t t, uint16_t vb)
        : MutationLogEntry(
                  r, t, vb, {nullptr, 0, DocNamespace::DefaultCollection}) {
    }

    uint64_t _rowid;
    uint16_t _vbucket;
    uint8_t magic;
    uint8_t _type;
    uint8_t pad; // explicit padding to ensure _key is the final member
    SerialisedDocKey _key;

    DISALLOW_COPY_AND_ASSIGN(MutationLogEntry);
};

std::ostream& operator<<(std::ostream& out, const MutationLogEntry& mle);
