/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "serialised_dockey.h"
#include "utility.h"

#include <gsl/gsl-lite.hpp>
#include <memcached/vbucket.h>
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

/**
 * An entry in the MutationLog.
 * This is the V3 layout which stores leb encoded collectionID
 *
 * Stored by mad-hatter
 *
 */
class MutationLogEntry {
public:
    MutationLogEntry(const MutationLogEntry&) = delete;
    const MutationLogEntry& operator=(const MutationLogEntry&) = delete;

    static const uint8_t MagicMarker = 0x47;

    /**
     * Initialize a new entry inside the given buffer.
     *
     * @param r the rowid
     * @param t the type of log entry
     * @param vb the vbucket
     * @param k the key
     */
    static MutationLogEntry* newEntry(uint8_t* buf,
                                      MutationLogType t,
                                      Vbid vb,
                                      const DocKeyView& k) {
        return new (buf) MutationLogEntry(t, vb, k);
    }

    static MutationLogEntry* newEntry(uint8_t* buf,
                                      MutationLogType t,
                                      Vbid vb) {
        if (MutationLogType::Commit1 != t && MutationLogType::Commit2 != t) {
            throw std::invalid_argument(
                    "MutationLogEntry::newEntry: invalid type");
        }
        return new (buf) MutationLogEntry(t, vb);
    }

    /**
     * Initialize a new entry using the contents of the given buffer.
     *
     * @param buf a chunk of memory thought to contain a valid
     *        MutationLogEntry
     * @param buflen the length of said buf
     */
    static const MutationLogEntry* newEntry(
            std::vector<uint8_t>::const_iterator itr, size_t buflen) {
        if (buflen < len(0)) {
            throw std::invalid_argument(
                    "MutationLogEntry::newEntry: buflen "
                    "(which is " +
                    std::to_string(buflen) +
                    ") is less than minimum required (which is " +
                    std::to_string(len(0)) + ")");
        }

        const auto* me = reinterpret_cast<const MutationLogEntry*>(&(*itr));

        if (me->magic != MagicMarker) {
            throw std::invalid_argument(
                    "MutationLogEntry::newEntry: "
                    "magic (which is " +
                    std::to_string(me->magic) + ") is not equal to " +
                    std::to_string(MagicMarker));
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

    // Statically buffered.  There is no delete.
    void operator delete(void*) = delete;

    /**
     * The size of a MutationLogEntry, in bytes, containing a key of
     * the specified length.
     */
    static size_t len(size_t klen) {
        // The exact empty record size as will be packed into the layout
        // One byte of the key overlaps the end of this struct (_key.bytes).
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
     * This entry's vbucket.
     */
    Vbid vbucket() const {
        return _vbucket.ntoh();
    }

    /**
     * The type of this log entry.
     */
    MutationLogType type() const {
        return _type;
    }

private:
    friend std::ostream& operator<<(std::ostream& out,
                                    const MutationLogEntry& e);

    MutationLogEntry(MutationLogType t, Vbid vb, const DocKeyView& k)
        : _vbucket(vb.hton()), magic(MagicMarker), _type(t), _key(k) {
        // Shut up the warning about the unused member
        (void)pad;
        // Assert that _key is the final member
        static_assert(
                offsetof(MutationLogEntry, _key) ==
                        (sizeof(MutationLogEntry) - sizeof(SerialisedDocKey)),
                "_key must be the final member of MutationLogEntry");
    }

    MutationLogEntry(MutationLogType t, Vbid vb)
        : MutationLogEntry(t, vb, {nullptr, 0, DocKeyEncodesCollectionId::No}) {
    }

    const Vbid _vbucket;
    const uint8_t magic;
    const MutationLogType _type;
    const std::array<uint8_t, 2> pad =
            {}; // padding to ensure _key is the final member
    const SerialisedDocKey _key;

    static_assert(sizeof(MutationLogType) == sizeof(uint8_t),
                  "_type must be a uint8_t");
};

std::ostream& operator<<(std::ostream& out, const MutationLogEntry& mle);
