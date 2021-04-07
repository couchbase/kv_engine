/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

#include "storeddockey.h"
#include "utility.h"

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
 * Used only for backwards compatibility when reading MutationLogEntryV2 from
 * disk. SerialisedDocKey has changed layout, so this type is used to allow
 * reading of access logs written by spock/vulcan/alice.
 *
 * This should be removed if MutationLogEntryV2 is ever removed.
 *
 * See MB-38327.
 */
class SpockSerialisedDocKey : public DocKeyInterface<SpockSerialisedDocKey> {
public:
    /**
     * The copy/move constructor/assignment deleted due to the bytes living
     * outside of the object.
     */
    SpockSerialisedDocKey(const SpockSerialisedDocKey& obj) = delete;
    SpockSerialisedDocKey& operator=(const SpockSerialisedDocKey& obj) = delete;

    SpockSerialisedDocKey(SpockSerialisedDocKey&& obj) = delete;
    SpockSerialisedDocKey& operator=(SpockSerialisedDocKey&& obj) = delete;

    const uint8_t* data() const {
        return bytes;
    }

    size_t size() const {
        return length;
    }

    DocKeyEncodesCollectionId getEncoding() const {
        return DocKeyEncodesCollectionId::No;
    }

    explicit operator DocKey() const {
        return {bytes, length, DocKeyEncodesCollectionId::No};
    }

protected:
    /**
     * This type should _only_ be used by MutationLogEntryV2 to maintain
     * compatibility with mutation logs written by older versions.
     */
    friend class MutationLogEntryV2;

    SpockSerialisedDocKey() : length(0), docNamespace(0), bytes() {
    }

    /**
     * Create a LegacySerialisedDocKey from a byte_buffer that has no collection
     * data and requires the caller to state the collection-ID This is used by
     * MutationLogEntryV1 -> V2 upgrade
     */
    explicit SpockSerialisedDocKey(cb::const_byte_buffer key)
        : length(gsl::narrow_cast<uint8_t>(key.size())),
          docNamespace(0),
          bytes() {
        // Assertions to protect the layout of MutationLogEntryV2 as
        // compatibility must be maintained with entries written by older
        // versions (entries are read from disk and reinterpret_cast).
        static_assert(offsetof(SpockSerialisedDocKey, length) == 0,
                      "length must be be at offset 0");
        static_assert(offsetof(SpockSerialisedDocKey, bytes) == 2,
                      "bytes must be be at offset 2");
        std::copy(key.begin(), key.end(), reinterpret_cast<char*>(bytes));
    }

    uint8_t length{0};
    // V2 entries always set the docNamespace member to 0.
    // Upgrading to V3 ignores this value, so docNamespace is currently
    // unused. This member is present to maintain compatibility with entries
    // written by spock/vulcan/alice. Entries are read from disk then
    // reinterpret_cast, so the struct layout is crucial.
    uint8_t docNamespace;
    uint8_t bytes[1];
};

static_assert(std::is_standard_layout<SpockSerialisedDocKey>::value,
              "LegacySeralisedDocKey: must satisfy is_standard_layout");

class MutationLogEntryV2;
class MutationLogEntryV3;

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

    // Statically buffered.  There is no delete.
    void operator delete(void*) = delete;

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
    Vbid vbucket() const {
        return _vbucket.ntoh();
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
                       Vbid vb,
                       const std::string& k)
        : _rowid(htonll(r)),
          _vbucket(htons(vb.get())),
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

    const uint64_t _rowid;
    const Vbid _vbucket;
    const uint8_t magic;
    const MutationLogType _type;
    const uint8_t keylen;
    char _key[1];

    DISALLOW_COPY_AND_ASSIGN(MutationLogEntryV1);
};

/**
 * An entry in the MutationLog.
 * This is the V2 layout which stores document namespaces and removes the rowid
 * (sequence number) as it was unused.
 *
 *  V2 was persisted by spock/vulcan/alice
 *
 */
class MutationLogEntryV2 {
public:
    static const uint8_t MagicMarker = 0x46;

    /**
     * Construct a V2 from V1, this places the key into the default collection
     * No constructor delegation, copy the values raw (so no byte swaps occur)
     * key is initialised into the default collection
     */
    explicit MutationLogEntryV2(const MutationLogEntryV1& mleV1)
        : _vbucket(mleV1._vbucket),
          magic(MagicMarker),
          _type(mleV1._type),
          _key({reinterpret_cast<const uint8_t*>(mleV1._key), mleV1.keylen}) {
        (void)pad;
        static_assert(offsetof(MutationLogEntryV2, _key) ==
                              (sizeof(MutationLogEntryV2) -
                               sizeof(SpockSerialisedDocKey)),
                      "_key must be the final member of MutationLogEntryV2");
        static_assert(sizeof(_key) == 3,
                      "_key struct must be 3 bytes in size to maintain"
                      "MutationLogEntryV2 layout");
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

    // Statically buffered.  There is no delete.
    void operator delete(void*) = delete;

    /**
     * The size of a MutationLogEntryV2, in bytes, containing a key of
     * the specified length.
     */
    static size_t len(size_t klen) {
        // The exact empty record size as will be packed into the layout.
        // one byte of the (non namespace prefixed) key overlaps the end of
        // this struct; the first byte of the key byte array (_key.bytes).
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
    const SpockSerialisedDocKey& key() const {
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
    friend MutationLogEntryV3;

    friend std::ostream& operator<<(std::ostream& out,
                                    const MutationLogEntryV2& e);

    const Vbid _vbucket;
    const uint8_t magic;
    const MutationLogType _type;
    const uint8_t pad{}; // padding to ensure _key is the final member
    // MB-38327: SerialisedDocKey used to contain an explicit namespace
    // byte. This was since removed, and is now treated as a prefix of
    // the key itself.
    // BUT, to correctly read V2 entries from disk, the semantics of
    // the length field and the offset of the length field and of the
    // start of the key bytes _must_ be retained. SpockSerialisedDocKey
    // retains the previous layout. See the MB for details.
    const SpockSerialisedDocKey _key;

    DISALLOW_COPY_AND_ASSIGN(MutationLogEntryV2);

    static_assert(sizeof(MutationLogType) == sizeof(uint8_t),
                  "_type must be a uint8_t");
};

/**
 * An entry in the MutationLog.
 * This is the V3 layout which stores leb encoded collectionID
 *
 * Stored by mad-hatter
 *
 */
class MutationLogEntryV3 {
public:
    static const uint8_t MagicMarker = 0x47;

    /**
     * Construct a V3 from V2.
     * V2 stored a 1 byte namespace (which was the value of 0) but it is not
     * treated as a prefix of the key (i.e., key.size() does not include it).
     * For cleanliness, discard mleV2._key.docNamespace and re-encode the key
     * with the DefaultCollection prefix.
     */
    explicit MutationLogEntryV3(const MutationLogEntryV2& mleV2)
        : _vbucket(mleV2._vbucket),
          magic(MagicMarker),
          _type(mleV2._type),
          _key({mleV2._key.data(), mleV2._key.size()}, CollectionID::Default) {
        (void)pad;
    }

    /**
     * Initialize a new entry inside the given buffer.
     *
     * @param r the rowid
     * @param t the type of log entry
     * @param vb the vbucket
     * @param k the key
     */
    static MutationLogEntryV3* newEntry(uint8_t* buf,
                                        MutationLogType t,
                                        Vbid vb,
                                        const DocKey& k) {
        return new (buf) MutationLogEntryV3(t, vb, k);
    }

    static MutationLogEntryV3* newEntry(uint8_t* buf,
                                        MutationLogType t,
                                        Vbid vb) {
        if (MutationLogType::Commit1 != t && MutationLogType::Commit2 != t) {
            throw std::invalid_argument(
                    "MutationLogEntryV3::newEntry: invalid type");
        }
        return new (buf) MutationLogEntryV3(t, vb);
    }

    /**
     * Initialize a new entry using the contents of the given buffer.
     *
     * @param buf a chunk of memory thought to contain a valid
     *        MutationLogEntryV3
     * @param buflen the length of said buf
     */
    static const MutationLogEntryV3* newEntry(
            std::vector<uint8_t>::const_iterator itr, size_t buflen) {
        if (buflen < len(0)) {
            throw std::invalid_argument(
                    "MutationLogEntryV3::newEntry: buflen "
                    "(which is " +
                    std::to_string(buflen) +
                    ") is less than minimum required (which is " +
                    std::to_string(len(0)) + ")");
        }

        const auto* me = reinterpret_cast<const MutationLogEntryV3*>(&(*itr));

        if (me->magic != MagicMarker) {
            throw std::invalid_argument(
                    "MutationLogEntryV3::newEntry: "
                    "magic (which is " +
                    std::to_string(me->magic) + ") is not equal to " +
                    std::to_string(MagicMarker));
        }
        if (me->len() > buflen) {
            throw std::invalid_argument(
                    "MutationLogEntryV3::newEntry: "
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
     * The size of a MutationLogEntryV3, in bytes, containing a key of
     * the specified length.
     */
    static size_t len(size_t klen) {
        // The exact empty record size as will be packed into the layout
        // One byte of the key overlaps the end of this struct (_key.bytes).
        return sizeof(MutationLogEntryV3) + (klen - 1);
    }

    /**
     * The size of a MutationLogEntryV3, in bytes, needed to contain the key
     * from a MutationLogEntryV2.
     */
    static size_t len(const MutationLogEntryV2& mlev2) {
        // the constructor of MLEV3 adds a one-byte namespace prefix to the key.
        // MLEV2 does have a docNamespace field, but it is not included in the
        // key length. The V2->V3 upgrade is the transition point, changing the
        // semantics of the key length.
        auto keylen = mlev2._key.size() + 1;
        // The exact empty record size as will be packed into the layout
        // One byte of the key overlaps the end of this struct (_key.bytes).
        return len(keylen);
    }

    /**
     * The number of bytes of the serialized form of this
     * MutationLogEntryV3.
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
                                    const MutationLogEntryV3& e);

    MutationLogEntryV3(MutationLogType t, Vbid vb, const DocKey& k)
        : _vbucket(vb.hton()), magic(MagicMarker), _type(t), _key(k) {
        // Assert that _key is the final member
        static_assert(
                offsetof(MutationLogEntryV3, _key) ==
                        (sizeof(MutationLogEntryV3) - sizeof(SerialisedDocKey)),
                "_key must be the final member of MutationLogEntryV3");
    }

    MutationLogEntryV3(MutationLogType t, Vbid vb)
        : MutationLogEntryV3(
                  t, vb, {nullptr, 0, DocKeyEncodesCollectionId::No}) {
    }

    const Vbid _vbucket;
    const uint8_t magic;
    const MutationLogType _type;
    const uint8_t pad[2] = {}; // padding to ensure _key is the final member
    const SerialisedDocKey _key;

    DISALLOW_COPY_AND_ASSIGN(MutationLogEntryV3);

    static_assert(sizeof(MutationLogType) == sizeof(uint8_t),
                  "_type must be a uint8_t");
};

/// V4 use the same on disk format as V3, except that it use the HW enabled
/// CRC calculation in libplatform
using MutationLogEntryV4 = MutationLogEntryV3;
using MutationLogEntry = MutationLogEntryV4;

std::ostream& operator<<(std::ostream& out, const MutationLogEntryV1& mle);
std::ostream& operator<<(std::ostream& out, const MutationLogEntryV2& mle);
std::ostream& operator<<(std::ostream& out, const MutationLogEntryV3& mle);
