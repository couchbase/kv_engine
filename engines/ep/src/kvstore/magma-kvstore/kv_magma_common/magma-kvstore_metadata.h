/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <memcached/types.h>
#include <platform/n_byte_integer.h>

#include <nlohmann/json_fwd.hpp>
#include <chrono>
#include <cstring>

namespace magmakv {
// MetaData is used to serialize and de-serialize metadata respectively when
// writing a Document mutation request to Magma and when reading a Document
// from Magma.

// The `#pragma pack(1)` directive and the order of members are to keep
// the size of MetaData as small as possible and uniform across different
// platforms.
#pragma pack(1)
class MetaData {
public:
    enum class Version : uint8_t {
        V0,
        V1, // extends V0 and adds durability data
        V2 // history defined (inside v0 storage) and optionally V1
    };

    MetaData() = default;

    explicit MetaData(std::string_view buf);

    Version getVersion() const {
        return version.version;
    }

    void setVersion(Version v) {
        version.version = v;
    }

    uint8_t getDurabilityLevel() const;

    void setDurabilityDetailsForPrepare(bool isDelete, uint8_t level);

    cb::uint48_t getPrepareSeqno() const;

    void setDurabilityDetailsForAbort(cb::uint48_t prepareSeqno);

    uint32_t getExptime() const {
        return allMeta.v0.exptime;
    }

    void setExptime(uint32_t exptime) {
        allMeta.v0.exptime = exptime;
    }

    void setDeleted(bool deleted, DeleteSource deleteSource);

    int64_t getBySeqno() const {
        return allMeta.v0.bySeqno;
    }

    void setBySeqno(cb::uint48_t bySeqno) {
        allMeta.v0.bySeqno = bySeqno;
    }

    uint32_t getValueSize() const {
        return allMeta.v0.valueSize;
    }

    void setValueSize(uint32_t valueSize) {
        allMeta.v0.valueSize = valueSize;
    }

    bool isDeleted() const;

    uint8_t getDatatype() const {
        return allMeta.v0.datatype;
    }

    void setDataType(uint8_t datatype) {
        allMeta.v0.datatype = datatype;
    }

    uint32_t getFlags() const {
        return allMeta.v0.flags;
    }

    void setFlags(uint32_t flags) {
        allMeta.v0.flags = flags;
    }

    uint64_t getCas() const {
        return allMeta.v0.cas;
    }

    void setCas(uint64_t cas) {
        allMeta.v0.cas = cas;
    }

    cb::uint48_t getRevSeqno() const {
        return allMeta.v0.revSeqno;
    }

    void setRevSeqno(cb::uint48_t revSeqno) {
        allMeta.v0.revSeqno = revSeqno;
    }

    DeleteSource getDeleteSource() const;

    bool isSyncDelete() const;

    // @return the current history timestamp (which is the CAS)
    std::chrono::seconds getHistoryTimeStamp() const {
        using namespace std::chrono;
        return duration_cast<seconds>(nanoseconds(getCas()));
    }

    /**
     * Set if this update of the Item is (value=true) or is not (value=false)
     * retained in the KVStore history
     */
    void setHistory(bool value);

    /// @return true if this item should be retained in the KVStore history
    bool isHistoryEnabled() const;

    bool isHistoryDefined() const {
        return getVersion() == Version::V2;
    }

    bool isDurabilityDefined() const {
        return getVersion() == Version::V1 ||
               (getVersion() == Version::V2 && allMeta.v2.containsV1Meta);
    }

    std::string to_string() const;

    /**
     * Encode the metadata into a std::string. The encoding leb128 encodes
     * various fields that may not always be populated or may generally have
     * low values.
     *
     * @return encoded metadata
     */
    std::string encode() const;

    bool operator==(const MetaData& other) const;

protected:
    /**
     * Version field, stored by all metadata types so that we can work out which
     * version it is to decode correctly
     */
    class VersionStorage {
    public:
        VersionStorage() = default;

        static std::pair<VersionStorage, std::string_view> decode(
                std::string_view buf);

        // See magmakv::MetaData::encode() documentation
        std::string encode() const;

        Version version = Version::V0;
    };
    static_assert(sizeof(Version) == 1,
                  "magmakv::Version is not the expected size.");

    /**
     * Standard metadata used for commit namespace items
     */
    class MetaDataV0 {
    public:
        MetaDataV0() = default;

        /**
         * Decode the given buffer and initialise everything except V0Bits
         * The decoded bits is returned so the caller can initialise the correct
         * v0 or v2 bits member.
         *
         * @return pair where first is the uint8 bits field and second is the
         *         remaining buffer.
         */
        std::pair<uint8_t, std::string_view> decode(std::string_view buf);

        /// encode v0 metadata with the provided bits field
        std::string encode(uint8_t bits) const;

        bool operator==(const MetaDataV0& other) const;

        cb::uint48_t bySeqno = 0;
        uint64_t cas = 0;
        uint32_t valueSize = 0;
        uint8_t datatype = 0;

        union V0Bits {
            V0Bits() = default;
            struct {
                uint8_t deleted : 1;
                uint8_t deleteSource : 1; // 0 Explicit, 1 TTL
            } bits;
            uint8_t all{0};
        } bits;

        // The leb128 encoded fields get added at the end of the encoded meta
        cb::uint48_t revSeqno = 0;
        uint32_t exptime = 0;
        uint32_t flags = 0;
    };

    static_assert(sizeof(MetaDataV0) == 34,
                  "magmakv::MetaDataV0 is not the expected size.");

    /**
     * Metadata extension used for Prepare and Abort items. Whilst we could
     * save space by using a 1 byte extension for prepares, and a 6 byte
     * extension for aborts, we don't expect prepares to live in the system for
     * long (as they're purge by compaction when older than the PCS) and this
     * matches what we do for couchstore buckets.
     */
    class MetaDataV1 {
    public:
        MetaDataV1() = default;

        bool operator==(const MetaDataV1& other) const;

        /**
         * Decode the given buffer and initialise all member variables
         *
         * @return a buffer with any remaining data
         */
        std::string_view decode(std::string_view buf);

        // See magmakv::MetaData::encode() documentation
        std::string encode() const;

        union durabilityDetails {
            // Need to supply a default constructor or the compiler will
            // complain about cb::uint48_t
            durabilityDetails() : raw(0){};

            struct {
                // 0:pendingSyncWrite, 1:pendingSyncDelete
                uint8_t isDelete : 1;
                // cb::durability::level
                uint8_t level : 2;
            } pending;

            struct completedDetails {
                // prepareSeqno of the completed Syncwrite
                cb::uint48_t prepareSeqno;
            } completed;

            cb::uint48_t raw;
        } durabilityDetails;
    };

    static_assert(sizeof(MetaDataV1) == 6,
                  "magmakv::MetaDataV1 is not the expected size.");

    class MetaDataV2 {
    public:
        MetaDataV2() = default;

        bool operator==(const MetaDataV2& other) const;

        // clone of V0 bits + a history bit
        union V2Bits {
            V2Bits() = default;
            struct {
                uint8_t deleted : 1;
                uint8_t deleteSource : 1; // 0 Explicit, 1 TTL
                uint8_t history : 1;
            } bits;
            uint8_t all{0};
        } bits;

        // true if the V1 (durability) data is available
        bool containsV1Meta{false};

        // See magmakv::MetaData::encode() documentation
        std::string encode(MetaDataV1 v1) const;
    };

    VersionStorage version;
    class AllMetaData {
    public:
        MetaDataV0 v0;
        // Contains info for prepare namespace items
        MetaDataV1 v1;
        // Extends to store an extra bit
        MetaDataV2 v2;
    } allMeta;
};
#pragma pack()

// Note that MetaData (and sizeof) doesn't impact stored data, this is an
// structure held in memory for encode/decode purposes.
static_assert(sizeof(MetaData) == 43,
              "magmakv::MetaData is not the expected size.");

void to_json(nlohmann::json& json, const MetaData& meta);

} // namespace magmakv
