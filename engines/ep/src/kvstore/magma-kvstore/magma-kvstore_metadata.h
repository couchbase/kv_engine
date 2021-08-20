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

#include "item.h"

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
    enum class Version : uint8_t { V0, V1 };

    MetaData() = default;

    explicit MetaData(const Item& it);

    explicit MetaData(std::string_view buf) {
        // Read version first, that should always exist
        std::tie(version, buf) = VersionStorage::parse(buf);

        switch (getVersion()) {
        case Version::V0:
            std::tie(allMeta.v0, buf) = MetaDataV0::parse(buf);
            break;
        case Version::V1:
            std::tie(allMeta.v0, buf) = MetaDataV0::parse(buf);
            std::tie(allMeta.v1, buf) = MetaDataV1::parse(buf);
            break;
        }

        Expects(buf.empty());
    }

    cb::durability::Level getDurabilityLevel() const;

    cb::uint48_t getPrepareSeqno() const {
        Expects(getVersion() == Version::V1);
        return allMeta.v1.durabilityDetails.completed.prepareSeqno;
    }

    Version getVersion() const {
        return version.version;
    }

    Vbid getVbid() const {
        return Vbid(allMeta.v0.vbid);
    }

    void setVbid(Vbid vbid) {
        allMeta.v0.vbid = vbid.get();
    }

    uint32_t getExptime() const {
        return allMeta.v0.exptime;
    }

    void setExptime(uint32_t exptime) {
        allMeta.v0.exptime = exptime;
    }

    void setDeleted(bool deleted) {
        allMeta.v0.deleted = deleted;
    }

    int64_t getBySeqno() const {
        return allMeta.v0.bySeqno;
    }

    uint32_t getValueSize() const {
        return allMeta.v0.valueSize;
    }

    bool isDeleted() const {
        return allMeta.v0.deleted;
    }

    uint8_t getDatatype() const {
        return allMeta.v0.datatype;
    }

    uint32_t getFlags() const {
        return allMeta.v0.flags;
    }

    uint64_t getCas() const {
        return allMeta.v0.cas;
    }

    cb::uint48_t getRevSeqno() const {
        return allMeta.v0.revSeqno;
    }

    DeleteSource getDeleteSource() const {
        return static_cast<DeleteSource>(allMeta.v0.deleteSource);
    }

    bool isSyncDelete() const {
        Expects(getVersion() == Version::V1);
        return allMeta.v1.durabilityDetails.pending.isDelete;
    }

    std::string to_string() const;

    size_t getLength() const {
        auto versionSize = sizeof(VersionStorage);

        switch (getVersion()) {
        case Version::V0:
            return versionSize + sizeof(MetaDataV0);
        case Version::V1:
            return versionSize + sizeof(MetaDataV0) + sizeof(MetaDataV1);
        }

        folly::assume_unreachable();
    }

protected:
    /**
     * Version field, stored by all metadata types so that we can work out which
     * version it is to decode correctly
     */
    class VersionStorage {
    public:
        VersionStorage() = default;

        static std::pair<VersionStorage, std::string_view> parse(
                std::string_view buf) {
            Expects(buf.size() >= sizeof(VersionStorage));

            VersionStorage v;
            std::memcpy(&v, buf.data(), sizeof(VersionStorage));

            buf.remove_prefix(sizeof(VersionStorage));
            return {v, buf};
        }

        Version version = Version::V0;
    };
    static_assert(sizeof(Version) == 1,
                  "magmakv::Version is not the expected size.");

    /**
     * Standard metadata used for commit namespace items
     */
    class MetaDataV0 {
    public:
        MetaDataV0() : deleted(0), deleteSource(0) {
        }

        static std::pair<MetaDataV0, std::string_view> parse(
                std::string_view buf) {
            Expects(buf.size() >= sizeof(MetaDataV0));

            MetaDataV0 v;
            std::memcpy(&v, buf.data(), sizeof(MetaDataV0));

            buf.remove_prefix(sizeof(MetaDataV0));
            return {v, buf};
        }

        cb::uint48_t bySeqno = 0;
        uint64_t cas = 0;
        cb::uint48_t revSeqno = 0;
        uint32_t exptime = 0;
        uint32_t flags = 0;
        uint32_t valueSize = 0;
        uint16_t vbid = 0;
        uint8_t datatype = 0;
        uint8_t deleted : 1;
        uint8_t deleteSource : 1;
    };

    static_assert(sizeof(MetaDataV0) == 36,
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

        static std::pair<MetaDataV1, std::string_view> parse(
                std::string_view buf) {
            Expects(buf.size() >= sizeof(MetaDataV1));

            MetaDataV1 v;
            std::memcpy(&v, buf.data(), sizeof(MetaDataV1));

            buf.remove_prefix(sizeof(MetaDataV1));
            return {v, buf};
        }

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

    VersionStorage version;
    class AllMetaData {
    public:
        MetaDataV0 v0;
        // Contains info for prepare namespace items
        MetaDataV1 v1;
    } allMeta;
};
#pragma pack()

static_assert(sizeof(MetaData) == 43,
              "magmakv::MetaData is not the expected size.");
} // namespace magmakv
