/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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

#include "queue_op.h"

#include <folly/lang/Assume.h>
#include <folly/lang/Bits.h>
#include <libcouchstore/couch_common.h>
#include <memcached/durability_spec.h>
#include <memcached/protocol_binary.h>
#include <memcached/types.h>
#include <platform/n_byte_integer.h>

#include <memory>
#include <type_traits>

// Bitwise masks for manipulating the flexCode variable inside MetaDataV1
const uint8_t flexCodeMask = 0x7F;
const uint8_t deleteSourceMask = 0x80;

// These classes are written to disk in couchstore, so we want to (a) have
// a stable binary layout and (b) minimise the space they take.
// Therefore turn on packing of structures. The resulting sizes are verified
// with static_assert()s below to ensure they are always fixed at the expected
// size.
#pragma pack(1)
class MetaData {
protected:
    /*
     * Declare the metadata formats in protected visibility.
     *
     * Each version generally extends the previous version, thus in memory you
     * have
     * [V0] or
     * [V0][V1]
     *
     * V3 is special - given that V2 is no longer used (contained fields which
     * were subsequently removed); it extends from V1 - i.e.
     *
     * [V0][V1][V3]
     */
    class MetaDataV0 {
    public:
        MetaDataV0() : cas(0), exptime(0), flags(0) {
        }

        void initialise(const char* raw) {
            std::memcpy(this, raw, sizeof(MetaDataV0));
            // Re-write only cas/exptime to get them in the correct byte-order
            cas = ntohll(cas);
            exptime = ntohl(exptime);
        }

        /*
         * When V0 is persisted, the cas/exptime are in network byte order.
         */
        void prepareForPersistence() {
            cas = htonll(cas);
            exptime = htonl(exptime);
        }

        uint64_t getCas() const {
            return cas;
        }

        void setCas(uint64_t cas) {
            this->cas = cas;
        }

        uint32_t getExptime() const {
            return exptime;
        }

        void setExptime(uint32_t exptime) {
            this->exptime = exptime;
        }

        uint32_t getFlags() const {
            return flags;
        }

        void setFlags(uint32_t flags) {
            this->flags = flags;
        }

        void copyToBuf(char* raw) const {
            // cas and exptime need converting to network byte-order
            uint64_t casNBO = htonll(cas);
            uint32_t exptimeNBO = htonl(exptime);
            std::memcpy(raw, &casNBO, sizeof(uint64_t));
            std::memcpy(raw + sizeof(uint64_t), &exptimeNBO, sizeof(uint32_t));
            std::memcpy(raw + sizeof(uint64_t) + sizeof(uint32_t),
                        &flags,
                        sizeof(uint32_t));
        }

    private:
        /*
         * V0 knows about CAS, expiry time and flags.
         */
        uint64_t cas;

        /**
         * For Alive documents, the time it should expire. For Deleted
         * documents, the time the document was deleted.
         * Expressed as seconds since unix epoch (time_t).
         */
        uint32_t exptime;
        uint32_t flags;
    };

    static_assert(sizeof(MetaDataV0) == 16,
                  "MetaDataV0 is not the expected size.");

    class MetaDataV1 {
    public:
        MetaDataV1() : flexCode(0), dataType(PROTOCOL_BINARY_RAW_BYTES) {
        }

        void initialise(const char* raw) {
            flexCode = raw[0];
            dataType = raw[1];

            if (getFlexCode() != FLEX_META_CODE) {
                std::invalid_argument(
                        "MetaDataV1::initialise illegal "
                        "flexCode \"" +
                        std::to_string(flexCode) + "\"");
            }
        }

        void setFlexCode() {
            setFlexCode(FLEX_META_CODE);
        }

        void setFlexCode(uint8_t code) {
            auto codeIn = code & flexCodeMask;
            flexCode = codeIn + (flexCode & deleteSourceMask);
        }

        uint8_t getFlexCode() const {
            return static_cast<uint8_t>(flexCode & flexCodeMask);
        }

        void setDataType(protocol_binary_datatype_t dataType) {
            this->dataType = static_cast<uint8_t>(dataType);
        }

        protocol_binary_datatype_t getDataType() const {
            return static_cast<protocol_binary_datatype_t>(dataType);
        }

        void copyToBuf(char* raw) const {
            raw[0] = flexCode;
            raw[1] = dataType;
        }

        void setDeleteSource(DeleteSource source) {
            auto deleteInt = (static_cast<uint8_t>(source)) << 7;
            flexCode = deleteInt + (flexCode & flexCodeMask);
        }

        DeleteSource getDeleteSource() const {
            auto deleteBit = flexCode >> 7;
            return static_cast<DeleteSource>(deleteBit);
        }

    private:
        /*
         * V1 is a 2 byte extension storing datatype
         *   0 - flexCode (which also holds deleteSource in bit 7)
         *   1 - dataType
         */
        uint8_t flexCode;
        uint8_t dataType;
    };

    static_assert(sizeof(MetaDataV1) == 2,
                  "MetaDataV1 is not the expected size.");

    class MetaDataV2 {
    public:
        MetaDataV2() = default;

        void initialise(const char* raw) {
            confResMode = raw[0];
        }

    private:
        /*
         * V2 is a 1 byte extension storing the conflict resolution mode.
         * This 1 byte extension is never stored out anymore, but may
         * exist from down-level ep-engine.
         */
        uint8_t confResMode = 0;
    };

    static_assert(sizeof(MetaDataV2) == 1,
                  "MetaDataV2 is not the expected size.");

    /*
     * V3 is a 2 byte[1] extension storing Synchronous Replication state.
     *
     * [1] It /could/ fit all required state into a single byte, however that
     * would mean it's the same size as V2 (1Byte) and we use metadata size to
     * distinguish between the different metadata versions. As such 2bytes are
     * used which logically wastes 1 byte per SyncWrite. If / when we
     * restructure MetaData to say use variable-length encoding with explicit
     * versions (a la flex framing extras) the size could be reduced.
     */
    class MetaDataV3 {
    public:
        // The Operation this represents - maps to queue_op types:
        enum class Operation : uint8_t {
            // A pending sync write. 'level' field defines the durability_level.
            // Present in the DurabilityPrepare namespace.
            Pending = 0,
            // A committed SyncWrite.
            // This exists so we can correctly backfill from disk a Committed
            // mutation and sent out as a DCP_COMMIT to sync_replication
            // enabled DCP clients.
            // Present in the 'normal' (committed) namespace.
            Commit = 1,
            // An aborted SyncWrite.
            // This exists so we can correctly backfill from disk an Aborted
            // mutation and sent out as a DCP_ABORT to sync_replication
            // enabled DCP clients.
            // Present in the DurabilityPrepare namespace.
            Abort = 2,
        };

        MetaDataV3() = default;

        void initialise(const char* raw) {
            operation = Operation(raw[0]);
            uint64_t buf;
            std::memcpy(&buf, &raw[1], sizeof(cb::uint48_t));
            details.raw = cb::uint48_t(buf).ntoh();
        };

        void copyToBuf(char* raw) const {
            raw[0] = char(operation);
            std::memcpy(&raw[1], &details.raw, sizeof(cb::uint48_t));
        }

        void setDurabilityOp(queue_op op) {
            switch (op) {
            case queue_op::pending_sync_write:
                operation = Operation::Pending;
                break;
            case queue_op::commit_sync_write:
                operation = Operation::Commit;
                break;
            case queue_op::abort_sync_write:
                operation = Operation::Abort;
                break;
            default:
                throw std::invalid_argument(
                        "MetaDataV3::setDurabilityOp: Unsupported op " +
                        to_string(op));
            }
        }

        queue_op getDurabilityOp() const {
            switch (operation) {
            case Operation::Pending:
                return queue_op::pending_sync_write;
            case Operation::Commit:
                return queue_op::commit_sync_write;
            case Operation::Abort:
                return queue_op::abort_sync_write;
            default:
                throw std::invalid_argument(
                        "MetaDataV3::getDurabiltyOp: Unsupported op " +
                        std::to_string(int(operation)));
            }
        }

        cb::durability::Level getDurabilityLevel() const {
            Expects(operation == Operation::Pending);
            return static_cast<cb::durability::Level>(details.pending.level);
        }

        void setDurabilityLevel(cb::durability::Level level_) {
            Expects(operation == Operation::Pending);
            details.pending.level = static_cast<char>(level_);
        }

        bool isPreparedDelete() const {
            Expects(operation == Operation::Pending);
            return details.pending.isDelete == 1;
        }

        void setPreparedDelete(bool isPreparedSyncDelete) {
            Expects(operation == Operation::Pending);
            details.pending.isDelete = isPreparedSyncDelete;
        }

        cb::uint48_t getPrepareSeqno() const {
            Expects(operation == Operation::Commit ||
                    operation == Operation::Abort);
            return details.completed.prepareSeqno;
        }

        void setPrepareSeqno(cb::uint48_t prepareSeqno) {
            Expects(operation == Operation::Commit ||
                    operation == Operation::Abort);
            details.completed.prepareSeqno = prepareSeqno;
        }

        void prepareForPersistence() {
            details.raw = details.raw.hton();
        }

        bool isCommit() const {
            return operation == Operation::Commit;
        }

        bool isPrepare() const {
            return operation == Operation::Pending;
        }

        bool isAbort() const {
            return operation == Operation::Abort;
        }

    private:
        // Assigning a whole byte to this (see MetaDataV3 class comment)
        // although only currently need 2 bits.
        Operation operation;

        // [[if Pending]] Properties of the pending SyncWrite.
        // Currently using 3 bits out of the available 8 in this byte.
        union detailsUnion {
            // Need to supply a default constructor or the compiler will
            // complain about cb::uint48_t
            detailsUnion() : raw(0){};
            struct {
                // 0:pendingSyncWrite, 1:pendingSyncDelete.
                uint8_t isDelete : 1;
                // cb::durability::Level
                uint8_t level : 2;
            } pending;
            struct completedDetails {
                // prepareSeqno of the completed Sync Write
                cb::uint48_t prepareSeqno;
            } completed;

            cb::uint48_t raw;
        } details;
    };

    static_assert(sizeof(MetaDataV3) == 7,
                  "MetaDataV3 is not the expected size.");

public:
    enum class Version {
        V0, // Cas/Exptime/Flags
        V1, // Flex code and datatype
        V2, // Conflict Resolution Mode - not stored, but can be read
        V3, // Synchronous Replication state.
        /*
         * !!MetaData Warning!!
         * Sherlock began storing the V2 MetaData.
         * Watson stops storing the V2 MetaData (now storing V1)
         *
         * Any new MetaData (e.g a V3) we wish to store may cause trouble if it
         * has the size of V2, code assumes the version from the size.
         */
    };

    MetaData() {
    }

    /*
     * Construct metadata from a sized_buf, the assumption is that the
     * data has come back from couchstore.
     */
    explicit MetaData(const sized_buf& in) : initVersion(Version::V0) {
        // Expect metadata to be V0, V1, V2 or V3
        // V2 part is ignored, but valid to find in storage.
        if (in.size < getMetaDataSize(Version::V0) ||
            in.size > getMetaDataSize(Version::V3)) {
            throw std::invalid_argument("MetaData::MetaData in.size \"" +
                                        std::to_string(in.size) +
                                        "\" is out of range.");
        }

        // Initialise at least the V0 metadata
        allMeta.v0.initialise(in.buf);

        // The rest depends on in.size
        if (in.size >= (sizeof(MetaDataV0) + sizeof(MetaDataV1))) {
            // The size extends enough to include V1 meta, initialise that.
            allMeta.v1.initialise(in.buf + sizeof(MetaDataV0));
            initVersion = Version::V1;
        }

        // Not initialising V2 from 'in' as V2 is ignored.

        if (in.size >=
            (sizeof(MetaDataV0) + sizeof(MetaDataV1) + sizeof(MetaDataV3))) {
            // The size extends enough to include V3 meta, initialise that.
            allMeta.v3.initialise(in.buf + sizeof(MetaDataV0) +
                                  sizeof(MetaDataV1));
            initVersion = Version::V3;
        }
    }

    /*
     * The reverse of MetaData(const sized_buf& in), copy the data out
     * to a pre-allocated sized_buf ready for passing to couchstore.
     */
    void copyToBuf(sized_buf& out) const {
        if ((out.size != getMetaDataSize(Version::V1) &&
             (out.size != getMetaDataSize(Version::V3)))) {
            throw std::invalid_argument(
                    "MetaData::copyToBuf out.size \"" +
                    std::to_string(out.size) +
                    "\" incorrect size (only V1 and V3 supported)");
        }
        // Copy the V0/V1 meta data holders to the output buffer
        allMeta.v0.copyToBuf(out.buf);
        allMeta.v1.copyToBuf(out.buf + sizeof(MetaDataV0));

        // We can write either V1 or V3 at present (V3 contains metadata which
        // is only applicable to SyncWrites, so we use V1 for non-SyncWrites
        // and V3 for SyncWrites.
        if (out.size == getMetaDataSize(Version::V3)) {
            allMeta.v3.copyToBuf(out.buf + sizeof(MetaDataV0) +
                                 sizeof(MetaDataV1));
        }
    }

    /*
     * Prepare the metadata for persistence (byte-swap certain fields) and
     * return a pointer to the metadata ready for persistence.
     */
    char* prepareAndGetForPersistence() {
        allMeta.v0.prepareForPersistence();
        allMeta.v3.prepareForPersistence();
        return reinterpret_cast<char*>(&allMeta);
    }

    void setCas(uint64_t cas) {
        allMeta.v0.setCas(cas);
    }

    uint64_t getCas() const {
        return allMeta.v0.getCas();
    }

    void setExptime(uint32_t exptime) {
        allMeta.v0.setExptime(exptime);
    }

    uint32_t getExptime() const {
        return allMeta.v0.getExptime();
    }

    void setFlags(uint32_t flags) {
        allMeta.v0.setFlags(flags); // flags are not byteswapped
    }

    uint32_t getFlags() const {
        return allMeta.v0.getFlags(); // flags are not byteswapped
    }

    void setFlexCode() {
        allMeta.v1.setFlexCode();
    }

    void setFlexCode(uint8_t code) {
        allMeta.v1.setFlexCode(code);
    }

    uint8_t getFlexCode() const {
        return allMeta.v1.getFlexCode();
    }

    void setDeleteSource(DeleteSource source) {
        allMeta.v1.setDeleteSource(source);
    }

    DeleteSource getDeleteSource() const {
        return allMeta.v1.getDeleteSource();
    }

    /*
     * Note that setting the data type will also set the flex code.
     */
    void setDataType(protocol_binary_datatype_t dataType) {
        setFlexCode();
        allMeta.v1.setDataType(dataType);
    }

    protocol_binary_datatype_t getDataType() const {
        return allMeta.v1.getDataType();
    }

    void setDurabilityOp(queue_op op) {
        allMeta.v3.setDurabilityOp(op);
    }

    queue_op getDurabilityOp() const {
        return allMeta.v3.getDurabilityOp();
    }

    void setPrepareProperties(cb::durability::Level level, bool isSyncDelete) {
        allMeta.v3.setDurabilityLevel(level);
        allMeta.v3.setPreparedDelete(isSyncDelete);
    }

    void setCompletedProperties(cb::uint48_t prepareSeqno) {
        allMeta.v3.setPrepareSeqno(prepareSeqno);
    }

    cb::durability::Level getDurabilityLevel() const {
        return allMeta.v3.getDurabilityLevel();
    }

    bool isPreparedSyncDelete() const {
        return allMeta.v3.isPreparedDelete();
    }

    bool isCommit() const {
        return getVersionInitialisedFrom() != MetaData::Version::V3 ||
               allMeta.v3.isCommit();
    }

    bool isPrepare() const {
        return getVersionInitialisedFrom() == MetaData::Version::V3 &&
               allMeta.v3.isPrepare();
    }

    bool isAbort() const {
        return getVersionInitialisedFrom() == MetaData::Version::V3 &&
               allMeta.v3.isAbort();
    }

    cb::uint48_t getPrepareSeqno() const {
        return allMeta.v3.getPrepareSeqno();
    }

    Version getVersionInitialisedFrom() const {
        return initVersion;
    }

    static size_t getMetaDataSize(Version version) {
        switch (version) {
        case Version::V0:
            return sizeof(MetaDataV0);
        case Version::V1:
            return sizeof(MetaDataV0) + sizeof(MetaDataV1);
        case Version::V2:
            return sizeof(MetaDataV0) + sizeof(MetaDataV1) + sizeof(MetaDataV2);
        case Version::V3:
            return sizeof(MetaDataV0) + sizeof(MetaDataV1) + sizeof(MetaDataV3);
        }
        folly::assume_unreachable();
    }

protected:
    class AllMetaData {
    public:
        MetaDataV0 v0;
        MetaDataV1 v1;
        // V2 is essentially a dead version, we no longer write it. Therefore
        // V3 (and upwards) do not include it, and instead just extend V1.
        MetaDataV3 v3;
    } allMeta;
    Version initVersion;
};
#pragma pack()

/*
 * Create the appropriate MetaData container.
 */
class MetaDataFactory {
public:
    static std::unique_ptr<MetaData> createMetaData(sized_buf metadata) {
        return std::unique_ptr<MetaData>(new MetaData(metadata));
    }

    static std::unique_ptr<MetaData> createMetaData() {
        return std::unique_ptr<MetaData>(new MetaData());
    }
};
