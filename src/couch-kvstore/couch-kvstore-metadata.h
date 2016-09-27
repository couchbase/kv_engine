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

#include <memory>
#include <type_traits>

#include "item.h"
#include <memcached/protocol_binary.h>

class MetaData {
protected:
    /*
     * Declare the metadata formats in protected visibility.
     *
     * Each version extends the previous version, thus in memory you have
     * [V0] or
     * [V0][V1] or
     * [V0][V1][V2]
     */
    class MetaDataV0 {
    public:
        MetaDataV0()
            : cas(0),
              exptime(0),
              flags(0) {}

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
            std::memcpy(raw + sizeof(uint64_t),
                        &exptimeNBO, sizeof(uint32_t));
            std::memcpy(raw + sizeof(uint64_t) + sizeof(uint32_t),
                        &flags, sizeof(uint32_t));
        }

     private:
        /*
         * V0 knows about CAS, expiry time and flags.
         */
#pragma pack(1)
        uint64_t cas;
        uint32_t exptime;
        uint32_t flags;
#pragma pack()
    };

static_assert(sizeof(MetaDataV0) == 16,
              "MetaDataV0 is not the expected size.");

    class MetaDataV1 {
    public:
        MetaDataV1()
            : flexCode(0),
              dataType(PROTOCOL_BINARY_RAW_BYTES) {}

        void initialise(const char* raw) {
            flexCode = raw[0];
            dataType = raw[1];

            if (flexCode != FLEX_META_CODE) {
                std::invalid_argument("MetaDataV1::initialise illegal "
                                      "flexCode \"" + std::to_string(flexCode) +
                                      "\"");
            }
        }

        void setFlexCode() {
            setFlexCode(FLEX_META_CODE);
        }

        void setFlexCode(uint8_t code) {
            flexCode = code;
        }

        uint8_t getFlexCode() const {
            return flexCode;
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

    private:
        /*
         * V1 is a 2 byte extension storing datatype
         *   0 - flexCode
         *   1 - dataType
         */
        uint8_t flexCode;
        uint8_t dataType;
    };

static_assert(sizeof(MetaDataV1) == 2,
              "MetaDataV1 is not the expected size.");

    class MetaDataV2 {
    public:
        MetaDataV2()
            : confResMode(revision_seqno) // equivalent to zero
              {}

        void initialise(const char* raw) {
            confResMode = raw[0];
        }

        void setConfResMode(conflict_resolution_mode mode) {
            confResMode = static_cast<uint8_t>(mode);
        }

        conflict_resolution_mode getConfResMode() const {
            return static_cast<conflict_resolution_mode>(confResMode);
        }

        void copyToBuf(char* raw) const {
            raw[0] = confResMode;
        }

    private:
        /*
         * V2 is a 1 byte extension storing the conflict resolution mode.
         */
        uint8_t confResMode;
    };

static_assert(sizeof(MetaDataV2) == 1,
              "MetaDataV2 is not the expected size.");

public:

    enum class Version {
        V0, // Cas/Exptime/Flags
        V1, // Flex code and datatype
        V2  // Conflict Resolution Mode
    };

    MetaData () {}

    /*
     * Construct metadata from a sized_buf, the assumption is that the
     * data has come back from couchstore.
     */
    MetaData(const sized_buf& in)
        : initVersion(Version::V0) {

        if (in.size < getMetaDataSize(Version::V0)
            || in.size > getMetaDataSize(Version::V2)) {
            throw std::invalid_argument("MetaData::MetaData in.size \"" +
                                        std::to_string(in.size) +
                                        "\" is out of range.");
        }

        // Initialise at least the V0 metadata
        allMeta.v0.initialise(in.buf);

        // The rest depends on in.size
        if (in.size >=
            (sizeof(MetaDataV0) + sizeof(MetaDataV1))) {
            // The size extends enough to include V1 meta, initialise that.
            allMeta.v1.initialise(in.buf + sizeof(MetaDataV0));
            initVersion = Version::V1;
        }

        if (in.size ==
            (sizeof(MetaDataV0) + sizeof(MetaDataV1) + sizeof(MetaDataV2))) {
            // The size extends enough to include V2 meta, overlay that.
            allMeta.v2.initialise(in.buf + sizeof(MetaDataV0) +
                                  sizeof(MetaDataV1));
            initVersion = Version::V2;
        }
    }

    /*
     * The reverse of MetaData(const sized_buf& in), copy the data out
     * to a pre-allocated sized_buf ready for passing to couchstore.
     */
    void copyToBuf(sized_buf& out) const {
        if (out.size != getMetaDataSize(Version::V2)) {
            throw std::invalid_argument("MetaData::copyToBuf out.size \"" +
                                        std::to_string(out.size) +
                                        "\" incorrect size.");
        }
        // Copy the 3 meta data holders to the output buffer
        allMeta.v0.copyToBuf(out.buf);
        allMeta.v1.copyToBuf(out.buf + sizeof(MetaDataV0));
        allMeta.v2.copyToBuf(out.buf + sizeof(MetaDataV0) + sizeof(MetaDataV1));
    }

    /*
     * Prepare the metadata for persistence (byte-swap certain fields) and
     * return a pointer to the metadata ready for persistence.
     * This will change the value obtained by getCas/getExptime only.
     */
    char* prepareAndGetForPersistence() {
        allMeta.v0.prepareForPersistence();
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

    conflict_resolution_mode getConfResMode() const {
        return allMeta.v2.getConfResMode();
    }

    void setConfResMode(conflict_resolution_mode mode) {
        allMeta.v2.setConfResMode(mode);
    }

    Version getVersionInitialisedFrom() const {
        return initVersion;
    }

    static size_t getMetaDataSize(Version version) {
        switch (version) {
            case Version::V0:
                return sizeof(MetaDataV0);
            case Version::V1:
                return sizeof(MetaDataV0) +
                       sizeof(MetaDataV1);
            case Version::V2:
                return sizeof(MetaDataV0) +
                       sizeof(MetaDataV1) +
                       sizeof(MetaDataV2);
        }

        return sizeof(MetaDataV0) + sizeof(MetaDataV1) + sizeof(MetaDataV2);
    }

protected:
    class AllMetaData {
    public:
#pragma pack(1)
        MetaDataV0 v0;
        MetaDataV1 v1;
        MetaDataV2 v2;
#pragma pack()
    } allMeta;
    Version initVersion;
};

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
