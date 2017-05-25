/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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

#include "ext_meta_parser.h"

#include <cstring>

ExtendedMetaData::ExtendedMetaData(const void *meta, uint16_t nmeta) {
    len = nmeta;
    data = static_cast<const char*>(meta);
    ret = ENGINE_SUCCESS;
    memoryAllocated = false;
    decodeMeta();
}

ExtendedMetaData::~ExtendedMetaData() {
    if (memoryAllocated) {
        delete[] data;
    }
}

void ExtendedMetaData::decodeMeta() {
    /**
     * Structure of extended meta data:
     * | Ver (1B) | Type (1B) | Length (2B) | Field1 | ...
     *        ... | Type (1B) | Length (2B) | Field2 | ...
     */
    uint16_t offset = 0,bytes_left = len;

    if (bytes_left > 0) {
        uint8_t version;
        memcpy(&version, data, sizeof(version));
        if (version == META_EXT_VERSION_ONE) {
            bytes_left -= sizeof(version);
            offset += sizeof(version);
            while (bytes_left != 0 && ret != ENGINE_EINVAL) {
                uint8_t type;
                uint16_t length;

                if (bytes_left < sizeof(type) + sizeof(length)) {
                    ret = ENGINE_EINVAL;
                    break;
                }
                memcpy(&type, data + offset, sizeof(type));
                bytes_left -= sizeof(type);
                offset += sizeof(type);
                memcpy(&length, data + offset, sizeof(length));
                length = ntohs(length);
                bytes_left -= sizeof(length);
                offset += sizeof(length);
                if (bytes_left < length) {
                    ret = ENGINE_EINVAL;
                    break;
                }
                switch (type) {
                    case CMD_META_ADJUSTED_TIME:
                        // Ignoring adjusted_time
                    case CMD_META_CONFLICT_RES_MODE:
                        // MB-21143: Now ignoring conflict_res_mode
                        // 4.6 no longer sends, but older versions
                        // may send it to us.
                        break;
                    default:
                        ret = ENGINE_EINVAL;
                        break;
                }
                bytes_left -= length;
                offset += length;
            }
        } else {
            ret = ENGINE_EINVAL;
        }
    } else {
        ret = ENGINE_EINVAL;
    }
}
