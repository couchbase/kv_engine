/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "ext_meta_parser.h"

#include <cstring>

ExtendedMetaData::ExtendedMetaData(const void *meta, uint16_t nmeta) {
    len = nmeta;
    data = static_cast<const char*>(meta);
    ret = cb::engine_errc::success;
    decodeMeta();
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
            while (bytes_left != 0 &&
                   ret != cb::engine_errc::invalid_arguments) {
                uint8_t type;
                uint16_t length;

                if (bytes_left < sizeof(type) + sizeof(length)) {
                    ret = cb::engine_errc::invalid_arguments;
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
                    ret = cb::engine_errc::invalid_arguments;
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
                        ret = cb::engine_errc::invalid_arguments;
                        break;
                }
                bytes_left -= length;
                offset += length;
            }
        } else {
            ret = cb::engine_errc::invalid_arguments;
        }
    } else {
        ret = cb::engine_errc::invalid_arguments;
    }
}
