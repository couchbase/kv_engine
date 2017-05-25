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

#ifndef SRC_EXT_META_PARSER_H_
#define SRC_EXT_META_PARSER_H_ 1

#include "config.h"

#include <memcached/types.h>

#include <utility>

/**
 * Version for extras-detail in setWithMeta/delWithMeta and
 * DCP mutation/expiration
 */
enum cmd_meta_extras_version {
    /* Extras format: | type:1B | len:2B | field1 | type | len | field2 | ...
     */
    META_EXT_VERSION_ONE = 0x01
};

/**
 * Definition of extras-types for setWithMeta, delWithMeta
 * commands and DCP mutation/expiration messages
 */
enum cmd_meta_extras_type {
    /* adjusted time */
    CMD_META_ADJUSTED_TIME     = 0x01,
    /* conflict resolution mode is no longer sent, but could be received on upgrade.*/
    CMD_META_CONFLICT_RES_MODE = 0x02
};

/**
 * This class will be used to parse the extended meta data section
 * in setWithMeta/delWithMeta commands and DCP mutation/deletion
 * messages.
 */
class ExtendedMetaData {
public:
    ExtendedMetaData()
          : data(nullptr),
            ret(ENGINE_SUCCESS),
            len(0),
            memoryAllocated(false) {}

    ExtendedMetaData(const void *meta, uint16_t nmeta);
    ~ExtendedMetaData();

    ENGINE_ERROR_CODE getStatus() {
        return ret;
    }

    std::pair<const char*, uint16_t> getExtMeta() {
        return std::make_pair(data, len);
    }

private:
    /*
    void encodeMeta(); is currently removed as there's no extmeta to encode.
    Resurrect from history as required.
    */
    void decodeMeta();

    const char* data;
    ENGINE_ERROR_CODE ret;
    uint16_t len;
    bool memoryAllocated;
};

#endif  // SRC_EXT_META_PARSER_H_
