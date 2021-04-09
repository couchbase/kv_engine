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
#pragma once

#include <memcached/engine_error.h>
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
    ExtendedMetaData() : data(nullptr), ret(cb::engine_errc::success), len(0) {
    }

    ExtendedMetaData(const void *meta, uint16_t nmeta);

    cb::engine_errc getStatus() {
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
    cb::engine_errc ret;
    uint16_t len;
};
