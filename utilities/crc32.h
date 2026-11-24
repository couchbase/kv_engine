/*
 *     Copyright 2011-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Compute the standard CRC-32 (IEEE 802.3 polynomial 0x04C11DB7, reflected
 * 0xEDB88320, as used by zlib) checksum of the provided buffer.
 *
 * Note that this is NOT CRC-32C (Castagnoli). The result is used to map
 * document keys to vBuckets, so the output must never change.
 *
 * @param buf the data to checksum
 * @param len the number of bytes in buf
 * @return the CRC-32 checksum of the data
 */
uint32_t crc32buf(const uint8_t* buf, size_t len);

#ifdef __cplusplus
}
#endif
