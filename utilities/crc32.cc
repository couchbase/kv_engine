/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "crc32.h"

#include <folly/hash/Checksum.h>

uint32_t crc32buf(const uint8_t* buf, size_t len) {
    // folly::crc32_type() computes the standard (IEEE 802.3 / zlib) CRC-32
    // with an initial value of ~0 and a final inversion, which is exactly
    // what the previous table driven implementation computed. It selects a
    // hardware accelerated implementation at runtime when available (the
    // ARMv8 CRC32 instructions, or PCLMULQDQ folding on x86-64) and falls
    // back to a portable software implementation otherwise.
    return folly::crc32_type(buf, len);
}
