/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include <folly/portability/GTest.h>
#include <xattr/utils.h>

/**
 * The CAS written into documents by ${Mutation.CAS} and the cas_offsets of
 * MutateWithMeta is decoded by clients as the CAS bytes in little endian
 * order. The string must therefore be the same on every platform
 * (independent of the host byte order). Use constant values so that the
 * test fails if the encoding depends on the host byte order.
 */
TEST(XattrMacros, CasToHexIsPlatformIndependent) {
    EXPECT_EQ("0x0000e551056be718",
              cb::xattr::macros::cas_to_hex(0x18e76b0551e50000ULL));
    EXPECT_EQ("0x0807060504030201",
              cb::xattr::macros::cas_to_hex(0x0102030405060708ULL));
    EXPECT_EQ("0x0000000000000000", cb::xattr::macros::cas_to_hex(0));
    EXPECT_EQ("0xffffffffffffffff",
              cb::xattr::macros::cas_to_hex(0xffffffffffffffffULL));
}
