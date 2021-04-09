/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <folly/portability/GTest.h>

#include <mcbp/protocol/framebuilder.h>
#include <memcached/durability_spec.h>

using namespace cb::mcbp;
using namespace cb::durability;

/**
 * helper method to encode a frame info value
 */
static std::vector<uint8_t> encodeFrameInfo(request::FrameInfoId id,
                                            cb::const_byte_buffer payload) {
    std::vector<uint8_t> result;

    auto idbits = static_cast<uint16_t>(id);
    if (idbits < 0x0f) {
        result.emplace_back(uint8_t(idbits << 4u));
    } else {
        result.emplace_back(0xf0);
        result.emplace_back(uint8_t(idbits - 0x0f));
    }

    if (payload.size() < 0x0f) {
        result[0] |= uint8_t(payload.size());
    } else {
        result[0] |= 0x0fu;
        result.emplace_back(uint8_t(payload.size() - 0x0f));
    }

    std::copy(payload.begin(), payload.end(), std::back_inserter(result));
    return result;
}

TEST(Request_ParseFrameExtras, ExtendedIdAndSize) {
    uint8_t blob[37];
    auto fe = encodeFrameInfo(request::FrameInfoId(32), {blob, sizeof(blob)});
    std::vector<uint8_t> packet(1024);
    RequestBuilder builder({packet.data(), packet.size()});
    builder.setMagic(Magic::AltClientRequest);
    builder.setFramingExtras({fe.data(), fe.size()});

    auto* req = reinterpret_cast<Request*>(packet.data());
    bool found = false;
    req->parseFrameExtras([&found](request::FrameInfoId id,
                                   cb::const_byte_buffer data) -> bool {
        if (id != request::FrameInfoId(32)) {
            ADD_FAILURE() << "Expected ID to be 32";
        }
        if (data.size() != 37) {
            ADD_FAILURE() << "Expected payload to be 37 bytes";
        }
        found = true;
        return true;
    });
    EXPECT_TRUE(found);
}

/// When we hit 15 for frame id or size we need to add an extra byte
/// containing the additional value, but for the value of 15 this extra
/// byte is 0. Create a unit test to validate that
TEST(Request_ParseFrameExtras, ExtendedIdAndSize_Edge) {
    uint8_t blob[15] = {};
    auto fe = encodeFrameInfo(request::FrameInfoId(15), {blob, sizeof(blob)});
    std::vector<uint8_t> expected(18);
    expected[0] = 0xff;
    EXPECT_EQ(expected, fe);

    // Verify that we can properly decode it
    std::vector<uint8_t> packet(1024);
    RequestBuilder builder({packet.data(), packet.size()});
    builder.setMagic(Magic::AltClientRequest);
    builder.setFramingExtras({fe.data(), fe.size()});

    auto* req = reinterpret_cast<Request*>(packet.data());
    bool found = false;
    req->parseFrameExtras([&found](request::FrameInfoId id,
                                   cb::const_byte_buffer data) -> bool {
        if (id != request::FrameInfoId(15)) {
            ADD_FAILURE() << "Expected ID to be 15";
        }
        if (data.size() != 15) {
            ADD_FAILURE() << "Expected payload to be 15 bytes";
        }
        found = true;
        return true;
    });
    EXPECT_TRUE(found);
}

TEST(Request_ParseFrameExtras, Barrier_LegalPacket) {
    std::vector<uint8_t> fe;
    fe.push_back(0x00); // ID 0, length 0
    std::vector<uint8_t> packet(27);
    RequestBuilder builder({packet.data(), packet.size()});
    builder.setMagic(Magic::AltClientRequest);
    builder.setFramingExtras({fe.data(), fe.size()});

    auto* req = reinterpret_cast<Request*>(packet.data());
    bool found = false;
    req->parseFrameExtras([&found](request::FrameInfoId id,
                                   cb::const_byte_buffer data) -> bool {
        if (id != request::FrameInfoId::Barrier) {
            ADD_FAILURE() << "Expected ID to be Barrier";
        }
        if (!data.empty()) {
            ADD_FAILURE() << "Barrier should not have any payload";
        }
        found = true;
        return true;
    });
    EXPECT_TRUE(found);
}

TEST(Request_ParseFrameExtras, Barrier_BufferOverflow) {
    std::vector<uint8_t> fe;
    fe.push_back(0x02); // ID 0, length 2
    fe.push_back(0x00); // Add the 0 byte (1 byte too little)
    std::vector<uint8_t> packet(27);
    RequestBuilder builder({packet.data(), packet.size()});
    builder.setMagic(Magic::AltClientRequest);
    builder.setFramingExtras({fe.data(), fe.size()});

    try {
        auto* req = reinterpret_cast<Request*>(packet.data());
        req->parseFrameExtras([](request::FrameInfoId id,
                                 cb::const_byte_buffer data) -> bool {
            ADD_FAILURE() << "Expected parser to fail. Called with "
                          << to_string(id);
            return true;
        });
        FAIL() << "Parser should detect invalid length";
    } catch (const std::overflow_error& e) {
        EXPECT_STREQ("parseFrameExtras: outside frame extras", e.what());
    }
}

TEST(Request_ParseFrameExtras, DurabilityRequirement_LegalPacket) {
    std::vector<uint8_t> fe;
    fe.push_back(0x11); // ID 1, length 1
    fe.push_back(0x00); // level 0
    std::vector<uint8_t> packet(30);
    RequestBuilder builder({packet.data(), packet.size()});
    builder.setMagic(Magic::AltClientRequest);
    builder.setFramingExtras({fe.data(), fe.size()});

    auto* req = reinterpret_cast<Request*>(packet.data());
    bool found = false;
    req->parseFrameExtras([&found](request::FrameInfoId id,
                                   cb::const_byte_buffer data) -> bool {
        if (id != request::FrameInfoId::DurabilityRequirement) {
            ADD_FAILURE() << "Expected ID to be DurabilityRequirement";
        }
        if (data.size() != 1) {
            ADD_FAILURE() << "DurabilityRequirement needs 1 byte of level";
        }
        found = true;
        return true;
    });
    EXPECT_TRUE(found);

    std::fill(packet.begin(), packet.end(), 0);
    builder.setMagic(Magic::AltClientRequest);
    fe.resize(4); // 1 byte magic, 3 bytes value
    fe[0] = 0x13;
    builder.setFramingExtras({fe.data(), fe.size()});
    found = false;
    req->parseFrameExtras([&found](request::FrameInfoId id,
                                   cb::const_byte_buffer data) -> bool {
        if (id != request::FrameInfoId::DurabilityRequirement) {
            ADD_FAILURE() << "Expected ID to be DurabilityRequirement";
        }
        if (data.size() != 3) {
            ADD_FAILURE() << "DurabilityRequirement needs 3 byte of level";
        }
        found = true;
        return true;
    });
    EXPECT_TRUE(found);
}

TEST(Request_ParseFrameExtras, MultipleEncoding) {
    std::vector<uint8_t> fe(5);
    fe[0] = 0x13; // Durability Requirement with 3 bytes
    fe[1] = 0xaa;
    fe[2] = 0xbb;
    fe[3] = 0xcc;
    fe[4] = 0x00; // Barrier
    std::vector<uint8_t> packet(30);
    RequestBuilder builder({packet.data(), packet.size()});
    builder.setMagic(Magic::AltClientRequest);
    builder.setFramingExtras({fe.data(), fe.size()});

    auto* req = reinterpret_cast<Request*>(packet.data());
    bool durability_found = false;
    bool barrier_found = false;
    req->parseFrameExtras([&durability_found, &barrier_found](
                                  request::FrameInfoId id,
                                  cb::const_byte_buffer data) -> bool {
        if (id == request::FrameInfoId::Barrier) {
            barrier_found = true;
        }

        if (id == request::FrameInfoId::DurabilityRequirement) {
            durability_found = true;
            if (data.size() != 3) {
                ADD_FAILURE() << "Invalid data size";
                return false;
            }
            if (data[0] != 0xaa || data[1] != 0xbb || data[2] != 0xcc) {
                ADD_FAILURE() << "Invalid data content";
                return false;
            }
        }
        return true;
    });

    EXPECT_TRUE(durability_found);
    EXPECT_TRUE(barrier_found);
}

TEST(Request_GetDurationSpec, NoSpecPresent) {
    std::vector<uint8_t> packet(sizeof(Request));
    RequestBuilder builder({packet.data(), packet.size()});
    builder.setMagic(Magic::ClientRequest);
    auto& req = *builder.getFrame();
    auto dur = req.getDurabilityRequirements();
    EXPECT_FALSE(dur);
}

TEST(Request_GetDurationSpec, OnlyRequirement) {
    std::vector<uint8_t> fe;
    fe.push_back(0x11);
    fe.push_back(0x01);
    std::vector<uint8_t> packet(sizeof(Request) + fe.size());
    RequestBuilder builder({packet.data(), packet.size()});
    builder.setMagic(Magic::AltClientRequest);
    builder.setFramingExtras({fe.data(), fe.size()});
    auto& req = *builder.getFrame();
    auto dur = req.getDurabilityRequirements();
    EXPECT_TRUE(dur);
    EXPECT_EQ(Level::Majority, dur->getLevel());
    EXPECT_TRUE(dur->getTimeout().isDefault());
}

TEST(Request_GetDurationSpec, FullSpecPresent) {
    std::vector<uint8_t> fe;
    fe.push_back(0x13);
    fe.push_back(0x03);
    fe.push_back(0xaa);
    fe.push_back(0xbb);
    std::vector<uint8_t> packet(sizeof(Request) + fe.size());
    RequestBuilder builder({packet.data(), packet.size()});
    builder.setMagic(Magic::AltClientRequest);
    builder.setFramingExtras({fe.data(), fe.size()});
    auto& req = *builder.getFrame();
    auto dur = req.getDurabilityRequirements();
    EXPECT_TRUE(dur);
    EXPECT_EQ(Level::PersistToMajority, dur->getLevel());
    EXPECT_EQ(0xaabb, dur->getTimeout().get());
}

/// Test that reserved timeout value 0x0 raises the correct error.
TEST(Request_GetDurationSpec, InvalidTimeoutZero) {
    std::vector<uint8_t> fe;
    // ID:1, Len:3 packed into first byte:
    fe.push_back(0x13);
    // Level:Majority (0x1)
    fe.push_back(0x03);
    // Timeout:0x0000 (invalid)
    fe.push_back(0x00);
    fe.push_back(0x00);
    std::vector<uint8_t> packet(sizeof(Request) + fe.size());
    RequestBuilder builder({packet.data(), packet.size()});
    builder.setMagic(Magic::AltClientRequest);
    builder.setFramingExtras({fe.data(), fe.size()});
    EXPECT_THROW(builder.getFrame()->getDurabilityRequirements(),
                 std::invalid_argument);
}

/// Test that reserved timeout value 0xffff raises the correct error.
TEST(Request_GetDurationSpec, InvalidTimeoutFFFF) {
    std::vector<uint8_t> fe;
    // ID:1, Len:3 packed into first byte:
    fe.push_back(0x13);
    // Level:Majority (0x1)
    fe.push_back(0x03);
    // Timeout:0xffff (invalid)
    fe.push_back(0xff);
    fe.push_back(0xff);
    std::vector<uint8_t> packet(sizeof(Request) + fe.size());
    RequestBuilder builder({packet.data(), packet.size()});
    builder.setMagic(Magic::AltClientRequest);
    builder.setFramingExtras({fe.data(), fe.size()});
    EXPECT_THROW(builder.getFrame()->getDurabilityRequirements(),
                 std::invalid_argument);
}
