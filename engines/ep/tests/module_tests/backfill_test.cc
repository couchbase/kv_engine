/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "dcp/backfill.h"

#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>
#include <memory>

using ::testing::_;
using ::testing::InSequence;
using ::testing::Return;
using ::testing::StrictMock;

class MockDCPBackfill : public DCPBackfill {
public:
    MOCK_METHOD0(create, backfill_status_t());
    MOCK_METHOD0(scan, backfill_status_t());
    MOCK_METHOD0(scanHistory, backfill_status_t());
    MOCK_METHOD1(getNextScanState, State(State));
    MOCK_CONST_METHOD0(getVBucketId, Vbid());
    MOCK_CONST_METHOD0(shouldCancel, bool());
};

TEST(BackfillTest, CreateOnlyMode) {
    auto backfill = std::make_unique<StrictMock<MockDCPBackfill>>();
    backfill->setCreateMode(DCPBackfillCreateMode::CreateOnly);

    InSequence s;
    EXPECT_CALL(*backfill, create()).WillOnce(Return(backfill_success));
    EXPECT_CALL(*backfill, getNextScanState(DCPBackfill::State::Create))
            .WillOnce(Return(DCPBackfill::State::Scan));

    backfill->run();
}

TEST(BackfillTest, CreateAndScanMode) {
    auto backfill = std::make_unique<StrictMock<MockDCPBackfill>>();
    backfill->setCreateMode(DCPBackfillCreateMode::CreateAndScan);

    InSequence s;
    EXPECT_CALL(*backfill, create()).WillOnce(Return(backfill_success));
    EXPECT_CALL(*backfill, getNextScanState(DCPBackfill::State::Create))
            .WillOnce(Return(DCPBackfill::State::Scan));
    EXPECT_CALL(*backfill, scan()).WillOnce(Return(backfill_success));

    backfill->run();
}
