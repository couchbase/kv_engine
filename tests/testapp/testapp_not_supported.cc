/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "testapp.h"

using cb::mcbp::ClientOpcode;

class NotSupportedTest : public TestappTest {
public:
    static void SetUpTestCase() {
        TestappTest::SetUpTestCase();
        createUserConnection = true;
    }
};

TEST_F(NotSupportedTest, VerifyNotSupported) {
    std::vector<ClientOpcode> opcodes = {
            {ClientOpcode::DcpFlush_Unsupported,
             ClientOpcode::Rget_Unsupported,
             ClientOpcode::Rset_Unsupported,
             ClientOpcode::Rsetq_Unsupported,
             ClientOpcode::Rappend_Unsupported,
             ClientOpcode::Rappendq_Unsupported,
             ClientOpcode::Rprepend_Unsupported,
             ClientOpcode::Rprependq_Unsupported,
             ClientOpcode::Rdelete_Unsupported,
             ClientOpcode::Rdeleteq_Unsupported,
             ClientOpcode::Rincr_Unsupported,
             ClientOpcode::Rincrq_Unsupported,
             ClientOpcode::Rdecr_Unsupported,
             ClientOpcode::Rdecrq_Unsupported,
             ClientOpcode::ResetReplicationChain_Unsupported,
             ClientOpcode::NotifyVbucketUpdate_Unsupported,
             ClientOpcode::SnapshotVbStates_Unsupported,
             ClientOpcode::VbucketBatchCount_Unsupported,
             ClientOpcode::ChangeVbFilter_Unsupported,
             ClientOpcode::SetDriftCounterState_Unsupported,
             ClientOpcode::GetAdjustedTime_Unsupported,
             ClientOpcode::TapConnect_Unsupported,
             ClientOpcode::TapMutation_Unsupported,
             ClientOpcode::TapDelete_Unsupported,
             ClientOpcode::TapFlush_Unsupported,
             ClientOpcode::TapOpaque_Unsupported,
             ClientOpcode::TapVbucketSet_Unsupported,
             ClientOpcode::TapCheckpointStart_Unsupported,
             ClientOpcode::TapCheckpointEnd_Unsupported,
             ClientOpcode::DeregisterTapClient_Unsupported}};

    for (const auto opcode : opcodes) {
        const auto rsp = userConnection->execute(BinprotGenericCommand(opcode));
        ASSERT_EQ(cb::mcbp::Status::NotSupported, rsp.getStatus())
                << "Unexpected response packet: "
                << rsp.getResponse().toJSON(false);
    }
}
