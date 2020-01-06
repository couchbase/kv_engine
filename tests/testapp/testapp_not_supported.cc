/*
 *     Copyright 2020 Couchbase, Inc.
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

#include "testapp.h"

using cb::mcbp::ClientOpcode;

class NotSupportedTest : public TestappTest {};

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
        auto rsp = getConnection().execute(BinprotGenericCommand(opcode));
        ASSERT_EQ(cb::mcbp::Status::NotSupported, rsp.getStatus())
                << "Unexpected response packet: "
                << rsp.getResponse().toJSON(false);
    }
}
