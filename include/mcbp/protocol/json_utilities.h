/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "include/memcached/protocol_binary.h"
#include "platform/string_hex.h"
#include <nlohmann/json.hpp>

namespace cb::mcbp {
void to_json(nlohmann::json& json, const DcpOpenFlag& flags);
void to_json(nlohmann::json& json, const DcpAddStreamFlag& flags);
}

namespace cb::mcbp::request {

void to_json(nlohmann::json& json, const MutationPayload& payload);
void to_json(nlohmann::json& json, const ArithmeticPayload& payload);
void to_json(nlohmann::json& json, const SetClusterConfigPayload& payload);
void to_json(nlohmann::json& json, const VerbosityPayload& payload);
void to_json(nlohmann::json& json, const TouchPayload& payload);
void to_json(nlohmann::json& json, const SetCtrlTokenPayload& payload);
void to_json(nlohmann::json& json,
             const SetBucketDataLimitExceededPayload& payload);
void to_json(nlohmann::json& json, const DcpOpenPayload& payload);
void to_json(nlohmann::json& json, const DcpAddStreamPayload& payload);
void to_json(nlohmann::json& json, const DcpStreamReqPayload& payload);
void to_json(nlohmann::json& json, const DcpStreamEndPayload& payload);
void to_json(nlohmann::json& json, const DcpSnapshotMarkerFlag& flags);
void to_json(nlohmann::json& json, const DcpSnapshotMarkerV1Payload& payload);
void to_json(nlohmann::json& json, const DcpSnapshotMarkerV2xPayload& payload);
void to_json(nlohmann::json& json, const DcpSnapshotMarkerV2_0Value& payload);
void to_json(nlohmann::json& json, const DcpSnapshotMarkerV2_2Value& payload);
void to_json(nlohmann::json& json, const DcpMutationPayload& payload);
void to_json(nlohmann::json& json, const DcpDeletionV1Payload& payload);
void to_json(nlohmann::json& json, const DcpDeleteRequestV1& payload);
void to_json(nlohmann::json& json, const DcpDeletionV2Payload& payload);
void to_json(nlohmann::json& json, const DcpDeleteRequestV2& payload);
void to_json(nlohmann::json& json, const DcpExpirationPayload& payload);
void to_json(nlohmann::json& json, const DcpSetVBucketState& payload);
void to_json(nlohmann::json& json, const DcpBufferAckPayload& payload);
void to_json(nlohmann::json& json, const DcpOsoSnapshotPayload& payload);
void to_json(nlohmann::json& json, const DcpSeqnoAdvancedPayload& payload);
void to_json(nlohmann::json& json, const DcpSystemEventPayload& payload);
void to_json(nlohmann::json& json, const DcpPreparePayload& payload);
void to_json(nlohmann::json& json, const DcpSeqnoAcknowledgedPayload& payload);
void to_json(nlohmann::json& json, const DcpCommitPayload& payload);
void to_json(nlohmann::json& json, const DcpAbortPayload& payload);
void to_json(nlohmann::json& json, const SetParamPayload& payload);
void to_json(nlohmann::json& json, const SetWithMetaPayload& payload);
void to_json(nlohmann::json& json, const DelWithMetaPayload& payload);
void to_json(nlohmann::json& json, const ReturnMetaPayload& payload);
void to_json(nlohmann::json& json, const CompactDbPayload& payload);
void to_json(nlohmann::json& json, const AdjustTimePayload& payload);
void to_json(nlohmann::json& json, const EWB_Payload& payload);
void to_json(nlohmann::json& json, const GetErrmapPayload& payload);
void to_json(nlohmann::json& json, const GetCollectionIDPayload& payload);
void to_json(nlohmann::json& json, const GetScopeIDPayload& payload);
void to_json(nlohmann::json& json, const GetRandomKeyPayload& payload);
void to_json(nlohmann::json& json, const RangeScanContinuePayload& payload);

} // namespace cb::mcbp::request

namespace cb::mcbp::response {
void to_json(nlohmann::json& json,
             const RangeScanContinueMetaResponse& payload);
void to_json(nlohmann::json& json, const DcpAddStreamPayload& payload);
} // namespace cb::mcbp::response

namespace cb::mcbp::subdoc {
void to_json(nlohmann::json& josn, const PathFlag& flag);
void to_json(nlohmann::json& josn, const DocFlag& flag);
}
