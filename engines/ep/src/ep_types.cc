/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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

#include "ep_types.h"

#include <boost/optional.hpp>
#include <folly/lang/Assume.h>
#include <ostream>

GenerateBySeqno getGenerateBySeqno(const OptionalSeqno& seqno) {
    return seqno ? GenerateBySeqno::No : GenerateBySeqno::Yes;
}

std::string to_string(GenerateBySeqno generateBySeqno) {
    using GenerateBySeqnoUType = std::underlying_type<GenerateBySeqno>::type;

    switch (generateBySeqno) {
    case GenerateBySeqno::Yes:
        return "Yes";
    case GenerateBySeqno::No:
        return "No";
    }
    throw std::invalid_argument(
            "to_string(GenerateBySeqno) unknown " +
            std::to_string(static_cast<GenerateBySeqnoUType>(generateBySeqno)));
}

std::string to_string(GenerateCas generateCas) {
    using GenerateByCasUType = std::underlying_type<GenerateCas>::type;

    switch (generateCas) {
    case GenerateCas::Yes:
        return "Yes";
    case GenerateCas::No:
        return "No";
    }
    throw std::invalid_argument(
            "to_string(GenerateCas) unknown " +
            std::to_string(static_cast<GenerateByCasUType>(generateCas)));
}

std::string to_string(TrackCasDrift trackCasDrift) {
    using TrackCasDriftUType = std::underlying_type<TrackCasDrift>::type;

    switch (trackCasDrift) {
    case TrackCasDrift::Yes:
        return "Yes";
    case TrackCasDrift::No:
        return "No";
    }
    throw std::invalid_argument(
            "to_string(TrackCasDrift) unknown " +
            std::to_string(static_cast<TrackCasDriftUType>(trackCasDrift)));
}

std::string to_string(CheckpointType checkpointType) {
    switch (checkpointType) {
    case CheckpointType::Disk:
        return "Disk";
    case CheckpointType::Memory:
        return "Memory";
    }
    folly::assume_unreachable();
}

std::string to_string(HighPriorityVBNotify hpNotifyType) {
    using HighPriorityVBNotifyUType =
            std::underlying_type<HighPriorityVBNotify>::type;

    switch (hpNotifyType) {
    case HighPriorityVBNotify::Seqno:
        return "seqno";
    case HighPriorityVBNotify::ChkPersistence:
        return "checkpoint persistence";
    }
    throw std::invalid_argument(
            "to_string(HighPriorityVBNotify) unknown " +
            std::to_string(
                    static_cast<HighPriorityVBNotifyUType>(hpNotifyType)));
}

std::ostream& operator<<(std::ostream& os, const EvictionPolicy& policy) {
    return os << to_string(policy);
}

std::string to_string(EvictionPolicy policy) {
    switch (policy) {
    case EvictionPolicy::Value:
        return "Value";
    case EvictionPolicy::Full:
        return "Full";
    }
    folly::assume_unreachable();
}

std::ostream& operator<<(std::ostream& os, TransferVB transfer) {
    switch (transfer) {
    case TransferVB::No:
        return os << "No";
    case TransferVB::Yes:
        return os << "Yes";
    }
    throw std::invalid_argument("operator<<(TransferVB) unknown value " +
                                std::to_string(static_cast<int>(transfer)));
}

std::ostream& operator<<(std::ostream& os, const snapshot_range_t& range) {
    return os << "{" << range.getStart() << "," << range.getEnd() << "}";
}

std::ostream& operator<<(std::ostream& os, const snapshot_info_t& info) {
    return os << "start:" << info.start << ", range:" << info.range;
}
