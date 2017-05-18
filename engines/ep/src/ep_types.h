/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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

#pragma once
#include <stdexcept>
#include <string>
#include <type_traits>

#include <boost/optional/optional.hpp>

// Forward declarations for types defined elsewhere.
class Item;

template <class T>
class SingleThreadedRCPtr;
using queued_item = SingleThreadedRCPtr<Item>;

enum class GenerateBySeqno {
    No, Yes
};

typedef std::underlying_type<GenerateBySeqno>::type GenerateBySeqnoUType;

static inline std::string to_string(const GenerateBySeqno generateBySeqno) {
    switch (generateBySeqno) {
        case GenerateBySeqno::Yes:
            return "Yes";
        case GenerateBySeqno::No:
            return "No";
        default:
            throw std::invalid_argument("to_string(GenerateBySeqno) unknown " +
                    std::to_string(static_cast<GenerateBySeqnoUType>(generateBySeqno)));
            return "";
    }
}

enum class GenerateCas {
    No, Yes
};

typedef std::underlying_type<GenerateCas>::type GenerateByCasUType;

static inline std::string to_string(GenerateCas generateCas) {
    switch (generateCas) {
        case GenerateCas::Yes:
            return "Yes";
        case GenerateCas::No:
            return "No";
        default:
            throw std::invalid_argument("to_string(GenerateCas) unknown " +
                    std::to_string(static_cast<GenerateByCasUType>(generateCas)));
            return "";
    }
}

enum class TrackCasDrift { No, Yes };

typedef std::underlying_type<TrackCasDrift>::type TrackCasDriftUType;

static inline std::string to_string(TrackCasDrift trackCasDrift) {
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

enum class WantsDeleted { No, Yes };
enum class TrackReference { No, Yes };
enum class QueueExpired { No, Yes };

/**
 * The following options can be specified
 * for retrieving an item for get calls
 */
enum get_options_t {
    NONE = 0x0000, // no option
    TRACK_STATISTICS = 0x0001, // whether statistics need to be tracked or not
    QUEUE_BG_FETCH = 0x0002, // whether a background fetch needs to be queued
    HONOR_STATES = 0x0004, // whether a retrieval should depend on the state
    // of the vbucket
    TRACK_REFERENCE = 0x0008, // whether NRU bit needs to be set for the item
    DELETE_TEMP = 0x0010, // whether temporary items need to be deleted
    HIDE_LOCKED_CAS = 0x0020, // whether locked items should have their CAS
    // hidden (return -1).
    GET_DELETED_VALUE = 0x0040, // whether to retrieve value of a deleted item
    ALLOW_META_ONLY = 0x0080 // Allow only the meta to be returned for an item
};

/* Meta data versions for GET_META */
enum class GetMetaVersion : uint8_t {
    V1 = 1, // returns deleted, flags, expiry and seqno
    V2 = 2, // The 'spock' version returns V1 + the datatype
};

/// Allow for methods to optionally accept a seqno
using OptionalSeqno = boost::optional<int64_t>;

/**
 * Captures the result of a rollback request.
 * Contains if the rollback was successful, highSeqno of the vBucket after
 * rollback, and the last snaspshot range in the vb after rollback.
 */
class RollbackResult {
public:
    RollbackResult(bool success,
                   uint64_t highSeqno,
                   uint64_t snapStartSeqno,
                   uint64_t snapEndSeqno)
        : success(success),
          highSeqno(highSeqno),
          snapStartSeqno(snapStartSeqno),
          snapEndSeqno(snapEndSeqno) {
    }

    bool success;
    uint64_t highSeqno;
    uint64_t snapStartSeqno;
    uint64_t snapEndSeqno;
};

/**
 * Indicates the type of the HighPriorityVB request causing the notify
 */
enum class HighPriorityVBNotify { Seqno, ChkPersistence };

/**
 * Overloads the to_string method to give the string format of a member of the
 * class HighPriorityVBNotify
 */
std::string to_string(HighPriorityVBNotify hpNotifyType);

/**
 * Result of a HighPriorityVB request
 */
enum class HighPriorityVBReqStatus {
    NotSupported,
    RequestScheduled,
    RequestNotScheduled
};
