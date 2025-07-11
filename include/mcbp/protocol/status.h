/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <cstdint>
#include <iosfwd>
#include <string>
#include <system_error>

namespace cb::mcbp {
/**
 * Definition of the valid response status numbers.
 *
 * A well written client should be "future proof" by handling new
 * error codes to be defined. Note that new error codes means that
 * the requested operation wasn't performed.
 */
enum class Status : uint16_t {
    /** The operation completed successfully */
    Success = 0x00,
    /** The key does not exists */
    KeyEnoent = 0x01,
    /** The key exists in the cluster (with another CAS value) */
    KeyEexists = 0x02,
    /** The document exceeds the maximum size */
    E2big = 0x03,
    /** Invalid request */
    Einval = 0x04,
    /** The document was not stored for some reason. This is
     * currently a "catch all" for number or error situations, and
     * should be split into multiple error codes. */
    NotStored = 0x05,
    /** Non-numeric server-side value for incr or decr */
    DeltaBadval = 0x06,
    /** The server is not responsible for the requested vbucket */
    NotMyVbucket = 0x07,
    /** Not connected to a bucket */
    NoBucket = 0x08,
    /** The requested resource is locked */
    Locked = 0x09,
    /** Stream not found for DCP message */
    DcpStreamNotFound = 0x0A,
    /** The message's opaque does not match  of the stream */
    OpaqueNoMatch = 0x0B,
    /// The command would have been throttled
    EWouldThrottle = 0x0c,
    /// The command can't be run in a config-only-bucket
    EConfigOnly = 0x0d,
    /** The requested resource is not locked */
    NotLocked = 0x0e,
    /** The provided CAS value is invalid */
    CasValueInvalid = 0x0f,
    /** The authentication context is stale. You should reauthenticate*/
    AuthStale = 0x1f,
    /** Authentication failure (invalid user/password combination,
     * OR an internal error in the authentication library. Could
     * be a misconfigured SASL configuration. See server logs for
     * more information.) */
    AuthError = 0x20,
    /** Authentication OK so far, please continue */
    AuthContinue = 0x21,
    /** The requested value is outside the legal range
     * (similar to EINVAL, but more specific) */
    Erange = 0x22,
    /** Roll back to an earlier version of the vbucket UUID
     * (_currently_ only used by DCP for agreeing on selecting a
     * starting point) */
    Rollback = 0x23,
    /** No access (could be opcode, value, bucket etc) */
    Eaccess = 0x24,
    /** The Couchbase cluster is currently initializing this
     * node, and the Cluster manager has not yet granted all
     * users access to the cluster. */
    NotInitialized = 0x25,
    /// An encryption key is not available for the requested operation
    EncryptionKeyNotAvailable = 0x26,
    /// The calculated checksum of the data does not match the expected
    /// checksum
    ChecksumMismatch = 0x27,

    // Start error codes for rate limits. Reserve segment [0x30, 0x4f]

    /// The tenant exceeded network ingress limit
    RateLimitedNetworkIngress = 0x30,
    /// The tenant exceeded network egress limit
    RateLimitedNetworkEgress = 0x31,
    /// The tenant exceeded max number of connections
    RateLimitedMaxConnections = 0x32,
    /// The tenant exceeded max number of commands
    RateLimitedMaxCommands = 0x33,
    /// There is too much data in the bucket
    BucketSizeLimitExceeded = 0x35,
    /// Bucket RR% is too low, write to the bucket is rejected
    BucketResidentRatioTooLow = 0x36,
    /// Bucket Data Size too high, writes to this bucket is rejected
    BucketDataSizeTooBig = 0x37,
    /// Disk Space where the bucket is deployed too low, writes to
    /// this bucket is rejected
    BucketDiskSpaceTooLow = 0x38,

    // END Rate limit error codes. Next error should be 0x50

    /// The bucket is paused and cannot perform the requested operation.
    BucketPaused = 0x50,

    /// The operation was cancelled before completion and had no effect.
    Cancelled = 0x51,

    /// The server don't know about the frame info, and can't use it
    UnknownFrameInfo = 0x80,
    /** The server have no idea what this command is for */
    UnknownCommand = 0x81,
    /** Not enough memory */
    Enomem = 0x82,
    /** The server does not support this command */
    NotSupported = 0x83,
    /** An internal error in the server */
    Einternal = 0x84,
    /** The system is currently too busy to handle the request. */
    Ebusy = 0x85,
    /** A temporary error condition occurred. Retrying the
     * operation may resolve the problem. This could be that the
     * server is in a degraded situation (like running warmup on
     * the node), the vbucket could be in an "incorrect" state, a
     * temporary failure from the underlying persistence layer,
     * etc).
     */
    Etmpfail = 0x86,

    /**
     * There is something wrong with the syntax of the provided
     * XATTR.
     */
    XattrEinval = 0x87,

    /**
     * Operation attempted with an unknown collection.
     */
    UnknownCollection = 0x88,

    /**
     * Bucket Manifest update could not be applied to vbucket(s)
     */
    CannotApplyCollectionsManifest = 0x8a,

    /**
     * Operation attempted with an unknown scope.
     */
    UnknownScope = 0x8c,

    /**
     * Operation attempted and the stream-ID is invalid
     */
    DcpStreamIdInvalid = 0x8d,

    /// The specified durability level is invalid.
    DurabilityInvalidLevel = 0xa0,

    /**
     * The specified durability requirements are not currently possible (for
     * example there are insufficient replicas online).
     */
    DurabilityImpossible = 0xa1,

    /// A SyncWrite is already in progress on the specified key.
    SyncWriteInProgress = 0xa2,

    /**
     * The SyncWrite request has not completed in the specified time and has
     * ambiguous result - it may Succeed or Fail; but the final value is not
     * yet known.
     */
    SyncWriteAmbiguous = 0xa3,

    /// The SyncWrite is being re-committed after a change in active node.
    SyncWriteReCommitInProgress = 0xa4,

    /// A RangeScan was cancelled whilst executing a continue
    RangeScanCancelled = 0xa5,

    /// A RangeScan has more data available
    RangeScanMore = 0xa6,

    /// A RangeScan has completed successfully
    RangeScanComplete = 0xa7,

    /// A RangeScan create failed because of vb-uuid mismatch
    VbUuidNotEqual = 0xa8,

    /*
     * Sub-document specific responses.
     */

    /** The provided path does not exist in the document. */
    SubdocPathEnoent = 0xc0,

    /** One of path components treats a non-dictionary as a dictionary, or
     * a non-array as an array.
     * [Arithmetic operations only] The value the path points to is not
     * a number. */
    SubdocPathMismatch = 0xc1,

    /** The path’s syntax was incorrect. */
    SubdocPathEinval = 0xc2,

    /** The path provided is too large; either the string is too long,
     * or it contains too many components. */
    SubdocPathE2big = 0xc3,

    /** The document has too many levels to parse. */
    SubdocDocE2deep = 0xc4,

    /** [For mutations only] The value provided will invalidate the JSON if
     * inserted. */
    SubdocValueCantinsert = 0xc5,

    /** The existing document is not valid JSON. */
    SubdocDocNotJson = 0xc6,

    /** [For arithmetic ops] The existing number is out of the valid range
     * for arithmetic ops (cannot be represented as an int64_t). */
    SubdocNumErange = 0xc7,

    /** [For arithmetic ops] The delta supplied is invalid. It is either
     * 0, not an integer, or out of the int64 range */
    SubdocDeltaEinval = 0xc8,

    /** [For mutations only] The requested operation requires the path to
     * not already exist, but it exists. */
    SubdocPathEexists = 0xc9,

    /** [For mutations only] Inserting the value would cause the document
     * to be too deep. */
    SubdocValueEtoodeep = 0xca,

    /** [For multi-path commands only] An invalid combination of commands
     * was specified. */
    SubdocInvalidCombo = 0xcb,

    /** [For multi-path commands only] Specified key was successfully
     * found, but one or more path operations failed. Examine the individual
     * lookup_result (MULTI_LOOKUP) / mutation_result (MULTI_MUTATION)
     * structures for details. */
    SubdocMultiPathFailure = 0xcc,

    /**
     * The operation completed successfully, but operated on a deleted
     * document.
     */
    SubdocSuccessDeleted = 0xcd,

    /**
     * The combination of the subdoc flags for the xattrs doesn't make
     * any sense
     */
    SubdocXattrInvalidFlagCombo = 0xce,

    /**
     * Only a single xattr key may be accessed at the same time.
     */
    SubdocXattrInvalidKeyCombo = 0xcf,

    /**
     * The server has no knowledge of the requested macro
     */
    SubdocXattrUnknownMacro = 0xd0,

    /**
     * The server has no knowledge of the requested virtual xattr
     */
    SubdocXattrUnknownVattr = 0xd1,

    /**
     * Virtual xattrs can't be modified
     */
    SubdocXattrCantModifyVattr = 0xd2,

    /**
     * [For multi-path commands only] Specified key was found as a
     * Deleted document, but one or more path operations
     * failed. Examine the individual lookup_result (MULTI_LOOKUP) /
     * mutation_result (MULTI_MUTATION) structures for details.
     */
    SubdocMultiPathFailureDeleted = 0xd3,

    /**
     * According to the spec all xattr commands should come first,
     * followed by the commands for the document body
     */
    SubdocInvalidXattrOrder = 0xd4,

    /// The server don't know this virtual macro
    SubdocXattrUnknownVattrMacro = 0xd5,

    /// The document isn't dead (and we wanted to revive the document)
    SubdocCanOnlyReviveDeletedDocuments = 0xd6,

    /// A deleted document can't have a user value
    SubdocDeletedDocumentCantHaveValue = 0xd7,

    /// The operation used SUBDOC_FLAG_BINARY_VALUE, but the value
    /// was not a binary field.
    SubdocFieldNotBinaryValue = 0xd8,

    /*************************************************************************/

    /**
     * Number of valid elements in the enumeration (as used by Couchbase).
     * Note there are additional values reserved for user application below).
     */
    COUNT,

    /**
     * The following range of 256 error codes is reserved for end-user
     * applications (e.g. proxies). Couchbase itself does not return them.
     */
    ReservedUserStart = 0xff00,
    ReservedUserEnd = 0xffff
};

const std::error_category& error_category() noexcept;

class error : public std::system_error {
public:
    error(Status ev, const std::string& what_arg)
        : system_error(int(ev), error_category(), what_arg) {
    }

    error(Status ev, const char* what_arg)
        : system_error(int(ev), error_category(), what_arg) {
    }
};

static inline std::error_condition make_error_condition(Status e) {
    return std::error_condition(int(e), error_category());
}

/// Is the status code known to the system or not
bool is_known(Status status);

/**
 * Check if the provided status code represents success or a failure
 *
 * @param status the status code code to check
 * @return true if the status code represents a successful critera
 *         false if the status code represents a failure and the payload
 *               should be replaced with the standard payload containing
 *               the error context and UUID (if set)
 */
bool isStatusSuccess(Status status);
std::ostream& operator<<(std::ostream& out, cb::mcbp::Status status);
std::string format_as(Status status);

template <typename BasicJsonType>
void to_json(BasicJsonType& j, cb::mcbp::Status status) {
    j = format_as(status);
}
} // namespace cb::mcbp

/**
 * Get a textual representation of the given error code
 */
std::string to_string(cb::mcbp::Status status, bool shortname = false);

namespace std {

template <>
struct is_error_condition_enum<cb::mcbp::Status> : public true_type {};

} // namespace std
