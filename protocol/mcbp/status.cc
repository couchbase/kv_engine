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

#include <fmt/ostream.h>
#include <mcbp/protocol/status.h>

namespace cb::mcbp {

bool is_known(Status status) {
    switch (status) {
    case Status::Success:
    case Status::KeyEnoent:
    case Status::KeyEexists:
    case Status::E2big:
    case Status::Einval:
    case Status::NotStored:
    case Status::DeltaBadval:
    case Status::NotMyVbucket:
    case Status::NoBucket:
    case Status::Locked:
    case Status::DcpStreamNotFound:
    case Status::OpaqueNoMatch:
    case Status::EWouldThrottle:
    case Status::EConfigOnly:
    case Status::AuthStale:
    case Status::AuthError:
    case Status::AuthContinue:
    case Status::Erange:
    case Status::Rollback:
    case Status::Eaccess:
    case Status::NotInitialized:
    case Status::RateLimitedNetworkIngress:
    case Status::RateLimitedNetworkEgress:
    case Status::RateLimitedMaxConnections:
    case Status::RateLimitedMaxCommands:
    case Status::ScopeSizeLimitExceeded:
    case Status::BucketSizeLimitExceeded:
    case Status::Cancelled:
    case Status::BucketPaused:
    case Status::UnknownFrameInfo:
    case Status::UnknownCommand:
    case Status::Enomem:
    case Status::NotSupported:
    case Status::Einternal:
    case Status::Ebusy:
    case Status::Etmpfail:
    case Status::XattrEinval:
    case Status::UnknownCollection:
    case Status::CannotApplyCollectionsManifest:
    case Status::UnknownScope:
    case Status::DcpStreamIdInvalid:
    case Status::DurabilityInvalidLevel:
    case Status::DurabilityImpossible:
    case Status::SyncWriteInProgress:
    case Status::SyncWriteAmbiguous:
    case Status::SyncWriteReCommitInProgress:
    case Status::RangeScanCancelled:
    case Status::RangeScanMore:
    case Status::RangeScanComplete:
    case Status::VbUuidNotEqual:
    case Status::SubdocPathEnoent:
    case Status::SubdocPathMismatch:
    case Status::SubdocPathEinval:
    case Status::SubdocPathE2big:
    case Status::SubdocDocE2deep:
    case Status::SubdocValueCantinsert:
    case Status::SubdocDocNotJson:
    case Status::SubdocNumErange:
    case Status::SubdocDeltaEinval:
    case Status::SubdocPathEexists:
    case Status::SubdocValueEtoodeep:
    case Status::SubdocInvalidCombo:
    case Status::SubdocMultiPathFailure:
    case Status::SubdocSuccessDeleted:
    case Status::SubdocXattrInvalidFlagCombo:
    case Status::SubdocXattrInvalidKeyCombo:
    case Status::SubdocXattrUnknownMacro:
    case Status::SubdocXattrUnknownVattr:
    case Status::SubdocXattrCantModifyVattr:
    case Status::SubdocMultiPathFailureDeleted:
    case Status::SubdocInvalidXattrOrder:
    case Status::SubdocXattrUnknownVattrMacro:
    case Status::SubdocCanOnlyReviveDeletedDocuments:
    case Status::SubdocDeletedDocumentCantHaveValue:
        return true;
    case Status::COUNT:
    case Status::ReservedUserStart:
    case Status::ReservedUserEnd:
        break;
    }
    return false;
}

bool isStatusSuccess(Status status) {
    switch (status) {
    case Status::Success:
    case Status::AuthContinue:
    case Status::SubdocSuccessDeleted:
    case Status::SubdocMultiPathFailure:
    case Status::SubdocMultiPathFailureDeleted:
    case Status::Rollback:
    case Status::RangeScanMore:
    case Status::RangeScanComplete:
        return true;

    case Status::KeyEnoent:
    case Status::KeyEexists:
    case Status::E2big:
    case Status::Einval:
    case Status::NotStored:
    case Status::DeltaBadval:
    case Status::NotMyVbucket:
    case Status::NoBucket:
    case Status::Locked:
    case Status::DcpStreamNotFound:
    case Status::OpaqueNoMatch:
    case Status::EWouldThrottle:
    case Status::EConfigOnly:
    case Status::AuthStale:
    case Status::AuthError:
    case Status::Erange:
    case Status::Eaccess:
    case Status::NotInitialized:
    case Status::RateLimitedNetworkIngress:
    case Status::RateLimitedNetworkEgress:
    case Status::RateLimitedMaxConnections:
    case Status::RateLimitedMaxCommands:
    case Status::ScopeSizeLimitExceeded:
    case Status::BucketSizeLimitExceeded:
    case Status::Cancelled:
    case Status::BucketPaused:
    case Status::UnknownFrameInfo:
    case Status::UnknownCommand:
    case Status::Enomem:
    case Status::NotSupported:
    case Status::Einternal:
    case Status::Ebusy:
    case Status::Etmpfail:
    case Status::XattrEinval:
    case Status::UnknownCollection:
    case Status::CannotApplyCollectionsManifest:
    case Status::UnknownScope:
    case Status::DurabilityInvalidLevel:
    case Status::DurabilityImpossible:
    case Status::SyncWriteInProgress:
    case Status::SyncWriteAmbiguous:
    case Status::SyncWriteReCommitInProgress:
    case Status::RangeScanCancelled:
    case Status::VbUuidNotEqual:
    case Status::SubdocPathEnoent:
    case Status::SubdocPathMismatch:
    case Status::SubdocPathEinval:
    case Status::SubdocPathE2big:
    case Status::SubdocDocE2deep:
    case Status::SubdocValueCantinsert:
    case Status::SubdocDocNotJson:
    case Status::SubdocNumErange:
    case Status::SubdocDeltaEinval:
    case Status::SubdocPathEexists:
    case Status::SubdocValueEtoodeep:
    case Status::SubdocInvalidCombo:
    case Status::SubdocXattrInvalidFlagCombo:
    case Status::SubdocXattrInvalidKeyCombo:
    case Status::SubdocXattrUnknownMacro:
    case Status::SubdocXattrUnknownVattr:
    case Status::SubdocXattrCantModifyVattr:
    case Status::SubdocInvalidXattrOrder:
    case Status::SubdocXattrUnknownVattrMacro:
    case Status::SubdocCanOnlyReviveDeletedDocuments:
    case Status::SubdocDeletedDocumentCantHaveValue:
    case Status::COUNT:
    case Status::ReservedUserStart:
    case Status::ReservedUserEnd:
    case Status::DcpStreamIdInvalid:
        return false;
    }
    throw std::invalid_argument("isStatusSuccess(): invalid status provided");
}

class status_category : public std::error_category {
public:
    const char* name() const noexcept override {
        return "MCBP status codes";
    }

    std::string message(int code) const override {
        return ::to_string(cb::mcbp::Status(code));
    }

    std::error_condition default_error_condition(int code) const
            noexcept override {
        return std::error_condition(code, *this);
    }
};

const std::error_category& error_category() noexcept {
    static status_category category_instance;
    return category_instance;
}
std::ostream& operator<<(std::ostream& out, cb::mcbp::Status status) {
    out << ::to_string(status);
    return out;
}
} // namespace cb::mcbp

std::string to_string(cb::mcbp::Status status, bool shortname) {
    using namespace cb::mcbp;

    if (shortname) {
        switch (status) {
        case Status::Success:
            return "Success";
        case Status::KeyEnoent:
            return "KeyEnoent";
        case Status::KeyEexists:
            return "KeyEexists";
        case Status::E2big:
            return "E2big";
        case Status::Einval:
            return "Einval";
        case Status::NotStored:
            return "NotStored";
        case Status::DeltaBadval:
            return "DeltaBadval";
        case Status::NotMyVbucket:
            return "NotMyVbucket";
        case Status::NoBucket:
            return "NoBucket";
        case Status::Locked:
            return "Locked";
        case Status::DcpStreamNotFound:
            return "DcpStreamNotFound";
        case Status::OpaqueNoMatch:
            return "OpaqueNoMatch";
        case Status::EWouldThrottle:
            return "EWouldThrottle";
        case Status::EConfigOnly:
            return "EConfigOnly";
        case Status::AuthStale:
            return "AuthStale";
        case Status::AuthError:
            return "AuthError";
        case Status::AuthContinue:
            return "AuthContinue";
        case Status::Erange:
            return "Erange";
        case Status::Rollback:
            return "Rollback";
        case Status::Eaccess:
            return "Eaccess";
        case Status::NotInitialized:
            return "NotInitialized";
        case Status::RateLimitedNetworkIngress:
            return "RateLimitedNetworkIngress";
        case Status::RateLimitedNetworkEgress:
            return "RateLimitedNetworkEgress";
        case Status::RateLimitedMaxConnections:
            return "RateLimitedMaxConnections";
        case Status::RateLimitedMaxCommands:
            return "RateLimitedMaxCommands";
        case Status::ScopeSizeLimitExceeded:
            return "ScopeSizeLimitExceeded";
        case Status::BucketSizeLimitExceeded:
            return "BucketSizeLimitExceeded";
        case Status::Cancelled:
            return "Cancelled";
        case Status::BucketPaused:
            return "BucketPaused";
        case Status::UnknownFrameInfo:
            return "UnknownFrameInfo";
        case Status::UnknownCommand:
            return "UnknownCommand";
        case Status::Enomem:
            return "Enomem";
        case Status::NotSupported:
            return "NotSupported";
        case Status::Einternal:
            return "Einternal";
        case Status::Ebusy:
            return "Ebusy";
        case Status::Etmpfail:
            return "Etmpfail";
        case Status::XattrEinval:
            return "XattrEinval";
        case Status::UnknownCollection:
            return "UnknownCollection";
        case Status::CannotApplyCollectionsManifest:
            return "CannotApplyCollectionsManifest";
        case Status::UnknownScope:
            return "UnknownScope";
        case Status::DcpStreamIdInvalid:
            return "DcpStreamIdInvalid";
        case Status::DurabilityInvalidLevel:
            return "DurabilityInvalidLevel";
        case Status::DurabilityImpossible:
            return "DurabilityImpossible";
        case Status::SyncWriteInProgress:
            return "SyncWriteInProgress";
        case Status::SyncWriteAmbiguous:
            return "SyncWriteAmbiguous";
        case Status::SyncWriteReCommitInProgress:
            return "SyncWriteReCommitInProgress";
        case Status::RangeScanCancelled:
            return "RangeScanCancelled";
        case Status::RangeScanMore:
            return "RangeScanMore";
        case Status::RangeScanComplete:
            return "RangeScanComplete";
        case Status::VbUuidNotEqual:
            return "VbUuidNotEqual";
        case Status::SubdocPathEnoent:
            return "SubdocPathEnoent";
        case Status::SubdocPathMismatch:
            return "SubdocPathMismatch";
        case Status::SubdocPathEinval:
            return "SubdocPathEinval";
        case Status::SubdocPathE2big:
            return "SubdocPathE2big";
        case Status::SubdocDocE2deep:
            return "SubdocDocE2deep";
        case Status::SubdocValueCantinsert:
            return "SubdocValueCantinsert";
        case Status::SubdocDocNotJson:
            return "SubdocDocNotJson";
        case Status::SubdocNumErange:
            return "SubdocNumErange";
        case Status::SubdocDeltaEinval:
            return "SubdocDeltaEinval";
        case Status::SubdocPathEexists:
            return "SubdocPathEexists";
        case Status::SubdocValueEtoodeep:
            return "SubdocValueEtoodeep";
        case Status::SubdocInvalidCombo:
            return "SubdocInvalidCombo";
        case Status::SubdocMultiPathFailure:
            return "SubdocMultiPathFailure";
        case Status::SubdocSuccessDeleted:
            return "SubdocSuccessDeleted";
        case Status::SubdocXattrInvalidFlagCombo:
            return "SubdocXattrInvalidFlagCombo";
        case Status::SubdocXattrInvalidKeyCombo:
            return "SubdocXattrInvalidKeyCombo";
        case Status::SubdocXattrUnknownMacro:
            return "SubdocXattrUnknownMacro";
        case Status::SubdocXattrUnknownVattr:
            return "SubdocXattrUnknownVattr";
        case Status::SubdocXattrCantModifyVattr:
            return "SubdocXattrCantModifyVattr";
        case Status::SubdocMultiPathFailureDeleted:
            return "SubdocMultiPathFailureDeleted";
        case Status::SubdocInvalidXattrOrder:
            return "SubdocInvalidXattrOrder";
        case Status::SubdocXattrUnknownVattrMacro:
            return "SubdocXattrUnknownVattrMacro";
        case Status::SubdocCanOnlyReviveDeletedDocuments:
            return "SubdocCanOnlyReviveDeletedDocuments";
        case Status::SubdocDeletedDocumentCantHaveValue:
            return "SubdocDeletedDocumentCantHaveValue";
        case Status::COUNT:
        case Status::ReservedUserStart:
        case Status::ReservedUserEnd:
            break;
        }
    } else {
        switch (status) {
        case Status::Success:
            return "Success";
        case Status::KeyEnoent:
            return "Not found";
        case Status::KeyEexists:
            return "Data exists for key";
        case Status::E2big:
            return "Too large";
        case Status::Einval:
            return "Invalid arguments";
        case Status::NotStored:
            return "Not stored";
        case Status::DeltaBadval:
            return "Non-numeric server-side value for incr or decr";
        case Status::NotMyVbucket:
            return "I'm not responsible for this vbucket";
        case Status::NoBucket:
            return "Not connected to a bucket";
        case Status::Locked:
            return "Resource locked";
        case Status::DcpStreamNotFound:
            return "No DCP Stream for this request";
        case Status::OpaqueNoMatch:
            return "Opaque does not match";
        case Status::EWouldThrottle:
            return "The command would have been throttled";
        case Status::EConfigOnly:
            return "Command can't be executed in a config-only bucket";
        case Status::AuthStale:
            return "Authentication stale. Please reauthenticate";
        case Status::AuthError:
            return "Auth failure";
        case Status::AuthContinue:
            return "Auth continue";
        case Status::Erange:
            return "Outside range";
        case Status::Rollback:
            return "Rollback";
        case Status::Eaccess:
            return "No access";
        case Status::NotInitialized:
            return "Node not initialized";
        case Status::RateLimitedNetworkIngress:
            return "Rate limit: Network ingress";
        case Status::RateLimitedNetworkEgress:
            return "Rate limit: Network Egress";
        case Status::RateLimitedMaxConnections:
            return "Rate limit: Max Connections";
        case Status::RateLimitedMaxCommands:
            return "Rate limit: Max Commands";
        case Status::ScopeSizeLimitExceeded:
            return "Too much data in Scope";
        case Status::BucketSizeLimitExceeded:
            return "Too much data in Bucket";
        case Status::Cancelled:
            return "The operation was cancelled";
        case Status::BucketPaused:
            return "The Bucket is paused";
        case Status::UnknownFrameInfo:
            return "Unknown frame info";
        case Status::UnknownCommand:
            return "Unknown command";
        case Status::Enomem:
            return "Out of memory";
        case Status::NotSupported:
            return "Not supported";
        case Status::Einternal:
            return "Internal error";
        case Status::Ebusy:
            return "Server too busy";
        case Status::Etmpfail:
            return "Temporary failure";
        case Status::XattrEinval:
            return "Invalid XATTR section";
        case Status::UnknownCollection:
            return "Unknown Collection";
        case Status::CannotApplyCollectionsManifest:
            return "Cannot apply collections manifest";
        case Status::UnknownScope:
            return "Unknown Scope";
        case Status::DcpStreamIdInvalid:
            return "DCP stream-ID is invalid";
        case Status::DurabilityInvalidLevel:
            return "Invalid durability level";
        case Status::DurabilityImpossible:
            return "Durability impossible";
        case Status::SyncWriteInProgress:
            return "Synchronous write in progress";
        case Status::SyncWriteAmbiguous:
            return "Synchronous write ambiguous";
        case Status::SyncWriteReCommitInProgress:
            return "Synchronous write re-commit in progress";
        case Status::RangeScanCancelled:
            return "RangeScan was cancelled";
        case Status::RangeScanMore:
            return "RangeScan has more data available";
        case Status::RangeScanComplete:
            return "RangeScan has completed";
        case Status::VbUuidNotEqual:
            return "VBucket uuid does not match";
        case Status::SubdocPathEnoent:
            return "Subdoc: Path not does not exist";
        case Status::SubdocPathMismatch:
            return "Subdoc: Path mismatch";
        case Status::SubdocPathEinval:
            return "Subdoc: Invalid path";
        case Status::SubdocPathE2big:
            return "Subdoc: Path too large";
        case Status::SubdocDocE2deep:
            return "Subdoc: Document too deep";
        case Status::SubdocValueCantinsert:
            return "Subdoc: Cannot insert specified value";
        case Status::SubdocDocNotJson:
            return "Subdoc: Existing document not JSON";
        case Status::SubdocNumErange:
            return "Subdoc: Existing number outside valid arithmetic range";
        case Status::SubdocDeltaEinval:
            return "Subdoc: Delta is 0, not a number, or outside the valid "
                   "range";
        case Status::SubdocPathEexists:
            return "Subdoc: Document path already exists";
        case Status::SubdocValueEtoodeep:
            return "Subdoc: Inserting value would make document too deep";
        case Status::SubdocInvalidCombo:
            return "Subdoc: Invalid combination for multi-path command";
        case Status::SubdocMultiPathFailure:
            return "Subdoc: One or more paths in a multi-path command failed";
        case Status::SubdocSuccessDeleted:
            return "Subdoc: Operation completed successfully on a deleted "
                   "document";
        case Status::SubdocXattrInvalidFlagCombo:
            return "Subdoc: Invalid combination of xattr flags";
        case Status::SubdocXattrInvalidKeyCombo:
            return "Subdoc: Invalid combination of xattr keys";
        case Status::SubdocXattrUnknownMacro:
            return "Subdoc: Unknown xattr macro";
        case Status::SubdocXattrUnknownVattr:
            return "Subdoc: Unknown xattr virtual attribute";
        case Status::SubdocXattrCantModifyVattr:
            return "Subdoc: Can't modify virtual attributes";
        case Status::SubdocMultiPathFailureDeleted:
            return "Subdoc: One or more paths in a multi-path command failed "
                   "on a "
                   "deleted document";
        case Status::SubdocInvalidXattrOrder:
            return "Subdoc: Invalid XATTR order (xattrs should come first)";
        case Status::SubdocXattrUnknownVattrMacro:
            return "Subdoc: The server don't know this virtual macro";
        case Status::SubdocCanOnlyReviveDeletedDocuments:
            return "Subdoc: Only deleted documents can be revived";
        case Status::SubdocDeletedDocumentCantHaveValue:
            return "Subdoc: A deleted document can't have a value";

        // Following are here to keep compiler happy; either handled below or
        // will throw if invalid (e.g. COUNT).
        case Status::COUNT:
        case Status::ReservedUserStart:
        case Status::ReservedUserEnd:
            break;
        }

        if (status >= cb::mcbp::Status::ReservedUserStart &&
            status <= cb::mcbp::Status::ReservedUserEnd) {
            return "ReservedUserRange: " + std::to_string(int(status));
        }
    }

    throw std::invalid_argument(
            "to_string(cb::mcbp::Status): Invalid status code: " +
            std::to_string(int(status)));
}
