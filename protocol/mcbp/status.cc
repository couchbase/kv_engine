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

#include <mcbp/protocol/status.h>

namespace cb {
namespace mcbp {

bool isStatusSuccess(Status status) {
    switch (status) {
    case Status::Success:
    case Status::AuthContinue:
    case Status::SubdocSuccessDeleted:
    case Status::SubdocMultiPathFailure:
    case Status::SubdocMultiPathFailureDeleted:
    case Status::Rollback:
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
    case Status::AuthStale:
    case Status::AuthError:
    case Status::Erange:
    case Status::Eaccess:
    case Status::NotInitialized:
    case Status::UnknownFrameInfo:
    case Status::UnknownCommand:
    case Status::Enomem:
    case Status::NotSupported:
    case Status::Einternal:
    case Status::Ebusy:
    case Status::Etmpfail:
    case Status::XattrEinval:
    case Status::UnknownCollection:
    case Status::NoCollectionsManifest:
    case Status::CannotApplyCollectionsManifest:
    case Status::CollectionsManifestIsAhead:
    case Status::UnknownScope:
    case Status::DurabilityInvalidLevel:
    case Status::DurabilityImpossible:
    case Status::SyncWriteInProgress:
    case Status::SyncWriteAmbiguous:
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
    const char* name() const NOEXCEPT override {
        return "MCBP status codes";
    }

    std::string message(int code) const override {
        return to_string(cb::mcbp::Status(code));
    }

    std::error_condition default_error_condition(
            int code) const NOEXCEPT override {
        return std::error_condition(code, *this);
    }
};

const std::error_category& error_category() NOEXCEPT {
    static status_category category_instance;
    return category_instance;
}

} // namespace mcbp
} // namespace cb

std::string to_string(cb::mcbp::Status status) {
    using namespace cb::mcbp;

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
    case Status::NoCollectionsManifest:
        return "No Collections Manifest";
    case Status::CannotApplyCollectionsManifest:
        return "Cannot apply collections manifest";
    case Status::CollectionsManifestIsAhead:
        return "Collections manifest is ahead";
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
        return "Subdoc: Delta is 0, not a number, or outside the valid range";
    case Status::SubdocPathEexists:
        return "Subdoc: Document path already exists";
    case Status::SubdocValueEtoodeep:
        return "Subdoc: Inserting value would make document too deep";
    case Status::SubdocInvalidCombo:
        return "Subdoc: Invalid combination for multi-path command";
    case Status::SubdocMultiPathFailure:
        return "Subdoc: One or more paths in a multi-path command failed";
    case Status::SubdocSuccessDeleted:
        return "Subdoc: Operation completed successfully on a deleted document";
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
        return "Subdoc: One or more paths in a multi-path command failed on a "
               "deleted document";
    case Status::SubdocInvalidXattrOrder:
        return "Subdoc: Invalid XATTR order (xattrs should come first)";

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

    throw std::invalid_argument(
            "to_string(cb::mcbp::Status): Invalid status code: " +
            std::to_string(int(status)));
}

std::ostream& operator<<(std::ostream& out, cb::mcbp::Status status) {
    out << ::to_string(status);
    return out;
}
