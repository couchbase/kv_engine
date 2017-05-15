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
    switch (status) {
    case cb::mcbp::Status::Success:
        return "Success";
    case cb::mcbp::Status::KeyEnoent:
        return "Not found";
    case cb::mcbp::Status::KeyEexists:
        return "Data exists for key";
    case cb::mcbp::Status::E2big:
        return "Too large";
    case cb::mcbp::Status::Einval:
        return "Invalid arguments";
    case cb::mcbp::Status::NotStored:
        return "Not stored";
    case cb::mcbp::Status::DeltaBadval:
        return "Non-numeric server-side value for incr or decr";
    case cb::mcbp::Status::NotMyVbucket:
        return "I'm not responsible for this vbucket";
    case cb::mcbp::Status::NoBucket:
        return "Not connected to a bucket";
    case cb::mcbp::Status::Locked:
        return "Resource locked";
    case cb::mcbp::Status::AuthStale:
        return "Authentication stale. Please reauthenticate";
    case cb::mcbp::Status::AuthError:
        return "Auth failure";
    case cb::mcbp::Status::AuthContinue:
        return "Auth continue";
    case cb::mcbp::Status::Erange:
        return "Outside range";
    case cb::mcbp::Status::Rollback:
        return "Rollback";
    case cb::mcbp::Status::Eaccess:
        return "No access";
    case cb::mcbp::Status::NotInitialized:
        return "Node not initialized";
    case cb::mcbp::Status::UnknownCommand:
        return "Unknown command";
    case cb::mcbp::Status::Enomem:
        return "Out of memory";
    case cb::mcbp::Status::NotSupported:
        return "Not supported";
    case cb::mcbp::Status::Einternal:
        return "Internal error";
    case cb::mcbp::Status::Ebusy:
        return "Server too busy";
    case cb::mcbp::Status::Etmpfail:
        return "Temporary failure";
    case cb::mcbp::Status::XattrEinval:
        return "Invalid XATTR section";
    case cb::mcbp::Status::UnknownCollection:
        return "Unknown Collection";
    case cb::mcbp::Status::SubdocPathEnoent:
        return "Subdoc: Path not does not exist";
    case cb::mcbp::Status::SubdocPathMismatch:
        return "Subdoc: Path mismatch";
    case cb::mcbp::Status::SubdocPathEinval:
        return "Subdoc: Invalid path";
    case cb::mcbp::Status::SubdocPathE2big:
        return "Subdoc: Path too large";
    case cb::mcbp::Status::SubdocDocE2deep:
        return "Subdoc: Document too deep";
    case cb::mcbp::Status::SubdocValueCantinsert:
        return "Subdoc: Cannot insert specified value";
    case cb::mcbp::Status::SubdocDocNotJson:
        return "Subdoc: Existing document not JSON";
    case cb::mcbp::Status::SubdocNumErange:
        return "Subdoc: Existing number outside valid arithmetic range";
    case cb::mcbp::Status::SubdocDeltaEinval:
        return "Subdoc: Delta is 0, not a number, or outside the valid range";
    case cb::mcbp::Status::SubdocPathEexists:
        return "Subdoc: Document path already exists";
    case cb::mcbp::Status::SubdocValueEtoodeep:
        return "Subdoc: Inserting value would make document too deep";
    case cb::mcbp::Status::SubdocInvalidCombo:
        return "Subdoc: Invalid combination for multi-path command";
    case cb::mcbp::Status::SubdocMultiPathFailure:
        return "Subdoc: One or more paths in a multi-path command failed";
    case cb::mcbp::Status::SubdocSuccessDeleted:
        return "Subdoc: Operation completed successfully on a deleted document";
    case cb::mcbp::Status::SubdocXattrInvalidFlagCombo:
        return "Subdoc: Invalid combination of xattr flags";
    case cb::mcbp::Status::SubdocXattrInvalidKeyCombo:
        return "Subdoc: Invalid combination of xattr keys";
    case cb::mcbp::Status::SubdocXattrUnknownMacro:
        return "Subdoc: Unknown xattr macro";
    case cb::mcbp::Status::SubdocXattrUnknownVattr:
        return "Subdoc: Unknown xattr virtual attribute";
    case cb::mcbp::Status::SubdocXattrCantModifyVattr:
        return "Subdoc: Can't modify virtual attributes";
    case cb::mcbp::Status::SubdocMultiPathFailureDeleted:
        return "Subdoc: One or more paths in a multi-path command failed on a "
               "deleted document";
    }

    throw std::invalid_argument(
        "to_string(cb::mcbp::Status): Invalid status code: " +
        std::to_string(int(status)));
}
