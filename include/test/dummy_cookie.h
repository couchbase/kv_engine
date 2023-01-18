/*
 *     Copyright 2023 Couchbase, Inc
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

#include "dummy_connection.h"
#include <memcached/cookie_iface.h>

namespace cb::test {
/**
 * The DummyCookie is an implementation of the CookieIface which don't
 * offer _ANY_ functionality, but may be used in the case where you don't
 * need anything but a holder class.
 */
class DummyCookie : public CookieIface {
public:
    ConnectionIface& getConnectionIface() override {
        return connection;
    }
    uint32_t getConnectionId() const override {
        return 0;
    }
    void notifyIoComplete(cb::engine_errc status) override {
    }
    void reserve() override {
    }
    void release() override {
    }
    bool isValidJson(std::string_view view) override {
        return false;
    }
    bool isMutationExtrasSupported() const override {
        return false;
    }
    bool isCollectionsSupported() const override {
        return false;
    }
    bool isDatatypeSupported(
            protocol_binary_datatype_t datatype) const override {
        return false;
    }
    uint32_t getPrivilegeContextRevision() override {
        return 0;
    }
    cb::rbac::PrivilegeAccess testPrivilege(
            cb::rbac::Privilege privilege,
            std::optional<ScopeID> sid,
            std::optional<CollectionID> cid) const override {
        return cb::rbac::PrivilegeAccessOk;
    }
    cb::rbac::PrivilegeAccess checkPrivilege(
            cb::rbac::Privilege privilege,
            std::optional<ScopeID> sid,
            std::optional<CollectionID> cid) override {
        return cb::rbac::PrivilegeAccessOk;
    }
    cb::rbac::PrivilegeAccess checkForPrivilegeAtLeastInOneCollection(
            cb::rbac::Privilege privilege) const override {
        return cb::rbac::PrivilegeAccessOk;
    }
    std::string_view getInflatedInputPayload() const override {
        return {};
    }
    void setCurrentCollectionInfo(ScopeID sid,
                                  CollectionID cid,
                                  uint64_t manifestUid,
                                  bool metered) override {
    }
    bool checkThrottle(size_t pendingRBytes, size_t pendingWBytes) override {
        return false;
    }
    bool sendResponse(cb::engine_errc status,
                      std::string_view extras,
                      std::string_view value) override {
        return false;
    }
    void setErrorContext(std::string message) override {
    }
    std::string getErrorContext() const override {
        return "";
    }
    void setUnknownCollectionErrorContext(uint64_t manifestUid) override {
    }
    void auditDocumentAccess(
            cb::audit::document::Operation operation) override {
    }
    cb::engine_errc preLinkDocument(item_info& info) override {
        return cb::engine_errc::failed;
    }

protected:
    DummyConnection connection;
};
} // namespace cb::test
