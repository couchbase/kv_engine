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

#include <memcached/connection_iface.h>
#include <memcached/rbac/privilege_database.h>
#include <memcached/types.h>

namespace cb::test {
/**
 * The DummyConnection is an implementation of the ConnectionIface which don't
 * offer _ANY_ functionality, but may be used in the case where you don't
 * need anything but a holder class.
 */
class DummyConnection : public ConnectionIface {
public:
    DummyConnection()
        : ConnectionIface({{"ip", "127.0.0.1"}, {"port", 665}},
                          {{"ip", "127.0.0.1"}, {"port", 666}}) {
    }
    ~DummyConnection() override = default;
    void scheduleDcpStep() override {
    }
    ConnectionPriority getPriority() const override {
        return ConnectionPriority::Medium;
    }
    void setPriority(ConnectionPriority value) override {
    }
    std::string_view getDescription() const override {
        using namespace std::string_view_literals;
        return "dummy"sv;
    }
    const cb::rbac::UserIdent& getUser() const override {
        return user;
    }
    void setDcpFlowControlBufferSize(std::size_t size) override {
    }

protected:
    cb::rbac::UserIdent user{"dummy", cb::rbac::Domain::Local};
};
} // namespace cb::test
