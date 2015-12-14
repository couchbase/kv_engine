/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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

#include "testapp_client_test.h"
#include "testapp_greenstack_connection.h"
#include "testapp_mcbp_connection.h"

std::ostream& operator<<(std::ostream& os, const TransportProtocols& t) {
    os << to_string(t);
    return os;
}

const char* to_string(const TransportProtocols& transport) {
    switch (transport) {
    case TransportProtocols::PlainMcbp:
        return "Mcbp";
    case TransportProtocols::PlainGreenstack:
        return "Greenstack";
    case TransportProtocols::PlainIpv6Mcbp:
        return "Mcbp-Ipv6";
    case TransportProtocols::PlainIpv6Greenstack:
        return "Greenstack-Ipv6";
    case TransportProtocols::SslMcbp:
        return "Mcbp-Ssl";
    case TransportProtocols::SslGreenstack:
        return "Greenstack-Ssl";
    case TransportProtocols::SslIpv6Mcbp:
        return "Mcbp-Ipv6-Ssl";
    case TransportProtocols::SslIpv6Greenstack:
        return "Greenstack-Ipv6-Ssl";
    }
    throw std::logic_error("Unknown transport");
}

MemcachedConnection& TestappClientTest::prepare(MemcachedConnection& connection) {
    connection.reconnect();
    if (connection.getProtocol() == Protocol::Memcached) {
        auto& c = dynamic_cast<MemcachedBinprotConnection&>(connection);
        c.setDatatypeSupport(true);
        c.setMutationSeqnoSupport(true);
    } else {
        auto& c = dynamic_cast<MemcachedGreenstackConnection&>(connection);
        c.hello("memcached_testapp", "1,0", "BucketTest");
    }
    return connection;
}

MemcachedConnection& TestappClientTest::getConnection() {
    switch (GetParam()) {
    case TransportProtocols::PlainMcbp:
        return prepare(connectionMap.getConnection(Protocol::Memcached,
                                                   false, AF_INET));
    case TransportProtocols::PlainGreenstack:
        return prepare(connectionMap.getConnection(Protocol::Greenstack,
                                                   false, AF_INET));
    case TransportProtocols::PlainIpv6Mcbp:
        return prepare(connectionMap.getConnection(Protocol::Memcached,
                                                   false, AF_INET6));
    case TransportProtocols::PlainIpv6Greenstack:
        return prepare(connectionMap.getConnection(Protocol::Greenstack,
                                                   false, AF_INET6));
    case TransportProtocols::SslMcbp:
        return prepare(connectionMap.getConnection(Protocol::Memcached,
                                                   true, AF_INET));
    case TransportProtocols::SslGreenstack:
        return prepare(connectionMap.getConnection(Protocol::Greenstack,
                                                   true, AF_INET));
    case TransportProtocols::SslIpv6Mcbp:
        return prepare(connectionMap.getConnection(Protocol::Memcached,
                                                   true, AF_INET6));
    case TransportProtocols::SslIpv6Greenstack:
        return prepare(connectionMap.getConnection(Protocol::Greenstack,
                                                   true, AF_INET6));
    }
    throw std::logic_error("Unknown transport");
}
