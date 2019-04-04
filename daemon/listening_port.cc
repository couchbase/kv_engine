/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc.
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
#include "listening_port.h"

ListeningPort::ListeningPort(std::string tag,
                             std::string host,
                             in_port_t port,
                             bool tcp_nodelay)
    : tag(std::move(tag)),
      host(std::move(host)),
      port(port),
      ipv6(false),
      ipv4(false),
      tcp_nodelay(tcp_nodelay) {
}

std::shared_ptr<ListeningPort::Ssl> ListeningPort::getSslSettings() const {
    return ssl;
}

void ListeningPort::setSslSettings(const std::string& key,
                                   const std::string& cert) {
    if (key.empty() || cert.empty()) {
        ssl.reset();
    } else {
        ssl = std::make_shared<ListeningPort::Ssl>(key, cert);
    }
}
