/*
 *     Copyright 2019 Couchbase, Inc
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

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

namespace cb::test {

/// The DCP packet filter is called with the packet read from the source
/// node _before_ the DCP pipe does anything with the packet. The callback
/// may is free to do whatever it wants with the packet (inspect, modify
/// (or clear the content to drop the packet)). You could also attach more data
/// to the packet (for instance multiple new packets). When the callback returns
/// the content of the packet is sent to the destination node.
///
/// The following snippet filters out all DcpMutation packets (note: it
/// may not work very well ;-))
///
///  [](const std::string& source,
///     const std::string& destination,
///     std::vector<uint8_t>& packet) {
///      const auto* h = reinterpret_cast<const cb::mcbp::Header*>(
///                        packet.data());
///      if (h->isRequest()) {
///          const auto& req = h->getRequest();
///          if (req.getClientOpcode() == cb::mcbp::ClientOpcode::DcpMutation) {
///              packet.clear();
///          }
///      }
///  }
///
using DcpPacketFilter = std::function<void(const std::string& source,
                                           const std::string& destination,
                                           std::vector<uint8_t>& packet)>;

} // namespace cb::test
