/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
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
