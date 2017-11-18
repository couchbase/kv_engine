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

#include <daemon/mcbp.h>
#include "executors.h"

void process_hello_packet_executor(Cookie& cookie) {
    auto& connection = cookie.getConnection();
    auto* req = reinterpret_cast<protocol_binary_request_hello*>(
            cookie.getPacketAsVoidPtr());
    std::string log_buffer;
    log_buffer.reserve(512);
    log_buffer.append("HELO ");

    const cb::const_char_buffer key{
        reinterpret_cast<const char*>(req->bytes + sizeof(req->bytes)),
        ntohs(req->message.header.request.keylen)};

    const cb::sized_buffer<const uint16_t> input{
        reinterpret_cast<const uint16_t*>(key.data() + key.size()),
        (ntohl(req->message.header.request.bodylen) - key.size()) / 2};

    std::vector<uint16_t> out;
    bool tcpdelay_handled = false;

    /*
     * Disable all features the hello packet may enable, so that
     * the client can toggle features on/off during a connection
     */
    connection.disableAllDatatypes();
    connection.setSupportsMutationExtras(false);
    connection.setXerrorSupport(false);
    connection.setCollectionsSupported(false);
    connection.setDuplexSupported(false);
    connection.setClustermapChangeNotificationSupported(false);

    if (!key.empty()) {
        log_buffer.append("[");
        if (key.size() > 256) {
            log_buffer.append(key.data(), 256);
            log_buffer.append("...");
        } else {
            log_buffer.append(key.data(), key.size());
        }
        log_buffer.append("] ");
    }

    for (const auto& value : input) {
        bool added = false;
        const uint16_t in = ntohs(value);
        const auto feature = cb::mcbp::Feature(in);

        switch (feature) {
        case cb::mcbp::Feature::Invalid:
        case cb::mcbp::Feature::TLS:
            /* Not implemented */
            LOG_NOTICE(nullptr,
                       "%u: %s requested unupported feature %s",
                       connection.getId(),
                       connection.getDescription().c_str(),
                       to_string(feature).c_str());
            break;
        case cb::mcbp::Feature::TCPNODELAY:
        case cb::mcbp::Feature::TCPDELAY:
            if (!tcpdelay_handled) {
                connection.setTcpNoDelay(feature ==
                                         cb::mcbp::Feature::TCPNODELAY);
                tcpdelay_handled = true;
                added = true;
            }
            break;

        case cb::mcbp::Feature::MUTATION_SEQNO:
            if (!connection.isSupportsMutationExtras()) {
                connection.setSupportsMutationExtras(true);
                added = true;
            }
            break;
        case cb::mcbp::Feature::XATTR:
            if ((Datatype::isSupported(cb::mcbp::Feature::XATTR) ||
                 connection.isInternal()) &&
                !connection.isXattrEnabled()) {
                connection.enableDatatype(cb::mcbp::Feature::XATTR);
                added = true;
            }
            break;
        case cb::mcbp::Feature::JSON:
            if (Datatype::isSupported(cb::mcbp::Feature::JSON) &&
                !connection.isJsonEnabled()) {
                connection.enableDatatype(cb::mcbp::Feature::JSON);
                added = true;
            }
            break;
        case cb::mcbp::Feature::SNAPPY:
            if (Datatype::isSupported(cb::mcbp::Feature::SNAPPY) &&
                !connection.isSnappyEnabled()) {
                connection.enableDatatype(cb::mcbp::Feature::SNAPPY);
                added = true;
            }
            break;
        case cb::mcbp::Feature::XERROR:
            if (!connection.isXerrorSupport()) {
                connection.setXerrorSupport(true);
                added = true;
            }
            break;
        case cb::mcbp::Feature::SELECT_BUCKET:
            // The select bucket is only informative ;-)
            added = true;
            break;
        case cb::mcbp::Feature::COLLECTIONS:
            if (!connection.isCollectionsSupported()) {
                connection.setCollectionsSupported(true);
                added = true;
            }
            break;
        case cb::mcbp::Feature::Duplex:
            if (!connection.isDuplexSupported()) {
                connection.setDuplexSupported(true);
                added = true;
            }
            break;
        case cb::mcbp::Feature::ClustermapChangeNotification:
            if (!connection.isClustermapChangeNotificationSupported() &&
                connection.isDuplexSupported()) {
                connection.setClustermapChangeNotificationSupported(true);
                added = true;
            }
            break;
        case cb::mcbp::Feature::UnorderedExecution:
            if (!connection.allowUnorderedExecution()) {
                connection.setAllowUnorderedExecution(true);
                added = true;
            }
        }

        if (added) {
            out.push_back(value);
            log_buffer.append(to_string(feature));
            log_buffer.append(", ");
        }
    }

    cookie.sendResponse(
            cb::mcbp::Status::Success,
            {},
            {},
            {reinterpret_cast<const char*>(out.data()), 2 * out.size()},
            cb::mcbp::Datatype::Raw,
            0);

    // Trim off the trailing whitespace (and potentially comma)
    log_buffer.resize(log_buffer.size() - 1);
    if (log_buffer.back() == ',') {
        log_buffer.resize(log_buffer.size() - 1);
    }

    LOG_NOTICE(&connection,
               "%u: %s %s",
               connection.getId(),
               log_buffer.c_str(),
               connection.getDescription().c_str());
}
