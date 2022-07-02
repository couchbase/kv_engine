/*
 *    Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "dcp_drain.h"

#include <fmt/core.h>
#include <folly/io/IOBuf.h>
#include <folly/io/async/AsyncSocketException.h>
#include <serverless/config.h>

namespace cb::test {
DcpDrain::DcpDrain(std::string host,
                   std::string port,
                   std::string username,
                   std::string password,
                   std::string bucketname)
    : host(std::move(host)),
      port(std::move(port)),
      username(std::move(username)),
      password(std::move(password)),
      bucketname(std::move(bucketname)),
      connection(AsyncClientConnection::create(base)) {
    connection->setIoErrorListener(
            [this](AsyncClientConnection::Direction dir,
                   const folly::AsyncSocketException& ex) {
                error = ex.what();
                base.terminateLoopSoon();
            });
}

void DcpDrain::drain() {
    connect();
    // We need to use PLAIN auth as we're using the external auth
    // service
    connection->authenticate(username, password, "PLAIN");
    setFeatures();
    selectBucket();
    openDcp();
    setControlMessages();
    sendStreamRequest();
    connection->setFrameReceivedListener(
            [this](const auto& header) { onFrameReceived(header); });

    // Now loop until we're done
    base.loopForever();
    if (error) {
        throw std::runtime_error(*error);
    }
}

void DcpDrain::onFrameReceived(const cb::mcbp::Header& header) {
    if (header.isRequest()) {
        onRequest(header.getRequest());
    } else {
        onResponse(header.getResponse());
    }
}

void DcpDrain::onResponse(const cb::mcbp::Response& res) {
    if (res.getClientOpcode() == cb::mcbp::ClientOpcode::DcpStreamReq) {
        if (!cb::mcbp::isStatusSuccess(res.getStatus())) {
            error = "onResponse::DcpStreamReq returned error: " +
                    ::to_string(res.getStatus());
            base.terminateLoopSoon();
        }
    } else {
        error = "onResponse(): Unexpected message received: " +
                res.toJSON(false).dump();
        base.terminateLoopSoon();
    }
}

void DcpDrain::onRequest(const cb::mcbp::Request& req) {
    if (req.getClientOpcode() == cb::mcbp::ClientOpcode::DcpStreamEnd) {
        base.terminateLoopSoon();
    }

    switch (req.getClientOpcode()) {
    case cb::mcbp::ClientOpcode::DcpStreamEnd:
        base.terminateLoopSoon();
        break;
    case cb::mcbp::ClientOpcode::DcpNoop:
        handleDcpNoop(req);
        break;
    case cb::mcbp::ClientOpcode::DcpMutation:
        ++num_mutations;
        ru += cb::serverless::Config::instance().to_ru(req.getValue().size() +
                                                       req.getKey().size());
        break;
    case cb::mcbp::ClientOpcode::DcpDeletion:
        ++num_deletions;
        ru += cb::serverless::Config::instance().to_ru(req.getValue().size() +
                                                       req.getKey().size());
        break;
    case cb::mcbp::ClientOpcode::DcpExpiration:
        ++num_expirations;
        ru += cb::serverless::Config::instance().to_ru(req.getValue().size() +
                                                       req.getKey().size());
        break;

    case cb::mcbp::ClientOpcode::DcpSnapshotMarker:
        break;

    case cb::mcbp::ClientOpcode::DcpAddStream:
    case cb::mcbp::ClientOpcode::DcpCloseStream:
    case cb::mcbp::ClientOpcode::DcpStreamReq:
    case cb::mcbp::ClientOpcode::DcpGetFailoverLog:
    case cb::mcbp::ClientOpcode::DcpFlush_Unsupported:
    case cb::mcbp::ClientOpcode::DcpSetVbucketState:
    case cb::mcbp::ClientOpcode::DcpBufferAcknowledgement:
    case cb::mcbp::ClientOpcode::DcpControl:
    case cb::mcbp::ClientOpcode::DcpSystemEvent:
    case cb::mcbp::ClientOpcode::DcpPrepare:
    case cb::mcbp::ClientOpcode::DcpSeqnoAcknowledged:
    case cb::mcbp::ClientOpcode::DcpCommit:
    case cb::mcbp::ClientOpcode::DcpAbort:
    case cb::mcbp::ClientOpcode::DcpSeqnoAdvanced:
    case cb::mcbp::ClientOpcode::DcpOsoSnapshot:
        // fallthrough
    default:
        error = "Received unexpected message: " + req.toJSON(false).dump();
        base.terminateLoopSoon();
    }
}

void DcpDrain::connect() {
    connection->setConnectListener([this]() { base.terminateLoopSoon(); });
    connection->connect(host, port);
    base.loopForever();
    if (error) {
        throw std::runtime_error("DcpDrain::connect: " + *error);
    }
}

void DcpDrain::setFeatures() {
    using cb::mcbp::Feature;

    const std::vector<Feature> requested{{Feature::MUTATION_SEQNO,
                                          Feature::XATTR,
                                          Feature::XERROR,
                                          Feature::SNAPPY,
                                          Feature::JSON,
                                          Feature::Tracing,
                                          Feature::Collections,
                                          Feature::ReportUnitUsage}};

    auto enabled = connection->hello("serverless", "MeterDCP", requested);
    if (enabled != requested) {
        throw std::runtime_error(
                "DcpDrain::setFeatures(): Failed to enable the "
                "requested "
                "features");
    }
}

void DcpDrain::selectBucket() {
    const auto rsp = connection->execute(BinprotGenericCommand{
            cb::mcbp::ClientOpcode::SelectBucket, bucketname});
    if (!rsp.isSuccess()) {
        throw std::runtime_error(
                "DcpDrain::selectBucket: " + ::to_string(rsp.getStatus()) +
                " " + rsp.getDataString());
    }
}

void DcpDrain::openDcp() {
    const auto rsp = connection->execute(BinprotDcpOpenCommand{
            "MeterDcpName", cb::mcbp::request::DcpOpenPayload::Producer});
    if (!rsp.isSuccess()) {
        throw std::runtime_error(
                "DcpDrain::openDcp: " + ::to_string(rsp.getStatus()) + " " +
                rsp.getDataString());
    }
}

void DcpDrain::setControlMessages() {
    auto setCtrlMessage = [this](const std::string& key,
                                 const std::string& value) {
        const auto rsp = connection->execute(BinprotGenericCommand{
                cb::mcbp::ClientOpcode::DcpControl, key, value});
        if (!rsp.isSuccess()) {
            throw std::runtime_error(
                    fmt::format(R"(setControlMessages("{}", "{}") failed: {})",
                                key,
                                value,
                                ::to_string(rsp.getStatus())));
        }
    };
    std::vector<std::pair<std::string, std::string>> controls{
            {"set_priority", "high"},
            {"supports_cursor_dropping_vulcan", "true"},
            {"supports_hifi_MFU", "true"},
            {"send_stream_end_on_client_close_stream", "true"},
            {"enable_expiry_opcode", "true"},
            {"set_noop_interval", "1"},
            {"enable_noop", "true"}};
    for (const auto& [k, v] : controls) {
        setCtrlMessage(k, v);
    }
}

void DcpDrain::sendStreamRequest() {
    BinprotDcpStreamRequestCommand cmd;
    cmd.setDcpFlags(DCP_ADD_STREAM_FLAG_TO_LATEST);
    cmd.setDcpReserved(0);
    cmd.setDcpStartSeqno(0);
    cmd.setDcpEndSeqno(~0);
    cmd.setDcpVbucketUuid(0);
    cmd.setDcpSnapStartSeqno(0);
    cmd.setDcpSnapEndSeqno(0);
    cmd.setVBucket(Vbid(0));

    connection->send(cmd);
}

void DcpDrain::handleDcpNoop(const cb::mcbp::Request& header) {
    cb::mcbp::Response resp = {};
    resp.setMagic(cb::mcbp::Magic::ClientResponse);
    resp.setOpaque(header.getOpaque());
    resp.setOpcode(header.getClientOpcode());

    auto iob = folly::IOBuf::createCombined(sizeof(resp));
    std::memcpy(iob->writableData(), &resp, sizeof(resp));
    iob->append(sizeof(resp));
    connection->send(std::move(iob));
}

} // namespace cb::test
