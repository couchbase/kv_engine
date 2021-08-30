/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "testapp_client_test.h"
#include <protocol/connection/frameinfo.h>
#include <xattr/blob.h>

void TestappXattrClientTest::SetUpTestCase() {
    TestappTest::SetUpTestCase();
    createUserConnection = true;
}

bool TestappClientTest::isTlsEnabled() const {
    switch (GetParam()) {
    case TransportProtocols::McbpPlain:
        return false;
    case TransportProtocols::McbpSsl:
        return true;
    }
    throw std::logic_error("isTlsEnabled(): unknown transport");
}

void TestappXattrClientTest::setBodyAndXattr(
        MemcachedConnection& connection,
        const std::string& startValue,
        std::initializer_list<std::pair<std::string, std::string>> xattrList,
        bool compressValue) {
    document.info.flags = 0xcaffee;
    document.info.id = name;

    if (mcd_env->getTestBucket().supportsOp(
                cb::mcbp::ClientOpcode::SetWithMeta)) {
        // Combine the body and Extended Attribute into a single value -
        // this allows us to store already compressed documents which
        // have XATTRs.
        cb::xattr::Blob xattrs;
        for (auto& kv : xattrList) {
            xattrs.set(kv.first, kv.second);
        }
        auto encoded = xattrs.finalize();
        ASSERT_TRUE(cb::xattr::validate(encoded)) << "Invalid xattr encoding";
        document.info.cas = 10; // withMeta requires a non-zero CAS.
        document.info.datatype = cb::mcbp::Datatype::Xattr;
        document.value = encoded;
        document.value.append(startValue);
        if (compressValue) {
            // Compress the complete body.
            document.compress();
        }

        // As we are using setWithMeta; we need to explicitly set JSON
        // if our connection supports it.
        if (hasJSONSupport() == ClientJSONSupport::Yes) {
            document.info.datatype =
                    cb::mcbp::Datatype(int(document.info.datatype) |
                                       int(cb::mcbp::Datatype::JSON));
        }
        connection.mutateWithMeta(document,
                                  Vbid(0),
                                  mcbp::cas::Wildcard,
                                  /*seqno*/ 1,
                                  FORCE_WITH_META_OP | REGENERATE_CAS |
                                          SKIP_CONFLICT_RESOLUTION_FLAG);
    } else {
        // No SetWithMeta support, must construct the
        // document+XATTR with primitives (and cannot compress
        // it).
        document.info.cas = mcbp::cas::Wildcard;
        document.info.datatype = cb::mcbp::Datatype::Raw;
        document.value = startValue;
        connection.mutate(document, Vbid(0), MutationType::Set);
        auto doc = connection.get(name, Vbid(0));

        EXPECT_EQ(doc.value, document.value);

        // Now add the XATTRs
        for (auto& kv : xattrList) {
            xattr_upsert(connection, kv.first, kv.second);
        }
    }
}

void TestappXattrClientTest::setBodyAndXattr(
        MemcachedConnection& connection,
        const std::string& value,
        std::initializer_list<std::pair<std::string, std::string>> xattrList) {
    setBodyAndXattr(connection,
                    value,
                    xattrList,
                    hasSnappySupport() == ClientSnappySupport::Yes);
}

void TestappXattrClientTest::setClusterSessionToken(uint64_t nval) {
    const auto response = adminConnection->execute(
            BinprotSetControlTokenCommand{nval, token});

    if (!response.isSuccess()) {
        throw ConnectionError("TestappClientTest::setClusterSessionToken",
                              response);
    }
    ASSERT_EQ(nval, response.getCas());
    token = nval;
}

BinprotSubdocResponse TestappXattrClientTest::subdoc(
        MemcachedConnection& conn,
        cb::mcbp::ClientOpcode opcode,
        const std::string& key,
        const std::string& path,
        const std::string& value,
        protocol_binary_subdoc_flag flag,
        mcbp::subdoc::doc_flag docFlag,
        const std::optional<cb::durability::Requirements>& durReqs) {
    BinprotSubdocCommand cmd;
    cmd.setOp(opcode);
    cmd.setKey(key);
    cmd.setPath(path);
    cmd.setValue(value);
    cmd.addPathFlags(flag);
    cmd.addDocFlags(docFlag);

    if (durReqs) {
        cmd.addFrameInfo(DurabilityFrameInfo(durReqs->getLevel(),
                                             durReqs->getTimeout()));
    }

    conn.sendCommand(cmd);

    BinprotSubdocResponse resp;
    conn.recvResponse(resp);

    return resp;
}

BinprotSubdocResponse TestappXattrClientTest::subdocMultiMutation(
        MemcachedConnection& conn, BinprotSubdocMultiMutationCommand cmd) {
    conn.sendCommand(cmd);
    BinprotSubdocResponse resp;
    conn.recvResponse(resp);
    return resp;
}

cb::mcbp::Status TestappXattrClientTest::xattr_upsert(
        MemcachedConnection& conn,
        const std::string& path,
        const std::string& value) {
    auto resp = subdoc(conn,
                       cb::mcbp::ClientOpcode::SubdocDictUpsert,
                       name,
                       path,
                       value,
                       SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_MKDIR_P,
                       mcbp::subdoc::doc_flag::Mkdoc);
    return resp.getStatus();
}

void TestappXattrClientTest::SetUp() {
    TestappTest::SetUp();

    mcd_env->getTestBucket().setXattrEnabled(
            *adminConnection,
            bucketName,
            ::testing::get<1>(GetParam()) == XattrSupport::Yes);
    if (::testing::get<1>(GetParam()) == XattrSupport::No) {
        xattrOperationStatus = cb::mcbp::Status::NotSupported;
    }

    document.info.cas = mcbp::cas::Wildcard;
    document.info.flags = 0xcaffee;
    document.info.id = name;
    document.info.expiration = 0;
    document.value = memcached_cfg.dump();

    // If the client has Snappy support, enable passive compression
    // on the bucket and compress our initial document we work with.
    if (hasSnappySupport() == ClientSnappySupport::Yes) {
        setCompressionMode("passive");
        document.compress();
    }

    setMinCompressionRatio(0);
}

void TestappXattrClientTest::createXattr(MemcachedConnection& conn,
                                         const std::string& path,
                                         const std::string& value,
                                         bool macro) {
    runCreateXattr(conn, path, value, macro, xattrOperationStatus);
}

bool TestappXattrClientTest::isTlsEnabled() const {
    switch (::testing::get<0>(GetParam())) {
    case TransportProtocols::McbpPlain:
        return false;
    case TransportProtocols::McbpSsl:
        return true;
    }
    throw std::logic_error("isTlsEnabled(): Unknown transport");
}

ClientJSONSupport TestappXattrClientTest::hasJSONSupport() const {
    return ::testing::get<2>(GetParam());
}

ClientSnappySupport TestappXattrClientTest::hasSnappySupport() const {
    return ::testing::get<3>(GetParam());
}

cb::mcbp::Datatype TestappXattrClientTest::expectedJSONDatatype() const {
    return hasJSONSupport() == ClientJSONSupport::Yes ? cb::mcbp::Datatype::JSON
                                                      : cb::mcbp::Datatype::Raw;
}

cb::mcbp::Datatype TestappXattrClientTest::expectedJSONSnappyDatatype() const {
    cb::mcbp::Datatype datatype = expectedJSONDatatype();
    if (hasSnappySupport() == ClientSnappySupport::Yes) {
        datatype = cb::mcbp::Datatype(int(datatype) |
                                      int(cb::mcbp::Datatype::Snappy));
    }
    return datatype;
}

/**
 * Helper function to check datatype is what we expect for this test config,
 * and if datatype says JSON; validate the value /is/ JSON.
 */
::testing::AssertionResult TestappXattrClientTest::hasCorrectDatatype(
        const Document& doc, cb::mcbp::Datatype expectedType) {
    return hasCorrectDatatype(expectedType,
                              doc.info.datatype,
                              {doc.value.data(), doc.value.size()});
}

::testing::AssertionResult TestappXattrClientTest::hasCorrectDatatype(
        cb::mcbp::Datatype expectedType,
        cb::mcbp::Datatype actualDatatype,
        std::string_view value) {
    using namespace mcbp::datatype;
    if (actualDatatype != expectedType) {
        return ::testing::AssertionFailure()
               << "Datatype mismatch - expected:"
               << to_string(protocol_binary_datatype_t(expectedType))
               << " actual:"
               << to_string(protocol_binary_datatype_t(actualDatatype));
    }

    if (actualDatatype == cb::mcbp::Datatype::JSON) {
        if (!isJSON(value)) {
            return ::testing::AssertionFailure()
                   << "JSON validation failed for response data:'" << value
                   << "''";
        }
    }
    return ::testing::AssertionSuccess();
}

void TestappXattrClientTest::runCreateXattr(MemcachedConnection& connection,
                                            std::string path,
                                            std::string value,
                                            bool macro,
                                            cb::mcbp::Status expectedStatus) {
    BinprotSubdocCommand cmd;
    cmd.setOp(cb::mcbp::ClientOpcode::SubdocDictAdd);
    cmd.setKey(name);
    cmd.setPath(std::move(path));
    cmd.setValue(std::move(value));
    if (macro) {
        cmd.addPathFlags(SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_EXPAND_MACROS |
                         SUBDOC_FLAG_MKDIR_P);
    } else {
        cmd.addPathFlags(SUBDOC_FLAG_XATTR_PATH | SUBDOC_FLAG_MKDIR_P);
    }

    connection.sendCommand(cmd);

    BinprotResponse resp;
    connection.recvResponse(resp);
    EXPECT_EQ(expectedStatus, resp.getStatus());
}

BinprotSubdocResponse TestappXattrClientTest::runGetXattr(
        MemcachedConnection& connection,
        std::string path,
        bool deleted,
        cb::mcbp::Status expectedStatus) {
    BinprotSubdocCommand cmd;
    cmd.setOp(cb::mcbp::ClientOpcode::SubdocGet);
    cmd.setKey(name);
    cmd.setPath(std::move(path));
    if (deleted) {
        cmd.addPathFlags(SUBDOC_FLAG_XATTR_PATH);
        cmd.addDocFlags(mcbp::subdoc::doc_flag::AccessDeleted);
    } else {
        cmd.addPathFlags(SUBDOC_FLAG_XATTR_PATH);
    }
    connection.sendCommand(cmd);

    BinprotSubdocResponse resp;
    connection.recvResponse(resp);
    auto status = resp.getStatus();
    if (deleted && status == cb::mcbp::Status::SubdocSuccessDeleted) {
        status = cb::mcbp::Status::Success;
    }

    if (status != expectedStatus) {
        throw ConnectionError("runGetXattr() failed: ", resp);
    }
    return resp;
}

BinprotSubdocResponse TestappXattrClientTest::getXattr(
        MemcachedConnection& conn, const std::string& path, bool deleted) {
    return runGetXattr(conn, path, deleted, xattrOperationStatus);
}

std::ostream& operator<<(std::ostream& os, const XattrSupport& xattrSupport) {
    os << to_string(xattrSupport);
    return os;
}

std::string to_string(const XattrSupport& xattrSupport) {
    switch (xattrSupport) {
    case XattrSupport::Yes:
        return "XattrYes";
    case XattrSupport::No:
        return "XattrNo";
    }
    throw std::logic_error("Unknown xattr support");
}

std::string PrintToStringCombinedName::operator()(
        const ::testing::TestParamInfo<::testing::tuple<TransportProtocols,
                                                        XattrSupport,
                                                        ClientJSONSupport,
                                                        ClientSnappySupport>>&
                info) const {
    std::string rv = to_string(::testing::get<0>(info.param)) + "_" +
                     to_string(::testing::get<1>(info.param)) + "_" +
                     to_string(::testing::get<2>(info.param)) + "_" +
                     to_string(::testing::get<3>(info.param));
    return rv;
}
