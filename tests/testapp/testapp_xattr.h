/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include "testapp_client_test.h"
#include "xattr/blob.h"

#include <platform/cb_malloc.h>

/**
 * Text fixture for XAttr tests which do not want to have an initial document
 * created.
 */
class XattrNoDocTest : public TestappXattrClientTest {
protected:
    void SetUp() override {
        TestappXattrClientTest::SetUp();
    }

    BinprotSubdocResponse subdoc_get(const std::string& path,
                                     cb::mcbp::subdoc::PathFlag flag = {},
                                     cb::mcbp::subdoc::DocFlag docFlag =
                                             cb::mcbp::subdoc::DocFlag::None) {
        return subdoc(cb::mcbp::ClientOpcode::SubdocGet,
                      name,
                      path,
                      {},
                      flag,
                      docFlag);
    }

    BinprotSubdocMultiLookupResponse subdoc_multi_lookup(
            std::vector<BinprotSubdocMultiLookupCommand::LookupSpecifier> specs,
            cb::mcbp::subdoc::DocFlag docFlags =
                    cb::mcbp::subdoc::DocFlag::None);

    BinprotSubdocMultiMutationResponse subdoc_multi_mutation(
            std::vector<BinprotSubdocMultiMutationCommand::MutationSpecifier>
                    specs,
            cb::mcbp::subdoc::DocFlag docFlags =
                    cb::mcbp::subdoc::DocFlag::None);

    GetMetaResponse get_meta();

    /// Constructs a Subdoc multi-mutation matching the style of an SDK
    // Transactions mutation.
    BinprotSubdocMultiMutationCommand makeSDKTxnMultiMutation() const;

    void testRequiresMkdocOrAdd();
    void testRequiresXattrPath();
    void testSinglePathDictAdd();
    void testMultipathDictAdd();
    void testMultipathDictUpsert();
    void testMultipathArrayPushLast();
    void testMultipathArrayPushFirst();
    void testMultipathArrayAddUnique();
    void testMultipathCounter();
    void testMultipathCombo();
    void testMultipathAccessDeletedCreateAsDeleted();

    std::optional<cb::durability::Requirements> durReqs = {};
};

class XattrNoDocDurabilityTest : public XattrNoDocTest {
protected:
    void SetUp() override {
        XattrNoDocTest::SetUp();
        // level:majority, timeout:default
        durReqs = cb::durability::Requirements();
    }
};
