/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

#include "conn_store_test.h"

#include "../mock/mock_conn_store.h"
#include "../mock/mock_synchronous_ep_engine.h"
#include "conn_store.h"
#include "dcp/consumer.h"
#include "programs/engine_testapp/mock_cookie.h"
#include "programs/engine_testapp/mock_server.h"

#include <memory>

void ConnStoreTest::SetUp() {
    SingleThreadedKVBucketTest::SetUp();
    connStore = std::make_unique<MockConnStore>(*engine.get());
}

void ConnStoreTest::TearDown() {
    // Need to destroy our connStore before the engine
    connStore.reset();
    SingleThreadedKVBucketTest::TearDown();
}

std::shared_ptr<ConnHandler> ConnStoreTest::addConnHandler(
        const void* cookie, const std::string& name) {
    // ConnHandler is abstract, so just use a DcpConsumer
    auto consumer = std::make_shared<DcpConsumer>(
            *engine, cookie, name, "" /* consumerName*/);
    auto handle = connStore->getCookieToConnectionMapHandle();
    handle->addConnByCookie(cookie, consumer);
    return consumer;
}

void ConnStoreTest::removeConnHandler(const void* cookie) {
    auto handle = connStore->getCookieToConnectionMapHandle();
    int startSize = handle->copyCookieToConn().size();
    handle->removeConnByCookie(cookie);
    auto max = std::max(0, startSize - 1);
    ASSERT_EQ(max, handle->copyCookieToConn().size());
}

void ConnStoreTest::addVbConn(Vbid vb, ConnHandler& conn) {
    // We are assuming that the cookie and vbid are valid
    auto& map = connStore->getVBToConnsMap();
    auto& list = map[vb.get()];
    auto listSize = list.size();

    auto itr = connStore->getVBToConnsItr(vb, conn);
    auto refCount = 0;
    if (itr != list.end()) {
        refCount = itr->refCount;
    }

    ASSERT_NO_THROW(connStore->addVBConnByVbid(vb, conn));

    itr = connStore->getVBToConnsItr(vb, conn);
    ASSERT_NE(itr, list.end());
    ASSERT_EQ(refCount + 1, itr->refCount);

    if (listSize == 0) {
        ASSERT_EQ(listSize + 1, list.size());

        // We'll put the new connection at the back of the list so grab the last
        // one And the cookie should match
        ASSERT_EQ(conn.getCookie(), list.back().connHandler.getCookie());
    }
}

void ConnStoreTest::removeVbConn(Vbid vb, const void* cookie) {
    // We are assuming that the cookie and vbid are valid
    auto& map = connStore->getVBToConnsMap();
    auto& list = map[vb.get()];

    // Check beforehand if we should delete anything
    auto itr = std::find_if(
            list.begin(), list.end(), [cookie](ConnStore::VBConn conn) {
                return cookie == conn.connHandler.getCookie();
            });

    int expectedSize = list.size();
    if (itr != list.end()) {
        expectedSize = std::max(0, expectedSize - 1);
    }

    ASSERT_NO_THROW(connStore->removeVBConnByVbid(vb, cookie));

    ASSERT_EQ(expectedSize, list.size());

    // Check that we removed the element for this cookie
    itr = std::find_if(
            list.begin(), list.end(), [cookie](ConnStore::VBConn conn) {
                return cookie == conn.connHandler.getCookie();
            });

    ASSERT_EQ(list.end(), itr);
}

TEST_F(ConnStoreTest, AddConnHandler) {
    addConnHandler(cookie, "consumer");

    // We should now find our element, and the DcpConsumer should still be valid
    auto map = connStore->getCookieToConnectionMapHandle()->copyCookieToConn();
    EXPECT_EQ(1, map.size());
    EXPECT_NE(nullptr, map[cookie].get());
    EXPECT_EQ("consumer", map[cookie].get()->getName());
}

TEST_F(ConnStoreTest, RemoveConnHandler) {
    addConnHandler(cookie, "consumer");
    removeConnHandler(cookie);

    auto map = connStore->getCookieToConnectionMapHandle()->copyCookieToConn();
    EXPECT_EQ(nullptr, map[cookie].get());
}

// Failure case - we should not be able to add a connection to a vBucket that
// should not exist in the map
TEST_F(ConnStoreTest, AddVBConnInvalidVbid) {
    auto consumer = addConnHandler(cookie, "consumer");
    EXPECT_THROW(connStore->addVBConnByVbid(Vbid(-1), *consumer.get()),
                 std::out_of_range);
    EXPECT_THROW(connStore->addVBConnByVbid(
                         Vbid(engine->getConfiguration().getMaxVbuckets() + 1),
                         *consumer.get()),
                 std::out_of_range);
}

TEST_F(ConnStoreTest, AddVbConnValid) {
    auto consumer = addConnHandler(cookie, "consumer");
    addVbConn(Vbid(0), *consumer.get());
}

TEST_F(ConnStoreTest, AddMultipleVbConnsOneConnHandler) {
    auto& map = connStore->getVBToConnsMap();
    // vbToConns should be empty
    for (const auto& list : map) {
        ASSERT_EQ(0, list.size());
    }

    auto consumer = addConnHandler(cookie, "consumer");

    Vbid vb(0);
    addVbConn(vb, *consumer.get());

    for (size_t i = 0; i < map.size(); i++) {
        auto list = map[i];
        if (i == vb.get()) {
            EXPECT_EQ(1, list.size());
        } else {
            EXPECT_EQ(0, list.size());
        }
    }

    connStore->addVBConnByVbid(vb, *consumer.get());

    // Don't add duplicates
    for (size_t i = 0; i < map.size(); i++) {
        auto list = map[i];
        if (i == vb.get()) {
            EXPECT_EQ(1, list.size());
        } else {
            EXPECT_EQ(0, list.size());
        }
    }
}

// Failure case - we should not be able to remove a connection for a vBucket
// that should not exist in the map
TEST_F(ConnStoreTest, RemoveVBConnInvalidVbid) {
    EXPECT_THROW(connStore->removeVBConnByVbid(Vbid(-1), cookie),
                 std::out_of_range);
    EXPECT_THROW(connStore->removeVBConnByVbid(
                         Vbid(engine->getConfiguration().getMaxVbuckets() + 1),
                         cookie),
                 std::out_of_range);
}

// It's okay to attempt to remove an invalid cookie. Should be a no-op
TEST_F(ConnStoreTest, RemoveVbConnInvalidCookie) {
    Vbid vb(0);
    removeVbConn(vb, cookie);
}

TEST_F(ConnStoreTest, RemoveVbConnValid) {
    Vbid vb(0);
    auto consumer = addConnHandler(cookie, "consumer");
    addVbConn(vb, *consumer.get());
    removeVbConn(vb, cookie);
}

// Test that we remove all vbConns for the given ConnHandler if we remove the
// ConnHandler
TEST_F(ConnStoreTest, RemoveConnHandlerWithVbConns) {
    auto& map = connStore->getVBToConnsMap();
    // vbToConns should be empty
    for (const auto& list : map) {
        ASSERT_EQ(0, list.size());
    }

    auto consumer = addConnHandler(cookie, "consumer");

    for (size_t i = 0; i < map.size(); i++) {
        Vbid vb(i);
        addVbConn(vb, *consumer.get());
    }

    for (const auto& list : map) {
        EXPECT_EQ(1, list.size());
    }

    removeConnHandler(cookie);

    // We should have removed the ConnHandler from all lists in vbToConns
    // map
    for (const auto& list : map) {
        EXPECT_EQ(0, list.size());
    }
}

// Test that we don't mess up other ConnHandlers when removing one
TEST_F(ConnStoreTest, RemoveOneConnHandlerWithVbConns) {
    auto& map = connStore->getVBToConnsMap();
    // vbToConns should be empty
    for (const auto& list : map) {
        ASSERT_EQ(0, list.size());
    }

    auto consumer1 = addConnHandler(cookie, "consumer1");

    // Add the vbConns
    for (size_t i = 0; i < map.size(); i++) {
        Vbid vb(i);
        addVbConn(vb, *consumer1.get());
    }

    // Check we added something
    for (const auto& list : map) {
        EXPECT_EQ(1, list.size());
    }

    auto cookie2 = create_mock_cookie();
    auto consumer2 = addConnHandler(cookie2, "consumer2");

    // Add the vbConns
    for (size_t i = 0; i < map.size(); i++) {
        Vbid vb(i);
        addVbConn(vb, *consumer2.get());
    }

    // Check we added something
    for (const auto& list : map) {
        EXPECT_EQ(2, list.size());
    }

    // Remove consumer1
    removeConnHandler(cookie);

    // We should have removed the ConnHandler for cookie1
    for (const auto& list : map) {
        EXPECT_EQ(1, list.size());
        EXPECT_EQ(consumer2->getCookie(), list.front().connHandler.getCookie());
    }

    destroy_mock_cookie(cookie2);
}

TEST_F(ConnStoreTest, FindConnHandlerByCookie) {
    {
        auto handle = connStore->getCookieToConnectionMapHandle();
        auto res = handle->findConnHandlerByCookie(cookie);
        EXPECT_EQ(nullptr, res.get());
    }

    addConnHandler(cookie, "consumer");

    {
        auto handle = connStore->getCookieToConnectionMapHandle();
        auto res = handle->findConnHandlerByCookie(cookie);
        EXPECT_NE(nullptr, res.get());
        EXPECT_EQ(cookie, res->getCookie());
        auto consumer = std::dynamic_pointer_cast<DcpConsumer>(res);
        EXPECT_NE(nullptr, consumer);
    }

    removeConnHandler(cookie);

    {
        auto handle = connStore->getCookieToConnectionMapHandle();
        auto res = handle->findConnHandlerByCookie(cookie);
        EXPECT_EQ(nullptr, res.get());
    }
}

// Shouldn't be able to add a second ConnHandler for a given cookie or overwrite
// one that is already in the map
TEST_F(ConnStoreTest, AddSecondConnHandlerForCookie) {
    addConnHandler(cookie, "consumer1");

    std::shared_ptr<ConnHandler> consumer1;
    {
        auto handle = connStore->getCookieToConnectionMapHandle();
        consumer1 = handle->findConnHandlerByCookie(cookie);
    }

    EXPECT_THROW(addConnHandler(cookie, "consumer2"), std::runtime_error);
    {
        auto handle = connStore->getCookieToConnectionMapHandle();
        auto consumer2 = handle->findConnHandlerByCookie(cookie);
        // We should not have made any change to cookieToConn
        EXPECT_EQ(consumer1.get(), consumer2.get());
    }
}

TEST_F(ConnStoreTest, FindConnHandlerByName) {
    {
        auto handle = connStore->getCookieToConnectionMapHandle();
        auto res = handle->findConnHandlerByName("consumer");
        EXPECT_EQ(nullptr, res.get());
    }

    addConnHandler(cookie, "consumer");

    {
        auto handle = connStore->getCookieToConnectionMapHandle();
        auto res = handle->findConnHandlerByName("consumer");
        EXPECT_NE(nullptr, res.get());
        EXPECT_EQ("consumer", res->getName());
    }

    removeConnHandler(cookie);

    {
        auto handle = connStore->getCookieToConnectionMapHandle();
        auto res = handle->findConnHandlerByName("consumer");
        EXPECT_EQ(nullptr, res.get());
    }
}
