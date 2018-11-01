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

#include <gtest/gtest.h>
#include <mcbp/protocol/framebuilder.h>
#include <mcbp/protocol/response.h>

using namespace cb::mcbp;

static cb::const_byte_buffer buffer(const std::string& value) {
    return cb::const_byte_buffer{(const uint8_t*)value.data(), value.size()};
}

static void compare(const std::string& expected, cb::const_byte_buffer value) {
    std::string provided{(const char*)value.data(), value.size()};
    EXPECT_EQ(expected.size(), value.size());
    EXPECT_EQ(expected, provided);
}

TEST(ResponseBuilder, check_range) {
    std::vector<uint8_t> blob(1024);

    // we need to at least fit a the header
    EXPECT_THROW(FrameBuilder<Response> fb({blob.data(), sizeof(Response) - 1}),
                 std::logic_error);

    EXPECT_NO_THROW(
            FrameBuilder<Response> builder({blob.data(), sizeof(Response)}));

    FrameBuilder<Response> builder({blob.data(), sizeof(Response) + 10});

    // We should be able to add 10 bytes of extras
    builder.setExtras({blob.data(), 10});
    // But not 11!
    EXPECT_THROW(builder.setExtras({blob.data(), 11}), std::logic_error);
    builder.setExtras({blob.data(), 0});

    // same goes for key
    builder.setKey({blob.data(), 10});
    EXPECT_THROW(builder.setKey({blob.data(), 11}), std::logic_error);
    builder.setKey({blob.data(), 0});

    // same goes for value
    builder.setValue({blob.data(), 10});
    EXPECT_THROW(builder.setValue({blob.data(), 11}), std::logic_error);
    builder.setValue({blob.data(), 0});
}

TEST(ResponseBuilder, SetFields) {
    std::vector<uint8_t> backing_store(1024);
    ResponseBuilder builder({backing_store.data(), backing_store.size()});

    auto* response = builder.getFrame();

    // Fill the payload with 'z' to make it easier to detect that it works
    // as expected
    std::fill(backing_store.begin() + sizeof(*response),
              backing_store.end(),
              'z');

    // Insert the data from the end (so that we can check that it is
    // correctly moved...
    const std::string value{"Hello world"};
    builder.setValue(buffer(value));
    EXPECT_EQ(value.size(), response->getBodylen());

    const std::string key{"key"};
    builder.setKey(buffer(key));
    EXPECT_EQ(key.size(), response->getKeylen());
    EXPECT_EQ(value.size() + key.size(), response->getBodylen());

    const std::string extras{"extras"};
    builder.setExtras(buffer(extras));
    EXPECT_EQ(extras.size(), response->getExtlen());
    EXPECT_EQ(key.size(), response->getKeylen());
    EXPECT_EQ(value.size() + key.size() + extras.size(),
              response->getBodylen());

    compare(extras, response->getExtdata());
    compare(key, response->getKey());
    compare(value, response->getValue());

    // nuke the key, and the value should be correct
    builder.setKey({nullptr, 0});
    EXPECT_EQ(0, response->getKeylen());
    EXPECT_EQ(value.size() + extras.size(), response->getBodylen());
    compare(value, response->getValue());

    // nuke the extras.. value should be correct
    builder.setExtras({nullptr, 0});
    EXPECT_EQ(value.size(), response->getBodylen());
    compare(value, response->getValue());
    EXPECT_EQ(backing_store.data() + sizeof(*response),
              response->getValue().data());

    // setStatus should only be available for Response packets
    builder.setStatus(Status::Einval);
    EXPECT_EQ(Status::Einval, Status(response->getStatus()));
}

TEST(RequestBuilder, check_range) {
    std::vector<uint8_t> blob(1024);

    // we need to at least fit a the header
    EXPECT_THROW(
            FrameBuilder<Request> builder({blob.data(), sizeof(Request) - 1}),
            std::logic_error);

    EXPECT_NO_THROW(
            FrameBuilder<Request> builder({blob.data(), sizeof(Request)}));

    RequestBuilder builder({blob.data(), sizeof(Request) + 10});

    // We should be able to add 10 bytes of extras
    builder.setExtras({blob.data(), 10});
    // But not 11!
    EXPECT_THROW(builder.setExtras({blob.data(), 11}), std::logic_error);
    builder.setExtras({blob.data(), 0});

    // same goes for key
    builder.setKey({blob.data(), 10});
    EXPECT_THROW(builder.setKey({blob.data(), 11}), std::logic_error);
    builder.setKey({blob.data(), 0});

    // same goes for value
    builder.setValue({blob.data(), 10});
    EXPECT_THROW(builder.setValue({blob.data(), 11}), std::logic_error);
    builder.setValue({blob.data(), 0});
}

TEST(RequestBuilder, SetFields) {
    std::vector<uint8_t> backing_store(1024);
    FrameBuilder<Request> builder({backing_store.data(), backing_store.size()});
    auto* request = builder.getFrame();

    // Fill the payload with 'z' to make it easier to detect that it works
    // as expected
    std::fill(
            backing_store.begin() + sizeof(*request), backing_store.end(), 'z');

    // Insert the data from the end (so that we can check that it is
    // correctly moved...
    const std::string value{"Hello world"};
    builder.setValue(buffer(value));
    EXPECT_EQ(value.size(), request->getBodylen());

    const std::string key{"key"};
    builder.setKey(buffer(key));
    EXPECT_EQ(key.size(), request->getKeylen());
    EXPECT_EQ(value.size() + key.size(), request->getBodylen());

    const std::string extras{"extras"};
    builder.setExtras(buffer(extras));
    EXPECT_EQ(extras.size(), request->getExtlen());
    EXPECT_EQ(key.size(), request->getKeylen());
    EXPECT_EQ(value.size() + key.size() + extras.size(), request->getBodylen());

    compare(extras, request->getExtdata());
    compare(key, request->getKey());
    compare(value, request->getValue());

    // nuke the key, and the value should be correct
    builder.setKey({nullptr, 0});
    EXPECT_EQ(0, request->getKeylen());
    EXPECT_EQ(value.size() + extras.size(), request->getBodylen());
    compare(value, request->getValue());

    // nuke the extras.. value should be correct
    builder.setExtras({nullptr, 0});
    EXPECT_EQ(value.size(), request->getBodylen());
    compare(value, request->getValue());
    EXPECT_EQ(backing_store.data() + sizeof(*request),
              request->getValue().data());

    // setVBucket should only be available for Request packets
    builder.setVBucket(Vbid(0xfeed));
    EXPECT_EQ(uint16_t(0xfeed), request->getVBucket().get());
}
