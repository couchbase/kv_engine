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

#include <folly/portability/GTest.h>
#include <mcbp/protocol/framebuilder.h>
#include <mcbp/protocol/response.h>

using namespace cb::mcbp;

static cb::const_byte_buffer buffer(const std::string& value) {
    return cb::const_byte_buffer{(const uint8_t*)value.data(), value.size()};
}

static void compare(const std::string& expected, cb::const_byte_buffer value) {
    std::string provided{(const char*)value.data(), value.size()};
    ASSERT_EQ(expected.size(), value.size());
    ASSERT_EQ(expected, provided);
}

TEST(ResponseBuilder, check_range) {
    std::vector<uint8_t> blob(1024);

    // we need to at least fit a the header
    EXPECT_THROW(FrameBuilder<Response> fb({blob.data(), sizeof(Response) - 1}),
                 std::logic_error);

    EXPECT_NO_THROW(
            FrameBuilder<Response> builder({blob.data(), sizeof(Response)}));

    FrameBuilder<Response> builder({blob.data(), sizeof(Response) + 10});
    builder.setMagic(Magic::AltClientResponse);

    // We should be able to add 10 bytes of frame extras
    builder.setFramingExtras({blob.data(), 10});
    // But not 11!
    EXPECT_THROW(builder.setFramingExtras({blob.data(), 11}), std::logic_error);
    builder.setFramingExtras({blob.data(), 0});

    // Same goes for extras
    builder.setExtras({blob.data(), 10});
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
    const std::string value{"Hello world"};
    const std::string key{"key"};
    const std::string extras{"extras"};
    const std::string frame_extras{"frame extras"};
    const auto expected_size = sizeof(Request) + frame_extras.size() +
                               extras.size() + key.size() + value.size();

    std::vector<uint8_t> backing_store(1024);
    ResponseBuilder builder({backing_store.data(), backing_store.size()});
    builder.setMagic(Magic::AltClientResponse);

    auto* response = builder.getFrame();

    // Fill the payload with 'z' to make it easier to detect that it works
    // as expected
    std::fill(backing_store.begin() + sizeof(*response),
              backing_store.end(),
              'z');

    // Insert the data from the end (so that we can check that it is
    // correctly moved...
    builder.setValue(buffer(value));
    ASSERT_EQ(value.size(), response->getBodylen());
    ASSERT_EQ(value.back(), backing_store[sizeof(Request) + value.size() - 1]);

    builder.setKey(buffer(key));
    ASSERT_EQ(key.size(), response->getKeylen());
    ASSERT_EQ(value.size() + key.size(), response->getBodylen());
    ASSERT_EQ(value.back(),
              backing_store[sizeof(Request) + value.size() + key.size() - 1]);

    builder.setExtras(buffer(extras));
    ASSERT_EQ(extras.size(), response->getExtlen());
    ASSERT_EQ(key.size(), response->getKeylen());
    ASSERT_EQ(value.size() + key.size() + extras.size(),
              response->getBodylen());
    ASSERT_EQ(value.back(),
              backing_store[sizeof(Request) + value.size() + key.size() +
                            extras.size() - 1]);

    builder.setFramingExtras(buffer(frame_extras));
    ASSERT_EQ(frame_extras.size(), response->getFramingExtraslen());
    ASSERT_EQ(extras.size(), response->getExtlen());
    ASSERT_EQ(key.size(), response->getKeylen());
    ASSERT_EQ(value.size() + key.size() + extras.size() + frame_extras.size(),
              response->getBodylen());

    ASSERT_EQ(expected_size, builder.getFrame()->getFrame().size());
    ASSERT_EQ('z', backing_store[expected_size]);
    ASSERT_EQ(value.back(), backing_store[expected_size - 1]);

    compare(frame_extras, response->getFramingExtras());
    compare(extras, response->getExtdata());
    compare(key, response->getKey());
    compare(value, response->getValue());

    // nuke the key, and the value should be correct
    builder.setKey(std::string_view{nullptr, 0});
    ASSERT_EQ(0, response->getKeylen());
    ASSERT_EQ(value.size() + extras.size() + frame_extras.size(),
              response->getBodylen());
    compare(value, response->getValue());

    // Nuke the frame extras, value and extras should be correct
    builder.setFramingExtras({nullptr, 0});
    compare(extras, response->getExtdata());
    compare(value, response->getValue());

    // nuke the extras.. value should be correct
    builder.setExtras(cb::const_byte_buffer{});
    ASSERT_EQ(value.size(), response->getBodylen());
    compare(value, response->getValue());
    ASSERT_EQ(backing_store.data() + sizeof(*response),
              response->getValue().data());

    // setStatus should only be available for Response packets
    builder.setStatus(Status::Einval);
    ASSERT_EQ(Status::Einval, Status(response->getStatus()));
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
    builder.setMagic(Magic::AltClientRequest);

    // We should be able to add 10 bytes of frame extras
    builder.setFramingExtras({blob.data(), 10});
    // But not 11!
    EXPECT_THROW(builder.setFramingExtras({blob.data(), 11}), std::logic_error);
    builder.setFramingExtras({blob.data(), 0});

    // Same goes for extras
    builder.setExtras({blob.data(), 10});
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
    builder.setMagic(Magic::AltClientRequest);
    auto* request = builder.getFrame();

    // Fill the payload with 'z' to make it easier to detect that it works
    // as expected
    std::fill(
            backing_store.begin() + sizeof(*request), backing_store.end(), 'z');

    // Insert the data from the end (so that we can check that it is
    // correctly moved...
    const std::string value{"Hello world"};
    builder.setValue(buffer(value));
    ASSERT_EQ(value.size(), request->getBodylen());

    const std::string key{"key"};
    builder.setKey(buffer(key));
    ASSERT_EQ(key.size(), request->getKeylen());
    ASSERT_EQ(value.size() + key.size(), request->getBodylen());

    const std::string extras{"extras"};
    builder.setExtras(buffer(extras));
    ASSERT_EQ(extras.size(), request->getExtlen());
    ASSERT_EQ(key.size(), request->getKeylen());
    ASSERT_EQ(value.size() + key.size() + extras.size(), request->getBodylen());

    const std::string frame_extras{"frame extras"};
    builder.setFramingExtras(buffer(frame_extras));
    ASSERT_EQ(frame_extras.size(), request->getFramingExtraslen());
    ASSERT_EQ(extras.size(), request->getExtlen());
    ASSERT_EQ(key.size(), request->getKeylen());
    ASSERT_EQ(value.size() + key.size() + extras.size() + frame_extras.size(),
              request->getBodylen());

    compare(frame_extras, request->getFramingExtras());
    compare(extras, request->getExtdata());
    compare(key, request->getKey());
    compare(value, request->getValue());

    // nuke the key, and the value should be correct
    builder.setKey(std::string_view{nullptr, 0});
    ASSERT_EQ(0, request->getKeylen());
    ASSERT_EQ(value.size() + extras.size() + request->getFramingExtraslen(),
              request->getBodylen());
    compare(value, request->getValue());

    // Nuke the frame extras, value and extras should be correct
    builder.setFramingExtras({nullptr, 0});
    compare(extras, request->getExtdata());
    compare(value, request->getValue());

    // nuke the extras.. value should be correct
    builder.setExtras(cb::const_byte_buffer{});
    ASSERT_EQ(value.size(), request->getBodylen());
    compare(value, request->getValue());
    ASSERT_EQ(backing_store.data() + sizeof(*request),
              request->getValue().data());

    // setVBucket should only be available for Request packets
    builder.setVBucket(Vbid(0xfeed));
    ASSERT_EQ(uint16_t(0xfeed), request->getVBucket().get());
}
