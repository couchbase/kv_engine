/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

#include "config.h"

#include "unique_tagged_ptr.h"

#include <gtest/gtest.h>

/*
 * Unit tests for the UniqueTaggedPtr class.
 */

class TestObject {
public:
    TestObject() : data(123) {
    }

    uint32_t getData() {
        return data;
    }

private:
    uint32_t data;
};

/// Custom deleter for TestObject objects.
struct Deleter {
    void operator()(TestObject* val) {
        // No additional work required
    }
};

/// Owning pointer type for TestObject objects.
using UniquePtr = UniqueTaggedPtr<TestObject, Deleter>;

/// Test constructor taking object
TEST(UniqueTaggedPtrTest, constructorObjectTest) {
    TestObject to;
    UniquePtr taggedPtr(&to);
    ASSERT_EQ(&to, taggedPtr.get());
}

/// Test constructor taking object and tag
TEST(UniqueTaggedPtrTest, constructorObjectAndTagTest) {
    TestObject to;
    UniquePtr taggedPtr(&to, 456);
    EXPECT_EQ(&to, taggedPtr.get());
    EXPECT_EQ(456, taggedPtr.getTag());
}

/// Test for move construction
TEST(UniqueTaggedPtrTest, moveConstructor) {
    TestObject to;
    UniquePtr taggedPtr1(&to, 456);
    UniquePtr taggedPtr2(std::move(taggedPtr1));
    EXPECT_EQ(&to, taggedPtr2.get());
    EXPECT_EQ(456, taggedPtr2.getTag());
}

/// Test equal operator of UniqueTaggedPtr
TEST(UniqueTaggedPtrTest, equalTest) {
    TestObject to;
    UniquePtr taggedPtr(&to);
    EXPECT_TRUE(taggedPtr.get() == &to);
}

/// Test not equal operator of UniqueTaggedPtr
TEST(UniqueTaggedPtrTest, notEqualTest) {
    TestObject to;
    TestObject to2;
    UniquePtr taggedPtr(&to);
    EXPECT_TRUE(taggedPtr.get() != &to2);
}

/// Test boolean operator of UniqueTaggedPtr - True case
TEST(UniqueTaggedPtrTest, boolTrueTest) {
    TestObject to;
    UniquePtr taggedPtr(&to);
    EXPECT_TRUE(taggedPtr);
}

/// Test boolean operator of UniqueTaggedPtr - False case
TEST(UniqueTaggedPtrTest, boolFalseTest) {
    UniquePtr taggedPtr;
    EXPECT_FALSE(taggedPtr);
}

/// Test the -> operator of UniqueTaggedPtr
TEST(UniqueTaggedPtrTest, ptrTest) {
    TestObject to;
    UniquePtr taggedPtr(&to);
    EXPECT_EQ(123, taggedPtr->getData());
}

/// Test set and get of UniqueTaggedPtr
TEST(UniqueTaggedPtrTest, setObjTest) {
    TestObject to;
    UniquePtr taggedPtr(nullptr);
    taggedPtr.set(&to);
    EXPECT_EQ(&to, taggedPtr.get());
}

/// Test setTag and getTag of UniqueTaggedPtr
TEST(UniqueTaggedPtrTest, setTagTest) {
    UniquePtr taggedPtr(nullptr);
    taggedPtr.setTag(123);
    EXPECT_EQ(123, taggedPtr.getTag());
}

/// Check that the unique tagged pointer can have its tag set without affecting
/// where the pointer points to.
TEST(UniqueTaggedPtrTest, pointerUnaffectedTest) {
    TestObject to;

    UniquePtr taggedPtr(&to);
    auto obj = taggedPtr.get();

    // Tag should start at zero i.e. empty
    ASSERT_EQ(0, taggedPtr.getTag());
    taggedPtr.setTag(456);
    ASSERT_EQ(456, taggedPtr.getTag());
    EXPECT_EQ(obj, taggedPtr.get());
    EXPECT_EQ(123, taggedPtr->getData());
}

/// Check that the unique tagged pointer can have its pointer set without
/// affecting the data held in the tag
TEST(UniqueTaggedPtrTest, tagUnaffectedTest) {
    TestObject to;

    UniquePtr taggedPtr(nullptr, 123);
    ASSERT_EQ(123, taggedPtr.getTag());
    taggedPtr.set(&to);
    ASSERT_EQ(&to, taggedPtr.get());
    EXPECT_EQ(123, taggedPtr.getTag());
}
