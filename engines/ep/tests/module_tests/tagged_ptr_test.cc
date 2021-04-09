/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "atomic.h"
#include "tagged_ptr.h"

#include <folly/portability/GTest.h>

/*
 * Unit tests for the TaggedPtr class.
 */

/// Test constructor taking object
TEST(TaggedPtrTest, constructorObjectTest) {
    uint32_t data = 123;
    TaggedPtr<uint32_t> taggedPtr(&data, TaggedPtrBase::NoTagValue);
    ASSERT_EQ(&data, taggedPtr.get());
}

/// Test constructor taking object and tag
TEST(TaggedPtrTest, constructorObjectAndTagTest) {
    uint32_t data = 123;
    TaggedPtr<uint32_t> taggedPtr(&data, 456);
    EXPECT_EQ(&data, taggedPtr.get());
    EXPECT_EQ(456, taggedPtr.getTag());
}

/// Test equal operator of TaggedPtr
TEST(TaggedPtrTest, equalTest) {
    uint32_t data = 0;
    TaggedPtr<uint32_t> taggedPtr(&data, TaggedPtrBase::NoTagValue);
    EXPECT_TRUE(taggedPtr.get() == &data);
}

/// Test not equal operator of TaggedPtr
TEST(TaggedPtrTest, notEqualTest) {
    uint32_t data = 0;
    uint32_t newData = 0;
    TaggedPtr<uint32_t> taggedPtr(&data, TaggedPtrBase::NoTagValue);
    EXPECT_TRUE(taggedPtr.get() != &newData);
}

/// Test boolean operator of TaggedPtr - True case
TEST(TaggedPtrTest, boolTrueTest) {
    uint32_t data = 123;
    TaggedPtr<uint32_t> taggedPtr(&data, TaggedPtrBase::NoTagValue);
    EXPECT_TRUE(taggedPtr);
}

/// Test boolean operator of TaggedPtr - False case
TEST(TaggedPtrTest, boolFalseTest) {
    TaggedPtr<uint32_t> taggedPtr;
    EXPECT_FALSE(taggedPtr);
}

/// Check that when the tag is set if the pointer is nullptr then calling the
/// bool method on the taggedptr will return false
TEST(TaggedPtrTest, boolWithTagFalseTest) {
    TaggedPtr<uint32_t> taggedPtr(nullptr, 123);
    ASSERT_EQ(123, taggedPtr.getTag());
    EXPECT_FALSE(taggedPtr);
}

/// Test the -> operator of TaggedPtr
TEST(TaggedPtrTest, ptrTest) {
    class TestObject {
    public:
        TestObject() = default;
        uint32_t data;
    };

    TestObject testObject;
    testObject.data = 123;

    TaggedPtr<TestObject> taggedPtr(&testObject, TaggedPtrBase::NoTagValue);
    EXPECT_EQ(123, taggedPtr->data);
}

/// Test set and get of TaggedPtr
TEST(TaggedPtrTest, setObjTest) {
    uint32_t data = 0;
    TaggedPtr<uint32_t> taggedPtr;
    taggedPtr.set(&data);
    EXPECT_EQ(&data, taggedPtr.get());
}

/// Test setTag and getTag of TaggedPtr
TEST(TaggedPtrTest, setTagTest) {
    TaggedPtr<uint32_t> taggedPtr;
    taggedPtr.setTag(123);
    EXPECT_EQ(123, taggedPtr.getTag());
}

/// Check that the tagged pointer can have its tag set without affecting where
/// the pointer points to.
TEST(TaggedPtrTest, pointerUnaffectedTest) {
    uint32_t data = 123;

    TaggedPtr<uint32_t> taggedPtr(&data, TaggedPtrBase::NoTagValue);
    auto obj = taggedPtr.get();

    // Tag should start at zero i.e. empty
    ASSERT_EQ(0, taggedPtr.getTag());
    taggedPtr.setTag(456);
    ASSERT_EQ(456, taggedPtr.getTag());
    EXPECT_EQ(obj, taggedPtr.get());
    EXPECT_EQ(123, *(taggedPtr.get()));
}

/// Check that the tagged pointer can have its pointer set without affecting the
/// data held in the tag
TEST(TaggedPtrTest, tagUnaffectedTest) {
    uint32_t data = 0;

    TaggedPtr<uint32_t> taggedPtr(nullptr, 123);
    ASSERT_EQ(123, taggedPtr.getTag());
    taggedPtr.set(&data);
    ASSERT_EQ(&data, taggedPtr.get());
    EXPECT_EQ(123, taggedPtr.getTag());
}

/// Check that the tag can be set using the updateTag helper method
TEST(TaggedPtrTest, updateTagTest) {
    // TestObject needs to inherit from RCValue because SingleThreadedRCPtr
    // only takes RCValues
    class TestObject : public RCValue {
    public:
        TestObject() : data(123) {
        }

        uint32_t getData() {
            return data;
        }

    private:
        uint32_t data;
    };

    // Custom deleter for TestObject objects
    struct Deleter {
        void operator()(TaggedPtr<TestObject> val) {
            // Does not do anything
        }
    };

    TestObject to;
    SingleThreadedRCPtr<TestObject, TaggedPtr<TestObject>, Deleter> ptr{
            TaggedPtr<TestObject>(&to, TaggedPtrBase::NoTagValue)};
    TaggedPtr<TestObject>::updateTag(ptr, 456);
    EXPECT_EQ(456, ptr.get().getTag());
}

/// Check that the tag can be set using the updateTag helper method when pointer
/// is a std::unique_ptr
TEST(TaggedPtrTest, updateTagTestUniquePtr) {
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

    // Custom deleter for TestObject objects
    struct Deleter {
        void operator()(TestObject* val) {
            // Does not do anything
        }
    };

    using UniquePtr =
            std::unique_ptr<TestObject, TaggedPtrDeleter<TestObject, Deleter>>;

    TestObject to;
    UniquePtr ptr{TaggedPtr<TestObject>(&to, TaggedPtrBase::NoTagValue)};
    TaggedPtr<TestObject>::updateTag(ptr, 456);
    EXPECT_EQ(456, ptr.get().getTag());
}
