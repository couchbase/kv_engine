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

/*
 * Unit tests for the StoredValue class.
 */

#include "hash_table.h"
#include "item.h"
#include "learning_age_and_mfu_based_eviction.h"
#include "stats.h"
#include "stored_value_factories.h"
#include "tests/module_tests/test_helpers.h"

#include <folly/portability/GTest.h>
#include <platform/cb_arena_malloc.h>

/**
 * Test fixture for StoredValue tests. Type-parameterized to test both
 * StoredValue and OrderedStoredValue.
 */
template <typename Factory>
class ValueTest : public ::testing::Test {
public:
    ValueTest()
        : stats(),
          ht(stats,
             std::make_unique<Factory>(),
             /*size*/ 47,
             /*locks*/ 1,
             /*freqCounterIncFactor*/ 0.012),
          item(make_item(Vbid(0), makeStoredDocKey("key"), "value")) {
    }

    void SetUp() override {
        // Create an initial stored value for testing - key length (3) and
        // value length (5).
        sv = factory(item, {});

        // For better testing below we will move the age away from 0, there was
        // a bug where it was reset to zero and the tests didn't spot that
        EXPECT_EQ(0, sv->getAge())
                << "Age not the default value of " << ageInitialValue;
        sv->setAge(ageInitialValue);
        EXPECT_EQ(ageInitialValue, sv->getAge());
    }

    /// Returns the number of bytes in the Fixed part of StoredValue
    static size_t getFixedSize() {
        return sizeof(typename Factory::value_type);
    }

    /// Allow testing access to StoredValue::getRequiredStorage
    static size_t public_getRequiredStorage(const DocKeyView& key) {
        return Factory::value_type::getRequiredStorage(key);
    }

protected:
    const uint8_t ageInitialValue = 101;
    EPStats stats;
    Factory factory;
    HashTable ht;
    Item item;
    StoredValue::UniquePtr sv;
};

using ValueFactories =
        ::testing::Types<StoredValueFactory, OrderedStoredValueFactory>;
TYPED_TEST_SUITE(ValueTest, ValueFactories);

// Check that the size calculation methods return the expected sizes.

TYPED_TEST(ValueTest, getObjectSize) {
    // Check the size are as expected: Fixed size of (Ordered)StoredValue, plus
    // 3 bytes
    // for 'key', 1 byte for length of key and 1 byte for StoredDocKey
    // namespace.
    EXPECT_EQ(this->getFixedSize() + /*key*/ 3 + /*len*/ 1 +
                      /*default collection-ID*/ 1,
              this->sv->getObjectSize());
}

/* Disabled if jemalloc is not in use as this test relies upon
 * the specific bin sizes used by jemalloc. Additionally, other
 * AllocHooks don't necessarily implement get_allocation_size.
 */
#if defined(HAVE_JEMALLOC)
TYPED_TEST(ValueTest, StoredValueReallocateGivesSameSize) {
#else
TYPED_TEST(ValueTest, DISABLED_StoredValueReallocateGivesSameSize) {
#endif

    /* MB-25143: Ensure reallocation doesn't allocate excess bytes
     * Make an item with a value of size 182.
     * sizeof(Blob) = 12, but three of those bytes are padding
     * used for the data. Therefore, the allocation size for the blob is
     * blob.size + sizeof(Blob) - 3
     * 182 + 12 - 3 = 191
     * Jemalloc has a bin of size 192, which should be chosen for
     * the allocation of the blob.
     * As noted in MB-25143, the reallocation done by the defragmenter
     * overallocated by two bytes. This would push it over the bin size
     */

    auto sv = this->factory(
            make_item(Vbid(0),
                      makeStoredDocKey(std::string(10, 'k').c_str()),
                      std::string(182, 'v').c_str()),
            {});

    auto blob = sv->getValue();
    ASSERT_EQ(191, blob->getSize());
    int before = cb::ArenaMalloc::malloc_usable_size(blob.get().get());

    /* While the initial bug in MB-25143 would only increase the size of
     * the blob once, by two bytes, we iterate here to ensure that there
     * is no slow increase with each reallocation. We would only see
     * an increase in je_malloc_usable_size once a bin size is exceeded.
     * ( in this case, the bin size is 192)
     */
    for (int i = 0; i < 100; ++i) {
        sv->reallocate();

        blob = sv->getValue();
        int after = cb::ArenaMalloc::malloc_usable_size(blob.get().get());

        EXPECT_EQ(before, after);
    }
}

/* MB-30097: Set the stored value as uncompressible and ensure that
 * the size of the Blob doesn't change on a reallocation
 */
TYPED_TEST(ValueTest, StoredValueUncompressibleReallocateGivesSameSize) {
    auto sv = this->factory(
            make_item(Vbid(0),
                      makeStoredDocKey(std::string(10, 'k').c_str()),
                      std::string(182, 'v').c_str()),
            {});

    sv->setAge(this->ageInitialValue);
    auto blob = sv->getValue();
    int beforeValueSize = blob->valueSize();
    sv->setUncompressible();
    sv->reallocate();
    EXPECT_EQ(this->ageInitialValue, sv->getAge());
    blob = sv->getValue();
    EXPECT_EQ(beforeValueSize, blob->valueSize());
}

// MB-61790: Ensure the flag is read correctly
TYPED_TEST(ValueTest, isCompressible) {
    auto sv = this->factory(
            make_item(Vbid(0), makeStoredDocKey("key"), "value"), {});
    EXPECT_TRUE(sv->isCompressible());
    sv->setUncompressible();
    EXPECT_FALSE(sv->isCompressible());
}

TYPED_TEST(ValueTest, metaDataSize) {
    // Check metadata size reports correctly.
    EXPECT_EQ(this->getFixedSize() + /*key*/ 3 + /*len*/ 1 +
                      /*default collection-ID*/ 1,
              this->sv->metaDataSize());
}

TYPED_TEST(ValueTest, valuelen) {
    // Check valuelen reports correctly.
    EXPECT_EQ(/*value length*/ 5, this->sv->valuelen())
            << "valuelen() expected to be sum of raw value length + extended "
               "meta";
}

TYPED_TEST(ValueTest, valuelenDeletedWithValue) {
    // Check valuelen reports correctly for a StoredValue just marked delete
    // (with xattrs deleted items can have value)
    this->sv->markDeleted(DeleteSource::Explicit);
    EXPECT_EQ(/*value length*/ 5, this->sv->valuelen())
            << "valuelen() expected to be sum of raw value length + extended "
               "meta as we want to keep deleted body";
}

TYPED_TEST(ValueTest, valuelenDeletedWithoutValue) {
    // Check valuelen reports correctly for a StoredValue logically delete
    this->sv->del(DeleteSource::Explicit);
    EXPECT_TRUE(this->sv->isResident());
    EXPECT_EQ(0, this->sv->valuelen())
            << "valuelen() expected to be 0 as we do not want to keep deleted "
               "body";
}

TYPED_TEST(ValueTest, size) {
    // Check size reports correctly.
    EXPECT_EQ(this->getFixedSize() + /*key*/ 3 + /*len*/ 1 +
                      /*default collection-ID*/ 1 + /*valuelen*/ 5,
              this->sv->size());
}

TYPED_TEST(ValueTest, getRequiredStorage) {
    EXPECT_EQ(this->sv->getObjectSize(),
              this->public_getRequiredStorage(this->item.getKey()))
            << "Actual object size doesn't match what getRequiredStorage "
               "predicted";
}

/// Check if the value is resident. Also, check for
/// residency once the value has been ejected
TYPED_TEST(ValueTest, checkIfResident) {
    EXPECT_TRUE(this->sv->getValue());
    EXPECT_TRUE(this->sv->isResident());

    this->sv->ejectValue();
    EXPECT_FALSE(this->sv->getValue());
    EXPECT_FALSE(this->sv->isResident());
}

/// Check if the value is resident for a temporary
/// item
TYPED_TEST(ValueTest, checkIfTempItemIsResident) {
    Item itm(makeStoredDocKey("k"),
             0,
             0,
             (const value_t) TaggedPtr<Blob>{},
             PROTOCOL_BINARY_RAW_BYTES,
             0,
             StoredValue::state_temp_init);
    this->sv->setValue(itm);
    EXPECT_TRUE(this->sv->isTempItem());
    EXPECT_FALSE(this->sv->isResident());
}

/// Check if the deleted item is resident or not
TYPED_TEST(ValueTest, checkIfDeletedWithValueIsResident) {
    ///Just mark the value as deleted. There should
    ///be a value and the item should be resident
    this->sv->markDeleted(DeleteSource::Explicit);
    EXPECT_TRUE(this->sv->getValue());
    EXPECT_TRUE(this->sv->isResident());

    this->sv->ejectValue();
    EXPECT_FALSE(this->sv->isResident());
}

/** Check that when an item is deleted (with no value) its datatype is set
 * to RAW_BYTES.
 */
TYPED_TEST(ValueTest, deletedValueDatatypeIsBinary) {
    ASSERT_EQ(PROTOCOL_BINARY_DATATYPE_JSON, this->sv->getDatatype());
    this->sv->del(DeleteSource::Explicit);
    EXPECT_EQ(PROTOCOL_BINARY_RAW_BYTES, this->sv->getDatatype())
            << "datatype should be RAW BYTES after deletion.";
}

/**
 * Test the get / set of the frequency counter
 */
TYPED_TEST(ValueTest, freqCounter) {
    EXPECT_EQ(4, this->sv->getFreqCounterValue());
    this->sv->setFreqCounterValue(1);
    EXPECT_EQ(1, this->sv->getFreqCounterValue());
    EXPECT_EQ(this->ageInitialValue, this->sv->getAge());
}

TYPED_TEST(ValueTest, initialFreqCounterForTemp) {
    Item itm = make_item(Vbid(0),
                         makeStoredDocKey(std::string("key").c_str()),
                         std::string("value").c_str());
    itm.setBySeqno(StoredValue::state_temp_init);

    auto storedVal = this->factory(itm, {});

    ASSERT_TRUE(storedVal->isTempItem());
    EXPECT_EQ(0, storedVal->getFreqCounterValue());
    EXPECT_EQ(this->ageInitialValue, this->sv->getAge());
}

TYPED_TEST(ValueTest, replaceValue) {
    ASSERT_EQ(4, this->sv->getFreqCounterValue());
    this->sv->setFreqCounterValue(100);
    ASSERT_EQ(100, this->sv->getFreqCounterValue());

    auto sv = this->factory(
            make_item(Vbid(0),
                      makeStoredDocKey(std::string("key").c_str()),
                      std::string("value").c_str()),
            {});

    this->sv->replaceValue(std::unique_ptr<Blob>(sv->getValue().get().get()));
    EXPECT_EQ(100, this->sv->getFreqCounterValue());
    EXPECT_EQ(this->ageInitialValue, this->sv->getAge());
}

TYPED_TEST(ValueTest, restoreValue) {
    ASSERT_EQ(4, this->sv->getFreqCounterValue());
    this->sv->setFreqCounterValue(100);
    ASSERT_EQ(100, this->sv->getFreqCounterValue());

    auto itm = make_item(Vbid(0),
                         makeStoredDocKey(std::string("key").c_str()),
                         std::string("value").c_str());

    this->sv->restoreValue(itm);
    EXPECT_EQ(4, this->sv->getFreqCounterValue());
    EXPECT_EQ(this->ageInitialValue, this->sv->getAge());
}

TYPED_TEST(ValueTest, restoreMeta) {
    ASSERT_EQ(4, this->sv->getFreqCounterValue());
    this->sv->setFreqCounterValue(100);
    ASSERT_EQ(100, this->sv->getFreqCounterValue());

    auto itm = make_item(Vbid(0),
                         makeStoredDocKey(std::string("key").c_str()),
                         std::string("value").c_str());

    this->sv->restoreMeta(itm);
    EXPECT_EQ(4, this->sv->getFreqCounterValue());
    EXPECT_EQ(this->ageInitialValue, this->sv->getAge());
}

/**
 * Test the get / set of the age field
 */
TYPED_TEST(ValueTest, age) {
    const auto freq = this->sv->getFreqCounterValue();
    EXPECT_EQ(this->ageInitialValue, this->sv->getAge());
    this->sv->setAge(55);
    EXPECT_EQ(55, this->sv->getAge());
    // We also test the freq-counter which shares the tag with age
    EXPECT_EQ(freq, this->sv->getFreqCounterValue());
    this->sv->incrementAge();
    EXPECT_EQ(56, this->sv->getAge());
    EXPECT_EQ(freq, this->sv->getFreqCounterValue());
}

// Check that CommittedState is correctly copied from an Item object.
TYPED_TEST(ValueTest, committedState) {
    Item itm(makeStoredDocKey("k"),
             0,
             0,
             (const value_t)TaggedPtr<Blob>{},
             PROTOCOL_BINARY_RAW_BYTES,
             0,
             1);

    this->sv->setValue(itm);
    EXPECT_EQ(CommittedState::CommittedViaMutation, this->sv->getCommitted());
    EXPECT_TRUE(this->sv->isCommitted());
    EXPECT_FALSE(this->sv->isPending());

    itm.setPendingSyncWrite({cb::durability::Level::Majority, {}});
    this->sv->setValue(itm);
    EXPECT_EQ(CommittedState::Pending, this->sv->getCommitted());
    EXPECT_FALSE(this->sv->isCommitted());
    EXPECT_TRUE(this->sv->isPending());

    itm.setPreparedMaybeVisible();
    this->sv->setValue(itm);
    EXPECT_EQ(CommittedState::PreparedMaybeVisible, this->sv->getCommitted());
    EXPECT_FALSE(this->sv->isCommitted());
    EXPECT_TRUE(this->sv->isPending());

    itm.setCommittedviaPrepareSyncWrite();
    this->sv->setValue(itm);
    EXPECT_EQ(CommittedState::CommittedViaPrepare, this->sv->getCommitted());
    EXPECT_TRUE(this->sv->isCommitted());
    EXPECT_FALSE(this->sv->isPending());
}

/**
 *  Test that an mutation does not reset the frequency counter
 */
TYPED_TEST(ValueTest, freqCounterNotReset) {
    Item itm(makeStoredDocKey("k"),
             0,
             0,
             (const value_t)TaggedPtr<Blob>{},
             PROTOCOL_BINARY_RAW_BYTES,
             0,
             1);
    this->sv->setValue(itm);
    this->sv->setFreqCounterValue(10);
    ASSERT_EQ(10, this->sv->getFreqCounterValue());
    this->sv->setValue(itm);
    EXPECT_EQ(10, this->sv->getFreqCounterValue());
}

/// Check that StoredValue / OrderedStoredValue don't unexpectedly change in
/// size (we've carefully crafted them to be as efficient as possible).
TEST(StoredValueTest, expectedSize) {
    auto key = makeStoredDocKey("k");
    EXPECT_EQ(sizeof(StoredValue) + 3, StoredValue::getRequiredStorage(key))
            << "Unexpected change in StoredValue storage size for key: " << key;
}

// Disable if we don't have jemalloc or if on Windows (because Win32 SVs are
// larger due to the compiler ignoring the pack() attribute when there are
// std::atomic members.
#if defined(HAVE_JEMALLOC) && !defined(_WIN32)
TEST(StoredValueTest, StoredValuesAllocatedInExpectedBin) {
#else
TEST(StoredValueTest, DISABLED_StoredValuesAllocatedInExpectedBin) {
#endif
    for (auto keySize : {22, 23, 24, 25}) {
        const int expectedBin = 80;
        auto stats = EPStats();
        auto sv = StoredValueFactory()(
                make_item(Vbid(0),
                          makeStoredDocKey(std::string(keySize, 'k').c_str()),
                          ""),
                {});

        int usableSize = cb::ArenaMalloc::malloc_usable_size(sv.get().get());
        EXPECT_EQ(expectedBin, usableSize) << "keySize=" << keySize;
    }

    for (auto keySize : {26, 27, 28, 29, 30}) {
        const int expectedBin = 96;
        auto stats = EPStats();
        auto sv = StoredValueFactory()(
                make_item(Vbid(0),
                          makeStoredDocKey(std::string(keySize, 'k').c_str()),
                          ""),
                {});

        int usableSize = cb::ArenaMalloc::malloc_usable_size(sv.get().get());
        EXPECT_EQ(expectedBin, usableSize) << "keySize=" << keySize;
    }
}

// Validate the deletion source propagates via setValue
TYPED_TEST(ValueTest, MB_32568) {
    Item itm(makeStoredDocKey("k"),
             0,
             0,
             (const value_t)TaggedPtr<Blob>{},
             PROTOCOL_BINARY_RAW_BYTES,
             0,
             StoredValue::state_temp_init);
    itm.setDeleted();
    this->sv->setValue(itm);
    EXPECT_EQ(DeleteSource::Explicit, this->sv->getDeletionSource());

    // And now for TTL
    this->sv = this->factory(this->item, {});
    itm.setDeleted(DeleteSource::TTL);
    this->sv->setValue(itm);
    EXPECT_EQ(DeleteSource::TTL, this->sv->getDeletionSource());
}

// Validate the CAS for writing changes when locked, and is restored afterwards
// when unlocked.
TYPED_TEST(ValueTest, LockedCas) {
    this->sv->setCas(123);
    ASSERT_EQ(123, this->sv->getCasForWrite(0))
            << "CAS used for writing should be main CAS if not locked";

    rel_time_t lock_expiry{10};
    this->sv->lock(lock_expiry, 456);
    EXPECT_EQ(456, this->sv->getCasForWrite(lock_expiry - 1))
            << "CAS used for writing should have changed to locked CAS while "
               "lock still valid";
    EXPECT_EQ(123, this->sv->getCas())
            << "main CAS should be unchanged when locked";

    this->sv->unlock();
    EXPECT_EQ(123, this->sv->getCasForWrite(lock_expiry - 1))
            << "CAS used for writing should have reverted to previous VAS "
               "after explicit unlock";
    EXPECT_EQ(123, this->sv->getCas())
            << "main CAS should be unchanged after explicit unlock";
}

// Validate the CAS for writing changes when locked, and is restored when lock
// expires.
TYPED_TEST(ValueTest, LockedCasExpired) {
    this->sv->setCas(123);
    ASSERT_EQ(123, this->sv->getCasForWrite(0))
            << "CAS used for writing should be main CAS if not locked";

    rel_time_t lock_expiry{10};
    this->sv->lock(lock_expiry, 456);
    EXPECT_EQ(456, this->sv->getCasForWrite(lock_expiry - 1))
            << "CAS used for writing should have changed to locked CAS while "
               "lock still valid";
    EXPECT_EQ(123, this->sv->getCas())
            << "main CAS should be unchanged when locked";

    EXPECT_EQ(123, this->sv->getCasForWrite(lock_expiry + 1))
            << "CAS used for writing should have reverted to previous VAS "
               "after lock expires";
    EXPECT_EQ(123, this->sv->getCas())
            << "main CAS should be unchanged after locked expires";
}

// Validate behavior when destroying a (O)SV while locked - ensure the
// SeparateDouble CAS encoding is correctly deleted.
TYPED_TEST(ValueTest, DtorWhileLocked) {
    rel_time_t lock_expiry{10};
    this->sv->lock(lock_expiry, 456);
    // Want to run under ASan or similar memory checking tool to confirm
    // no leak.
    this->sv.reset();
}

// Validate behavior when setting a (O)SV while locked - ensure the
// SeparateDouble CAS encoding is not leaked.
TYPED_TEST(ValueTest, DeleteWhileLocked) {
    rel_time_t lock_expiry{10};
    this->sv->setCas(123);
    this->sv->lock(lock_expiry, 456);

    this->sv->del(DeleteSource::Explicit);
    EXPECT_EQ(123, this->sv->getCas())
            << "After del(), CAS should be unchanged";

    // Want to run under ASan or similar memory checking tool to confirm
    // no leak.
}

/**
 * Test fixture for OrderedStoredValue-only tests.
 */
class OrderedStoredValueTest : public ValueTest<OrderedStoredValueFactory> {};

TEST_F(OrderedStoredValueTest, expectedSize) {
    auto key = makeStoredDocKey("k");
    EXPECT_EQ(sizeof(OrderedStoredValue) + 3,
              OrderedStoredValue::getRequiredStorage(key))
            << "Unexpected change in OrderedStoredValue storage size for key: "
            << key;
}

// Check that when we copy a OSV, the freqCounter is also copied. (Cannot copy
// StoredValues, hence no version for them).
TEST_F(OrderedStoredValueTest, copyStoreValue) {
    ASSERT_EQ(4, sv->getFreqCounterValue());
    sv->setFreqCounterValue(100);
    ASSERT_EQ(100, sv->getFreqCounterValue());

    auto copy = factory.copyStoredValue(*sv, {});

    EXPECT_EQ(100, copy->getFreqCounterValue());
}

/**
 * Test fixture for implementation testing of StoredValue, requiring access
 * to protected items in StoredValue
 */
template <typename Factory>
class StoredValueProtectedTest : public ValueTest<Factory> {
public:
    void setDeleteSource(DeleteSource delSource, StoredValue::UniquePtr& sv) {
        return sv->setDeletionSource(delSource);
    }
};

using ValueFactories =
        ::testing::Types<StoredValueFactory, OrderedStoredValueFactory>;
TYPED_TEST_SUITE(StoredValueProtectedTest, ValueFactories);

/**
 * Check that deleteSource does not get compared between two non-deleted values.
 * This requires the use of StoredValueProtectedTest as deleteSource cannot be
 * changed publicly without deleting the stored value, which is required to
 * fully test this issue.
 */

TYPED_TEST(StoredValueProtectedTest, MB_32835) {
    auto sv2 = this->factory(this->item, {});
    Item itm(makeStoredDocKey("k"),
             0,
             0,
             (const value_t)TaggedPtr<Blob>{},
             PROTOCOL_BINARY_RAW_BYTES,
             0,
             1);
    this->sv->setValue(itm);
    sv2->setValue(itm);

    this->setDeleteSource(DeleteSource::TTL, this->sv);
    ASSERT_EQ(false, this->sv->isDeleted());

    this->setDeleteSource(DeleteSource::Explicit, sv2);
    ASSERT_EQ(false, sv2->isDeleted());

    EXPECT_EQ(*this->sv, *sv2);
}
