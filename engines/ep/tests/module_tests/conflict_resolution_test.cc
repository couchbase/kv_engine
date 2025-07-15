/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>

#include <engines/ep/src/conflict_resolution.h>
#include <engines/ep/src/item.h>
#include <engines/ep/src/stats.h>
#include <engines/ep/src/stored-value.h>
#include <engines/ep/src/stored_value_factories.h>
#include <mcbp/protocol/datatype.h>

#include "test_helpers.h"

#include <vector>

/**
 * Base test class for non-parameterised tests of seqno/lww resolution logic.
 */
class ConflictResolutionTest : public ::testing::Test {
public:
    ConflictResolutionTest() : localValue(getReferenceValue()) {
    }

    /**
     * Generate the "existing" stored value for conflict resolution purposes.
     *
     * The Cas, RevSeqno, Flags, and ExpTime are all set to specific (but
     * arbitrary) values, such that tests can provide an "incoming" item
     * with higher/lower/equal values for each field to observe the result of
     * conflict resolution.
     *
     * @return
     */
    StoredValue::UniquePtr getReferenceValue();

    StoredValueFactory factory;

    StoredValue::UniquePtr localValue;

    RevisionSeqnoResolution seqnoResolution;
    LastWriteWinsResolution lwwResolution;
};

// conveniences for declaring test instantiations
// resolution doesn't care about the _exact_ cas of the incoming item, just
// how it compares to the existing value.

// Assuming an existing document exists with cas/rev/flags/exptime with the
// values for "Same" below, tests will be instantiated for every combination of
// the incoming document being lower/same/higher in each value.
enum class Cas : uint64_t {
    Lower = 999,
    Same = 1000,
    Higher = 1001,

    // tests may wish to declare that a remote document will be accepted
    // regardless of the value of a given field - they may use Any
    // in this case, rather than repeating the accept case
    // for every possible value.
    Any = std::numeric_limits<uint64_t>::max(),
};

enum class Rev : uint64_t {
    Lower = 999,
    Same = 1000,
    Higher = 1001,
    Any = std::numeric_limits<uint64_t>::max(),
};

enum class ExpTime : uint32_t {
    Lower = 999,
    Same = 1000,
    Higher = 1001,
    Any = std::numeric_limits<uint32_t>::max(),
};

enum class Flags : uint32_t {
    Lower = 999,
    Same = 1000,
    Higher = 1001,
    Any = std::numeric_limits<uint32_t>::max(),
};

// xattrs are handled a little differently; the local document can't have a
// fixed value for xattrs to test all the cases, so test instantiations
// control whether the "existing" document has xattrs as well as the "incoming"
enum class Xattrs {
    Any,
    None,
    ExistingOnly,
    IncomingOnly,
    Both,
};

StoredValue::UniquePtr ConflictResolutionTest::getReferenceValue() {
    Item item = make_item(Vbid(0),
                          makeStoredDocKey("someKey"),
                          "someValue",
                          0,
                          PROTOCOL_BINARY_RAW_BYTES);
    item.setCas(uint64_t(Cas::Same));
    item.setRevSeqno(uint64_t(Rev::Same));
    item.setExpTime(time_t(ExpTime::Same));
    item.setFlags(uint32_t(Flags::Same));
    return factory(item, nullptr);
}

/**
 * Test class for parameterised tests of seqno/lww resolution logic.
 *
 * Tests will be instantiated for every combination of having the
 * incoming value be:
 *
 *  * Higher/Ahead
 *  * Equal
 *  * Lower/Behind
 *
 * for each of:
 *
 * * Cas
 * * RevSeqno
 * * Flags (historically compared  with < despite not necessarily being
 *          meaningfully ordered)
 * * ExpiryTime
 *
 *
 * And also for Xattrs being present in either the incoming or existing docs,
 * or neither, or both.
 */
class ConflictResolutionParamTest
    : public ConflictResolutionTest,
      public ::testing::WithParamInterface<
              std::tuple<Cas, Rev, ExpTime, Flags, Xattrs>> {
public:
    ConflictResolutionParamTest() {
        // update the datatype of the "existing value" to match the test
        // instantiation
        switch (std::get<Xattrs>(GetParam())) {
        case Xattrs::Both:
        case Xattrs::ExistingOnly:
            localValue->setDatatype(localValue->getDatatype() |
                                    uint8_t(cb::mcbp::Datatype::Xattr));
        default:; // do nothing to the existing value
        }
    }
    /**
     * Compare a Cas/Rev/Flags/ExpTime value against some expectation.
     *
     * @return true if the two values match, or the "expected" value is Any
     */
    template <class EnumClass>
    static bool matches(const EnumClass& observed, const EnumClass& expected) {
        // it is not correct to instantiate tests over "Any" value for any
        // of the enums - Any can only be used as an expectation.
        Expects(observed != EnumClass::Any);

        // If expected == Any, then that means the value for that quantity
        // is not relevant to the resolution in this case - e.g.,
        // a test with an accept case:
        //   Cas::Higher, Rev::Any, ...
        // is declaring that if the incoming document has a _higher_ cas
        // than the local document, the value of the revSeqno is not important
        // the remote document is expected to be picked regardless.
        // this is largely a convenience for tests; this could equivalently
        // be written:
        //   Cas::Higher, Rev::Lower, ...
        //   Cas::Higher, Rev::Same, ...
        //   Cas::Higher, Rev::Higher, ...
        // but that is very verbose for 4 dimensions, and hides the actual
        // interesting pattern in the test.

        return expected == EnumClass::Any || (observed == expected);
    }

    /**
     * Evaluate whether a given test param "matches" a pattern as declared by
     * a test.
     *
     * E.g.,
     *
     *  matchesCase({Cas::Higher, Rev::Higher, Flags::Lower, ExpTime::Lower},
     *              {Cas::Higher, Rev::Any,    Flags::Any,   ExpTime::Lower});
     *
     * will return true - each element in the first arg matches the elements
     * in the second arg according to `matches()` (see above).
     *
     * Any functions as a wildcard to avoid having to repeat a case for
     * each possible value of a given field if it is not critical to
     * whether conflict resolution would accept or reject the document.
     */
    static bool matchesCase(
            const ConflictResolutionParamTest::ParamType& observedTuple,
            const ConflictResolutionParamTest::ParamType& expectedTuple) {
        return std::apply(
                [&expectedTuple](const auto&... observed) {
                    return std::apply(
                            [&observed...](const auto&... expected) {
                                return (matches(observed, expected) && ...);
                            },
                            expectedTuple);
                },
                observedTuple);
    }

    /**
     * Check if the current test instantiation matches any case present in
     * @p acceptCases.
     *
     * Used by tests to check the _expected_ result of conflict resolution,
     * using a declared vector of cases where "accept" is the expected result.
     */
    bool paramMatchesAnyCase(
            const std::vector<ConflictResolutionParamTest::ParamType>&
                    acceptCases) {
        return std::ranges::any_of(
                acceptCases, [&observed = GetParam()](const auto& acceptCase) {
                    return matchesCase(observed, acceptCase);
                });
    }

    /**
     * Create an item based on the test instantiation.
     *
     * Used as the "incoming" document for conflict resolution purposes.
     * The cas/rev/flags/exptime are determined based in the test param;
     * they may be same as/lower/higher than the values used by the "existing"
     * StoredValue. Conflict resolution currently only cares about these
     * comparisons; the _exact_ values of each field are not relevant.
     */
    Item getTestItem() {
        auto datatype = uint8_t(cb::mcbp::Datatype::Raw);

        switch (std::get<Xattrs>(GetParam())) {
        case Xattrs::Both:
        case Xattrs::IncomingOnly:
            datatype |= uint8_t(cb::mcbp::Datatype::Xattr);
        default:; // incoming value should not be xattr
        }

        Item item = make_item(
                Vbid(0), makeStoredDocKey("someKey"), "someValue", 0, datatype);
        item.setCas(uint64_t(std::get<Cas>(GetParam())));
        item.setRevSeqno(uint64_t(std::get<Rev>(GetParam())));
        item.setExpTime(uint32_t(std::get<ExpTime>(GetParam())));
        item.setFlags(uint32_t(std::get<Flags>(GetParam())));
        return item;
    }

    bool isIdentical(bool isDelete) {
        if (isDelete) {
            // delete with meta does not check exptime/flags/xattrs, so
            // values will be rejected as "identical" if cas and revseqno match
            return paramMatchesAnyCase({{Cas::Same,
                                         Rev::Same,
                                         ExpTime::Any,
                                         Flags::Any,
                                         Xattrs::Any}});
        }

        // a set with meta will be rejected as "identical" if they exactly match
        // the existing value in cas/revseqno/exptime/flags, AND one of:
        // * Both the existing and incoming values have xattrs
        // * Both the existing and incoming values do not have xattrs
        // * The existing value has xattrs but the incoming does not
        // That is, if the existing value _has_ xattrs, but the incoming
        // value _does not_, but is otherwise identical, this will still be
        // rejected as "identical" rather than "behind"
        return paramMatchesAnyCase({{Cas::Same,
                                     Rev::Same,
                                     ExpTime::Same,
                                     Flags::Same,
                                     Xattrs::Both},
                                    {Cas::Same,
                                     Rev::Same,
                                     ExpTime::Same,
                                     Flags::Same,
                                     Xattrs::ExistingOnly},
                                    {Cas::Same,
                                     Rev::Same,
                                     ExpTime::Same,
                                     Flags::Same,
                                     Xattrs::None}});
    }

    /**
     * Print any of the Cas/Rev/Flags/ExpTime test helper enums.
     *
     * Used to generate a human readable test case name.
     */
    template <class EnumClass>
    static std::string printInner(std::string name, EnumClass value) {
        switch (value) {
        case EnumClass::Lower:
            return name + "Lower";
        case EnumClass::Same:
            return name + "Same";
        case EnumClass::Higher:
            return name + "Higher";
        case EnumClass::Any:
            throw std::logic_error(
                    "Tests should not be parameterised over 'Any' value for " +
                    name);
        }
        throw std::logic_error("Test parameterised over unexpected value for " +
                               name + " " + std::to_string(uint64_t(value)));
    }

    // Overload for Xattrs enum
    static std::string printInner(std::string name, Xattrs value) {
        switch (value) {
        case Xattrs::None:
            return name + "None";
        case Xattrs::ExistingOnly:
            return name + "ExistingOnly";
        case Xattrs::IncomingOnly:
            return name + "IncomingOnly";
        case Xattrs::Both:
            return name + "Both";
        case Xattrs::Any:
            throw std::logic_error(
                    "Tests should not be parameterised over 'Any' value for "
                    "Xattrs");
        }
        throw std::logic_error(
                "Test parameterised over unexpected value for Xattrs " +
                std::to_string(uint64_t(value)));
    }

    /// print the current test instantiation as a human readable string.
    static std::string PrintToStringParamName(
            const ::testing::TestParamInfo<ParamType>& info) {
        std::string res;
        res += printInner("Cas", std::get<Cas>(info.param));
        res += printInner("Rev", std::get<Rev>(info.param));
        res += printInner("ExpTime", std::get<ExpTime>(info.param));
        res += printInner("Flags", std::get<Flags>(info.param));
        res += printInner("Xattrs", std::get<Xattrs>(info.param));
        return res;
    }
};

TEST_P(ConflictResolutionParamTest, NonExistent) {
    // if the local value is a temp non-existent item, conflict resolution
    // should _always_ accept the remote version, regardless of any of the
    // Cas/RevSeqno/Flags/ExpTime

    auto nonExistentValue = getReferenceValue();
    nonExistentValue->setNonExistent();

    Item item = getTestItem();
    const auto& meta = item.getMetaData();

    // set with meta
    EXPECT_EQ(ConflictResolution::Result::Accept,
              seqnoResolution.resolve(*nonExistentValue,
                                      meta,
                                      item.getDataType(),
                                      /*isDelete*/ false));

    EXPECT_EQ(ConflictResolution::Result::Accept,
              lwwResolution.resolve(*nonExistentValue,
                                    meta,
                                    item.getDataType(),
                                    /*isDelete*/ false));

    // del with meta
    EXPECT_EQ(ConflictResolution::Result::Accept,
              seqnoResolution.resolve(*nonExistentValue,
                                      meta,
                                      item.getDataType(),
                                      /*isDelete*/ true));

    EXPECT_EQ(ConflictResolution::Result::Accept,
              lwwResolution.resolve(*nonExistentValue,
                                    meta,
                                    item.getDataType(),
                                    /*isDelete*/ true));
}

TEST_P(ConflictResolutionParamTest, SeqnoResolution_Set) {
    // declare the set of cases in which the incoming set with meta is
    // expected to be accepted.

    // the price of long lines is worth paying for reading one case
    // per line - turn off clang format for this section!

    // clang-format off
    static const std::vector<ConflictResolutionParamTest::ParamType>
            acceptCases{
                    // the below line can be read as:
                    // "The incoming document should be accepted if it
                    //  has a higher revSeqno than the existing document,
                    //  and none of the other fields affect this"
                    {Cas::Any, Rev::Higher, ExpTime::Any, Flags::Any, Xattrs::Any},
                    {Cas::Higher, Rev::Same, ExpTime::Any, Flags::Any, Xattrs::Any},
                    {Cas::Same, Rev::Same, ExpTime::Higher, Flags::Any, Xattrs::Any},
                    {Cas::Same, Rev::Same, ExpTime::Same, Flags::Higher, Xattrs::Any},
                    {Cas::Same, Rev::Same, ExpTime::Same, Flags::Same, Xattrs::IncomingOnly},
            };
    // clang-format on

    Item item = getTestItem();
    const auto& meta = item.getMetaData();

    ConflictResolution::Result expectedResult;
    if (paramMatchesAnyCase(acceptCases)) {
        expectedResult = ConflictResolution::Result::Accept;
    } else if (isIdentical(/*isDelete*/ false)) {
        expectedResult = ConflictResolution::Result::RejectIdentical;
    } else {
        expectedResult = ConflictResolution::Result::RejectBehind;
    }
    // check that a set with meta would be accepted/rejected
    EXPECT_EQ(expectedResult,
              seqnoResolution.resolve(*localValue,
                                      meta,
                                      item.getDataType(),
                                      /*isDelete*/ false));
}

TEST_P(ConflictResolutionParamTest, LwwResolution_Set) {
    // clang-format off
    static const std::vector<ConflictResolutionParamTest::ParamType>
            acceptCases{
                    {Cas::Higher, Rev::Any, ExpTime::Any, Flags::Any, Xattrs::Any},
                    {Cas::Same, Rev::Higher, ExpTime::Any, Flags::Any, Xattrs::Any},
                    {Cas::Same, Rev::Same, ExpTime::Higher, Flags::Any, Xattrs::Any},
                    {Cas::Same, Rev::Same, ExpTime::Same, Flags::Higher, Xattrs::Any},
                    {Cas::Same, Rev::Same, ExpTime::Same, Flags::Same, Xattrs::IncomingOnly},
            };
    // clang-format on

    Item item = getTestItem();
    const auto& meta = item.getMetaData();

    ConflictResolution::Result expectedResult;
    if (paramMatchesAnyCase(acceptCases)) {
        expectedResult = ConflictResolution::Result::Accept;
    } else if (isIdentical(/*isDelete*/ false)) {
        expectedResult = ConflictResolution::Result::RejectIdentical;
    } else {
        expectedResult = ConflictResolution::Result::RejectBehind;
    }

    EXPECT_EQ(expectedResult,
              lwwResolution.resolve(*localValue,
                                    meta,
                                    item.getDataType(),
                                    /*isDelete*/ false));
}

TEST_P(ConflictResolutionParamTest, SeqnoResolution_Del) {
    // clang-format off
    static const std::vector<ConflictResolutionParamTest::ParamType>
            acceptCases{
                    {Cas::Any, Rev::Higher, ExpTime::Any, Flags::Any, Xattrs::Any},
                    {Cas::Higher, Rev::Same, ExpTime::Any, Flags::Any, Xattrs::Any},
            };
    // clang-format on

    Item item = getTestItem();
    const auto& meta = item.getMetaData();

    ConflictResolution::Result expectedResult;
    if (paramMatchesAnyCase(acceptCases)) {
        expectedResult = ConflictResolution::Result::Accept;
    } else if (isIdentical(/*isDelete*/ true)) {
        expectedResult = ConflictResolution::Result::RejectIdentical;
    } else {
        expectedResult = ConflictResolution::Result::RejectBehind;
    }

    // check that a del with meta would be accepted/rejected
    EXPECT_EQ(expectedResult,
              seqnoResolution.resolve(*localValue,
                                      meta,
                                      item.getDataType(),
                                      /*isDelete*/ true));
}

TEST_P(ConflictResolutionParamTest, LwwResolution_Del) {
    // clang-format off
    static const std::vector<ConflictResolutionParamTest::ParamType>
            acceptCases{
                    {Cas::Higher, Rev::Any, ExpTime::Any, Flags::Any, Xattrs::Any},
                    {Cas::Same, Rev::Higher, ExpTime::Any, Flags::Any, Xattrs::Any},
            };
    // clang-format on

    Item item = getTestItem();
    const auto& meta = item.getMetaData();

    ConflictResolution::Result expectedResult;
    if (paramMatchesAnyCase(acceptCases)) {
        expectedResult = ConflictResolution::Result::Accept;
    } else if (isIdentical(/*isDelete*/ true)) {
        expectedResult = ConflictResolution::Result::RejectIdentical;
    } else {
        expectedResult = ConflictResolution::Result::RejectBehind;
    }

    // check that a del with meta would be accepted/rejected
    EXPECT_EQ(expectedResult,
              lwwResolution.resolve(*localValue,
                                    meta,
                                    item.getDataType(),
                                    /*isDelete*/ true));
}

using namespace ::testing;

INSTANTIATE_TEST_SUITE_P(
        ConflictResolutionParamTest,
        ConflictResolutionParamTest,
        Combine(Values(Cas::Lower, Cas::Same, Cas::Higher),
                Values(Rev::Lower, Rev::Same, Rev::Higher),
                Values(ExpTime::Lower, ExpTime::Same, ExpTime::Higher),
                Values(Flags::Lower, Flags::Same, Flags::Higher),
                Values(Xattrs::None,
                       Xattrs::ExistingOnly,
                       Xattrs::IncomingOnly,
                       Xattrs::Both)),
        ConflictResolutionParamTest::PrintToStringParamName);
