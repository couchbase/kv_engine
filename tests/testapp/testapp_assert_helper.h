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
#pragma once

#include "config.h"
#include "testapp.h"
#include <sstream>
#include <type_traits>

#include <utilities/protocol2text.h>
#include <include/memcached/util.h>

/**
 * Unfortunately I'm largely recreating some of GTest's internals for this
 * function. GTest provides similar helpers like these, but they are not
 * intended for public consumption. Ultimately, many tests call this, so
 * it's best to favor clear messages over small code.
 */
class AssertHelper {
public:
    AssertHelper() : cur_failure(::testing::AssertionFailure()) {
    }

    AssertHelper(::testing::AssertionResult& existing)
        : cur_failure(existing), has_failure(!cur_failure) {
    }

    void combine(::testing::AssertionResult& existing) {
        if (!existing) {
            has_failure = true;
            cur_failure << existing;
        }
    }

    template <typename T1, typename T2>
    ::testing::AssertionResult& eq(const char* astr, const char* bstr,
                                   const T1& a, const T2& b) {
        if (a != b) {
            return assertOp(astr, bstr, a, b, "==");
        } else {
            return returnDummy();
        }
    }

    template <typename T1, typename T2>
    ::testing::AssertionResult& ne(const char* astr, const char* bstr,
                                   const T1& a, const T2& b) {
        if (a == b) {
            return assertOp(astr, bstr, a, b, "!=");
        } else {
            return returnDummy();
        }
    }

    template <typename T1, typename T2>
    ::testing::AssertionResult gt(const char* astr, const char* bstr,
                                  const T1& a, const T2& b) {
        if (!(a > b)) {
            return assertOp(astr, bstr, a, b, ">");
        } else {
            return returnDummy();
        }
    }

    template <typename T1, typename T2>
    ::testing::AssertionResult ge(const char* astr, const char* bstr,
                                   const T1& a, const T2& b) {
        if (!(a >= b)) {
            return assertOp(astr, bstr, a, b, ">=");
        } else {
            return returnDummy();
        }
    }

    template <typename T>
    ::testing::AssertionResult empty(const char* s, const T& a) {
        if (!a.empty()) {
            return assertOp("empty()", std::to_string(a.size()), "<EMPTY>");
        } else {
            return returnDummy();
        }
    }

    ::testing::AssertionResult& fail(const char* msg = nullptr) {
        if (msg != nullptr) {
            cur_failure << msg << std::endl;
        }
        has_failure = true;
        return cur_failure;
    }

    ::testing::AssertionResult result() {
        if (has_failure) {
            return cur_failure;
        } else {
            return ::testing::AssertionSuccess();
        }
    }

    bool hasFailure() const {
        return has_failure;
    }

private:
    template <typename T>
    std::string formatArg(const T& arg) const {
        return std::to_string(arg);
    }

    // Don't call this variant iff the first argument is an enum and the second
    // argument isn't. We want to convert it in the other variant
    template <typename T1, typename T2,
              typename std::enable_if<
                  !std::is_enum<T1>::value || std::is_same<T1,T2>::value,
                  bool>::type = true
             >
    ::testing::AssertionResult& assertOp(const char* expstr, const char* gotstr,
                                         const T1& expected, const T2& got,
                                         const char* opstr) {
        return fail()
                << "Expected: " << std::endl
                << "  " << gotstr << " " << opstr << " " << expstr
                << " (" << formatArg(expected) << ")" << std::endl
                << "Actual: " << std::endl
                << "  " << gotstr << ": " << formatArg(got) << std::endl;
    }

    // Called to coerce the second argument into an enum.
    template <typename T1, typename T2,
              typename std::enable_if<
                  std::is_enum<T1>::value && !std::is_same<T1, T2>::value,
                  bool>::type = true
             >
    ::testing::AssertionResult& assertOp(const char* expstr, const char *gotstr,
                                         const T1& expected, const T2& got,
                                         const char* opstr) {
        auto converted_got = static_cast<T1>(got);
        return assertOp(expstr, gotstr, expected, converted_got, opstr);
    }

    ::testing::AssertionResult& returnDummy() {
        static auto tmp = ::testing::AssertionSuccess();
        return tmp;
    }
    ::testing::AssertionResult cur_failure;
    bool has_failure = false;
};

template<>
inline std::string AssertHelper::formatArg(const protocol_binary_response_status& arg) const {
    // We want a hex representation
    std::stringstream ss;
    ss << memcached_status_2_text(arg) << " (0x" << std::hex << arg << ")";
    return ss.str();
}

template<>
inline std::string AssertHelper::formatArg(const protocol_binary_command& cmd) const {
    std::stringstream ss;
    ss << memcached_opcode_2_text(cmd) << "(0x" << std::hex << cmd << ")";
    return ss.str();
}

template<>
inline std::string AssertHelper::formatArg(const std::string& s) const {
    return s;
}

#define TESTAPP_EXPECT_EQ(helper, a, b) helper.eq(#a, #b, a, b)
#define TESTAPP_EXPECT_NE(helper, a, b) helper.ne(#b, #b, a, b)
#define TESTAPP_EXPECT_GE(helper, a, b) helper.ge(#a, #b, a, b)
#define TESTAPP_EXPECT_GT(helper, a, b) helper.gt(#a, #b, a, b)
