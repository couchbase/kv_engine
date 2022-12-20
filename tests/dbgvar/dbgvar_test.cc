/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <folly/CPortability.h>
#include <folly/portability/GTest.h>

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string_view>

#include "utilities/debug_variable.h"

class DebugVariableTest : public ::testing::Test {};

/**
 * Creates a readable representation of the contents of the string_view.
 */
std::string dumpBytes(std::string_view sv) {
    std::stringstream ss;
    ss << std::hex;

    int i = 0;
    for (char ch : sv) {
        if (isprint(ch)) {
            ss << ' ' << ch << ' ';
        } else {
            ss << std::setfill('0') << std::setw(2) << int(uint8_t(ch)) << ' ';
        }
        if (i++ % 8 == 0) {
            ss << '\n';
        }
    }
    ss << '\n';
    return ss.str();
}

template <typename F, typename... Args>
FOLLY_NOINLINE auto callNonInline(F&& f, Args&&... args) -> decltype(auto) {
    return f(std::forward<Args>(args)...);
}

/**
 * Memcpy implementation which is not checked by sanitizers. We use this to be
 * able to copy bytes off the stack in StackCapture.
 */
FOLLY_NOINLINE FOLLY_DISABLE_SANITIZERS void* memcpy_nosanitize(
        void* dest, const void* src, std::size_t count) {
    char* it = reinterpret_cast<char*>(dest);
    char* end = it + count;
    const char* inputIt = reinterpret_cast<const char*>(src);

    while (it != end) {
        *it = *inputIt;
        it++;
        inputIt++;
    }

    return dest;
}

/**
 * A callable object which will record the contents of program memory between
 * the address of the *this object and the stack frame of the operator().
 */
struct StackCapture {
public:
    StackCapture() = default;

    StackCapture(const StackCapture&) = delete;
    StackCapture(StackCapture&&) = delete;
    StackCapture& operator=(const StackCapture&) = delete;
    StackCapture& operator=(StackCapture&&) = delete;

    FOLLY_NOINLINE void operator()() {
        const char* startMarker = reinterpret_cast<const char*>(this);
        const char* endMarker = reinterpret_cast<const char*>(&startMarker);
        // Adjust for different stack growth strategies.
        if (startMarker > endMarker) {
            std::swap(startMarker, endMarker);
        }

        // Record the stack between *this and the stack frame of this method.
        std::size_t bytesToCopy = endMarker - startMarker;
        stackBytes.resize(bytesToCopy);
        memcpy_nosanitize(stackBytes.data(), startMarker, bytesToCopy);
    }

    std::string_view getBytes() const {
        return stackBytes;
    }

private:
    std::string stackBytes;
};

template <typename F>
FOLLY_NOINLINE std::string stackCapture(F&& f) {
    StackCapture sc;
    callNonInline(f, sc);
    return std::string(sc.getBytes());
}

TEST_F(DebugVariableTest, StringPresentInStack) {
    auto bytes = stackCapture([](auto& capture) {
        cb::DebugVariable var1(cb::toCharArrayN<9>("b"));
        capture();
    });

    // Sanity check.
    ASSERT_EQ(bytes.find("nonexistent"), std::string_view::npos);

    EXPECT_NE(bytes.find({"b\0\0\0\0\0\0\0\0", 9}), std::string_view::npos)
            << "Actual: " << dumpBytes(bytes);
}

TEST_F(DebugVariableTest, TruncatedStringPresentInStack) {
    auto val = cb::toCharArrayN<8>("blarblarblar");
    auto bytes = stackCapture([&](auto& capture) {
        cb::DebugVariable var1(val);
        capture();
    });
    EXPECT_NE(bytes.find("blarblar"), std::string_view::npos)
            << "Actual: " << dumpBytes(bytes);
    EXPECT_EQ(bytes.find("blarblarblar"), std::string_view::npos)
            << "Actual: " << dumpBytes(bytes);
}

TEST_F(DebugVariableTest, VarsDoNotLeak) {
    auto bytes = stackCapture([](auto& capture) {
        cb::DebugVariable var1(cb::toCharArrayN<3>("foo"));
        capture();
        cb::DebugVariable var2(cb::toCharArrayN<3>("bar"));
    });

    EXPECT_NE(bytes.find("foo"), std::string_view::npos)
            << "Actual: " << dumpBytes(bytes);
    EXPECT_EQ(bytes.find("bar"), std::string_view::npos)
            << "Actual: " << dumpBytes(bytes);
}

TEST_F(DebugVariableTest, MultipleVars) {
    auto bytes = stackCapture([](auto& capture) {
        cb::DebugVariable var1(cb::toCharArrayN<3>("abc"));
        cb::DebugVariable var2(cb::toCharArrayN<3>("def"));
        cb::DebugVariable var3(cb::toCharArrayN<3>("ghi"));
        capture();
    });
    EXPECT_NE(bytes.find("abc"), std::string_view::npos)
            << "Actual: " << dumpBytes(bytes);
    EXPECT_NE(bytes.find("def"), std::string_view::npos)
            << "Actual: " << dumpBytes(bytes);
    EXPECT_NE(bytes.find("ghi"), std::string_view::npos)
            << "Actual: " << dumpBytes(bytes);
}
