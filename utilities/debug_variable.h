/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <folly/lang/Hint.h>
#include <algorithm>
#include <array>
#include <cstring>
#include <stdexcept>
#include <string_view>
#include <utility>

namespace cb {

/**
 * Copies the input into a fixed-size char array, truncating it if longer than
 * the size of the array.
 *
 * If N == s.size(), no null-terminator will be inserted.
 */
template <std::size_t N>
constexpr std::array<char, N> toCharArrayN(std::string_view s) noexcept {
    static_assert(N > 1, "String will always be empty.");
    std::array<char, N> m{};
    std::memcpy(m.data(), s.data(), std::min(N, s.size()));
    return m;
}

/**
 * A container for a value which uses compiler hints to keep the object on the
 * program stack. This means that DebugVariables show up in crash dumps, and
 * makes them a useful debugging aid.
 *
 * If the value is a string type, it is best to use toCharArrayN to create a
 * fixed-size char array, because std::string with SVO is hard to read and
 * char* will only show the address of the string.
 *
 * ### Implementation
 *
 * Folly provides a "compiler hint" compiler_must_not_elide, which prevents
 * elision by the compiler of the given value.
 *
 * By applying compiler_must_not_elide to the `this` pointer, we effectively
 * require that the object to be present somewhere in memory (because the
 * pointer must be valid and not elided). We do this compiler_must_not_elide
 * trick twice - once in the constructor, and once in the destructor, ensuring
 * that the object "exists" in memory at both times.
 *
 * This is reliant on how the compiler decides to allocate objects, however,
 * in practice, gcc and clang will allocate this object on the stack and keep it
 * there until the destructor runs.
 *
 * There is an alternative possible implementation that an imaginary compiler
 * could use -- push the object on the stack, call the constructor, pop the
 * object off the stack, do some stuff, then push on the stack again, call the
 * destructor and pop it off for good. This is less optimal and would make sense
 * if the compiler were trying to save stack memory, however no compiler I
 * tested with does this.
 */
template <typename T>
class [[maybe_unused]] DebugVariable {
public:
    // Dynamic allocation does not make sense for this object. It must be
    // allocated on the stack.
    void* operator new(std::size_t) = delete;

    /**
     * Construct a DebugVariable.
     */
    constexpr DebugVariable(const T& v) : value(std::move(v)) {
        static_assert(!std::is_pointer_v<T> && !std::is_same_v<void*, T> &&
                              !std::is_same_v<const void*, T>,
                      "Storing a pointer doesn't make sense in most cases. "
                      "Use a void* if you want the address to be visible on "
                      "the stack.");

        // Value must be in memory when the constructor runs.
        folly::compiler_must_not_elide(this);
    }

    DebugVariable(const DebugVariable&) = delete;
    DebugVariable(DebugVariable&&) = delete;

    DebugVariable& operator=(const DebugVariable&) = delete;
    DebugVariable& operator=(DebugVariable&&) = delete;

    ~DebugVariable() {
        // Value must be in memory when the destructor runs.
        folly::compiler_must_not_elide(this);
    }

private:
    T value;
};

} // namespace cb
