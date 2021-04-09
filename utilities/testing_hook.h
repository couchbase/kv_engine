/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2021-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <functional>

/**
 * Testing hook which can be used to execute some code in a particular place
 * in the codebase.
 * Essentially this is a std::function which does nothing if empty (not set),
 * as opposed to throwing std::bad_function_call as std::function itself does.
 */
template <class... Args>
struct TestingHook : public std::function<void(Args...)> {
    using FuncType = std::function<void(Args...)>;

    void operator=(FuncType&& other) {
        FuncType::operator=(other);
    }

    void operator=(const FuncType& other) {
        FuncType::operator=(other);
    }

    void operator()(Args... args) {
        if (*this) {
            FuncType::operator()(args...);
        }
    }

    void operator()(Args... args) const {
        if (*this) {
            FuncType::operator()(args...);
        }
    }
};
