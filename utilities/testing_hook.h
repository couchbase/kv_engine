/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2021 Couchbase, Inc
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
