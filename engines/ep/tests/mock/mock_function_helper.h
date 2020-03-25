/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc
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

#include <folly/portability/GMock.h>

#include <functional>

// MockFunction.AsStdFunction exists, but due to MB-37860 could not
// be used under windows. For now, this serves as a replacement
template <class Return, class... Args>
std::function<Return(Args...)> asStdFunction(
        testing::MockFunction<Return(Args...)>& mockFunction) {
    return [&mockFunction](Args&&... args) {
        return mockFunction.Call(std::forward<Args>(args)...);
    };
}