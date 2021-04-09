/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <algorithm>
#include <cstdint>
#include <functional>
#include <vector>

/*
 * A Function class stores a function pointer for later storage in
 * a FunctionChain.
 *
 * @tparam ReturnType must be the function's return type.
 * @tparam ReturnType Success() must be a function that returns a value which is
 *                              considered to successful.
 * @tparam arguments are a variable pack of function args.
 *
 */
template <typename ReturnType, ReturnType Success(), typename... arguments>
class Function {
public:
    /*
     * Construct a Function that stores 'f'
     */
    explicit Function(std::function<ReturnType(arguments...)> f) : func(f) {
    }

    /*
     * Call the stored function
     */
    ReturnType operator()(arguments... args) const {
        return func(args...);
    }

    /*
     * Expose the stored function's address (required for == operator)
     */
    uintptr_t getAddress() const {
        typedef ReturnType(fnType)(arguments...);
        return reinterpret_cast<uintptr_t>(*func.template target<fnType*>());
    }

private:
    std::function<ReturnType(arguments...)> func;
};

/*
 * Function == operator: required for std::find used in FunctionChain
 */
template <typename ReturnType, ReturnType Success(), typename... arguments>
bool operator==(const Function<ReturnType, Success, arguments...>& a,
                const Function<ReturnType, Success, arguments...>& b) {
    return a.getAddress() == b.getAddress();
}

/*
 * Factory function to create a Function object from a function pointer
 */
template <typename ReturnType, ReturnType Success(), typename... arguments>
Function<ReturnType, Success, arguments...> makeFunction(
        ReturnType (*f)(arguments...)) {
    return Function<ReturnType, Success, arguments...>(f);
}

/*
 * FunctionChain stores a list of Function objects.
 * An invoke method attempts to call all Function's in the chain, stopping
 * when a Function doesn't return Success.
 *
 * An empty chain returns Success.
 *
 * ReturnType must be the function's return type.
 * ReturnType Success must be a value to be returned when the function is
 *  successful.
 * arguments are a variable pack of function args.
 *
 */
template <typename ReturnType, ReturnType Success(), typename... arguments>
class FunctionChain {
public:
    /*
     * push the Function only if it's not already in the FunctionChain
     */
    void push_unique(Function<ReturnType, Success, arguments...> f) {
        if (chain.end() == std::find(chain.begin(), chain.end(), f)) {
           chain.push_back(f);
        }
    }

    /*
     * Invoke the chain, stopping if a Function doesn't return Success.
     */
    ReturnType invoke(arguments... args) const {
        ReturnType rval = Success();

        for (auto function : chain) {
            if ((rval = function(args...)) != Success()) {
                return rval;
            }
        }

        return rval;
    }

    bool empty() const {
        return chain.empty();
    }

private:
    /*
     * Vector to store the chain of Function objects.
     */
    std::vector<Function<ReturnType, Success, arguments...> > chain;
};
