/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

#include <string>

namespace cb::rangescan {

// client can request a Key or Key/Value scan.
enum class KeyOnly : char { No, Yes };

// How the key is to be interpreted in a range start/end
enum class KeyType : char { Inclusive };

// KeyView wraps a std::string_view and is the type passed through from
// executor to engine.
class KeyView {
public:
    /**
     * Construct a KeyView onto key/len
     */
    KeyView(const char* key, size_t len) : key{key, len} {
    }

    /**
     * Construct a KeyView onto a const char* (this is to support test code
     * where string literals are in use)
     */
    KeyView(std::string_view key) : key{key} {
    }

    std::string_view getKeyView() const {
        return key;
    }

    bool isInclusive() const {
        return type == KeyType::Inclusive;
    }

private:
    std::string_view key;
    KeyType type;
};

} // namespace cb::rangescan
