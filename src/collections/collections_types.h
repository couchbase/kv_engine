/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

#include <platform/sized_buffer.h>

namespace Collections {

// The reserved name of the system owned, default collection.
const char* const _DefaultCollectionIdentifier = "$default";
static cb::const_char_buffer DefaultCollectionIdentifier(
        _DefaultCollectionIdentifier);

// The default separator we will use for identifying collections in keys.
const char* const DefaultSeparator = "::";

// SystemEvent keys
const char* const CreateEventKey = "$collections::create:";
const char* const DeleteEventKey = "$collections::delete:";

} // end namespace Collections
