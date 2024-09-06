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

#include <string>

/**
 * Apply https://www.ietf.org/rfc/rfc4013.txt to the input string
 *
 * The input string is supposed to be UTF-8 (but given that we don't
 * support bucket names with multibyte characters, we only support
 * single-byte UTF-8 characters ;-))
 *
 * @param string The string to run stringprep with the SASL profile on
 * @return a SASLPrep'd string
 * @throws std::runtime_error if we encounter a multibyte character
 */
std::string SASLPrep(const std::string& string);
