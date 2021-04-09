/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include <iostream>
#include <string>

namespace cb::sasl::pwdb {

/**
 * Convert an isasl.pw style password file to the json-style
 * password database
 *
 * @param ifile input file
 * @param ofile output file
 * @throws std::runtime_error if we're failing to open files
 * @throws std::bad_alloc for memory issues
 */
void convert(const std::string& ifile, const std::string& ofile);

/**
 * Convert an isasl.pw style password stream to the json-style
 * password stream
 *
 * @param ifile input stream
 * @param ofile output stream
 * @throws std::bad_alloc for memory issues
 */
void convert(std::istream& is, std::ostream& os);

/**
 * Read the password file from the specified filename.
 *
 * If the environment variable `COUCHBASE_CBSASL_SECRETS` is set it
 * contains the cipher, key and iv to use to decrypt the file.
 *
 * @param filename the name of the file to read
 * @return the content of the file
 * @throws std::exception if an error occurs while reading or decrypting
 *                        the content
 */
std::string read_password_file(const std::string& filename);

/**
 * Write the password data to the specified filename.
 *
 * If the environment variable `COUCHBASE_CBSASL_SECRETS` is set it
 * contains the cipher, key and iv to use to encrypt the file.
 *
 * @param filename the name of the file to write
 * @param content the data to write
 * @throws std::exception if an error occurs while reading or encrypting
 *                        the content
 */
void write_password_file(const std::string& filename,
                         const std::string& content);

} // namespace cb::sasl::pwdb
