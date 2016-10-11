/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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

#include <istream>
#include <ostream>
#include <string>

/**
 * Convert an isasl.pw style password file to the json-style
 * password database
 *
 * @param ifile input file
 * @param ofile output file
 * @throws std::runtime_error if we're failing to open files
 * @throws std::bad_alloc for memory issues
 */
void cbsasl_pwconv(const std::string& ifile, const std::string& ofile);

/**
 * Convert an isasl.pw style password stream to the json-style
 * password stream
 *
 * @param ifile input stream
 * @param ofile output stream
 * @throws std::bad_alloc for memory issues
 */
void cbsasl_pwconv(std::istream& is, std::ostream& os);

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
std::string cbsasl_read_password_file(const std::string& filename);

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
void cbsasl_write_password_file(const std::string& filename,
                                const std::string& content);
