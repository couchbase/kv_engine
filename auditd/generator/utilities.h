/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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

#include <cJSON_utils.h>
#include <gsl/gsl>
#include <list>
#include <string>

struct cJSON;
class Module;
class Event;

/**
 * Search through the provided JSON object for the attribute with
 * the given name and validate that the type is correct.
 *
 * @param root the JSON to search
 * @param name the attribute to search for
 * @param type The expected type (or -1 for a boolean type as cJSON
 *             use two different types for boolean values)
 * @return The object
 * @throws std::logic_error if the requested attribute isn't found / wrong type
 */
cJSON* getMandatoryObject(gsl::not_null<const cJSON*> root,
                          const std::string& name,
                          int type);

/**
 * Search through the provided JSON object for the attribute with
 * the given name and validate that the type is correct.
 *
 * @param root the JSON to search
 * @param name the attribute to search for
 * @param type The expected type (or -1 for a boolean type as cJSON
 *             use two different types for boolean values)
 * @return The object or nullptr if the object isn't found
 * @throws std::logic_error if the requested attribute has the wrong type
 */
cJSON* getOptionalObject(gsl::not_null<const cJSON*> root,
                         const std::string& name,
                         int type);

/**
 * Is this build for enterprise edition?
 *
 * @return true when building EE, false for CE
 */
bool is_enterprise_edition();

/**
 * Load the requested file and parse it as JSON
 *
 * @param fname the name of the file
 * @return the cJSON representation of the file
 * @throws std::system_error if we fail to read the file
 *         std::logic_error if we fail to parse the content of the file
 */
unique_cJSON_ptr load_file(const std::string& fname);

/**
 * Iterate over the module descriptor json and populate each entry
 * in the modules array into the provided modules list.
 *
 * @param ptr The JSON representation of the module description. See
 *            ../README.md for a description of the syntax
 * @param modules Where to store the list of all of the entries found
 * @param srcroot The source root to prepend to all of the paths in the spec
 * @param objroot The object root to prepend to all of the paths in the spec
 * @throws std::invalid_argument if the provided JSON is of an unexpected format
 */
void validate_module_descriptors(gsl::not_null<const cJSON*> ptr,
                                 std::list<std::unique_ptr<Module>>& modules,
                                 const std::string& srcroot,
                                 const std::string& objroot);

/**
 * Build the master event file
 *
 * @param modules The modules to include
 * @param output_file Where to store the result
 * @throws std::system_error if we fail to write the file
 */
void create_master_file(const std::list<std::unique_ptr<Module>>& modules,
                        const std::string& output_file);
