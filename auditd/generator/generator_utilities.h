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

#include <nlohmann/json_fwd.hpp>
#include <gsl/gsl>
#include <list>
#include <string>

class Module;
class Event;

/**
 * Is this build for enterprise edition?
 *
 * @return true when building EE, false for CE
 */
bool is_enterprise_edition();

/**
 * In order to allow making unit tests we want to be able to mock the
 * enterprise edition settings dynamically
 */
void set_enterprise_edition(bool enable);

/**
 * Load the requested file and parse it as JSON
 *
 * @param fname the name of the file
 * @return the json representation of the file
 * @throws std::system_error if we fail to read the file
 *         std::runtime_error if we fail to parse the content of the file
 */
nlohmann::json load_file(const std::string& fname);

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
void parse_module_descriptors(const nlohmann::json&,
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
