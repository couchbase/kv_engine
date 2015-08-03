/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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

/*
 * Traits classes for Sub-document API commands.
 *
 * Used by both the validators and executors.
 */

#pragma once

#include "config.h"

#include <memcached/protocol_binary.h>
#include <subdoc/operations.h>


/* Convert integers to types, to allow type traits for different
 * protocol_binary_commands, etc.
 */
template <protocol_binary_command C>
struct Cmd2Type
{
  enum { value = C };
};

/* Traits of each of the sub-document commands. These are used to build up
 * the individual implementations using generic building blocks:
 *
 *   optype: The subjson API optype for this command.
 *   request_has_value: Does the command request require a value?
 *   allow_empty_path: Is the path field allowed to be empty (zero-length)?
 *   response_has_value: Does the command response require a value?
 *   is_mutator: Does the command mutate (modify) the document?
 *   valid_flags: What flags are valid for this command.
 */
template <typename T>
struct cmd_traits;

template <>
struct cmd_traits<Cmd2Type<PROTOCOL_BINARY_CMD_SUBDOC_GET> > {
  static const Subdoc::Command::Code optype = Subdoc::Command::GET;
  static const bool request_has_value = false;
  static const bool allow_empty_path = false;
  static const bool response_has_value = true;
  static const bool is_mutator = false;
  static const protocol_binary_subdoc_flag valid_flags =
      protocol_binary_subdoc_flag(0);
};

template <>
struct cmd_traits<Cmd2Type<PROTOCOL_BINARY_CMD_SUBDOC_EXISTS> > {
  static const Subdoc::Command::Code optype = Subdoc::Command::EXISTS;
  static const bool request_has_value = false;
  static const bool allow_empty_path = false;
  static const bool response_has_value = false;
  static const bool is_mutator = false;
  static const protocol_binary_subdoc_flag valid_flags =
      protocol_binary_subdoc_flag(0);
};

template <>
struct cmd_traits<Cmd2Type<PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD> > {
  static const Subdoc::Command::Code optype = Subdoc::Command::DICT_ADD;
  static const bool request_has_value = true;
  static const bool allow_empty_path = false;
  static const bool response_has_value = false;
  static const bool is_mutator = true;
  static const protocol_binary_subdoc_flag valid_flags = SUBDOC_FLAG_MKDIR_P;
};

template <>
struct cmd_traits<Cmd2Type<PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT> > {
  static const Subdoc::Command::Code optype = Subdoc::Command::DICT_UPSERT;
  static const bool request_has_value = true;
  static const bool allow_empty_path = false;
  static const bool response_has_value = false;
  static const bool is_mutator = true;
  static const protocol_binary_subdoc_flag valid_flags = SUBDOC_FLAG_MKDIR_P;
};

template <>
struct cmd_traits<Cmd2Type<PROTOCOL_BINARY_CMD_SUBDOC_DELETE> > {
  static const Subdoc::Command::Code optype = Subdoc::Command::REMOVE;
  static const bool request_has_value = false;
  static const bool allow_empty_path = false;
  static const bool response_has_value = false;
  static const bool is_mutator = true;
  static const protocol_binary_subdoc_flag valid_flags =
          protocol_binary_subdoc_flag(0);
};

template <>
struct cmd_traits<Cmd2Type<PROTOCOL_BINARY_CMD_SUBDOC_REPLACE> > {
  static const Subdoc::Command::Code optype = Subdoc::Command::REPLACE;
  static const bool request_has_value = true;
  static const bool allow_empty_path = false;
  static const bool response_has_value = false;
  static const bool is_mutator = true;
  static const protocol_binary_subdoc_flag valid_flags =
          protocol_binary_subdoc_flag(0);
};

template <>
struct cmd_traits<Cmd2Type<PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST> > {
  static const Subdoc::Command::Code optype = Subdoc::Command::ARRAY_APPEND;
  static const bool request_has_value = true;
  static const bool allow_empty_path = true;
  static const bool response_has_value = false;
  static const bool is_mutator = true;
  static const protocol_binary_subdoc_flag valid_flags = SUBDOC_FLAG_MKDIR_P;
};

template <>
struct cmd_traits<Cmd2Type<PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST> > {
  static const Subdoc::Command::Code optype = Subdoc::Command::ARRAY_PREPEND;
  static const bool request_has_value = true;
  static const bool allow_empty_path = true;
  static const bool response_has_value = false;
  static const bool is_mutator = true;
  static const protocol_binary_subdoc_flag valid_flags = SUBDOC_FLAG_MKDIR_P;
};

template <>
struct cmd_traits<Cmd2Type<PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT> > {
  static const Subdoc::Command::Code optype = Subdoc::Command::ARRAY_INSERT;
  static const bool request_has_value = true;
  static const bool allow_empty_path = false;
  static const bool response_has_value = false;
  static const bool is_mutator = true;
  static const protocol_binary_subdoc_flag valid_flags =
          protocol_binary_subdoc_flag(0);
};

template <>
struct cmd_traits<Cmd2Type<PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE> > {
  static const Subdoc::Command::Code optype = Subdoc::Command::ARRAY_ADD_UNIQUE;
  static const bool request_has_value = true;
  static const bool allow_empty_path = true;
  static const bool response_has_value = false;
  static const bool is_mutator = true;
  static const protocol_binary_subdoc_flag valid_flags = SUBDOC_FLAG_MKDIR_P;
};

template <>
struct cmd_traits<Cmd2Type<PROTOCOL_BINARY_CMD_SUBDOC_COUNTER> > {
  static const Subdoc::Command::Code optype = Subdoc::Command::COUNTER;
  static const bool request_has_value = true;
  static const bool allow_empty_path = true;
  static const bool response_has_value = true;
  static const bool is_mutator = true;
  static const protocol_binary_subdoc_flag valid_flags = SUBDOC_FLAG_MKDIR_P;
};
