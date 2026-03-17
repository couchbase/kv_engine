# AGENTS.md — kv_engine Repository Guide

Repository (mirror): https://github.com/couchbase/kv_engine Primary code review
system: https://review.couchbase.org

---

## 📌 Purpose

This document defines expectations for:

- Automated coding agents
- Human contributors
- Reviewers

kv_engine uses Gerrit (review.couchbase.org) for all change submission,
validation, and approval.

GitHub is a mirror only. Do NOT open GitHub Pull Requests.

---

## 🧱 Repository Overview

Welcome to the Couchbase _KV-Engine_ project.

The committed README.md provides a high-level overview of the project and is a
good starting point for new contributors. For more detailed information on
architecture, protocols, and other topics, please refer to the various
documentation files linked from the README:

@instructions/README.md


## 🚦 Authoritative Contribution Workflow

All changes must be submitted via:

    https://review.couchbase.org

### Standard Flow

1. Sync with latest target branch (typically `master`)
2. Create a local topic branch
3. Make atomic, focused commits
4. Always create suitable unit tests
5. Run local build and tests
6. Push to Gerrit (substitute master for correct branch when working on named
   branches):

       git push gerrit HEAD:refs/for/master

7. CI validation runs automatically on jenkins servers that are monitoring for
   new code pushed to gerrit. 8. Review feedback is asynchronous

---

## 📐 Code Standards

Language: **C++ (up to C++20 supported)**

Guidelines:

- Prefer modern C++ where appropriate (C++17/20 features are supported)
- Use RAII, smart pointers, and standard library facilities where they improve
  safety and clarity
- Structured bindings, `std::optional`, `std::variant`, `constexpr`, and other
  modern features are acceptable
- Match the surrounding style within the modified module. This is particularly
  important as the style of ep-engine differs vastly from the style of daemon
  code.
- It is acceptable to use older constructs if required for consistency with the
  existing code in that area
- Avoid unnecessary large-scale modernization in unrelated code
- Maintain cross-platform compatibility
- New source files must use the current year in the banner comment.
- The `C++ Core Guidelines<http://isocpp.github.io/CppCoreGuidelines/CppCoreGuidelines>`
  are also a key part of our coding style.
- If in doubt, full coding standards are defined by
  `docs/CodingStandards.rst`.
- Always build (see building hints and testing hints) before running tests!

---

## Building hints

The project uses CMake for build configuration.

- To build all code (including all tests) use the target `kv_engine_everything` which can be
  invoked from `build/`
- Prefer to reuse an existing build directory and do incremental builds.
  - Do not create a new build directory unless asked.
  - Check for an existing `build/` directory in the directory above the
   `kv_engine` git repository.
- Prefer `ninja` when building.
- If configuring a new build prefer `ninja` as build tool (`cmake -G Ninja`)

## Testing hints

When making code changes just to code `engines/ep` a preferred test stragtegy is:

1. From the `build/kv_engine` directory run engine_ep_unit_tests tests e.g. `ctest -R ep-engine_ep_unit_tests`
2. From the `build/kv_engine` directory run ep_testsuite e.g. `ctest -R ep_testsuite`

Only run step 2 if step 1 is successful.

If in doubt that the code changes are covered by the above the entire test suite
can be invoked e.g. from `build/kv_engine` just run `ctest`

---

## 🚨 Safety Rules

Agents must not:

- Change file formats silently
- Break backward compatibility unintentionally
- Reformat large areas of code
- Submit speculative refactors
- Bypass Gerrit workflow
- Create new build directories unless asked

## 📝 Commit Message Requirements

All commit messages must:

- Start with: `MB-XXXXX: Short summary`
- Be wrapped at **72 characters per line**
- Include a clear explanation of:
  - What changed
  - Why it changed
  - Risk or compatibility impact

Example:

```
MB-12345: Fix incorrect StoredValue::size calculation

Correct off-by-one error in StoredValue::size when the value is
compressed.
```

### Change-Id Handling

Do NOT manually add or generate a `Change-Id`.

A local Gerrit commit-msg hook is expected to already be installed and will
automatically add the required `Change-Id` footer when the commit is created
or amended.

Agents must not fabricate or modify the Change-Id.

---

## 🧪 Testing Requirements

All functional changes must include:

- New or updated unit tests
- Passing test suite locally
- No unintended regression in file format compatibility


