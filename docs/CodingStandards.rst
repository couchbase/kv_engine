=====================
KV-Engine Coding Standards
=====================

.. contents::
   :local:

Couchbase Editors's Note
==============

This document is a fork of LLVM's `CodingStandards
<http://llvm.org/docs/CodingStandards.html>`_, with additions /
removals as appropriate for KV-Engine. This was choden as a staritng
point (instead of creating our own from scratch) because the vast
majority of LLVM's document is applicable to Couchbase:

- LLVM has a number of parallels with KV-Engine (systems /
  infrastructure software implemented in Modern C++, but needs to
  support multiple platforms so not /too/ modern).
- LLVM's coding style is similar to ours.

Having said that, there are a number of differences. Those which are
simply textual have been search-replaced (LLVM -> KV-Engine); others
which are not at all relevent simply deleted.

The remaining differences are *aspirational* - the LLVM standard (and
hence now this one ;) ) - contains many suggestions for good
style which I would like to encourage in our code.  While we don't at
time of writing meet all these guidelines, we should strive to.

    CB Note:
     Couchbase-specific annotations/commentary are indicated
     with (CB Note:) tag (such as here).  These are used when it is
     useful to keep the original LLVM content, but highlight how the
     KV-Engine standards differ.

Introduction
============

This document attempts to describe a few coding standards that are being used in
the KV-Engine source tree.  Although no coding standards should be regarded as
absolute requirements to be followed in all instances, coding standards are
particularly important for large-scale code bases that follow a library-based
design (like KV-Engine).

While this document may provide guidance for some mechanical formatting issues,
whitespace, or other "microscopic details", these are not fixed standards.
Always follow the golden rule:

.. _Golden Rule:

    **If you are extending, enhancing, or bug fixing already implemented code,
    use the style that is already being used so that the source is uniform and
    easy to follow.**

There are some conventions that are not uniformly followed in the code base
(e.g. the naming convention).  This is because they are relatively new, and a
lot of code was written before they were put in place.  Our long term goal is
for the entire codebase to follow the convention, but we explicitly *do not*
want patches that do large-scale reformating of existing code.  On the other
hand, it is reasonable to rename the methods of a class if you're about to
change it in some other way.  Just do the reformating as a separate commit from
the functionality change.

The ultimate goal of these guidelines is to increase the readability and
maintainability of our common source base. If you have suggestions for topics to
be included, please mail them to `DaveR <mailto:daver.remove.colour@red.couchbase.com>`_.

Languages, Libraries, and Standards
===================================

Most source code in KV-Engine and other KV-Engine projects using these coding standards
is C++ code. There are some places where C code is used either due to
environment restrictions, historical restrictions, or due to third-party source
code imported into the tree. Generally, our preference is for standards
conforming, modern, and portable C++ code as the implementation language of
choice.

C++ Standard Versions
---------------------

KV-Engine is currently written using C++11 conforming code,
although we restrict ourselves to features which are available in the major
toolchains supported as host compilers. Regardless of the supported features, code is expected to (when
reasonable) be standard, portable, and modern C++11 code. We avoid unnecessary
vendor-specific extensions, etc.

C++ Standard Library
--------------------

Use the C++ standard library facilities whenever they are available for
a particular task. KV-Engine and related projects emphasize and rely on the standard
library facilities for as much as possible. Common support libraries providing
functionality missing from the standard library for which there are standard
interfaces or active work on adding standard interfaces will often be
implemented in the `cb` namespace following the expected standard interface.

Supported C++11 Language and Library Features
---------------------------------------------

While KV-Engine uses C++11, not all features are available in all of
the toolchains which we support.

The ultimate definition of this set is what build bots with those respective
toolchains accept. Don't argue with the build bots. However, we have some
guidance below to help you know what to expect.

Each toolchain provides a good reference for what it accepts:

* Clang: http://clang.llvm.org/cxx_status.html
* GCC: http://gcc.gnu.org/projects/cxx0x.html
* MSVC: http://msdn.microsoft.com/en-us/library/hh567368.aspx

In most cases, the MSVC list will be the dominating factor. Here is a summary
of the features that are expected to work. Features not on this list are
unlikely to be supported by our host compilers.

    CB Note:
     The following list hasn't been verified against the
     Couchbase builders, but it's probably a reasonable starting point
     for our environment also.

* Rvalue references: N2118_

  * But *not* Rvalue references for ``*this`` or member qualifiers (N2439_)

* Static assert: N1720_
* ``auto`` type deduction: N1984_, N1737_
* Trailing return types: N2541_
* Lambdas: N2927_

  * But *not* lambdas with default arguments.

* ``decltype``: N2343_
* Nested closing right angle brackets: N1757_
* Extern templates: N1987_
* ``nullptr``: N2431_
* Strongly-typed and forward declarable enums: N2347_, N2764_
* Local and unnamed types as template arguments: N2657_
* Range-based for-loop: N2930_

  * But ``{}`` are required around inner ``do {} while()`` loops.  As a result,
    ``{}`` are required around function-like macros inside range-based for
    loops.

* ``override`` and ``final``: N2928_, N3206_, N3272_
* Atomic operations and the C++11 memory model: N2429_
* Variadic templates: N2242_
* Explicit conversion operators: N2437_
* Defaulted and deleted functions: N2346_
* Initializer lists: N2627_
* Delegating constructors: N1986_
* Default member initializers (non-static data member initializers): N2756_

  * Feel free to use these wherever they make sense and where the `=`
    syntax is allowed. Don't use braced initialization syntax.

.. _N2118: http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2006/n2118.html
.. _N2439: http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2007/n2439.htm
.. _N1720: http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2004/n1720.html
.. _N1984: http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2006/n1984.pdf
.. _N1737: http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2004/n1737.pdf
.. _N2541: http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2008/n2541.htm
.. _N2927: http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2009/n2927.pdf
.. _N2343: http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2007/n2343.pdf
.. _N1757: http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2005/n1757.html
.. _N1987: http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2006/n1987.htm
.. _N2431: http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2007/n2431.pdf
.. _N2347: http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2007/n2347.pdf
.. _N2764: http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2008/n2764.pdf
.. _N2657: http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2008/n2657.htm
.. _N2930: http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2009/n2930.html
.. _N2928: http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2009/n2928.htm
.. _N3206: http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2010/n3206.htm
.. _N3272: http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2011/n3272.htm
.. _N2429: http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2007/n2429.htm
.. _N2242: http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2007/n2242.pdf
.. _N2437: http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2007/n2437.pdf
.. _N2346: http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2007/n2346.htm
.. _N2627: http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2008/n2672.htm
.. _N1986: http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2006/n1986.pdf
.. _N2756: http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2008/n2756.htm

The supported features in the C++11 standard libraries are less well tracked,
but also much greater. Most of the standard libraries implement most of C++11's
library. The most likely lowest common denominator is Linux support. For
libc++, the support is just poorly tested and undocumented but expected to be
largely complete. YMMV. For libstdc++, the support is documented in detail in
`the libstdc++ manual`_. There are some very minor missing facilities that are
unlikely to be common problems, and there are a few larger gaps that are worth
being aware of:

* Not all of the type traits are implemented
* No regular expression library.
* While most of the atomics library is well implemented, the fences are
  missing. Fortunately, they are rarely needed.
* The locale support is incomplete.

Other than these areas you should assume the standard library is available and
working as expected until some build bot tells you otherwise. If you're in an
uncertain area of one of the above points, but you cannot test on a Linux
system, your best approach is to minimize your use of these features, and watch
the Linux build bots to find out if your usage triggered a bug.

.. _the libstdc++ manual:
  http://gcc.gnu.org/onlinedocs/gcc-4.8.0/libstdc++/manual/manual/status.html#status.iso.2011

Mechanical Source Issues
========================

Source Code Formatting
----------------------

Commenting
^^^^^^^^^^

Comments are one critical part of readability and maintainability.  Everyone
knows they should comment their code, and so should you.  When writing comments,
write them as English prose, which means they should use proper capitalization,
punctuation, etc.  Aim to describe what the code is trying to do and why, not
*how* it does it at a micro level. Here are a few critical things to document:

.. _header file comment:

File Headers
""""""""""""

Every source file should have a header on it that describes the basic purpose of
the file.  The standard header looks like this:

.. code-block:: c++

  /* -*- MODE: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

  /**
   * Checkpoint Cursor implementation
   *
   * A checkpoint cursor, representing the current position in a Checkpoint
   * series.
   * ...
   */

The main body is a ``doxygen`` comment (identified by the ``/**`` comment
marker instead of the usual ``//``) describing the purpose of the file.  The
first sentence (or a passage beginning with ``@brief``) is used as an abstract.
Any additional information should be separated by a blank line.  If an
algorithm is being implemented or something tricky is going on, a reference
to the paper where it is published should be included, as well as any notes or
*gotchas* in the code to watch out for.

Class overviews
"""""""""""""""

Classes are one fundamental part of a good object oriented design.  As such, a
class definition should have a comment block that explains what the class is
used for and how it works.  Every non-trivial class is expected to have a
``doxygen`` comment block.

Method information
""""""""""""""""""

Methods defined in a class (as well as any global functions) should also be
documented properly.  A quick note about what it does and a description of the
borderline behaviour is all that is necessary here (unless something
particularly tricky or insidious is going on).  The hope is that people can
figure out how to use your interfaces without reading the code itself.

Good things to talk about here are what happens when something unexpected
happens: does the method return null?  Abort?  Format your hard disk?

Comment Formatting
^^^^^^^^^^^^^^^^^^

In general, prefer C++ style comments (``//`` for normal comments, ``///`` for
``doxygen`` documentation comments).  They take less space, require
less typing, don't have nesting problems, etc.  There are a few cases when it is
useful to use C style (``/* */`` for normal, ``/** */`` for ``doxygen``) comments however:

#. When writing C code: Obviously if you are writing C code, use C style
   comments.

#. When writing a header file that may be ``#include``\d by a C source file.

#. When writing a source file that is used by a tool that only accepts C style
   comments.

#. When writing a multi-line comment (3 or more lines).

Commenting out large blocks of code is discouraged, but if you really have to do
this (for documentation purposes or as a suggestion for debug printing), use
``#if 0`` and ``#endif``. These nest properly and are better behaved in general
than C style comments.

Doxygen Use in Documentation Comments
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Use the ``@file`` command to turn the standard file header into a file-level
comment.

Include descriptive paragraphs for all public interfaces (public classes,
member and non-member functions).  Don't just restate the information that can
be inferred from the API name.  The first sentence (or a paragraph beginning
with ``@brief``) is used as an abstract. Try to use a single sentence as the
``@brief`` adds visual clutter.  Put detailed discussion into separate
paragraphs.

To refer to parameter names inside a paragraph, use the ``@p name`` command.
Don't use the ``@arg name`` command since it starts a new paragraph that
contains documentation for the parameter.

Wrap non-inline code examples in ``@code ... @endcode``.

To document a function parameter, start a new paragraph with the
``@param name`` command.  If the parameter is used as an out or an in/out
parameter, use the ``@param [out] name`` or ``@param [in,out] name`` command,
respectively.

To describe function return value, start a new paragraph with the ``@return``
command.

A minimal documentation comment:

.. code-block:: c++

  /// Sets the xyzzy property to @p baz.
  void setXyzzy(bool baz);

A documentation comment that uses all Doxygen features in a preferred way:

.. code-block:: c++

  /// Does foo and bar.
  ///
  /// Does not do foo the usual way if Wp Baz is true.
  ///
  /// Typical usage:
  /// @code
  ///   fooBar(false, "quux", res);
  /// @endcode
  ///
  /// @param quux kind of foo to do.
  /// @param [out] result filled with bar sequence on foo success.
  ///
  /// @return true on success.
  bool fooBar(bool baz, StringRef quux, std::vector<int>& result);

Don't duplicate the documentation comment in the header file and in the
implementation file.  Put the documentation comments for public APIs into the
header file.  Documentation comments for private APIs can go to the
implementation file.  In any case, implementation files can include additional
comments (not necessarily in Doxygen markup) to explain implementation details
as needed.

Don't duplicate function or class name at the beginning of the comment.
For humans it is obvious which function or class is being documented;
automatic documentation processing tools are smart enough to bind the comment
to the correct declaration.

Wrong:

.. code-block:: c++

  // In Something.h:

  /// Something - An abstraction for some complicated thing.
  class Something {
  public:
    /// fooBar - Does foo and bar.
    void fooBar();
  };

  // In Something.cpp:

  /// fooBar - Does foo and bar.
  void Something::fooBar() { ... }

Correct:

.. code-block:: c++

  // In Something.h:

  /// An abstraction for some complicated thing.
  class Something {
  public:
    /// Does foo and bar.
    void fooBar();
  };

  // In Something.cpp:

  // Builds a B-tree in order to do foo.  See paper by...
  void Something::fooBar() { ... }

It is not required to use additional Doxygen features, but sometimes it might
be a good idea to do so.

``#include`` Style
^^^^^^^^^^^^^^^^^^

Immediately after the `header file comment`_ (and ``#pragma once`` guard if working on a
header file), the `minimal list of #includes`_ required by the file should be
listed.  We prefer these ``#include``\s to be listed in this order:

.. _Main Module Header:
.. _Local/Private Headers:

#. Project configuration header (``"config.h"``)
#. Main Module Header
#. Local/Private Headers
#. subproject headers (``platform/...``, ``memcached/...``, etc)
#. System ``#include``\s

and each category should be sorted lexicographically by the full path.

The `Main Module Header`_ file applies to ``.cc`` files which implement an
interface defined by a ``.h`` file.  This ``#include`` should always be included
**first** regardless of where it lives on the file system.  By including a
header file first in the ``.cc`` files that implement the interfaces, we ensure
that the header does not have any hidden dependencies which are not explicitly
``#include``\d in the header, but should be. It is also a form of documentation
in the ``.cc`` file to indicate where the interfaces it implements are defined.

.. _fit into 80 columns:

Source Code Width
^^^^^^^^^^^^^^^^^

Write your code to fit within 80 columns of text.  This helps those of us who
like to print out code and look at your code in an ``xterm`` without resizing
it.

The longer answer is that there must be some limit to the width of the code in
order to reasonably allow developers to have multiple files side-by-side in
windows on a modest display.  If you are going to pick a width limit, it is
somewhat arbitrary but you might as well pick something standard.  Going with 90
columns (for example) instead of 80 columns wouldn't add any significant value
and would be detrimental to printing out code.  Also many other projects have
standardized on 80 columns, so some people have already configured their editors
for it (vs something else, like 90 columns).

This is one of many contentious issues in coding standards, but it is not up for
debate.

Use Spaces Instead of Tabs
^^^^^^^^^^^^^^^^^^^^^^^^^^

In all cases, prefer spaces to tabs in source files.  People have different
preferred indentation levels, and different styles of indentation that they
like; this is fine.  What isn't fine is that different editors/viewers expand
tabs out to different tab stops.  This can cause your code to look completely
unreadable, and it is not worth dealing with.

As always, follow the `Golden Rule`_ above: follow the style of
existing code if you are modifying and extending it.  If you like two spaces of
indentation, **DO NOT** do that in the middle of a chunk of code with four spaces
of indentation.  Also, do not reindent a whole source file: it makes for
incredible diffs that are absolutely worthless.

Indent Code Consistently
^^^^^^^^^^^^^^^^^^^^^^^^

Okay, in your first year of programming you were told that indentation is
important. If you didn't believe and internalize this then, now is the time.
Just do it. With the introduction of C++11, there are some new formatting
challenges that merit some suggestions to help have consistent, maintainable,
and tool-friendly formatting and indentation.

Use Braces for All Control Structures
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Braces should be used for *all* control structures (``if``, ``else``,
``switch``, ``do``, ``while``). This avoids any parse ambiguity, and
also reduces the "impact" on existing lines if a single-line condition
has to be expanded.

Example:

.. code-block:: c++

  int manipulate(const std::vector<Foo>& vec) {
      if (v.size() == 0) {
          return 0;
      }

      for (auto& v: vec) {
          if (v.isBlah()) {
              // handle Blah case...
          } else if (v.isUnusual()) {
              // handle Unusual case...
          }
      }
      ...
  }

Format Lambdas Like Blocks Of Code
""""""""""""""""""""""""""""""""""

When formatting a multi-line lambda, format it like a block of code, that's
what it is. If there is only one multi-line lambda in a statement, and there
are no expressions lexically after it in the statement, drop the indent to the
standard four space indent for a block of code, as if it were an if-block opened
by the preceding part of the statement:

.. code-block:: c++

  std::sort(foo.begin(), foo.end(), [&](Foo a, Foo b) -> bool {
      if (a.blah < b.blah) {
          return true;
      }
      if (a.baz < b.baz) {
          return true;
      }
      return a.bam < b.bam;
  });

To take best advantage of this formatting, if you are designing an API which
accepts a continuation or single callable argument (be it a functor, or
a ``std::function``), it should be the last argument if at all possible.

If there are multiple multi-line lambdas in a statement, or there is anything
interesting after the lambda in the statement, indent the block four spaces from
the indent of the ``[]``:

.. code-block:: c++

  dyn_switch(v->stripPointerCasts(),
             [] (PHINode* pn) {
                 // process phis...
             },
             [] (SelectInst* si) {
                 // process selects...
             },
             [] (LoadInst* li) {
                 // process loads...
             },
             [] (AllocaInst* ai) {
                 // process allocas...
             });

Braced Initializer Lists
""""""""""""""""""""""""

With C++11, there are significantly more uses of braced lists to perform
initialization. These allow you to easily construct aggregate temporaries in
expressions among other niceness. They now have a natural way of ending up
nested within each other and within function calls in order to build up
aggregates (such as option structs) from local variables. To make matters
worse, we also have many more uses of braces in an expression context that are
*not* performing initialization.

The historically common formatting of braced initialization of aggregate
variables does not mix cleanly with deep nesting, general expression contexts,
function arguments, and lambdas. We suggest new code use a simple rule for
formatting braced initialization lists: act as-if the braces were parentheses
in a function call. The formatting rules exactly match those already well
understood for formatting nested function calls. Examples:

.. code-block:: c++

  foo({a, b, c}, {1, 2, 3});

  llvm::Constant* mask[] = {
      llvm::ConstantInt::get(llvm::Type::getInt32Ty(getLLVMContext()), 0),
      llvm::ConstantInt::get(llvm::Type::getInt32Ty(getLLVMContext()), 1),
      llvm::ConstantInt::get(llvm::Type::getInt32Ty(getLLVMContext()), 2)};

This formatting scheme also makes it particularly easy to get predictable,
consistent, and automatic formatting with tools like `Clang Format`_.

    CB Note:
     We have a `Clang Format`_ config file (in
     ``tlm/dot-clang-format``) which specifies the code style which
     should be used. This is installed by ``repo`` into the top-level
     of the checkout, and so is automatically picked up by
     ``clang-format``.

     Do *not* completely reformat a whole file when you change it -
     this introduces unnecessary whitespace (see the `Golden
     Rule`_). Instead, use `git clang-format`_ which only reformats
     the line(s) which have already been touched by a patch.

.. _Clang Format: http://clang.llvm.org/docs/ClangFormat.html
.. _git clang-format: https://github.com/llvm-mirror/clang/blob/master/tools/clang-format/git-clang-format

Language and Compiler Issues
----------------------------

Treat Compiler Warnings Like Errors
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If your code has compiler warnings in it, something is wrong --- you aren't
casting values correctly, you have "questionable" constructs in your code, or
you are doing something legitimately wrong.  Compiler warnings can cover up
legitimate errors in output and make dealing with a translation unit difficult.

It is not possible to prevent all warnings from all compilers, nor is it
desirable.  Instead, pick a standard compiler (like ``gcc``) that provides a
good thorough set of warnings, and stick to it.  At least in the case of
``gcc``, it is possible to work around any spurious errors by changing the
syntax of the code slightly.  For example, a warning that annoys me occurs when
I write code like this:

.. code-block:: c++

  if (v = getValue()) {
      ...
  }

``gcc`` will warn me that I probably want to use the ``==`` operator, and that I
probably mistyped it.  In most cases, I haven't, and I really don't want the
spurious errors.  To fix this particular problem, I rewrite the code like
this:

.. code-block:: c++

  if ((v = getValue())) {
      ...
  }

which shuts ``gcc`` up.  Any ``gcc`` warning that annoys you can be fixed by
massaging the code appropriately.

Write Portable Code
^^^^^^^^^^^^^^^^^^^

In almost all cases, it is possible and within reason to write completely
portable code.  If there are cases where it isn't possible to write portable
code, isolate it behind a well defined (and well documented) interface.

In practice, this means that you shouldn't assume much about the host compiler
(and Visual Studio tends to be the lowest common denominator).  If advanced
features are used, they should only be an implementation detail of a library
which has a simple exposed API.

.. _static constructor:

Do not use Static Constructors
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Static constructors and destructors (e.g. global variables whose types have a
constructor or destructor) should not be added to the code base, and should be
removed wherever possible.  Note the `well known problems
<http://yosefk.com/c++fqa/ctors.html#fqa-10.12>`_ where the order of
initialization is undefined between globals in different source files.

That said, KV-Engine unfortunately does contain static constructors.  It would be a
great project for someone to purge all static
constructors from KV-Engine, and then enable the ``-Wglobal-constructors`` warning
flag (when building with Clang) to ensure we do not regress in the future.

Use of ``class`` and ``struct`` Keywords
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In C++, the ``class`` and ``struct`` keywords can be used almost
interchangeably. The only difference is when they are used to declare a class:
``class`` makes all members private by default while ``struct`` makes all
members public by default.

Unfortunately, not all compilers follow the rules and some will generate
different symbols based on whether ``class`` or ``struct`` was used to declare
the symbol (e.g., MSVC).  This can lead to problems at link time.

* All declarations and definitions of a given ``class`` or ``struct`` must use
  the same keyword.  For example:

.. code-block:: c++

  class Foo;

  // Breaks mangling in MSVC.
  struct Foo { int data; };

* As a rule of thumb, ``struct`` should be kept to structures where *all*
  members are declared public.

.. code-block:: c++

  // Foo feels like a class... this is strange.
  struct Foo {
  private:
    int data;
  public:
    Foo() : Data(0) { }
    int getData() const { return data; }
    void setData(int d) { data = d; }
  };

  // Bar isn't POD, but it does look like a struct.
  struct Bar {
    int data;
    Bar() : data(0) { }
  };

Do not use Braced Initializer Lists to Call a Constructor
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In C++11 there is a "generalized initialization syntax" which allows calling
constructors using braced initializer lists. Do not use these to call
constructors with any interesting logic or if you care that you're calling some
*particular* constructor. Those should look like function calls using
parentheses rather than like aggregate initialization. Similarly, if you need
to explicitly name the type and call its constructor to create a temporary,
don't use a braced initializer list. Instead, use a braced initializer list
(without any type for temporaries) when doing aggregate initialization or
something notionally equivalent. Examples:

.. code-block:: c++

  class Foo {
  public:
      // Construct a Foo by reading data from the disk in the whizbang format, ...
      Foo(std::string filename);

      // Construct a Foo by looking up the Nth element of some global data ...
      Foo(int n);

      // ...
  };

  // The Foo constructor call is very deliberate, no braces.
  std::fill(foo.begin(), foo.end(), Foo("name"));

  // The pair is just being constructed like an aggregate, use braces.
  bar_map.insert({my_key, my_value});

If you use a braced initializer list when initializing a variable, use an equals before the open curly brace:

.. code-block:: c++

  int data[] = {0, 1, 2, 3};

Use ``auto`` Type Deduction to Make Code More Readable
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Some are advocating a policy of "almost always ``auto``" in C++11, however KV-Engine
uses a more moderate stance. Use ``auto`` if and only if it makes the code more
readable or easier to maintain. Don't "almost always" use ``auto``, but do use
``auto`` with initializers like ``cast<Foo>(...)`` or other places where the
type is already obvious from the context. Another time when ``auto`` works well
for these purposes is when the type would have been abstracted away anyways,
often behind a container's typedef such as ``std::vector<T>::iterator``.

Beware unnecessary copies with ``auto``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The convenience of ``auto`` makes it easy to forget that its default behavior
is a copy.  Particularly in range-based ``for`` loops, careless copies are
expensive.

As a rule of thumb, use ``auto &`` unless you need to copy the result, and use
``auto *`` when copying pointers.

.. code-block:: c++

  // Typically there's no reason to copy.
  for (const auto& val : Container) { observe(val); }
  for (auto& val : Container) { val.change(); }

  // Remove the reference if you really want a new copy.
  for (auto val : Container) { val.change(); saveSomewhere(val); }

  // Copy pointers, but make it clear that they're pointers.
  for (const auto* ptr : container) { observe(*ptr); }
  for (auto* ptr : container) { ptr->change(); }

Style Issues
============

The High-Level Issues
---------------------

A Public Header File **is** a Module
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

C++ doesn't do too well in the modularity department.  There is no real
encapsulation or data hiding (unless you use expensive protocol classes), but it
is what we have to work with.  When you write a public header file (in the memcached
source tree, they live in the top level "``include``" directory), you are
defining a module of functionality.

Ideally, modules should be completely independent of each other, and their
header files should only ``#include`` the absolute minimum number of headers
possible. A module is not just a class, a function, or a namespace: it's a
collection of these that defines an interface.  This interface may be several
functions, classes, or data structures, but the important issue is how they work
together.

In general, a module should be implemented by one or more ``.cc`` files.  Each
of these ``.cc`` files should include the header that defines their interface
first.  This ensures that all of the dependences of the module header have been
properly added to the module header itself, and are not implicit.  System
headers should be included after user headers for a translation unit.

.. _minimal list of #includes:

``#include`` as Little as Possible
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``#include`` hurts compile time performance.  Don't do it unless you have to,
especially in header files.

But wait! Sometimes you need to have the definition of a class to use it, or to
inherit from it.  In these cases go ahead and ``#include`` that header file.  Be
aware however that there are many cases where you don't need to have the full
definition of a class.  If you are using a pointer or reference to a class, you
don't need the header file.  If you are simply returning a class instance from a
prototyped function or method, you don't need it.  In fact, for most cases, you
simply don't need the definition of a class. And not ``#include``\ing speeds up
compilation.

It is easy to try to go too overboard on this recommendation, however.  You
**must** include all of the header files that you are using --- you can include
them either directly or indirectly through another header file.  To make sure
that you don't accidentally forget to include a header file in your module
header, make sure to include your module header **first** in the implementation
file (as mentioned above).  This way there won't be any hidden dependencies that
you'll find out about later.

Keep "Internal" Headers Private
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Many modules have a complex implementation that causes them to use more than one
implementation (``.cc``) file.  It is often tempting to put the internal
communication interface (helper classes, extra functions, etc) in the public
module header file.  Don't do this!

If you really need to do something like this, put a private header file in the
same directory as the source files, and include it locally.  This ensures that
your private interface remains private and undisturbed by outsiders.

.. note::

    It's okay to put extra implementation methods in a public class itself. Just
    make them private (or protected) and all is well.

.. _early exits:

Use Early Exits and ``continue`` to Simplify Code
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When reading code, keep in mind how much state and how many previous decisions
have to be remembered by the reader to understand a block of code.  Aim to
reduce indentation where possible when it doesn't make it more difficult to
understand the code.  One great way to do this is by making use of early exits
and the ``continue`` keyword in long loops.  As an example of using an early
exit from a function, consider this "bad" code:

.. code-block:: c++

  Value* doSomething(Instruction* i) {
    if (!isa<TerminatorInst>(i) &&
        i->hasOneUse() && doOtherThing(i)) {
        ... some long code ....
    }

    return 0;
  }

This code has several problems if the body of the ``'if'`` is large.  When
you're looking at the top of the function, it isn't immediately clear that this
*only* does interesting things with non-terminator instructions, and only
applies to things with the other predicates.  Second, it is relatively difficult
to describe (in comments) why these predicates are important because the ``if``
statement makes it difficult to lay out the comments.  Third, when you're deep
within the body of the code, it is indented an extra level.  Finally, when
reading the top of the function, it isn't clear what the result is if the
predicate isn't true; you have to read to the end of the function to know that
it returns null.

It is much preferred to format the code like this:

.. code-block:: c++

  Value* doSomething(Instruction* i) {
    // Terminators never need 'something' done to them because ...
    if (isa<TerminatorInst>(i)) {
        return 0;
    }

    // We conservatively avoid transforming instructions with multiple uses
    // because goats like cheese.
    if (!i->hasOneUse()) {
        return 0;
    }

    // This is really just here for example.
    if (!doOtherThing(i)) {
        return 0;
    }

    ... some long code ....
  }

This fixes these problems.  A similar problem frequently happens in ``for``
loops.  A silly example is something like this:

.. code-block:: c++

  for (auto& op : basicBlocks) {
      if (BinaryOperator* bo = dyn_cast<BinaryOperator>(op)) {
          Value* lhs = bo->getOperand(0);
          Value* rhs = bo->getOperand(1);
          if (lhs != rhs) {
              ...
          }
      }
  }

When you have very, very small loops, this sort of structure is fine. But if it
exceeds more than 10-15 lines, it becomes difficult for people to read and
understand at a glance. The problem with this sort of code is that it gets very
nested very quickly. Meaning that the reader of the code has to keep a lot of
context in their brain to remember what is going immediately on in the loop,
because they don't know if/when the ``if`` conditions will have ``else``\s etc.
It is strongly preferred to structure the loop like this:

.. code-block:: c++

  for (auto& op : basicBlocks) {
      BinaryOperator* bo = dyn_cast<BinaryOperator>(op);
      if (!bo) continue;

      Value *lhs = bo->getOperand(0);
      Value *rhs = bo->getOperand(1);
      if (lhs == rhs) continue;

      ...
  }

This has all the benefits of using early exits for functions: it reduces nesting
of the loop, it makes it easier to describe why the conditions are true, and it
makes it obvious to the reader that there is no ``else`` coming up that they
have to push context into their brain for.  If a loop is large, this can be a
big understandability win.

Don't use ``else`` after a ``return``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For similar reasons above (reduction of indentation and easier reading), please
do not use ``'else'`` or ``'else if'`` after something that interrupts control
flow --- like ``return``, ``break``, ``continue``, etc. For
example, this is *bad*:

.. code-block:: c++

  case 'J': {
      if (signed) {
          type = context.getsigjmp_bufType();
          if (type.isNull()) {
              error = ASTContext::GE_Missing_sigjmp_buf;
              return qualType();
          } else {
              break;
          }
      } else {
          type = context.getjmp_bufType();
          if (type.isNull()) {
              error = ASTContext::GE_Missing_jmp_buf;
              return qualType();
          } else {
              break;
          }
      }
  }

It is better to write it like this:

.. code-block:: c++

  case 'J':
      if (signed) {
          type = context.getsigjmp_bufType();
          if (type.isNull()) {
              error = ASTContext::GE_Missing_sigjmp_buf;
              return qualType();
          }
      } else {
          type = context.getjmp_bufType();
          if (type.isNull()) {
              error = ASTContext::GE_Missing_jmp_buf;
              return qualType();
          }
      }
      break;

Or better yet (in this case) as:

.. code-block:: c++

  case 'J':
      if (signed) {
          type = context.getsigjmp_bufType();
      } else {
          type = context.getjmp_bufType();
      }

      if (type.isNull()) {
          error = signed ? ASTContext::GE_Missing_sigjmp_buf
                         : ASTContext::GE_Missing_jmp_buf;
          return qualType();
      }
      break;

The idea is to reduce indentation and the amount of code you have to keep track
of when reading the code.

Avoid Double-Negation in ``if`` / ``else`` Statements
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When writing ``if`` / ``else`` statements, prefer to write the
if-statment with the true case first and then the false - for example this
is bad - it can be confusing to read as it reads "backwards":

.. code-block:: c++

  if (!foo) {
      // code for false case...
  } else {
      // code for true case...
  }

Instead, prefer giving the positive case first:

.. code-block:: c++

  if (foo) {
      // code for true case...
  } else {
      // code for false case...
  }

Having said that, one should prioritize simpler code over ``if`` /
``else`` ordering - see `early exits`_.

Turn Predicate Loops into Predicate Functions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

It is very common to write small loops that just compute a boolean value.  There
are a number of ways that people commonly write these, but an example of this
sort of thing is:

.. code-block:: c++

  bool foundFoo = false;
  for (auto& bar : barList) {
      if (bar->isFoo()) {
          foundFoo = true;
          break;
      }

  if (foundFoo) {
      ...
  }

This sort of code is awkward to write, and is almost always a bad sign.  Instead
of this sort of loop, we strongly prefer to use a predicate function (which may
be `static`_) that uses `early exits`_ to compute the predicate.  We prefer the
code to be structured like this:

.. code-block:: c++

  /// @return true if the specified list has an element that is a foo.
  static bool containsFoo(const std::vector<Bar*>& barList) {
      for (const auto& bar : barList) {
          if (bar.isFoo()) {
              return true;
          }
      }
      return false;
  }
  ...

  if (containsFoo(barList)) {
    ...
  }

There are many reasons for doing this: it reduces indentation and factors out
code which can often be shared by other code that checks for the same predicate.
More importantly, it *forces you to pick a name* for the function, and forces
you to write a comment for it.  In this silly example, this doesn't add much
value.  However, if the condition is complex, this can make it a lot easier for
the reader to understand the code that queries for this predicate.  Instead of
being faced with the in-line details of how we check to see if the BarList
contains a foo, we can trust the function name and continue reading with better
locality.

The Low-Level Issues
--------------------

Name Types, Functions, Variables, and Enumerators Properly
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Poorly-chosen names can mislead the reader and cause bugs. We cannot stress
enough how important it is to use *descriptive* names.  Pick names that match
the semantics and role of the underlying entities, within reason.  Avoid
abbreviations unless they are well known.  After picking a good name, make sure
to use consistent capitalization for the name, as inconsistency requires clients
to either memorize the APIs or to look it up to find the exact spelling.

In general, names should be in camel case (e.g. ``TextFileReader`` and
``isLValue()``).  Different kinds of declarations have different rules:

* **Type names** (including classes, structs, enums, typedefs, etc) should be
  nouns and start with an upper-case letter (e.g. ``TextFileReader``).

* **Variable names** should be nouns (as they represent state).  The name should
  be camel case, and start with an lower case letter (e.g. ``leader`` or
  ``boats``).

* **Function names** should be verb phrases (as they represent actions), and
  command-like function should be imperative.  The name should be camel case,
  and start with a lower case letter (e.g. ``openFile()`` or ``isFoo()``).

* **Enum declarations** (e.g. ``enum Foo {...}``) are types, so they should
  follow the naming conventions for types.

* **Enumerators** (e.g. ``enum { Foo, Bar }``) should start with an
  upper-case letter, just like types. Prefer C++11 enum classes where
  possible.  Explicit values for enumerations (``enum Foo { Bar = 0,
  Baz = 1, ...}`` should only be used when the actual values
  matter, for example when using an enum for a bitfield.

As an exception, classes that mimic STL classes can have member names in STL's
style of lower-case words separated by underscores (e.g. ``begin()``,
``push_back()``, and ``empty()``). Classes that provide multiple
iterators should add a singular prefix to ``begin()`` and ``end()``
(e.g. ``global_begin()`` and ``use_begin()``).

Here are some examples of good and bad names:

.. code-block:: c++

  class VehicleMaker {
    ...
    Factory<Tire> f;            // Bad -- abbreviation and non-descriptive.
    Factory<Tire> factory;      // Better.
    Factory<Tire> tireFactory;  // Even better -- if VehicleMaker has more than one
                                // kind of factories.
  };

  Vehicle makeVehicle(VehicleType Type) {
    VehicleMaker m;                         // Might be OK if having a short life-span.
    Tire tmp1 = m.makeTire();               // Bad -- 'tmp1' provides no information.
    Light headlight = m.makeLight("head");  // Good -- descriptive.
    ...
  }

Use exceptions instead of assert()s
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Use exceptions for preconditions and assumptions, you never know when
a bug (not necessarily even yours) might be caught early by a check,
which reduces debugging time dramatically.

`assert` (or even our custom `cb_assert`) should not be used in
non-test code - `assert` will always abort (and hence terminate)
KV-Engine. Exceptions on the other hand can be caught and one can
choose how to handle them on a case-by-case basis.  See
`./ErrorHandling.md`_ for further discussion on handling errors.

To further assist with debugging, make sure to put some kind of error
message in the exception `what()` message. This should include an
indication of where the exception was raised (Class::methodName), and
a description of what exceptional situation occurred.

Throw exceptions by value (i.e. don't use `new`), and catch by (`const`)
reference. This ensures that there's no explicit need to `delete` an
exception, and no unnecessary copies are made.

Here are some examples of good and bad code:

.. code:block:: c++

  void doSomething(int a) {
      try {
          if (a > 100) {
              throw new std::invalid_argument(  // Bad -- thrown via `new`.
                  "a too large");               // Bad -- no indication where
                                                // exception came from.
                                                // Bad -- no indication what
                                                // value `a` was.
          }
      } catch (std::invalid_argument e) {       // Bad -- caught by value.
         ...
      }
      ...
      try {
          if (a < 10) {
              throw std::invalid_argument(      // Good -- throw directly
                  "doSomething: a (which is " + std::to_string(a) +
                  ") is less than 10")          // Good -- include method name
                                                // and value
          }
      } catch (std::invalid_argument& e) {      // Good -- caught by reference.
         ...
      }

Do Not Use ``using namespace std``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In KV-Engine, we prefer to explicitly prefix all identifiers from the standard
namespace with an "``std::``" prefix, rather than rely on "``using namespace
std;``".

In header files, adding a ``'using namespace XXX'`` directive pollutes the
namespace of any source file that ``#include``\s the header.  This is clearly a
bad thing.

In implementation files (e.g. ``.cc`` files), the rule is more of a stylistic
rule, but is still important.  Basically, using explicit namespace prefixes
makes the code **clearer**, because it is immediately obvious what facilities
are being used and where they are coming from. And **more portable**, because
namespace clashes cannot occur between KV-Engine code and other namespaces.  The
portability rule is important because different standard library implementations
expose different symbols (potentially ones they shouldn't), and future revisions
to the C++ standard will add more symbols to the ``std`` namespace.  As such, we
never use ``'using namespace std;'`` in KV-Engine.

The exception to the general rule (i.e. it's not an exception for the ``std``
namespace) is for implementation files.  For example, code in the
KV-Engine project implements code that lives in the 'cb' namespace.  As such, it is
ok, and actually clearer, for the ``.cc`` files to have a ``'using namespace
cb;'`` directive at the top, after the ``#include``\s.  This reduces
indentation in the body of the file for source editors that indent based on
braces, and keeps the conceptual context cleaner.  The general form of this rule
is that any ``.cc`` file that implements code in any namespace may use that
namespace (and its parents'), but should not use any others.

Provide a Virtual Method Anchor for Classes in Headers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If a class is defined in a header file and has a vtable (either it has virtual
methods or it derives from classes with virtual methods), it must always have at
least one out-of-line virtual method in the class.  Without this, the compiler
will copy the vtable and RTTI into every ``.o`` file that ``#include``\s the
header, bloating ``.o`` file sizes and increasing link times.

Don't use default labels in fully covered switches over enumerations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``-Wswitch`` warns if a switch, without a default label, over an enumeration
does not cover every enumeration value. If you write a default label on a fully
covered switch over an enumeration then the ``-Wswitch`` warning won't fire
when new elements are added to that enumeration.

Don't use ``inline`` when defining a function in a class definition
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A member function defined in a class definition is implicitly inline, so don't
put the ``inline`` keyword in this case.

Don't:

.. code-block:: c++

  class Foo {
  public:
      inline void bar() {
          // ...
      }
  };

Do:

.. code-block:: c++

  class Foo {
  public:
      void bar() {
          // ...
      }
  };

Microscopic Details
-------------------

This section describes preferred low-level formatting guidelines along with
reasoning on why we prefer them.

Spaces Before Parentheses
^^^^^^^^^^^^^^^^^^^^^^^^^

We prefer to put a space before an open parenthesis only in control flow
statements, but not in normal function call expressions and function-like
macros.  For example, this is good:

.. code-block:: c++

  if (x) ...
  for (i = 0; i != 100; ++i) ...
  while (llvmRocks) ...

  somefunc(42);
  cb_assert(3 != 4 && "laws of math are failing me");

  a = foo(42, 92) + bar(x);

and this is bad:

.. code-block:: c++

  if(x) ...
  for(i = 0; i != 100; ++i) ...
  while(llvmRocks) ...

  somefunc (42);
  cb_assert (3 != 4 && "laws of math are failing me");

  a = foo (42, 92) + bar (x);

The reason for doing this is not completely arbitrary.  This style makes control
flow operators stand out more, and makes expressions flow better. The function
call operator binds very tightly as a postfix operator.  Putting a space after a
function name (as in the last example) makes it appear that the code might bind
the arguments of the left-hand-side of a binary operator with the argument list
of a function and the name of the right side.  More specifically, it is easy to
misread the "``A``" example as:

.. code-block:: c++

  a = foo ((42, 92) + bar) (x);

when skimming through the code.  By avoiding a space in a function, we avoid
this misinterpretation.

Pointer (*) and Reference (&) Symbols Next to Type
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Pointers and references should be written with the ``*`` / ``&``
symbol next to the type, not the variable. For example, this is good:

.. code-block:: c++

  const char* str;
  Foo& foo = otherFoo;
  ...
  void frobnicate(const Foo& foo, size_t amount);
  ...
  void consumeFoo(Foo&& foo);

This is ultimately a stylistic choice - we basically have two choices
(next to type or variable) and so for consistency (see `Golden Rule`_)
we must pick one. However this is also the style recommended by the
`Core C++ Guidelines`_ so we are in good company :).

Prefer Preincrement
^^^^^^^^^^^^^^^^^^^

Hard fast rule: Preincrement (``++x``) may be no slower than postincrement
(``x++``) and could very well be a lot faster than it.  Use preincrementation
whenever possible.

The semantics of postincrement include making a copy of the value being
incremented, returning it, and then preincrementing the "work value".  For
primitive types, this isn't a big deal. But for iterators, it can be a huge
issue (for example, some iterators contains stack and set objects in them...
copying an iterator could invoke the copy ctor's of these as well).  In general,
get in the habit of always using preincrement, and you won't have a problem.


Namespace Indentation
^^^^^^^^^^^^^^^^^^^^^

In general, we strive to reduce indentation wherever possible.  This is useful
because we want code to `fit into 80 columns`_ without wrapping horribly, but
also because it makes it easier to understand the code. To facilitate this and
avoid some insanely deep nesting on occasion, don't indent namespaces. If it
helps readability, feel free to add a comment indicating what namespace is
being closed by a ``}``.  For example:

.. code-block:: c++

  namespace llvm {
  namespace knowledge {

  /// This class represents things that Smith can have an intimate
  /// understanding of and contains the data associated with it.
  class Grokable {
  ...
  public:
      explicit Grokable() { ... }
      virtual ~Grokable() = 0;

    ...

  };

  } // end namespace knowledge
  } // end namespace llvm


Feel free to skip the closing comment when the namespace being closed is
obvious for any reason. For example, the outer-most namespace in a header file
is rarely a source of confusion. But namespaces both anonymous and named in
source files that are being closed half way through the file probably could use
clarification.

.. _static:

Anonymous Namespaces
^^^^^^^^^^^^^^^^^^^^

After talking about namespaces in general, you may be wondering about anonymous
namespaces in particular.  Anonymous namespaces are a great language feature
that tells the C++ compiler that the contents of the namespace are only visible
within the current translation unit, allowing more aggressive optimization and
eliminating the possibility of symbol name collisions.  Anonymous namespaces are
to C++ as "static" is to C functions and global variables.  While "``static``"
is available in C++, anonymous namespaces are more general: they can make entire
classes private to a file.

The problem with anonymous namespaces is that they naturally want to encourage
indentation of their body, and they reduce locality of reference: if you see a
random function definition in a C++ file, it is easy to see if it is marked
static, but seeing if it is in an anonymous namespace requires scanning a big
chunk of the file.

Because of this, we have a simple guideline: make anonymous namespaces as small
as possible, and only use them for class declarations.  For example, this is
good:

.. code-block:: c++

  namespace {
  class StringSort {
  ...
  public:
      StringSort(...)
      bool operator<(const char* rhs) const;
  };
  } // end anonymous namespace

  static void runHelper() {
      ...
  }

  bool StringSort::operator<(const char* rhs) const {
      ...
  }

This is bad:

.. code-block:: c++

  namespace {

  class StringSort {
  ...
  public:
      StringSort(...)
      bool operator<(const char* RHS) const;
  };

  void runHelper() {
      ...
  }

  bool StringSort::operator<(const char* rhs) const {
      ...
  }

  } // end anonymous namespace

This is bad specifically because if you're looking at "``runHelper``" in the middle
of a large C++ file, that you have no immediate way to tell if it is local to
the file.  When it is marked static explicitly, this is immediately obvious.
Also, there is no reason to enclose the definition of "``operator<``" in the
namespace just because it was declared there.

See Also
========

A lot of these comments and recommendations have been culled from other sources.
Two particularly important books for our work are:

#. `Effective C++
   <http://www.amazon.com/Effective-Specific-Addison-Wesley-Professional-Computing/dp/0321334876>`_
   by Scott Meyers.  Also interesting and useful are "More Effective C++" and
   "Effective STL" by the same author.

#. `Large-Scale C++ Software Design
   <http://www.amazon.com/Large-Scale-Software-Design-John-Lakos/dp/0201633620/ref=sr_1_1>`_
   by John Lakos

#. `Core C++ Guidelines
   <http://isocpp.github.io/CppCoreGuidelines/CppCoreGuidelines>`_
   editod by Bjarne Stroustrup, Herb Sutter. This is an excellent
   resource for best practices in Modern C++.

If you get some free time, and you haven't read them: do so, you might learn
something.
