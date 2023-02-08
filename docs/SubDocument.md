[This document was initially imported from the private document https://docs.
google.com/document/d/1vaQJxIA5nhWJqji7X2R1xQDZadb5PabfKAid1kVe65o/edit?ouid=103017102960582759825&usp=docs_home&ths=true
Note that formatting may differ from the original document, and pieces of
the original document was removed as it referenced other things or may
no longer be needed]: #

# High Level Description

The sub-document commands are efficient ways (network-wise) to
atomically manipulate JSON documents on the server side, so that
applications do not have to fetch the entire (potentially large)
document in order to perform a modification or access a single field (or
several fields). This also answers many of our user's questions such
as:

- How can I remove a field in a document?
- How can I add a new field in a document?
- How can I append to a string in a document?
- How can I increment a field in a document?

This document covers the conceptual API offered by the server. The
specific format for the client-side API is not yet determined, but
should be able to provide a one-to-one mapping with commands on the
server. The actual client side API will be provided in a different
document, and parties are intended to iterate over both APIs until
perfection™ is reached.

## Document Conventions

The name of commands and errors are abbreviated for brevity and ease of
reading; unless specified otherwise, all theoretical **CONSTANTS** are
considered to be prefixed with **SUBDOC_**

The terms "JSON Object" and "JSON Dictionary" are used interchangeably.
Likewise, the term "JSON List" and "JSON Array" are used
interchangeably.

# API Concepts

The sub-document API centers around several concepts:

## Document

The document is the existing document the operation is executed
against. The document should be a JSON object. Documents which are not
JSON objects (i.e. arrays and primitives) are out-of-scope for this
proposal. From the perspective of this API, the document simply needs to
be valid JSON. From the perspective of everything else, it is simply a
value in the key-value store.

## Path

The path specifies a *location* within the document upon which
the command should execute. The path assumes dots (.) to describe the
hierarchy which should point to the target field. The syntax of the path
is intended to conform to the N1QL syntax for specifying a location
within a document. The rationale behind using the N1QL path syntax is
two-fold

- There will be a universal way of addressing JSON in Couchbase
- The JSON path used in N1QL allows disambiguation between array
  indices and dictionary keys; for example, using JSONPointer, the
  path `/foo/0/1/2` can either refer to
  `{"foo":[[null,[null,null,THIS]]]}` or
  `{"foo":{"0":{"1":{"2"}}}`.

The path points to a dictionary key or an array index, or a containing
array, depending on the command being performed. The path is composed of
components, with each component separated by a dot. For array elements,
square brackets should be placed *immediately* after the path
indicating the array itself.

To indicate the *last element* of an array, use the value -1 in
brackets. Note that this is a special rule for accessing the last
element of the array. Other negative values are invalid (or in the
current implementation, treated the same as `-1`.

Paths must conform to two rules in order to be valid. First, they must
properly escape any path-specific metacharacters; second, they must
escape any JSON metacharacters.

Path-specific meta-characters are:

- Dot (`.`)
- Open bracket (`[`)
- Close bracket (`]`)

To escape a *path-meta-character*, enclose the portion to escape
within a set of backticks (`\``). To place a *literal* backtick, specify two
backticks.

In the document

    {
      "type":"product",
      "pType":"toy",
      "pName": "Tickle Me Elmo",
      "pDetails": {
        "audience":"children"
      },
      "pDistributors":[
        {
          "dName": "Going Out of Business Wholesale",
          "dAdded": ["Feb", 36, 2025]
        }, {
          "dName": "Everything Must Go!",
          "dAdded": ["May", 72, 1492]
        }
      ],
      "dot.ted.field":null,
      "back`tick`field":null,
      "field.with.\"quotes\"":null
    }

| path                         | value                               |
|------------------------------|-------------------------------------|
| type                         | `"product"`                         |
| pDistributors                | `[...]`                             |
| pDistributors[0].dName       | `"Going Out of Business Wholesale"` |
| pDistributors[1].dAdded[2]   | `1492`                              |
| \`dot.ted.field\`.subfield   | `...`                               |
| \`back\`\`tick\`\`field\`    | `...`                               |
| \`field.with.\”quotes\”\`    | `...`                               |
| pDistributors[-1].dAdded[-1] | `1492`                              |

### Path Errors

If a path cannot be parsed correctly, a command shall fail and return a
`STATUS_PATH_EINVAL`.

If the path parses correctly, but specifies an impossible path (for
example, treating a JSON object as a JSON array), the command shall fail
and return a `STATUS_PATH_MISMATCH`. An example of a
*mismatch* in the document above would be the path
`pDistributors.count`. Since `pDistributors` is an array,
any of its children must be specified using the index specifier.

Another example of a path mismatch is `pType.category` . Since
`pType` exists and its current value is a JSON string, the path
treating `pType` as a dictionary is invalid.

If the path parses correctly, but a component of the path does not
exist, `STATUS_PATH_ENOENT` is returned. The rationale is to
allow differentiation between *application errors* (where
application logic is erroneous -- perhaps an older version is running),
and *state errors* (where the application may be valid, but
something somewhere has caused the document to not yet have the given
path).

## Flags

As of Spock we have extended the flags used to include doc flags. The
motivation behind this comes from having multi path commands wanting to
differentiate flags that operate on a single path and those that only
make sense to operate on the document (or the multi-path command) as a
whole.

### Path Flags

These flags apply specifically to an operation on a single path.

| Flag                   | Description                                                                                                                             |
|------------------------|-----------------------------------------------------------------------------------------------------------------------------------------|
| `MKDIR_P` (0x01)       | (Mutation) Should non-existent intermediate paths be created?                                                                           |
| `XATTR_PATH` (0x04)    | If set, the path refers to an Extended Attribute (XATTR)                                                                                |
| `EXPAND_MACROS` (0x10) | [Expand macro values inside extended attributes. The request is invalid if this flag is set without `SUBDOC_FLAG_XATTR_PATH` being set. |

### Doc Flags

These flags conceptually apply to the doc as a whole. They really
become useful when used with multi-path commands. They allow you to set
a doc specific flag only once rather than having to set it with the path
flags for each operation spec in the multi-path command.

| Flag                    | Comment                                                                                                                                                                                                                                                                      |
|-------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------| 
| `MKDOC` (0x01)          | (Mutation) Create the document if it does not exist. Implies `SUBDOC_FLAG_MKDIR_P` on each path and Set (upsert) mutation semantics. Not valid with the `Add` doc flag. Note  that the sdk-rfc refers to this as *upsert_document* for consistency with the rest of the API. |
| `ADD` (0x02)            | (Mutation) Add the document only if it does not exist. Implies `SUBDOC_FLAG_MKDIR_P` on each path. Not valid with the `Mkdoc` doc flag. Note that the sdk-rfc refers to this as *insert_document* for consistency with the rest of the API.                                  |
| `ACCESS_DELETED` (0x04) | Allow access to XATTRs for deleted documents (instead of  returning `KEY_ENOENT`. Note that this is considered *PRIVATE* interface and is implemented in select SDKs for mobile convergence purposes.]{.c0}                                                                  |

## General Command Format

The command indicates what operation should be performed on the actual
JSON object. Specifically, each command operates on an *entry*
within the document, specified by the *path*.

The semantics of any command is dependent on the type of the *immediate
parent object* of the entry. The *immediate*  parent object
is the object or list which *contains*  the object. For example,
the path `pDistributors[0].dName` has an *immediate
parent* of `pDistributors[0]`, which is itself a JSON
object; whereas the path `pDistributors[1].dName` has an
immediate parent of `dName`, which is itself a JSON array.

Some commands are valid only where the immediate parent is an array;
others are valid only where the immediate parent is an object. Other
commands operate on the *value* of the entry itself, and can
operate in both array and list modes.

### Common inputs, outputs and errors

All commands must include:

- The document ID (aka Memcached Key). This is the document to
  operate on
- The path to the entry

#### Mutation semantics

Commands which perform insertion operations must also include the
*value* . The value must be valid JSON value. If the value is not
valid JSON, the error code `STATUS_VALUE_CANT_INSERT` will be
returned. Note that this does not necessarily mean that the value is
invalid JSON, but rather that the value cannot be inserted in the path
specified.

Commands may also supply the current document CAS. In this case the
operation will fail with a `PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS`
error if the document's CAS does not match the one supplied by the
client.

Mutation commands may also supply an expiry (TTL). This is handled in
the same way as normal "fulldoc" mutation commands - i.e. a non-zero
expiry specifies that the document will be automatically deleted with
the expiry time is reached. Note that there is no concept of
*per-path* expiry - the expiry value (if specified) applies to
the entire document. Specifying expiry is optional, and if present,
should make the *extras length* an additional 4 bytes longer, for
a total of 7 bytes.

Commands themselves shall return a *status code* indicating
whether the modification succeeded or not; and if applicable, the value
contained under the path.

Mutation commands support the optional `MUTATION_SEQNO` feature, enabled
via the HELO command. When enabled, all successful
mutation responses include a 64bit mutation sequence number and 64bit
vBucket UUID in the *extras* section.

If, in the course of an mutation operation, the document exceeds the set
size for document value sizes (e.g. 20MB), the
`PROTOCOL_BINARY_RESPONSE_E2BIG` is returned.

It's worth mentioning the mutation semantics exposed by the doc flags:
`MKDOC` and `ADD`. These can be nicely summarised in the
following table:

| Doc flags set    | Mutation semantics                                                                                                |
|------------------|-------------------------------------------------------------------------------------------------------------------|
| (Neither)        | Similar to `CMD_REAPLACE` (Memcached) semantics.                                                                  |
| `ADD`            | Similar to `CMD_ADD` semantics. Fails if the key exists or if a CAS is specified. Implies `MKDIR_P` on each path. |
| `MKDOC`          | Similar to `CMD_SET`. Implies `MKDIR_P` on each path                                                              |
| `ADD` \| `MKDOC` | Invalid (Currently no suitable semantics to map this to, hence it’s reserved for now)                             |

#### Existence semantics

If the path does not exist, `STATUS_PATH_ENOENT` is returned. The definition of
an "existing path" depends on the command being issued.

If the document itself does not exist,
`PROTOCOL_BINARY_RESPONSE_ENOENT` is returned.

#### Creating intermediate paths

For some mutation commands, the behavior when a specific path component
does not exist is dependent on options passed with the command. Consider
the document (`{"foo":{}}`) and the path `"foo.bar.baz"`. Commands such as
(sub-document) *add* or *upsert* will by default
fail. The rationale is to discourage the possibility of filling up
documents with random garbage, possibly due to typos, and to hinge this
on user behavior. In most cases if a specific part of a hierarchy is
missing, the application will want to get the entire document and
inspect it for validity.

The special flag value `FLAG_MKDIR_P` may be inserted into the
*flags* field in the protocol request header as part of the
*extras* payload.

The rationale behind this command is to avoid having the user
recursively add intermediate components one-by-one - which would result
in rather confusing application logic, as well as heavy network
I/O.

Note that this command will only create intermediate dictionary paths,
and not intermediate array elements. If an intermediate path specified
points to an array element, and the element does not exist, this command
will respond with `STATUS_PATH_ENOENT`. This limitation is by
design, as there is no defined way to define an array element in a JSON
document without first creating any prior elements.

For example, to add a *"radioactive"* key in a *"hazard"*
dictionary into the `pDetails` dictionary, one may use the PATH
of `"pDetails.hazards.radioactive"` and a VALUE of true. The
`FLAG_MKDIR_P` flag will automatically create the intermediate
*"hazards"* dictionary, and then insert the *"radioactive"* key with the
value of true.

#### Creating missing documents

For mutation commands, specifying the doc flag `MKDOC` will
create the document if it does not exist, *and* if the operation
would have succeeded if the document had existed, but was empty,
containing only the bare root object: either a bare dict (`{}`) or a bare
array (`[]`). The server will intelligently determine what the actual
root object is based on the command passed and the path syntax itself.

For example, performing a `DICT_UPSERT` with a path of `foo`
would succeed on an empty dictionary (`{}`), and performing an
`ARRAY_PUSH_FIRST` with an empty path would succeed on an empty
list.

Specifying this flag also *implies* `FLAG_MKDIR_P`

# Command Listings

## Retrieval Commands

These commands will retrieve the value stored under the path. These are
valid in both dictionary and array contexts.

### CMD_GET: Get a path's value

This command will return the  *value*  indicated by the
*path*.

- Inputs:
    - The key of the document to retrieve.
    - The path to the entry of the value to retrieve. Path must not be empty

- Errors:
    - If the path does not exist, `STATUS_PATH_ENOENT` shall be returned.
    - Generic subdoc errors

- Outputs:
    - The JSON value stored in the entry. The returned value is returned
      as valid JSON .

Note, memcached need not return the item's *flags* since it is
implicitly assumed that the returned value will parse as valid JSON on
the client. (Typically, flags encompass the entire document. Since
only a subset of the document is being returned, the flags do not convey
anything useful).

### CMD_EXISTS: Check if a path exists

This command functions exactly like `CMD_GET`, except that it
will not return the existing value in the payload.

## Dictionary Insertion Commands

These commands insert *key-value* pairs into a dictionary. In
these commands, the *last component* of the path should contain
the *new key* to insert. These commands must also contain a
value which is the JSON value to be inserted under the key.

Since a dictionary command **must** have the key in the path, it
follows that an *empty* path is invalid.

The format for the VALUE may be any text which evaluates as valid
JSON. The validity of the JSON value provided is executed
*as-if* the JSON was a value to a JSON dictionary, for
example

    { "dummyKey": [VALUE] }

As such, the VALUE may be a primitive, JSON array, or JSON dictionary
itself.

### CMD_DICT_ADD: Add a new dictionary entry

This command will add an entry of `"path":VALUE` into the
document. All path elements, *except the last* , must exist, or
this command shall fail with `STATUS_PATH_ENOENT`. If the *last
component* already exists, the command will fail with
`STATUS_PATH_EEXISTS`.

- Inputs:
    - The key of the document to modify.
    - The path to the key to be created. Path must not be empty, and must
      not end with an array index
    - A JSON object to be used as the value for the new key
    - Optional flags (`FLAG_MKDIR_P`, `FLAG_MKDOC`)

- Errors:
    - Generic subdoc errors
    - `STATUS_PATH_ENOENT` If an intermediate path does not exist and MKDIR_P
      was not present in the flags
    - `STATUS_PATH_EEXISTS` If the last component of the path already exists
    - `STATUS_PATH_EINVAL` If the path ends in an array index

For example, to add a "character":"elmo" field to the "pDetails"
dictionary, the PATH would be `pDetails.character`  and the VALUE
would be `"elmo"`.

### CMD_DICT_UPSERT: Add or replace a dictionary entry

This command is similar to `CMD_DICT_ADD`, except that if the
*last component* does exist, it will overwrite the existing value
and replace it with the value provided in the command.

## Generic Modification Commands

These commands are generic across dictionary and array
immediate-parents

### CMD_DELETE: Remove an entry from the document

This command removes an entry from the document. If the entry points to
a dictionary key-value, the key and the value are removed from the
document. If the entry points to an array element, the array element is
removed, and all following elements will implicitly shift back by one.
If the array element is specified as `[-1]` then the last
element is removed.

- Inputs:
    - The key of the document to delete
    - Path to the entry to delete. Must not be empty
- Errors
    - Generic subdoc errors
    - `STATUS_PATH_ENOENT` if the path does not exist

Note that as empty paths are not allowed, it is impossible for this
operation to result in an empty document (i.e. one which does not have a
top-level `[]` or `{}`).

### CMD_REPLACE: Replace the value of an entry in the document

This command will *replace* the VALUE of the entry with the new
value provided in the command. This command will fail if the path does
not exist

- Inputs:
    - The key of the document to modify
    - Path to entry. Must not be empty
    - JSON to be used as new value

- Errors:
    - Generic subdoc errors
    - `STATUS_PATH_ENOENT` if the path does not exist

## Array  commands

These commands are only valid if the last path component points to an
*array* (e.g. not an array element nor a dictionary). Since
arrays have no keys, the path consists of the *immediate parent*
which is the array itself.

An empty path should be considered valid, and implies that the entire
document itself is an array.

For commands which *insert* new items, the VALUE provided may be
a single JSON value, or a list of JSON values. So long as the string
passed as the value will produce valid JSON if placed within a JSON
array like so: **[ VALUE ]**

Thus, VALUE may also be

- "primitive"
- ["sub","array"]
- "multiple","elements", 1, 2, false

Note that any array command will *fail* with
`STATUS_PATH_MISMATCH` if the final component of the path is not
an array. For example, in the document above, `pDistributors`,
`pDistributors[0].dAdded` are valid paths to arrays; however
`pDistributors[0]` is not (since the value is a
dictionary).

To remove items from an array, refer to the `CMD_DELETE`.

### CMD_ARRAY_PUSH_LAST: Append an entry to an array

This will append the provided *value*  to the *end* of the
array specified by the path. *value* in this case can either be
a single JSON value, or a list of JSON values delimited by commas.

If the array itself does not exist, the `STATUS_PATH_ENOENT` error will be
returned.

- Inputs:
    - The key of the document to change
    - Path to containing *array*. The path *may* be empty to indicate a
      top-level array
    - JSON value(s) to insert
    - Optional flags (`FLAG_MKDIR_P`, `FLAG_MKDOC`)

- Errors:
    - Generic subdoc errors]
    - `STATUS_PATH_ENOENT` if the array does not exist (and `MKDIR_P` was not
      specified)

### CMD_ARRAY_PUSH_FIRST: Prepend an entry to an array

This will add the provided  *value*  to the *beginning* of
the array specified by the path. The semantics here follow that of
`CMD_ARRAY_PUSH_LAST`

### CMD_ARRAY_INSERT: Insert an array element at a given position

This will insert the value specified as VALUE into the array index. The
operation requires that PATH be a path to the array index at which to
insert this value. All array elements (including the one currently
residing at PATH) will be pushed over by one (so that an element which
was previously at position ARRAY[INDEX] will now be at position
ARRAY[INDEX+1]

This command requires that the actual path already exist. The
exceptions are:

- if INDEX is 0 (in which case the command is equivalent to
  PUSH_FIRST or PUSH_LAST with an empty array).
- If INDEX is SIZE, in which case the item is simply appended.

Note that using a negative index is *not* supported (there is
ambiguity in this case if the new item should be the last or
second-to-last item; command semantics suggest that the new element take
the position of the previous element, and the previous element be pushed
back by one).

Note that this is not suitable for append/prepend queue-type operations
since this command will fail if the existing index does not exist. This
command also has no `MKDIR_P` variant.

- Inputs:
    - The key of the document to change
    - Value(s) to insert
    - Path to the array element. This should point to an **existing**
      element. The existing element will be pushed back by one, and the
      new value(s) will be placed in its stead. Path must not be empty. If
      the index is equal to the size of the array then the new item is
      added to the end of the array. If it is bigger than the array size,
      `PATH_ENOENT` will be returned.

- Errors:
    - Generic subdoc errors
    - `PATH_EINVAL` if the last path component is not an array
      element  (eg. "array")
    - `PATH_EINVAL` if the given position is negative (eg.
      ("array[-1]")
    - `PATH_ENOENT` if the array does not exist or if no current
      element exists at the given position  (eg. "array[5]" for an array
      containing 3 elements)

### CMD_ARRAY_ADD_UNIQUE : Insert a unique element into an array

This will insert the value specified as VALUE into the array. This
operation will succeed **only** if the element does not already
exist in the array.

This operation places additional restrictions on the value *and*
the *existing contents* of the array. Specifically, both the
VALUE and the EXISTING VALUES of the array are restricted to *JSON
Primitives*; this means strings, numbers, and the special values
of true/false/null. The rationale behind this restriction is that there
is no good and/or efficient way to check for "Uniqueness" within
non-primitive objects, without first descending into each sub-object and
comparing.

Uniqueness checking is done by simple **string comparison**;
which means the JSON value to add is compared against all existing JSON
values that are elements in the array. Since JSON provides for no
ambiguity in its values, the following will currently not be considered
matching:

- "123" does not equal 123
- "true" does not equal true
- 1.0 does not equal 1

The rationale behind using strict string comparison is performance.
Converting JSON datatypes to their native equivalents may incur some
overhead

- Inputs:
    - The key of the document to change
    - Path to containing array. Path may be empty to indicate a top level
      array
    - JSON Primitive to insert
    - Optional flags (`FLAG_MKDIR_P`, `FLAG_MKDOC`)

- Errors:
    - Generic subdoc errors
    - `STATUS_PATH_EEXISTS` if the element is already present in
      the array
    - `STATUS_PATH_ENOENT` if the array does not exist
    - `STATUS_VALUE_CANTINSERT` if the input value is not a
      primitive
    - `STATUS_PATH_MISMATCH` or if the array contains non-primitive
      elements

### CMD_ARRAY_UPSERT_UNIQUE: Insert a unique element into an array if not present

This command functions entirely like CMD_ARRAY_ADD_UNIQUE, except that
EEXISTS is not treated as an error. That is to say,
CMD_ARRAY_UPSERT_UNIQUE silently pass on a value that is already in the
array, where CMD_ARRAY_ADD_UNIQUE would complain.

### CMD_GET_COUNT: Get the number of elements in an array or dictionary

When used on an array, returns the number of elements. When used on an
object, returns the number of key-value pairs. When used on anything
else, returns a PATH_MISMATCH error.

The size is returned as an ascii integer.

## Arithmetic Operations

These operations operate on *numerical values*. This requires
that the value pointed to by the path contains a whole, valid JSON
number. The existing value may be signed.

Arithmetic operations accept a *delta* which is either added to
or subtracted from the existing value; with the new value being returned
(in JSON format (TODO: shouldn't this be a 64 bit number instead))

Unlike memcached counters, subdoc-counters are 64-bit *signed* integers with
a lower range of INT64_MIN and an upper range of INT64_MAX.

If the existing value is not an integer, `STATUS_PATH_MISMATCH`
will be returned. If the existing value is a number, but cannot be
parsed into a signed *int64_t*, `STATUS_NUM_TOOBIG` will be
returned. If the *combination* of the provided *delta* and
the existing value yields an out-of-bounds value,
`STATUS_DELTA_OVERFLOW` is returned. If the *delta* itself
is not valid (0, too big, or not numeric), `STATUS_DELTA_EINVAL`
will be returned.

Arithmetic counters do *not* roll over; thus there are no defined
overflow or underflow semantics . If the existing operation results in
an overflow or underflow, `STATUS_DELTA_OVERFLOW` ]{.c3}` is returned

Conversion semantics between JSON float representations and their C
counterparts are to be determined. In this version of the specification,
only whole numbers are to be allowed. The rationale being that an
increment or decrement should never modify the *type*  of the
underlying value (so for example, an integer should never magically
become a float). This helps application developers maintain consistent
APIs in determining how to treat the resultant value. Additionally,
performance considerations may be taken into account when depending on
other libraries to handle arbitrarily long numbers of an arbitrary
precisison.

### CMD_COUNTER: Add or subtract from a numeric value

This will *add* the specified *delta* to the existing
amount. If the *delta* is negative, then the amount will be
subtracted.

If the path does not exist, but the *immediate parent* does, a
new entry will be created with the value being set to the delta itself
(i.e. the initial value is 0, and the delta is added to 0).

- Inputs:
  - The key of the document to perform the arithmetic operation
    on
  - Path to the existing number. Path must not be empty
  - The delta, as a string which can be parsed as a JSON number
      - Optional flags (`FLAG_MKDIR_P`, `FLAG_MKDOC`)

- Outputs:
    - New value, as a string containing a JSON number

- Errors:
  - Generic subdoc errors
  - `STATUS_PATH_ENOENT` if an intermediate path is missing
  - `STATUS_DELTA_EINVAL` if the specified delta is
    not-an-integer, is outside the range represented by a int64_t, or is
    zero.
  - `STATUS_DELTA_OVERFLOW` If the operation would result in an
    overflow/underflow
  - `STATUS_NUMBER_ETOOBIG` If the existing number is too
    big
  - `STATUS_PATH_MISMATCH` If the existing value is not a valid
    integer

## Whole Doc Commands

### [CMD_GET]

This is only valid in a multi-lookup and with an empty path. This will
return the whole document body

- Inputs:
  - Optional doc flag (AccessDeleted)

- Outputs:
    - The whole document]

- Errors:
  - Generic subdoc errors

### CMD_SET

This is only valid in a multi-mutation and with an empty path. This
will set the whole document body subject to the mutation semantics
specified by the doc flags.

- Inputs:
  - Optional doc flags (`Mkdoc`, `Add`)

- Outputs:
  - The whole document

- Errors:
    - Generic subdoc errors

### CMD_DELETE

This is only valid in a multi-mutation with an empty path. It will
delete both the body of the document and the user xattrs. If there are
no system xattrs then the document is removed from the bucket
completely. If there are system xattrs then the document is
"soft-deleted", leaving the system xattrs intact, but still marking the
document as deleted. This means that any future access of the document
will need to use the AccessDeleted flag.

- Inputs:
    - Optional doc flag (AccessDeleted)

- Outputs:
    - None

- Errors:
    - Generic subdoc errors

### ReplaceBodyWithXattr

This command will replace the body section of a document with the value
from an XATTR.

- Inputs:
  - The key of the document to perform the operation on
  - Path to the existing xattr to replace the body with. Path must not
    be empty

- Outputs:
  - None

- Errors:
    - Generic subdoc errors

## Multi-path Commands

These commands operate on multiple paths within a *single* document.

The benefit of multi-path operations is that the client may enjoy
the atomicity and performance of having multiple atomic operations
performed in a transactional fashion (i.e. while the document is
"Locked").

#### Multi-Path Mutations - motivation

The benefit of multi-path mutations is illustrated here by displaying
the problems it solves.

To perform multiple mutations on a single document, one can
either:

Approach #1 - Using document-level GET/SET

1. GET the entire document
2. Make mutations in the document locally
3. SET the document back on the server, accounting for CAS
   mismatches

While approach #1 may seem straight-forward, it causes unnecessary data
to be transported over the network; this may be a significant overhead,
especially if the document is large and the fields being modified are
small.

Approach #2 - Using subdoc-level operations without CAS

Simply pipeline each mutation operation as a single subdocument
command, e.g.

1. Send mutation #1
2. Send mutation #2
3. Await responses

This approach works well when the mutations are independent of each
other, and when the document can be considered valid to the application
logic for each mutation; for example, updating independent detail fields
of a product (such as metadata tags and image links; each can exist
independently without the other). If multiple updates are dependent on
each other (which happens if one field in the document references
another, or if business logic dictates such), then this solution is
inadequate because as each update is independent, it is possible for
clients to receive an inconsistent document if a GET is performed
between two mutation operations. Furthermore, if any of the mutations
failed for one reason or another, the document would be in a corrupt
state; and a truly defensive application would then need to get the
original copy of the document to be able to "rollback" the document to
its state before the mutation pipeline.

#### Multi-Path Lookups

Multi-path commands permit access to a consistent snapshot of the
document. For example, if the user wanted to read the values of two
paths and was only concerned with reading consistent values, without
multi-path they would need to:

1. Send `CMD_GET`]{.c3}` on first path.
2. Send `CMD_GET` on second path.
3. Compare CAS values returned from (1) and (2). If they match then
   success, otherwise retry both (1) and (2) until CAS values match.

With a multi command, the client would only need a single request, with
no need for a possible retry:

- Send multi-path `CMD_GET` for both first and second path.

There is an implementation-defined limit on the number of paths which
can be performed in a single multi-path command. Currently, this is 16
paths.

### CMD_MULTI_MUTATION

This command encapsulates several mutation directives on a single key .
The mutation directives are performed atomically on the server and are
executed *atomically*. This means that either all operations will
be completed successfully, or none of them are executed.

- Inputs:
  - The key of the document to *mutate*
  - CAS value (optional)
  - Expiry (optional)
  - For each mutation:
    - Mutation type (the command code). Acceptable codes are any
      combination of:
      - COUNTER
      - REPLACE
      - DICT_ADD
      - DICT_UPSERT
      - ARRAY_PUSH_FIRST
      - ARRAY_PUSH_LAST
      - ARRAY_ADD_UNIQUE
      - ARRAY_INSERT
    - Mutation path
    - Mutation value (if applicable)

- Output:
  - A set of ResultSpec (see below), with ResultSpecs being given for
    those operations which have something relevant to return (either a
    value or an error).
  - On Success
    - If MUTATION_SEQNO feature enabled, 16Byte mutation seqno/vBucket
      UUID in the *extras* section .
  - On failure: A single ResultSpec containing the index and error of the first
    failed operation.

- Errors:
  - `SUCCESS`if all commands succeeded.
  - Normal error codes if there was an error in accessing the requested
    key (e.g. `ENOENT`, `EEXISTS`, etc).
  - `SUBDOC_MULTI_PATH_FAILURE` if one or more of the mutations
    failed. The specific error code will be found in a single ResultSpec.
  - `STATUS_INVALID_CMD_COMBO` if an invalid combination of
    commands were specified, including too many paths specified.

#### Issues

It is debatable whether mutations involving overlapping paths should be
allowed. While there is little rationale for an application specifying
overlapping, it is a foreseeable situation in the case of automatically
generated requests (for example, "`SELECT foo, foo.bar, foo.baz`" in N1QL; while
inefficient, is still valid).

Having overlapping paths would make the parsing inefficient (it may be
possible to optimize the parser so it can detect all paths in a single
sweep, however the paths would not be able to overlap)

### CMD_MULTI_LOOKUP

This command encapsulates several lookup and retrieval directives. The
lookups are performed "atomically" on the server (meaning that they are
all performed using the same version of the document).

Unlike `CMD_MULTI_MUTATION`, multi-lookup operations can partially
fail (for example, if one path was present, but the other was not). To
allow this, the response format for MULTI_LOOKUP is different from
MULTI_MUTATION, in that every lookup path will receive a result
regardless of success or failure, as long as the actual key ("Document
ID") was successfully accessed (i.e. the overall status is SUCCESS or
PARTIAL_FAILURE - see below).

- Inputs:
  - The key of the document to lookup
  - A repeating list of:
    - Lookup type. This can be one of:
      - GET
      - EXISTS
      - GET_COUNT
    - Path to search

- Outputs:
  - Normal memcached response header.
  - A series of *Lookup Results* for each path requested in the
    lookup, as long as the specified key ("Document ID") could be
    accessed, indicated by an overall (header status) of `SUCCESS`
    or `SUBDOC_MULTI_PATH_FAILURE`
    - Status code
    - Value (if applicable)

- Errors
  - Top-level (i.e. response header) status codes:
    - `SUCCESS` if  all paths were looked up successfully.
    - `ERANGE` If the number of paths exceeds the implementation
      limit.
    - `INVALID_CMD_COMBO` if an invalid combination of commands
      was specified.
    - `SUBDOC_MULTI_PATH_FAILURE` if one or more path lookups
      failed. Examine the individual lookup results for details.
    - Normal memcached errors.
  - Per-lookup status codes:
    - Generic subdoc errors

#### Examples

Considering the following document:

    {
       "date": "22/16/2015",
       "from": "mnunberg",
       "to": "couchbase",
       "subject": "Subdoc Commands",
       "body": "This is the updated spec blah blah blah"
    }

**Request:* MULTI_LOOKUP(from, to, cc, bcc, subject)

**Response:**

    [
       (SUCCESS, “mnunberg”),
       (SUCCESS, “couchbase”),
       (PATH_ENOENT, NULL),
       (PATH_ENOENT, NULL),
       (SUCCESS, “Subdoc Commands”)
    ]


# Wire Protocol Format

The codes 0xC5 to 0xE0 shall be reserved for subdoc operations. Any
modifiers to the commands (for example, `MKDIR_P` will be
present as a bit in an optional 1 byte extras field

## Single-Path Commands

For operations which require *inputs* (such as the various
UPSERT/INSERT/REPLACE commands), the actual input value is placed at the
end of the payload.

**Header**

| # Bytes @offset | Description                                                  |
|-----------------|--------------------------------------------------------------|
| 1 @0            | Memcached magic                                              |
| 1 @1            | Opcode                                                       |
| 2 @2            | Length of document ID                                        |
| 1 @4            | Size of extras, 3 or 7 (extras are 3 or 7 bytes , see below) |
| 1 @5            | Datatype                                                     |
| 18 @6           | Rest of memcached frame                                      |

**Body**

| # Bytes @offset                   | Description                                     |
|-----------------------------------|-------------------------------------------------|
| 2 @ 24                            | Path length                                     |
| 1 @ 26                            | Flags                                           |
| (Optional) 4@27                   | Expiration time (if present, extras is 7 or 8). |
| (Optional) 1@27                   | Doc flags (if present, extras is 4 or 8)        |
| [KEYLEN] @ 27                     | The key of the document to find                 |
| [PATHLEN] @ 27 + KEYLEN           | Sub-document Path                               |
| [BODYLEN] @ 27 + [KEYLEN+PATHLEN] | Sub-doducment value (if applicable)             |

### Single Response

Since the memcached status-code space is 16 bits wide, we can afford to
add a few status codes in the top level. Successful commands should
yield the status code 0x00

For responses that yield a value, the value shall be placed as the
response body. None of the responses shall contain the key of the
command.

If the *MUTATION_SEQNO* feature is enabled (via the HELLO
command), the additional uuid,seqno (16 bytes) are placed *after*
the opcode in the extras, making the response extras lenght 16.

**Response Header**

| # Bytes @offset                  | Description                           |
|----------------------------------|---------------------------------------|
| 1 @0 | memcached magic                       |
| 1 @1 | Opcode (PROTOCOL_BINARY_CMD_SUBDOC_\* |
| 2 @2 | Key length (always 0) |
| 1 @4 | Size of extras, 0 or 16 depending if mutation tokens are present | 
| 1 @5 | Memcached datatype |
| 18@6 | Rest of memcached frame (size, opaque, cas) |

If there is a value (present for successful GET and COUNTER responses),
they are after any extras. Extras are only present when there is any
kind of mutation token.

#### Example

The following example shows how a request/response sequence will look
like for setting the *rank* of a certain *game*  (
*revolution*) for a given *user* (
`"user:hugo_chavez"` to the value of "Comandante".

##### Request

- Header:
  - MAGIC: `PROTOCOL_BINARY_REQ`
  - OP: `PROTOCOL_BINARY_CMD_SUBDOCUMENT_DICT_UPSERT`
  - KEYLEN: 16
  - EXTRA LENGTH: 3
  - DATATYPE: 0
  - VBUCKET: 668
  - BODYLEN: 50
  - OPAQUE: 0xFEE5
  - CAS: 0xDEADBEEF

- Extras:
  - extras[0:1 = 21
  - extras[2] = `PROTOCOL_BINARY_SUBDOCUMENT_FLAG_MKDIR_P`

- Body:
  - "user:hugo_chavez" (document *key*)
  - "games.revolution.rank" (subdoc path)
  - "Comandante" (subdoc value)

##### Response

- Header:
  - MAGIC: `PROTOCOL_BINARY_RES`
  - OP: `PROTOCOL_BINARY_SUBDOCUMENT_UPSERT`
  - KEYLEN: 0
  - EXTRA LENGTH: 0
  - DATATYPE: 0
  - STATUS: *SUCCESS*
  - BODYLEN: 0
  - OPAQUE: 0xFEE5
  - CAS: 0xCAFEBABE

## Multi-Path Commands

This format is for use with `CMD_MULTI_MUTATION` and
`CMD_MULTI_LOOKUP`. These commands act as container frames for the
individual operations they contain.

### Request Format

**Header**

| # Bytes @offset | Description                                      |
|-----------------|--------------------------------------------------|
| 1 @0            | Memcached magic                                  |
| 1 @1            | `CMD_MULTI_MUTATION` or `CMD_MULTI_LOOKUP`       |
| 2 @2            | Length of document ID                            |
| 1 @4            | Length of extras, 0 or 4 depending on expiration |
| 1 @5            | Datatype                                         |
| 18 @6           | Rest of memcached frame                          |

**Body**

| # Bytes @offset        | Description                                                                    |
|------------------------|--------------------------------------------------------------------------------|
| (Optional) 4 @ 24      | Expiry (if extras len is 4 or 5)                                               |
| (Optional) 1 @ 24      | Doc Flags (If extras len is 1 or 5)                                            |
| KEYLEN @ 24            | The key of the document to lookup.                                             |
| <variable> @ 24+KEYLEN | An *Operation Spec*. This is repeated once for each path / command to perform. |

**Operation Spec**

| # Bytes @offset                  | Description                                                                                                                          |
|----------------------------------|--------------------------------------------------------------------------------------------------------------------------------------|
| 1 @0                             | Opcode                                                                                                                               |
| 1 @1                             | Opcode flags. This accepts flags which would be specified in the subdoc flags field for the single form.                             |
| 2 @2                             | Path Length                                                                                                                          |
| 4 @4                             | Value length, if the specific *Opcode* requires a value (this is false for lookup operations, and true for most mutation operations) |
| path length @[4 or 8]            | Path                                                                                                                                 |
| [value length] @[8+path length]  | Value (if mutation operation requires a value. This is empty for DELETE, for example)                                                |


### Response Format

**Header**


| # Bytes @offset                  | Description                                                      |
|----------------------------------|------------------------------------------------------------------|
| 1 @0 | memcached magic                                                  |
| 1 @1 | `CMD_MULTI_MUTATION` or `CMD_MULTI_LOOKUP`                       |
| 2 @2 | Length of document key                                           |
| 1 @4 | Size of extras, 0 or 16 depending if mutation tokens are present | 
| 1 @5 | Memcached datatype                                               |
| 18@6 | Rest of memcached frame (size, opaque, cas)                      |

**Body - Successful Mutation**

- MUTATION_SEQNO disabled:
    - Empty

- MUTATION_SEQNO enabled:

| # Bytes @offset | Description                                       |
|-----------------|---------------------------------------------------|
| 8 @0            | Mutation sequence numberMutation sequence numberc |
| 8 @8            | vbucket UUID                                      |
 
**Body - Mutation Result**

- For failed multi mutations, there will always be exactly **one**
  lookup result containing the index and payload
- For successful mutli mutations, there will be *zero* or more
  results; each of the results containing a value.

| # Bytes @offset | Description                                                                                                                                        |
|-----------------|----------------------------------------------------------------------------------------------------------------------------------------------------|
| 1 @0            | Index (i.e. request number) this result pertains to                                                                                                |
| 2 @1            | Status of the mutation. If the status indicates success, the next two fields are applicable. If it is an error then the result has been fully read |
| 4 @3            | Result value length (only if status is success)                                                                                                    |
| vallen @7       | Value payload                                                                                                                                      |

**Body - Lookups**

A series of *Lookup Results*. These are repeated in sequence,
with each result corresponding to an *Operation Spec* specified
at the same index within the command.

**Lookup Result**

| # Bytes @offset | Description                                                                                                                                        |
|-----------------|----------------------------------------------------------------------------------------------------------------------------------------------------|
| 2 @0            | Status Code |
| 4 @2            | Value Length (Can be 0 if !SUCCESS or cmd != GET)                                                                                                    |
| vallen @6       | Value payload                                                                                                                                      |

### Examples

#### Example - Multiple Mutations

The following request UNIQUE_ADDs "192.168.3.4" to "login_locations",
increments a "login_count" counter, and sets "state" to "logged_in"
within the item with the key of "u:1234"

##### Request

- Header:
  - MAGIC: PROTOCOL_BINARY_REQ
  - OP: MULTI_MUTATION
  - KEYLEN: 6
  - EXTRA LENGTH: 1
  - DATATYPE: 0
  - VBUCKET: 668
  - BODYLEN: 87
  - OPAQUE: 0xFEE5
  - CAS: 0xDEADBEEF

- Extras:
  - extras[0]: 0

- Body:
  - Key: u:1234
  - Mutations:
    1. First mutation
       - OP: ARRAY_ADD_UNIQUE
       - FLAGS: MKDIR_P
       - PATHLEN: 15
       - VALLEN: 13
       - PATH: login_locations
       - VALUE: "192.168.3.4"
    2. Second mutation
       - OP: COUNTER
       - FLAGS: MKDIR_P
       - PATHLEN: 11
       - VALLEN: 1
       - PATH: login_count
       - VALUE: 1
    3. Third Mutation:
       - OP: DICT_UPSERT
       - FLAGS: MKDIR_P
       - PATHLEN: 5
       - VALLEN: 11
       - PATH: state
       - VALUE: "logged_in"
    4. Fourth Mutation:
       - OP: REMOVE
       - FLAGS: RETURN_VALUE
       - PATHLEN: 5
       - PATH: queue

##### Response (Success)

- Header:
  - MAGIC: PROTOCOL_BINARY_RES
  - OP: CMD_MULTI_MUTATION
  - KEYLEN: 0
  - EXTRA LENGTH: 0
  - DATATYPE: 0
  - STATUS: SUCCESS
  - BODYLEN: 9
  - OPAQUE: 0xFEE5
  - CAS: 0xCAFEBABE

- Body:
  - Result:
    1. INDEX: 1 (COUNTER)
    2. STATUS: SUCCESS
    3. VALLEN: 2
    4. VALUE: 42
  - Result:
    1. INDEX 3: (REMOVE)
    2. STATUS: SUCCESS
    3. VALLEN: 10
    4. VALUE: "deleteme"

##### Response (Error, failed on second mutation)

- Header:
  - MAGIC: PROTOCOL_BINARY_RES
  - OP: CMD_MULTI_MUTATION
  - KEYLEN: 0
  - EXTRA LENGTH: 0
  - DATATYPE: 0
  - STATUS: SUBDOC_MULTI_PATH_FAILURE
  - BODYLEN: 1
  - OPAQUE: 0xFEE5
  - CAS: 0xCAFEBABE

- Body:
  - Result:
    1. INDEX: 1
    2. STATUS_PATH_MISMATCH

#### Example - Multi Lookups

##### Request

- Header:
  - MAGIC: PROTOCOL_BINARY_REQ
  - OP: MULTI_LOOKUP
  - KEYLEN: 6
  - EXTRA LENGTH: 1
  - DATATYPE: 0
  - VBUCKET: 668
  - BODYLEN: 47
  - OPAQUE: 0xFEE5
  - CAS: 0xDEADBEEF

- Extras:
 - extras[0]: 0

- Body:
  - Key: u:1234
  - Lookups:
    1. First lookup
       - OP: GET
       - FLAGS: 0
       - PATHLEN: 4
       - PATH: from
    2. Second lookup
       - OP: GET
       - FLAGS: 0
       - PATHLEN: 2
       - PATH: to
    3. Third Lookup:
       - OP: EXISTS
       - FLAGS: 0
       - PATHLEN: 3
       - PATH: bcc
    4. Fourth lookup:
       - OP: GET
       - FLAGS: 0
       - PATHLEN: 7
       - PATH: subject
    5. Fifth lookup:
       - OP: EXISTS
       - FLAGS: 0
       - PATHLEN: 4
       - PATH: body

##### Response

- Header:
  - MAGIC: PROTOCOL_BINARY_RES
  - OP: CMD_MULTI_LOOKUP
  - KEYLEN: 0
  - EXTRA LENGTH: 0
  - DATATYPE: 0
  - STATUS: SUCCESS
  - BODYLEN: 68
  - OPAQUE: 0xFEE5
  - CAS: 0xCAFEBABE

- Body
  - Lookup Results
    1. Result for Spec #1
       - STATUS: SUCCESS
       - VALLEN: 10
       - VALUE: "mnunberg"
    2. Result for spec #2
       - STATUS: SUCCESS
       - VALLEN: 11
       - VALUE: "couchbase"
    3. Result for spec #3
       - STATUS: PATH_ENOENT
       - VALLEN: 0
    4. Result for spec #4
       - STATUS: SUCCESS
       - VALLEN: 17
       - VALUE: "Subdoc Commands"
    5. Result for spec #5
       - STATUS: SUCCESS
       - VALLEN: 0 (was exists, so no value)

# Limits

| # Description                          | Value                                   |
|----------------------------------------|-----------------------------------------|
| Maximum path components                | 32 (subjson:MAX_COMPONENTS)             |
| Maximum path length (bytes)            | 1024 (memcached:SUBDOC_MAX_PATH_LENGTH) |
| Maximum # paths in a multipath request | 16 (memcached: SUBDOC_MULTI_MAX_PATHS)  |

