# Bucket Configuration

## Format
The configuration passed in the `Create Bucket` command is in a simple
key value format. This format is also used elsewhere in Couchbase to encode the
bucket config - e.g. when ns_server passes it to services for validation.

Key value pairs are separated by a semicolon (`;`). The key
and value in a pair are separated by an equals (`=`). Both keys and values have
preceeding and trailing whitespace characters trimmed.

In order to handle spaces and/or separators being present in the keys or values
some escaping rules must be defined. Any character that needs escaping should
be preceeded with a backslash (`\`):

1. Whitespace at the beginning or end of a key or value must be escaped
2. An equals in a key must be escaped
3. A semicolon in a value must be escaped

If a backslash is followed by a character then the result is the following
character, even if the character is not in the list of rules above.

### Examples

| Input           | Parsed value              | Notes                    |
| --------------- | ------------------------- | ------------------------ |
| `k1=v1`         | {"k1": "v1"}              |                          |
| ` k1 = v1 `     | {"k1": "v1"}              | Trim spaces              |
| `\ k1\ =\ v1\ ` | {" k1 ": " v1 "}          | Escape spaces            |
| `k1=v\1`        | {"k1": "v1"}              | Spurious escape          |
| `k1=v1;k2=v2`   | {"k1": "v1", "k2": "v2"}  |                          |
| `k1\==v1`       | {"k1=": "v1"}             | Escape `=`               |
| `k1=v1\;`       | {"k1": "v1;"}             | Escape `;`               |
| `k1==v1`        | {"k1": "=v1"}             | Additional `=` in value  |
| `k1=v1;;k2=v2`  | {"k1": "v1", ";k2": "v2"} | Additional `;` in key    |
