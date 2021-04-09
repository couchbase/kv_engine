#! /bin/bash
#     Copyright 2019-Present Couchbase, Inc.
#
#   Use of this software is governed by the Business Source License included
#   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
#   in that file, in accordance with the Business Source License, use of this
#   software will be governed by the Apache License, Version 2.0, included in
#   the file licenses/APL2.txt.

auth=Administrator:asdfasdf
host=http://127.0.0.1:8091
library=/opt/jaeger/lib/libjaegertracing.so.0.5.0

set -e
curl -u ${auth} \
          -X POST \
           ${host}/pools/default/settings/memcached/node/self \
          --data "opentracing_module=${library}" > /dev/zero

curl -u ${auth} \
          -X POST \
           ${host}/pools/default/settings/memcached/node/self \
          --data 'opentracing_config=service_name: Couchbase' > /dev/zero

curl -u ${auth} \
          -X POST \
           ${host}/pools/default/settings/memcached/node/self \
          --data 'opentracing_enabled=true' > /dev/zero
