#! /bin/bash
#     Copyright 2019 Couchbase, Inc
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

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
