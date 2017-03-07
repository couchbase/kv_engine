#! /bin/bash
#
# The following script is used to generate the certificates we use
# in the unit tests.
#

set -e

openssl genrsa -out testapp.pem 2048
openssl req -new -x509  -days 3650 -sha256 -key testapp.pem -out testapp.cert \
        -subj '/C=NO/O=Couchbase Inc/OU=kv eng/CN=Root CA'
openssl genrsa -out client.key 2048
openssl req -new -key client.key -out int.csr \
	-subj '/C=NO/O=Couchbase Inc/OU=kv eng/G=Trond/S=Norbye/CN=Trond Norbye'
cat > v3_ca.ext <<EOF
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid:always,issuer:always
basicConstraints = CA:true
EOF
openssl x509 -req -in int.csr -CA testapp.cert \
	-CAkey testapp.pem -CAcreateserial \
        -CAserial rootCA.srl -extfile v3_ca.ext -out client.pem -days 365

rm rootCA.srl v3_ca.ext int.csr
