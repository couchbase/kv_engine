#! /bin/bash

# Small script to allow for creating self signed certificates with a custom
# configuration (number of bits, digest algorithm, server IP etc).

# Update the following to match your environment
server=127.0.0.1:8091
server_ip=192.168.101.139
user=Administrator
password=asdfasdf
ROOT=/Users/trond.norbye/compile/trunk/cmake-install-debug/var/lib/couchbase/inbox
digest=sha512
bits=4096

set -e
mkdir -p /tmp/self-signed-certs
cd /tmp/self-signed-certs

rm -rf servercertfiles
mkdir servercertfiles
cd servercertfiles
mkdir -p {public,private,requests,clientcertfiles}

openssl genrsa -out ca.key ${bits}
openssl req -new -x509 -days 3650 -${digest} -key ca.key -out ca.pem \
        -subj "/CN=Couchbase Root CA"

openssl genrsa -out private/couchbase.default.svc.key ${bits}
openssl req -new -key private/couchbase.default.svc.key \
        -out requests/couchbase.default.svc.csr -subj "/CN=Couchbase Server"

cat > server.ext <<EOF
basicConstraints=CA:FALSE
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid,issuer:always
extendedKeyUsage=serverAuth
keyUsage = digitalSignature,keyEncipherment
EOF

cp ./server.ext ./server.ext.tmp

echo "subjectAltName = IP:${server_ip}" >> ./server.ext.tmp

openssl x509 -CA ca.pem -CAkey ca.key -CAcreateserial -days 365 -req \
        -in requests/couchbase.default.svc.csr \
        -out public/couchbase.default.svc.pem \
        -extfile server.ext.tmp

cd ./public
mv couchbase.default.svc.pem chain.pem
cd ../private
mv couchbase.default.svc.key pkey.key
cd ..

rm -rf $ROOT
mkdir -p $ROOT
chmod go-rwx $ROOT
cp  ./public/chain.pem ./private/pkey.key $ROOT

mkdir -p ${ROOT}/CA
cp ca.pem  ${ROOT}/CA

curl -X POST http://${server}/node/controller/loadTrustedCAs -u ${user}:${password}
curl -X POST http://${server}/node/controller/reloadCertificate -u ${user}:${password}
