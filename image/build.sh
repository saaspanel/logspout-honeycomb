#!/bin/sh
set -e

echo "http://alpine.gliderlabs.com/alpine/v3.5/community" >> /etc/apk/repositories
apk add --update 'go>1.7' git mercurial build-base ca-certificates

mkdir -p /go/src/github.com/gliderlabs
cp -r /src /go/src/github.com/gliderlabs/logspout
cd /go/src/github.com/gliderlabs/logspout
export GOPATH=/go

go get github.com/honeycombio/libhoney-go
go get
go get github.com/saaspanel/logspout-honeycomb@v0.0.5
go build -ldflags "-X main.Version=$1" -o /bin/logspout
apk del go git mercurial build-base
rm -rf /go
rm -rf /var/cache/apk/*

# backwards compatibility
ln -fs /tmp/docker.sock /var/run/docker.sock
