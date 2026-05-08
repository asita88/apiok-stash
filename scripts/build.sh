#!/bin/bash
set -e

CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags "" -o apiok-stash stash/stash.go

cp apiok-stash release/
chmod +x release/apiok-stash
cp -r stash/etc release/etc