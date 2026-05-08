#!/bin/bash
set -e

cd dashboard
if [ -f package-lock.json ]; then
  npm ci
else
  npm install
fi
npm run build
cd ..

mkdir -p html
cp -r dashboard/html/* html/

CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags "" -o apiok-admin main.go

cp apiok-admin release/
chmod +x release/apiok-admin
cp -r config release/
