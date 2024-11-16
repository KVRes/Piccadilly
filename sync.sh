#!/bin/bash

rm -fr ./SDK/pb
cp -r ./pb ./SDK/pb

rm -fr ./SDK/client
cp -r ./client ./SDK/client

rm -fr ./SDK/types
cp -r ./types ./SDK/types

rm -fr ./SDK/go.mod
cp -r ./go.mod ./SDK/go.mod
cd SDK
bash replace.sh