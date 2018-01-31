#!/usr/bin/env bash
DIRS="./storagepb ./errorpb ./configpb ./metapb ./datapb"
for dir in ${DIRS}; do
    protoc -I=. -I=$GOPATH/src/github.com/gogo/protobuf --gofast_out=. ${dir}/*.proto
done
