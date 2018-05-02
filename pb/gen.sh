#!/usr/bin/env bash
DIRS="./storagepb ./errorpb ./configpb ./metapb ./datapb"
for dir in ${DIRS}; do
    protoc -I=. -I=$GOPATH/src/ --gofast_out=. --twirp_out=. ${dir}/*.proto
done
