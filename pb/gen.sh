#!/bin/bash
#
# Generate all prophet protobuf bindings.
# Run from repository root.
#
set -e

# directories containing protos to be built
DIRS="./metapb ./rpcpb"

PRJ_PB_PATH="${GOPATH}/src/github.com/deepfabric/prophet/pb"

for dir in ${DIRS}; do
	pushd ${dir}
		protoc  -I=.:"${PRJ_PB_PATH}":"${GOPATH}/src" --gogofaster_out=plugins=grpc:.  *.proto
		goimports -w *.pb.go
	popd
done
