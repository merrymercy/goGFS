#!/bin/bash
dir=$1
export GOPATH=$dir
cd $dir
go run stress/stress_node.go -conf ~/conf
