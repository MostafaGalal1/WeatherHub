#!/bin/bash

case $1 in
  --view-all)
    curl -X GET "http://localhost:8080/bitcask-kv/view-all"
    ;;
  --view)
    curl -X GET "http://localhost:8080/bitcask-kv/view?key=$2"
    ;;
  --perf)
    curl -X GET "http://localhost:8080/bitcask-kv/perf?clients=$2"
    ;;
  *)
    echo "Usage: $0 [--view-all | --view --key=<some_key> | --perf --clients=<no_clients>]"
    ;;
esac
