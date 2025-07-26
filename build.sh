#!/bin/bash
docker build -t "$1" "$2" 2> >(while read line; do echo "[STDERR] $line"; done) | while read line; do echo "[STDOUT] $line"; done
exit ${PIPESTATUS[0]}
