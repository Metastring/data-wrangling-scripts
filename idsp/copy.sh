#!/bin/sh

set -e

for DIRECTORY in `ls data`; do
    if [ -d "data/${DIRECTORY}/clean" ]
    then
        mkdir -p "$1/$DIRECTORY"
        cp data/${DIRECTORY}/clean/*.csv "$1/$DIRECTORY"
    fi
done
