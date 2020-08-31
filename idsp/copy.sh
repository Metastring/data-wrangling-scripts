#!/bin/sh

set -e

if [ -z "$1" ]
  then
    echo "Pass in the data directory as first argument"
    exit
fi

for DIRECTORY in `ls data`; do
    if [ -d "data/${DIRECTORY}/clean" ]
    then
        mkdir -p "$1/$DIRECTORY"
        cp data/${DIRECTORY}/clean/*.csv "$1/$DIRECTORY"
    fi
done
