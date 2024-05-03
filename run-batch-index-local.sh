#!/bin/sh
ip=$(hostname -i)

./distribution.js --local 2 --ip "$ip" --port 7070 --index --prefixname map-seed-output