#!/bin/sh
./distribution.js --ip 172.31.24.189 --port 7070 --crawl &
./distribution.js --ip 172.31.24.189 --port 7080 &
./distribution.js --ip 172.31.24.189 --port 7090
