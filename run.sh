#!/bin/sh
ip=$(hostname -i)
./distribution.js --ip "$ip" --port 7070
