#!/bin/sh
ip=$(hostname -i)
while
	./distribution.js --ip "$ip" --port 7070 &
	./distribution.js --ip "$ip" --port 7080 &
	./distribution.js --ip "$ip" --port 7090
do :; done
