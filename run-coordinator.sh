#!/bin/sh
ip=$(hostname -i)
ls ./data | grep output | while read -r line
do
	./distribution.js --ip "$ip" --port 7070 &
	./distribution.js --ip "$ip" --port 7080 &
	./distribution.js --ip "$ip" --port 7090
done
