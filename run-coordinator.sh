#!/bin/sh
ip=$(hostname -i)
ls ./data | grep output | while read -r line
do
	echo "Running batch $line"
	sleep 2
	./distribution.js --ip "$ip" --port 7070 --aws --crawl --filename $line &
	./distribution.js --ip "$ip" --port 7080 &
	./distribution.js --ip "$ip" --port 7090
done
