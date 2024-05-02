#!/bin/sh
ip=$(hostname -i)
ls ./data | grep output | while read -r line
do
	echo "Running batch $line"
	./distribution.js --local --crawl --filename $line
	# ./distribution.js --ip "$ip" --port 7080 &
	# ./distribution.js --ip "$ip" --port 7090
    sleep 10
done