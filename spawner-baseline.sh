#!/bin/sh

i=10; while [ $i -le 200 ]; do
	echo $i
	node baseline-spawn.js $i | grep duration | awk '{print $2}' >> baseline-spawner-results.txt
	i=$(( i + 10 ))
done
